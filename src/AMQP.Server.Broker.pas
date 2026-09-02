unit AMQP.Server.Broker;

{$I amqp.inc}

{ O broker: ciclo de vida do listener, loop de accept e registro de conexões
  (sub-módulo server, WS6 skeleton).

  WS6 entrega o esqueleto: Start liga o listener e uma thread de accept que
  cria uma TAMQPServerConnection por conexão; Stop derruba tudo sem leak.
  A FSM de handshake, o dispatch de métodos, a engine de roteamento e os
  heartbeats entram nos workstreams seguintes (WS4/WS5/WS7).

  Uso:
    B := TAMQPServer.Create;
    B.BindAddress := '0.0.0.0';
    B.Port := 5672;
    // B.Authenticator := TAMQPStaticAuthenticator.Create(['admin','s3cr3t']);
    B.VirtualHosts.Add('/app');
    B.Start;
    ...
    B.Stop;  // idempotente; Destroy também para
}

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Transport,
  AMQP.Server.Auth,
  AMQP.Server.Connection;

type
  { Conjunto de virtual-hosts que o broker aceita no Connection.Open.
    Fase 1: só a existência importa (sem isolamento de recursos, sem
    permissões — ver desvios no CLAUDE.md). Default: só a raiz "/".
    Thread-safe. }
  TAMQPVirtualHostRegistry = class
  private
    FLock: TCriticalSection;
    FNames: TStringList; // CaseSensitive, Sorted, Duplicates ignorados
  public
    constructor Create;
    destructor Destroy; override;
    procedure Add(const AName: string);
    procedure Remove(const AName: string);
    function Contains(const AName: string): Boolean;
    function ToArray: TArray<string>;
  end;

  TAMQPServer = class;

  TAMQPAcceptThread = class(TThread)
  private
    FServer: TAMQPServer;
  protected
    procedure Execute; override;
  public
    constructor Create(AServer: TAMQPServer);
  end;

  TAMQPServer = class
  private
    FListener: TAMQPTcpListener;
    FAccept: TAMQPAcceptThread;
    FLock: TCriticalSection;
    FActive: TList<TAMQPServerConnection>;
    FDead: TList<TAMQPServerConnection>;
    FAuth: IAMQPAuthenticator;
    FAuthorizer: IAMQPAuthorizer;
    FVHosts: TAMQPVirtualHostRegistry;
    FBindAddress: string;
    FPort: Word;
    FBacklog: Integer;
    FRunning: Boolean;
    FStopping: Boolean;
    FTotalAccepted: Integer; // atômico
    procedure AcceptLoop;
    procedure HandleConnClosed(AConn: TAMQPServerConnection);
    procedure ReapDead;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Start;
    procedure Stop;

    /// Nº de conexões vivas neste momento.
    function ConnectionCount: Integer;
    /// Total de conexões aceitas desde o Start (inclui as já fechadas).
    property TotalAccepted: Integer read FTotalAccepted;

    property BindAddress: string read FBindAddress write FBindAddress;
    /// Porta a ligar; 0 = o SO escolhe. Depois do Start, reflete a efetiva.
    property Port: Word read FPort write FPort;
    property Backlog: Integer read FBacklog write FBacklog;
    property Running: Boolean read FRunning;

    /// Default: TAMQPStaticAuthenticator com guest/guest. Só antes do Start.
    property Authenticator: IAMQPAuthenticator read FAuth write FAuth;
    property Authorizer: IAMQPAuthorizer read FAuthorizer write FAuthorizer;
    property VirtualHosts: TAMQPVirtualHostRegistry read FVHosts;
  end;

implementation

{ TAMQPVirtualHostRegistry }

constructor TAMQPVirtualHostRegistry.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FNames := TStringList.Create;
  FNames.CaseSensitive := True;
  FNames.Sorted := True;
  FNames.Duplicates := dupIgnore;
  FNames.Add('/');
end;

destructor TAMQPVirtualHostRegistry.Destroy;
begin
  FNames.Free;
  FLock.Free;
  inherited;
end;

procedure TAMQPVirtualHostRegistry.Add(const AName: string);
begin
  FLock.Enter;
  try
    FNames.Add(AName);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPVirtualHostRegistry.Remove(const AName: string);
var
  I: Integer;
begin
  FLock.Enter;
  try
    I := FNames.IndexOf(AName);
    if I >= 0 then
      FNames.Delete(I);
  finally
    FLock.Leave;
  end;
end;

function TAMQPVirtualHostRegistry.Contains(const AName: string): Boolean;
begin
  FLock.Enter;
  try
    Result := FNames.IndexOf(AName) >= 0;
  finally
    FLock.Leave;
  end;
end;

function TAMQPVirtualHostRegistry.ToArray: TArray<string>;
var
  I: Integer;
begin
  Result := nil;
  FLock.Enter;
  try
    SetLength(Result, FNames.Count);
    for I := 0 to FNames.Count - 1 do
      Result[I] := FNames[I];
  finally
    FLock.Leave;
  end;
end;

{ TAMQPAcceptThread }

constructor TAMQPAcceptThread.Create(AServer: TAMQPServer);
begin
  FServer := AServer;
  inherited Create(True);
end;

procedure TAMQPAcceptThread.Execute;
begin
  FServer.AcceptLoop;
end;

{ TAMQPServer }

constructor TAMQPServer.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FActive := TList<TAMQPServerConnection>.Create;
  FDead := TList<TAMQPServerConnection>.Create;
  FVHosts := TAMQPVirtualHostRegistry.Create;
  FAuth := TAMQPStaticAuthenticator.Create; // guest/guest
  FAuthorizer := TAMQPAllowAllAuthorizer.Create;
  FBindAddress := '0.0.0.0';
  FPort := 5672;
  FBacklog := 64;
end;

destructor TAMQPServer.Destroy;
begin
  Stop;
  FActive.Free;
  FDead.Free;
  FVHosts.Free;
  FLock.Free;
  FAuth := nil;
  FAuthorizer := nil;
  inherited;
end;

procedure TAMQPServer.Start;
begin
  if FRunning then
    Exit;
  FStopping := False;
  FTotalAccepted := 0;
  FListener := TAMQPTcpListener.Create;
  FListener.Listen(FBindAddress, FPort, FBacklog);
  FPort := FListener.Port; // efetiva (relevante se veio 0)
  FRunning := True;
  FAccept := TAMQPAcceptThread.Create(Self);
  FAccept.Start;
end;

procedure TAMQPServer.Stop;
var
  LSnapshot: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  LSnapshot := nil;
  if not FRunning then
  begin
    ReapDead;
    Exit;
  end;
  FStopping := True;

  // 1) para o accept: fecha o listener (desbloqueia o Accept pendente).
  if FListener <> nil then
    FListener.Close;
  if FAccept <> nil then
  begin
    FAccept.WaitFor;
    FreeAndNil(FAccept);
  end;
  FreeAndNil(FListener);

  // 2) derruba as conexões vivas. Snapshot sob lock, depois age sem o lock
  //    (HandleConnClosed também pega o lock).
  FLock.Enter;
  try
    SetLength(LSnapshot, FActive.Count);
    for I := 0 to FActive.Count - 1 do
      LSnapshot[I] := FActive[I];
  finally
    FLock.Leave;
  end;
  for I := 0 to High(LSnapshot) do
    LSnapshot[I].Shutdown;
  for I := 0 to High(LSnapshot) do
    LSnapshot[I].WaitFor;

  // 3) o OnClosed de cada conexão já as moveu para FDead; libera todas.
  ReapDead;

  // resto que por acaso ainda esteja em FActive (corrida rara): move e libera.
  FLock.Enter;
  try
    for I := 0 to FActive.Count - 1 do
      FDead.Add(FActive[I]);
    FActive.Clear;
  finally
    FLock.Leave;
  end;
  ReapDead;

  FRunning := False;
end;

procedure TAMQPServer.AcceptLoop;
var
  LSock: TAMQPTcpSocket;
  LConn: TAMQPServerConnection;
begin
  while not FStopping do
  begin
    LSock := FListener.Accept;
    if LSock = nil then
      Break; // listener fechado
    if FStopping then
    begin
      LSock.Free;
      Break;
    end;

    ReapDead; // aproveita para limpar conexões já mortas

    LConn := TAMQPServerConnection.Create(LSock); // conexão vira dona do socket
    LConn.OnClosed := HandleConnClosed;
    FLock.Enter;
    try
      FActive.Add(LConn);
    finally
      FLock.Leave;
    end;
    AmqpAtomicInc(FTotalAccepted);
    LConn.Start;
  end;
end;

procedure TAMQPServer.HandleConnClosed(AConn: TAMQPServerConnection);
begin
  // Disparado NA thread da conexão: só move para a lista de reap.
  FLock.Enter;
  try
    FActive.Remove(AConn);
    if FDead.IndexOf(AConn) < 0 then
      FDead.Add(AConn);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPServer.ReapDead;
var
  LToFree: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  LToFree := nil;
  FLock.Enter;
  try
    SetLength(LToFree, FDead.Count);
    for I := 0 to FDead.Count - 1 do
      LToFree[I] := FDead[I];
    FDead.Clear;
  finally
    FLock.Leave;
  end;
  // Free fora do lock: o destrutor da conexão faz join da thread de leitura.
  for I := 0 to High(LToFree) do
    LToFree[I].Free;
end;

function TAMQPServer.ConnectionCount: Integer;
begin
  FLock.Enter;
  try
    Result := FActive.Count;
  finally
    FLock.Leave;
  end;
end;

end.
