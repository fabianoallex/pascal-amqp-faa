unit AMQP.Server.Connection;

{$I amqp.inc}

{ Uma conexão do broker: a thread de leitura + o writer serial (sub-módulo
  server). WS6 (skeleton): lê o protocol-header, valida, e então drena frames
  até o peer desconectar — SEM handshake nem dispatch ainda. WS4 acopla a FSM
  de Connection.Start/Tune/Open, o mapa de canais e o despacho de métodos.

  Modelo de execução (ver CLAUDE.md "Sub-módulo broker"): thread-por-conexão,
  I/O bloqueante. Só esta thread lê o socket. Shutdown (chamado pelo broker de
  outra thread) fecha o socket para desbloquear um ReadFrom pendente. A conexão
  NUNCA se auto-libera (sem FreeOnTerminate) — o broker faz o reap. }

interface

uses
  SysUtils,
  Classes,
  AMQP.Threading,
  AMQP.Protocol,
  AMQP.Frame,
  AMQP.Transport,
  AMQP.Server.FrameIO;

const
  /// frame-max que o broker aceita até negociar no Tune (WS4). Piso da spec.
  AMQP_SERVER_FRAME_MAX_BOOT = 131072;

type
  TAMQPServerConnection = class;

  /// Notificação de que a conexão terminou (thread de leitura saiu). Disparada
  /// NA thread da conexão; o handler (o broker) deve só mover a conexão para a
  /// lista de reap, sem bloquear.
  TAMQPConnectionClosedEvent = procedure(AConn: TAMQPServerConnection) of object;

  TAMQPServerConnThread = class(TThread)
  private
    FOwner: TAMQPServerConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TAMQPServerConnection);
  end;

  TAMQPServerConnection = class
  private
    FSocket: TAMQPTcpSocket;
    FStream: TAMQPSocketStream;
    FWriter: TAMQPFrameWriter;
    FThread: TAMQPServerConnThread;
    FPeer: string;
    FFramesRead: Integer;      // atômico
    FProtocolOk: Boolean;
    FError: string;
    FClosing: Integer;         // atômico (0/1)
    FOnClosed: TAMQPConnectionClosedEvent;
    function ReadProtocolHeader: Boolean;
    procedure RunReadLoop;
  public
    constructor Create(ASocket: TAMQPTcpSocket);
    destructor Destroy; override;

    /// Arranca a thread de leitura.
    procedure Start;
    /// Fecha o socket (desbloqueia o ReadFrom) e para o writer. Chamável de
    /// outra thread e mais de uma vez. Não faz join — use WaitFor.
    procedure Shutdown;
    /// Bloqueia até a thread de leitura sair.
    procedure WaitFor;

    property Peer: string read FPeer;
    property Writer: TAMQPFrameWriter read FWriter;
    property FramesRead: Integer read FFramesRead;
    property ProtocolOk: Boolean read FProtocolOk;
    property LastError: string read FError;
    property OnClosed: TAMQPConnectionClosedEvent read FOnClosed write FOnClosed;
  end;

implementation

{ TAMQPServerConnThread }

constructor TAMQPServerConnThread.Create(AOwner: TAMQPServerConnection);
begin
  FOwner := AOwner;
  inherited Create(True); // suspenso; Start explícito
end;

procedure TAMQPServerConnThread.Execute;
begin
  FOwner.RunReadLoop;
end;

{ TAMQPServerConnection }

constructor TAMQPServerConnection.Create(ASocket: TAMQPTcpSocket);
begin
  inherited Create;
  FSocket := ASocket;
  FPeer := ASocket.PeerAddress;
  FStream := TAMQPSocketStream.Create(FSocket); // não é dono do socket
  FWriter := TAMQPFrameWriter.Create(FStream);
  FThread := TAMQPServerConnThread.Create(Self);
end;

destructor TAMQPServerConnection.Destroy;
begin
  Shutdown;
  if FThread <> nil then
  begin
    FThread.WaitFor;
    FThread.Free;
  end;
  FWriter.Free;   // Stop já foi chamado no Shutdown
  FStream.Free;
  FSocket.Free;   // a conexão É dona do socket aceito
  inherited;
end;

procedure TAMQPServerConnection.Start;
begin
  FThread.Start;
end;

procedure TAMQPServerConnection.Shutdown;
begin
  if AmqpAtomicCompareExchange(FClosing, 1, 0) <> 0 then
    Exit; // já em curso
  // Fecha o socket: desbloqueia um ReadFrom pendente na thread de leitura.
  if FSocket <> nil then
    FSocket.Close;
  if FWriter <> nil then
    FWriter.Stop;
end;

procedure TAMQPServerConnection.WaitFor;
begin
  if FThread <> nil then
    FThread.WaitFor;
end;

function TAMQPServerConnection.ReadProtocolHeader: Boolean;
var
  LBuf: array[0..7] of Byte;
  LTotal, LRead, I: Integer;
begin
  FillChar(LBuf, SizeOf(LBuf), 0);
  LTotal := 0;
  while LTotal < 8 do
  begin
    LRead := FStream.Read(LBuf[LTotal], 8 - LTotal);
    if LRead <= 0 then
      Exit(False);
    Inc(LTotal, LRead);
  end;
  for I := 0 to 7 do
    if LBuf[I] <> AMQP_PROTOCOL_HEADER[I] then
    begin
      // spec 4.2.2: devolve o header suportado e encerra.
      try
        FSocket.Send(AMQP_PROTOCOL_HEADER[0], Length(AMQP_PROTOCOL_HEADER));
      except
      end;
      FError := 'protocol-header inválido';
      Exit(False);
    end;
  Result := True;
end;

procedure TAMQPServerConnection.RunReadLoop;
var
  LFrame: TAMQPFrame;
begin
  try
    try
      FProtocolOk := ReadProtocolHeader;
      if not FProtocolOk then
        Exit;

      // WS6: sem handshake ainda — só drena frames até o peer desconectar.
      // (Um cliente AMQP real ficaria esperando Connection.Start; os testes
      //  do skeleton usam socket cru.)
      while True do
      begin
        LFrame := TAMQPFrame.ReadFrom(FStream, AMQP_SERVER_FRAME_MAX_BOOT);
        AmqpAtomicInc(FFramesRead);
        if LFrame.IsHeartbeat then
          FWriter.PostFrame(TAMQPFrame.Heartbeat);
      end;
    except
      on E: EAMQPFrame do
        ; // fim de stream / peer desconectou — normal
      on E: Exception do
        if FError = '' then
          FError := E.ClassName + ': ' + E.Message;
    end;
  finally
    if FWriter <> nil then
      FWriter.Stop;
    if Assigned(FOnClosed) then
      FOnClosed(Self);
  end;
end;

end.
