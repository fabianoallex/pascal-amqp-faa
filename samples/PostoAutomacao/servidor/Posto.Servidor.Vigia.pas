unit Posto.Servidor.Vigia;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ POS presence, for the supervisor's panel (ARCHITECTURE.md §11, R3).

  Subscribes to the broker's observability (Phase 4.1) and keeps a table
  user -> connected/since. It does NOT act on reservations -- it only informs, so
  the supervisor can decide whether a POS just dropped off the network or is
  out of order (R1/R2).

  The handler runs on the broker's notifier thread: fast, it only updates the
  table under lock and calls OnMudou (which must only marshal to the UI).

  Phase 4.1 contract: Parar (Unsubscribe) BEFORE destroying this object. }

interface

uses
  SysUtils, SyncObjs, Generics.Collections,
  AMQP.Server.Events, AMQP.Server.Broker,
  Posto.Contratos;

type
  TInfoPresenca = record
    Usuario: string;
    Conectado: Boolean;
    DesdeMs: Int64;          // wall ms of the last state change
    RemoteAddr: string;
  end;

  TOnPresencaMudou = procedure of object;

  TPostoVigia = class
  private
    FBroker: TAMQPServer;
    FLock: TCriticalSection;
    FMapa: TDictionary<string, TInfoPresenca>;
    FConnUser: TDictionary<UInt64, string>;
    FOnMudou: TOnPresencaMudou;
    FAtivo: Boolean;
    procedure OnEventoBroker(const AEvent: TAMQPServerEvent);
    procedure MarcaConectado(const AUser, AAddr: string; AConn: UInt64;
      AConectado: Boolean; AWallMs: Int64);
  public
    constructor Create(ABroker: TAMQPServer);
    destructor Destroy; override;
    procedure Iniciar;
    procedure Parar;
    function Presenca: TArray<TInfoPresenca>;
    property OnMudou: TOnPresencaMudou read FOnMudou write FOnMudou;
  end;

implementation

constructor TPostoVigia.Create(ABroker: TAMQPServer);
begin
  inherited Create;
  FBroker := ABroker;
  FLock := TCriticalSection.Create;
  FMapa := TDictionary<string, TInfoPresenca>.Create;
  FConnUser := TDictionary<UInt64, string>.Create;
end;

destructor TPostoVigia.Destroy;
begin
  Parar;
  FConnUser.Free;
  FMapa.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TPostoVigia.Iniciar;
begin
  if FAtivo then
    Exit;
  FBroker.Subscribe(OnEventoBroker,
    [seConnectionAuthenticated, seConnectionClosed]);
  FAtivo := True;
end;

procedure TPostoVigia.Parar;
begin
  if not FAtivo then
    Exit;
  FBroker.Unsubscribe(OnEventoBroker);
  FAtivo := False;
end;

procedure TPostoVigia.MarcaConectado(const AUser, AAddr: string; AConn: UInt64;
  AConectado: Boolean; AWallMs: Int64);
var
  LInfo: TInfoPresenca;
begin
  if (AUser = '') or (Copy(AUser, 1, Length(PREFIXO_USER_PDV)) <> PREFIXO_USER_PDV) then
    Exit;   // POS terminals only; the 'automacao' connection is not in the table

  FLock.Enter;
  try
    if not FMapa.TryGetValue(AUser, LInfo) then
    begin
      LInfo := Default(TInfoPresenca);
      LInfo.Usuario := AUser;
    end;
    LInfo.Conectado := AConectado;
    LInfo.DesdeMs := AWallMs;
    if AAddr <> '' then
      LInfo.RemoteAddr := AAddr;
    FMapa.AddOrSetValue(AUser, LInfo);
    if AConectado then
      FConnUser.AddOrSetValue(AConn, AUser)
    else
      FConnUser.Remove(AConn);
  finally
    FLock.Leave;
  end;

  if Assigned(FOnMudou) then
    FOnMudou();
end;

procedure TPostoVigia.OnEventoBroker(const AEvent: TAMQPServerEvent);
var
  LUser: string;
begin
  LUser := AEvent.Username;
  case AEvent.EventType of
    seConnectionAuthenticated:
      MarcaConectado(LUser, AEvent.RemoteAddr, AEvent.ConnectionId, True,
        AEvent.WallMs);
    seConnectionClosed:
      begin
        if LUser = '' then
        begin
          FLock.Enter;
          try
            FConnUser.TryGetValue(AEvent.ConnectionId, LUser);
          finally
            FLock.Leave;
          end;
        end;
        MarcaConectado(LUser, AEvent.RemoteAddr, AEvent.ConnectionId, False,
          AEvent.WallMs);
      end;
  end;
end;

function TPostoVigia.Presenca: TArray<TInfoPresenca>;
var
  LInfo: TInfoPresenca;
  L: TList<TInfoPresenca>;
begin
  L := TList<TInfoPresenca>.Create;
  FLock.Enter;
  try
    for LInfo in FMapa.Values do
      L.Add(LInfo);
    Result := L.ToArray;
  finally
    FLock.Leave;
    L.Free;
  end;
end;

end.
