unit FuelStation.Server.Watcher;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ POS presence, for the supervisor's panel (ARCHITECTURE.md §11, R3).

  Subscribes to the broker's observability (Phase 4.1) and keeps a table
  user -> connected/since. It does NOT act on reservations -- it only informs, so
  the supervisor can decide whether a POS just dropped off the network or is
  out of order (R1/R2).

  The handler runs on the broker's notifier thread: fast, it only updates the
  table under lock and calls OnChanged (which must only marshal to the UI).

  Phase 4.1 contract: Shutdown (Unsubscribe) BEFORE destroying this object. }

interface

uses
  SysUtils, SyncObjs, Generics.Collections,
  AMQP.Server.Events, AMQP.Server.Broker,
  FuelStation.Contracts;

type
  TPresenceInfo = record
    LoginUser: string;
    Connected: Boolean;
    SinceMs: Int64;          // wall ms of the last state change
    RemoteAddr: string;
  end;

  TOnPresenceChanged = procedure of object;

  TFuelStationWatcher = class
  private
    FBroker: TAMQPServer;
    FLock: TCriticalSection;
    FMap: TDictionary<string, TPresenceInfo>;
    FConnUser: TDictionary<UInt64, string>;
    FOnChanged: TOnPresenceChanged;
    FActive: Boolean;
    procedure OnBrokerEvent(const AEvent: TAMQPServerEvent);
    procedure MarkConnected(const AUser, AAddr: string; AConn: UInt64;
      AConnected: Boolean; AWallMs: Int64);
  public
    constructor Create(ABroker: TAMQPServer);
    destructor Destroy; override;
    procedure Startup;
    procedure Shutdown;
    function Presence: TArray<TPresenceInfo>;
    property OnChanged: TOnPresenceChanged read FOnChanged write FOnChanged;
  end;

implementation

constructor TFuelStationWatcher.Create(ABroker: TAMQPServer);
begin
  inherited Create;
  FBroker := ABroker;
  FLock := TCriticalSection.Create;
  FMap := TDictionary<string, TPresenceInfo>.Create;
  FConnUser := TDictionary<UInt64, string>.Create;
end;

destructor TFuelStationWatcher.Destroy;
begin
  Shutdown;
  FConnUser.Free;
  FMap.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TFuelStationWatcher.Startup;
begin
  if FActive then
    Exit;
  FBroker.Subscribe(OnBrokerEvent,
    [seConnectionAuthenticated, seConnectionClosed]);
  FActive := True;
end;

procedure TFuelStationWatcher.Shutdown;
begin
  if not FActive then
    Exit;
  FBroker.Unsubscribe(OnBrokerEvent);
  FActive := False;
end;

procedure TFuelStationWatcher.MarkConnected(const AUser, AAddr: string; AConn: UInt64;
  AConnected: Boolean; AWallMs: Int64);
var
  LInfo: TPresenceInfo;
begin
  if (AUser = '') or (Copy(AUser, 1, Length(POS_USER_PREFIX)) <> POS_USER_PREFIX) then
    Exit;   // POS terminals only; the 'automacao' connection is not in the table

  FLock.Enter;
  try
    if not FMap.TryGetValue(AUser, LInfo) then
    begin
      LInfo := Default(TPresenceInfo);
      LInfo.LoginUser := AUser;
    end;
    LInfo.Connected := AConnected;
    LInfo.SinceMs := AWallMs;
    if AAddr <> '' then
      LInfo.RemoteAddr := AAddr;
    FMap.AddOrSetValue(AUser, LInfo);
    if AConnected then
      FConnUser.AddOrSetValue(AConn, AUser)
    else
      FConnUser.Remove(AConn);
  finally
    FLock.Leave;
  end;

  if Assigned(FOnChanged) then
    FOnChanged();
end;

procedure TFuelStationWatcher.OnBrokerEvent(const AEvent: TAMQPServerEvent);
var
  LUser: string;
begin
  LUser := AEvent.Username;
  case AEvent.EventType of
    seConnectionAuthenticated:
      MarkConnected(LUser, AEvent.RemoteAddr, AEvent.ConnectionId, True,
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
        MarkConnected(LUser, AEvent.RemoteAddr, AEvent.ConnectionId, False,
          AEvent.WallMs);
      end;
  end;
end;

function TFuelStationWatcher.Presence: TArray<TPresenceInfo>;
var
  LInfo: TPresenceInfo;
  L: TList<TPresenceInfo>;
begin
  L := TList<TPresenceInfo>.Create;
  FLock.Enter;
  try
    for LInfo in FMap.Values do
      L.Add(LInfo);
    Result := L.ToArray;
  finally
    FLock.Leave;
    L.Free;
  end;
end;

end.
