unit FuelStation.POS.Sync;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Local mirror of the fuelings on the POS (ARCHITECTURE.md §7).

  On start (and on every reconnection): subscribes to the events and BUFFERS
  them, asks for the snapshot, applies it as the baseline and reprocesses the
  buffer, dropping what already came in the picture. After that it applies them
  live, with version-gap detection -> re-snapshot (heals an event lost for any
  reason).

  The event type comes from the ROUTING KEY (abastecida.nova/lancando/lancado/
  disponivel/descartada), not from the body format.

  Everything that mutates FMap/FLastVersion goes through FLock. The event
  callbacks and the sync run on pool threads; the UI only reads (Pending,
  TryState) and reacts to OnChanged by marking itself dirty. }

interface

uses
  SysUtils, Classes, SyncObjs, Generics.Collections,
  PascalCommon.ThreadPool, AMQP.Connection,
  FuelStation.Json, FuelStation.Fueling, FuelStation.Contracts,
  FuelStation.POS.Client;

type
  TOnSyncChanged = procedure of object;

  TFuelStationSync = class
  private
    FClient: TFuelStationPosClient;
    FLock: TCriticalSection;
    FMap: TDictionary<string, TTrackedFueling>;
    FBuffer: TStringList;         // "route"#9"json" received during the snapshot
    FLastVersion: Int64;
    FBuffering: Boolean;
    FSynced: Boolean;
    FOnChanged: TOnSyncChanged;
    procedure OnAmqpEvent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure ApplyEvent(const ARoute, AText: string);
    procedure RequestResync;
    procedure Notify;
    procedure EnterBuffering;
  public
    constructor Create(AClient: TFuelStationPosClient);
    destructor Destroy; override;

    { Subscribes to the events and fires the 1st sync (on a pool worker). }
    procedure Startup;
    { Call on OnReconnect: re-buffer + re-snapshot. }
    procedure Resync;
    { Runs the sync NOW (blocking -- internal use through a worker). }
    procedure SyncNow;

    function Pending: TArray<TFueling>;
    function TryState(const AId: string; out AState: TTrackedFueling): Boolean;
    function Synced: Boolean;
    property OnChanged: TOnSyncChanged read FOnChanged write FOnChanged;
  end;

implementation

type
  { Pool worker to run the sync off the callback thread. }
  TSyncWork = class(TPcWorkItem)
  private
    FTarget: TFuelStationSync;
  public
    constructor Create(ATarget: TFuelStationSync);
    procedure Execute; override;
  end;

constructor TSyncWork.Create(ATarget: TFuelStationSync);
begin
  inherited Create;
  FTarget := ATarget;
end;

procedure TSyncWork.Execute;
begin
  FTarget.SyncNow;
end;

{ TFuelStationSync }

constructor TFuelStationSync.Create(AClient: TFuelStationPosClient);
begin
  inherited Create;
  FClient := AClient;
  FLock := TCriticalSection.Create;
  FMap := TDictionary<string, TTrackedFueling>.Create;
  FBuffer := TStringList.Create;
  FBuffering := True;
end;

destructor TFuelStationSync.Destroy;
begin
  FBuffer.Free;
  FMap.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TFuelStationSync.EnterBuffering;
begin
  FLock.Enter;
  try
    FBuffering := True;
    FSynced := False;
    FBuffer.Clear;
  finally
    FLock.Leave;
  end;
end;

procedure TFuelStationSync.Startup;
begin
  EnterBuffering;
  FClient.SubscribeEvents(OnAmqpEvent);
  PcPool.Queue(TSyncWork.Create(Self));
end;

procedure TFuelStationSync.Resync;
begin
  EnterBuffering;
  PcPool.Queue(TSyncWork.Create(Self));
end;

procedure TFuelStationSync.OnAmqpEvent(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LRoute, LText: string;
  LShouldBuffer: Boolean;
begin
  LRoute := ADelivery.RoutingKey;
  LText := ADelivery.BodyAsText;
  FLock.Enter;
  try
    LShouldBuffer := FBuffering;
    if LShouldBuffer then
      FBuffer.Add(LRoute + #9 + LText);
  finally
    FLock.Leave;
  end;
  if not LShouldBuffer then
    ApplyEvent(LRoute, LText);
end;

procedure TFuelStationSync.SyncNow;
var
  LResp: TRpcReply;
  LVersion: Int64;
  LArr, LItem, LFuelingJson: TJsonValue;
  I, LSep: Integer;
  LE: TTrackedFueling;
  LPending: TStringList;
  LLine: string;
begin
  LResp := FClient.Call(EncReqSnapshot, 5000);
  if not LResp.Arrived then
  begin
    if LResp.Json <> nil then
      LResp.Json.Free;
    Exit;   // no server right now; the reconnection fires it again
  end;

  LPending := TStringList.Create;
  try
    LVersion := LResp.Json.AsInt('versao');

    FLock.Enter;
    try
      FMap.Clear;

      LArr := LResp.Json.Get('disponiveis');
      if (LArr <> nil) and (LArr.Kind = jkArray) then
        for I := 0 to LArr.Count - 1 do
        begin
          LFuelingJson := LArr.Item(I);
          LE := Default(TTrackedFueling);
          JsonToFueling(LFuelingJson, LE.Data);
          LE.State := fsAvailable;
          FMap.AddOrSetValue(LE.Data.Id, LE);
        end;

      LArr := LResp.Json.Get('bloqueadas');
      if (LArr <> nil) and (LArr.Kind = jkArray) then
        for I := 0 to LArr.Count - 1 do
        begin
          LItem := LArr.Item(I);
          LE := Default(TTrackedFueling);
          LE.Data.Id := LItem.AsStr('id');
          LE.State := StrToState(LItem.AsStr('estado'));
          LE.PosName := LItem.AsStr('pdv');
          LE.Sale := LItem.AsStr('venda');
          LE.SinceMs := LItem.AsInt('desde');
          FMap.AddOrSetValue(LE.Data.Id, LE);
        end;

      FLastVersion := LVersion;
      LPending.Assign(FBuffer);
      FBuffer.Clear;
      FBuffering := False;
      FSynced := True;
    finally
      FLock.Leave;
    end;
  finally
    LResp.Json.Free;
  end;

  for I := 0 to LPending.Count - 1 do
  begin
    LLine := LPending[I];
    LSep := Pos(#9, LLine);
    if LSep > 0 then
      ApplyEvent(Copy(LLine, 1, LSep - 1), Copy(LLine, LSep + 1, MaxInt));
  end;
  LPending.Free;

  Notify;
end;

procedure TFuelStationSync.ApplyEvent(const ARoute, AText: string);
var
  J: TJsonValue;
  LVersion: Int64;
  LId, LPosName: string;
  LE: TTrackedFueling;
  LHasId, LGap: Boolean;
  LFuelingJson: TJsonValue;
begin
  J := nil;
  try
    J := TJsonValue.Parse(AText);
  except
    if J <> nil then J.Free;
    Exit;
  end;

  LGap := False;
  try
    LVersion := J.AsInt('versao');
    LId := J.AsStr('id');

    FLock.Enter;
    try
      if FBuffering then
      begin
        FBuffer.Add(ARoute + #9 + AText);
        Exit;
      end;
      if LVersion <= FLastVersion then
        Exit;   // ja' visto
      LGap := LVersion > FLastVersion + 1;
      FLastVersion := LVersion;

      LHasId := FMap.TryGetValue(LId, LE);

      if ARoute = EVT_NEW then
      begin
        LFuelingJson := J.Get('abastecida');
        if LFuelingJson <> nil then
        begin
          LE := Default(TTrackedFueling);
          JsonToFueling(LFuelingJson, LE.Data);
          LE.State := fsAvailable;
          FMap.AddOrSetValue(LE.Data.Id, LE);
        end;
      end
      else if ARoute = EVT_AVAILABLE then
      begin
        if LHasId then
        begin
          LE.State := fsAvailable;
          LE.PosName := '';
          LE.Sale := '';
          FMap.AddOrSetValue(LId, LE);
        end;
      end
      else if (ARoute = EVT_POSTING) or (ARoute = EVT_POSTED) then
      begin
        if not LHasId then
        begin
          LE := Default(TTrackedFueling);
          LE.Data.Id := LId;
        end;
        LPosName := J.AsStr('pdv');
        LE.PosName := LPosName;
        LE.Sale := J.AsStr('venda');
        if ARoute = EVT_POSTING then
          LE.State := fsPosting
        else
          LE.State := fsPosted;
        FMap.AddOrSetValue(LId, LE);
      end
      else if ARoute = EVT_DISCARDED then
      begin
        if LHasId then
          FMap.Remove(LId);
      end;
    finally
      FLock.Leave;
    end;
  finally
    J.Free;
  end;

  if LGap then
    RequestResync;
  Notify;
end;

procedure TFuelStationSync.RequestResync;
var
  LAlreadyBuffering: Boolean;
begin
  FLock.Enter;
  try
    LAlreadyBuffering := FBuffering;
  finally
    FLock.Leave;
  end;
  if not LAlreadyBuffering then
    Resync;
end;

procedure TFuelStationSync.Notify;
begin
  if Assigned(FOnChanged) then
    FOnChanged();
end;

function TFuelStationSync.Pending: TArray<TFueling>;
var
  LE: TTrackedFueling;
  L: TList<TFueling>;
begin
  L := TList<TFueling>.Create;
  FLock.Enter;
  try
    for LE in FMap.Values do
      if LE.State = fsAvailable then
        L.Add(LE.Data);
    Result := L.ToArray;
  finally
    FLock.Leave;
    L.Free;
  end;
end;

function TFuelStationSync.TryState(const AId: string;
  out AState: TTrackedFueling): Boolean;
begin
  FLock.Enter;
  try
    Result := FMap.TryGetValue(AId, AState);
  finally
    FLock.Leave;
  end;
end;

function TFuelStationSync.Synced: Boolean;
begin
  FLock.Enter;
  try
    Result := FSynced;
  finally
    FLock.Leave;
  end;
end;

end.
