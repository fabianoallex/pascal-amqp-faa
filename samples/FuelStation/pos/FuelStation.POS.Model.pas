unit FuelStation.POS.Model;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The sale in progress on the POS + the outbox + the reconciliation
  (ARCHITECTURE.md §8, §9).

  Each operation (add item, remove, finalize, cancel, reconcile)
  runs on a PcPool worker -- the UI never blocks on an RPC. The result
  comes back to the UI through OnChanged/OnLog (the UI marks itself dirty and
  reads the state).

  Online, adding an item is synchronous (immediate feedback). Offline / timeout,
  it is optimistic: the item goes in marked "pending" and the command goes to
  the outbox, replayed on reconnection.

  v1: the outbox is an in-memory list. In a real application it lives in the
  POS's local database (the automation runs in contingency). See ARCHITECTURE.md §9. }

interface

uses
  SysUtils, Classes, SyncObjs, Generics.Collections,
  AMQP.Wire, PascalCommon.ThreadPool,
  FuelStation.Json, FuelStation.Fueling, FuelStation.Contracts,
  FuelStation.POS.Client, FuelStation.POS.Sync;

type
  TSaleItem = record
    Id: string;
    Fueling: TFueling;
    Confirmed: Boolean;   // the server accepted the post
    Conflict: Boolean;     // the server refused (in use at another register)
    Obs: string;
  end;

  TOnModelLog = procedure(const AMsg: string) of object;
  TOnModelChanged = procedure of object;

  TFuelStationModel = class
  private
    FClient: TFuelStationPosClient;
    FSync: TFuelStationSync;
    FPosName: string;
    FTimeoutMs: Integer;
    FLock: TCriticalSection;
    FSale: string;
    FItems: TList<TSaleItem>;
    FOutbox: TStringList;   // pending JSON requests (one per line)
    FSeq: Integer;
    FOnLog: TOnModelLog;
    FOnChanged: TOnModelChanged;
    procedure Log(const AMsg: string);
    procedure Notify;
    function IndexOfId(const AId: string): Integer;
    procedure PutItem(const AItem: TSaleItem);
    procedure MarkItem(const AId: string; AConfirmed, AConflict: Boolean;
      const ANote: string);
    procedure EnqueueOutbox(const AReqJson: TBytes);
    function EnsureSale: string;
    function CallJson(const AReqJson: TBytes): TJsonValue; // nil = no reply
  public
    constructor Create(AClient: TFuelStationPosClient; ASync: TFuelStationSync;
      const APosName: string);
    destructor Destroy; override;

    procedure NewSale;
    procedure AddItem(const AId: string);
    procedure RemoveItem(const AId: string);
    procedure CancelSale;
    procedure FinishSale;
    procedure ReconcileAfterReconnect;

    // --- blocking bodies (called by the workers) ---
    procedure ExecAdd(const AId: string);
    procedure ExecRemove(const AId: string);
    procedure ExecCancel;
    procedure ExecFinish;
    procedure ExecReconcile;

    function SaleItems: TArray<TSaleItem>;
    function CurrentSale: string;
    function ItemsInOutbox: Integer;

    property OnLog: TOnModelLog read FOnLog write FOnLog;
    property OnChanged: TOnModelChanged read FOnChanged write FOnChanged;
  end;

implementation

type
  TModelOp = (moAdd, moRemove, moCancel, moFinish, moReconcile);

  TModelWork = class(TPcWorkItem)
  private
    FModel: TFuelStationModel;
    FOp: TModelOp;
    FArg: string;
  public
    constructor Create(AModel: TFuelStationModel; AOp: TModelOp; const AArg: string);
    procedure Execute; override;
  end;

constructor TModelWork.Create(AModel: TFuelStationModel; AOp: TModelOp;
  const AArg: string);
begin
  inherited Create;
  FModel := AModel;
  FOp := AOp;
  FArg := AArg;
end;

procedure TModelWork.Execute;
begin
  case FOp of
    moAdd:   FModel.ExecAdd(FArg);
    moRemove:     FModel.ExecRemove(FArg);
    moCancel:    FModel.ExecCancel;
    moFinish:   FModel.ExecFinish;
    moReconcile: FModel.ExecReconcile;
  end;
end;

{ TFuelStationModel }

constructor TFuelStationModel.Create(AClient: TFuelStationPosClient;
  ASync: TFuelStationSync; const APosName: string);
begin
  inherited Create;
  FClient := AClient;
  FSync := ASync;
  FPosName := APosName;
  FTimeoutMs := 3000;
  FLock := TCriticalSection.Create;
  FItems := TList<TSaleItem>.Create;
  FOutbox := TStringList.Create;
end;

destructor TFuelStationModel.Destroy;
begin
  FOutbox.Free;
  FItems.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TFuelStationModel.Log(const AMsg: string);
begin
  if Assigned(FOnLog) then
    FOnLog(AMsg);
end;

procedure TFuelStationModel.Notify;
begin
  if Assigned(FOnChanged) then
    FOnChanged();
end;

function TFuelStationModel.IndexOfId(const AId: string): Integer;
var
  I: Integer;
begin
  for I := 0 to FItems.Count - 1 do
    if FItems[I].Id = AId then
      Exit(I);
  Result := -1;
end;

procedure TFuelStationModel.PutItem(const AItem: TSaleItem);
var
  LIdx: Integer;
begin
  FLock.Enter;
  try
    LIdx := IndexOfId(AItem.Id);
    if LIdx >= 0 then
      FItems[LIdx] := AItem
    else
      FItems.Add(AItem);
  finally
    FLock.Leave;
  end;
end;

procedure TFuelStationModel.MarkItem(const AId: string; AConfirmed, AConflict: Boolean;
  const ANote: string);
var
  LIdx: Integer;
  LItem: TSaleItem;
begin
  FLock.Enter;
  try
    LIdx := IndexOfId(AId);
    if LIdx < 0 then
      Exit;
    LItem := FItems[LIdx];
    LItem.Confirmed := AConfirmed;
    LItem.Conflict := AConflict;
    LItem.Obs := ANote;
    FItems[LIdx] := LItem;
  finally
    FLock.Leave;
  end;
end;

procedure TFuelStationModel.EnqueueOutbox(const AReqJson: TBytes);
begin
  FLock.Enter;
  try
    FOutbox.Add(BytesToText(AReqJson));
  finally
    FLock.Leave;
  end;
end;

function TFuelStationModel.EnsureSale: string;
begin
  FLock.Enter;
  try
    if FSale = '' then
    begin
      Inc(FSeq);
      FSale := Format('S-%s-%.4d', [FormatDateTime('yyyymmdd-hhnnss', Now), FSeq]);
    end;
    Result := FSale;
  finally
    FLock.Leave;
  end;
end;

function TFuelStationModel.CallJson(const AReqJson: TBytes): TJsonValue;
var
  LResp: TRpcReply;
begin
  LResp := FClient.Call(AReqJson, FTimeoutMs);
  if LResp.Arrived then
    Result := LResp.Json
  else
  begin
    if LResp.Json <> nil then
      LResp.Json.Free;
    Result := nil;
  end;
end;

{ --- triggers (UI thread) --- }

procedure TFuelStationModel.NewSale;
begin
  FLock.Enter;
  try
    FSale := '';
    FItems.Clear;
  finally
    FLock.Leave;
  end;
  Log('New sale.');
  Notify;
end;

procedure TFuelStationModel.AddItem(const AId: string);
var
  LItem: TSaleItem;
  LE: TTrackedFueling;
begin
  if IndexOfId(AId) >= 0 then
    Exit;
  EnsureSale;
  LItem := Default(TSaleItem);
  LItem.Id := AId;
  if FSync.TryState(AId, LE) then
    LItem.Fueling := LE.Data;
  LItem.Confirmed := False;
  LItem.Obs := 'sending...';
  PutItem(LItem);
  Notify;
  PcPool.Queue(TModelWork.Create(Self, moAdd, AId));
end;

procedure TFuelStationModel.RemoveItem(const AId: string);
var
  LIdx: Integer;
  LSent: Boolean;
begin
  FLock.Enter;
  try
    LIdx := IndexOfId(AId);
    if LIdx < 0 then
      Exit;
    LSent := FItems[LIdx].Confirmed or (FItems[LIdx].Obs = 'sending...');
    FItems.Delete(LIdx);
  finally
    FLock.Leave;
  end;
  Notify;
  if LSent then
    PcPool.Queue(TModelWork.Create(Self, moRemove, AId));
end;

procedure TFuelStationModel.CancelSale;
begin
  PcPool.Queue(TModelWork.Create(Self, moCancel, ''));
end;

procedure TFuelStationModel.FinishSale;
begin
  PcPool.Queue(TModelWork.Create(Self, moFinish, ''));
end;

procedure TFuelStationModel.ReconcileAfterReconnect;
begin
  PcPool.Queue(TModelWork.Create(Self, moReconcile, ''));
end;

{ --- blocking bodies (pool worker) --- }

procedure TFuelStationModel.ExecAdd(const AId: string);
var
  LSale: string;
  LReq: TBytes;
  J: TJsonValue;
begin
  LSale := CurrentSale;
  LReq := EncReqPost(AId, FPosName, LSale);
  J := CallJson(LReq);
  if J = nil then
  begin
    MarkItem(AId, False, False, 'pending (no connection)');
    EnqueueOutbox(LReq);
    Log(Format('post %s: no reply -> outbox', [AId]));
  end
  else
    try
      if J.AsBool('ok') then
      begin
        MarkItem(AId, True, False, '');
        Log(Format('post %s: OK', [AId]));
      end
      else
      begin
        MarkItem(AId, False, True,
          Format('in use at POS %s', [J.AsStr('currentPos', '?')]));
        Log(Format('post %s REFUSED: %s (POS %s)',
          [AId, J.AsStr('reason'), J.AsStr('currentPos')]));
      end;
    finally
      J.Free;
    end;
  Notify;
end;

procedure TFuelStationModel.ExecRemove(const AId: string);
var
  LReq: TBytes;
  J: TJsonValue;
begin
  LReq := EncReqUnpost(AId, FPosName, CurrentSale);
  J := CallJson(LReq);
  if J = nil then
  begin
    EnqueueOutbox(LReq);
    Log(Format('unpost %s: no reply -> outbox', [AId]));
  end
  else
  begin
    J.Free;
    Log(Format('unpost %s: OK', [AId]));
  end;
  Notify;
end;

procedure TFuelStationModel.ExecCancel;
var
  LIds: TArray<string>;
  LSale: string;
  I: Integer;
  LReq: TBytes;
  J: TJsonValue;
begin
  FLock.Enter;
  try
    LSale := FSale;
    SetLength(LIds, FItems.Count);
    for I := 0 to FItems.Count - 1 do
      LIds[I] := FItems[I].Id;
    FItems.Clear;
    FSale := '';
  finally
    FLock.Leave;
  end;
  Notify;
  if LSale = '' then
    Exit;

  for I := 0 to High(LIds) do
  begin
    LReq := EncReqUnpost(LIds[I], FPosName, LSale);
    // CANCEL_SALE reason in the body
    J := TJsonValue.Parse(BytesToText(LReq));
    try
      J.Put('reason', RSN_CANCEL_SALE);
      LReq := JsonToBytes(J);
    finally
      J.Free;
    end;
    J := CallJson(LReq);
    if J = nil then
      EnqueueOutbox(LReq)
    else
      J.Free;
  end;
  Log(Format('Sale %s cancelled (%d items).', [LSale, Length(LIds)]));
  Notify;
end;

procedure TFuelStationModel.ExecFinish;
var
  LIds: TArray<string>;
  LSale: string;
  I: Integer;
  LReq: TBytes;
  J, LItems, LIt: TJsonValue;
  LAllOk: Boolean;
begin
  FLock.Enter;
  try
    LSale := FSale;
    SetLength(LIds, FItems.Count);
    for I := 0 to FItems.Count - 1 do
      LIds[I] := FItems[I].Id;
  finally
    FLock.Leave;
  end;
  if (LSale = '') or (Length(LIds) = 0) then
  begin
    Log('Finish: empty sale.');
    Exit;
  end;

  LReq := EncReqFinish(FPosName, LSale, LIds);
  J := CallJson(LReq);
  if J = nil then
  begin
    EnqueueOutbox(LReq);
    Log(Format('finish %s: no reply -> outbox ' +
      '(the sale stays open until reconciliation)', [LSale]));
    Notify;
    Exit;
  end;

  try
    LAllOk := True;
    LItems := J.Get('items');
    if (LItems <> nil) and (LItems.Kind = jkArray) then
      for I := 0 to LItems.Count - 1 do
      begin
        LIt := LItems.Item(I);
        if not LIt.AsBool('ok') then
        begin
          LAllOk := False;
          MarkItem(LIt.AsStr('id'), False, True,
            'conflict on finish: ' + LIt.AsStr('reason'));
          Log(Format('finish item %s: CONFLICT (%s)',
            [LIt.AsStr('id'), LIt.AsStr('reason')]));
        end;
      end;
  finally
    J.Free;
  end;

  if LAllOk then
  begin
    FLock.Enter;
    try
      if FSale = LSale then
      begin
        FSale := '';
        FItems.Clear;
      end;
    finally
      FLock.Leave;
    end;
    Log(Format('Sale %s finished.', [LSale]));
  end
  else
    Log(Format('Sale %s finished with conflicts -- review with the supervisor.',
      [LSale]));
  Notify;
end;

procedure TFuelStationModel.ExecReconcile;
var
  LQueue: TStringList;
  I, LResent: Integer;
  J: TJsonValue;
  LOpenItems: TArray<string>;
  LSale: string;
  LE: TTrackedFueling;
begin
  // 1) drain the outbox, in order
  LQueue := TStringList.Create;
  try
    FLock.Enter;
    try
      LQueue.Assign(FOutbox);
      FOutbox.Clear;
    finally
      FLock.Leave;
    end;

    LResent := 0;
    I := 0;
    while I < LQueue.Count do
    begin
      J := CallJson(AmqpUtf8Encode(LQueue[I]));
      if J = nil then
      begin
        // still no server: put the rest back in the outbox and leave
        FLock.Enter;
        try
          while I < LQueue.Count do
          begin
            FOutbox.Add(LQueue[I]);
            Inc(I);
          end;
        finally
          FLock.Leave;
        end;
        Log('Reconciliation: server unavailable, outbox kept.');
        Notify;
        Exit;
      end;
      J.Free;
      Inc(LResent);
      Inc(I);
    end;
    if LResent > 0 then
      Log(Format('Reconciliation: %d outbox commands resent.', [LResent]));
  finally
    LQueue.Free;
  end;

  // 2) check the open sale's items against the already re-synced mirror
  FLock.Enter;
  try
    LSale := FSale;
    SetLength(LOpenItems, FItems.Count);
    for I := 0 to FItems.Count - 1 do
      LOpenItems[I] := FItems[I].Id;
  finally
    FLock.Leave;
  end;
  if LSale = '' then
  begin
    Notify;
    Exit;
  end;

  for I := 0 to High(LOpenItems) do
  begin
    if not FSync.TryState(LOpenItems[I], LE) then
      Continue;
    if (LE.State = fsPosting) and (LE.PosName = FPosName) then
      MarkItem(LOpenItems[I], True, False, '')
    else if LE.State = fsAvailable then
    begin
      // the reservation was lost during the outage -- try again
      J := CallJson(EncReqPost(LOpenItems[I], FPosName, LSale));
      if J <> nil then
      try
        if J.AsBool('ok') then
          MarkItem(LOpenItems[I], True, False, 're-reserved')
        else
          MarkItem(LOpenItems[I], False, True,
            'lost in the outage (POS ' + J.AsStr('currentPos', '?') + ')');
      finally
        J.Free;
      end;
    end
    else
      MarkItem(LOpenItems[I], False, True,
        Format('now %s at POS %s',
        [StateToStr(LE.State), LE.PosName]));
  end;
  Log('Sale reconciliation done.');
  Notify;
end;

{ --- reading (UI thread) --- }

function TFuelStationModel.SaleItems: TArray<TSaleItem>;
begin
  FLock.Enter;
  try
    Result := FItems.ToArray;
  finally
    FLock.Leave;
  end;
end;

function TFuelStationModel.CurrentSale: string;
begin
  FLock.Enter;
  try
    Result := FSale;
  finally
    FLock.Leave;
  end;
end;

function TFuelStationModel.ItemsInOutbox: Integer;
begin
  FLock.Enter;
  try
    Result := FOutbox.Count;
  finally
    FLock.Leave;
  end;
end;

end.
