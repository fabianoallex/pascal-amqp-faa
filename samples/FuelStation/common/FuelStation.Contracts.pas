unit FuelStation.Contracts;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The message contract between the automation server and the POS terminals
  (ARCHITECTURE.md §5 and §6): topology names, command/event names and the
  JSON (de)serialization of each payload. One source for both compilers.

  Every message body is UTF-8 JSON (content-type application/json).
  The RPC request carries the command name both in the AMQP 'type' property
  and in the "cmd" field of the body; the server dispatches on "cmd". }

interface

uses
  SysUtils,
  AMQP.Wire,          // AmqpUtf8Encode / AmqpUtf8Decode
  FuelStation.Json,
  FuelStation.Fueling;

const
  { --- topology --- }
  EVENTS_EXCHANGE = 'station.events';
  COMMANDS_QUEUE    = 'station.commands';
  EVENTS_BINDING     = 'fueling.#';

  { --- AMQP users --- }
  AUTOMATION_USER   = 'automation';
  POS_USER_PREFIX = 'pos-';      // pos-01, pos-02, ...

  { --- events (routing key on station.events) --- }
  EVT_NEW       = 'fueling.new';
  EVT_POSTING   = 'fueling.posting';
  EVT_POSTED    = 'fueling.posted';
  EVT_AVAILABLE = 'fueling.available';
  EVT_DISCARDED = 'fueling.discarded';

  { --- commands ('type' property and "cmd" field) --- }
  CMD_SNAPSHOT            = 'snapshot';
  CMD_POST             = 'post';
  CMD_UNPOST           = 'unpost';
  CMD_FINISH          = 'finish';
  CMD_CANCEL_FINISHED = 'cancel_finished';
  CMD_FORCE_RELEASE    = 'force_release';
  CMD_DISCARD          = 'discard';

  { the reasons for fueling.available (MOT_*) and for refusal (REC_*) live in
    FuelStation.Fueling; they are outcomes of the state machine. }

type
  { Result for one id inside finish / cancel_finished. }
  TItemResult = record
    Id: string;
    Ok: Boolean;
    Reason: string;
  end;

{ ------------------------------------------------------- bytes <-> json --- }

function JsonToBytes(J: TJsonValue): TBytes;
function BytesToJson(const ABytes: TBytes): TJsonValue;   // raises EFuelStationJson
function BytesToText(const ABytes: TBytes): string;

{ ------------------------------------------------------- fueling <-> json ---- }

{ New object node with the fueling's fields (the caller takes ownership
  or hands it to a Put*). }
function FuelingToJson(const A: TFueling): TJsonValue;
procedure JsonToFueling(J: TJsonValue; out A: TFueling);

{ Object node for the snapshot's "locked" list. }
function StateToJson(const AE: TTrackedFueling): TJsonValue;

{ ------------------------------------------------------- events (server) ---- }

function EncEventNew(const A: TFueling; AVersion: Int64): TBytes;
function EncEventPosting(const AId, APosName, ASale: string; AVersion: Int64): TBytes;
function EncEventPosted(const AId, APosName, ASale: string; AVersion: Int64): TBytes;
function EncEventAvailable(const AId, AReason: string; AVersion: Int64): TBytes;
function EncEventDiscarded(const AId: string; AVersion: Int64): TBytes;

{ ------------------------------------------------------- replies (server) --- }

function EncRespSnapshot(AVersion: Int64;
  const AAvailable: array of TFueling;
  const ALocked: array of TTrackedFueling): TBytes;
function EncRespOk(AVersion: Int64): TBytes;
function EncRespRefusal(const AReason, ACurrentPos, ACurrentSale: string): TBytes;
function EncRespItems(AVersion: Int64; const AItems: array of TItemResult): TBytes;
function EncRespForceRelease(AVersion: Int64;
  const APreviousState, APreviousPos: string): TBytes;

{ ------------------------------------------------------- requests (POS) ----- }

function EncReqSnapshot: TBytes;
function EncReqPost(const AId, APosName, ASale: string): TBytes;
function EncReqUnpost(const AId, APosName, ASale: string): TBytes;
function EncReqFinish(const APosName, ASale: string;
  const AIds: array of string): TBytes;
function EncReqCancelFinished(const APosName, ASale, AOperator: string;
  const AIds: array of string): TBytes;
function EncReqForceRelease(const AId, AOperator, AReason: string): TBytes;
function EncReqDiscard(const AId, AOperator, AReason: string): TBytes;

implementation

{ ------------------------------------------------------- bytes <-> json --- }

function JsonToBytes(J: TJsonValue): TBytes;
begin
  Result := AmqpUtf8Encode(J.ToJson);
end;

function BytesToJson(const ABytes: TBytes): TJsonValue;
begin
  Result := TJsonValue.Parse(AmqpUtf8Decode(ABytes));
end;

function BytesToText(const ABytes: TBytes): string;
begin
  Result := AmqpUtf8Decode(ABytes);
end;

{ a local helper: builds the body, serializes it, frees it. }
function Serialize(J: TJsonValue): TBytes;
begin
  try
    Result := AmqpUtf8Encode(J.ToJson);
  finally
    J.Free;
  end;
end;

{ ------------------------------------------------------- fueling <-> json ---- }

function FuelingToJson(const A: TFueling): TJsonValue;
begin
  Result := TJsonValue.NewObject;
  Result.Put('id', A.Id);
  Result.PutInt('pump', A.Pump);
  Result.PutInt('nozzle', A.Nozzle);
  Result.Put('product', A.Product);
  Result.Put('liters', A.Liters);
  Result.Put('pricePerLiter', A.PricePerLiter);
  Result.Put('totalAmount', A.TotalAmount);
  Result.Put('startTotalizer', A.StartTotalizer);
  Result.Put('endTotalizer', A.EndTotalizer);
  Result.PutInt('timestampMs', A.TimestampMs);
  Result.Put('kind', FuelingTypeToStr(A.Kind));
end;

procedure JsonToFueling(J: TJsonValue; out A: TFueling);
begin
  A := Default(TFueling);
  if J = nil then
    Exit;
  A.Id := J.AsStr('id');
  A.Pump := J.AsInt('pump');
  A.Nozzle := J.AsInt('nozzle');
  A.Product := J.AsStr('product');
  A.Liters := J.AsNum('liters');
  A.PricePerLiter := J.AsNum('pricePerLiter');
  A.TotalAmount := J.AsNum('totalAmount');
  A.StartTotalizer := J.AsNum('startTotalizer');
  A.EndTotalizer := J.AsNum('endTotalizer');
  A.TimestampMs := J.AsInt('timestampMs');
  A.Kind := StrToFuelingType(J.AsStr('kind'));
end;

function StateToJson(const AE: TTrackedFueling): TJsonValue;
begin
  Result := TJsonValue.NewObject;
  Result.Put('id', AE.Data.Id);
  Result.Put('state', StateToStr(AE.State));
  Result.Put('pos', AE.PosName);
  Result.Put('sale', AE.Sale);
  Result.PutInt('since', AE.SinceMs);
end;

{ ------------------------------------------------------- events --- }

function EncEventNew(const A: TFueling; AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.PutRaw('fueling', FuelingToJson(A));
  J.PutInt('version', AVersion);
  Result := Serialize(J);
end;

function EncEventIdPosSale(const AId, APosName, ASale: string;
  AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.Put('pos', APosName);
  J.Put('sale', ASale);
  J.PutInt('version', AVersion);
  Result := Serialize(J);
end;

function EncEventPosting(const AId, APosName, ASale: string;
  AVersion: Int64): TBytes;
begin
  Result := EncEventIdPosSale(AId, APosName, ASale, AVersion);
end;

function EncEventPosted(const AId, APosName, ASale: string;
  AVersion: Int64): TBytes;
begin
  Result := EncEventIdPosSale(AId, APosName, ASale, AVersion);
end;

function EncEventAvailable(const AId, AReason: string; AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.Put('reason', AReason);
  J.PutInt('version', AVersion);
  Result := Serialize(J);
end;

function EncEventDiscarded(const AId: string; AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.PutInt('version', AVersion);
  Result := Serialize(J);
end;

{ ------------------------------------------------------- replies --- }

function EncRespSnapshot(AVersion: Int64;
  const AAvailable: array of TFueling;
  const ALocked: array of TTrackedFueling): TBytes;
var
  J, LArr: TJsonValue;
  I: Integer;
begin
  J := TJsonValue.NewObject;
  J.PutInt('version', AVersion);
  LArr := J.PutArray('available');
  for I := 0 to High(AAvailable) do
    LArr.AddItem(FuelingToJson(AAvailable[I]));
  LArr := J.PutArray('locked');
  for I := 0 to High(ALocked) do
    LArr.AddItem(StateToJson(ALocked[I]));
  Result := Serialize(J);
end;

function EncRespOk(AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', True);
  J.PutInt('version', AVersion);
  Result := Serialize(J);
end;

function EncRespRefusal(const AReason, ACurrentPos, ACurrentSale: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', False);
  J.Put('reason', AReason);
  if ACurrentPos <> '' then
    J.Put('currentPos', ACurrentPos);
  if ACurrentSale <> '' then
    J.Put('currentSale', ACurrentSale);
  Result := Serialize(J);
end;

function EncRespItems(AVersion: Int64;
  const AItems: array of TItemResult): TBytes;
var
  J, LArr, LItem: TJsonValue;
  I: Integer;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', True);
  J.PutInt('version', AVersion);
  LArr := J.PutArray('items');
  for I := 0 to High(AItems) do
  begin
    LItem := LArr.AddItemObject;
    LItem.Put('id', AItems[I].Id);
    LItem.Put('ok', AItems[I].Ok);
    if AItems[I].Reason <> '' then
      LItem.Put('reason', AItems[I].Reason);
  end;
  Result := Serialize(J);
end;

function EncRespForceRelease(AVersion: Int64;
  const APreviousState, APreviousPos: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', True);
  J.PutInt('version', AVersion);
  J.Put('previousState', APreviousState);
  J.Put('previousPos', APreviousPos);
  Result := Serialize(J);
end;

{ ------------------------------------------------------- requests --- }

function EncReqSnapshot: TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', CMD_SNAPSHOT);
  Result := Serialize(J);
end;

function EncReqIdPosSale(const ACmd, AId, APosName, ASale: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', ACmd);
  J.Put('id', AId);
  J.Put('pos', APosName);
  J.Put('sale', ASale);
  Result := Serialize(J);
end;

function EncReqPost(const AId, APosName, ASale: string): TBytes;
begin
  Result := EncReqIdPosSale(CMD_POST, AId, APosName, ASale);
end;

function EncReqUnpost(const AId, APosName, ASale: string): TBytes;
begin
  Result := EncReqIdPosSale(CMD_UNPOST, AId, APosName, ASale);
end;

procedure PutIds(J: TJsonValue; const AIds: array of string);
var
  LArr: TJsonValue;
  I: Integer;
  LV: TJsonValue;
begin
  LArr := J.PutArray('ids');
  for I := 0 to High(AIds) do
  begin
    LV := TJsonValue.Create(jkString);
    LV.StrVal := AIds[I];
    LArr.AddItem(LV);
  end;
end;

function EncReqFinish(const APosName, ASale: string;
  const AIds: array of string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', CMD_FINISH);
  J.Put('pos', APosName);
  J.Put('sale', ASale);
  PutIds(J, AIds);
  Result := Serialize(J);
end;

function EncReqCancelFinished(const APosName, ASale, AOperator: string;
  const AIds: array of string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', CMD_CANCEL_FINISHED);
  J.Put('pos', APosName);
  J.Put('sale', ASale);
  J.Put('operator', AOperator);
  PutIds(J, AIds);
  Result := Serialize(J);
end;

function EncReqOperator(const ACmd, AId, AOperator, AReason: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', ACmd);
  J.Put('id', AId);
  J.Put('operator', AOperator);
  J.Put('reason', AReason);
  Result := Serialize(J);
end;

function EncReqForceRelease(const AId, AOperator, AReason: string): TBytes;
begin
  Result := EncReqOperator(CMD_FORCE_RELEASE, AId, AOperator, AReason);
end;

function EncReqDiscard(const AId, AOperator, AReason: string): TBytes;
begin
  Result := EncReqOperator(CMD_DISCARD, AId, AOperator, AReason);
end;

end.
