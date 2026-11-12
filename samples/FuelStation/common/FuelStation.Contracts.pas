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
  EVENTS_EXCHANGE = 'posto.eventos';
  COMMANDS_QUEUE    = 'posto.comandos';
  EVENTS_BINDING     = 'abastecida.#';

  { --- AMQP users --- }
  AUTOMATION_USER   = 'automacao';
  POS_USER_PREFIX = 'pdv-';      // pdv-01, pdv-02, ...

  { --- events (routing key on posto.eventos) --- }
  EVT_NEW       = 'abastecida.nova';
  EVT_POSTING   = 'abastecida.lancando';
  EVT_POSTED    = 'abastecida.lancado';
  EVT_AVAILABLE = 'abastecida.disponivel';
  EVT_DISCARDED = 'abastecida.descartada';

  { --- commands ('type' property and "cmd" field) --- }
  CMD_SNAPSHOT            = 'snapshot';
  CMD_POST             = 'lancar';
  CMD_UNPOST           = 'estornar';
  CMD_FINISH          = 'finalizar';
  CMD_CANCEL_FINISHED = 'cancelar_finalizada';
  CMD_FORCE_RELEASE    = 'liberar_forcado';
  CMD_DISCARD          = 'descartar';

  { the reasons for abastecida.disponivel (MOT_*) and for refusal (REC_*) live in
    FuelStation.Fueling; they are outcomes of the state machine. }

type
  { Result for one id inside finalizar / cancelar_finalizada. }
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

{ Object node for the snapshot's "bloqueadas" list. }
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
  Result.PutInt('bomba', A.Pump);
  Result.PutInt('bico', A.Nozzle);
  Result.Put('produto', A.Product);
  Result.Put('litros', A.Liters);
  Result.Put('valorLitro', A.PricePerLiter);
  Result.Put('valorTotal', A.TotalAmount);
  Result.Put('encerranteInicio', A.StartTotalizer);
  Result.Put('encerranteFim', A.EndTotalizer);
  Result.PutInt('dataHoraMs', A.TimestampMs);
  Result.Put('tipo', FuelingTypeToStr(A.Kind));
end;

procedure JsonToFueling(J: TJsonValue; out A: TFueling);
begin
  A := Default(TFueling);
  if J = nil then
    Exit;
  A.Id := J.AsStr('id');
  A.Pump := J.AsInt('bomba');
  A.Nozzle := J.AsInt('bico');
  A.Product := J.AsStr('produto');
  A.Liters := J.AsNum('litros');
  A.PricePerLiter := J.AsNum('valorLitro');
  A.TotalAmount := J.AsNum('valorTotal');
  A.StartTotalizer := J.AsNum('encerranteInicio');
  A.EndTotalizer := J.AsNum('encerranteFim');
  A.TimestampMs := J.AsInt('dataHoraMs');
  A.Kind := StrToFuelingType(J.AsStr('tipo'));
end;

function StateToJson(const AE: TTrackedFueling): TJsonValue;
begin
  Result := TJsonValue.NewObject;
  Result.Put('id', AE.Data.Id);
  Result.Put('estado', StateToStr(AE.State));
  Result.Put('pdv', AE.PosName);
  Result.Put('venda', AE.Sale);
  Result.PutInt('desde', AE.SinceMs);
end;

{ ------------------------------------------------------- events --- }

function EncEventNew(const A: TFueling; AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.PutRaw('abastecida', FuelingToJson(A));
  J.PutInt('versao', AVersion);
  Result := Serialize(J);
end;

function EncEventIdPosSale(const AId, APosName, ASale: string;
  AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.Put('pdv', APosName);
  J.Put('venda', ASale);
  J.PutInt('versao', AVersion);
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
  J.Put('motivo', AReason);
  J.PutInt('versao', AVersion);
  Result := Serialize(J);
end;

function EncEventDiscarded(const AId: string; AVersion: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.PutInt('versao', AVersion);
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
  J.PutInt('versao', AVersion);
  LArr := J.PutArray('disponiveis');
  for I := 0 to High(AAvailable) do
    LArr.AddItem(FuelingToJson(AAvailable[I]));
  LArr := J.PutArray('bloqueadas');
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
  J.PutInt('versao', AVersion);
  Result := Serialize(J);
end;

function EncRespRefusal(const AReason, ACurrentPos, ACurrentSale: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', False);
  J.Put('motivo', AReason);
  if ACurrentPos <> '' then
    J.Put('pdvAtual', ACurrentPos);
  if ACurrentSale <> '' then
    J.Put('vendaAtual', ACurrentSale);
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
  J.PutInt('versao', AVersion);
  LArr := J.PutArray('itens');
  for I := 0 to High(AItems) do
  begin
    LItem := LArr.AddItemObject;
    LItem.Put('id', AItems[I].Id);
    LItem.Put('ok', AItems[I].Ok);
    if AItems[I].Reason <> '' then
      LItem.Put('motivo', AItems[I].Reason);
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
  J.PutInt('versao', AVersion);
  J.Put('estadoAnterior', APreviousState);
  J.Put('pdvAnterior', APreviousPos);
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
  J.Put('pdv', APosName);
  J.Put('venda', ASale);
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
  J.Put('pdv', APosName);
  J.Put('venda', ASale);
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
  J.Put('pdv', APosName);
  J.Put('venda', ASale);
  J.Put('operador', AOperator);
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
  J.Put('operador', AOperator);
  J.Put('motivo', AReason);
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
