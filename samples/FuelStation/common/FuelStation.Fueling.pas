unit FuelStation.Fueling;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The domain object: a fueling (a refuel completed at a pump)
  and its state on the server. One source for both compilers.

  The ID is stable and unique: pump + nozzle + final totalizer. It is the dedup
  key across the whole system (event, command, POS outbox). }

interface

uses
  SysUtils;

const
  { Refusal reasons in an RPC reply ("motivo" field, ok=false).
    They are outcomes of the state machine, so they live here, in the domain. }
  REF_ALREADY_POSTING = 'JA_LANCANDO';
  REF_ALREADY_POSTED  = 'JA_LANCADO';
  REF_NOT_FOUND  = 'NAO_EXISTE';
  REF_DISCARDED  = 'DESCARTADA';
  REF_NOT_OWNER  = 'NAO_E_DONO';
  REF_LOCKED   = 'BLOQUEADA';

  { Reasons for abastecida.disponivel. }
  RSN_ITEM_UNPOST     = 'ESTORNO_ITEM';
  RSN_CANCEL_SALE     = 'CANCEL_VENDA';
  RSN_CANCEL_INVOICE      = 'CANCEL_NOTA';
  RSN_MANUAL_RELEASE = 'LIBERACAO_MANUAL';

type
  { Life cycle on the server (ARCHITECTURE.md §4). Both fsPosting and
    fsPosted lock the fueling against the other POS terminals. }
  TFuelingState = (fsAvailable, fsPosting, fsPosted);

  { ftCalibration = pump test/calibration: never becomes a sale, the supervisor
    discards it from the panel. }
  TFuelingType = (ftSale, ftCalibration);

  TFueling = record
    Id: string;
    Pump: Integer;
    Nozzle: Integer;
    Product: string;            // 'GC','GA','ET','S10','DS'...
    Liters: Double;
    PricePerLiter: Double;
    TotalAmount: Double;
    StartTotalizer: Double;
    EndTotalizer: Double;
    TimestampMs: Int64;           // epoch ms UTC (AmqpWallMs)
    Kind: TFuelingType;
  end;

  { How the server keeps a fueling: the data + the state and whose it is. }
  TTrackedFueling = record
    Data: TFueling;
    State: TFuelingState;
    PosName: string;                 // owner, when locked ('' if available)
    Sale: string;               // owner's sale ('' if available)
    SinceMs: Int64;              // time of the last transition (epoch ms)
  end;

function StateToStr(AState: TFuelingState): string;
function StrToState(const AStr: string): TFuelingState;
function FuelingTypeToStr(AType: TFuelingType): string;
function StrToFuelingType(const AStr: string): TFuelingType;

{ Canonical ID from the pump fields. }
function BuildFuelingId(APump, ANozzle: Integer;
  const AEndTotalizer: Double): string;

implementation

function StateToStr(AState: TFuelingState): string;
begin
  case AState of
    fsAvailable: Result := 'disponivel';
    fsPosting:   Result := 'lancando';
    fsPosted:    Result := 'lancado';
  else
    Result := 'disponivel';
  end;
end;

function StrToState(const AStr: string): TFuelingState;
begin
  if AStr = 'lancando' then
    Result := fsPosting
  else if AStr = 'lancado' then
    Result := fsPosted
  else
    Result := fsAvailable;
end;

function FuelingTypeToStr(AType: TFuelingType): string;
begin
  if AType = ftCalibration then
    Result := 'AFERICAO'
  else
    Result := 'VENDA';
end;

function StrToFuelingType(const AStr: string): TFuelingType;
begin
  if SameText(AStr, 'AFERICAO') then
    Result := ftCalibration
  else
    Result := ftSale;
end;

function BuildFuelingId(APump, ANozzle: Integer;
  const AEndTotalizer: Double): string;
begin
  // E.g. "B03-BC2-0001486094": totalizer in hundredths of a liter, no dot.
  Result := Format('B%.2d-BC%d-%.10d',
    [APump, ANozzle, Round(AEndTotalizer * 100)]);
end;

end.
