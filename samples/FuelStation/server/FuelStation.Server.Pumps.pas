unit FuelStation.Server.Pumps;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Pump simulator: a named thread that at each random interval
  makes a completed fueling and hands it over through the callback
  (ARCHITECTURE.md §1, "here: a timer-driven simulator").

  The callback runs on the pump thread. The server handler (RegisterFueling +
  Events.NewFueling + panel refresh) must be thread-safe -- and it is (Registry and
  Events have their own lock; the panel marshals). }

interface

uses
  SysUtils, Classes, SyncObjs,
  PascalCommon.Threading,
  AMQP.Threading,     // AmqpWallMs
  FuelStation.Fueling;

type
  TOnFueling = procedure(const A: TFueling) of object;

  TFuelStationPumps = class(TThread)
  private
    FOnFueling: TOnFueling;
    FStop: TEvent;
    FPaused: Integer;   // atomic: 1 = does not generate (stays in the loop)
    FPumpCount: Integer;
    FNozzlesPerPump: Integer;
    FIntervalMinMs: Integer;
    FIntervalMaxMs: Integer;
    FTotalizers: array of Double;   // accumulated per nozzle ((pump-1)*nozzles + (nozzle-1))
    function TotalizerRef(APump, ANozzle: Integer): Integer;
    procedure Generate;
  protected
    procedure Execute; override;
  public
    constructor Create(AOnFueling: TOnFueling);
    destructor Destroy; override;
    { Signals the stop; the thread leaves on the next loop. }
    procedure Shutdown;
    { Turns generation on/off without stopping the thread (the panel uses this). }
    procedure SetPaused(AValue: Boolean);
    function Paused: Boolean;

    property PumpCount: Integer read FPumpCount write FPumpCount;
    property NozzlesPerPump: Integer read FNozzlesPerPump write FNozzlesPerPump;
    property IntervalMinMs: Integer read FIntervalMinMs write FIntervalMinMs;
    property IntervalMaxMs: Integer read FIntervalMaxMs write FIntervalMaxMs;
  end;

implementation

const
  PRODUCTS: array[0..4] of string = ('GC', 'GA', 'ET', 'S10', 'DS');
  PRICES:   array[0..4] of Double = (5.899, 7.490, 4.290, 6.190, 6.090);

constructor TFuelStationPumps.Create(AOnFueling: TOnFueling);
begin
  inherited Create(True);   // suspended; the caller sets the props and calls Start
  FOnFueling := AOnFueling;
  FStop := TEvent.Create(nil, True, False, '');
  FPumpCount := 4;
  FNozzlesPerPump := 2;
  FIntervalMinMs := 4000;
  FIntervalMaxMs := 12000;
end;

destructor TFuelStationPumps.Destroy;
begin
  Shutdown;
  inherited Destroy;   // Terminate + WaitFor
  FStop.Free;
end;

procedure TFuelStationPumps.Shutdown;
begin
  Terminate;
  FStop.SetEvent;
end;

procedure TFuelStationPumps.SetPaused(AValue: Boolean);
begin
  if AValue then
    PcAtomicSet(FPaused, 1)
  else
    PcAtomicSet(FPaused, 0);
end;

function TFuelStationPumps.Paused: Boolean;
begin
  Result := PcAtomicGet(FPaused) <> 0;
end;

function TFuelStationPumps.TotalizerRef(APump, ANozzle: Integer): Integer;
begin
  Result := (APump - 1) * FNozzlesPerPump + (ANozzle - 1);
end;

procedure TFuelStationPumps.Generate;
var
  LPump, LNozzle, LProd, LRef: Integer;
  LA: TFueling;
begin
  if Length(FTotalizers) <> FPumpCount * FNozzlesPerPump then
  begin
    SetLength(FTotalizers, FPumpCount * FNozzlesPerPump);
    for LRef := 0 to High(FTotalizers) do
      FTotalizers[LRef] := 100000 + Random(900000) + Random(1000) / 1000;
  end;

  LPump := 1 + Random(FPumpCount);
  LNozzle := 1 + Random(FNozzlesPerPump);
  LProd := Random(Length(PRODUCTS));
  LRef := TotalizerRef(LPump, LNozzle);

  LA := Default(TFueling);
  LA.Pump := LPump;
  LA.Nozzle := LNozzle;
  LA.Product := PRODUCTS[LProd];
  LA.PricePerLiter := PRICES[LProd];
  LA.Liters := (30 + Random(6500) / 100);   // 30,00 a 95,00 L
  LA.TotalAmount := Round(LA.Liters * LA.PricePerLiter * 100) / 100;
  LA.StartTotalizer := FTotalizers[LRef];
  FTotalizers[LRef] := FTotalizers[LRef] + LA.Liters;
  LA.EndTotalizer := FTotalizers[LRef];
  LA.TimestampMs := AmqpWallMs;
  if Random(20) = 0 then
    LA.Kind := ftCalibration
  else
    LA.Kind := ftSale;
  LA.Id := BuildFuelingId(LA.Pump, LA.Nozzle, LA.EndTotalizer);

  if Assigned(FOnFueling) then
    FOnFueling(LA);
end;

procedure TFuelStationPumps.Execute;
var
  LWait: Integer;
begin
  {$IFNDEF FPC}
  TThread.NameThreadForDebugging('StationPumps');
  {$ENDIF}
  Randomize;
  while not Terminated do
  begin
    LWait := FIntervalMinMs +
      Random(1 + FIntervalMaxMs - FIntervalMinMs);
    if FStop.WaitFor(LWait) = wrSignaled then
      Break;
    if Terminated then
      Break;
    if Paused then
      Continue;
    try
      Generate;
    except
      // an error in the callback must not kill the simulator
    end;
  end;
end;

end.
