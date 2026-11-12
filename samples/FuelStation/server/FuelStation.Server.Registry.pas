unit FuelStation.Server.Registry;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The authoritative state of the fuelings on the automation server
  (ARCHITECTURE.md §2 and §4). The whole state machine lives here; no
  other part mutates a fueling.

  Concurrency: a single TCriticalSection protects the dictionary and the version.
  The scale is tens of pumps and a handful of POS terminals, and the command
  consumer is serial (prefetch 1) -- a global lock is enough and simple.
  The "serialization per id" of the ARCHITECTURE is a consequence of that, not one
  more mechanism.

  v1: memory only. Restarting the server loses what was pending
  (ARCHITECTURE.md §12). }

interface

uses
  SysUtils, SyncObjs, Generics.Collections,
  AMQP.Threading,      // AmqpWallMs
  FuelStation.Fueling;

type
  { Outcome of a transition. Version is always the current one after the operation.
    Changed = the state really changed (the caller publishes an event iff Ok and
    Changed). }
  TTransitionResult = record
    Ok: Boolean;
    Changed: Boolean;
    Reason: string;             // when not Ok
    CurrentPos: string;           // refusal because already locked
    CurrentSale: string;
    Version: Int64;
    PreviousState: TFuelingState;   // liberar_forcado
    PreviousPos: string;
  end;

  TFuelStationRegistry = class
  private
    FLock: TCriticalSection;
    FItems: TDictionary<string, TTrackedFueling>;
    FVersion: Int64;
    function Refuse(const AReason: string): TTransitionResult;
    function OkChanged(AChanged: Boolean): TTransitionResult;
  public
    constructor Create;
    destructor Destroy; override;

    { The pump completed a refuel. True = new fueling (publish
      EVT_NEW); False = id already known (ignored, idempotent). }
    function RegisterFueling(const A: TFueling; out AVersion: Int64): Boolean;

    function Post(const AId, APosName, ASale: string): TTransitionResult;
    function Unpost(const AId, APosName, ASale: string): TTransitionResult;
    function Finish(const AId, APosName, ASale: string): TTransitionResult;
    function CancelFinished(const AId, APosName,
      ASale: string): TTransitionResult;
    function ForceRelease(const AId: string): TTransitionResult;
    function Discard(const AId: string): TTransitionResult;

    function Snapshot(out AAvailable: TArray<TFueling>;
      out ALocked: TArray<TTrackedFueling>): Int64;
    { Everything, for the panel (includes available and locked ones). }
    function All: TArray<TTrackedFueling>;
    function Version: Int64;
  end;

implementation

constructor TFuelStationRegistry.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FItems := TDictionary<string, TTrackedFueling>.Create;
end;

destructor TFuelStationRegistry.Destroy;
begin
  FItems.Free;
  FLock.Free;
  inherited Destroy;
end;

function TFuelStationRegistry.Refuse(const AReason: string): TTransitionResult;
begin
  Result := Default(TTransitionResult);
  Result.Ok := False;
  Result.Changed := False;
  Result.Reason := AReason;
  Result.Version := FVersion;
end;

function TFuelStationRegistry.OkChanged(AChanged: Boolean): TTransitionResult;
begin
  Result := Default(TTransitionResult);
  Result.Ok := True;
  Result.Changed := AChanged;
  if AChanged then
    Inc(FVersion);
  Result.Version := FVersion;
end;

function TFuelStationRegistry.RegisterFueling(const A: TFueling;
  out AVersion: Int64): Boolean;
var
  LE: TTrackedFueling;
begin
  FLock.Enter;
  try
    if FItems.ContainsKey(A.Id) then
    begin
      AVersion := FVersion;
      Exit(False);
    end;
    LE := Default(TTrackedFueling);
    LE.Data := A;
    LE.State := fsAvailable;
    LE.SinceMs := AmqpWallMs;
    FItems.Add(A.Id, LE);
    Inc(FVersion);
    AVersion := FVersion;
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TFuelStationRegistry.Post(const AId, APosName,
  ASale: string): TTransitionResult;
var
  LE: TTrackedFueling;
begin
  FLock.Enter;
  try
    if not FItems.TryGetValue(AId, LE) then
      Exit(Refuse(REF_NOT_FOUND));
    case LE.State of
      fsAvailable:
        begin
          LE.State := fsPosting;
          LE.PosName := APosName;
          LE.Sale := ASale;
          LE.SinceMs := AmqpWallMs;
          FItems.AddOrSetValue(AId, LE);
          Result := OkChanged(True);
        end;
      fsPosting:
        begin
          // Idempotent for the owner/sale itself (outbox replay).
          if (LE.PosName = APosName) and (LE.Sale = ASale) then
            Result := OkChanged(False)
          else
          begin
            Result := Refuse(REF_ALREADY_POSTING);
            Result.CurrentPos := LE.PosName;
            Result.CurrentSale := LE.Sale;
          end;
        end;
      fsPosted:
        begin
          Result := Refuse(REF_ALREADY_POSTED);
          Result.CurrentPos := LE.PosName;
          Result.CurrentSale := LE.Sale;
        end;
    end;
  finally
    FLock.Leave;
  end;
end;

function TFuelStationRegistry.Unpost(const AId, APosName,
  ASale: string): TTransitionResult;
var
  LE: TTrackedFueling;
begin
  FLock.Enter;
  try
    if not FItems.TryGetValue(AId, LE) then
      Exit(Refuse(REF_NOT_FOUND));
    if LE.State = fsAvailable then
      // Already available: idempotent (outbox replay / double removal).
      Exit(OkChanged(False));
    if (LE.State <> fsPosting) or (LE.PosName <> APosName) then
      Exit(Refuse(REF_NOT_OWNER));
    LE.State := fsAvailable;
    LE.PosName := '';
    LE.Sale := '';
    LE.SinceMs := AmqpWallMs;
    FItems.AddOrSetValue(AId, LE);
    Result := OkChanged(True);
  finally
    FLock.Leave;
  end;
end;

function TFuelStationRegistry.Finish(const AId, APosName,
  ASale: string): TTransitionResult;
var
  LE: TTrackedFueling;
begin
  FLock.Enter;
  try
    if not FItems.TryGetValue(AId, LE) then
      Exit(Refuse(REF_NOT_FOUND));
    if (LE.State = fsPosted) and (LE.PosName = APosName) and (LE.Sale = ASale) then
      Exit(OkChanged(False)); // idempotent (replay)
    if (LE.State <> fsPosting) or (LE.PosName <> APosName) or (LE.Sale <> ASale) then
      Exit(Refuse(REF_NOT_OWNER));
    LE.State := fsPosted;
    LE.SinceMs := AmqpWallMs;
    FItems.AddOrSetValue(AId, LE);
    Result := OkChanged(True);
  finally
    FLock.Leave;
  end;
end;

function TFuelStationRegistry.CancelFinished(const AId, APosName,
  ASale: string): TTransitionResult;
var
  LE: TTrackedFueling;
begin
  FLock.Enter;
  try
    if not FItems.TryGetValue(AId, LE) then
      Exit(Refuse(REF_NOT_FOUND));
    if LE.State = fsAvailable then
      Exit(OkChanged(False)); // idempotent
    if (LE.State <> fsPosted) or (LE.PosName <> APosName) or (LE.Sale <> ASale) then
      Exit(Refuse(REF_NOT_OWNER));
    LE.State := fsAvailable;
    LE.PosName := '';
    LE.Sale := '';
    LE.SinceMs := AmqpWallMs;
    FItems.AddOrSetValue(AId, LE);
    Result := OkChanged(True);
  finally
    FLock.Leave;
  end;
end;

function TFuelStationRegistry.ForceRelease(const AId: string): TTransitionResult;
var
  LE: TTrackedFueling;
begin
  FLock.Enter;
  try
    if not FItems.TryGetValue(AId, LE) then
      Exit(Refuse(REF_NOT_FOUND));
    if LE.State = fsAvailable then
      Exit(OkChanged(False));
    Result := Default(TTransitionResult);
    Result.PreviousState := LE.State;
    Result.PreviousPos := LE.PosName;
    LE.State := fsAvailable;
    LE.PosName := '';
    LE.Sale := '';
    LE.SinceMs := AmqpWallMs;
    FItems.AddOrSetValue(AId, LE);
    Result.Ok := True;
    Result.Changed := True;
    Inc(FVersion);
    Result.Version := FVersion;
  finally
    FLock.Leave;
  end;
end;

function TFuelStationRegistry.Discard(const AId: string): TTransitionResult;
var
  LE: TTrackedFueling;
begin
  FLock.Enter;
  try
    if not FItems.TryGetValue(AId, LE) then
      Exit(OkChanged(False)); // idempotent
    if LE.State <> fsAvailable then
      Exit(Refuse(REF_LOCKED));
    FItems.Remove(AId);
    Result := OkChanged(True);
  finally
    FLock.Leave;
  end;
end;

function TFuelStationRegistry.Snapshot(out AAvailable: TArray<TFueling>;
  out ALocked: TArray<TTrackedFueling>): Int64;
var
  LE: TTrackedFueling;
  LD: TList<TFueling>;
  LB: TList<TTrackedFueling>;
begin
  LD := TList<TFueling>.Create;
  LB := TList<TTrackedFueling>.Create;
  FLock.Enter;
  try
    for LE in FItems.Values do
      if LE.State = fsAvailable then
        LD.Add(LE.Data)
      else
        LB.Add(LE);
    AAvailable := LD.ToArray;
    ALocked := LB.ToArray;
    Result := FVersion;
  finally
    FLock.Leave;
    LD.Free;
    LB.Free;
  end;
end;

function TFuelStationRegistry.All: TArray<TTrackedFueling>;
var
  LE: TTrackedFueling;
  L: TList<TTrackedFueling>;
begin
  L := TList<TTrackedFueling>.Create;
  FLock.Enter;
  try
    for LE in FItems.Values do
      L.Add(LE);
    Result := L.ToArray;
  finally
    FLock.Leave;
    L.Free;
  end;
end;

function TFuelStationRegistry.Version: Int64;
begin
  FLock.Enter;
  try
    Result := FVersion;
  finally
    FLock.Leave;
  end;
end;

end.
