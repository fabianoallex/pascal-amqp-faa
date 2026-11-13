program FuelStationSmoke;

{ Smoke test of the FuelStation sample: brings up TFuelStationServerApp in-process
  (no GUI, no pumps), connects two POS terminals through TFuelStationPosClient and
  exercises the full message path -- snapshot, post, refusal on concurrency,
  finish, unpost, force_release.

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild FuelStationSmoke.lpi
    Delphi: open FuelStationSmoke.dproj in the IDE

  Exits with code 0 if everything passed, 1 otherwise. }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ELSE}
  {$APPTYPE CONSOLE}
{$ENDIF}

uses
  {$IFDEF FPC}{$IFDEF UNIX}cthreads,{$ENDIF}{$ENDIF}
  SysUtils,
  AMQP.Threading,
  FuelStation.Json, FuelStation.Fueling, FuelStation.Contracts,
  FuelStation.Server.App,
  FuelStation.POS.Client;

const
  SMOKE_PORT = 5799;

var
  GFailures: Integer = 0;

procedure Check(ACond: Boolean; const AMsg: string);
begin
  if ACond then
    WriteLn('  ok  ', AMsg)
  else
  begin
    WriteLn('  FAIL  ', AMsg);
    Inc(GFailures);
  end;
end;

function MakeFueling(APump, ANozzle: Integer; const AProduct: string;
  ALiters, APrice: Double): TFueling;
begin
  Result := Default(TFueling);
  Result.Pump := APump;
  Result.Nozzle := ANozzle;
  Result.Product := AProduct;
  Result.Liters := ALiters;
  Result.PricePerLiter := APrice;
  Result.TotalAmount := Round(ALiters * APrice * 100) / 100;
  Result.StartTotalizer := 1000;
  Result.EndTotalizer := 1000 + ALiters;
  Result.TimestampMs := AmqpWallMs;
  Result.Kind := ftSale;
  Result.Id := BuildFuelingId(APump, ANozzle, Result.EndTotalizer);
end;

var
  LApp: TFuelStationServerApp;
  LPosClient1, LPosClient2: TFuelStationPosClient;
  LA1, LA2: TFueling;
  LV: Int64;
  LR: TRpcReply;
  LArr: TJsonValue;

begin
  SetMultiByteConversionCodePage(CP_UTF8);

  LApp := TFuelStationServerApp.Create;
  LPosClient1 := nil;
  LPosClient2 := nil;
  try
    LApp.Startup(SMOKE_PORT, '', 'pos', False);

    LA1 := MakeFueling(1, 1, 'GC', 40.0, 5.899);
    LA2 := MakeFueling(2, 1, 'S10', 55.5, 6.19);
    LApp.Registry.RegisterFueling(LA1, LV);
    LApp.Events.NewFueling(LA1, LV);
    LApp.Registry.RegisterFueling(LA2, LV);
    LApp.Events.NewFueling(LA2, LV);

    LPosClient1 := TFuelStationPosClient.Create('127.0.0.1', SMOKE_PORT, 'pos-01', 'pos');
    LPosClient1.Connect(nil, nil, nil);
    LPosClient2 := TFuelStationPosClient.Create('127.0.0.1', SMOKE_PORT, 'pos-02', 'pos');
    LPosClient2.Connect(nil, nil, nil);

    WriteLn('--- snapshot ---');
    LR := LPosClient1.Call(EncReqSnapshot, 3000);
    Check(LR.Arrived, 'snapshot replied');
    if LR.Arrived then
    begin
      LArr := LR.Json.Get('available');
      Check((LArr <> nil) and (LArr.Count = 2), 'snapshot has 2 available');
      LR.Json.Free;
    end;

    WriteLn('--- post (pos-01) ---');
    LR := LPosClient1.Call(EncReqPost(LA1.Id, 'pos-01', 'S-1'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'pos-01 posted A1');
    if LR.Arrived then LR.Json.Free;

    WriteLn('--- concurrent post (pos-02) ---');
    LR := LPosClient2.Call(EncReqPost(LA1.Id, 'pos-02', 'S-2'), 3000);
    Check(LR.Arrived and (not LR.Json.AsBool('ok')) and
      (LR.Json.AsStr('reason') = REF_ALREADY_POSTING), 'pos-02 refused (ALREADY_POSTING)');
    if LR.Arrived then
    begin
      Check(LR.Json.AsStr('currentPos') = 'pos-01', 'refusal names POS pos-01');
      LR.Json.Free;
    end;

    WriteLn('--- finish (pos-01) ---');
    LR := LPosClient1.Call(EncReqFinish('pos-01', 'S-1', [LA1.Id]), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'finish replied ok');
    if LR.Arrived then
    begin
      LArr := LR.Json.Get('items');
      Check((LArr <> nil) and (LArr.Count = 1) and LArr.Item(0).AsBool('ok'),
        'item A1 finished');
      LR.Json.Free;
    end;

    WriteLn('--- post + unpost A2 (pos-02) ---');
    LR := LPosClient2.Call(EncReqPost(LA2.Id, 'pos-02', 'S-9'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'pos-02 posted A2');
    if LR.Arrived then LR.Json.Free;
    LR := LPosClient2.Call(EncReqUnpost(LA2.Id, 'pos-02', 'S-9'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'pos-02 unposted A2');
    if LR.Arrived then LR.Json.Free;
    LR := LPosClient1.Call(EncReqSnapshot, 3000);
    if LR.Arrived then
    begin
      LArr := LR.Json.Get('available');
      Check((LArr <> nil) and (LArr.Count = 1), 'A2 back to available (1 available)');
      LR.Json.Free;
    end;

    WriteLn('--- force_release A1 (supervisor via RPC) ---');
    LR := LPosClient1.Call(EncReqForceRelease(LA1.Id, 'sup', 'smoke'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok') and
      (LR.Json.AsStr('previousState') = 'posted'),
      'force_release returned previousState=posted');
    if LR.Arrived then LR.Json.Free;

    Sleep(300);  // let the last events drain
  finally
    LPosClient1.Free;
    LPosClient2.Free;
    LApp.Free;
  end;

  WriteLn;
  if GFailures = 0 then
    WriteLn('SMOKE OK')
  else
    WriteLn('SMOKE FAILED: ', GFailures, ' check(s)');
  ExitCode := Ord(GFailures <> 0);
end.
