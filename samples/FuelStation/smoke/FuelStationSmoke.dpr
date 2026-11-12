program FuelStationSmoke;

{ Smoke test of the Posto Automacao sample: brings up TFuelStationServerApp in-process
  (no GUI, no pumps), connects two POS terminals through TFuelStationPosClient and
  exercises the full message path -- snapshot, lancar, refusal on concurrency,
  finalizar, estornar, liberar_forcado.

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
  PORTA = 5799;

var
  GFailures: Integer = 0;

procedure Check(ACond: Boolean; const AMsg: string);
begin
  if ACond then
    WriteLn('  ok  ', AMsg)
  else
  begin
    WriteLn('  FALHA  ', AMsg);
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
    LApp.Startup(PORTA, '', 'pdv', False);

    LA1 := MakeFueling(1, 1, 'GC', 40.0, 5.899);
    LA2 := MakeFueling(2, 1, 'S10', 55.5, 6.19);
    LApp.Registry.RegisterFueling(LA1, LV);
    LApp.Events.NewFueling(LA1, LV);
    LApp.Registry.RegisterFueling(LA2, LV);
    LApp.Events.NewFueling(LA2, LV);

    LPosClient1 := TFuelStationPosClient.Create('127.0.0.1', PORTA, 'pdv-01', 'pdv');
    LPosClient1.Connect(nil, nil, nil);
    LPosClient2 := TFuelStationPosClient.Create('127.0.0.1', PORTA, 'pdv-02', 'pdv');
    LPosClient2.Connect(nil, nil, nil);

    WriteLn('--- snapshot ---');
    LR := LPosClient1.Call(EncReqSnapshot, 3000);
    Check(LR.Arrived, 'snapshot respondeu');
    if LR.Arrived then
    begin
      LArr := LR.Json.Get('disponiveis');
      Check((LArr <> nil) and (LArr.Count = 2), 'snapshot tem 2 disponiveis');
      LR.Json.Free;
    end;

    WriteLn('--- lancar (pdv-01) ---');
    LR := LPosClient1.Call(EncReqPost(LA1.Id, 'pdv-01', 'V-1'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'pdv-01 lancou A1');
    if LR.Arrived then LR.Json.Free;

    WriteLn('--- lancar concorrente (pdv-02) ---');
    LR := LPosClient2.Call(EncReqPost(LA1.Id, 'pdv-02', 'V-2'), 3000);
    Check(LR.Arrived and (not LR.Json.AsBool('ok')) and
      (LR.Json.AsStr('motivo') = REF_ALREADY_POSTING), 'pdv-02 recusado (JA_LANCANDO)');
    if LR.Arrived then
    begin
      Check(LR.Json.AsStr('pdvAtual') = 'pdv-01', 'recusa aponta o caixa pdv-01');
      LR.Json.Free;
    end;

    WriteLn('--- finalizar (pdv-01) ---');
    LR := LPosClient1.Call(EncReqFinish('pdv-01', 'V-1', [LA1.Id]), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'finalizar respondeu ok');
    if LR.Arrived then
    begin
      LArr := LR.Json.Get('itens');
      Check((LArr <> nil) and (LArr.Count = 1) and LArr.Item(0).AsBool('ok'),
        'item A1 finalizado');
      LR.Json.Free;
    end;

    WriteLn('--- lancar + estornar A2 (pdv-02) ---');
    LR := LPosClient2.Call(EncReqPost(LA2.Id, 'pdv-02', 'V-9'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'pdv-02 lancou A2');
    if LR.Arrived then LR.Json.Free;
    LR := LPosClient2.Call(EncReqUnpost(LA2.Id, 'pdv-02', 'V-9'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok'), 'pdv-02 estornou A2');
    if LR.Arrived then LR.Json.Free;
    LR := LPosClient1.Call(EncReqSnapshot, 3000);
    if LR.Arrived then
    begin
      LArr := LR.Json.Get('disponiveis');
      Check((LArr <> nil) and (LArr.Count = 1), 'A2 voltou a disponivel (1 disponivel)');
      LR.Json.Free;
    end;

    WriteLn('--- liberar_forcado A1 (supervisor via RPC) ---');
    LR := LPosClient1.Call(EncReqForceRelease(LA1.Id, 'sup', 'smoke'), 3000);
    Check(LR.Arrived and LR.Json.AsBool('ok') and
      (LR.Json.AsStr('estadoAnterior') = 'lancado'),
      'liberar_forcado devolveu estadoAnterior=lancado');
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
    WriteLn('SMOKE FAILED: ', GFailures, ' checagem(ns)');
  ExitCode := Ord(GFailures <> 0);
end.
