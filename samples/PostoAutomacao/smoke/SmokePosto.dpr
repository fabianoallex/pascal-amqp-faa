program SmokePosto;

{ Smoke test do sample Posto Automacao: sobe a TPostoServidorApp in-process
  (sem GUI, sem bombas), conecta dois PDVs pelo TPostoPdvCliente e exercita o
  caminho completo de mensagens -- snapshot, lancar, recusa por concorrencia,
  finalizar, estornar, liberar_forcado.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild SmokePosto.lpi
    Delphi: abrir SmokePosto.dproj no IDE

  Sai com codigo 0 se tudo passou, 1 caso contrario. }

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
  Posto.Json, Posto.Abastecida, Posto.Contratos,
  Posto.Servidor.App,
  Posto.PDV.Cliente;

const
  PORTA = 5799;

var
  GFalhas: Integer = 0;

procedure Checa(ACond: Boolean; const AMsg: string);
begin
  if ACond then
    WriteLn('  ok  ', AMsg)
  else
  begin
    WriteLn('  FALHA  ', AMsg);
    Inc(GFalhas);
  end;
end;

function NovaAbastecida(ABomba, ABico: Integer; const AProd: string;
  ALitros, APreco: Double): TAbastecida;
begin
  Result := Default(TAbastecida);
  Result.Bomba := ABomba;
  Result.Bico := ABico;
  Result.Produto := AProd;
  Result.Litros := ALitros;
  Result.ValorLitro := APreco;
  Result.ValorTotal := Round(ALitros * APreco * 100) / 100;
  Result.EncerranteInicio := 1000;
  Result.EncerranteFim := 1000 + ALitros;
  Result.DataHoraMs := AmqpWallMs;
  Result.Tipo := taVenda;
  Result.Id := MontaIdAbastecida(ABomba, ABico, Result.EncerranteFim);
end;

var
  LApp: TPostoServidorApp;
  LPdv1, LPdv2: TPostoPdvCliente;
  LA1, LA2: TAbastecida;
  LV: Int64;
  LR: TRespostaRpc;
  LArr: TJsonValue;

begin
  SetMultiByteConversionCodePage(CP_UTF8);

  LApp := TPostoServidorApp.Create;
  LPdv1 := nil;
  LPdv2 := nil;
  try
    LApp.Iniciar(PORTA, '', 'pdv', False);

    LA1 := NovaAbastecida(1, 1, 'GC', 40.0, 5.899);
    LA2 := NovaAbastecida(2, 1, 'S10', 55.5, 6.19);
    LApp.Registro.Registrar(LA1, LV);
    LApp.Eventos.Nova(LA1, LV);
    LApp.Registro.Registrar(LA2, LV);
    LApp.Eventos.Nova(LA2, LV);

    LPdv1 := TPostoPdvCliente.Create('127.0.0.1', PORTA, 'pdv-01', 'pdv');
    LPdv1.Conectar(nil, nil, nil);
    LPdv2 := TPostoPdvCliente.Create('127.0.0.1', PORTA, 'pdv-02', 'pdv');
    LPdv2.Conectar(nil, nil, nil);

    WriteLn('--- snapshot ---');
    LR := LPdv1.Chamar(EncReqSnapshot, 3000);
    Checa(LR.Chegou, 'snapshot respondeu');
    if LR.Chegou then
    begin
      LArr := LR.Json.Get('disponiveis');
      Checa((LArr <> nil) and (LArr.Count = 2), 'snapshot tem 2 disponiveis');
      LR.Json.Free;
    end;

    WriteLn('--- lancar (pdv-01) ---');
    LR := LPdv1.Chamar(EncReqLancar(LA1.Id, 'pdv-01', 'V-1'), 3000);
    Checa(LR.Chegou and LR.Json.AsBool('ok'), 'pdv-01 lancou A1');
    if LR.Chegou then LR.Json.Free;

    WriteLn('--- lancar concorrente (pdv-02) ---');
    LR := LPdv2.Chamar(EncReqLancar(LA1.Id, 'pdv-02', 'V-2'), 3000);
    Checa(LR.Chegou and (not LR.Json.AsBool('ok')) and
      (LR.Json.AsStr('motivo') = REC_JA_LANCANDO), 'pdv-02 recusado (JA_LANCANDO)');
    if LR.Chegou then
    begin
      Checa(LR.Json.AsStr('pdvAtual') = 'pdv-01', 'recusa aponta o caixa pdv-01');
      LR.Json.Free;
    end;

    WriteLn('--- finalizar (pdv-01) ---');
    LR := LPdv1.Chamar(EncReqFinalizar('pdv-01', 'V-1', [LA1.Id]), 3000);
    Checa(LR.Chegou and LR.Json.AsBool('ok'), 'finalizar respondeu ok');
    if LR.Chegou then
    begin
      LArr := LR.Json.Get('itens');
      Checa((LArr <> nil) and (LArr.Count = 1) and LArr.Item(0).AsBool('ok'),
        'item A1 finalizado');
      LR.Json.Free;
    end;

    WriteLn('--- lancar + estornar A2 (pdv-02) ---');
    LR := LPdv2.Chamar(EncReqLancar(LA2.Id, 'pdv-02', 'V-9'), 3000);
    Checa(LR.Chegou and LR.Json.AsBool('ok'), 'pdv-02 lancou A2');
    if LR.Chegou then LR.Json.Free;
    LR := LPdv2.Chamar(EncReqEstornar(LA2.Id, 'pdv-02', 'V-9'), 3000);
    Checa(LR.Chegou and LR.Json.AsBool('ok'), 'pdv-02 estornou A2');
    if LR.Chegou then LR.Json.Free;
    LR := LPdv1.Chamar(EncReqSnapshot, 3000);
    if LR.Chegou then
    begin
      LArr := LR.Json.Get('disponiveis');
      Checa((LArr <> nil) and (LArr.Count = 1), 'A2 voltou a disponivel (1 disponivel)');
      LR.Json.Free;
    end;

    WriteLn('--- liberar_forcado A1 (supervisor via RPC) ---');
    LR := LPdv1.Chamar(EncReqLiberarForcado(LA1.Id, 'sup', 'smoke'), 3000);
    Checa(LR.Chegou and LR.Json.AsBool('ok') and
      (LR.Json.AsStr('estadoAnterior') = 'lancado'),
      'liberar_forcado devolveu estadoAnterior=lancado');
    if LR.Chegou then LR.Json.Free;

    Sleep(300);  // deixa os ultimos eventos drenarem
  finally
    LPdv1.Free;
    LPdv2.Free;
    LApp.Free;
  end;

  WriteLn;
  if GFalhas = 0 then
    WriteLn('SMOKE OK')
  else
    WriteLn('SMOKE FALHOU: ', GFalhas, ' checagem(ns)');
  ExitCode := Ord(GFalhas <> 0);
end.
