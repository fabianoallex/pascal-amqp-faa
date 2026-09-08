unit Posto.Servidor.Comandos;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Consumidor da fila posto.comandos (ARQUITETURA.md §6.2). Para cada
  requisicao RPC: parseia o corpo, aplica a transicao no registro autoritativo,
  publica o(s) evento(s) da mudanca e responde pelo reply-to ecoando o
  correlation-id. Ack manual so' depois de responder.

  prefetch 1 + consumidor unico => os comandos sao aplicados em serie, que e'
  o que garante "uma abastecida em uma venda so'". }

interface

uses
  SysUtils,
  AMQP.Connection, AMQP.Basic.Methods,
  Posto.Json, Posto.Abastecida, Posto.Contratos,
  Posto.Servidor.Registro, Posto.Servidor.Eventos;

type
  TOnLogServidor = procedure(const AMsg: string) of object;
  TOnAlgoMudou = procedure of object;

  TPostoComandos = class
  private
    FCanal: TAMQPChannel;
    FRegistro: TPostoRegistro;
    FEventos: TPostoEventos;
    FTag: string;
    FOnLog: TOnLogServidor;
    FOnMudou: TOnAlgoMudou;
    procedure Log(const AMsg: string);
    procedure OnComando(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure Responde(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery;
      const ABytes: TBytes);
    function Despacha(J: TJsonValue): TBytes;
    function TrataLancar(J: TJsonValue): TBytes;
    function TrataEstornar(J: TJsonValue): TBytes;
    function TrataFinalizar(J: TJsonValue; ACancelamento: Boolean): TBytes;
    function TrataLiberarForcado(J: TJsonValue): TBytes;
    function TrataDescartar(J: TJsonValue): TBytes;
  public
    constructor Create(ACanal: TAMQPChannel; ARegistro: TPostoRegistro;
      AEventos: TPostoEventos);
    procedure Iniciar;
    procedure Parar;
    property OnLog: TOnLogServidor read FOnLog write FOnLog;
    property OnMudou: TOnAlgoMudou read FOnMudou write FOnMudou;
  end;

implementation

constructor TPostoComandos.Create(ACanal: TAMQPChannel;
  ARegistro: TPostoRegistro; AEventos: TPostoEventos);
begin
  inherited Create;
  FCanal := ACanal;
  FRegistro := ARegistro;
  FEventos := AEventos;
end;

procedure TPostoComandos.Iniciar;
begin
  FCanal.Qos(1);
  FTag := FCanal.Consume(FILA_COMANDOS, OnComando);   // ack manual
end;

procedure TPostoComandos.Parar;
begin
  if FTag <> '' then
  begin
    try
      FCanal.Cancel(FTag);
    except
    end;
    FTag := '';
  end;
end;

procedure TPostoComandos.Log(const AMsg: string);
begin
  if Assigned(FOnLog) then
    FOnLog(AMsg);
end;

procedure TPostoComandos.Responde(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery; const ABytes: TBytes);
var
  LProps: TAMQPBasicProperties;
begin
  if ADelivery.Properties.ReplyTo = '' then
    Exit;
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  if ADelivery.Properties.CorrelationId <> '' then
    LProps.SetCorrelationId(ADelivery.Properties.CorrelationId);
  AChannel.Publish('', ADelivery.Properties.ReplyTo, ABytes, LProps);
end;

procedure TPostoComandos.OnComando(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  J: TJsonValue;
  LResp: TBytes;
begin
  J := nil;
  try
    try
      J := TJsonValue.Parse(ADelivery.BodyAsText);
      LResp := Despacha(J);
    except
      on E: Exception do
      begin
        Log('Comando invalido: ' + E.Message);
        LResp := EncRespRecusa('ERRO_FORMATO', '', '');
      end;
    end;
    Responde(AChannel, ADelivery, LResp);
  finally
    J.Free;
    AChannel.Ack(ADelivery.DeliveryTag);
  end;
  if Assigned(FOnMudou) then
    FOnMudou();
end;

function TPostoComandos.Despacha(J: TJsonValue): TBytes;
var
  LCmd: string;
  LVersao: Int64;
  LDisp: TArray<TAbastecida>;
  LBloq: TArray<TAbastecidaEstado>;
begin
  LCmd := J.AsStr('cmd');
  if LCmd = CMD_SNAPSHOT then
  begin
    LVersao := FRegistro.Snapshot(LDisp, LBloq);
    Result := EncRespSnapshot(LVersao, LDisp, LBloq);
  end
  else if LCmd = CMD_LANCAR then
    Result := TrataLancar(J)
  else if LCmd = CMD_ESTORNAR then
    Result := TrataEstornar(J)
  else if LCmd = CMD_FINALIZAR then
    Result := TrataFinalizar(J, False)
  else if LCmd = CMD_CANCELAR_FINALIZADA then
    Result := TrataFinalizar(J, True)
  else if LCmd = CMD_LIBERAR_FORCADO then
    Result := TrataLiberarForcado(J)
  else if LCmd = CMD_DESCARTAR then
    Result := TrataDescartar(J)
  else
  begin
    Log('Comando desconhecido: "' + LCmd + '"');
    Result := EncRespRecusa('CMD_DESCONHECIDO', '', '');
  end;
end;

function TPostoComandos.TrataLancar(J: TJsonValue): TBytes;
var
  LId, LPdv, LVenda: string;
  LR: TResultadoTransicao;
begin
  LId := J.AsStr('id');
  LPdv := J.AsStr('pdv');
  LVenda := J.AsStr('venda');
  LR := FRegistro.Lancar(LId, LPdv, LVenda);
  if LR.Ok then
  begin
    if LR.Mudou then
    begin
      FEventos.Lancando(LId, LPdv, LVenda, LR.Versao);
      Log(Format('lancar: %s -> %s (venda %s), versao %d',
        [LId, LPdv, LVenda, LR.Versao]));
    end;
    Result := EncRespOk(LR.Versao);
  end
  else
  begin
    Log(Format('lancar RECUSADO: %s por %s (motivo %s, dono %s)',
      [LId, LPdv, LR.Motivo, LR.PdvAtual]));
    Result := EncRespRecusa(LR.Motivo, LR.PdvAtual, LR.VendaAtual);
  end;
end;

function TPostoComandos.TrataEstornar(J: TJsonValue): TBytes;
var
  LId, LPdv, LVenda, LMotivo: string;
  LR: TResultadoTransicao;
begin
  LId := J.AsStr('id');
  LPdv := J.AsStr('pdv');
  LVenda := J.AsStr('venda');
  LMotivo := J.AsStr('motivo', MOT_ESTORNO_ITEM);
  LR := FRegistro.Estornar(LId, LPdv, LVenda);
  if LR.Ok then
  begin
    if LR.Mudou then
    begin
      FEventos.Disponivel(LId, LMotivo, LR.Versao);
      Log(Format('estornar: %s liberada por %s (%s), versao %d',
        [LId, LPdv, LMotivo, LR.Versao]));
    end;
    Result := EncRespOk(LR.Versao);
  end
  else
    Result := EncRespRecusa(LR.Motivo, LR.PdvAtual, LR.VendaAtual);
end;

function TPostoComandos.TrataFinalizar(J: TJsonValue;
  ACancelamento: Boolean): TBytes;
var
  LPdv, LVenda, LId: string;
  LIds: TJsonValue;
  I: Integer;
  LR: TResultadoTransicao;
  LItens: array of TItemResultado;
  LMotivo: string;
begin
  LPdv := J.AsStr('pdv');
  LVenda := J.AsStr('venda');
  LIds := J.Get('ids');
  if ACancelamento then
    LMotivo := MOT_CANCEL_NOTA
  else
    LMotivo := '';
  SetLength(LItens, 0);
  if (LIds <> nil) and (LIds.Kind = jkArray) then
    for I := 0 to LIds.Count - 1 do
    begin
      LId := LIds.Item(I).StrVal;
      if ACancelamento then
        LR := FRegistro.CancelarFinalizada(LId, LPdv, LVenda)
      else
        LR := FRegistro.Finalizar(LId, LPdv, LVenda);

      if LR.Ok and LR.Mudou then
      begin
        if ACancelamento then
          FEventos.Disponivel(LId, LMotivo, LR.Versao)
        else
          FEventos.Lancado(LId, LPdv, LVenda, LR.Versao);
      end;

      SetLength(LItens, Length(LItens) + 1);
      LItens[High(LItens)].Id := LId;
      LItens[High(LItens)].Ok := LR.Ok;
      if not LR.Ok then
        LItens[High(LItens)].Motivo := LR.Motivo;
    end;

  if ACancelamento then
    Log(Format('cancelar_finalizada: venda %s (%d itens)', [LVenda, Length(LItens)]))
  else
    Log(Format('finalizar: venda %s (%d itens)', [LVenda, Length(LItens)]));
  Result := EncRespItens(FRegistro.Versao, LItens);
end;

function TPostoComandos.TrataLiberarForcado(J: TJsonValue): TBytes;
var
  LId, LOperador, LMotivo: string;
  LR: TResultadoTransicao;
begin
  LId := J.AsStr('id');
  LOperador := J.AsStr('operador');
  LMotivo := J.AsStr('motivo');
  LR := FRegistro.LiberarForcado(LId);
  if not LR.Ok then
    Exit(EncRespRecusa(LR.Motivo, '', ''));
  if LR.Mudou then
  begin
    FEventos.Disponivel(LId, MOT_LIBERACAO_MANUAL, LR.Versao);
    Log(Format('LIBERACAO MANUAL: %s (era %s de %s) por "%s": %s',
      [LId, EstadoParaStr(LR.EstadoAnterior), LR.PdvAnterior, LOperador,
       LMotivo]));
    Result := EncRespLiberarForcado(LR.Versao,
      EstadoParaStr(LR.EstadoAnterior), LR.PdvAnterior);
  end
  else
    Result := EncRespLiberarForcado(LR.Versao, 'disponivel', '');
end;

function TPostoComandos.TrataDescartar(J: TJsonValue): TBytes;
var
  LId, LOperador, LMotivo: string;
  LR: TResultadoTransicao;
begin
  LId := J.AsStr('id');
  LOperador := J.AsStr('operador');
  LMotivo := J.AsStr('motivo');
  LR := FRegistro.Descartar(LId);
  if not LR.Ok then
    Exit(EncRespRecusa(LR.Motivo, '', ''));
  if LR.Mudou then
  begin
    FEventos.Descartada(LId, LR.Versao);
    Log(Format('DESCARTE: %s por "%s": %s', [LId, LOperador, LMotivo]));
  end;
  Result := EncRespOk(LR.Versao);
end;

end.
