unit Posto.Contratos;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ O contrato de mensagens entre o servidor de automacao e os PDVs
  (ARQUITETURA.md §5 e §6): nomes de topologia, nomes de comando/evento e a
  (de)serializacao JSON de cada payload. Um fonte para os dois compiladores.

  O corpo de toda mensagem e' JSON UTF-8 (content-type application/json).
  A requisicao RPC leva o nome do comando tanto na propriedade 'type' do AMQP
  quanto no campo "cmd" do corpo -- o servidor despacha pelo "cmd". }

interface

uses
  SysUtils,
  AMQP.Wire,          // AmqpUtf8Encode / AmqpUtf8Decode
  Posto.Json,
  Posto.Abastecida;

const
  { --- topologia --- }
  EXCHANGE_EVENTOS = 'posto.eventos';
  FILA_COMANDOS    = 'posto.comandos';
  BIND_EVENTOS     = 'abastecida.#';

  { --- usuarios AMQP --- }
  USER_AUTOMACAO   = 'automacao';
  PREFIXO_USER_PDV = 'pdv-';      // pdv-01, pdv-02, ...

  { --- eventos (routing key em posto.eventos) --- }
  EVT_NOVA       = 'abastecida.nova';
  EVT_LANCANDO   = 'abastecida.lancando';
  EVT_LANCADO    = 'abastecida.lancado';
  EVT_DISPONIVEL = 'abastecida.disponivel';
  EVT_DESCARTADA = 'abastecida.descartada';

  { --- comandos (propriedade 'type' e campo "cmd") --- }
  CMD_SNAPSHOT            = 'snapshot';
  CMD_LANCAR             = 'lancar';
  CMD_ESTORNAR           = 'estornar';
  CMD_FINALIZAR          = 'finalizar';
  CMD_CANCELAR_FINALIZADA = 'cancelar_finalizada';
  CMD_LIBERAR_FORCADO    = 'liberar_forcado';
  CMD_DESCARTAR          = 'descartar';

  { motivos de abastecida.disponivel (MOT_*) e de recusa (REC_*) vivem em
    Posto.Abastecida -- sao desfechos da maquina de estados. }

type
  { Resultado de um id dentro de finalizar / cancelar_finalizada. }
  TItemResultado = record
    Id: string;
    Ok: Boolean;
    Motivo: string;
  end;

{ ------------------------------------------------------- bytes <-> json --- }

function JsonParaBytes(J: TJsonValue): TBytes;
function BytesParaJson(const ABytes: TBytes): TJsonValue;   // raises EPostoJson
function BytesParaTexto(const ABytes: TBytes): string;

{ ------------------------------------------------------- abastecida <-> json - }

{ Novo no' de objeto com os campos da abastecida (o chamador assume a posse
  ou o passa a um Put*). }
function AbastecidaParaJson(const A: TAbastecida): TJsonValue;
procedure JsonParaAbastecida(J: TJsonValue; out A: TAbastecida);

{ No' de objeto para a lista "bloqueadas" do snapshot. }
function EstadoParaJson(const AE: TAbastecidaEstado): TJsonValue;

{ ------------------------------------------------------- eventos (servidor) - }

function EncEventoNova(const A: TAbastecida; AVersao: Int64): TBytes;
function EncEventoLancando(const AId, APdv, AVenda: string; AVersao: Int64): TBytes;
function EncEventoLancado(const AId, APdv, AVenda: string; AVersao: Int64): TBytes;
function EncEventoDisponivel(const AId, AMotivo: string; AVersao: Int64): TBytes;
function EncEventoDescartada(const AId: string; AVersao: Int64): TBytes;

{ ------------------------------------------------------- respostas (servidor) }

function EncRespSnapshot(AVersao: Int64;
  const ADisponiveis: array of TAbastecida;
  const ABloqueadas: array of TAbastecidaEstado): TBytes;
function EncRespOk(AVersao: Int64): TBytes;
function EncRespRecusa(const AMotivo, APdvAtual, AVendaAtual: string): TBytes;
function EncRespItens(AVersao: Int64; const AItens: array of TItemResultado): TBytes;
function EncRespLiberarForcado(AVersao: Int64;
  const AEstadoAnterior, APdvAnterior: string): TBytes;

{ ------------------------------------------------------- requisicoes (PDV) -- }

function EncReqSnapshot: TBytes;
function EncReqLancar(const AId, APdv, AVenda: string): TBytes;
function EncReqEstornar(const AId, APdv, AVenda: string): TBytes;
function EncReqFinalizar(const APdv, AVenda: string;
  const AIds: array of string): TBytes;
function EncReqCancelarFinalizada(const APdv, AVenda, AOperador: string;
  const AIds: array of string): TBytes;
function EncReqLiberarForcado(const AId, AOperador, AMotivo: string): TBytes;
function EncReqDescartar(const AId, AOperador, AMotivo: string): TBytes;

implementation

{ ------------------------------------------------------- bytes <-> json --- }

function JsonParaBytes(J: TJsonValue): TBytes;
begin
  Result := AmqpUtf8Encode(J.ToJson);
end;

function BytesParaJson(const ABytes: TBytes): TJsonValue;
begin
  Result := TJsonValue.Parse(AmqpUtf8Decode(ABytes));
end;

function BytesParaTexto(const ABytes: TBytes): string;
begin
  Result := AmqpUtf8Decode(ABytes);
end;

{ um helper local: monta o corpo, serializa, libera. }
function Serializa(J: TJsonValue): TBytes;
begin
  try
    Result := AmqpUtf8Encode(J.ToJson);
  finally
    J.Free;
  end;
end;

{ ------------------------------------------------------- abastecida <-> json - }

function AbastecidaParaJson(const A: TAbastecida): TJsonValue;
begin
  Result := TJsonValue.NewObject;
  Result.Put('id', A.Id);
  Result.PutInt('bomba', A.Bomba);
  Result.PutInt('bico', A.Bico);
  Result.Put('produto', A.Produto);
  Result.Put('litros', A.Litros);
  Result.Put('valorLitro', A.ValorLitro);
  Result.Put('valorTotal', A.ValorTotal);
  Result.Put('encerranteInicio', A.EncerranteInicio);
  Result.Put('encerranteFim', A.EncerranteFim);
  Result.PutInt('dataHoraMs', A.DataHoraMs);
  Result.Put('tipo', TipoParaStr(A.Tipo));
end;

procedure JsonParaAbastecida(J: TJsonValue; out A: TAbastecida);
begin
  A := Default(TAbastecida);
  if J = nil then
    Exit;
  A.Id := J.AsStr('id');
  A.Bomba := J.AsInt('bomba');
  A.Bico := J.AsInt('bico');
  A.Produto := J.AsStr('produto');
  A.Litros := J.AsNum('litros');
  A.ValorLitro := J.AsNum('valorLitro');
  A.ValorTotal := J.AsNum('valorTotal');
  A.EncerranteInicio := J.AsNum('encerranteInicio');
  A.EncerranteFim := J.AsNum('encerranteFim');
  A.DataHoraMs := J.AsInt('dataHoraMs');
  A.Tipo := StrParaTipo(J.AsStr('tipo'));
end;

function EstadoParaJson(const AE: TAbastecidaEstado): TJsonValue;
begin
  Result := TJsonValue.NewObject;
  Result.Put('id', AE.Dados.Id);
  Result.Put('estado', EstadoParaStr(AE.Estado));
  Result.Put('pdv', AE.Pdv);
  Result.Put('venda', AE.Venda);
  Result.PutInt('desde', AE.DesdeMs);
end;

{ ------------------------------------------------------- eventos --- }

function EncEventoNova(const A: TAbastecida; AVersao: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.PutRaw('abastecida', AbastecidaParaJson(A));
  J.PutInt('versao', AVersao);
  Result := Serializa(J);
end;

function EncEventoIdPdvVenda(const AId, APdv, AVenda: string;
  AVersao: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.Put('pdv', APdv);
  J.Put('venda', AVenda);
  J.PutInt('versao', AVersao);
  Result := Serializa(J);
end;

function EncEventoLancando(const AId, APdv, AVenda: string;
  AVersao: Int64): TBytes;
begin
  Result := EncEventoIdPdvVenda(AId, APdv, AVenda, AVersao);
end;

function EncEventoLancado(const AId, APdv, AVenda: string;
  AVersao: Int64): TBytes;
begin
  Result := EncEventoIdPdvVenda(AId, APdv, AVenda, AVersao);
end;

function EncEventoDisponivel(const AId, AMotivo: string; AVersao: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.Put('motivo', AMotivo);
  J.PutInt('versao', AVersao);
  Result := Serializa(J);
end;

function EncEventoDescartada(const AId: string; AVersao: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('id', AId);
  J.PutInt('versao', AVersao);
  Result := Serializa(J);
end;

{ ------------------------------------------------------- respostas --- }

function EncRespSnapshot(AVersao: Int64;
  const ADisponiveis: array of TAbastecida;
  const ABloqueadas: array of TAbastecidaEstado): TBytes;
var
  J, LArr: TJsonValue;
  I: Integer;
begin
  J := TJsonValue.NewObject;
  J.PutInt('versao', AVersao);
  LArr := J.PutArray('disponiveis');
  for I := 0 to High(ADisponiveis) do
    LArr.AddItem(AbastecidaParaJson(ADisponiveis[I]));
  LArr := J.PutArray('bloqueadas');
  for I := 0 to High(ABloqueadas) do
    LArr.AddItem(EstadoParaJson(ABloqueadas[I]));
  Result := Serializa(J);
end;

function EncRespOk(AVersao: Int64): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', True);
  J.PutInt('versao', AVersao);
  Result := Serializa(J);
end;

function EncRespRecusa(const AMotivo, APdvAtual, AVendaAtual: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', False);
  J.Put('motivo', AMotivo);
  if APdvAtual <> '' then
    J.Put('pdvAtual', APdvAtual);
  if AVendaAtual <> '' then
    J.Put('vendaAtual', AVendaAtual);
  Result := Serializa(J);
end;

function EncRespItens(AVersao: Int64;
  const AItens: array of TItemResultado): TBytes;
var
  J, LArr, LItem: TJsonValue;
  I: Integer;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', True);
  J.PutInt('versao', AVersao);
  LArr := J.PutArray('itens');
  for I := 0 to High(AItens) do
  begin
    LItem := LArr.AddItemObject;
    LItem.Put('id', AItens[I].Id);
    LItem.Put('ok', AItens[I].Ok);
    if AItens[I].Motivo <> '' then
      LItem.Put('motivo', AItens[I].Motivo);
  end;
  Result := Serializa(J);
end;

function EncRespLiberarForcado(AVersao: Int64;
  const AEstadoAnterior, APdvAnterior: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('ok', True);
  J.PutInt('versao', AVersao);
  J.Put('estadoAnterior', AEstadoAnterior);
  J.Put('pdvAnterior', APdvAnterior);
  Result := Serializa(J);
end;

{ ------------------------------------------------------- requisicoes --- }

function EncReqSnapshot: TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', CMD_SNAPSHOT);
  Result := Serializa(J);
end;

function EncReqIdPdvVenda(const ACmd, AId, APdv, AVenda: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', ACmd);
  J.Put('id', AId);
  J.Put('pdv', APdv);
  J.Put('venda', AVenda);
  Result := Serializa(J);
end;

function EncReqLancar(const AId, APdv, AVenda: string): TBytes;
begin
  Result := EncReqIdPdvVenda(CMD_LANCAR, AId, APdv, AVenda);
end;

function EncReqEstornar(const AId, APdv, AVenda: string): TBytes;
begin
  Result := EncReqIdPdvVenda(CMD_ESTORNAR, AId, APdv, AVenda);
end;

procedure PoeIds(J: TJsonValue; const AIds: array of string);
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

function EncReqFinalizar(const APdv, AVenda: string;
  const AIds: array of string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', CMD_FINALIZAR);
  J.Put('pdv', APdv);
  J.Put('venda', AVenda);
  PoeIds(J, AIds);
  Result := Serializa(J);
end;

function EncReqCancelarFinalizada(const APdv, AVenda, AOperador: string;
  const AIds: array of string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', CMD_CANCELAR_FINALIZADA);
  J.Put('pdv', APdv);
  J.Put('venda', AVenda);
  J.Put('operador', AOperador);
  PoeIds(J, AIds);
  Result := Serializa(J);
end;

function EncReqOperador(const ACmd, AId, AOperador, AMotivo: string): TBytes;
var
  J: TJsonValue;
begin
  J := TJsonValue.NewObject;
  J.Put('cmd', ACmd);
  J.Put('id', AId);
  J.Put('operador', AOperador);
  J.Put('motivo', AMotivo);
  Result := Serializa(J);
end;

function EncReqLiberarForcado(const AId, AOperador, AMotivo: string): TBytes;
begin
  Result := EncReqOperador(CMD_LIBERAR_FORCADO, AId, AOperador, AMotivo);
end;

function EncReqDescartar(const AId, AOperador, AMotivo: string): TBytes;
begin
  Result := EncReqOperador(CMD_DESCARTAR, AId, AOperador, AMotivo);
end;

end.
