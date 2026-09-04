unit AMQP.Server.Records;

{$I amqp.inc}

{ O que cada registro do WAL carrega -- sub-modulo broker, WS3 da Fase 4
  (durabilidade, ver CLAUDE.md, decisoes D19-D28).

  A WS1 (AMQP.Server.Wal) trata o payload de um registro como bytes opacos e o
  campo Kind como um numero sem significado. E' aqui que os dois ganham sentido:
  esta unit e' o dicionario entre "bytes num arquivo" e "um exchange duravel foi
  declarado". Ela nao escreve nada e nao conhece journal nenhum -- so' codifica
  e decodifica.

  POR QUE O CODEC E' O DA AMQP.WIRE, E NAO UM NOVO
  ------------------------------------------------
  Os campos de topologia sao os MESMOS que vem no fio: nomes sao shortstr e os
  x-arguments sao um field-table. A AMQP.Wire ja sabe ler e escrever os dois, e
  o field-table dela e' a parte da lib com mais teste em cima (121 assercoes
  unitarias, incluindo o round-trip de tabelas aninhadas e arrays que a WS1 da
  Fase 3 consertou). Inventar um segundo formato para o mesmo dado seria criar
  uma segunda chance de errar, com metade da cobertura.

  A NUMERACAO DOS KINDS E' PERMANENTE
  -----------------------------------
  Um Kind gravado hoje sera' lido por uma versao futura do broker. Numero de
  Kind NUNCA e' reaproveitado: se um registro sair de uso, o numero dele fica
  aposentado. A faixa 10-19 e' topologia (WS3); 20-29 fica para conteudo e
  colocacao (WS4, D22).

  O QUE NAO ENTRA NO REGISTRO, DE PROPOSITO
  -----------------------------------------
  Fila EXCLUSIVA nao tem registro nenhum (D20): ela morre com a conexao dona, e
  depois de um restart nao existe conexao dona. Repare que o registro de fila
  nem CARREGA o flag `exclusive` -- assim o formato nao consegue expressar uma
  coisa que nao deveria existir, e nao ha como um caminho futuro gravar isso por
  descuido.

  POSSE
  -----
  Decodificar CRIA a TAMQPFieldTable de Arguments (ou devolve nil se a tabela
  estava vazia); quem chama e' o dono e tem de liberar. Codificar NAO toma posse
  de nada. }

interface

uses
  SysUtils,
  AMQP.Wire;

const
  // --- topologia (WS3) -- faixa 10-19 -------------------------------------
  AMQP_REC_EXCHANGE_DECLARE = 10;
  AMQP_REC_EXCHANGE_DELETE  = 11;
  AMQP_REC_QUEUE_DECLARE    = 12;
  AMQP_REC_QUEUE_DELETE     = 13;
  AMQP_REC_QUEUE_BIND       = 14;
  AMQP_REC_QUEUE_UNBIND     = 15;
  AMQP_REC_EXCHANGE_BIND    = 16;
  AMQP_REC_EXCHANGE_UNBIND  = 17;

  // 20-29 reservados para CONTENT / ENQ / DEQ (WS4, decisao D22).

type
  EAMQPRecords = class(Exception);

  { Declare de exchange duravel. }
  TAMQPRecExchange = record
    VHost: string;
    Name: string;
    ExchangeType: string;
    /// Hoje sempre True (so' o duravel e' gravado, D20). Guardado assim mesmo
    /// para o arquivo ser legivel sem conhecer a regra de quem o escreveu.
    Durable: Boolean;
    AutoDelete: Boolean;
    Internal: Boolean;
    /// nil quando nao havia argumentos. O DECODE cria; o chamador libera.
    Arguments: TAMQPFieldTable;
  end;

  { Declare de fila duravel. Sem `exclusive`, de proposito -- ver o cabecalho. }
  TAMQPRecQueue = record
    VHost: string;
    Name: string;
    Durable: Boolean;
    AutoDelete: Boolean;
    Arguments: TAMQPFieldTable;
  end;

  { Bind/unbind, de fila ou de exchange. Destination e' a fila ou o exchange de
    destino; Source e' sempre o exchange de origem. }
  TAMQPRecBinding = record
    VHost: string;
    Source: string;
    Destination: string;
    RoutingKey: string;
    Arguments: TAMQPFieldTable;
  end;

  { Delete: so' o nome. }
  TAMQPRecName = record
    VHost: string;
    Name: string;
  end;

function AmqpEncodeRecExchange(const ARec: TAMQPRecExchange): TBytes;
function AmqpDecodeRecExchange(const APayload: TBytes): TAMQPRecExchange;

function AmqpEncodeRecQueue(const ARec: TAMQPRecQueue): TBytes;
function AmqpDecodeRecQueue(const APayload: TBytes): TAMQPRecQueue;

function AmqpEncodeRecBinding(const ARec: TAMQPRecBinding): TBytes;
function AmqpDecodeRecBinding(const APayload: TBytes): TAMQPRecBinding;

function AmqpEncodeRecName(const ARec: TAMQPRecName): TBytes;
function AmqpDecodeRecName(const APayload: TBytes): TAMQPRecName;

/// True se AKind e' um dos registros de topologia desta unit. A recuperacao
/// (WS6) usa para separar o passe de topologia do de entradas.
function AmqpIsTopoRecord(AKind: Byte): Boolean;

/// Nome legivel do Kind, para mensagem de erro e diagnostico.
function AmqpRecordKindName(AKind: Byte): string;

implementation

function AmqpIsTopoRecord(AKind: Byte): Boolean;
begin
  Result := (AKind >= AMQP_REC_EXCHANGE_DECLARE)
    and (AKind <= AMQP_REC_EXCHANGE_UNBIND);
end;

function AmqpRecordKindName(AKind: Byte): string;
begin
  case AKind of
    AMQP_REC_EXCHANGE_DECLARE: Result := 'exchange.declare';
    AMQP_REC_EXCHANGE_DELETE:  Result := 'exchange.delete';
    AMQP_REC_QUEUE_DECLARE:    Result := 'queue.declare';
    AMQP_REC_QUEUE_DELETE:     Result := 'queue.delete';
    AMQP_REC_QUEUE_BIND:       Result := 'queue.bind';
    AMQP_REC_QUEUE_UNBIND:     Result := 'queue.unbind';
    AMQP_REC_EXCHANGE_BIND:    Result := 'exchange.bind';
    AMQP_REC_EXCHANGE_UNBIND:  Result := 'exchange.unbind';
  else
    Result := Format('desconhecido(%d)', [AKind]);
  end;
end;

// A tabela vazia e' escrita como tabela vazia (quatro bytes de tamanho zero) e
// volta como nil -- assim codificar nil e codificar uma tabela sem chaves dao
// o MESMO byte, e nao ha dois jeitos de dizer "sem argumentos" no arquivo.
procedure EscreveArgs(AWriter: TAMQPWriter; ATable: TAMQPFieldTable);
var
  LVazia: TAMQPFieldTable;
begin
  if ATable <> nil then
    AWriter.WriteFieldTable(ATable)
  else
  begin
    LVazia := TAMQPFieldTable.Create;
    try
      AWriter.WriteFieldTable(LVazia);
    finally
      LVazia.Free;
    end;
  end;
end;

function LeArgs(AReader: TAMQPReader): TAMQPFieldTable;
begin
  Result := AReader.ReadFieldTable;
  if (Result <> nil) and (Result.Count = 0) then
    FreeAndNil(Result);
end;

{ exchange declare }

function AmqpEncodeRecExchange(const ARec: TAMQPRecExchange): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Name);
    LW.WriteShortStr(ARec.ExchangeType);
    LW.WriteBit(ARec.Durable);
    LW.WriteBit(ARec.AutoDelete);
    LW.WriteBit(ARec.Internal);
    EscreveArgs(LW, ARec.Arguments);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecExchange(const APayload: TBytes): TAMQPRecExchange;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Name := LR.ReadShortStr;
    Result.ExchangeType := LR.ReadShortStr;
    Result.Durable := LR.ReadBit;
    Result.AutoDelete := LR.ReadBit;
    Result.Internal := LR.ReadBit;
    Result.Arguments := LeArgs(LR);
  finally
    LR.Free;
  end;
end;

{ queue declare }

function AmqpEncodeRecQueue(const ARec: TAMQPRecQueue): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Name);
    LW.WriteBit(ARec.Durable);
    LW.WriteBit(ARec.AutoDelete);
    EscreveArgs(LW, ARec.Arguments);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecQueue(const APayload: TBytes): TAMQPRecQueue;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Name := LR.ReadShortStr;
    Result.Durable := LR.ReadBit;
    Result.AutoDelete := LR.ReadBit;
    Result.Arguments := LeArgs(LR);
  finally
    LR.Free;
  end;
end;

{ bind / unbind }

function AmqpEncodeRecBinding(const ARec: TAMQPRecBinding): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Source);
    LW.WriteShortStr(ARec.Destination);
    LW.WriteShortStr(ARec.RoutingKey);
    EscreveArgs(LW, ARec.Arguments);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecBinding(const APayload: TBytes): TAMQPRecBinding;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Source := LR.ReadShortStr;
    Result.Destination := LR.ReadShortStr;
    Result.RoutingKey := LR.ReadShortStr;
    Result.Arguments := LeArgs(LR);
  finally
    LR.Free;
  end;
end;

{ delete }

function AmqpEncodeRecName(const ARec: TAMQPRecName): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Name);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecName(const APayload: TBytes): TAMQPRecName;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Name := LR.ReadShortStr;
  finally
    LR.Free;
  end;
end;

end.
