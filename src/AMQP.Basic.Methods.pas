unit AMQP.Basic.Methods;

{$I amqp.inc}

{ Classe Basic (60): publicação de mensagens.

  Uma mensagem publicada vai em três partes no wire:
    1) um frame de MÉTODO com Basic.Publish (BuildBasicPublish);
    2) um frame de CONTENT HEADER (BuildContentHeader) com class-id, body-size e
       as propriedades presentes;
    3) zero ou mais frames de CONTENT BODY com os bytes do corpo.

  As propriedades (TAMQPBasicProperties) são todas opcionais; um bitmap
  (property-flags, u16, bit 15 = primeira propriedade) indica quais estão
  presentes. O conjunto Flags controla isso — use os setters (SetXxx), que
  preenchem o valor e marcam o flag juntos. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

type
  { Propriedades na ordem do property-flags (a primeira ocupa o bit 15). }
  TAMQPBasicProp = (
    bpContentType, bpContentEncoding, bpHeaders, bpDeliveryMode, bpPriority,
    bpCorrelationId, bpReplyTo, bpExpiration, bpMessageId, bpTimestamp,
    bpMsgType, bpUserId, bpAppId, bpClusterId);
  TAMQPBasicProps = set of TAMQPBasicProp;

  TAMQPBasicProperties = record
    Flags: TAMQPBasicProps;      // quais propriedades estão presentes
    ContentType: string;         // ex. 'application/json'; livre, não validado pelo broker
    ContentEncoding: string;     // ex. 'gzip'; livre, não validado pelo broker
    Headers: TAMQPFieldTable;    // dono: chamador (liberar se presente)
    DeliveryMode: Byte;          // 1 = transiente, 2 = persistente
    Priority: Byte;              // 0-9; só tem efeito se a fila foi declarada com x-max-priority
    CorrelationId: string;       // correlaciona request/reply (RPC); ecoada pelo servidor na resposta
    ReplyTo: string;             // fila (ou 'amq.rabbitmq.reply-to') para onde enviar a resposta
    Expiration: string;          // TTL da mensagem em ms, como STRING decimal (ex. '5000'); não é número
    MessageId: string;           // id de aplicação; a lib não gera nem valida, é opaco pro broker
    Timestamp: UInt64;           // epoch em segundos (não ms); opaco pro broker, informativo
    MsgType: string;             // a propriedade 'type'; livre, uso da aplicação
    UserId: string;              // se setado, o broker valida contra o usuário autenticado da conexão
    AppId: string;               // livre, uso da aplicação
    ClusterId: string;           // reservado pelo protocolo; sem uso prático no RabbitMQ

    procedure SetContentType(const AValue: string);
    procedure SetContentEncoding(const AValue: string);
    procedure SetHeaders(AValue: TAMQPFieldTable);
    procedure SetDeliveryMode(AValue: Byte);
    procedure SetPersistent;                 // DeliveryMode := 2
    procedure SetPriority(AValue: Byte);
    procedure SetCorrelationId(const AValue: string);
    procedure SetReplyTo(const AValue: string);
    procedure SetExpiration(const AValue: string);
    procedure SetMessageId(const AValue: string);
    procedure SetTimestamp(AValue: UInt64);
    procedure SetMsgType(const AValue: string);
    procedure SetUserId(const AValue: string);
    procedure SetAppId(const AValue: string);
    procedure SetClusterId(const AValue: string);

    function Has(AProp: TAMQPBasicProp): Boolean;
    class function Empty: TAMQPBasicProperties; static;
  end;

  TAMQPContentHeader = record
    BodySize: UInt64;
    Properties: TAMQPBasicProperties;
  end;

  { Argumentos de Basic.Get-Ok (a mensagem em si vem no content header + body). }
  TAMQPBasicGetOk = record
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    MessageCount: Cardinal;
  end;

  { Argumentos de Basic.Consume. }
  TAMQPBasicConsume = record
    Queue: string;
    ConsumerTag: string;    // '' => o servidor gera
    NoLocal: Boolean;
    NoAck: Boolean;         // True = sem ack (auto); False = ack manual
    Exclusive: Boolean;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable; // pode ser nil
    class function Create(const AQueue, AConsumerTag: string;
      ANoAck: Boolean = False): TAMQPBasicConsume; static;
  end;

  { Argumentos de Basic.Deliver (a mensagem vem no content header + body). }
  TAMQPBasicDeliver = record
    ConsumerTag: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
  end;

  { Argumentos de Basic.Return (a mensagem devolvida vem no content header +
    body). Enviado pelo broker quando um publish `mandatory` não pôde ser
    roteado a nenhuma fila. }
  TAMQPBasicReturn = record
    ReplyCode: Word;
    ReplyText: string;
    Exchange: string;
    RoutingKey: string;
  end;

  { Confirmação de publish enviada pelo broker no modo `confirm` (publisher
    confirms). O broker reusa Basic.Ack/Basic.Nack (mesmos IDs de método que o
    cliente usa para confirmar consumo), mas aqui DeliveryTag é o seq-no do
    publish e Multiple significa "este e todos os anteriores ainda pendentes".
    São frames de método puros — sem content header nem body. }
  TAMQPBasicAck = record
    DeliveryTag: UInt64;
    Multiple: Boolean;
  end;

  TAMQPBasicNack = record
    DeliveryTag: UInt64;
    Multiple: Boolean;
    Requeue: Boolean;    // sem uso prático em confirms; presente por simetria
  end;

function BuildBasicPublish(const AExchange, ARoutingKey: string;
  AMandatory: Boolean = False; AImmediate: Boolean = False): TBytes;

function BuildBasicGet(const AQueue: string; ANoAck: Boolean = True): TBytes;
function DecodeBasicGetOk(const AReader: TAMQPReader): TAMQPBasicGetOk;

function BuildBasicAck(ADeliveryTag: UInt64; AMultiple: Boolean = False): TBytes;
function BuildBasicNack(ADeliveryTag: UInt64; AMultiple: Boolean = False;
  ARequeue: Boolean = True): TBytes;

/// Decodifica um Basic.Ack/Basic.Nack vindo do broker (publisher confirms).
/// O cabeçalho de método (class/method) já deve ter sido lido do reader.
function DecodeBasicAck(const AReader: TAMQPReader): TAMQPBasicAck;
function DecodeBasicNack(const AReader: TAMQPReader): TAMQPBasicNack;

/// Confirm.Select (classe 85): coloca o canal em modo publisher confirms.
function BuildConfirmSelect(ANoWait: Boolean = False): TBytes;

function BuildBasicQos(APrefetchCount: Word; AGlobal: Boolean = False;
  APrefetchSize: Cardinal = 0): TBytes;

function BuildBasicConsume(const AConsume: TAMQPBasicConsume): TBytes;
function DecodeBasicConsumeOk(const AReader: TAMQPReader): string; // consumer-tag

function BuildBasicCancel(const AConsumerTag: string; ANoWait: Boolean = False): TBytes;
function DecodeBasicCancelOk(const AReader: TAMQPReader): string;  // consumer-tag
function DecodeBasicCancel(const AReader: TAMQPReader): string;    // servidor -> cliente

function DecodeBasicDeliver(const AReader: TAMQPReader): TAMQPBasicDeliver;

function DecodeBasicReturn(const AReader: TAMQPReader): TAMQPBasicReturn;

/// Payload do frame de content header (tipo 2). ABodySize = total do corpo.
function BuildContentHeader(ABodySize: UInt64;
  const AProps: TAMQPBasicProperties): TBytes;

/// Lê um content header já decodificado do payload de um frame tipo 2.
/// Se as propriedades incluírem Headers, o chamador é dono dessa tabela.
function DecodeContentHeader(const AReader: TAMQPReader): TAMQPContentHeader;

implementation

// Bit da property-flags para uma propriedade: a primeira (Ord=0) é o bit 15.
function PropBit(AProp: TAMQPBasicProp): Word;
begin
  Result := Word(1 shl (15 - Ord(AProp)));
end;

{ TAMQPBasicProperties }

class function TAMQPBasicProperties.Empty: TAMQPBasicProperties;
begin
  Result := Default(TAMQPBasicProperties);
end;

function TAMQPBasicProperties.Has(AProp: TAMQPBasicProp): Boolean;
begin
  Result := AProp in Flags;
end;

procedure TAMQPBasicProperties.SetContentType(const AValue: string);
begin
  ContentType := AValue;
  Include(Flags, bpContentType);
end;

procedure TAMQPBasicProperties.SetContentEncoding(const AValue: string);
begin
  ContentEncoding := AValue;
  Include(Flags, bpContentEncoding);
end;

procedure TAMQPBasicProperties.SetHeaders(AValue: TAMQPFieldTable);
begin
  Headers := AValue;
  Include(Flags, bpHeaders);
end;

procedure TAMQPBasicProperties.SetDeliveryMode(AValue: Byte);
begin
  DeliveryMode := AValue;
  Include(Flags, bpDeliveryMode);
end;

procedure TAMQPBasicProperties.SetPersistent;
begin
  SetDeliveryMode(2);
end;

procedure TAMQPBasicProperties.SetPriority(AValue: Byte);
begin
  Priority := AValue;
  Include(Flags, bpPriority);
end;

procedure TAMQPBasicProperties.SetCorrelationId(const AValue: string);
begin
  CorrelationId := AValue;
  Include(Flags, bpCorrelationId);
end;

procedure TAMQPBasicProperties.SetReplyTo(const AValue: string);
begin
  ReplyTo := AValue;
  Include(Flags, bpReplyTo);
end;

procedure TAMQPBasicProperties.SetExpiration(const AValue: string);
begin
  Expiration := AValue;
  Include(Flags, bpExpiration);
end;

procedure TAMQPBasicProperties.SetMessageId(const AValue: string);
begin
  MessageId := AValue;
  Include(Flags, bpMessageId);
end;

procedure TAMQPBasicProperties.SetTimestamp(AValue: UInt64);
begin
  Timestamp := AValue;
  Include(Flags, bpTimestamp);
end;

procedure TAMQPBasicProperties.SetMsgType(const AValue: string);
begin
  MsgType := AValue;
  Include(Flags, bpMsgType);
end;

procedure TAMQPBasicProperties.SetUserId(const AValue: string);
begin
  UserId := AValue;
  Include(Flags, bpUserId);
end;

procedure TAMQPBasicProperties.SetAppId(const AValue: string);
begin
  AppId := AValue;
  Include(Flags, bpAppId);
end;

procedure TAMQPBasicProperties.SetClusterId(const AValue: string);
begin
  ClusterId := AValue;
  Include(Flags, bpClusterId);
end;

{ Basic.Publish }

function BuildBasicPublish(const AExchange, ARoutingKey: string;
  AMandatory, AImmediate: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH);
  try
    W.WriteShortUInt(0); // reserved-1 (ticket)
    W.WriteShortStr(AExchange);
    W.WriteShortStr(ARoutingKey);
    W.WriteBit(AMandatory);
    W.WriteBit(AImmediate);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildBasicGet(const AQueue: string; ANoAck: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_GET);
  try
    W.WriteShortUInt(0); // reserved-1 (ticket)
    W.WriteShortStr(AQueue);
    W.WriteBit(ANoAck);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicGetOk(const AReader: TAMQPReader): TAMQPBasicGetOk;
begin
  Result.DeliveryTag := AReader.ReadLongLongUInt;
  Result.Redelivered := AReader.ReadBit;
  Result.Exchange := AReader.ReadShortStr;
  Result.RoutingKey := AReader.ReadShortStr;
  Result.MessageCount := AReader.ReadLongUInt;
end;

function BuildBasicAck(ADeliveryTag: UInt64; AMultiple: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_ACK);
  try
    W.WriteLongLongUInt(ADeliveryTag);
    W.WriteBit(AMultiple);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildBasicNack(ADeliveryTag: UInt64; AMultiple, ARequeue: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_NACK);
  try
    W.WriteLongLongUInt(ADeliveryTag);
    W.WriteBit(AMultiple);
    W.WriteBit(ARequeue);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicAck(const AReader: TAMQPReader): TAMQPBasicAck;
begin
  Result.DeliveryTag := AReader.ReadLongLongUInt;
  Result.Multiple := AReader.ReadBit;
end;

function DecodeBasicNack(const AReader: TAMQPReader): TAMQPBasicNack;
begin
  Result.DeliveryTag := AReader.ReadLongLongUInt;
  Result.Multiple := AReader.ReadBit;
  Result.Requeue := AReader.ReadBit;
end;

function BuildConfirmSelect(ANoWait: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT);
  try
    W.WriteBit(ANoWait);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildBasicQos(APrefetchCount: Word; AGlobal: Boolean;
  APrefetchSize: Cardinal): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_QOS);
  try
    W.WriteLongUInt(APrefetchSize);
    W.WriteShortUInt(APrefetchCount);
    W.WriteBit(AGlobal);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

{ TAMQPBasicConsume }

class function TAMQPBasicConsume.Create(const AQueue, AConsumerTag: string;
  ANoAck: Boolean): TAMQPBasicConsume;
begin
  Result := Default(TAMQPBasicConsume);
  Result.Queue := AQueue;
  Result.ConsumerTag := AConsumerTag;
  Result.NoAck := ANoAck;
end;

function BuildBasicConsume(const AConsume: TAMQPBasicConsume): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME);
  try
    W.WriteShortUInt(0); // reserved-1 (ticket)
    W.WriteShortStr(AConsume.Queue);
    W.WriteShortStr(AConsume.ConsumerTag);
    W.WriteBit(AConsume.NoLocal);
    W.WriteBit(AConsume.NoAck);
    W.WriteBit(AConsume.Exclusive);
    W.WriteBit(AConsume.NoWait);
    W.WriteFieldTable(AConsume.Arguments);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicConsumeOk(const AReader: TAMQPReader): string;
begin
  Result := AReader.ReadShortStr; // consumer-tag
end;

function BuildBasicCancel(const AConsumerTag: string; ANoWait: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL);
  try
    W.WriteShortStr(AConsumerTag);
    W.WriteBit(ANoWait);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicCancelOk(const AReader: TAMQPReader): string;
begin
  Result := AReader.ReadShortStr;
end;

function DecodeBasicCancel(const AReader: TAMQPReader): string;
begin
  Result := AReader.ReadShortStr; // consumer-tag (o no-wait bit que segue é ignorado)
end;

function DecodeBasicDeliver(const AReader: TAMQPReader): TAMQPBasicDeliver;
begin
  Result.ConsumerTag := AReader.ReadShortStr;
  Result.DeliveryTag := AReader.ReadLongLongUInt;
  Result.Redelivered := AReader.ReadBit;
  Result.Exchange := AReader.ReadShortStr;
  Result.RoutingKey := AReader.ReadShortStr;
end;

function DecodeBasicReturn(const AReader: TAMQPReader): TAMQPBasicReturn;
begin
  Result.ReplyCode := AReader.ReadShortUInt;
  Result.ReplyText := AReader.ReadShortStr;
  Result.Exchange := AReader.ReadShortStr;
  Result.RoutingKey := AReader.ReadShortStr;
end;

{ Content header }

function BuildContentHeader(ABodySize: UInt64;
  const AProps: TAMQPBasicProperties): TBytes;
var
  W: TAMQPWriter;
  LFlags: Word;
  P: TAMQPBasicProp;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteShortUInt(AMQP_CLASS_BASIC); // class-id
    W.WriteShortUInt(0);                // weight (sempre 0)
    W.WriteLongLongUInt(ABodySize);

    LFlags := 0;
    for P in AProps.Flags do
      LFlags := LFlags or PropBit(P);
    W.WriteShortUInt(LFlags);

    // Propriedades presentes, na ordem do enum.
    if bpContentType in AProps.Flags then W.WriteShortStr(AProps.ContentType);
    if bpContentEncoding in AProps.Flags then W.WriteShortStr(AProps.ContentEncoding);
    if bpHeaders in AProps.Flags then W.WriteFieldTable(AProps.Headers);
    if bpDeliveryMode in AProps.Flags then W.WriteOctet(AProps.DeliveryMode);
    if bpPriority in AProps.Flags then W.WriteOctet(AProps.Priority);
    if bpCorrelationId in AProps.Flags then W.WriteShortStr(AProps.CorrelationId);
    if bpReplyTo in AProps.Flags then W.WriteShortStr(AProps.ReplyTo);
    if bpExpiration in AProps.Flags then W.WriteShortStr(AProps.Expiration);
    if bpMessageId in AProps.Flags then W.WriteShortStr(AProps.MessageId);
    if bpTimestamp in AProps.Flags then W.WriteTimestamp(AProps.Timestamp);
    if bpMsgType in AProps.Flags then W.WriteShortStr(AProps.MsgType);
    if bpUserId in AProps.Flags then W.WriteShortStr(AProps.UserId);
    if bpAppId in AProps.Flags then W.WriteShortStr(AProps.AppId);
    if bpClusterId in AProps.Flags then W.WriteShortStr(AProps.ClusterId);

    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeContentHeader(const AReader: TAMQPReader): TAMQPContentHeader;
var
  LFlags, LMore: Word;
begin
  Result.Properties := TAMQPBasicProperties.Empty;

  AReader.ReadShortUInt;                       // class-id (60)
  AReader.ReadShortUInt;                       // weight
  Result.BodySize := AReader.ReadLongLongUInt;

  LFlags := AReader.ReadShortUInt;
  // Palavras de continuação (bit 0). Nenhuma propriedade de Basic vive além da
  // primeira palavra, então apenas consumimos e ignoramos as extras.
  LMore := LFlags;
  while (LMore and 1) <> 0 do
    LMore := AReader.ReadShortUInt;

  if (LFlags and PropBit(bpContentType)) <> 0 then
    Result.Properties.SetContentType(AReader.ReadShortStr);
  if (LFlags and PropBit(bpContentEncoding)) <> 0 then
    Result.Properties.SetContentEncoding(AReader.ReadShortStr);
  if (LFlags and PropBit(bpHeaders)) <> 0 then
    Result.Properties.SetHeaders(AReader.ReadFieldTable);
  if (LFlags and PropBit(bpDeliveryMode)) <> 0 then
    Result.Properties.SetDeliveryMode(AReader.ReadOctet);
  if (LFlags and PropBit(bpPriority)) <> 0 then
    Result.Properties.SetPriority(AReader.ReadOctet);
  if (LFlags and PropBit(bpCorrelationId)) <> 0 then
    Result.Properties.SetCorrelationId(AReader.ReadShortStr);
  if (LFlags and PropBit(bpReplyTo)) <> 0 then
    Result.Properties.SetReplyTo(AReader.ReadShortStr);
  if (LFlags and PropBit(bpExpiration)) <> 0 then
    Result.Properties.SetExpiration(AReader.ReadShortStr);
  if (LFlags and PropBit(bpMessageId)) <> 0 then
    Result.Properties.SetMessageId(AReader.ReadShortStr);
  if (LFlags and PropBit(bpTimestamp)) <> 0 then
    Result.Properties.SetTimestamp(AReader.ReadTimestamp);
  if (LFlags and PropBit(bpMsgType)) <> 0 then
    Result.Properties.SetMsgType(AReader.ReadShortStr);
  if (LFlags and PropBit(bpUserId)) <> 0 then
    Result.Properties.SetUserId(AReader.ReadShortStr);
  if (LFlags and PropBit(bpAppId)) <> 0 then
    Result.Properties.SetAppId(AReader.ReadShortStr);
  if (LFlags and PropBit(bpClusterId)) <> 0 then
    Result.Properties.SetClusterId(AReader.ReadShortStr);
end;

end.
