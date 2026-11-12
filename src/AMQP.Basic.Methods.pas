unit AMQP.Basic.Methods;

{$I amqp.inc}

{ Basic class (60): message publishing.

  A published message goes on the wire in three parts:
    1) a METHOD frame with Basic.Publish (BuildBasicPublish);
    2) a CONTENT HEADER frame (BuildContentHeader) with class-id, body-size and
       the properties present;
    3) zero or more CONTENT BODY frames with the body bytes.

  The properties (TAMQPBasicProperties) are all optional; a bitmap
  (property-flags, u16, bit 15 = first property) tells which are
  present. The Flags set controls that — use the setters (SetXxx), which
  fill in the value and set the flag together. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

type
  { Properties in property-flags order (the first takes bit 15). }
  TAMQPBasicProp = (
    bpContentType, bpContentEncoding, bpHeaders, bpDeliveryMode, bpPriority,
    bpCorrelationId, bpReplyTo, bpExpiration, bpMessageId, bpTimestamp,
    bpMsgType, bpUserId, bpAppId, bpClusterId);
  TAMQPBasicProps = set of TAMQPBasicProp;

  TAMQPBasicProperties = record
    Flags: TAMQPBasicProps;      // which properties are present
    ContentType: string;         // e.g. 'application/json'; free-form, not validated by the broker
    ContentEncoding: string;     // e.g. 'gzip'; free-form, not validated by the broker
    Headers: TAMQPFieldTable;    // owner: caller (free if present)
    DeliveryMode: Byte;          // 1 = transient, 2 = persistent
    Priority: Byte;              // 0-9; only has effect if the queue was declared with x-max-priority
    CorrelationId: string;       // correlates request/reply (RPC); echoed by the server in the reply
    ReplyTo: string;             // queue (or 'amq.rabbitmq.reply-to') to send the reply to
    Expiration: string;          // message TTL in ms, as a decimal STRING (e.g. '5000'); not a number
    MessageId: string;           // application id; the lib neither generates nor validates it, opaque to the broker
    Timestamp: UInt64;           // epoch in seconds (not ms); opaque to the broker, informational
    MsgType: string;             // the 'type' property; free-form, for the application
    UserId: string;              // if set, the broker validates it against the connection's authenticated user
    AppId: string;               // free-form, for the application
    ClusterId: string;           // reserved by the protocol; no practical use in RabbitMQ

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

  { Arguments of Basic.Get-Ok (the message itself comes in the content header + body). }
  TAMQPBasicGetOk = record
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    MessageCount: Cardinal;
  end;

  { Arguments of Basic.Consume. }
  TAMQPBasicConsume = record
    Queue: string;
    ConsumerTag: string;    // '' => the server generates one
    NoLocal: Boolean;
    NoAck: Boolean;         // True = no ack (auto); False = manual ack
    Exclusive: Boolean;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable; // may be nil
    class function Create(const AQueue, AConsumerTag: string;
      ANoAck: Boolean = False): TAMQPBasicConsume; static;
  end;

  { Arguments of Basic.Deliver (the message comes in the content header + body). }
  TAMQPBasicDeliver = record
    ConsumerTag: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
  end;

  { Arguments of Basic.Return (the returned message comes in the content header +
    body). Sent by the broker when a `mandatory` publish could not be
    routed to any queue. }
  TAMQPBasicReturn = record
    ReplyCode: Word;
    ReplyText: string;
    Exchange: string;
    RoutingKey: string;
  end;

  { Publish confirmation sent by the broker in `confirm` mode (publisher
    confirms). The broker reuses Basic.Ack/Basic.Nack (the same method IDs the
    client uses to acknowledge consumption), but here DeliveryTag is the publish's
    seq-no and Multiple means "this one and all previous ones still pending".
    They are pure method frames — no content header or body. }
  TAMQPBasicAck = record
    DeliveryTag: UInt64;
    Multiple: Boolean;
  end;

  TAMQPBasicNack = record
    DeliveryTag: UInt64;
    Multiple: Boolean;
    Requeue: Boolean;    // no practical use in confirms; present for symmetry
  end;

function BuildBasicPublish(const AExchange, ARoutingKey: string;
  AMandatory: Boolean = False; AImmediate: Boolean = False): TBytes;

function BuildBasicGet(const AQueue: string; ANoAck: Boolean = True): TBytes;
function DecodeBasicGetOk(const AReader: TAMQPReader): TAMQPBasicGetOk;

function BuildBasicAck(ADeliveryTag: UInt64; AMultiple: Boolean = False): TBytes;
function BuildBasicNack(ADeliveryTag: UInt64; AMultiple: Boolean = False;
  ARequeue: Boolean = True): TBytes;

/// Decodes a Basic.Ack/Basic.Nack coming from the broker (publisher confirms).
/// The method header (class/method) must already have been read from the reader.
function DecodeBasicAck(const AReader: TAMQPReader): TAMQPBasicAck;
function DecodeBasicNack(const AReader: TAMQPReader): TAMQPBasicNack;

/// Confirm.Select (class 85): puts the channel in publisher confirms mode.
function BuildConfirmSelect(ANoWait: Boolean = False): TBytes;

function BuildBasicQos(APrefetchCount: Word; AGlobal: Boolean = False;
  APrefetchSize: Cardinal = 0): TBytes;

function BuildBasicConsume(const AConsume: TAMQPBasicConsume): TBytes;
function DecodeBasicConsumeOk(const AReader: TAMQPReader): string; // consumer-tag

function BuildBasicCancel(const AConsumerTag: string; ANoWait: Boolean = False): TBytes;
function DecodeBasicCancelOk(const AReader: TAMQPReader): string;  // consumer-tag
function DecodeBasicCancel(const AReader: TAMQPReader): string;    // server -> client

function DecodeBasicDeliver(const AReader: TAMQPReader): TAMQPBasicDeliver;

function DecodeBasicReturn(const AReader: TAMQPReader): TAMQPBasicReturn;

/// Payload of the content header frame (type 2). ABodySize = total body size.
function BuildContentHeader(ABodySize: UInt64;
  const AProps: TAMQPBasicProperties): TBytes; overload;

/// The same, with the W3C trace context in the headers: 'traceparent' (and
/// 'tracestate', when not empty) as longstr at the end of the table. The caller's
/// table is NOT changed — the pairs go only into the bytes. If the table already has
/// 'traceparent', the publisher decided the context and nothing is added.
/// Empty ATraceParent = the first form.
function BuildContentHeader(ABodySize: UInt64; const AProps: TAMQPBasicProperties;
  const ATraceParent, ATraceState: string): TBytes; overload;

/// Reads an already decoded content header from the payload of a type 2 frame.
/// If the properties include Headers, the caller owns that table.
function DecodeContentHeader(const AReader: TAMQPReader): TAMQPContentHeader;

// =========================================================================
//  Server side (broker sub-module) — decode of the client->server methods
//  and build of the server->client methods. DecodeBasicAck/DecodeBasicNack and
//  BuildBasicAck/BuildBasicNack above already serve both directions (the client
//  uses them for consumption; the broker, for publisher confirms).
//  In decoded records with an Arguments/Headers field the table is new and
//  belongs to the caller (who must free it).
// =========================================================================

type
  { Basic.Qos (client -> server). }
  TAMQPBasicQosArgs = record
    PrefetchSize: Cardinal;
    PrefetchCount: Word;
    Global: Boolean;
  end;

  { Basic.Publish (client -> server) — the method header; the content
    (content header + body) comes in the following frames. }
  TAMQPBasicPublish = record
    Exchange: string;
    RoutingKey: string;
    Mandatory: Boolean;
    Immediate: Boolean;
  end;

  { Basic.Get (client -> server). }
  TAMQPBasicGet = record
    Queue: string;
    NoAck: Boolean;
  end;

  { Basic.Cancel (client -> server) — with the no-wait, unlike the
    legacy DecodeBasicCancel (used in the server->client consumer-cancel-notify,
    which only wants the consumer-tag). }
  TAMQPBasicCancelArgs = record
    ConsumerTag: string;
    NoWait: Boolean;
  end;

  { Basic.Reject (client -> server) — rejects ONE delivery (no 'multiple',
    unlike Nack). }
  TAMQPBasicReject = record
    DeliveryTag: UInt64;
    Requeue: Boolean;
  end;

function DecodeBasicQos(const AReader: TAMQPReader): TAMQPBasicQosArgs;
function BuildBasicQosOk: TBytes;

function DecodeBasicPublish(const AReader: TAMQPReader): TAMQPBasicPublish;

function DecodeBasicConsume(const AReader: TAMQPReader): TAMQPBasicConsume;
function BuildBasicConsumeOk(const AConsumerTag: string): TBytes;

function DecodeBasicCancelArgs(const AReader: TAMQPReader): TAMQPBasicCancelArgs;
function BuildBasicCancelOk(const AConsumerTag: string): TBytes;

function DecodeBasicGet(const AReader: TAMQPReader): TAMQPBasicGet;
function BuildBasicGetOk(const AGetOk: TAMQPBasicGetOk): TBytes;
function BuildBasicGetEmpty: TBytes;

function BuildBasicDeliver(const ADeliver: TAMQPBasicDeliver): TBytes;
function BuildBasicReturn(const AReturn: TAMQPBasicReturn): TBytes;

function DecodeBasicReject(const AReader: TAMQPReader): TAMQPBasicReject;
function BuildBasicReject(ADeliveryTag: UInt64; ARequeue: Boolean = True): TBytes;

/// Confirm.Select (class 85, client -> server) — returns the no-wait.
function DecodeConfirmSelect(const AReader: TAMQPReader): Boolean;
function BuildConfirmSelectOk: TBytes;

implementation

// property-flags bit for a property: the first (Ord=0) is bit 15.
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
  Result := AReader.ReadShortStr; // consumer-tag (the no-wait bit that follows is ignored)
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
begin
  Result := BuildContentHeader(ABodySize, AProps, '', '');
end;

function BuildContentHeader(ABodySize: UInt64; const AProps: TAMQPBasicProperties;
  const ATraceParent, ATraceState: string): TBytes;
var
  W: TAMQPWriter;
  LFlags: Word;
  P: TAMQPBasicProp;
  LHeaders: TAMQPFieldTable;
  LInject: Boolean;
begin
  LHeaders := nil;
  if bpHeaders in AProps.Flags then
    LHeaders := AProps.Headers;
  LInject := (ATraceParent <> '') and
    not (Assigned(LHeaders) and LHeaders.ContainsKey('traceparent'));

  W := TAMQPWriter.Create;
  try
    W.WriteShortUInt(AMQP_CLASS_BASIC); // class-id
    W.WriteShortUInt(0);                // weight (always 0)
    W.WriteLongLongUInt(ABodySize);

    LFlags := 0;
    for P in AProps.Flags do
      LFlags := LFlags or PropBit(P);
    if LInject then
      LFlags := LFlags or PropBit(bpHeaders);
    W.WriteShortUInt(LFlags);

    // Properties present, in enum order.
    if bpContentType in AProps.Flags then W.WriteShortStr(AProps.ContentType);
    if bpContentEncoding in AProps.Flags then W.WriteShortStr(AProps.ContentEncoding);
    if LInject then
    begin
      // a stray tracestate in the table (without traceparent) stays the caller's:
      // a field-table does not allow a repeated name.
      if (ATraceState <> '') and
        not (Assigned(LHeaders) and LHeaders.ContainsKey('tracestate')) then
        W.WriteFieldTable(LHeaders, ['traceparent', 'tracestate'],
          [ATraceParent, ATraceState])
      else
        W.WriteFieldTable(LHeaders, ['traceparent'], [ATraceParent]);
    end
    else if bpHeaders in AProps.Flags then
      W.WriteFieldTable(AProps.Headers);
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
  // Continuation words (bit 0). No Basic property lives beyond the
  // first word, so we just consume and ignore the extra ones.
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

{ ===================================================================== }
{  Server side                                                          }
{ ===================================================================== }

function DecodeBasicQos(const AReader: TAMQPReader): TAMQPBasicQosArgs;
begin
  Result.PrefetchSize := AReader.ReadLongUInt;
  Result.PrefetchCount := AReader.ReadShortUInt;
  Result.Global := AReader.ReadBit;
end;

function BuildBasicQosOk: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_QOS_OK);
  try
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicPublish(const AReader: TAMQPReader): TAMQPBasicPublish;
begin
  AReader.ReadShortUInt; // reserved-1 (ticket)
  Result.Exchange := AReader.ReadShortStr;
  Result.RoutingKey := AReader.ReadShortStr;
  Result.Mandatory := AReader.ReadBit;
  Result.Immediate := AReader.ReadBit;
end;

function DecodeBasicConsume(const AReader: TAMQPReader): TAMQPBasicConsume;
begin
  Result := Default(TAMQPBasicConsume);
  AReader.ReadShortUInt; // reserved-1
  Result.Queue := AReader.ReadShortStr;
  Result.ConsumerTag := AReader.ReadShortStr;
  Result.NoLocal := AReader.ReadBit;
  Result.NoAck := AReader.ReadBit;
  Result.Exclusive := AReader.ReadBit;
  Result.NoWait := AReader.ReadBit;
  Result.Arguments := AReader.ReadFieldTable; // owner: caller
end;

function BuildBasicConsumeOk(const AConsumerTag: string): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK);
  try
    W.WriteShortStr(AConsumerTag);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicCancelArgs(const AReader: TAMQPReader): TAMQPBasicCancelArgs;
begin
  Result.ConsumerTag := AReader.ReadShortStr;
  Result.NoWait := AReader.ReadBit;
end;

function BuildBasicCancelOk(const AConsumerTag: string): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL_OK);
  try
    W.WriteShortStr(AConsumerTag);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicGet(const AReader: TAMQPReader): TAMQPBasicGet;
begin
  AReader.ReadShortUInt; // reserved-1
  Result.Queue := AReader.ReadShortStr;
  Result.NoAck := AReader.ReadBit;
end;

function BuildBasicGetOk(const AGetOk: TAMQPBasicGetOk): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK);
  try
    W.WriteLongLongUInt(AGetOk.DeliveryTag);
    W.WriteBit(AGetOk.Redelivered);
    W.WriteShortStr(AGetOk.Exchange);
    W.WriteShortStr(AGetOk.RoutingKey);
    W.WriteLongUInt(AGetOk.MessageCount);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildBasicGetEmpty: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_GET_EMPTY);
  try
    W.WriteShortStr(''); // reserved-1 (cluster-id) — obsolete
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildBasicDeliver(const ADeliver: TAMQPBasicDeliver): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER);
  try
    W.WriteShortStr(ADeliver.ConsumerTag);
    W.WriteLongLongUInt(ADeliver.DeliveryTag);
    W.WriteBit(ADeliver.Redelivered);
    W.WriteShortStr(ADeliver.Exchange);
    W.WriteShortStr(ADeliver.RoutingKey);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildBasicReturn(const AReturn: TAMQPBasicReturn): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN);
  try
    W.WriteShortUInt(AReturn.ReplyCode);
    W.WriteShortStr(AReturn.ReplyText);
    W.WriteShortStr(AReturn.Exchange);
    W.WriteShortStr(AReturn.RoutingKey);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeBasicReject(const AReader: TAMQPReader): TAMQPBasicReject;
begin
  Result.DeliveryTag := AReader.ReadLongLongUInt;
  Result.Requeue := AReader.ReadBit;
end;

function BuildBasicReject(ADeliveryTag: UInt64; ARequeue: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_BASIC, AMQP_BASIC_REJECT);
  try
    W.WriteLongLongUInt(ADeliveryTag);
    W.WriteBit(ARequeue);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeConfirmSelect(const AReader: TAMQPReader): Boolean;
begin
  Result := AReader.ReadBit; // no-wait
end;

function BuildConfirmSelectOk: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT_OK);
  try
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

end.
