unit AMQP.Item2MethodsTests;

{ Testes do codec do item 2: métodos de channel/exchange/queue/basic e o
  content header (property-flags). Sem broker. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Rtti,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Channel.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  [TestFixture]
  TChannelQueueExchangeTests = class
  public
    [Test] procedure ChannelOpen_RoundTrip;
    [Test] procedure ChannelClose_RoundTrip;
    [Test] procedure ExchangeDeclare_RoundTrip;
    [Test] procedure ExchangeBind_RoundTrip;
    [Test] procedure ExchangeUnbind_RoundTrip;
    [Test] procedure QueueDeclare_RoundTrip;
    [Test] procedure QueueDeclareOk_Decode;
    [Test] procedure QueueBind_RoundTrip;
    [Test] procedure QueueUnbind_RoundTrip;
    [Test] procedure BasicPublish_RoundTrip;
    [Test] procedure BasicGet_RoundTrip;
    [Test] procedure BasicGetOk_Decode;
    [Test] procedure BasicAck_RoundTrip;
    [Test] procedure BasicNack_RoundTrip;
    [Test] procedure BasicQos_RoundTrip;
    [Test] procedure BasicConsume_RoundTrip;
    [Test] procedure BasicConsumeOk_Decode;
    [Test] procedure BasicDeliver_Decode;
    [Test] procedure BasicReturn_Decode;
    [Test] procedure ConfirmSelect_RoundTrip;
    [Test] procedure BasicAckConfirm_Decode;
    [Test] procedure BasicNackConfirm_Decode;
  end;

  [TestFixture]
  TContentHeaderTests = class
  public
    [Test] procedure PropertyFlags_ContentTypeEDeliveryMode;
    [Test] procedure RoundTrip_VariasPropriedades;
    [Test] procedure Persistente_DeliveryMode2;
    [Test] procedure Headers_RoundTrip;
    [Test] procedure SemPropriedades_FlagsZero;
  end;

implementation

{ TChannelQueueExchangeTests }

procedure TChannelQueueExchangeTests.ChannelOpen_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildChannelOpen);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN));
    Assert.AreEqual('', R.ReadShortStr, 'reserved-1');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.ChannelClose_RoundTrip;
var
  LClose, LDecoded: TAMQPCloseInfo;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LClose.ReplyCode := 404;
  LClose.ReplyText := 'NOT_FOUND';
  LClose.ClassId := AMQP_CLASS_QUEUE;
  LClose.MethodId := AMQP_QUEUE_DECLARE;

  R := TAMQPReader.Create(BuildChannelClose(LClose));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE));
    LDecoded := DecodeChannelClose(R);
    Assert.AreEqual(Word(404), LDecoded.ReplyCode);
    Assert.AreEqual('NOT_FOUND', LDecoded.ReplyText);
    Assert.AreEqual(Word(AMQP_CLASS_QUEUE), LDecoded.ClassId);
    Assert.AreEqual(Word(AMQP_QUEUE_DECLARE), LDecoded.MethodId);
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.ExchangeDeclare_RoundTrip;
var
  LDeclare: TAMQPExchangeDeclare;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LDeclare := TAMQPExchangeDeclare.Create('nfe.exchange',
    AMQP_EXCHANGE_TYPE_TOPIC, True);
  LDeclare.AutoDelete := True;

  R := TAMQPReader.Create(BuildExchangeDeclare(LDeclare));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.exchange', R.ReadShortStr, 'exchange');
    Assert.AreEqual('topic', R.ReadShortStr, 'type');
    Assert.IsFalse(R.ReadBit, 'passive');
    Assert.IsTrue(R.ReadBit, 'durable');
    Assert.IsTrue(R.ReadBit, 'auto-delete');
    Assert.IsFalse(R.ReadBit, 'internal');
    Assert.IsFalse(R.ReadBit, 'no-wait');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.ExchangeBind_RoundTrip;
var
  LBind: TAMQPExchangeBinding;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LArgs: TAMQPFieldTable;
begin
  LBind := Default(TAMQPExchangeBinding);
  LBind.Destination := 'nfe.dest';
  LBind.Source := 'nfe.source';
  LBind.RoutingKey := 'resposta.*';

  R := TAMQPReader.Create(BuildExchangeBind(LBind));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_BIND));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.dest', R.ReadShortStr, 'destination');
    Assert.AreEqual('nfe.source', R.ReadShortStr, 'source');
    Assert.AreEqual('resposta.*', R.ReadShortStr, 'routing-key');
    Assert.IsFalse(R.ReadBit, 'no-wait');
    LArgs := R.ReadFieldTable;
    try
      Assert.AreEqual(0, LArgs.Count, 'arguments vazio');
    finally
      LArgs.Free;
    end;
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.ExchangeUnbind_RoundTrip;
var
  LUnbind: TAMQPExchangeBinding;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LUnbind := Default(TAMQPExchangeBinding);
  LUnbind.Destination := 'nfe.dest';
  LUnbind.Source := 'nfe.source';
  LUnbind.RoutingKey := 'resposta.*';

  R := TAMQPReader.Create(BuildExchangeUnbind(LUnbind));
  try
    LId := ReadMethodHeader(R);
    // Mesmo layout do bind, mas com o method-id de unbind (40).
    Assert.IsTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_UNBIND));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.dest', R.ReadShortStr, 'destination');
    Assert.AreEqual('nfe.source', R.ReadShortStr, 'source');
    Assert.AreEqual('resposta.*', R.ReadShortStr, 'routing-key');
    Assert.IsFalse(R.ReadBit, 'no-wait');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.QueueDeclare_RoundTrip;
var
  LDeclare: TAMQPQueueDeclare;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LDeclare := TAMQPQueueDeclare.Create('nfe.respostas', True);

  R := TAMQPReader.Create(BuildQueueDeclare(LDeclare));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.respostas', R.ReadShortStr, 'queue');
    Assert.IsFalse(R.ReadBit, 'passive');
    Assert.IsTrue(R.ReadBit, 'durable');
    Assert.IsFalse(R.ReadBit, 'exclusive');
    Assert.IsFalse(R.ReadBit, 'auto-delete');
    Assert.IsFalse(R.ReadBit, 'no-wait');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.QueueDeclareOk_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LOk: TAMQPQueueDeclareOk;
begin
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK);
    W.WriteShortStr('nfe.respostas');
    W.WriteLongUInt(7);   // message-count
    W.WriteLongUInt(2);   // consumer-count
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK));
      LOk := DecodeQueueDeclareOk(R);
      Assert.AreEqual('nfe.respostas', LOk.QueueName);
      Assert.AreEqual(Cardinal(7), LOk.MessageCount);
      Assert.AreEqual(Cardinal(2), LOk.ConsumerCount);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TChannelQueueExchangeTests.QueueBind_RoundTrip;
var
  LBind: TAMQPQueueBind;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := 'nfe.respostas';
  LBind.ExchangeName := 'nfe.exchange';
  LBind.RoutingKey := 'resposta.autorizada';

  R := TAMQPReader.Create(BuildQueueBind(LBind));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_BIND));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.respostas', R.ReadShortStr, 'queue');
    Assert.AreEqual('nfe.exchange', R.ReadShortStr, 'exchange');
    Assert.AreEqual('resposta.autorizada', R.ReadShortStr, 'routing-key');
    Assert.IsFalse(R.ReadBit, 'no-wait');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.QueueUnbind_RoundTrip;
var
  LUnbind: TAMQPQueueUnbind;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LArgs: TAMQPFieldTable;
begin
  LUnbind := Default(TAMQPQueueUnbind);
  LUnbind.QueueName := 'nfe.respostas';
  LUnbind.ExchangeName := 'nfe.exchange';
  LUnbind.RoutingKey := 'resposta.autorizada';

  R := TAMQPReader.Create(BuildQueueUnbind(LUnbind));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_UNBIND));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.respostas', R.ReadShortStr, 'queue');
    Assert.AreEqual('nfe.exchange', R.ReadShortStr, 'exchange');
    Assert.AreEqual('resposta.autorizada', R.ReadShortStr, 'routing-key');
    // Diferente de bind: unbind NÃO tem no-wait — vem a field-table direto.
    LArgs := R.ReadFieldTable;
    try
      Assert.AreEqual(0, LArgs.Count, 'arguments vazio');
    finally
      LArgs.Free;
    end;
    Assert.IsTrue(R.EndOfData, 'field-table encerra o payload');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicPublish_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildBasicPublish('nfe.exchange', 'resposta.autorizada', True, False));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.exchange', R.ReadShortStr, 'exchange');
    Assert.AreEqual('resposta.autorizada', R.ReadShortStr, 'routing-key');
    Assert.IsTrue(R.ReadBit, 'mandatory');
    Assert.IsFalse(R.ReadBit, 'immediate');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicGet_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildBasicGet('nfe.respostas', True));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.respostas', R.ReadShortStr, 'queue');
    Assert.IsTrue(R.ReadBit, 'no-ack');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicGetOk_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LOk: TAMQPBasicGetOk;
begin
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK);
    W.WriteLongLongUInt(42);      // delivery-tag
    W.WriteBit(True);             // redelivered
    W.WriteShortStr('nfe.ex');    // exchange
    W.WriteShortStr('resp.rk');   // routing-key
    W.WriteLongUInt(5);           // message-count
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK));
      LOk := DecodeBasicGetOk(R);
      Assert.IsTrue(UInt64(42) = LOk.DeliveryTag, 'delivery-tag');
      Assert.IsTrue(LOk.Redelivered, 'redelivered');
      Assert.AreEqual('nfe.ex', LOk.Exchange);
      Assert.AreEqual('resp.rk', LOk.RoutingKey);
      Assert.AreEqual(Cardinal(5), LOk.MessageCount);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicAck_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildBasicAck(7, True));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK));
    Assert.IsTrue(UInt64(7) = R.ReadLongLongUInt, 'delivery-tag');
    Assert.IsTrue(R.ReadBit, 'multiple');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicNack_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildBasicNack(9, False, True));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK));
    Assert.IsTrue(UInt64(9) = R.ReadLongLongUInt, 'delivery-tag');
    Assert.IsFalse(R.ReadBit, 'multiple');
    Assert.IsTrue(R.ReadBit, 'requeue');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicQos_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildBasicQos(10, False));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS));
    Assert.AreEqual(Cardinal(0), R.ReadLongUInt, 'prefetch-size');
    Assert.AreEqual(Word(10), R.ReadShortUInt, 'prefetch-count');
    Assert.IsFalse(R.ReadBit, 'global');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicConsume_RoundTrip;
var
  LConsume: TAMQPBasicConsume;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LConsume := TAMQPBasicConsume.Create('nfe.respostas', 'ctag-1', False);
  LConsume.Exclusive := True;

  R := TAMQPReader.Create(BuildBasicConsume(LConsume));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME));
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'reserved-1');
    Assert.AreEqual('nfe.respostas', R.ReadShortStr, 'queue');
    Assert.AreEqual('ctag-1', R.ReadShortStr, 'consumer-tag');
    Assert.IsFalse(R.ReadBit, 'no-local');
    Assert.IsFalse(R.ReadBit, 'no-ack');
    Assert.IsTrue(R.ReadBit, 'exclusive');
    Assert.IsFalse(R.ReadBit, 'no-wait');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicConsumeOk_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK);
    W.WriteShortStr('ctag-gerada');
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK));
      Assert.AreEqual('ctag-gerada', DecodeBasicConsumeOk(R));
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicDeliver_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LDeliver: TAMQPBasicDeliver;
begin
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER);
    W.WriteShortStr('ctag-1');    // consumer-tag
    W.WriteLongLongUInt(99);      // delivery-tag
    W.WriteBit(False);            // redelivered
    W.WriteShortStr('nfe.ex');    // exchange
    W.WriteShortStr('resp.rk');   // routing-key
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER));
      LDeliver := DecodeBasicDeliver(R);
      Assert.AreEqual('ctag-1', LDeliver.ConsumerTag);
      Assert.IsTrue(UInt64(99) = LDeliver.DeliveryTag, 'delivery-tag');
      Assert.IsFalse(LDeliver.Redelivered);
      Assert.AreEqual('nfe.ex', LDeliver.Exchange);
      Assert.AreEqual('resp.rk', LDeliver.RoutingKey);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicReturn_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LReturn: TAMQPBasicReturn;
begin
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_BASIC, AMQP_BASIC_RETURN);
    W.WriteShortUInt(312);         // reply-code (NO_ROUTE)
    W.WriteShortStr('NO_ROUTE');   // reply-text
    W.WriteShortStr('nfe.ex');     // exchange
    W.WriteShortStr('resp.rk');    // routing-key
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN));
      LReturn := DecodeBasicReturn(R);
      Assert.AreEqual(Word(312), LReturn.ReplyCode);
      Assert.AreEqual('NO_ROUTE', LReturn.ReplyText);
      Assert.AreEqual('nfe.ex', LReturn.Exchange);
      Assert.AreEqual('resp.rk', LReturn.RoutingKey);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TChannelQueueExchangeTests.ConfirmSelect_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildConfirmSelect(False));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT));
    Assert.IsFalse(R.ReadBit, 'no-wait');
  finally
    R.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicAckConfirm_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LAck: TAMQPBasicAck;
begin
  // Basic.Ack vindo do broker (publisher confirm): delivery-tag = seq-no.
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_BASIC, AMQP_BASIC_ACK);
    W.WriteLongLongUInt(5);   // delivery-tag (seq-no)
    W.WriteBit(True);         // multiple
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK));
      LAck := DecodeBasicAck(R);
      Assert.IsTrue(UInt64(5) = LAck.DeliveryTag, 'delivery-tag');
      Assert.IsTrue(LAck.Multiple, 'multiple');
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TChannelQueueExchangeTests.BasicNackConfirm_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LNack: TAMQPBasicNack;
begin
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_BASIC, AMQP_BASIC_NACK);
    W.WriteLongLongUInt(8);   // delivery-tag (seq-no)
    W.WriteBit(False);        // multiple
    W.WriteBit(False);        // requeue
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK));
      LNack := DecodeBasicNack(R);
      Assert.IsTrue(UInt64(8) = LNack.DeliveryTag, 'delivery-tag');
      Assert.IsFalse(LNack.Multiple, 'multiple');
      Assert.IsFalse(LNack.Requeue, 'requeue');
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

{ TContentHeaderTests }

procedure TContentHeaderTests.PropertyFlags_ContentTypeEDeliveryMode;
var
  LProps: TAMQPBasicProperties;
  R: TAMQPReader;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  LProps.SetDeliveryMode(2);

  R := TAMQPReader.Create(BuildContentHeader(123, LProps));
  try
    Assert.AreEqual(Word(AMQP_CLASS_BASIC), R.ReadShortUInt, 'class-id');
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'weight');
    Assert.IsTrue(UInt64(123) = R.ReadLongLongUInt, 'body-size');
    // content-type = bit 15 (0x8000), delivery-mode = bit 12 (0x1000) => 0x9000
    Assert.AreEqual(Word($9000), R.ReadShortUInt, 'property-flags');
    Assert.AreEqual('application/json', R.ReadShortStr, 'content-type');
    Assert.AreEqual(2, Integer(R.ReadOctet), 'delivery-mode (octeto)');
  finally
    R.Free;
  end;
end;

procedure TContentHeaderTests.RoundTrip_VariasPropriedades;
var
  LProps: TAMQPBasicProperties;
  R: TAMQPReader;
  LHeader: TAMQPContentHeader;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LProps.SetPersistent;
  LProps.SetCorrelationId('corr-42');
  LProps.SetMessageId('msg-1');
  LProps.SetTimestamp(UInt64(1700000000));
  LProps.SetAppId('pdv');
  LProps.SetMsgType('nfe.autorizada');

  R := TAMQPReader.Create(BuildContentHeader(999, LProps));
  try
    LHeader := DecodeContentHeader(R);
    Assert.IsTrue(UInt64(999) = LHeader.BodySize, 'body-size');
    Assert.AreEqual('text/plain', LHeader.Properties.ContentType);
    Assert.AreEqual(Word(2), Word(LHeader.Properties.DeliveryMode));
    Assert.AreEqual('corr-42', LHeader.Properties.CorrelationId);
    Assert.AreEqual('msg-1', LHeader.Properties.MessageId);
    Assert.IsTrue(UInt64(1700000000) = LHeader.Properties.Timestamp);
    Assert.AreEqual('pdv', LHeader.Properties.AppId);
    Assert.AreEqual('nfe.autorizada', LHeader.Properties.MsgType);
    Assert.IsTrue(LHeader.Properties.Has(bpCorrelationId));
    Assert.IsFalse(LHeader.Properties.Has(bpPriority), 'priority não foi setado');
    Assert.IsFalse(LHeader.Properties.Has(bpReplyTo));
  finally
    R.Free;
  end;
end;

procedure TContentHeaderTests.Persistente_DeliveryMode2;
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetPersistent;
  Assert.IsTrue(LProps.Has(bpDeliveryMode));
  Assert.AreEqual(Word(2), Word(LProps.DeliveryMode));
end;

procedure TContentHeaderTests.Headers_RoundTrip;
var
  LProps: TAMQPBasicProperties;
  LTbl: TAMQPFieldTable;
  R: TAMQPReader;
  LHeader: TAMQPContentHeader;
begin
  LTbl := TAMQPFieldTable.Create;
  LTbl.Put('chave-nfe', 'NFe35240712345678000199550010000012341000012349');

  LProps := TAMQPBasicProperties.Empty;
  LProps.SetHeaders(LTbl);

  R := TAMQPReader.Create(BuildContentHeader(0, LProps));
  try
    LHeader := DecodeContentHeader(R);
    try
      Assert.IsTrue(LHeader.Properties.Has(bpHeaders));
      Assert.AreEqual('NFe35240712345678000199550010000012341000012349',
        LHeader.Properties.Headers['chave-nfe'].AsString);
    finally
      LHeader.Properties.Headers.Free; // tabela decodificada
    end;
  finally
    R.Free;
    LTbl.Free; // tabela de entrada
  end;
end;

procedure TContentHeaderTests.SemPropriedades_FlagsZero;
var
  R: TAMQPReader;
begin
  R := TAMQPReader.Create(BuildContentHeader(0, TAMQPBasicProperties.Empty));
  try
    R.ReadShortUInt;   // class-id
    R.ReadShortUInt;   // weight
    R.ReadLongLongUInt; // body-size
    Assert.AreEqual(Word(0), R.ReadShortUInt, 'property-flags = 0 (nenhuma propriedade)');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TChannelQueueExchangeTests);
  TDUnitX.RegisterTestFixture(TContentHeaderTests);

end.
