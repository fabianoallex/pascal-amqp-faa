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

  { Lado servidor (sub-módulo broker) das classes Channel/Exchange/Queue/Basic:
    decode dos métodos cliente->servidor e build dos *-Ok / Deliver / Return /
    Get-Ok. Vários testes alimentam o Decode do servidor com o output do Build
    do CLIENTE — travam a interop com o próprio cliente da lib. }
  [TestFixture]
  TServerCodecTests = class
  public
    [Test] procedure ChannelOpen_DecodeDoBuildDoCliente;
    [Test] procedure ChannelOpenOk_ClienteConsome;
    [Test] procedure ChannelFlow_RoundTrip;
    [Test] procedure ExchangeDeclare_DecodeDoBuildDoCliente;
    [Test] procedure ExchangeDeclareOk_ClienteConsome;
    [Test] procedure ExchangeBind_DecodeDoBuildDoCliente;
    [Test] procedure ExchangeUnbindOk_Metodo51;
    [Test] procedure QueueDeclare_DecodeDoBuildDoCliente;
    [Test] procedure QueueDeclareOk_ClienteDecodifica;
    [Test] procedure QueueBind_DecodeDoBuildDoCliente;
    [Test] procedure QueueUnbind_DecodeSemNoWait;
    [Test] procedure QueuePurge_RoundTrip;
    [Test] procedure QueueDelete_RoundTrip;
    [Test] procedure BasicQos_DecodeDoBuildDoCliente;
    [Test] procedure BasicPublish_DecodeDoBuildDoCliente;
    [Test] procedure BasicConsume_DecodeDoBuildDoCliente;
    [Test] procedure BasicConsumeOk_ClienteDecodifica;
    [Test] procedure BasicCancel_ArgsComNoWait;
    [Test] procedure BasicGet_DecodeDoBuildDoCliente;
    [Test] procedure BasicGetOk_ClienteDecodifica;
    [Test] procedure BasicGetEmpty_SoReservado;
    [Test] procedure BasicDeliver_ClienteDecodifica;
    [Test] procedure BasicReturn_ClienteDecodifica;
    [Test] procedure BasicReject_RoundTrip;
    [Test] procedure ConfirmSelect_DecodeEBuildOk;
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

{ TServerCodecTests }

procedure TServerCodecTests.ChannelOpen_DecodeDoBuildDoCliente;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildChannelOpen);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN));
    DecodeChannelOpen(R);
    Assert.IsTrue(R.EndOfData, 'reservado consumido');
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.ChannelOpenOk_ClienteConsome;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildChannelOpenOk);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK));
    DecodeChannelOpenOk(R); // decode do CLIENTE
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.ChannelFlow_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildChannelFlow(False));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW));
    Assert.IsFalse(DecodeChannelFlow(R), 'active');
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildChannelFlowOk(True));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW_OK));
    Assert.IsTrue(DecodeChannelFlowOk(R), 'active');
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.ExchangeDeclare_DecodeDoBuildDoCliente;
var
  LDeclare, LDecoded: TAMQPExchangeDeclare;
  R: TAMQPReader;
begin
  LDeclare := TAMQPExchangeDeclare.Create('eventos', AMQP_EXCHANGE_TYPE_TOPIC, True);
  LDeclare.AutoDelete := True;
  LDeclare.Arguments := TAMQPFieldTable.Create;
  LDeclare.Arguments.Put('alternate-exchange', 'ae');
  try
    R := TAMQPReader.Create(BuildExchangeDeclare(LDeclare));
    try
      ReadMethodHeader(R);
      LDecoded := DecodeExchangeDeclare(R);
      try
        Assert.AreEqual('eventos', LDecoded.ExchangeName, 'nome');
        Assert.AreEqual('topic', LDecoded.ExchangeType, 'tipo');
        Assert.IsFalse(LDecoded.Passive, 'passive');
        Assert.IsTrue(LDecoded.Durable, 'durable');
        Assert.IsTrue(LDecoded.AutoDelete, 'auto-delete');
        Assert.IsFalse(LDecoded.Internal, 'internal');
        Assert.IsTrue(LDecoded.Arguments.ContainsKey('alternate-exchange'), 'arg presente');
        Assert.IsTrue(R.EndOfData);
      finally
        LDecoded.Arguments.Free;
      end;
    finally
      R.Free;
    end;
  finally
    LDeclare.Arguments.Free;
  end;
end;

procedure TServerCodecTests.ExchangeDeclareOk_ClienteConsome;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildExchangeDeclareOk);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE_OK));
    DecodeExchangeDeclareOk(R); // decode do CLIENTE
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.ExchangeBind_DecodeDoBuildDoCliente;
var
  LBind, LDecoded: TAMQPExchangeBinding;
  R: TAMQPReader;
begin
  LBind := Default(TAMQPExchangeBinding);
  LBind.Destination := 'dest';
  LBind.Source := 'src';
  LBind.RoutingKey := 'nota.#';

  R := TAMQPReader.Create(BuildExchangeBind(LBind));
  try
    ReadMethodHeader(R);
    LDecoded := DecodeExchangeBind(R);
    try
      Assert.AreEqual('dest', LDecoded.Destination, 'destination');
      Assert.AreEqual('src', LDecoded.Source, 'source');
      Assert.AreEqual('nota.#', LDecoded.RoutingKey, 'routing-key');
      Assert.IsTrue(R.EndOfData);
    finally
      LDecoded.Arguments.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.ExchangeUnbindOk_Metodo51;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildExchangeUnbindOk);
  try
    LId := ReadMethodHeader(R);
    // peculiaridade da spec estendida RabbitMQ: unbind-ok é 51, não 41.
    Assert.AreEqual(Word(51), LId.MethodId, 'method-id');
    Assert.IsTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_UNBIND_OK));
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueueDeclare_DecodeDoBuildDoCliente;
var
  LDeclare, LDecoded: TAMQPQueueDeclare;
  R: TAMQPReader;
begin
  LDeclare := TAMQPQueueDeclare.Create('fila.retry', True);
  LDeclare.Exclusive := True;
  LDeclare.Arguments := TAMQPFieldTable.Create;
  LDeclare.Arguments.Put('x-message-ttl', Integer(5000));
  try
    R := TAMQPReader.Create(BuildQueueDeclare(LDeclare));
    try
      ReadMethodHeader(R);
      LDecoded := DecodeQueueDeclare(R);
      try
        Assert.AreEqual('fila.retry', LDecoded.QueueName, 'nome');
        Assert.IsTrue(LDecoded.Durable, 'durable');
        Assert.IsTrue(LDecoded.Exclusive, 'exclusive');
        Assert.IsFalse(LDecoded.AutoDelete, 'auto-delete');
        Assert.IsTrue(LDecoded.Arguments.ContainsKey('x-message-ttl'), 'arg presente');
        Assert.IsTrue(R.EndOfData);
      finally
        LDecoded.Arguments.Free;
      end;
    finally
      R.Free;
    end;
  finally
    LDeclare.Arguments.Free;
  end;
end;

procedure TServerCodecTests.QueueDeclareOk_ClienteDecodifica;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LOk: TAMQPQueueDeclareOk;
begin
  R := TAMQPReader.Create(BuildQueueDeclareOk('amq.gen-Ab3', 5, 2));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK));
    LOk := DecodeQueueDeclareOk(R); // decode do CLIENTE
    Assert.AreEqual('amq.gen-Ab3', LOk.QueueName, 'nome gerado');
    Assert.AreEqual(Cardinal(5), LOk.MessageCount);
    Assert.AreEqual(Cardinal(2), LOk.ConsumerCount);
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueueBind_DecodeDoBuildDoCliente;
var
  LBind, LDecoded: TAMQPQueueBind;
  R: TAMQPReader;
begin
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := 'q';
  LBind.ExchangeName := 'ex';
  LBind.RoutingKey := 'rk';

  R := TAMQPReader.Create(BuildQueueBind(LBind));
  try
    ReadMethodHeader(R);
    LDecoded := DecodeQueueBind(R);
    try
      Assert.AreEqual('q', LDecoded.QueueName, 'queue');
      Assert.AreEqual('ex', LDecoded.ExchangeName, 'exchange');
      Assert.AreEqual('rk', LDecoded.RoutingKey, 'routing-key');
      Assert.IsTrue(R.EndOfData);
    finally
      LDecoded.Arguments.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueueUnbind_DecodeSemNoWait;
var
  LUnbind, LDecoded: TAMQPQueueUnbind;
  R: TAMQPReader;
begin
  LUnbind := Default(TAMQPQueueUnbind);
  LUnbind.QueueName := 'q';
  LUnbind.ExchangeName := 'ex';
  LUnbind.RoutingKey := 'rk';

  R := TAMQPReader.Create(BuildQueueUnbind(LUnbind));
  try
    ReadMethodHeader(R);
    LDecoded := DecodeQueueUnbind(R);
    try
      Assert.AreEqual('q', LDecoded.QueueName, 'queue');
      Assert.AreEqual('ex', LDecoded.ExchangeName, 'exchange');
      Assert.AreEqual('rk', LDecoded.RoutingKey, 'routing-key');
      Assert.IsTrue(R.EndOfData, 'sem no-wait: acaba direto nos arguments');
    finally
      LDecoded.Arguments.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueuePurge_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LPurge: TAMQPQueuePurge;
begin
  R := TAMQPReader.Create(BuildQueuePurge('q', True));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_PURGE));
    LPurge := DecodeQueuePurge(R);
    Assert.AreEqual('q', LPurge.QueueName, 'queue');
    Assert.IsTrue(LPurge.NoWait, 'no-wait');
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildQueuePurgeOk(42));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_PURGE_OK));
    Assert.AreEqual(Cardinal(42), DecodeQueuePurgeOk(R)); // decode do CLIENTE
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueueDelete_RoundTrip;
var
  LDelete, LDecoded: TAMQPQueueDelete;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LDelete := Default(TAMQPQueueDelete);
  LDelete.QueueName := 'q';
  LDelete.IfUnused := True;
  LDelete.IfEmpty := True;

  R := TAMQPReader.Create(BuildQueueDelete(LDelete));
  try
    ReadMethodHeader(R);
    LDecoded := DecodeQueueDelete(R);
    Assert.AreEqual('q', LDecoded.QueueName, 'queue');
    Assert.IsTrue(LDecoded.IfUnused, 'if-unused');
    Assert.IsTrue(LDecoded.IfEmpty, 'if-empty');
    Assert.IsFalse(LDecoded.NoWait, 'no-wait');
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildQueueDeleteOk(3));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DELETE_OK));
    Assert.AreEqual(Cardinal(3), DecodeQueueDeleteOk(R));
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicQos_DecodeDoBuildDoCliente;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LQos: TAMQPBasicQosArgs;
begin
  R := TAMQPReader.Create(BuildBasicQos(20, True, 0));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS));
    LQos := DecodeBasicQos(R);
    Assert.AreEqual(Cardinal(0), LQos.PrefetchSize);
    Assert.AreEqual(Word(20), LQos.PrefetchCount);
    Assert.IsTrue(LQos.Global, 'global');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildBasicQosOk);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS_OK));
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicPublish_DecodeDoBuildDoCliente;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LPub: TAMQPBasicPublish;
begin
  R := TAMQPReader.Create(BuildBasicPublish('ex', 'chave', True, False));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH));
    LPub := DecodeBasicPublish(R);
    Assert.AreEqual('ex', LPub.Exchange, 'exchange');
    Assert.AreEqual('chave', LPub.RoutingKey, 'routing-key');
    Assert.IsTrue(LPub.Mandatory, 'mandatory');
    Assert.IsFalse(LPub.Immediate, 'immediate');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicConsume_DecodeDoBuildDoCliente;
var
  LConsume, LDecoded: TAMQPBasicConsume;
  R: TAMQPReader;
begin
  LConsume := TAMQPBasicConsume.Create('fila', 'ctag-1', True);
  LConsume.Exclusive := True;

  R := TAMQPReader.Create(BuildBasicConsume(LConsume));
  try
    ReadMethodHeader(R);
    LDecoded := DecodeBasicConsume(R);
    try
      Assert.AreEqual('fila', LDecoded.Queue, 'queue');
      Assert.AreEqual('ctag-1', LDecoded.ConsumerTag, 'consumer-tag');
      Assert.IsFalse(LDecoded.NoLocal, 'no-local');
      Assert.IsTrue(LDecoded.NoAck, 'no-ack');
      Assert.IsTrue(LDecoded.Exclusive, 'exclusive');
      Assert.IsFalse(LDecoded.NoWait, 'no-wait');
      Assert.IsTrue(R.EndOfData);
    finally
      LDecoded.Arguments.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicConsumeOk_ClienteDecodifica;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildBasicConsumeOk('ctag-9'));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK));
    Assert.AreEqual('ctag-9', DecodeBasicConsumeOk(R)); // decode do CLIENTE
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicCancel_ArgsComNoWait;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LCancel: TAMQPBasicCancelArgs;
begin
  R := TAMQPReader.Create(BuildBasicCancel('ctag-1', True));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL));
    LCancel := DecodeBasicCancelArgs(R);
    Assert.AreEqual('ctag-1', LCancel.ConsumerTag, 'consumer-tag');
    Assert.IsTrue(LCancel.NoWait, 'no-wait');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildBasicCancelOk('ctag-1'));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL_OK));
    Assert.AreEqual('ctag-1', DecodeBasicCancelOk(R)); // decode do CLIENTE
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicGet_DecodeDoBuildDoCliente;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LGet: TAMQPBasicGet;
begin
  R := TAMQPReader.Create(BuildBasicGet('fila', False));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET));
    LGet := DecodeBasicGet(R);
    Assert.AreEqual('fila', LGet.Queue, 'queue');
    Assert.IsFalse(LGet.NoAck, 'no-ack');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicGetOk_ClienteDecodifica;
var
  LGetOk, LDecoded: TAMQPBasicGetOk;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LGetOk.DeliveryTag := 77;
  LGetOk.Redelivered := True;
  LGetOk.Exchange := 'ex';
  LGetOk.RoutingKey := 'rk';
  LGetOk.MessageCount := 4;

  R := TAMQPReader.Create(BuildBasicGetOk(LGetOk));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK));
    LDecoded := DecodeBasicGetOk(R); // decode do CLIENTE
    Assert.IsTrue(UInt64(77) = LDecoded.DeliveryTag, 'delivery-tag');
    Assert.IsTrue(LDecoded.Redelivered, 'redelivered');
    Assert.AreEqual('ex', LDecoded.Exchange);
    Assert.AreEqual('rk', LDecoded.RoutingKey);
    Assert.AreEqual(Cardinal(4), LDecoded.MessageCount);
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicGetEmpty_SoReservado;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildBasicGetEmpty);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_EMPTY));
    Assert.AreEqual('', R.ReadShortStr, 'reserved-1');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicDeliver_ClienteDecodifica;
var
  LDeliver, LDecoded: TAMQPBasicDeliver;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LDeliver.ConsumerTag := 'ctag';
  LDeliver.DeliveryTag := 12;
  LDeliver.Redelivered := False;
  LDeliver.Exchange := 'ex';
  LDeliver.RoutingKey := 'rk';

  R := TAMQPReader.Create(BuildBasicDeliver(LDeliver));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER));
    LDecoded := DecodeBasicDeliver(R); // decode do CLIENTE
    Assert.AreEqual('ctag', LDecoded.ConsumerTag);
    Assert.IsTrue(UInt64(12) = LDecoded.DeliveryTag, 'delivery-tag');
    Assert.IsFalse(LDecoded.Redelivered);
    Assert.AreEqual('ex', LDecoded.Exchange);
    Assert.AreEqual('rk', LDecoded.RoutingKey);
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicReturn_ClienteDecodifica;
var
  LReturn, LDecoded: TAMQPBasicReturn;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LReturn.ReplyCode := 312;
  LReturn.ReplyText := 'NO_ROUTE';
  LReturn.Exchange := 'ex';
  LReturn.RoutingKey := 'rk';

  R := TAMQPReader.Create(BuildBasicReturn(LReturn));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN));
    LDecoded := DecodeBasicReturn(R); // decode do CLIENTE
    Assert.AreEqual(Word(312), LDecoded.ReplyCode);
    Assert.AreEqual('NO_ROUTE', LDecoded.ReplyText);
    Assert.AreEqual('ex', LDecoded.Exchange);
    Assert.AreEqual('rk', LDecoded.RoutingKey);
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicReject_RoundTrip;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LReject: TAMQPBasicReject;
begin
  R := TAMQPReader.Create(BuildBasicReject(55, False));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_REJECT));
    LReject := DecodeBasicReject(R);
    Assert.IsTrue(UInt64(55) = LReject.DeliveryTag, 'delivery-tag');
    Assert.IsFalse(LReject.Requeue, 'requeue');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.ConfirmSelect_DecodeEBuildOk;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildConfirmSelect(True));
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT));
    Assert.IsTrue(DecodeConfirmSelect(R), 'no-wait');
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildConfirmSelectOk);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT_OK));
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TChannelQueueExchangeTests);
  TDUnitX.RegisterTestFixture(TContentHeaderTests);
  TDUnitX.RegisterTestFixture(TServerCodecTests);

end.
