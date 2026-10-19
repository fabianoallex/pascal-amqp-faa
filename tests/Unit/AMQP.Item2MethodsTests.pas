unit AMQP.Item2MethodsTests;

{ Testes do codec do item 2: métodos de channel/exchange/queue/basic e o
  content header (property-flags). Sem broker. }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Rtti,
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
    // Channel
    [Test] procedure ChannelOpen_DecodeDoBuildDoCliente;
    [Test] procedure ChannelOpenOk_ClienteConsome;
    [Test] procedure ChannelFlow_RoundTrip;
    // Exchange
    [Test] procedure ExchangeDeclare_DecodeDoBuildDoCliente;
    [Test] procedure ExchangeDeclareOk_ClienteConsome;
    [Test] procedure ExchangeBind_DecodeDoBuildDoCliente;
    [Test] procedure ExchangeUnbindOk_Metodo51;
    // Queue
    [Test] procedure QueueDeclare_DecodeDoBuildDoCliente;
    [Test] procedure QueueDeclareOk_ClienteDecodifica;
    [Test] procedure QueueBind_DecodeDoBuildDoCliente;
    [Test] procedure QueueUnbind_DecodeSemNoWait;
    [Test] procedure QueuePurge_RoundTrip;
    [Test] procedure QueueDelete_RoundTrip;
    // Basic
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN));
    TAssert.AssertEquals('reserved-1', '', R.ReadShortStr);
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE));
    LDecoded := DecodeChannelClose(R);
    TAssert.AssertEquals(404, Integer(LDecoded.ReplyCode));
    TAssert.AssertEquals('NOT_FOUND', LDecoded.ReplyText);
    TAssert.AssertEquals(Integer(AMQP_CLASS_QUEUE), Integer(LDecoded.ClassId));
    TAssert.AssertEquals(Integer(AMQP_QUEUE_DECLARE), Integer(LDecoded.MethodId));
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    TAssert.AssertEquals('type', 'topic', R.ReadShortStr);
    TAssert.AssertFalse('passive', R.ReadBit);
    TAssert.AssertTrue('durable', R.ReadBit);
    TAssert.AssertTrue('auto-delete', R.ReadBit);
    TAssert.AssertFalse('internal', R.ReadBit);
    TAssert.AssertFalse('no-wait', R.ReadBit);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_BIND));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('destination', 'nfe.dest', R.ReadShortStr);
    TAssert.AssertEquals('source', 'nfe.source', R.ReadShortStr);
    TAssert.AssertEquals('routing-key', 'resposta.*', R.ReadShortStr);
    TAssert.AssertFalse('no-wait', R.ReadBit);
    LArgs := R.ReadFieldTable;
    try
      TAssert.AssertEquals('arguments vazio', 0, LArgs.Count);
    finally
      LArgs.Free;
    end;
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_UNBIND));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('destination', 'nfe.dest', R.ReadShortStr);
    TAssert.AssertEquals('source', 'nfe.source', R.ReadShortStr);
    TAssert.AssertEquals('routing-key', 'resposta.*', R.ReadShortStr);
    TAssert.AssertFalse('no-wait', R.ReadBit);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    TAssert.AssertFalse('passive', R.ReadBit);
    TAssert.AssertTrue('durable', R.ReadBit);
    TAssert.AssertFalse('exclusive', R.ReadBit);
    TAssert.AssertFalse('auto-delete', R.ReadBit);
    TAssert.AssertFalse('no-wait', R.ReadBit);
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK));
      LOk := DecodeQueueDeclareOk(R);
      TAssert.AssertEquals('nfe.respostas', LOk.QueueName);
      TAssert.AssertEquals(Int64(7), Int64(LOk.MessageCount));
      TAssert.AssertEquals(Int64(2), Int64(LOk.ConsumerCount));
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_BIND));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    TAssert.AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    TAssert.AssertEquals('routing-key', 'resposta.autorizada', R.ReadShortStr);
    TAssert.AssertFalse('no-wait', R.ReadBit);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_UNBIND));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    TAssert.AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    TAssert.AssertEquals('routing-key', 'resposta.autorizada', R.ReadShortStr);
    // Diferente de bind: unbind NÃO tem no-wait — vem a field-table direto.
    LArgs := R.ReadFieldTable;
    try
      TAssert.AssertEquals('arguments vazio', 0, LArgs.Count);
    finally
      LArgs.Free;
    end;
    TAssert.AssertTrue('field-table encerra o payload', R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    TAssert.AssertEquals('routing-key', 'resposta.autorizada', R.ReadShortStr);
    TAssert.AssertTrue('mandatory', R.ReadBit);
    TAssert.AssertFalse('immediate', R.ReadBit);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    TAssert.AssertTrue('no-ack', R.ReadBit);
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK));
      LOk := DecodeBasicGetOk(R);
      TAssert.AssertTrue('delivery-tag', UInt64(42) = LOk.DeliveryTag);
      TAssert.AssertTrue('redelivered', LOk.Redelivered);
      TAssert.AssertEquals('nfe.ex', LOk.Exchange);
      TAssert.AssertEquals('resp.rk', LOk.RoutingKey);
      TAssert.AssertEquals(Int64(5), Int64(LOk.MessageCount));
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK));
    TAssert.AssertTrue('delivery-tag', UInt64(7) = R.ReadLongLongUInt);
    TAssert.AssertTrue('multiple', R.ReadBit);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK));
    TAssert.AssertTrue('delivery-tag', UInt64(9) = R.ReadLongLongUInt);
    TAssert.AssertFalse('multiple', R.ReadBit);
    TAssert.AssertTrue('requeue', R.ReadBit);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS));
    TAssert.AssertEquals('prefetch-size', Int64(0), Int64(R.ReadLongUInt));
    TAssert.AssertEquals('prefetch-count', 10, Integer(R.ReadShortUInt));
    TAssert.AssertFalse('global', R.ReadBit);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME));
    TAssert.AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    TAssert.AssertEquals('consumer-tag', 'ctag-1', R.ReadShortStr);
    TAssert.AssertFalse('no-local', R.ReadBit);
    TAssert.AssertFalse('no-ack', R.ReadBit);
    TAssert.AssertTrue('exclusive', R.ReadBit);
    TAssert.AssertFalse('no-wait', R.ReadBit);
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK));
      TAssert.AssertEquals('ctag-gerada', DecodeBasicConsumeOk(R));
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER));
      LDeliver := DecodeBasicDeliver(R);
      TAssert.AssertEquals('ctag-1', LDeliver.ConsumerTag);
      TAssert.AssertTrue('delivery-tag', UInt64(99) = LDeliver.DeliveryTag);
      TAssert.AssertFalse(LDeliver.Redelivered);
      TAssert.AssertEquals('nfe.ex', LDeliver.Exchange);
      TAssert.AssertEquals('resp.rk', LDeliver.RoutingKey);
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN));
      LReturn := DecodeBasicReturn(R);
      TAssert.AssertEquals(312, Integer(LReturn.ReplyCode));
      TAssert.AssertEquals('NO_ROUTE', LReturn.ReplyText);
      TAssert.AssertEquals('nfe.ex', LReturn.Exchange);
      TAssert.AssertEquals('resp.rk', LReturn.RoutingKey);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT));
    TAssert.AssertFalse('no-wait', R.ReadBit);
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK));
      LAck := DecodeBasicAck(R);
      TAssert.AssertTrue('delivery-tag', UInt64(5) = LAck.DeliveryTag);
      TAssert.AssertTrue('multiple', LAck.Multiple);
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK));
      LNack := DecodeBasicNack(R);
      TAssert.AssertTrue('delivery-tag', UInt64(8) = LNack.DeliveryTag);
      TAssert.AssertFalse('multiple', LNack.Multiple);
      TAssert.AssertFalse('requeue', LNack.Requeue);
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
    TAssert.AssertEquals('class-id', Integer(AMQP_CLASS_BASIC), Integer(R.ReadShortUInt));
    TAssert.AssertEquals('weight', 0, Integer(R.ReadShortUInt));
    TAssert.AssertTrue('body-size', UInt64(123) = R.ReadLongLongUInt);
    // content-type = bit 15 (0x8000), delivery-mode = bit 12 (0x1000) => 0x9000
    TAssert.AssertEquals('property-flags', $9000, Integer(R.ReadShortUInt));
    TAssert.AssertEquals('content-type', 'application/json', R.ReadShortStr);
    TAssert.AssertEquals('delivery-mode (octeto)', 2, Integer(R.ReadOctet));
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
    TAssert.AssertTrue('body-size', UInt64(999) = LHeader.BodySize);
    TAssert.AssertEquals('text/plain', LHeader.Properties.ContentType);
    TAssert.AssertEquals(2, Integer(LHeader.Properties.DeliveryMode));
    TAssert.AssertEquals('corr-42', LHeader.Properties.CorrelationId);
    TAssert.AssertEquals('msg-1', LHeader.Properties.MessageId);
    TAssert.AssertTrue(UInt64(1700000000) = LHeader.Properties.Timestamp);
    TAssert.AssertEquals('pdv', LHeader.Properties.AppId);
    TAssert.AssertEquals('nfe.autorizada', LHeader.Properties.MsgType);
    TAssert.AssertTrue(LHeader.Properties.Has(bpCorrelationId));
    TAssert.AssertFalse('priority não foi setado', LHeader.Properties.Has(bpPriority));
    TAssert.AssertFalse(LHeader.Properties.Has(bpReplyTo));
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
  TAssert.AssertTrue(LProps.Has(bpDeliveryMode));
  TAssert.AssertEquals(2, Integer(LProps.DeliveryMode));
end;

procedure TContentHeaderTests.Headers_RoundTrip;
var
  LProps: TAMQPBasicProperties;
  LTbl: TAMQPFieldTable;
  R: TAMQPReader;
  LHeader: TAMQPContentHeader;
  LChaveVal: TValue;
begin
  LTbl := TAMQPFieldTable.Create;
  LTbl.Put('chave-nfe', 'NFe35240712345678000199550010000012341000012349');

  LProps := TAMQPBasicProperties.Empty;
  LProps.SetHeaders(LTbl);

  R := TAMQPReader.Create(BuildContentHeader(0, LProps));
  try
    LHeader := DecodeContentHeader(R);
    try
      TAssert.AssertTrue(LHeader.Properties.Has(bpHeaders));
      LChaveVal := LHeader.Properties.Headers['chave-nfe'];
      TAssert.AssertEquals('NFe35240712345678000199550010000012341000012349',
        LChaveVal.AsString);
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
    TAssert.AssertEquals('property-flags = 0 (nenhuma propriedade)', 0, Integer(R.ReadShortUInt));
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN));
    DecodeChannelOpen(R);
    TAssert.AssertTrue('reservado consumido', R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK));
    DecodeChannelOpenOk(R); // decode do CLIENTE
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW));
    TAssert.AssertFalse('active', DecodeChannelFlow(R));
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildChannelFlowOk(True));
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW_OK));
    TAssert.AssertTrue('active', DecodeChannelFlowOk(R));
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
        TAssert.AssertEquals('nome', 'eventos', LDecoded.ExchangeName);
        TAssert.AssertEquals('tipo', 'topic', LDecoded.ExchangeType);
        TAssert.AssertFalse('passive', LDecoded.Passive);
        TAssert.AssertTrue('durable', LDecoded.Durable);
        TAssert.AssertTrue('auto-delete', LDecoded.AutoDelete);
        TAssert.AssertFalse('internal', LDecoded.Internal);
        TAssert.AssertTrue('arg presente', LDecoded.Arguments.ContainsKey('alternate-exchange'));
        TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE_OK));
    DecodeExchangeDeclareOk(R); // decode do CLIENTE
    TAssert.AssertTrue(R.EndOfData);
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
      TAssert.AssertEquals('destination', 'dest', LDecoded.Destination);
      TAssert.AssertEquals('source', 'src', LDecoded.Source);
      TAssert.AssertEquals('routing-key', 'nota.#', LDecoded.RoutingKey);
      TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertEquals('method-id', 51, Integer(LId.MethodId));
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_UNBIND_OK));
    TAssert.AssertTrue(R.EndOfData);
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
        TAssert.AssertEquals('nome', 'fila.retry', LDecoded.QueueName);
        TAssert.AssertTrue('durable', LDecoded.Durable);
        TAssert.AssertTrue('exclusive', LDecoded.Exclusive);
        TAssert.AssertFalse('auto-delete', LDecoded.AutoDelete);
        TAssert.AssertTrue('arg presente', LDecoded.Arguments.ContainsKey('x-message-ttl'));
        TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK));
    LOk := DecodeQueueDeclareOk(R); // decode do CLIENTE
    TAssert.AssertEquals('nome gerado', 'amq.gen-Ab3', LOk.QueueName);
    TAssert.AssertEquals(Int64(5), Int64(LOk.MessageCount));
    TAssert.AssertEquals(Int64(2), Int64(LOk.ConsumerCount));
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueueBind_DecodeDoBuildDoCliente;
var
  LBind: TAMQPQueueBind;
  LDecoded: TAMQPQueueBind;
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
      TAssert.AssertEquals('queue', 'q', LDecoded.QueueName);
      TAssert.AssertEquals('exchange', 'ex', LDecoded.ExchangeName);
      TAssert.AssertEquals('routing-key', 'rk', LDecoded.RoutingKey);
      TAssert.AssertTrue(R.EndOfData);
    finally
      LDecoded.Arguments.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueueUnbind_DecodeSemNoWait;
var
  LUnbind: TAMQPQueueUnbind;
  LDecoded: TAMQPQueueUnbind;
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
      TAssert.AssertEquals('queue', 'q', LDecoded.QueueName);
      TAssert.AssertEquals('exchange', 'ex', LDecoded.ExchangeName);
      TAssert.AssertEquals('routing-key', 'rk', LDecoded.RoutingKey);
      TAssert.AssertTrue('sem no-wait: acaba direto nos arguments', R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_PURGE));
    LPurge := DecodeQueuePurge(R);
    TAssert.AssertEquals('queue', 'q', LPurge.QueueName);
    TAssert.AssertTrue('no-wait', LPurge.NoWait);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildQueuePurgeOk(42));
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_PURGE_OK));
    TAssert.AssertEquals(Int64(42), Int64(DecodeQueuePurgeOk(R))); // decode do CLIENTE
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.QueueDelete_RoundTrip;
var
  LDelete: TAMQPQueueDelete;
  LDecoded: TAMQPQueueDelete;
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
    TAssert.AssertEquals('queue', 'q', LDecoded.QueueName);
    TAssert.AssertTrue('if-unused', LDecoded.IfUnused);
    TAssert.AssertTrue('if-empty', LDecoded.IfEmpty);
    TAssert.AssertFalse('no-wait', LDecoded.NoWait);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildQueueDeleteOk(3));
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DELETE_OK));
    TAssert.AssertEquals(Int64(3), Int64(DecodeQueueDeleteOk(R)));
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS));
    LQos := DecodeBasicQos(R);
    TAssert.AssertEquals(Int64(0), Int64(LQos.PrefetchSize));
    TAssert.AssertEquals(20, Integer(LQos.PrefetchCount));
    TAssert.AssertTrue('global', LQos.Global);
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildBasicQosOk);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS_OK));
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH));
    LPub := DecodeBasicPublish(R);
    TAssert.AssertEquals('exchange', 'ex', LPub.Exchange);
    TAssert.AssertEquals('routing-key', 'chave', LPub.RoutingKey);
    TAssert.AssertTrue('mandatory', LPub.Mandatory);
    TAssert.AssertFalse('immediate', LPub.Immediate);
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicConsume_DecodeDoBuildDoCliente;
var
  LConsume: TAMQPBasicConsume;
  LDecoded: TAMQPBasicConsume;
  R: TAMQPReader;
begin
  LConsume := TAMQPBasicConsume.Create('fila', 'ctag-1', True);
  LConsume.Exclusive := True;

  R := TAMQPReader.Create(BuildBasicConsume(LConsume));
  try
    ReadMethodHeader(R);
    LDecoded := DecodeBasicConsume(R);
    try
      TAssert.AssertEquals('queue', 'fila', LDecoded.Queue);
      TAssert.AssertEquals('consumer-tag', 'ctag-1', LDecoded.ConsumerTag);
      TAssert.AssertFalse('no-local', LDecoded.NoLocal);
      TAssert.AssertTrue('no-ack', LDecoded.NoAck);
      TAssert.AssertTrue('exclusive', LDecoded.Exclusive);
      TAssert.AssertFalse('no-wait', LDecoded.NoWait);
      TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK));
    TAssert.AssertEquals('ctag-9', DecodeBasicConsumeOk(R)); // decode do CLIENTE
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL));
    LCancel := DecodeBasicCancelArgs(R);
    TAssert.AssertEquals('consumer-tag', 'ctag-1', LCancel.ConsumerTag);
    TAssert.AssertTrue('no-wait', LCancel.NoWait);
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildBasicCancelOk('ctag-1'));
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL_OK));
    TAssert.AssertEquals('ctag-1', DecodeBasicCancelOk(R)); // decode do CLIENTE
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET));
    LGet := DecodeBasicGet(R);
    TAssert.AssertEquals('queue', 'fila', LGet.Queue);
    TAssert.AssertFalse('no-ack', LGet.NoAck);
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicGetOk_ClienteDecodifica;
var
  LGetOk: TAMQPBasicGetOk;
  LDecoded: TAMQPBasicGetOk;
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK));
    LDecoded := DecodeBasicGetOk(R); // decode do CLIENTE
    TAssert.AssertEquals(Int64(77), Int64(LDecoded.DeliveryTag));
    TAssert.AssertTrue('redelivered', LDecoded.Redelivered);
    TAssert.AssertEquals('ex', LDecoded.Exchange);
    TAssert.AssertEquals('rk', LDecoded.RoutingKey);
    TAssert.AssertEquals(Int64(4), Int64(LDecoded.MessageCount));
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_EMPTY));
    TAssert.AssertEquals('reserved-1', '', R.ReadShortStr);
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicDeliver_ClienteDecodifica;
var
  LDeliver: TAMQPBasicDeliver;
  LDecoded: TAMQPBasicDeliver;
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER));
    LDecoded := DecodeBasicDeliver(R); // decode do CLIENTE
    TAssert.AssertEquals('ctag', LDecoded.ConsumerTag);
    TAssert.AssertEquals(Int64(12), Int64(LDecoded.DeliveryTag));
    TAssert.AssertFalse(LDecoded.Redelivered);
    TAssert.AssertEquals('ex', LDecoded.Exchange);
    TAssert.AssertEquals('rk', LDecoded.RoutingKey);
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TServerCodecTests.BasicReturn_ClienteDecodifica;
var
  LReturn: TAMQPBasicReturn;
  LDecoded: TAMQPBasicReturn;
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN));
    LDecoded := DecodeBasicReturn(R); // decode do CLIENTE
    TAssert.AssertEquals(312, Integer(LDecoded.ReplyCode));
    TAssert.AssertEquals('NO_ROUTE', LDecoded.ReplyText);
    TAssert.AssertEquals('ex', LDecoded.Exchange);
    TAssert.AssertEquals('rk', LDecoded.RoutingKey);
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_REJECT));
    LReject := DecodeBasicReject(R);
    TAssert.AssertEquals(Int64(55), Int64(LReject.DeliveryTag));
    TAssert.AssertFalse('requeue', LReject.Requeue);
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT));
    TAssert.AssertTrue('no-wait', DecodeConfirmSelect(R));
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildConfirmSelectOk);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT_OK));
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TChannelQueueExchangeTests);
  TDUnitX.RegisterTestFixture(TContentHeaderTests);
  TDUnitX.RegisterTestFixture(TServerCodecTests);

end.
