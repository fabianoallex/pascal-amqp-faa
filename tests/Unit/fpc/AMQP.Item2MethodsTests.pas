unit AMQP.Item2MethodsTests;

{ Testes do codec do item 2: métodos de channel/exchange/queue/basic e o
  content header (property-flags). Sem broker. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Rtti,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Channel.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  TChannelQueueExchangeTests = class(TTestCase)
  published
    procedure ChannelOpen_RoundTrip;
    procedure ChannelClose_RoundTrip;
    procedure ExchangeDeclare_RoundTrip;
    procedure ExchangeBind_RoundTrip;
    procedure ExchangeUnbind_RoundTrip;
    procedure QueueDeclare_RoundTrip;
    procedure QueueDeclareOk_Decode;
    procedure QueueBind_RoundTrip;
    procedure QueueUnbind_RoundTrip;
    procedure BasicPublish_RoundTrip;
    procedure BasicGet_RoundTrip;
    procedure BasicGetOk_Decode;
    procedure BasicAck_RoundTrip;
    procedure BasicNack_RoundTrip;
    procedure BasicQos_RoundTrip;
    procedure BasicConsume_RoundTrip;
    procedure BasicConsumeOk_Decode;
    procedure BasicDeliver_Decode;
    procedure BasicReturn_Decode;
    procedure ConfirmSelect_RoundTrip;
    procedure BasicAckConfirm_Decode;
    procedure BasicNackConfirm_Decode;
  end;

  TContentHeaderTests = class(TTestCase)
  published
    procedure PropertyFlags_ContentTypeEDeliveryMode;
    procedure RoundTrip_VariasPropriedades;
    procedure Persistente_DeliveryMode2;
    procedure Headers_RoundTrip;
    procedure SemPropriedades_FlagsZero;
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
    AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN));
    AssertEquals('reserved-1', '', R.ReadShortStr);
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE));
    LDecoded := DecodeChannelClose(R);
    AssertEquals(404, Integer(LDecoded.ReplyCode));
    AssertEquals('NOT_FOUND', LDecoded.ReplyText);
    AssertEquals(Integer(AMQP_CLASS_QUEUE), Integer(LDecoded.ClassId));
    AssertEquals(Integer(AMQP_QUEUE_DECLARE), Integer(LDecoded.MethodId));
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
    AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    AssertEquals('type', 'topic', R.ReadShortStr);
    AssertFalse('passive', R.ReadBit);
    AssertTrue('durable', R.ReadBit);
    AssertTrue('auto-delete', R.ReadBit);
    AssertFalse('internal', R.ReadBit);
    AssertFalse('no-wait', R.ReadBit);
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
    AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_BIND));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('destination', 'nfe.dest', R.ReadShortStr);
    AssertEquals('source', 'nfe.source', R.ReadShortStr);
    AssertEquals('routing-key', 'resposta.*', R.ReadShortStr);
    AssertFalse('no-wait', R.ReadBit);
    LArgs := R.ReadFieldTable;
    try
      AssertEquals('arguments vazio', 0, LArgs.Count);
    finally
      LArgs.Free;
    end;
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_UNBIND));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('destination', 'nfe.dest', R.ReadShortStr);
    AssertEquals('source', 'nfe.source', R.ReadShortStr);
    AssertEquals('routing-key', 'resposta.*', R.ReadShortStr);
    AssertFalse('no-wait', R.ReadBit);
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
    AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    AssertFalse('passive', R.ReadBit);
    AssertTrue('durable', R.ReadBit);
    AssertFalse('exclusive', R.ReadBit);
    AssertFalse('auto-delete', R.ReadBit);
    AssertFalse('no-wait', R.ReadBit);
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
      AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK));
      LOk := DecodeQueueDeclareOk(R);
      AssertEquals('nfe.respostas', LOk.QueueName);
      AssertEquals(QWord(7), QWord(LOk.MessageCount));
      AssertEquals(QWord(2), QWord(LOk.ConsumerCount));
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
    AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_BIND));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    AssertEquals('routing-key', 'resposta.autorizada', R.ReadShortStr);
    AssertFalse('no-wait', R.ReadBit);
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
    AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_UNBIND));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    AssertEquals('routing-key', 'resposta.autorizada', R.ReadShortStr);
    // Diferente de bind: unbind NÃO tem no-wait — vem a field-table direto.
    LArgs := R.ReadFieldTable;
    try
      AssertEquals('arguments vazio', 0, LArgs.Count);
    finally
      LArgs.Free;
    end;
    AssertTrue('field-table encerra o payload', R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('exchange', 'nfe.exchange', R.ReadShortStr);
    AssertEquals('routing-key', 'resposta.autorizada', R.ReadShortStr);
    AssertTrue('mandatory', R.ReadBit);
    AssertFalse('immediate', R.ReadBit);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    AssertTrue('no-ack', R.ReadBit);
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
      AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK));
      LOk := DecodeBasicGetOk(R);
      AssertTrue('delivery-tag', UInt64(42) = LOk.DeliveryTag);
      AssertTrue('redelivered', LOk.Redelivered);
      AssertEquals('nfe.ex', LOk.Exchange);
      AssertEquals('resp.rk', LOk.RoutingKey);
      AssertEquals(QWord(5), QWord(LOk.MessageCount));
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK));
    AssertTrue('delivery-tag', UInt64(7) = R.ReadLongLongUInt);
    AssertTrue('multiple', R.ReadBit);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK));
    AssertTrue('delivery-tag', UInt64(9) = R.ReadLongLongUInt);
    AssertFalse('multiple', R.ReadBit);
    AssertTrue('requeue', R.ReadBit);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS));
    AssertEquals('prefetch-size', QWord(0), QWord(R.ReadLongUInt));
    AssertEquals('prefetch-count', 10, Integer(R.ReadShortUInt));
    AssertFalse('global', R.ReadBit);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME));
    AssertEquals('reserved-1', 0, Integer(R.ReadShortUInt));
    AssertEquals('queue', 'nfe.respostas', R.ReadShortStr);
    AssertEquals('consumer-tag', 'ctag-1', R.ReadShortStr);
    AssertFalse('no-local', R.ReadBit);
    AssertFalse('no-ack', R.ReadBit);
    AssertTrue('exclusive', R.ReadBit);
    AssertFalse('no-wait', R.ReadBit);
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
      AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK));
      AssertEquals('ctag-gerada', DecodeBasicConsumeOk(R));
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
      AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER));
      LDeliver := DecodeBasicDeliver(R);
      AssertEquals('ctag-1', LDeliver.ConsumerTag);
      AssertTrue('delivery-tag', UInt64(99) = LDeliver.DeliveryTag);
      AssertFalse(LDeliver.Redelivered);
      AssertEquals('nfe.ex', LDeliver.Exchange);
      AssertEquals('resp.rk', LDeliver.RoutingKey);
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
      AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN));
      LReturn := DecodeBasicReturn(R);
      AssertEquals(312, Integer(LReturn.ReplyCode));
      AssertEquals('NO_ROUTE', LReturn.ReplyText);
      AssertEquals('nfe.ex', LReturn.Exchange);
      AssertEquals('resp.rk', LReturn.RoutingKey);
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
    AssertTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT));
    AssertFalse('no-wait', R.ReadBit);
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
      AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK));
      LAck := DecodeBasicAck(R);
      AssertTrue('delivery-tag', UInt64(5) = LAck.DeliveryTag);
      AssertTrue('multiple', LAck.Multiple);
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
      AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK));
      LNack := DecodeBasicNack(R);
      AssertTrue('delivery-tag', UInt64(8) = LNack.DeliveryTag);
      AssertFalse('multiple', LNack.Multiple);
      AssertFalse('requeue', LNack.Requeue);
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
    AssertEquals('class-id', Integer(AMQP_CLASS_BASIC), Integer(R.ReadShortUInt));
    AssertEquals('weight', 0, Integer(R.ReadShortUInt));
    AssertTrue('body-size', UInt64(123) = R.ReadLongLongUInt);
    // content-type = bit 15 (0x8000), delivery-mode = bit 12 (0x1000) => 0x9000
    AssertEquals('property-flags', $9000, Integer(R.ReadShortUInt));
    AssertEquals('content-type', 'application/json', R.ReadShortStr);
    AssertEquals('delivery-mode (octeto)', 2, Integer(R.ReadOctet));
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
    AssertTrue('body-size', UInt64(999) = LHeader.BodySize);
    AssertEquals('text/plain', LHeader.Properties.ContentType);
    AssertEquals(2, Integer(LHeader.Properties.DeliveryMode));
    AssertEquals('corr-42', LHeader.Properties.CorrelationId);
    AssertEquals('msg-1', LHeader.Properties.MessageId);
    AssertTrue(UInt64(1700000000) = LHeader.Properties.Timestamp);
    AssertEquals('pdv', LHeader.Properties.AppId);
    AssertEquals('nfe.autorizada', LHeader.Properties.MsgType);
    AssertTrue(LHeader.Properties.Has(bpCorrelationId));
    AssertFalse('priority não foi setado', LHeader.Properties.Has(bpPriority));
    AssertFalse(LHeader.Properties.Has(bpReplyTo));
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
  AssertTrue(LProps.Has(bpDeliveryMode));
  AssertEquals(2, Integer(LProps.DeliveryMode));
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
      AssertTrue(LHeader.Properties.Has(bpHeaders));
      LChaveVal := LHeader.Properties.Headers['chave-nfe'];
      AssertEquals('NFe35240712345678000199550010000012341000012349',
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
    AssertEquals('property-flags = 0 (nenhuma propriedade)', 0, Integer(R.ReadShortUInt));
    AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

initialization
  RegisterTest(TChannelQueueExchangeTests);
  RegisterTest(TContentHeaderTests);

end.
