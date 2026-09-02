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

  { Lado servidor (sub-módulo broker) das classes Channel/Exchange/Queue/Basic:
    decode dos métodos cliente->servidor e build dos *-Ok / Deliver / Return /
    Get-Ok. Vários testes alimentam o Decode do servidor com o output do Build
    do CLIENTE — travam a interop com o próprio cliente da lib. }
  TServerCodecTests = class(TTestCase)
  published
    // Channel
    procedure ChannelOpen_DecodeDoBuildDoCliente;
    procedure ChannelOpenOk_ClienteConsome;
    procedure ChannelFlow_RoundTrip;
    // Exchange
    procedure ExchangeDeclare_DecodeDoBuildDoCliente;
    procedure ExchangeDeclareOk_ClienteConsome;
    procedure ExchangeBind_DecodeDoBuildDoCliente;
    procedure ExchangeUnbindOk_Metodo51;
    // Queue
    procedure QueueDeclare_DecodeDoBuildDoCliente;
    procedure QueueDeclareOk_ClienteDecodifica;
    procedure QueueBind_DecodeDoBuildDoCliente;
    procedure QueueUnbind_DecodeSemNoWait;
    procedure QueuePurge_RoundTrip;
    procedure QueueDelete_RoundTrip;
    // Basic
    procedure BasicQos_DecodeDoBuildDoCliente;
    procedure BasicPublish_DecodeDoBuildDoCliente;
    procedure BasicConsume_DecodeDoBuildDoCliente;
    procedure BasicConsumeOk_ClienteDecodifica;
    procedure BasicCancel_ArgsComNoWait;
    procedure BasicGet_DecodeDoBuildDoCliente;
    procedure BasicGetOk_ClienteDecodifica;
    procedure BasicGetEmpty_SoReservado;
    procedure BasicDeliver_ClienteDecodifica;
    procedure BasicReturn_ClienteDecodifica;
    procedure BasicReject_RoundTrip;
    procedure ConfirmSelect_DecodeEBuildOk;
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

{ TServerCodecTests }

procedure TServerCodecTests.ChannelOpen_DecodeDoBuildDoCliente;
var
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  R := TAMQPReader.Create(BuildChannelOpen);
  try
    LId := ReadMethodHeader(R);
    AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN));
    DecodeChannelOpen(R);
    AssertTrue('reservado consumido', R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK));
    DecodeChannelOpenOk(R); // decode do CLIENTE
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW));
    AssertFalse('active', DecodeChannelFlow(R));
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildChannelFlowOk(True));
  try
    LId := ReadMethodHeader(R);
    AssertTrue(LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW_OK));
    AssertTrue('active', DecodeChannelFlowOk(R));
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
        AssertEquals('nome', 'eventos', LDecoded.ExchangeName);
        AssertEquals('tipo', 'topic', LDecoded.ExchangeType);
        AssertFalse('passive', LDecoded.Passive);
        AssertTrue('durable', LDecoded.Durable);
        AssertTrue('auto-delete', LDecoded.AutoDelete);
        AssertFalse('internal', LDecoded.Internal);
        AssertTrue('arg presente', LDecoded.Arguments.ContainsKey('alternate-exchange'));
        AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE_OK));
    DecodeExchangeDeclareOk(R); // decode do CLIENTE
    AssertTrue(R.EndOfData);
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
      AssertEquals('destination', 'dest', LDecoded.Destination);
      AssertEquals('source', 'src', LDecoded.Source);
      AssertEquals('routing-key', 'nota.#', LDecoded.RoutingKey);
      AssertTrue(R.EndOfData);
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
    AssertEquals('method-id', 51, Integer(LId.MethodId));
    AssertTrue(LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_UNBIND_OK));
    AssertTrue(R.EndOfData);
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
        AssertEquals('nome', 'fila.retry', LDecoded.QueueName);
        AssertTrue('durable', LDecoded.Durable);
        AssertTrue('exclusive', LDecoded.Exclusive);
        AssertFalse('auto-delete', LDecoded.AutoDelete);
        AssertTrue('arg presente', LDecoded.Arguments.ContainsKey('x-message-ttl'));
        AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK));
    LOk := DecodeQueueDeclareOk(R); // decode do CLIENTE
    AssertEquals('nome gerado', 'amq.gen-Ab3', LOk.QueueName);
    AssertEquals(QWord(5), QWord(LOk.MessageCount));
    AssertEquals(QWord(2), QWord(LOk.ConsumerCount));
    AssertTrue(R.EndOfData);
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
      AssertEquals('queue', 'q', LDecoded.QueueName);
      AssertEquals('exchange', 'ex', LDecoded.ExchangeName);
      AssertEquals('routing-key', 'rk', LDecoded.RoutingKey);
      AssertTrue(R.EndOfData);
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
      AssertEquals('queue', 'q', LDecoded.QueueName);
      AssertEquals('exchange', 'ex', LDecoded.ExchangeName);
      AssertEquals('routing-key', 'rk', LDecoded.RoutingKey);
      AssertTrue('sem no-wait: acaba direto nos arguments', R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_PURGE));
    LPurge := DecodeQueuePurge(R);
    AssertEquals('queue', 'q', LPurge.QueueName);
    AssertTrue('no-wait', LPurge.NoWait);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildQueuePurgeOk(42));
  try
    LId := ReadMethodHeader(R);
    AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_PURGE_OK));
    AssertEquals(QWord(42), QWord(DecodeQueuePurgeOk(R))); // decode do CLIENTE
    AssertTrue(R.EndOfData);
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
    AssertEquals('queue', 'q', LDecoded.QueueName);
    AssertTrue('if-unused', LDecoded.IfUnused);
    AssertTrue('if-empty', LDecoded.IfEmpty);
    AssertFalse('no-wait', LDecoded.NoWait);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildQueueDeleteOk(3));
  try
    LId := ReadMethodHeader(R);
    AssertTrue(LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DELETE_OK));
    AssertEquals(QWord(3), QWord(DecodeQueueDeleteOk(R)));
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS));
    LQos := DecodeBasicQos(R);
    AssertEquals(QWord(0), QWord(LQos.PrefetchSize));
    AssertEquals(20, Integer(LQos.PrefetchCount));
    AssertTrue('global', LQos.Global);
    AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildBasicQosOk);
  try
    LId := ReadMethodHeader(R);
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_QOS_OK));
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH));
    LPub := DecodeBasicPublish(R);
    AssertEquals('exchange', 'ex', LPub.Exchange);
    AssertEquals('routing-key', 'chave', LPub.RoutingKey);
    AssertTrue('mandatory', LPub.Mandatory);
    AssertFalse('immediate', LPub.Immediate);
    AssertTrue(R.EndOfData);
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
      AssertEquals('queue', 'fila', LDecoded.Queue);
      AssertEquals('consumer-tag', 'ctag-1', LDecoded.ConsumerTag);
      AssertFalse('no-local', LDecoded.NoLocal);
      AssertTrue('no-ack', LDecoded.NoAck);
      AssertTrue('exclusive', LDecoded.Exclusive);
      AssertFalse('no-wait', LDecoded.NoWait);
      AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME_OK));
    AssertEquals('ctag-9', DecodeBasicConsumeOk(R)); // decode do CLIENTE
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL));
    LCancel := DecodeBasicCancelArgs(R);
    AssertEquals('consumer-tag', 'ctag-1', LCancel.ConsumerTag);
    AssertTrue('no-wait', LCancel.NoWait);
    AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildBasicCancelOk('ctag-1'));
  try
    LId := ReadMethodHeader(R);
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL_OK));
    AssertEquals('ctag-1', DecodeBasicCancelOk(R)); // decode do CLIENTE
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET));
    LGet := DecodeBasicGet(R);
    AssertEquals('queue', 'fila', LGet.Queue);
    AssertFalse('no-ack', LGet.NoAck);
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK));
    LDecoded := DecodeBasicGetOk(R); // decode do CLIENTE
    AssertEquals(QWord(77), QWord(LDecoded.DeliveryTag));
    AssertTrue('redelivered', LDecoded.Redelivered);
    AssertEquals('ex', LDecoded.Exchange);
    AssertEquals('rk', LDecoded.RoutingKey);
    AssertEquals(QWord(4), QWord(LDecoded.MessageCount));
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_EMPTY));
    AssertEquals('reserved-1', '', R.ReadShortStr);
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER));
    LDecoded := DecodeBasicDeliver(R); // decode do CLIENTE
    AssertEquals('ctag', LDecoded.ConsumerTag);
    AssertEquals(QWord(12), QWord(LDecoded.DeliveryTag));
    AssertFalse(LDecoded.Redelivered);
    AssertEquals('ex', LDecoded.Exchange);
    AssertEquals('rk', LDecoded.RoutingKey);
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN));
    LDecoded := DecodeBasicReturn(R); // decode do CLIENTE
    AssertEquals(312, Integer(LDecoded.ReplyCode));
    AssertEquals('NO_ROUTE', LDecoded.ReplyText);
    AssertEquals('ex', LDecoded.Exchange);
    AssertEquals('rk', LDecoded.RoutingKey);
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_REJECT));
    LReject := DecodeBasicReject(R);
    AssertEquals(QWord(55), QWord(LReject.DeliveryTag));
    AssertFalse('requeue', LReject.Requeue);
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT));
    AssertTrue('no-wait', DecodeConfirmSelect(R));
    AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  R := TAMQPReader.Create(BuildConfirmSelectOk);
  try
    LId := ReadMethodHeader(R);
    AssertTrue(LId.Matches(AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT_OK));
    AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

initialization
  RegisterTest(TChannelQueueExchangeTests);
  RegisterTest(TContentHeaderTests);
  RegisterTest(TServerCodecTests);

end.
