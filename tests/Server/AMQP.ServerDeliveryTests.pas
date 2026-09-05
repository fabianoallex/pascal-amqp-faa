unit AMQP.ServerDeliveryTests;

{ Testes da entrega -- WS4 da Fase 2 (ver CLAUDE.md).

  Duas fixtures, porque sao dois assuntos distintos:

  - TQueueDeliveryTests: o RODIZIO do ator da fila, com um alvo FALSO
    (TAlvoFalso). O que se testa aqui e' a politica -- quem recebe, em que
    ordem, o que acontece quando um consumidor recusa (D3) e para onde vao as
    nao-confirmadas. Nenhum byte de wire.
  - TDeliveryTargetTests: o alvo DE VERDADE (TAMQPChannelDeliveryTarget) sobre
    um TAMQPFrameWriter real escrevendo num stream de teste. Aqui se testa o
    wire: a sequencia method+header+body, o fatiamento pelo frame-max, as
    delivery-tags, o prefetch, o Channel.Flow, o detach e a recusa sem
    bloquear quando a fila de escrita enche.

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerDeliveryTests.pas -- mantenha
  os dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Frame,
  AMQP.Basic.Methods,
  AMQP.Threading,
  AMQP.Server.FrameIO,
  AMQP.Server.Message,
  AMQP.Server.Queue,
  AMQP.Server.Channel,
  AMQP.Server.Delivery,
  AMQP.ServerTestDoubles;

type
  [TestFixture]
  TQueueDeliveryTests = class
  public
    [Test] procedure Consumidor_RecebeAoPublicar;
    [Test] procedure ConsumidorNovo_RecebeOQueJaEstavaNaFila;
    [Test] procedure NoAck_NaoDeixaNaoConfirmada;
    [Test] procedure Rodizio_AlternaEntreConsumidores;
    [Test] procedure ConsumidorQueRecusa_NaoTravaOsOutros;
    [Test] procedure TodosRecusam_MensagemFicaNaFila;
    [Test] procedure DeliverTick_RetomaDepoisDaRecusa;
    [Test] procedure AlvoMorto_ConsumidorSaiEMensagemFica;
    [Test] procedure Entrega_MarcaRedeliveredNaSegundaVez;
    [Test] procedure RemoveChannel_DevolveEntregaNaoConfirmada;
  end;

  [TestFixture]
  TDeliveryTargetTests = class
  public
    [Test] procedure Emite_DeliverHeaderEBody;
    [Test] procedure FatiaCorpoPeloFrameMax;
    [Test] procedure TagsMonotonicasPorCanal;
    [Test] procedure PrefetchBloqueia_AckLibera;
    [Test] procedure RecusaNaoConsomeTag;
    [Test] procedure NoAck_NaoContaPrefetch;
    [Test] procedure FlowDesligado_Recusa;
    [Test] procedure NoteResolved_MultipleResolvePrefixo;
    [Test] procedure Detach_RecusaEDeixaDeEstarVivo;
    [Test] procedure FilaDeEscritaCheia_RecusaSemBloquear;
    [Test] procedure TryPostFrames_LoteAtomicoOuNada;
    [Test] procedure Canal_LiberadoDetachaOAlvo;
    [Test] procedure Canal_PropagaFlowEPrefetchParaOAlvo;
  end;

implementation

{ --- helpers comuns --- }

function NovaMensagem(const ATexto: string): TAMQPMessage;
begin
  Result := TAMQPMessage.Create('ex', 'rk', 'guest', nil,
    AmqpUtf8Encode(ATexto));
end;

// Publica ATexto na fila e solta a referencia do publicador.
procedure Publicar(AQueue: TAMQPServerQueue; const ATexto: string);
var
  LMsg: TAMQPMessage;
begin
  LMsg := NovaMensagem(ATexto);
  try
    AQueue.PostMessage(LMsg);
  finally
    LMsg.Release;
  end;
end;

{ --- TQueueDeliveryTests --- }

procedure TQueueDeliveryTests.Consumidor_RecebeAoPublicar;
var
  LQ: TAMQPServerQueue;
  LAlvo: TAlvoFalso;
  LRef: IAMQPDeliveryTarget;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LAlvo := TAlvoFalso.Create(1);
    LRef := LAlvo;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', False, False, LRef));
    Publicar(LQ, 'a');

    LStats := LQ.Stats; // barreira: a caixa e serial
    Assert.AreEqual(1, LAlvo.Count, 'entregou');
    Assert.AreEqual('a', LAlvo.Entrega(0).Corpo, 'corpo');
    Assert.AreEqual('ct', LAlvo.Entrega(0).ConsumerTag, 'consumer tag');
    Assert.AreEqual('q', LAlvo.Entrega(0).QueueName, 'nome da fila');
    Assert.IsFalse(LAlvo.Entrega(0).Redelivered, 'primeira entrega');
    Assert.AreEqual(0, LStats.MessageCount, 'saiu das prontas');
    Assert.AreEqual(1, LStats.UnackedCount, 'virou nao-confirmada');
    Assert.AreEqual(1, Integer(LStats.DeliveredCount), 'contou a entrega');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.ConsumidorNovo_RecebeOQueJaEstavaNaFila;
var
  LQ: TAMQPServerQueue;
  LAlvo: TAlvoFalso;
  LRef: IAMQPDeliveryTarget;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    Publicar(LQ, 'a');
    Publicar(LQ, 'b');
    Assert.AreEqual(2, LQ.Stats.MessageCount, 'acumulou sem consumidor');

    LAlvo := TAlvoFalso.Create(1);
    LRef := LAlvo;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', True, False, LRef));
    Assert.AreEqual(0, LQ.Stats.MessageCount, 'drenou ao entrar o consumidor');
    Assert.AreEqual('a|b', LAlvo.Corpos, 'na ordem da fila');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.NoAck_NaoDeixaNaoConfirmada;
var
  LQ: TAMQPServerQueue;
  LAlvo: TAlvoFalso;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LAlvo := TAlvoFalso.Create(1);
    LRef := LAlvo;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', True, False, LRef));

    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
      Assert.AreEqual(0, LQ.Stats.UnackedCount, 'no-ack nao gera pendencia');
      Assert.AreEqual(1, LAlvo.Count, 'entregou');
      Assert.AreEqual(1, LMsg.RefCount, 'a fila soltou a referencia dela');
    finally
      LMsg.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.Rodizio_AlternaEntreConsumidores;
var
  LQ: TAMQPServerQueue;
  LA, LB: TAlvoFalso;
  LRefA, LRefB: IAMQPDeliveryTarget;
  I: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(1);
    LRefA := LA;
    LB := TAlvoFalso.Create(2);
    LRefB := LB;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ca', True, False, LRefA));
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('cb', True, False, LRefB));

    for I := 1 to 4 do
      Publicar(LQ, 'm' + IntToStr(I));
    LQ.Stats; // barreira

    Assert.AreEqual(2, LA.Count, 'metade para o primeiro');
    Assert.AreEqual(2, LB.Count, 'metade para o segundo');
    Assert.AreEqual('m1|m3', LA.Corpos, 'alternou');
    Assert.AreEqual('m2|m4', LB.Corpos, 'alternou');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.ConsumidorQueRecusa_NaoTravaOsOutros;
var
  LQ: TAMQPServerQueue;
  LA, LB: TAlvoFalso;
  LRefA, LRefB: IAMQPDeliveryTarget;
  I: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(1);
    LA.Recusar := True; // consumidor lento: fila outbound cheia (D3)
    LRefA := LA;
    LB := TAlvoFalso.Create(2);
    LRefB := LB;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ca', True, False, LRefA));
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('cb', True, False, LRefB));

    for I := 1 to 3 do
      Publicar(LQ, 'm' + IntToStr(I));

    Assert.AreEqual(0, LQ.Stats.MessageCount, 'nada ficou preso');
    Assert.AreEqual(0, LA.Count, 'o que recusa nao recebe');
    Assert.AreEqual('m1|m2|m3', LB.Corpos,
      'o outro consumidor recebe tudo, sem head-of-line');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.TodosRecusam_MensagemFicaNaFila;
var
  LQ: TAMQPServerQueue;
  LA: TAlvoFalso;
  LRefA: IAMQPDeliveryTarget;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(1);
    LA.Recusar := True;
    LRefA := LA;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ca', True, False, LRefA));

    Publicar(LQ, 'a');
    Publicar(LQ, 'b');
    Assert.AreEqual(2, LQ.Stats.MessageCount, 'ficaram na fila');
    Assert.AreEqual(0, LA.Count, 'nada entregue');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.DeliverTick_RetomaDepoisDaRecusa;
var
  LQ: TAMQPServerQueue;
  LA: TAlvoFalso;
  LRefA: IAMQPDeliveryTarget;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(1);
    LA.Recusar := True;
    LRefA := LA;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ca', True, False, LRefA));
    Publicar(LQ, 'a');
    Publicar(LQ, 'b');
    Assert.AreEqual(2, LQ.Stats.MessageCount, 'recusadas');

    // O canal drenou. Sem ack nenhum (consumidor em no-ack), o unico jeito de
    // a fila reexaminar e' o tick -- e' o caso degradado da D3.
    LA.Recusar := False;
    LQ.PostDeliverTick;
    Assert.AreEqual(0, LQ.Stats.MessageCount, 'o tick retomou a entrega');
    Assert.AreEqual('a|b', LA.Corpos, 'na ordem, sem perder nenhuma');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.AlvoMorto_ConsumidorSaiEMensagemFica;
var
  LQ: TAMQPServerQueue;
  LA: TAlvoFalso;
  LRefA: IAMQPDeliveryTarget;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(1);
    LRefA := LA;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ca', True, False, LRefA));
    LA.Vivo := False; // canal/conexao morreu (Detach)

    Publicar(LQ, 'a');
    LStats := LQ.Stats;
    Assert.AreEqual(0, LStats.ConsumerCount, 'consumidor de canal morto sai');
    Assert.AreEqual(1, LStats.MessageCount, 'e a mensagem continua na fila');
    Assert.AreEqual(0, LA.Count, 'nada foi entregue');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.Entrega_MarcaRedeliveredNaSegundaVez;
var
  LQ: TAMQPServerQueue;
  LA: TAlvoFalso;
  LRefA: IAMQPDeliveryTarget;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(7);
    LRefA := LA;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ca', False, False, LRefA));
    Publicar(LQ, 'a');
    LQ.Stats; // barreira: a caixa e serial, o enqueue ja rodou
    Assert.AreEqual(1, LA.Count, 'primeira entrega');
    Assert.IsFalse(LA.Entrega(0).Redelivered, 'nao e reentrega');

    // O consumidor devolve: a mensagem volta e sai de novo, agora marcada.
    LQ.PostNack(7, LA.Entrega(0).DeliveryTag, False, True);
    LQ.Stats; // barreira
    Assert.AreEqual(2, LA.Count, 'entregue de novo');
    Assert.IsTrue(LA.Entrega(1).Redelivered, 'agora e reentrega');
    Assert.AreEqual('a', LA.Entrega(1).Corpo, 'mesma mensagem');
  finally
    LQ.Free;
  end;
end;

procedure TQueueDeliveryTests.RemoveChannel_DevolveEntregaNaoConfirmada;
var
  LQ: TAMQPServerQueue;
  LA: TAlvoFalso;
  LRefA: IAMQPDeliveryTarget;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(42);
    LRefA := LA;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ca', False, False, LRefA));
    Publicar(LQ, 'a');
    Assert.AreEqual(1, LQ.Stats.UnackedCount, 'entregue e nao confirmada');

    // A conexao caiu antes do ack: a mensagem tem de voltar para a fila
    // EXATAMENTE uma vez.
    LA.Vivo := False;
    LQ.PostRemoveChannel(42);
    LStats := LQ.Stats;
    Assert.AreEqual(0, LStats.UnackedCount, 'saiu das nao-confirmadas');
    Assert.AreEqual(1, LStats.MessageCount, 'voltou para as prontas');
    Assert.AreEqual(0, LStats.ConsumerCount, 'e o consumidor foi embora');
    Assert.AreEqual(1, LA.Count, 'nao foi reentregue no canal morto');
  finally
    LQ.Free;
  end;
end;

{ --- helpers do alvo de verdade --- }



{ --- TDeliveryTargetTests --- }

procedure TDeliveryTargetTests.Emite_DeliverHeaderEBody;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
  LFrames: TArray<TAMQPFrame>;
  LReader: TAMQPReader;
  LDeliver: TAMQPBasicDeliver;
  LHeaderCru: TBytes;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 3, 0);
    LRef := LAlvo;
    // Header cru como o que viria de um Basic.Publish (decisao D1: reemitido
    // byte a byte).
    LHeaderCru := BuildContentHeader(5, TAMQPBasicProperties.Empty);
    LMsg := TAMQPMessage.Create('ex1', 'rk1', 'guest', LHeaderCru,
      AmqpUtf8Encode('hello'));
    try
      Assert.IsTrue(LAlvo.TryDeliver('ctag', 'fila', True, False, LMsg, LTag),
        'entregou');
      Assert.AreEqual(1, Integer(LTag), 'primeira tag do canal e 1');
    finally
      LMsg.Release;
    end;

    LFrames := FramesEscritos(LWriter, LStream);
    Assert.AreEqual(3, Length(LFrames), 'method + header + body');
    Assert.AreEqual(Integer(AMQP_FRAME_METHOD), Integer(LFrames[0].FrameType), 'tipo do 1o');
    Assert.AreEqual(Integer(AMQP_FRAME_HEADER), Integer(LFrames[1].FrameType), 'tipo do 2o');
    Assert.AreEqual(Integer(AMQP_FRAME_BODY), Integer(LFrames[2].FrameType), 'tipo do 3o');
    Assert.AreEqual(3, Integer(LFrames[0].Channel), 'canal do method');
    Assert.AreEqual(3, Integer(LFrames[2].Channel), 'canal do body');

    LReader := TAMQPReader.Create(LFrames[0].Payload);
    try
      LReader.ReadShortUInt; // class-id
      LReader.ReadShortUInt; // method-id
      LDeliver := DecodeBasicDeliver(LReader);
    finally
      LReader.Free;
    end;
    Assert.AreEqual('ctag', LDeliver.ConsumerTag, 'consumer tag');
    Assert.AreEqual(1, Integer(LDeliver.DeliveryTag), 'delivery tag');
    Assert.AreEqual('ex1', LDeliver.Exchange, 'exchange');
    Assert.AreEqual('rk1', LDeliver.RoutingKey, 'routing key');
    Assert.IsFalse(LDeliver.Redelivered, 'redelivered');

    Assert.AreEqual(AmqpUtf8Decode(LHeaderCru),
      AmqpUtf8Decode(LFrames[1].Payload),
      'o content-header sai byte a byte como entrou (D1)');
    Assert.AreEqual('hello', AmqpUtf8Decode(LFrames[2].Payload), 'corpo');
  finally
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.FatiaCorpoPeloFrameMax;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
  LFrames: TArray<TAMQPFrame>;
  LCorpo, LJunto: string;
  I: Integer;
begin
  LCorpo := StringOfChar('x', 250);
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 100);
    LRef := LAlvo;
    LMsg := NovaMensagem(LCorpo);
    try
      Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag), 'entregou');
    finally
      LMsg.Release;
    end;

    LFrames := FramesEscritos(LWriter, LStream);
    Assert.AreEqual(5, Length(LFrames), 'method + header + 3 body (250/100)');
    LJunto := '';
    for I := 2 to High(LFrames) do
    begin
      Assert.IsTrue(Length(LFrames[I].Payload) <= 100,
        'nenhum body frame passa do frame-max');
      LJunto := LJunto + AmqpUtf8Decode(LFrames[I].Payload);
    end;
    Assert.AreEqual(LCorpo, LJunto, 'corpo remontado byte a byte');
  finally
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.TagsMonotonicasPorCanal;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
  I: Integer;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    for I := 1 to 3 do
    begin
      LMsg := NovaMensagem('m');
      try
        Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag),
          'entrega ' + IntToStr(I));
        Assert.AreEqual(I, Integer(LTag), 'tag monotonica');
      finally
        LMsg.Release;
      end;
    end;
    Assert.AreEqual(4, Integer(LAlvo.NextDeliveryTag),
      'o Basic.Get puxa da mesma sequencia do canal');
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.PrefetchBloqueia_AckLibera;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    LAlvo.SetPrefetch(2);

    LMsg := NovaMensagem('m');
    try
      Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag), '1a');
      Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag), '2a');
      Assert.IsFalse(LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag),
        '3a recusada: prefetch cheio');
      Assert.AreEqual(2, LAlvo.UnackedCount, 'duas pendentes');

      LAlvo.NoteResolved(1, False); // o cliente confirmou a tag 1
      Assert.AreEqual(1, LAlvo.UnackedCount, 'liberou uma');
      Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag),
        'com vaga, entrega de novo');
      Assert.AreEqual(3, Integer(LTag), 'a tag seguiu de 2 para 3');
    finally
      LMsg.Release;
    end;
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.RecusaNaoConsomeTag;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    LMsg := NovaMensagem('m');
    try
      Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag), '1a');
      Assert.AreEqual(1, Integer(LTag), 'tag 1');

      LAlvo.SetActive(False); // recusa por flow
      Assert.IsFalse(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag), 'recusou');
      LAlvo.SetActive(True);

      Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag), '2a');
      Assert.AreEqual(2, Integer(LTag),
        'a recusa nao abriu buraco na sequencia do canal');
    finally
      LMsg.Release;
    end;
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.NoAck_NaoContaPrefetch;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
  I: Integer;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    LAlvo.SetPrefetch(1);
    LMsg := NovaMensagem('m');
    try
      for I := 1 to 5 do
        Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag),
          'entrega ' + IntToStr(I) + ' em no-ack');
      Assert.AreEqual(0, LAlvo.UnackedCount,
        'entrega em no-ack nunca fica pendente');
    finally
      LMsg.Release;
    end;
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.FlowDesligado_Recusa;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    LMsg := NovaMensagem('m');
    try
      LAlvo.SetActive(False);
      Assert.IsFalse(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag),
        'Channel.Flow desligado recusa');
      LAlvo.SetActive(True);
      Assert.IsTrue(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag),
        'religado, entrega');
    finally
      LMsg.Release;
    end;
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.NoteResolved_MultipleResolvePrefixo;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
  I: Integer;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    LMsg := NovaMensagem('m');
    try
      for I := 1 to 4 do
        LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag);
      Assert.AreEqual(4, LAlvo.UnackedCount, 'quatro pendentes');

      Assert.AreEqual(2, Length(LAlvo.NoteResolved(2, True)), 'multiple ate a tag 2');
      Assert.AreEqual(2, LAlvo.UnackedCount, 'sobraram duas');
      Assert.AreEqual(0, Length(LAlvo.NoteResolved(2, False)),
        'tag ja resolvida nao conta de novo');
      Assert.AreEqual(2, Length(LAlvo.NoteResolved(0, True)),
        'multiple com tag 0 resolve todas as pendentes');
      Assert.AreEqual(0, LAlvo.UnackedCount, 'nada pendente');
    finally
      LMsg.Release;
    end;
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.Detach_RecusaEDeixaDeEstarVivo;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    Assert.IsTrue(LAlvo.IsAlive, 'nasce vivo');

    LAlvo.Detach;
    Assert.IsFalse(LAlvo.IsAlive, 'depois do detach, morto');

    LMsg := NovaMensagem('m');
    try
      Assert.IsFalse(LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag),
        'nao entrega depois do detach');
    finally
      LMsg.Release;
    end;
    LAlvo.Detach; // idempotente
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
    // O alvo (LRef) so' morre aqui, DEPOIS do writer -- e' exatamente a
    // situacao que o refcount+detach existe para tornar segura.
  end;
end;

procedure TDeliveryTargetTests.FilaDeEscritaCheia_RecusaSemBloquear;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LAlvo: TAMQPChannelDeliveryTarget;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
  I: Integer;
  LRecusou: Boolean;
  LInicio: UInt64;
begin
  LStream := TStreamGravador.Create;
  LStream.Bloquear; // a escritora trava no primeiro Write: a fila enche
  LWriter := TAMQPFrameWriter.Create(LStream, 8);
  try
    LAlvo := TAMQPChannelDeliveryTarget.Create(LWriter, 1, 0);
    LRef := LAlvo;
    LMsg := NovaMensagem('m');
    try
      LRecusou := False;
      LInicio := AmqpTickMs;
      for I := 1 to 50 do
        if not LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag) then
        begin
          LRecusou := True;
          Break;
        end;
      Assert.IsTrue(LRecusou, 'com a fila de escrita cheia, o alvo recusa');
      Assert.IsTrue(AmqpTickMs - LInicio < 2000,
        'e recusa SEM BLOQUEAR -- e o que a D2/D3 exige do ator');
    finally
      LMsg.Release;
    end;
  finally
    LStream.Liberar; // solta a escritora antes de encerrar
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.TryPostFrames_LoteAtomicoOuNada;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LLote: array[0..2] of TAMQPFrame;
  I: Integer;
  LCoube: Boolean;
  LFrames: TArray<TAMQPFrame>;
begin
  LStream := TStreamGravador.Create;
  LStream.Bloquear;
  LWriter := TAMQPFrameWriter.Create(LStream, 4);
  try
    for I := 0 to 2 do
      LLote[I] := TAMQPFrame.Create(AMQP_FRAME_BODY, 1,
        AmqpUtf8Encode('lote' + IntToStr(I)));

    // Enche a fila (a escritora esta travada no Write).
    LCoube := True;
    for I := 1 to 20 do
      if not LWriter.TryPostFrames([TAMQPFrame.Create(AMQP_FRAME_BODY, 1,
        AmqpUtf8Encode('x'))]) then
      begin
        LCoube := False;
        Break;
      end;
    Assert.IsFalse(LCoube, 'a fila de escrita encheu');
    Assert.IsFalse(LWriter.TryPostFrames(LLote),
      'lote nao cabe: recusa inteiro');

    LStream.Liberar;
    LFrames := FramesEscritos(LWriter, LStream);
    for I := 0 to High(LFrames) do
      Assert.AreEqual('x', AmqpUtf8Decode(LFrames[I].Payload),
        'nenhum frame do lote recusado vazou para o wire');
  finally
    LStream.Liberar;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.Canal_LiberadoDetachaOAlvo;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LCanal: TAMQPServerChannel;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LCanal := TAMQPServerChannel.Create(5);
    LCanal.AttachDelivery(LWriter, 0);
    LRef := LCanal.DeliveryTarget; // e' o que uma fila guardaria no consumidor
    Assert.IsTrue(LRef.IsAlive, 'alvo vivo com o canal aberto');

    // O canal morre (Channel.Close, queda da conexao, reaper...). A fila ainda
    // segura a interface -- e e' exatamente por isso que o objeto NAO pode
    // estar liberado, nem continuar escrevendo num writer que vai embora.
    LCanal.Free;

    Assert.IsFalse(LRef.IsAlive,
      'liberar o canal tem de destacar o alvo, senao a fila entrega num writer morto');
    LMsg := NovaMensagem('m');
    try
      Assert.IsFalse(LRef.TryDeliver('ct', 'q', True, False, LMsg, LTag),
        'e toda entrega posterior falha limpo');
    finally
      LMsg.Release;
    end;
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TDeliveryTargetTests.Canal_PropagaFlowEPrefetchParaOAlvo;
var
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LCanal: TAMQPServerChannel;
  LRef: IAMQPDeliveryTarget;
  LMsg: TAMQPMessage;
  LTag: UInt64;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LCanal := TAMQPServerChannel.Create(1);
    try
      LCanal.AttachDelivery(LWriter, 0);
      LRef := LCanal.DeliveryTarget;
      LMsg := NovaMensagem('m');
      try
        // Channel.Flow chega ao alvo.
        LCanal.Active := False;
        Assert.IsFalse(LRef.TryDeliver('ct', 'q', True, False, LMsg, LTag),
          'Channel.Flow=False para a entrega');
        LCanal.Active := True;

        // Basic.Qos tambem.
        LCanal.PrefetchCount := 1;
        Assert.IsTrue(LRef.TryDeliver('ct', 'q', False, False, LMsg, LTag), '1a');
        Assert.IsFalse(LRef.TryDeliver('ct', 'q', False, False, LMsg, LTag),
          'prefetch do canal vale para o alvo');
      finally
        LMsg.Release;
      end;
    finally
      LCanal.Free;
    end;
  finally
    LWriter.Stop;
    LWriter.Free;
    LStream.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TQueueDeliveryTests);
  TDUnitX.RegisterTestFixture(TDeliveryTargetTests);

end.
