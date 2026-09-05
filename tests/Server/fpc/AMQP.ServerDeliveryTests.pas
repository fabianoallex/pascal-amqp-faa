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

  Espelho DUnitX de tests\Server\AMQP.ServerDeliveryTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry,
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
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
  TQueueDeliveryTests = class(TTestCase)
  published
    procedure Consumidor_RecebeAoPublicar;
    procedure ConsumidorNovo_RecebeOQueJaEstavaNaFila;
    procedure NoAck_NaoDeixaNaoConfirmada;
    procedure Rodizio_AlternaEntreConsumidores;
    procedure ConsumidorQueRecusa_NaoTravaOsOutros;
    procedure TodosRecusam_MensagemFicaNaFila;
    procedure DeliverTick_RetomaDepoisDaRecusa;
    procedure AlvoMorto_ConsumidorSaiEMensagemFica;
    procedure Entrega_MarcaRedeliveredNaSegundaVez;
    procedure RemoveChannel_DevolveEntregaNaoConfirmada;
  end;

  TDeliveryTargetTests = class(TTestCase)
  published
    procedure Emite_DeliverHeaderEBody;
    procedure FatiaCorpoPeloFrameMax;
    procedure TagsMonotonicasPorCanal;
    procedure PrefetchBloqueia_AckLibera;
    procedure RecusaNaoConsomeTag;
    procedure NoAck_NaoContaPrefetch;
    procedure FlowDesligado_Recusa;
    procedure NoteResolved_MultipleResolvePrefixo;
    procedure Detach_RecusaEDeixaDeEstarVivo;
    procedure FilaDeEscritaCheia_RecusaSemBloquear;
    procedure TryPostFrames_LoteAtomicoOuNada;
    procedure Canal_LiberadoDetachaOAlvo;
    procedure Canal_PropagaFlowEPrefetchParaOAlvo;
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
    AssertEquals('entregou', 1, LAlvo.Count);
    AssertEquals('corpo', 'a', LAlvo.Entrega(0).Corpo);
    AssertEquals('consumer tag', 'ct', LAlvo.Entrega(0).ConsumerTag);
    AssertEquals('nome da fila', 'q', LAlvo.Entrega(0).QueueName);
    AssertFalse('primeira entrega', LAlvo.Entrega(0).Redelivered);
    AssertEquals('saiu das prontas', 0, LStats.MessageCount);
    AssertEquals('virou nao-confirmada', 1, LStats.UnackedCount);
    AssertEquals('contou a entrega', 1, Integer(LStats.DeliveredCount));
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
    AssertEquals('acumulou sem consumidor', 2, LQ.Stats.MessageCount);

    LAlvo := TAlvoFalso.Create(1);
    LRef := LAlvo;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', True, False, LRef));
    AssertEquals('drenou ao entrar o consumidor', 0, LQ.Stats.MessageCount);
    AssertEquals('na ordem da fila', 'a|b', LAlvo.Corpos);
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
      AssertEquals('no-ack nao gera pendencia', 0, LQ.Stats.UnackedCount);
      AssertEquals('entregou', 1, LAlvo.Count);
      AssertEquals('a fila soltou a referencia dela', 1, LMsg.RefCount);
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

    AssertEquals('metade para o primeiro', 2, LA.Count);
    AssertEquals('metade para o segundo', 2, LB.Count);
    AssertEquals('alternou', 'm1|m3', LA.Corpos);
    AssertEquals('alternou', 'm2|m4', LB.Corpos);
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

    AssertEquals('nada ficou preso', 0, LQ.Stats.MessageCount);
    AssertEquals('o que recusa nao recebe', 0, LA.Count);
    AssertEquals('o outro consumidor recebe tudo, sem head-of-line', 'm1|m2|m3', LB.Corpos);
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
    AssertEquals('ficaram na fila', 2, LQ.Stats.MessageCount);
    AssertEquals('nada entregue', 0, LA.Count);
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
    AssertEquals('recusadas', 2, LQ.Stats.MessageCount);

    // O canal drenou. Sem ack nenhum (consumidor em no-ack), o unico jeito de
    // a fila reexaminar e' o tick -- e' o caso degradado da D3.
    LA.Recusar := False;
    LQ.PostDeliverTick;
    AssertEquals('o tick retomou a entrega', 0, LQ.Stats.MessageCount);
    AssertEquals('na ordem, sem perder nenhuma', 'a|b', LA.Corpos);
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
    AssertEquals('consumidor de canal morto sai', 0, LStats.ConsumerCount);
    AssertEquals('e a mensagem continua na fila', 1, LStats.MessageCount);
    AssertEquals('nada foi entregue', 0, LA.Count);
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
    AssertEquals('primeira entrega', 1, LA.Count);
    AssertFalse('nao e reentrega', LA.Entrega(0).Redelivered);

    // O consumidor devolve: a mensagem volta e sai de novo, agora marcada.
    LQ.PostNack(7, LA.Entrega(0).DeliveryTag, False, True);
    LQ.Stats; // barreira
    AssertEquals('entregue de novo', 2, LA.Count);
    AssertTrue('agora e reentrega', LA.Entrega(1).Redelivered);
    AssertEquals('mesma mensagem', 'a', LA.Entrega(1).Corpo);
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
    AssertEquals('entregue e nao confirmada', 1, LQ.Stats.UnackedCount);

    // A conexao caiu antes do ack: a mensagem tem de voltar para a fila
    // EXATAMENTE uma vez.
    LA.Vivo := False;
    LQ.PostRemoveChannel(42);
    LStats := LQ.Stats;
    AssertEquals('saiu das nao-confirmadas', 0, LStats.UnackedCount);
    AssertEquals('voltou para as prontas', 1, LStats.MessageCount);
    AssertEquals('e o consumidor foi embora', 0, LStats.ConsumerCount);
    AssertEquals('nao foi reentregue no canal morto', 1, LA.Count);
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
      AssertTrue('entregou', LAlvo.TryDeliver('ctag', 'fila', True, False, LMsg, LTag));
      AssertEquals('primeira tag do canal e 1', 1, Integer(LTag));
    finally
      LMsg.Release;
    end;

    LFrames := FramesEscritos(LWriter, LStream);
    AssertEquals('method + header + body', 3, Length(LFrames));
    AssertEquals('tipo do 1o', Integer(AMQP_FRAME_METHOD), Integer(LFrames[0].FrameType));
    AssertEquals('tipo do 2o', Integer(AMQP_FRAME_HEADER), Integer(LFrames[1].FrameType));
    AssertEquals('tipo do 3o', Integer(AMQP_FRAME_BODY), Integer(LFrames[2].FrameType));
    AssertEquals('canal do method', 3, Integer(LFrames[0].Channel));
    AssertEquals('canal do body', 3, Integer(LFrames[2].Channel));

    LReader := TAMQPReader.Create(LFrames[0].Payload);
    try
      LReader.ReadShortUInt; // class-id
      LReader.ReadShortUInt; // method-id
      LDeliver := DecodeBasicDeliver(LReader);
    finally
      LReader.Free;
    end;
    AssertEquals('consumer tag', 'ctag', LDeliver.ConsumerTag);
    AssertEquals('delivery tag', 1, Integer(LDeliver.DeliveryTag));
    AssertEquals('exchange', 'ex1', LDeliver.Exchange);
    AssertEquals('routing key', 'rk1', LDeliver.RoutingKey);
    AssertFalse('redelivered', LDeliver.Redelivered);

    AssertEquals('o content-header sai byte a byte como entrou (D1)', AmqpUtf8Decode(LHeaderCru), AmqpUtf8Decode(LFrames[1].Payload));
    AssertEquals('corpo', 'hello', AmqpUtf8Decode(LFrames[2].Payload));
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
      AssertTrue('entregou', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
    finally
      LMsg.Release;
    end;

    LFrames := FramesEscritos(LWriter, LStream);
    AssertEquals('method + header + 3 body (250/100)', 5, Length(LFrames));
    LJunto := '';
    for I := 2 to High(LFrames) do
    begin
      AssertTrue('nenhum body frame passa do frame-max', Length(LFrames[I].Payload) <= 100);
      LJunto := LJunto + AmqpUtf8Decode(LFrames[I].Payload);
    end;
    AssertEquals('corpo remontado byte a byte', LCorpo, LJunto);
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
        AssertTrue('entrega ' + IntToStr(I), LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
        AssertEquals('tag monotonica', I, Integer(LTag));
      finally
        LMsg.Release;
      end;
    end;
    AssertEquals('o Basic.Get puxa da mesma sequencia do canal', 4, Integer(LAlvo.NextDeliveryTag));
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
      AssertTrue('1a', LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag));
      AssertTrue('2a', LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag));
      AssertFalse('3a recusada: prefetch cheio', LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag));
      AssertEquals('duas pendentes', 2, LAlvo.UnackedCount);

      LAlvo.NoteResolved(1, False); // o cliente confirmou a tag 1
      AssertEquals('liberou uma', 1, LAlvo.UnackedCount);
      AssertTrue('com vaga, entrega de novo', LAlvo.TryDeliver('ct', 'q', False, False, LMsg, LTag));
      AssertEquals('a tag seguiu de 2 para 3', 3, Integer(LTag));
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
      AssertTrue('1a', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
      AssertEquals('tag 1', 1, Integer(LTag));

      LAlvo.SetActive(False); // recusa por flow
      AssertFalse('recusou', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
      LAlvo.SetActive(True);

      AssertTrue('2a', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
      AssertEquals('a recusa nao abriu buraco na sequencia do canal', 2, Integer(LTag));
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
        AssertTrue('entrega ' + IntToStr(I) + ' em no-ack', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
      AssertEquals('entrega em no-ack nunca fica pendente', 0, LAlvo.UnackedCount);
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
      AssertFalse('Channel.Flow desligado recusa', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
      LAlvo.SetActive(True);
      AssertTrue('religado, entrega', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
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
      AssertEquals('quatro pendentes', 4, LAlvo.UnackedCount);

      AssertEquals('multiple ate a tag 2', 2, Length(LAlvo.NoteResolved(2, True)));
      AssertEquals('sobraram duas', 2, LAlvo.UnackedCount);
      AssertEquals('tag ja resolvida nao conta de novo', 0, Length(LAlvo.NoteResolved(2, False)));
      AssertEquals('multiple com tag 0 resolve todas as pendentes', 2, Length(LAlvo.NoteResolved(0, True)));
      AssertEquals('nada pendente', 0, LAlvo.UnackedCount);
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
    AssertTrue('nasce vivo', LAlvo.IsAlive);

    LAlvo.Detach;
    AssertFalse('depois do detach, morto', LAlvo.IsAlive);

    LMsg := NovaMensagem('m');
    try
      AssertFalse('nao entrega depois do detach', LAlvo.TryDeliver('ct', 'q', True, False, LMsg, LTag));
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
      AssertTrue('com a fila de escrita cheia, o alvo recusa', LRecusou);
      AssertTrue('e recusa SEM BLOQUEAR -- e o que a D2/D3 exige do ator', AmqpTickMs - LInicio < 2000);
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
    AssertFalse('a fila de escrita encheu', LCoube);
    AssertFalse('lote nao cabe: recusa inteiro', LWriter.TryPostFrames(LLote));

    LStream.Liberar;
    LFrames := FramesEscritos(LWriter, LStream);
    for I := 0 to High(LFrames) do
      AssertEquals('nenhum frame do lote recusado vazou para o wire', 'x', AmqpUtf8Decode(LFrames[I].Payload));
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
    AssertTrue('alvo vivo com o canal aberto', LRef.IsAlive);

    // O canal morre (Channel.Close, queda da conexao, reaper...). A fila ainda
    // segura a interface -- e e' exatamente por isso que o objeto NAO pode
    // estar liberado, nem continuar escrevendo num writer que vai embora.
    LCanal.Free;

    AssertFalse('liberar o canal tem de destacar o alvo, senao a fila entrega num writer morto',
      LRef.IsAlive);
    LMsg := NovaMensagem('m');
    try
      AssertFalse('e toda entrega posterior falha limpo',
        LRef.TryDeliver('ct', 'q', True, False, LMsg, LTag));
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
        AssertFalse('Channel.Flow=False para a entrega',
          LRef.TryDeliver('ct', 'q', True, False, LMsg, LTag));
        LCanal.Active := True;

        // Basic.Qos tambem.
        LCanal.PrefetchCount := 1;
        AssertTrue('1a', LRef.TryDeliver('ct', 'q', False, False, LMsg, LTag));
        AssertFalse('prefetch do canal vale para o alvo',
          LRef.TryDeliver('ct', 'q', False, False, LMsg, LTag));
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
  RegisterTest(TQueueDeliveryTests);
  RegisterTest(TDeliveryTargetTests);

end.
