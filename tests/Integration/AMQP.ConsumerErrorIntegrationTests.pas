unit AMQP.ConsumerErrorIntegrationTests;

{ Exceção no callback de consumer (v0.4.0) — precisa de um broker (RabbitMQ
  ou o embutido, pela aceitação).

  Até a v0.3.0 a exceção era relançada para o pool, que a engolia: a entrega
  ficava sem ack até a conexão cair, nada era logado, e com Qos(1) o consumer
  inteiro parava (medido contra o RabbitMQ 3: 4 prontas e 1 sem ack, para
  sempre). Agora ela vai para TAMQPChannel.OnConsumerError e, se a entrega
  ainda está pendente, leva Nack (sem requeue, por padrão).

  Quase todo teste usa Qos(1) com a mensagem ruim NA FRENTE de boas: se a lib
  deixar a ruim sem confirmação, as boas nunca chegam; se confirmar duas
  vezes, o broker fecha o canal com 406 e as boas também não chegam. A fila
  principal tem dead-letter para amq.direct, onde a DLQ espera pela chave da
  fila principal: é por ela que se vê o Nack sem requeue.

  Mestre DUnitX no dialeto do FPCUnit (TAssert.*); o espelho FPCUnit em
  fpc\ é gerado por tools/gen_fpc_mirror.py. }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Classes, SyncObjs, Rtti,
  PascalCommon.Threading,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPConsumerErrorIntegrationTests = class
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FReconConn: TAMQPConnection;
    FQueue: string;
    FDlq: string;
    // o que o callback faz com a mensagem 'ruim' (MODO_*) e o handler (H_*)
    FModo: Integer;
    FHandlerModo: Integer;
    FNoAck: Boolean;
    // contadores atômicos
    FCalls: Integer;      // callbacks chamados
    FAcks: Integer;       // mensagens boas confirmadas pelo callback
    FErrors: Integer;     // chamadas do OnConsumerError que terminaram
    FRedeliveredOk: Integer; // a ruim, reentregue, confirmada (MODO_REQUEUE)
    FReconnected: Integer;
    // o que o handler viu (escrito antes de FErrors subir)
    FInitialAction: Integer;
    FErrorClass: string;
    FErrorMessage: string;
    FErrorBody: string;
    FPendingTag: UInt64;
    // MODO_RECONEXAO: a entrega da sessão velha segura até a queda
    FOldStarted: TEvent;
    FReleaseOld: TEvent;
    procedure HandleDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure HandleError(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery;
      AError: Exception; var AAction: TAMQPConsumerErrorAction);
    procedure HandleReconnect(AConnection: TAMQPConnection);
    function Espera(var ACounter: Integer; AValue, ATimeoutMs: Integer): Boolean;
    /// Fila principal com dead-letter para amq.direct + DLQ ligada pela chave
    /// da principal. Com ANome vazio as duas são exclusivas e com nome gerado.
    procedure DeclaraFilas(AChannel: TAMQPChannel; const ANome: string);
    function LeDlq(AChannel: TAMQPChannel; ATimeoutMs: Integer): string;
    procedure PublicaRuimEBoas(ABoas: Integer);
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure SemHandler_CallbackLevanta_NackSemRequeue_E_NaoTrava;
    [Test] procedure Handler_RecebeExcecaoEntrega_E_AcaoInicialReject;
    [Test] procedure Handler_Requeue_ReentregaComRedelivered;
    [Test] procedure Handler_None_EntregaFicaComOAplicativo;
    [Test] procedure CallbackAckouAntesDeLevantar_NaoConfirmaDeNovo;
    [Test] procedure HandlerConfirmouEleMesmo_NaoConfirmaDeNovo;
    [Test] procedure HandlerLevanta_AcaoAindaVale;
    [Test] procedure NoAck_CallbackLevanta_HandlerChamado_SemNack;
    [Test] procedure Reconexao_CallbackLevantaNaQueda_MensagemVoltaNaSessaoNova;
  end;

implementation

const
  MODO_LEVANTA = 0;       // levanta direto
  MODO_ACK_E_LEVANTA = 1; // dá Ack e depois levanta
  MODO_REQUEUE = 2;       // levanta; reentregue (redelivered), confirma
  MODO_RECONEXAO = 3;     // ver Reconexao_CallbackLevantaNaQueda_MensagemVoltaNaSessaoNova

  H_PADRAO = 0;           // não mexe na ação
  H_REQUEUE = 1;
  H_NONE = 2;             // ceaNone + guarda o tag para o teste confirmar
  H_ACK_ELE_MESMO = 3;    // dá Ack no próprio handler e deixa a ação padrão
  H_LEVANTA = 4;          // levanta dentro do handler

  ESPERA_MS = 10000;

// Asserções com o tipo fixado: o valor entra já convertido, e a sobrecarga
// do TAssert é a mesma nos dois compiladores.
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaStr(const AMsg: string; const AEsperado, AObtido: string);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;

procedure TAMQPConsumerErrorIntegrationTests.SetUp;
begin
  FConn := TAMQPConnection.Create(IntegrationParams);
  FConn.Open;
  FChan := FConn.CreateChannel;
  FReconConn := nil;
  FModo := MODO_LEVANTA;
  FHandlerModo := H_PADRAO;
  FNoAck := False;
  FCalls := 0;
  FAcks := 0;
  FErrors := 0;
  FRedeliveredOk := 0;
  FReconnected := 0;
  FInitialAction := -1;
  FPendingTag := 0;
  FOldStarted := TEvent.Create(nil, True, False, '');
  FReleaseOld := TEvent.Create(nil, True, False, '');
end;

procedure TAMQPConsumerErrorIntegrationTests.TearDown;
begin
  // Solta callback preso de um teste que falhou no meio, senão o Free espera.
  FReleaseOld.SetEvent;
  FChan.Free;
  FConn.Free;
  FReconConn.Free; // nil-safe
  FOldStarted.Free;
  FReleaseOld.Free;
end;

procedure TAMQPConsumerErrorIntegrationTests.HandleDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  PcAtomicInc(FCalls);
  if ADelivery.BodyAsText <> 'ruim' then
  begin
    if not FNoAck then
      AChannel.Ack(ADelivery.DeliveryTag);
    PcAtomicInc(FAcks);
    Exit;
  end;

  case FModo of
    MODO_ACK_E_LEVANTA:
      AChannel.Ack(ADelivery.DeliveryTag);
    MODO_REQUEUE:
      if ADelivery.Redelivered then
      begin
        AChannel.Ack(ADelivery.DeliveryTag);
        PcAtomicInc(FRedeliveredOk);
        Exit;
      end;
    MODO_RECONEXAO:
      if ADelivery.Redelivered then
      begin
        // reentrega na sessão nova: processa normalmente
        AChannel.Ack(ADelivery.DeliveryTag);
        PcAtomicInc(FRedeliveredOk);
        Exit;
      end
      else
      begin
        // entrega da sessão velha: segura até a queda e levanta depois
        FOldStarted.SetEvent;
        FReleaseOld.WaitFor(ESPERA_MS);
      end;
  end;
  raise EConvertError.Create('"quantity":"x" is not a valid integer');
end;

procedure TAMQPConsumerErrorIntegrationTests.HandleError(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery; AError: Exception;
  var AAction: TAMQPConsumerErrorAction);
begin
  FInitialAction := Ord(AAction);
  FErrorClass := AError.ClassName;
  FErrorMessage := AError.Message;
  FErrorBody := ADelivery.BodyAsText;
  case FHandlerModo of
    H_REQUEUE:
      if not ADelivery.Redelivered then
        AAction := ceaRequeue;
    H_NONE:
      begin
        FPendingTag := ADelivery.DeliveryTag;
        AAction := ceaNone;
      end;
    H_ACK_ELE_MESMO:
      AChannel.Ack(ADelivery.DeliveryTag);
    H_LEVANTA:
      begin
        PcAtomicInc(FErrors);
        raise Exception.Create('o handler de erro também quebrou');
      end;
  end;
  PcAtomicInc(FErrors);
end;

procedure TAMQPConsumerErrorIntegrationTests.HandleReconnect(AConnection: TAMQPConnection);
begin
  PcAtomicSet(FReconnected, 1);
end;

function TAMQPConsumerErrorIntegrationTests.Espera(var ACounter: Integer;
  AValue, ATimeoutMs: Integer): Boolean;
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (PcAtomicGet(ACounter) < AValue) and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
  Result := PcAtomicGet(ACounter) >= AValue;
end;

procedure TAMQPConsumerErrorIntegrationTests.DeclaraFilas(AChannel: TAMQPChannel;
  const ANome: string);
var
  LArgs: TAMQPFieldTable;
  LVal: TValue;
  LDecl: TAMQPQueueDeclare;
  LBind: TAMQPQueueBind;
begin
  LArgs := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<string>('amq.direct');
    LArgs.Put('x-dead-letter-exchange', LVal);
    LDecl := Default(TAMQPQueueDeclare);
    LDecl.QueueName := ANome;
    LDecl.Exclusive := ANome = '';
    LDecl.Arguments := LArgs;
    FQueue := AChannel.DeclareQueue(LDecl).QueueName;
  finally
    LArgs.Free;
  end;

  LDecl := Default(TAMQPQueueDeclare);
  if ANome <> '' then
    LDecl.QueueName := ANome + '-dlq';
  LDecl.Exclusive := ANome = '';
  FDlq := AChannel.DeclareQueue(LDecl).QueueName;

  // a mensagem morta mantém a chave original: o nome da fila principal
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := FDlq;
  LBind.ExchangeName := 'amq.direct';
  LBind.RoutingKey := FQueue;
  AChannel.BindQueue(LBind);
end;

function TAMQPConsumerErrorIntegrationTests.LeDlq(AChannel: TAMQPChannel;
  ATimeoutMs: Integer): string;
var
  LGet: TAMQPGetResult;
  LWaited: Integer;
begin
  Result := '';
  LWaited := 0;
  repeat
    LGet := AChannel.BasicGet(FDlq, True);
    if LGet.Found then
    begin
      if LGet.Properties.Has(bpHeaders) and Assigned(LGet.Properties.Headers) then
        LGet.Properties.Headers.Free; // x-death
      Exit(LGet.BodyAsText);
    end;
    if LWaited >= ATimeoutMs then
      Exit;
    TThread.Sleep(50);
    Inc(LWaited, 50);
  until False;
end;

procedure TAMQPConsumerErrorIntegrationTests.PublicaRuimEBoas(ABoas: Integer);
var
  I: Integer;
begin
  FChan.PublishText('', FQueue, 'ruim');
  for I := 1 to ABoas do
    FChan.PublishText('', FQueue, 'boa-' + IntToStr(I));
end;

procedure TAMQPConsumerErrorIntegrationTests.SemHandler_CallbackLevanta_NackSemRequeue_E_NaoTrava;
begin
  // O cenário medido: sem try no callback e com Qos(1), a ruim segurava o
  // consumer inteiro. Sem handler, ela vai para a DLQ e as boas passam.
  DeclaraFilas(FChan, '');
  PublicaRuimEBoas(3);
  FChan.Qos(1);
  FChan.Consume(FQueue, HandleDelivery);

  ChecaOk('as 3 boas deveriam passar depois da ruim (Qos 1)', Espera(FAcks, 3, ESPERA_MS));
  ChecaStr('a ruim deveria ter ido para a DLQ (Nack sem requeue)', 'ruim', LeDlq(FChan, ESPERA_MS));
  ChecaInt('a ruim não deveria ter sido reentregue', 4, PcAtomicGet(FCalls));
  ChecaOk('o canal deveria seguir aberto', FChan.IsOpen);
end;

procedure TAMQPConsumerErrorIntegrationTests.Handler_RecebeExcecaoEntrega_E_AcaoInicialReject;
begin
  DeclaraFilas(FChan, '');
  FChan.OnConsumerError := HandleError;
  PublicaRuimEBoas(1);
  FChan.Qos(1);
  FChan.Consume(FQueue, HandleDelivery);

  ChecaOk('o handler deveria ter sido chamado', Espera(FErrors, 1, ESPERA_MS));
  ChecaStr('classe da exceção', 'EConvertError', FErrorClass);
  ChecaStr('mensagem da exceção', '"quantity":"x" is not a valid integer', FErrorMessage);
  ChecaStr('a entrega que levantou', 'ruim', FErrorBody);
  ChecaInt('entrega pendente: a ação inicial é ceaReject', Ord(ceaReject), FInitialAction);
  ChecaOk('a boa deveria passar', Espera(FAcks, 1, ESPERA_MS));
  ChecaStr('ação mantida: a ruim vai para a DLQ', 'ruim', LeDlq(FChan, ESPERA_MS));
end;

procedure TAMQPConsumerErrorIntegrationTests.Handler_Requeue_ReentregaComRedelivered;
begin
  FModo := MODO_REQUEUE;
  FHandlerModo := H_REQUEUE;
  DeclaraFilas(FChan, '');
  FChan.OnConsumerError := HandleError;
  PublicaRuimEBoas(2);
  FChan.Qos(1);
  FChan.Consume(FQueue, HandleDelivery);

  ChecaOk('a ruim deveria voltar (redelivered) e ser confirmada', Espera(FRedeliveredOk, 1, ESPERA_MS));
  ChecaOk('as boas deveriam passar', Espera(FAcks, 2, ESPERA_MS));
  ChecaInt('o handler roda só na primeira entrega', 1, PcAtomicGet(FErrors));
  ChecaStr('com requeue a DLQ fica vazia', '', LeDlq(FChan, 300));
  ChecaOk('o canal deveria seguir aberto', FChan.IsOpen);
end;

procedure TAMQPConsumerErrorIntegrationTests.Handler_None_EntregaFicaComOAplicativo;
begin
  FHandlerModo := H_NONE;
  DeclaraFilas(FChan, '');
  FChan.OnConsumerError := HandleError;
  PublicaRuimEBoas(2);
  FChan.Qos(1);
  FChan.Consume(FQueue, HandleDelivery);

  ChecaOk('o handler deveria ter sido chamado', Espera(FErrors, 1, ESPERA_MS));
  // ceaNone: a lib não confirma, então com Qos(1) nada mais chega...
  TThread.Sleep(300);
  ChecaInt('com a ruim pendente, nenhuma boa deveria chegar', 0, PcAtomicGet(FAcks));
  // ...até o aplicativo confirmar por conta própria.
  FChan.Nack(FPendingTag, False);
  ChecaOk('confirmada pelo aplicativo, as boas passam', Espera(FAcks, 2, ESPERA_MS));
  ChecaStr('o Nack do aplicativo mandou a ruim para a DLQ', 'ruim', LeDlq(FChan, ESPERA_MS));
end;

procedure TAMQPConsumerErrorIntegrationTests.CallbackAckouAntesDeLevantar_NaoConfirmaDeNovo;
begin
  // Confirmar duas vezes o mesmo tag é 406 PRECONDITION_FAILED: o broker
  // fecha o canal e as boas nunca chegam.
  FModo := MODO_ACK_E_LEVANTA;
  DeclaraFilas(FChan, '');
  FChan.OnConsumerError := HandleError;
  PublicaRuimEBoas(2);
  FChan.Qos(1);
  FChan.Consume(FQueue, HandleDelivery);

  ChecaOk('o handler deveria ter sido chamado', Espera(FErrors, 1, ESPERA_MS));
  ChecaInt('já confirmada: a ação inicial é ceaNone', Ord(ceaNone), FInitialAction);
  ChecaOk('as boas deveriam passar (sem 406)', Espera(FAcks, 2, ESPERA_MS));
  ChecaOk('o canal deveria seguir aberto', FChan.IsOpen);
  ChecaStr('a ruim foi confirmada com Ack: a DLQ fica vazia', '', LeDlq(FChan, 300));
end;

procedure TAMQPConsumerErrorIntegrationTests.HandlerConfirmouEleMesmo_NaoConfirmaDeNovo;
begin
  // O handler dá Ack e deixa a ação em ceaReject: a lib confere de novo
  // depois do handler e não manda o Nack.
  FHandlerModo := H_ACK_ELE_MESMO;
  DeclaraFilas(FChan, '');
  FChan.OnConsumerError := HandleError;
  PublicaRuimEBoas(2);
  FChan.Qos(1);
  FChan.Consume(FQueue, HandleDelivery);

  ChecaOk('o handler deveria ter sido chamado', Espera(FErrors, 1, ESPERA_MS));
  ChecaInt('a ação inicial era ceaReject', Ord(ceaReject), FInitialAction);
  ChecaOk('as boas deveriam passar (sem 406)', Espera(FAcks, 2, ESPERA_MS));
  ChecaOk('o canal deveria seguir aberto', FChan.IsOpen);
  ChecaStr('o Ack do handler venceu: a DLQ fica vazia', '', LeDlq(FChan, 300));
end;

procedure TAMQPConsumerErrorIntegrationTests.HandlerLevanta_AcaoAindaVale;
begin
  FHandlerModo := H_LEVANTA;
  DeclaraFilas(FChan, '');
  FChan.OnConsumerError := HandleError;
  PublicaRuimEBoas(2);
  FChan.Qos(1);
  FChan.Consume(FQueue, HandleDelivery);

  ChecaOk('as boas deveriam passar mesmo com o handler quebrado', Espera(FAcks, 2, ESPERA_MS));
  ChecaStr('a ação padrão seguiu valendo', 'ruim', LeDlq(FChan, ESPERA_MS));
end;

procedure TAMQPConsumerErrorIntegrationTests.NoAck_CallbackLevanta_HandlerChamado_SemNack;
begin
  // Num consumer no-ack não há o que confirmar: um Nack seria 406 (tag
  // desconhecido) e fecharia o canal.
  FNoAck := True;
  DeclaraFilas(FChan, '');
  FChan.OnConsumerError := HandleError;
  PublicaRuimEBoas(2);
  FChan.Consume(FQueue, HandleDelivery, True);

  ChecaOk('o handler deveria ser chamado também no no-ack', Espera(FErrors, 1, ESPERA_MS));
  ChecaInt('no-ack: a ação inicial é ceaNone', Ord(ceaNone), FInitialAction);
  ChecaOk('as boas deveriam chegar', Espera(FAcks, 2, ESPERA_MS));
  // um 406 chega de forma assíncrona: dá tempo a ele antes de olhar o canal
  ChecaStr('no-ack: nada vai para a DLQ', '', LeDlq(FChan, 300));
  ChecaOk('o canal deveria seguir aberto', FChan.IsOpen);
end;

procedure TAMQPConsumerErrorIntegrationTests.Reconexao_CallbackLevantaNaQueda_MensagemVoltaNaSessaoNova;
var
  LParams: TAMQPConnectionParams;
  LChan: TAMQPChannel;
  LDel: TAMQPQueueDelete;
  LNome: string;
begin
  // O callback levanta DEPOIS de a conexão cair. A reconexão espera os
  // callbacks da sessão velha terminarem antes de reabrir o canal
  // (DrainAllChannels), então o Nack automático cai no socket morto e é
  // descartado em silêncio: o broker já devolveu a mensagem à fila, e ela
  // volta com Redelivered=True na sessão nova. Se o Nack chegasse ao broker,
  // a mensagem iria para a DLQ e nunca seria reentregue.
  FModo := MODO_RECONEXAO;
  LNome := 'test-consumer-err-' + IntToStr(PcTickMs);
  LParams := IntegrationParams;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 200;
  FReconConn := TAMQPConnection.Create(LParams);
  FReconConn.OnReconnect := HandleReconnect;
  FReconConn.Open;
  LChan := FReconConn.CreateChannel;
  try
    LChan.OnConsumerError := HandleError;
    DeclaraFilas(LChan, LNome); // nomeadas e não exclusivas: sobrevivem à queda
    try
      LChan.PublishText('', FQueue, 'ruim');
      LChan.Consume(FQueue, HandleDelivery);
      ChecaOk('a entrega velha deveria começar', FOldStarted.WaitFor(ESPERA_MS) = wrSignaled);

      FReconConn.DropConnectionForTest;
      FReleaseOld.SetEvent; // a velha levanta agora, com o socket já fechado
      ChecaOk('o handler deveria ter sido chamado', Espera(FErrors, 1, ESPERA_MS));
      ChecaInt('a entrega ainda estava pendente: ceaReject', Ord(ceaReject), FInitialAction);

      ChecaOk('deveria reconectar', Espera(FReconnected, 1, ESPERA_MS));
      ChecaOk('a mensagem deveria voltar na sessão nova e ser confirmada',
        Espera(FRedeliveredOk, 1, ESPERA_MS));
      LChan.PublishText('', FQueue, 'depois');
      ChecaOk('o canal deveria seguir consumindo', Espera(FAcks, 1, ESPERA_MS));
      ChecaStr('o Nack no socket morto não chegou ao broker', '', LeDlq(LChan, 300));
    finally
      LDel := Default(TAMQPQueueDelete);
      LDel.QueueName := FQueue;
      try LChan.DeleteQueue(LDel); except end;
      LDel.QueueName := FDlq;
      try LChan.DeleteQueue(LDel); except end;
    end;
  finally
    LChan.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPConsumerErrorIntegrationTests);

end.
