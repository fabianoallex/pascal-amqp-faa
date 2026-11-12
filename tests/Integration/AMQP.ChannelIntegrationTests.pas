unit AMQP.ChannelIntegrationTests;

{ Channel/publish/get integration — needs RabbitMQ on localhost:5672.
  Start it with: docker compose -f docker/docker-compose.yml up -d }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Classes, Rtti,
  PascalCommon.Threading,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  [TestFixture]
  TAMQPChannelIntegrationTests = class
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    // State captured by the callbacks (of object — the library does not accept
    // anonymous methods here, see CLAUDE.md); each test resets what it uses
    // before arming the matching callback. TInterlocked does not exist in FPC -> PcAtomic*.
    FReturned: TAMQPReturnedMessage;
    FGotReturn: Integer;
    FConfirmedSeq: UInt64;
    FAckFlag: Integer;
    FAckCount: Integer;
    FNackCount: Integer;
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
    procedure HandleBasicReturn(AChannel: TAMQPChannel; const AReturned: TAMQPReturnedMessage);
    procedure HandleConfirmSingle(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
    procedure HandleConfirmAckNack(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure PublishesAndGetsWithProperties;
    [Test] procedure PublishText_AndGet;
    [Test] procedure GetEmpty_WhenQueueEmpty;
    [Test] procedure DeclareQueue_ReturnsGeneratedName;
    [Test] procedure Unbind_StopsRoutingToTheQueue;
    [Test] procedure ExchangeBind_RoutesSourceToDest;
    [Test] procedure PublishMandatory_NoRoute_FiresOnBasicReturn;
    [Test] procedure Confirm_RoutablePublish_Acked;
    [Test] procedure Confirm_WaitForConfirms_SeveralPublishes;
    [Test] procedure Confirm_WaitForConfirms_DetectsAlreadyResolvedNack;
  end;

implementation

procedure TAMQPChannelIntegrationTests.SetUp;
begin
  FConn := TAMQPConnection.Create(IntegrationParams);
  FConn.Open;
  FChan := FConn.CreateChannel;
  FGotReturn := 0;
  FConfirmedSeq := 0;
  FAckFlag := 0;
  FAckCount := 0;
  FNackCount := 0;
end;

procedure TAMQPChannelIntegrationTests.TearDown;
begin
  FChan.Free;
  FConn.Free;
end;

function TAMQPChannelIntegrationTests.DeclareTempQueue: string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;   // goes away when the connection closes
  LDecl.AutoDelete := True;
  Result := FChan.DeclareQueue(LDecl).QueueName; // '' => server-generated name
end;

function TAMQPChannelIntegrationTests.GetWithRetry(const AQueue: string): TAMQPGetResult;
var
  I: Integer;
begin
  // Delivery after publish is asynchronous; tries for up to ~0.5s.
  for I := 1 to 20 do
  begin
    Result := FChan.BasicGet(AQueue, True);
    if Result.Found then
      Exit;
    TThread.Sleep(25);
  end;
end;

procedure TAMQPChannelIntegrationTests.HandleBasicReturn(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
begin
  FReturned := AReturned;
  PcAtomicSet(FGotReturn, 1);
end;

procedure TAMQPChannelIntegrationTests.HandleConfirmSingle(AChannel: TAMQPChannel;
  ASeqNo: UInt64; AAck: Boolean);
begin
  FConfirmedSeq := ASeqNo;
  if AAck then
    PcAtomicSet(FAckFlag, 1);
end;

procedure TAMQPChannelIntegrationTests.HandleConfirmAckNack(AChannel: TAMQPChannel;
  ASeqNo: UInt64; AAck: Boolean);
begin
  if AAck then
    PcAtomicInc(FAckCount)
  else
    PcAtomicInc(FNackCount);
end;

procedure TAMQPChannelIntegrationTests.PublishesAndGetsWithProperties;
var
  LQueue: string;
  LProps: TAMQPBasicProperties;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;

  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  LProps.SetPersistent;
  LProps.SetCorrelationId('corr-99');
  LProps.SetMessageId('msg-77');

  // The default exchange ('') routes by routing-key = queue name.
  FChan.Publish('', LQueue, TEncoding.UTF8.GetBytes('{"chave":"NFe123"}'), LProps);

  LResult := GetWithRetry(LQueue);
  TAssert.AssertTrue('mensagem deveria ter sido entregue', LResult.Found);
  TAssert.AssertEquals('corpo', '{"chave":"NFe123"}', LResult.BodyAsText);
  TAssert.AssertEquals('content-type', 'application/json', LResult.Properties.ContentType);
  TAssert.AssertEquals('correlation-id', 'corr-99', LResult.Properties.CorrelationId);
  TAssert.AssertEquals('message-id', 'msg-77', LResult.Properties.MessageId);
  TAssert.AssertEquals('delivery-mode persistente', 2, Integer(LResult.Properties.DeliveryMode));
end;

procedure TAMQPChannelIntegrationTests.PublishText_AndGet;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;
  FChan.PublishText('', LQueue, 'olá mundo NFe');

  LResult := GetWithRetry(LQueue);
  TAssert.AssertTrue(LResult.Found);
  TAssert.AssertEquals('olá mundo NFe', LResult.BodyAsText);
  TAssert.AssertEquals('text/plain', LResult.Properties.ContentType);
end;

procedure TAMQPChannelIntegrationTests.GetEmpty_WhenQueueEmpty;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;
  LResult := FChan.BasicGet(LQueue, True);
  TAssert.AssertFalse('fila recém-criada deveria estar vazia', LResult.Found);
end;

procedure TAMQPChannelIntegrationTests.DeclareQueue_ReturnsGeneratedName;
var
  LQueue: string;
begin
  LQueue := DeclareTempQueue;
  TAssert.AssertTrue('servidor deveria gerar um nome de fila', LQueue <> '');
end;

procedure TAMQPChannelIntegrationTests.Unbind_StopsRoutingToTheQueue;
var
  LExchange, LQueue, LRk: string;
  LEx: TAMQPExchangeDeclare;
  LBind: TAMQPQueueBind;
  LUnbind: TAMQPQueueUnbind;
  LResult: TAMQPGetResult;
begin
  LExchange := 'test-unbind-ex-' + TGUID.NewGuid.ToString;
  LRk := 'rota.teste';
  // NON-durable exchange (goes away when the connection closes) and NOT
  // auto-delete: if it were auto-delete, the unbind would remove the last
  // binding and the broker would delete the exchange — the next publish would
  // land on a nonexistent exchange and the server would close the channel
  // asynchronously (a race with TearDown). Keeping it alive, the publish
  // without a binding is simply dropped (which is what we want).
  LEx := TAMQPExchangeDeclare.Create(LExchange, AMQP_EXCHANGE_TYPE_DIRECT, False);
  FChan.DeclareExchange(LEx);

  LQueue := DeclareTempQueue;

  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := LQueue;
  LBind.ExchangeName := LExchange;
  LBind.RoutingKey := LRk;
  FChan.BindQueue(LBind);

  // With the bind, the message reaches the queue.
  FChan.PublishText(LExchange, LRk, 'antes-do-unbind');
  LResult := GetWithRetry(LQueue);
  TAssert.AssertTrue('com o bind, a mensagem deveria chegar', LResult.Found);
  TAssert.AssertEquals('antes-do-unbind', LResult.BodyAsText);

  // Removes the binding.
  LUnbind := Default(TAMQPQueueUnbind);
  LUnbind.QueueName := LQueue;
  LUnbind.ExchangeName := LExchange;
  LUnbind.RoutingKey := LRk;
  FChan.UnbindQueue(LUnbind);

  // Without the bind, the same publication is no longer routed to the queue.
  FChan.PublishText(LExchange, LRk, 'depois-do-unbind');
  TThread.Sleep(200); // gives the broker time to route (or drop)
  LResult := FChan.BasicGet(LQueue, True);
  TAssert.AssertFalse('após o unbind, a fila não deveria mais receber', LResult.Found);
end;

procedure TAMQPChannelIntegrationTests.ExchangeBind_RoutesSourceToDest;
var
  LSource, LDest, LQueue, LRk: string;
  LEx: TAMQPExchangeDeclare;
  LQBind: TAMQPQueueBind;
  LEBind: TAMQPExchangeBinding;
  LResult: TAMQPGetResult;
begin
  // source (direct) --[rk]--> dest (fanout) --> queue. Auto-delete exchanges: since
  // the bindings exist throughout the test, they survive; on teardown the exclusive
  // queue goes away -> dest loses its binding -> goes away -> source loses its
  // binding -> goes away (a clean cascade, no garbage on the broker and no
  // publishing to a deleted exchange).
  LRk := 'rota.ex';
  LSource := 'test-exsrc-' + TGUID.NewGuid.ToString;
  LDest := 'test-exdst-' + TGUID.NewGuid.ToString;

  LEx := TAMQPExchangeDeclare.Create(LSource, AMQP_EXCHANGE_TYPE_DIRECT, False);
  LEx.AutoDelete := True;
  FChan.DeclareExchange(LEx);

  LEx := TAMQPExchangeDeclare.Create(LDest, AMQP_EXCHANGE_TYPE_FANOUT, False);
  LEx.AutoDelete := True;
  FChan.DeclareExchange(LEx);

  LQueue := DeclareTempQueue;
  LQBind := Default(TAMQPQueueBind);
  LQBind.QueueName := LQueue;
  LQBind.ExchangeName := LDest;
  LQBind.RoutingKey := ''; // fanout ignores the routing-key
  FChan.BindQueue(LQBind);

  LEBind := Default(TAMQPExchangeBinding);
  LEBind.Destination := LDest;
  LEBind.Source := LSource;
  LEBind.RoutingKey := LRk;
  FChan.BindExchange(LEBind);

  // Publishes to source with the binding's routing-key: source -> dest -> queue.
  FChan.PublishText(LSource, LRk, 'via-exchange-bind');
  LResult := GetWithRetry(LQueue);
  TAssert.AssertTrue('a mensagem deveria rotear source->dest->fila', LResult.Found);
  TAssert.AssertEquals('via-exchange-bind', LResult.BodyAsText);
end;

procedure TAMQPChannelIntegrationTests.PublishMandatory_NoRoute_FiresOnBasicReturn;
var
  I: Integer;
begin
  FChan.OnBasicReturn := HandleBasicReturn;

  // Default exchange + a routing-key with no queue bound: unroutable.
  FChan.Publish('', 'rota.inexistente.' + TGUID.NewGuid.ToString,
    TEncoding.UTF8.GetBytes('sem destino'), TAMQPBasicProperties.Empty, True {mandatory});

  for I := 1 to 80 do
  begin
    if PcAtomicGet(FGotReturn) = 1 then
      Break;
    TThread.Sleep(25);
  end;

  TAssert.AssertEquals('OnBasicReturn deveria disparar', 1, PcAtomicGet(FGotReturn));
  TAssert.AssertEquals('sem destino', FReturned.BodyAsText);
end;

procedure TAMQPChannelIntegrationTests.Confirm_RoutablePublish_Acked;
var
  LQueue: string;
  LSeq: UInt64;
  I: Integer;
begin
  LQueue := DeclareTempQueue;
  FChan.OnConfirm := HandleConfirmSingle;
  FChan.ConfirmSelect;

  // Default exchange + routing-key = queue name: routable, the broker confirms.
  LSeq := FChan.PublishText('', LQueue, 'confirma isso');
  TAssert.AssertTrue('primeiro publish em confirm mode deveria receber seq-no 1', UInt64(1) = LSeq);
  TAssert.AssertTrue('broker deveria confirmar (ack) o publish', FChan.WaitForConfirm(LSeq, 5000));

  for I := 1 to 80 do
  begin
    if PcAtomicGet(FAckFlag) = 1 then
      Break;
    TThread.Sleep(25);
  end;
  TAssert.AssertEquals('OnConfirm deveria disparar com ack', 1, PcAtomicGet(FAckFlag));
  TAssert.AssertTrue('OnConfirm deveria trazer o seq-no 1', UInt64(1) = FConfirmedSeq);
end;

procedure TAMQPChannelIntegrationTests.Confirm_WaitForConfirms_SeveralPublishes;
var
  LQueue: string;
  I: Integer;
  LLast: UInt64;
begin
  LQueue := DeclareTempQueue;
  FChan.ConfirmSelect;

  LLast := 0;
  for I := 1 to 10 do
    LLast := FChan.PublishText('', LQueue, 'msg ' + IntToStr(I));
  TAssert.AssertTrue('décimo publish deveria receber seq-no 10', UInt64(10) = LLast);

  TAssert.AssertTrue('todos os publishes roteáveis deveriam ser confirmados',
    FChan.WaitForConfirms(5000));
end;

procedure TAMQPChannelIntegrationTests.Confirm_WaitForConfirms_DetectsAlreadyResolvedNack;
var
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LQueue: string;
  I: Integer;
begin
  // A queue that REJECTS publishes when full: x-max-length=1 + x-overflow=reject-publish.
  // In confirm mode, the publish that exceeds the limit comes back as Basic.Nack.
  //
  // It was the ONLY test in the suite that depended on a Phase 3 feature
  // (per-queue x-max-length + x-overflow), and so it ignored itself against
  // the embedded broker since WS9 of Phase 2 — a backlog item, not a test
  // silently skipped. WS6 of Phase 3 implemented both, and the self-ignore
  // was REMOVED: the test now runs the same against RabbitMQ and against the
  // embedded broker, which is what decision D8 asked for from the start.

  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-max-length', TValue.From<Integer>(1));
    LArgs.Put('x-overflow', 'reject-publish');
    LDecl := Default(TAMQPQueueDeclare);
    LDecl.Exclusive := True;
    LDecl.AutoDelete := True;
    LDecl.Arguments := LArgs;
    LQueue := FChan.DeclareQueue(LDecl).QueueName;
  finally
    LArgs.Free;
  end;

  FChan.OnConfirm := HandleConfirmAckNack;
  FChan.ConfirmSelect;

  FChan.PublishText('', LQueue, 'A'); // fills the queue -> ack
  FChan.PublishText('', LQueue, 'B'); // exceeds the limit -> nack

  // Waits for BOTH to resolve BEFORE calling WaitForConfirms — that is exactly
  // what exposes finding #1: the nack has already arrived when WaitForConfirms is called.
  for I := 1 to 200 do
  begin
    if (PcAtomicGet(FAckCount) + PcAtomicGet(FNackCount)) >= 2 then
      Break;
    TThread.Sleep(25);
  end;

  // Proves the scenario worked (1 ack + 1 nack) — if the broker does not nack,
  // this assertion fails with a clear message, instead of masking the real test.
  TAssert.AssertEquals('o publish B deveria ter sido nack-ado (reject-publish)',
    1, PcAtomicGet(FNackCount));

  // Finding #1: since a publish in the batch was nacked, WaitForConfirms must
  // return False — even though the nack arrived BEFORE the call.
  TAssert.AssertFalse('WaitForConfirms deve reportar False quando um publish do lote foi nack-ado',
    FChan.WaitForConfirms(2000));
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPChannelIntegrationTests);

end.
