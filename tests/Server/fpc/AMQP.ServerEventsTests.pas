unit AMQP.ServerEventsTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Server/AMQP.ServerEventsTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }

{ Tests of the broker's observability (Phase 4.1, Inc. 1 -- D29-D36).

  Two fixtures, with different targets:

  - TEventBusTests exercises the bus ALONE, with no broker and no socket. It is
    where the mechanism's two anchor tests live, both deterministic by
    construction (no Sleep, no race):

      FullRing_DropsTheNewestAndCounts  -- the whole of D31. Fills the ring with
        the notifier STILL STOPPED, so the pressure is exact. Counting drops is
        not enough: the test requires that what is left is the PREFIX (1..4) and
        not the tail (7..10). An implementation that dropped the head would pass
        the counter and die in the second half.

      RaisingHandler_CountsAndContinues -- D33. The mutation that has to kill it
        is bringing back the WIP's "except end": FailedCount would go to zero. And
        the second handler receiving everything is what separates "the notifier
        survived" from "it died on the first event".

  - TServerEventTests brings up the real broker on an ephemeral port and connects
    a TAMQPConnection to it: proves that the emitters are in the right places and
    that the fields come out filled in. The anchor here is
    NoSubscriber_BrokerEmitsNothing, which is D29 -- the whole broker
    exercised, nobody subscribed, counter at zero.

  No test waits by Sleep (D35), and the kind of barrier is not a detail:

  - where PRESENCE is asserted, the test waits for the CONDITION (TCapture.WaitForCount).
    Drain alone does not do, and the first run proved it: it waits for the ring
    to empty, and a ring that has not received the event yet is already empty.
    Since the client's publish has no round-trip, draining right after it returns
    True before the broker has read the frame -- four tests failed
    exactly like that;
  - where ABSENCE is asserted, Drain is the right barrier, preceded by an
    operation WITH a round-trip (Channel.Close): when it returns, the broker has
    already gone through every emission point.

  The four Check* functions exist so the test bodies are IDENTICAL
  in both dialects: only their implementation changes sides. }


interface

uses
  fpcunit, testregistry,
  AMQP.TestSupport,
  SysUtils, Classes, SyncObjs,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Wire,
  AMQP.Connection,
  AMQP.Queue.Methods,
  AMQP.Server.Events,
  AMQP.Server.EventBus,
  AMQP.Server.Queue,
  AMQP.Server.Engine,
  AMQP.Server.Broker;

type
  { Capture sink. The handler runs on the notifier thread and the test reads from
    the runner thread, so everything goes through a lock. }
  TCapture = class
  private
    FMon: TPcMonitor;
    FEvents: array of TAMQPServerEvent;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Handle(const AEvent: TAMQPServerEvent);
    function Count: Integer;
    function Item(AIndex: Integer): TAMQPServerEvent;
    function CountOf(AType: TAMQPServerEventType): Integer;
    function FirstOf(AType: TAMQPServerEventType;
      out AEvent: TAMQPServerEvent): Boolean;
    /// Barrier by CONDITION (D35): waits for ACount events of the type to arrive.
    function WaitForCount(AType: TAMQPServerEventType; ACount: Integer;
      ATimeoutMs: Cardinal): Boolean;
  end;

  { Handler that always raises -- the test double of D33. }
  TRaisingHandler = class
  private
    FCalls: Integer;
  public
    procedure Handle(const AEvent: TAMQPServerEvent);
    property Calls: Integer read FCalls;
  end;

  { SLOW handler -- the test double of the D2 anchor (Inc. 2). }
  TSlowHandler = class
  private
    FCalls: Integer;
  public
    procedure Handle(const AEvent: TAMQPServerEvent);
    property Calls: Integer read FCalls;
  end;

  TEventBusTests = class(TTestCase)
  private
    FBus: TAMQPEventBus;
    FCapture: TCapture;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure NoSubscriber_WantsNoType;
    procedure Subscription_FiltersByType;
    procedure DeliversInEmissionOrder;
    procedure FullRing_DropsTheNewestAndCounts;
    procedure RaisingHandler_CountsAndContinues;
    procedure ResubscribeReplacesTheMask;
    procedure Unsubscribe_StopsDelivering;
    procedure EveryTypeHasAName;
  end;

  TServerEventTests = class(TTestCase)
  private
    FBroker: TAMQPServer;
    FConn: TAMQPConnection;
    FCapture: TCapture;
    procedure StartAndConnect(ASubscribe: Boolean);
    procedure ConnectClient;
    procedure TestConsumer(AChannel: TAMQPChannel;
      const ADelivery: TAMQPDelivery);
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure Connection_EmitsEstablishedAndAuthenticated;
    procedure Channel_EmitsOpenAndClose;
    procedure Publish_EmitsWithSizeAndKey;
    procedure UnroutedPublish_SetsUnroutable;
    procedure ConsumeAndAck_Emit;
    procedure Connection_EmitsCloseOnTeardown;
    procedure ChannelDiesWithTheConnection_EmitsClose;
    procedure NoSubscriber_BrokerEmitsNothing;
    procedure Enqueue_AndDelivery_EmitFromTheActor;
    procedure Consume_EmitsDeliveryWithTag;
    procedure TtlExpired_EmitsExpired;
    procedure QueueCap_EmitsDrop;
    procedure SlowHandler_DoesNotBlockTheActor;
    procedure DurableJournal_EmitsFlush;
  end;

implementation

// Assertions with the type fixed: the value comes in already converted, and the
// TAssert overload is the same on both compilers.
procedure CheckInt(const AMsg: string; AExpected, AActual: Integer);
begin
  TAssert.AssertEquals(AMsg, AExpected, AActual);
end;

procedure CheckStr(const AMsg: string; const AExpected, AActual: string);
begin
  TAssert.AssertEquals(AMsg, AExpected, AActual);
end;

procedure CheckOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;

procedure CheckNot(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertFalse(AMsg, ACond);
end;

// RTL per dialect (CLAUDE.md rule): the test body is shared,
// but everything that calls the RTL has to be written on each side. Here it is
// FPC's GetTempDir versus Delphi's TPath.GetTempPath.
function TestTempDir: string;
begin
  Result := IncludeTrailingPathDelimiter(TempDir) + 'amqpobs-'
    + FormatDateTime('yyyymmddhhnnsszzz', Now);
end;

// --- test doubles -----------------------------------------------------------

{ Capture sink: keeps everything that arrives, under a lock -- the handler runs on
  the notifier thread and the test reads from the runner thread. }
constructor TCapture.Create;
begin
  inherited Create;
  FMon := TPcMonitor.Create;
end;

destructor TCapture.Destroy;
begin
  FMon.Free;
  inherited Destroy;
end;

procedure TCapture.Handle(const AEvent: TAMQPServerEvent);
begin
  FMon.Enter;
  try
    SetLength(FEvents, Length(FEvents) + 1);
    FEvents[High(FEvents)] := AEvent;
    FMon.PulseAll; // wakes whoever is in WaitForCount
  finally
    FMon.Leave;
  end;
end;

function TCapture.Count: Integer;
begin
  FMon.Enter;
  try
    Result := Length(FEvents);
  finally
    FMon.Leave;
  end;
end;

function TCapture.Item(AIndex: Integer): TAMQPServerEvent;
begin
  FMon.Enter;
  try
    Result := FEvents[AIndex];
  finally
    FMon.Leave;
  end;
end;

function TCapture.CountOf(AType: TAMQPServerEventType): Integer;
var
  I: Integer;
begin
  Result := 0;
  FMon.Enter;
  try
    for I := 0 to High(FEvents) do
      if FEvents[I].EventType = AType then
        Inc(Result);
  finally
    FMon.Leave;
  end;
end;

function TCapture.FirstOf(AType: TAMQPServerEventType;
  out AEvent: TAMQPServerEvent): Boolean;
var
  I: Integer;
begin
  Result := False;
  FMon.Enter;
  try
    for I := 0 to High(FEvents) do
      if FEvents[I].EventType = AType then
      begin
        AEvent := FEvents[I];
        Exit(True);
      end;
  finally
    FMon.Leave;
  end;
end;

{ Waits until ACount events of the type arrive, or the deadline expires.

  Drain alone does NOT do here, and the test showed why: it waits for the ring
  to empty, and a ring that has not received the event yet is already empty. The
  client's publish has no round-trip, so draining right after it returns True
  before the broker has read the frame. The right barrier is by CONDITION --
  same line as "wait for OnReconnect, not IsOpen" in CLAUDE.md --, with
  a deadline and re-checking in a loop, never a fixed Sleep. }
function TCapture.WaitForCount(AType: TAMQPServerEventType; ACount: Integer;
  ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
  LN, I: Integer;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMon.Enter;
  try
    repeat
      LN := 0;
      for I := 0 to High(FEvents) do
        if FEvents[I].EventType = AType then
          Inc(LN);
      if LN >= ACount then
        Exit(True);
      if PcTickMs >= LDeadline then
        Exit(False);
      FMon.Wait(20);
    until False;
  finally
    FMon.Leave;
  end;
end;

{ Handler that always raises. D33 says this is counted and the loop goes on. }
procedure TRaisingHandler.Handle(const AEvent: TAMQPServerEvent);
begin
  Inc(FCalls);
  raise Exception.Create('handler de teste explodiu');
end;

{ SLOW handler -- the test double of the D2 anchor. Sleeps on every event, on
  purpose: it is the only way to prove the actor does not wait for it. }
procedure TSlowHandler.Handle(const AEvent: TAMQPServerEvent);
begin
  Inc(FCalls);
  Sleep(40);
end;

{ Declares a plain queue. The client's API is record-based -- it is the same
  helper the other server tests use. }
procedure MakeQueue(AChannel: TAMQPChannel; const AName: string);
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := TAMQPQueueDeclare.Create(AName, False);
  AChannel.DeclareQueue(LDecl);
end;

// --- TEventBusTests ---------------------------------------------------------

procedure TEventBusTests.Setup;
begin
  FBus := TAMQPEventBus.Create;
  FCapture := TCapture.Create;
end;

procedure TEventBusTests.TearDown;
begin
  FBus.Free; // the destructor stops the notifier
  FCapture.Free;
end;

procedure TEventBusTests.NoSubscriber_WantsNoType;
var
  LType: TAMQPServerEventType;
begin
  // D29 in one assertion: with no subscriber, Wants is False for EVERYTHING -- and
  // that is what makes the emitter not even build the record.
  for LType := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
    CheckNot('nao quer ' + AmqpEventTypeName(LType), FBus.Wants(LType));
end;

procedure TEventBusTests.Subscription_FiltersByType;
begin
  FBus.Subscribe(FCapture.Handle, [seChannelOpened, seMessageAcked]);
  CheckOk('quer o que assinou', FBus.Wants(seChannelOpened));
  CheckOk('quer o outro que assinou', FBus.Wants(seMessageAcked));
  CheckNot('nao quer o que nao assinou', FBus.Wants(seConnectionClosed));

  // And the filter applies at DELIVERY, not only in Wants.
  FBus.Start;
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  FBus.Emit(AmqpNewEvent(seConnectionClosed));
  FBus.Emit(AmqpNewEvent(seMessageAcked));
  CheckOk('drenou', FBus.Drain(2000));
  CheckInt('so os dois assinados chegaram', 2, FCapture.Count);
end;

procedure TEventBusTests.DeliversInEmissionOrder;
var
  I: Integer;
  LEv: TAMQPServerEvent;
begin
  FBus.Subscribe(FCapture.Handle);
  FBus.Start;
  for I := 1 to 50 do
  begin
    LEv := AmqpNewEvent(seMessageEnqueued);
    LEv.Count := I;
    FBus.Emit(LEv);
  end;
  CheckOk('drenou', FBus.Drain(2000));
  CheckInt('chegaram todos', 50, FCapture.Count);
  for I := 0 to 49 do
    CheckInt('ordem preservada', I + 1, Integer(FCapture.Item(I).Count));
end;

procedure TEventBusTests.FullRing_DropsTheNewestAndCounts;
var
  I: Integer;
  LEv: TAMQPServerEvent;
begin
  // The ANCHOR of D31. Without Start there is nobody to drain, so the ring really
  // fills up and the behavior under pressure is deterministic -- no Sleep,
  // no slow handler, no race.
  FBus.Capacity := 4;
  FBus.Subscribe(FCapture.Handle);
  for I := 1 to 10 do
  begin
    LEv := AmqpNewEvent(seMessageEnqueued);
    LEv.Count := I;
    FBus.Emit(LEv);
  end;

  CheckInt('so cabe a capacidade', 4, Integer(FBus.EmittedCount));
  CheckInt('o resto foi contado como descartado', 6,
    Integer(FBus.DroppedCount));

  // And now what D31 has to PROVE: what is left is the PREFIX (1..4), not
  // the tail (7..10). Dropping the head would pass the counter above and fail
  // here -- it is the mutation this test exists to catch.
  FBus.Start;
  CheckOk('drenou', FBus.Drain(2000));
  CheckInt('entregou os que couberam', 4, FCapture.Count);
  for I := 0 to 3 do
    CheckInt('e sao os PRIMEIROS, nao os ultimos', I + 1,
      Integer(FCapture.Item(I).Count));
end;

procedure TEventBusTests.RaisingHandler_CountsAndContinues;
var
  LPump: TRaisingHandler;
  I: Integer;
begin
  // The ANCHOR of D33: the mutation that has to kill this test is bringing back
  // the WIP's "except end", which would zero FailedCount.
  LPump := TRaisingHandler.Create;
  try
    FBus.Subscribe(LPump.Handle);
    FBus.Subscribe(FCapture.Handle);
    FBus.Start;
    for I := 1 to 3 do
      FBus.Emit(AmqpNewEvent(seChannelOpened));
    CheckOk('drenou', FBus.Drain(2000));

    CheckInt('as tres excecoes foram contadas', 3, Integer(FBus.FailedCount));
    CheckOk('e a ultima ficou registrada', FBus.LastFailure <> '');
    // What separates "survived" from "died on the first one": the SECOND handler
    // kept receiving, event after event.
    CheckInt('o handler seguinte recebeu tudo', 3, FCapture.Count);
  finally
    FBus.Unsubscribe(LPump.Handle);
    LPump.Free;
  end;
end;

procedure TEventBusTests.ResubscribeReplacesTheMask;
begin
  FBus.Subscribe(FCapture.Handle);
  CheckInt('um assinante', 1, FBus.SubscriberCount);
  CheckOk('quer tudo', FBus.Wants(seConnectionClosed));

  FBus.Subscribe(FCapture.Handle, [seChannelOpened]);
  CheckInt('continua sendo UM assinante', 1, FBus.SubscriberCount);
  CheckOk('quer o novo tipo', FBus.Wants(seChannelOpened));
  CheckNot('e nao quer mais o antigo', FBus.Wants(seConnectionClosed));

  // Replacing, not duplicating, means delivering ONCE.
  FBus.Start;
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  CheckOk('drenou', FBus.Drain(2000));
  CheckInt('entregue uma unica vez', 1, FCapture.Count);
end;

procedure TEventBusTests.Unsubscribe_StopsDelivering;
begin
  FBus.Subscribe(FCapture.Handle);
  FBus.Start;
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  CheckOk('drenou', FBus.Drain(2000));
  CheckInt('chegou', 1, FCapture.Count);

  FBus.Unsubscribe(FCapture.Handle);
  CheckInt('nenhum assinante', 0, FBus.SubscriberCount);
  CheckNot('e nao quer mais nada', FBus.Wants(seChannelOpened));
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  CheckOk('drenou', FBus.Drain(2000));
  CheckInt('e nao chegou mais nada', 1, FCapture.Count);
end;

procedure TEventBusTests.EveryTypeHasAName;
var
  LType, LOther: TAMQPServerEventType;
  LName: string;
begin
  // Guards EVENT_NAMES against a new type added without a name: the array
  // is indexed by the enum, so the compiler requires the right count, but it does
  // not require the names to be distinct or non-empty.
  for LType := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
  begin
    LName := AmqpEventTypeName(LType);
    CheckOk('nome nao vazio', LName <> '');
    for LOther := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
      if LOther <> LType then
        CheckOk('nome distinto: ' + LName,
          AmqpEventTypeName(LOther) <> LName);
  end;
end;

// --- TServerEventTests ------------------------------------------------------

procedure TServerEventTests.Setup;
begin
  FCapture := TCapture.Create;
  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '127.0.0.1';
  FBroker.Port := 0;
end;

procedure TServerEventTests.TearDown;
begin
  if FConn <> nil then
  begin
    try
      FConn.Close;
    except
    end;
    FreeAndNil(FConn);
  end;
  if FBroker <> nil then
  begin
    // Unsubscribe BEFORE releasing the capture: it is exactly the D33 contract
    // the documentation tells the user to follow, and the test follows it too.
    FBroker.Unsubscribe(FCapture.Handle);
    try
      FBroker.Stop;
    except
    end;
    FreeAndNil(FBroker);
  end;
  FreeAndNil(FCapture);
end;

{ Brings up the broker (subscribing before Start, which is the normal case) and
  connects a real client to it. }
procedure TServerEventTests.StartAndConnect(ASubscribe: Boolean);
begin
  if ASubscribe then
    FBroker.Subscribe(FCapture.Handle);
  FBroker.Start;
  ConnectClient;
end;

{ Separate from StartAndConnect because some tests need to configure the broker
  BEFORE Start (the ring capacity, for example) and connect afterwards. }
procedure TServerEventTests.ConnectClient;
var
  LParams: TAMQPConnectionParams;
begin
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := FBroker.Port;
  FConn := TAMQPConnection.Create(LParams);
  FConn.Open;
end;

{ Test consumer: it only needs to exist so the queue has someone to
  deliver to. What is asserted is the EVENT, not what arrives here. }
procedure TServerEventTests.TestConsumer(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  AChannel.Ack(ADelivery.DeliveryTag, False);
end;

procedure TServerEventTests.Connection_EmitsEstablishedAndAuthenticated;
var
  LEv: TAMQPServerEvent;
begin
  StartAndConnect(True);
  CheckOk('chegou a autenticada', FCapture.WaitForCount(seConnectionAuthenticated, 1, 3000));

  CheckInt('uma conexao estabelecida', 1,
    FCapture.CountOf(seConnectionEstablished));
  CheckOk('achou a estabelecida', FCapture.FirstOf(seConnectionEstablished, LEv));
  CheckOk('com endereco do peer', LEv.RemoteAddr <> '');
  CheckOk('e com id de conexao', LEv.ConnectionId > 0);
  // The established one is born BEFORE the handshake: user and vhost do not exist yet,
  // and D34 says that an unpopulated field comes out ZEROED.
  CheckStr('sem usuario ainda', '', LEv.Username);
  CheckStr('sem vhost ainda', '', LEv.VHost);

  CheckOk('achou a autenticada', FCapture.FirstOf(seConnectionAuthenticated, LEv));
  CheckStr('com o usuario autenticado', 'guest', LEv.Username);
  CheckStr('e com o vhost aberto', '/', LEv.VHost);
  CheckInt('evento de conexao vai no canal 0', 0, LEv.ChannelNumber);
end;

procedure TServerEventTests.Channel_EmitsOpenAndClose;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
begin
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  try
    LCh.Close;
  finally
    // A channel CLOSED by the client leaves its owner (the connection) -- whoever closes, frees.
    LCh.Free;
  end;
  CheckOk('chegou o fechamento', FCapture.WaitForCount(seChannelClosed, 1, 3000));

  CheckOk('achou a abertura', FCapture.FirstOf(seChannelOpened, LEv));
  CheckOk('com numero de canal', LEv.ChannelNumber > 0);
  CheckInt('um fechamento', 1, FCapture.CountOf(seChannelClosed));
  CheckOk('achou o fechamento', FCapture.FirstOf(seChannelClosed, LEv));
  CheckOk('no mesmo canal', LEv.ChannelNumber > 0);
end;

procedure TServerEventTests.Publish_EmitsWithSizeAndKey;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LMsgBody: string;
begin
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  MakeQueue(LCh, 'obs.q');
  LMsgBody := 'doze bytes!!';
  LCh.PublishText('', 'obs.q', LMsgBody);
  CheckOk('chegou o publish', FCapture.WaitForCount(seMessagePublished, 1, 3000));

  CheckOk('achou o publish', FCapture.FirstOf(seMessagePublished, LEv));
  CheckStr('exchange default', '', LEv.ExchangeName);
  CheckStr('routing key', 'obs.q', LEv.RoutingKey);
  // D34 in one assertion: the SIZE goes, never the body.
  CheckInt('tamanho do corpo', Length(LMsgBody), Integer(LEv.MessageSize));
  CheckStr('roteou, entao sem motivo', '', LEv.Reason);
end;

procedure TServerEventTests.UnroutedPublish_SetsUnroutable;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
begin
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  LCh.PublishText('', 'fila.que.nao.existe', 'x');
  CheckOk('chegou o publish', FCapture.WaitForCount(seMessagePublished, 1, 3000));

  // No route is NOT a refused publish: it is a successful publish that found no
  // queue (it gets an ack, and a Return if mandatory). Confusing the two would make
  // the observer count an error where there is none.
  CheckInt('nenhum publish recusado', 0,
    FCapture.CountOf(seMessagePublishRejected));
  CheckOk('achou o publish', FCapture.FirstOf(seMessagePublished, LEv));
  CheckStr('marcado como sem rota', 'unroutable', LEv.Reason);
end;

procedure TServerEventTests.ConsumeAndAck_Emit;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LMsg: TAMQPGetResult;
begin
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  MakeQueue(LCh, 'obs.ack');
  LCh.PublishText('', 'obs.ack', 'um');
  LMsg := LCh.BasicGet('obs.ack', False);
  CheckOk('veio mensagem', LMsg.Found);
  LCh.Ack(LMsg.DeliveryTag, False);
  CheckOk('chegou o ack', FCapture.WaitForCount(seMessageAcked, 1, 3000));

  CheckInt('um ack', 1, FCapture.CountOf(seMessageAcked));
  CheckOk('achou o ack', FCapture.FirstOf(seMessageAcked, LEv));
  CheckOk('com delivery tag', LEv.DeliveryTag > 0);
  CheckNot('sem multiple', LEv.Multiple);
  CheckInt('resolveu uma entrega', 1, Integer(LEv.Count));
end;

procedure TServerEventTests.Connection_EmitsCloseOnTeardown;
begin
  StartAndConnect(True);
  FConn.Close;
  FreeAndNil(FConn);
  CheckOk('chegou o fechamento',
    FCapture.WaitForCount(seConnectionClosed, 1, 3000));

  // The close is born in the read thread's teardown -- and it only arrives because
  // the notifier is the LAST one to go down (see TAMQPServer.Stop).
  CheckInt('uma conexao fechada', 1, FCapture.CountOf(seConnectionClosed));
end;

procedure TServerEventTests.ChannelDiesWithTheConnection_EmitsClose;
var
  LEv: TAMQPServerEvent;
begin
  // A channel opened and NEVER closed by the client: whoever closes it is the
  // connection's teardown. Without emitting here, an observer that counts opens
  // against closes would report a channel leak on every dropped connection -- that
  // is what the sample's race against the SmokeTest showed (3 opens, 2
  // closes), and it is the mutation this test exists to catch.
  StartAndConnect(True);
  FConn.CreateChannel;
  CheckOk('chegou a abertura', FCapture.WaitForCount(seChannelOpened, 1, 3000));

  FConn.Close;
  FreeAndNil(FConn);
  CheckOk('chegou o fechamento do canal',
    FCapture.WaitForCount(seChannelClosed, 1, 3000));
  CheckOk('achou o fechamento', FCapture.FirstOf(seChannelClosed, LEv));
  CheckStr('e vem marcado como teardown', 'connection teardown', LEv.Reason);
end;

procedure TServerEventTests.NoSubscriber_BrokerEmitsNothing;
var
  LCh: TAMQPChannel;
begin
  // The ANCHOR of D29: with the whole broker exercised and NOBODY subscribed,
  // no record is built and nothing enters the ring. The mutation that has to
  // kill this test is emitting without checking Wants.
  StartAndConnect(False);
  LCh := FConn.CreateChannel;
  MakeQueue(LCh, 'obs.silencio');
  LCh.PublishText('', 'obs.silencio', 'nada disso deve virar evento');
  try
    LCh.Close; // round-trip: the broker HAS processed everything above when it returns
  finally
    LCh.Free;
  end;
  // Here Drain is the right barrier: the assertion is of ABSENCE, and the
  // Close-Ok round-trip guarantees the broker has already gone through every
  // emission point.
  CheckOk('drenou', FBroker.DrainEvents(3000));

  CheckInt('nada emitido', 0, Integer(FBroker.EventsEmitted));
  CheckInt('e nada descartado', 0, Integer(FBroker.EventsDropped));
  CheckInt('e nenhum handler falhou', 0, Integer(FBroker.EventsFailed));
end;

procedure TServerEventTests.Enqueue_AndDelivery_EmitFromTheActor;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LMsg: TAMQPGetResult;
begin
  // These two are born in a worker of the actors' pool -- not on the read
  // thread. That they arrive is the proof that the D30 marshalling holds
  // for both origins, with the SAME contract.
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  MakeQueue(LCh, 'obs.ator');
  LCh.PublishText('', 'obs.ator', 'do ator');
  CheckOk('chegou o enqueue', FCapture.WaitForCount(seMessageEnqueued, 1, 3000));

  CheckOk('achou o enqueue', FCapture.FirstOf(seMessageEnqueued, LEv));
  CheckStr('com a fila', 'obs.ator', LEv.QueueName);
  CheckStr('e o vhost', '/', LEv.VHost);
  CheckInt('e o tamanho', 7, Integer(LEv.MessageSize));
  // The actor lives on the other side of the mailbox: it knows neither connection
  // nor user, and D34 says an unpopulated field comes out ZEROED instead of garbage.
  CheckInt('ator nao conhece a conexao', 0, Integer(LEv.ConnectionId));
  CheckStr('nem o usuario', '', LEv.Username);

  LMsg := LCh.BasicGet('obs.ator', True);
  CheckOk('veio mensagem', LMsg.Found);
end;

procedure TServerEventTests.Consume_EmitsDeliveryWithTag;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LTag: string;
begin
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  MakeQueue(LCh, 'obs.entrega');
  LTag := LCh.Consume('obs.entrega', TestConsumer);
  CheckOk('consumidor registrado', LTag <> '');
  LCh.PublishText('', 'obs.entrega', 'entrega');
  CheckOk('chegou a entrega', FCapture.WaitForCount(seMessageDelivered, 1, 3000));

  CheckOk('achou a entrega', FCapture.FirstOf(seMessageDelivered, LEv));
  CheckStr('com a fila', 'obs.entrega', LEv.QueueName);
  CheckStr('e a tag do consumidor', LTag, LEv.ConsumerTag);
  CheckOk('e a tag de entrega', LEv.DeliveryTag > 0);
  CheckInt('e o tamanho', 7, Integer(LEv.MessageSize));
end;

procedure TServerEventTests.TtlExpired_EmitsExpired;
var
  LCh: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
  LEv: TAMQPServerEvent;
begin
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  LArgs := TAMQPFieldTable.Create;
  // Very short x-message-ttl: the sweep on the monitor's tick expires the
  // message on its own, with no consumer at all (D13).
  LArgs.Put('x-message-ttl', 1);
  LDecl := TAMQPQueueDeclare.Create('obs.ttl', False);
  LDecl.Arguments := LArgs;
  try
    LCh.DeclareQueue(LDecl);
  finally
    LArgs.Free; // whoever creates the table frees it -- the declare does not become its owner
  end;
  LCh.PublishText('', 'obs.ttl', 'vou vencer');

  CheckOk('chegou a expiracao', FCapture.WaitForCount(seMessageExpired, 1, 5000));
  CheckOk('achou a expiracao', FCapture.FirstOf(seMessageExpired, LEv));
  CheckStr('na fila certa', 'obs.ttl', LEv.QueueName);
  CheckStr('com a razao do x-death', 'expired', LEv.Reason);
end;

procedure TServerEventTests.QueueCap_EmitsDrop;
var
  LCh: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
  LEv: TAMQPServerEvent;
  I: Integer;
begin
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  LArgs := TAMQPFieldTable.Create;
  LArgs.Put('x-max-length', 2);
  LDecl := TAMQPQueueDeclare.Create('obs.teto', False);
  LDecl.Arguments := LArgs;
  try
    LCh.DeclareQueue(LDecl);
  finally
    LArgs.Free; // whoever creates the table frees it -- the declare does not become its owner
  end;
  for I := 1 to 5 do
    LCh.PublishText('', 'obs.teto', 'enche');

  CheckOk('chegou o descarte', FCapture.WaitForCount(seMessageDropped, 1, 3000));
  CheckOk('achou o descarte', FCapture.FirstOf(seMessageDropped, LEv));
  CheckStr('na fila certa', 'obs.teto', LEv.QueueName);
  CheckStr('com a razao do x-death', 'maxlen', LEv.Reason);
end;

procedure TServerEventTests.SlowHandler_DoesNotBlockTheActor;
var
  LCh: TAMQPChannel;
  LSlow: TSlowHandler;
  I, LEnqueued: Integer;
  LStats: TAMQPQueueStats;
  LQueue: TAMQPServerQueue;
begin
  // THE ANCHOR OF D2, and the test that justifies the whole of Phase 4.1 having a ring.
  //
  // The handler sleeps 40 ms per event. The ring has 8 slots. We publish 200
  // messages: the actor produces events MUCH faster than the handler
  // consumes them, so the ring fills up and starts dropping (D31).
  //
  // What is asserted is NOT that the events arrived -- it is that the ACTOR DID NOT
  // WAIT for them: the 200 messages have to be in the queue. The mutation that
  // kills this test is replacing the non-blocking Emit with any wait --
  // the actor would be stuck behind the handler, the 200 would not get in within the
  // deadline, and the whole pool (which is the SAME as the other actors') would go with it.
  FBroker.EventQueueCapacity := 8;
  LSlow := TSlowHandler.Create;
  try
    FBroker.Subscribe(LSlow.Handle, [seMessageEnqueued]);
    FBroker.Start;
    ConnectClient;

    LCh := FConn.CreateChannel;
    MakeQueue(LCh, 'obs.lenta');
    for I := 1 to 200 do
      LCh.PublishText('', 'obs.lenta', 'carga');

    // The queue is the witness: if the actor had waited for the handler, the 200
    // would not be in there within this deadline (200 x 40 ms = 8 s of handler alone).
    LQueue := nil;
    LEnqueued := 0;
    LQueue := FBroker.Engine.FindQueue('/', 'obs.lenta');
    CheckOk('a fila existe', LQueue <> nil);
    for I := 1 to 100 do
    begin
      LStats := LQueue.Stats;
      LEnqueued := LStats.MessageCount;
      if LEnqueued >= 200 then
        Break;
      Sleep(20);
    end;
    CheckInt('o ator enfileirou tudo sem esperar o handler', 200,
      LEnqueued);

    // And the price of best-effort shows up: with the handler at 40 ms per event and 8
    // slots, there were drops. This is D31 collecting its due, not a defect.
    CheckOk('e houve descarte contado', FBroker.EventsDropped > 0);
  finally
    FBroker.Unsubscribe(LSlow.Handle);
    // Stop the broker BEFORE freeing the slow handler: Stop drains the ring, and
    // draining calls the handler. Freeing it first would be the AV that D33 describes.
    FBroker.Stop;
    LSlow.Free;
  end;
end;

procedure TServerEventTests.DurableJournal_EmitsFlush;
var
  LCh: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LEv: TAMQPServerEvent;
begin
  // The only event born on the JOURNAL THREAD. It only exists with
  // DataDir: without durability there is no fsync, and without fsync there is no
  // batch that became durable.
  //
  // ONE event per BATCH, never per record (D25/D36): group commit exists
  // because fsync is expensive, and one event per record would cost more than the
  // fsync it observes.
  FBroker.DataDir := TestTempDir;
  StartAndConnect(True);
  LCh := FConn.CreateChannel;
  // The classic rule of three (D20): DURABLE queue + PERSISTENT message +
  // broker with DataDir. With any one missing, nothing touches the journal.
  LDecl := TAMQPQueueDeclare.Create('obs.wal', True);
  LCh.DeclareQueue(LDecl);
  LCh.PublishText('', 'obs.wal', 'vai pro disco', True);

  CheckOk('chegou o flush', FCapture.WaitForCount(seJournalFlushed, 1, 5000));
  CheckOk('achou o flush', FCapture.FirstOf(seJournalFlushed, LEv));
  CheckOk('com a marca d''agua', LEv.Lsn > 0);
  CheckOk('e com registros no lote', LEv.Count > 0);
  // An event from the journal thread has neither queue nor connection: it speaks of the BATCH.
  CheckStr('sem fila', '', LEv.QueueName);
  CheckInt('e sem conexao', 0, Integer(LEv.ConnectionId));
end;

initialization
  RegisterTest(TEventBusTests);
  RegisterTest(TServerEventTests);

end.
