unit AMQP.ServerEventsTests;

{ Tests of the broker's observability (Phase 4.1, Inc. 1 -- D29-D36).

  Two fixtures, with different targets:

  - TEventBusTests exercises the bus ALONE, with no broker and no socket. It is
    where the mechanism's two anchor tests live, both deterministic by
    construction (no Sleep, no race):

      RingCheio_DescartaOMaisNovoEConta  -- the whole of D31. Fills the ring with
        the notifier STILL STOPPED, so the pressure is exact. Counting drops is
        not enough: the test requires that what is left is the PREFIX (1..4) and
        not the tail (7..10). An implementation that dropped the head would pass
        the counter and die in the second half.

      HandlerQueLevanta_ContaESegue -- D33. The mutation that has to kill it
        is bringing back the WIP's "except end": FailedCount would go to zero. And
        the second handler receiving everything is what separates "the notifier
        survived" from "it died on the first event".

  - TServerEventTests brings up the real broker on an ephemeral port and connects
    a TAMQPConnection to it: proves that the emitters are in the right places and
    that the fields come out filled in. The anchor here is
    SemAssinante_BrokerNaoEmiteNada, which is D29 -- the whole broker
    exercised, nobody subscribed, counter at zero.

  No test waits by Sleep (D35), and the kind of barrier is not a detail:

  - where PRESENCE is asserted, the test waits for the CONDITION (TCaptura.Espera).
    Drain alone does not do, and the first run proved it: it waits for the ring
    to empty, and a ring that has not received the event yet is already empty.
    Since the client's publish has no round-trip, draining right after it returns
    True before the broker has read the frame -- four tests failed
    exactly like that;
  - where ABSENCE is asserted, Drain is the right barrier, preceded by an
    operation WITH a round-trip (Channel.Close): when it returns, the broker has
    already gone through every emission point.

  The four Checa* functions exist so the test bodies are IDENTICAL
  in both dialects: only their implementation changes sides. }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
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
  TCaptura = class
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
    function Primeiro(AType: TAMQPServerEventType;
      out AEvent: TAMQPServerEvent): Boolean;
    /// Barrier by CONDITION (D35): waits for ACount events of the type to arrive.
    function Espera(AType: TAMQPServerEventType; ACount: Integer;
      ATimeoutMs: Cardinal): Boolean;
  end;

  { Handler that always raises -- the test double of D33. }
  TLevanta = class
  private
    FChamadas: Integer;
  public
    procedure Handle(const AEvent: TAMQPServerEvent);
    property Chamadas: Integer read FChamadas;
  end;

  { SLOW handler -- the test double of the D2 anchor (Inc. 2). }
  TLerdo = class
  private
    FChamadas: Integer;
  public
    procedure Handle(const AEvent: TAMQPServerEvent);
    property Chamadas: Integer read FChamadas;
  end;

  [TestFixture]
  TEventBusTests = class
  private
    FBus: TAMQPEventBus;
    FCap: TCaptura;
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure SemAssinante_NaoQuerNenhumTipo;
    [Test] procedure Assinatura_FiltraPorTipo;
    [Test] procedure EntregaNaOrdemDeEmissao;
    [Test] procedure RingCheio_DescartaOMaisNovoEConta;
    [Test] procedure HandlerQueLevanta_ContaESegue;
    [Test] procedure ReassinarSubstituiAMascara;
    [Test] procedure Unsubscribe_ParaDeEntregar;
    [Test] procedure TodoTipoTemNome;
  end;

  [TestFixture]
  TServerEventTests = class
  private
    FBroker: TAMQPServer;
    FConn: TAMQPConnection;
    FCap: TCaptura;
    procedure SobeEConecta(AAssina: Boolean);
    procedure ConectaCliente;
    procedure ConsumidorDeTeste(AChannel: TAMQPChannel;
      const ADelivery: TAMQPDelivery);
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure Conexao_EmiteEstabelecidaEAutenticada;
    [Test] procedure Canal_EmiteAberturaEFechamento;
    [Test] procedure Publish_EmiteComTamanhoEChave;
    [Test] procedure PublishSemRota_MarcaUnroutable;
    [Test] procedure ConsumoEAck_Emitem;
    [Test] procedure Conexao_EmiteFechamentoNoTeardown;
    [Test] procedure CanalMorreComAConexao_EmiteFechamento;
    [Test] procedure SemAssinante_BrokerNaoEmiteNada;
    [Test] procedure Enqueue_EDelivery_EmitemDoAtor;
    [Test] procedure Consumo_EmiteEntregaComTag;
    [Test] procedure TtlVencido_EmiteExpirada;
    [Test] procedure TetoDeFila_EmiteDescarte;
    [Test] procedure HandlerLento_NaoTravaOAtor;
    [Test] procedure JournalDuravel_EmiteFlush;
  end;

implementation

// Assertions with the type fixed: the value comes in already converted, and the
// TAssert overload is the same on both compilers.
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

procedure ChecaNao(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertFalse(AMsg, ACond);
end;

// RTL per dialect (CLAUDE.md rule): the test body is shared,
// but everything that calls the RTL has to be written on each side. Here it is
// FPC's GetTempDir versus Delphi's TPath.GetTempPath.
function DirTempDeTeste: string;
begin
  Result := IncludeTrailingPathDelimiter(DirTemporario) + 'amqpobs-'
    + FormatDateTime('yyyymmddhhnnsszzz', Now);
end;

// --- test doubles -----------------------------------------------------------

{ Capture sink: keeps everything that arrives, under a lock -- the handler runs on
  the notifier thread and the test reads from the runner thread. }
constructor TCaptura.Create;
begin
  inherited Create;
  FMon := TPcMonitor.Create;
end;

destructor TCaptura.Destroy;
begin
  FMon.Free;
  inherited Destroy;
end;

procedure TCaptura.Handle(const AEvent: TAMQPServerEvent);
begin
  FMon.Enter;
  try
    SetLength(FEvents, Length(FEvents) + 1);
    FEvents[High(FEvents)] := AEvent;
    FMon.PulseAll; // wakes whoever is in Espera
  finally
    FMon.Leave;
  end;
end;

function TCaptura.Count: Integer;
begin
  FMon.Enter;
  try
    Result := Length(FEvents);
  finally
    FMon.Leave;
  end;
end;

function TCaptura.Item(AIndex: Integer): TAMQPServerEvent;
begin
  FMon.Enter;
  try
    Result := FEvents[AIndex];
  finally
    FMon.Leave;
  end;
end;

function TCaptura.CountOf(AType: TAMQPServerEventType): Integer;
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

function TCaptura.Primeiro(AType: TAMQPServerEventType;
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
function TCaptura.Espera(AType: TAMQPServerEventType; ACount: Integer;
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
procedure TLevanta.Handle(const AEvent: TAMQPServerEvent);
begin
  Inc(FChamadas);
  raise Exception.Create('handler de teste explodiu');
end;

{ SLOW handler -- the test double of the D2 anchor. Sleeps on every event, on
  purpose: it is the only way to prove the actor does not wait for it. }
procedure TLerdo.Handle(const AEvent: TAMQPServerEvent);
begin
  Inc(FChamadas);
  Sleep(40);
end;

{ Declares a plain queue. The client's API is record-based -- it is the same
  helper the other server tests use. }
procedure DeclaraFila(AChannel: TAMQPChannel; const ANome: string);
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := TAMQPQueueDeclare.Create(ANome, False);
  AChannel.DeclareQueue(LDecl);
end;

// --- TEventBusTests ---------------------------------------------------------

procedure TEventBusTests.Setup;
begin
  FBus := TAMQPEventBus.Create;
  FCap := TCaptura.Create;
end;

procedure TEventBusTests.TearDown;
begin
  FBus.Free; // the destructor stops the notifier
  FCap.Free;
end;

procedure TEventBusTests.SemAssinante_NaoQuerNenhumTipo;
var
  LType: TAMQPServerEventType;
begin
  // D29 in one assertion: with no subscriber, Wants is False for EVERYTHING -- and
  // that is what makes the emitter not even build the record.
  for LType := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
    ChecaNao('nao quer ' + AmqpEventTypeName(LType), FBus.Wants(LType));
end;

procedure TEventBusTests.Assinatura_FiltraPorTipo;
begin
  FBus.Subscribe(FCap.Handle, [seChannelOpened, seMessageAcked]);
  ChecaOk('quer o que assinou', FBus.Wants(seChannelOpened));
  ChecaOk('quer o outro que assinou', FBus.Wants(seMessageAcked));
  ChecaNao('nao quer o que nao assinou', FBus.Wants(seConnectionClosed));

  // And the filter applies at DELIVERY, not only in Wants.
  FBus.Start;
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  FBus.Emit(AmqpNewEvent(seConnectionClosed));
  FBus.Emit(AmqpNewEvent(seMessageAcked));
  ChecaOk('drenou', FBus.Drain(2000));
  ChecaInt('so os dois assinados chegaram', 2, FCap.Count);
end;

procedure TEventBusTests.EntregaNaOrdemDeEmissao;
var
  I: Integer;
  LEv: TAMQPServerEvent;
begin
  FBus.Subscribe(FCap.Handle);
  FBus.Start;
  for I := 1 to 50 do
  begin
    LEv := AmqpNewEvent(seMessageEnqueued);
    LEv.Count := I;
    FBus.Emit(LEv);
  end;
  ChecaOk('drenou', FBus.Drain(2000));
  ChecaInt('chegaram todos', 50, FCap.Count);
  for I := 0 to 49 do
    ChecaInt('ordem preservada', I + 1, Integer(FCap.Item(I).Count));
end;

procedure TEventBusTests.RingCheio_DescartaOMaisNovoEConta;
var
  I: Integer;
  LEv: TAMQPServerEvent;
begin
  // The ANCHOR of D31. Without Start there is nobody to drain, so the ring really
  // fills up and the behavior under pressure is deterministic -- no Sleep,
  // no slow handler, no race.
  FBus.Capacity := 4;
  FBus.Subscribe(FCap.Handle);
  for I := 1 to 10 do
  begin
    LEv := AmqpNewEvent(seMessageEnqueued);
    LEv.Count := I;
    FBus.Emit(LEv);
  end;

  ChecaInt('so cabe a capacidade', 4, Integer(FBus.EmittedCount));
  ChecaInt('o resto foi contado como descartado', 6,
    Integer(FBus.DroppedCount));

  // And now what D31 has to PROVE: what is left is the PREFIX (1..4), not
  // the tail (7..10). Dropping the head would pass the counter above and fail
  // here -- it is the mutation this test exists to catch.
  FBus.Start;
  ChecaOk('drenou', FBus.Drain(2000));
  ChecaInt('entregou os que couberam', 4, FCap.Count);
  for I := 0 to 3 do
    ChecaInt('e sao os PRIMEIROS, nao os ultimos', I + 1,
      Integer(FCap.Item(I).Count));
end;

procedure TEventBusTests.HandlerQueLevanta_ContaESegue;
var
  LBomba: TLevanta;
  I: Integer;
begin
  // The ANCHOR of D33: the mutation that has to kill this test is bringing back
  // the WIP's "except end", which would zero FailedCount.
  LBomba := TLevanta.Create;
  try
    FBus.Subscribe(LBomba.Handle);
    FBus.Subscribe(FCap.Handle);
    FBus.Start;
    for I := 1 to 3 do
      FBus.Emit(AmqpNewEvent(seChannelOpened));
    ChecaOk('drenou', FBus.Drain(2000));

    ChecaInt('as tres excecoes foram contadas', 3, Integer(FBus.FailedCount));
    ChecaOk('e a ultima ficou registrada', FBus.LastFailure <> '');
    // What separates "survived" from "died on the first one": the SECOND handler
    // kept receiving, event after event.
    ChecaInt('o handler seguinte recebeu tudo', 3, FCap.Count);
  finally
    FBus.Unsubscribe(LBomba.Handle);
    LBomba.Free;
  end;
end;

procedure TEventBusTests.ReassinarSubstituiAMascara;
begin
  FBus.Subscribe(FCap.Handle);
  ChecaInt('um assinante', 1, FBus.SubscriberCount);
  ChecaOk('quer tudo', FBus.Wants(seConnectionClosed));

  FBus.Subscribe(FCap.Handle, [seChannelOpened]);
  ChecaInt('continua sendo UM assinante', 1, FBus.SubscriberCount);
  ChecaOk('quer o novo tipo', FBus.Wants(seChannelOpened));
  ChecaNao('e nao quer mais o antigo', FBus.Wants(seConnectionClosed));

  // Replacing, not duplicating, means delivering ONCE.
  FBus.Start;
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  ChecaOk('drenou', FBus.Drain(2000));
  ChecaInt('entregue uma unica vez', 1, FCap.Count);
end;

procedure TEventBusTests.Unsubscribe_ParaDeEntregar;
begin
  FBus.Subscribe(FCap.Handle);
  FBus.Start;
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  ChecaOk('drenou', FBus.Drain(2000));
  ChecaInt('chegou', 1, FCap.Count);

  FBus.Unsubscribe(FCap.Handle);
  ChecaInt('nenhum assinante', 0, FBus.SubscriberCount);
  ChecaNao('e nao quer mais nada', FBus.Wants(seChannelOpened));
  FBus.Emit(AmqpNewEvent(seChannelOpened));
  ChecaOk('drenou', FBus.Drain(2000));
  ChecaInt('e nao chegou mais nada', 1, FCap.Count);
end;

procedure TEventBusTests.TodoTipoTemNome;
var
  LType, LOutro: TAMQPServerEventType;
  LNome: string;
begin
  // Guards EVENT_NAMES against a new type added without a name: the array
  // is indexed by the enum, so the compiler requires the right count, but it does
  // not require the names to be distinct or non-empty.
  for LType := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
  begin
    LNome := AmqpEventTypeName(LType);
    ChecaOk('nome nao vazio', LNome <> '');
    for LOutro := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
      if LOutro <> LType then
        ChecaOk('nome distinto: ' + LNome,
          AmqpEventTypeName(LOutro) <> LNome);
  end;
end;

// --- TServerEventTests ------------------------------------------------------

procedure TServerEventTests.Setup;
begin
  FCap := TCaptura.Create;
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
    FBroker.Unsubscribe(FCap.Handle);
    try
      FBroker.Stop;
    except
    end;
    FreeAndNil(FBroker);
  end;
  FreeAndNil(FCap);
end;

{ Brings up the broker (subscribing before Start, which is the normal case) and
  connects a real client to it. }
procedure TServerEventTests.SobeEConecta(AAssina: Boolean);
begin
  if AAssina then
    FBroker.Subscribe(FCap.Handle);
  FBroker.Start;
  ConectaCliente;
end;

{ Separate from SobeEConecta because some tests need to configure the broker
  BEFORE Start (the ring capacity, for example) and connect afterwards. }
procedure TServerEventTests.ConectaCliente;
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
procedure TServerEventTests.ConsumidorDeTeste(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  AChannel.Ack(ADelivery.DeliveryTag, False);
end;

procedure TServerEventTests.Conexao_EmiteEstabelecidaEAutenticada;
var
  LEv: TAMQPServerEvent;
begin
  SobeEConecta(True);
  ChecaOk('chegou a autenticada', FCap.Espera(seConnectionAuthenticated, 1, 3000));

  ChecaInt('uma conexao estabelecida', 1,
    FCap.CountOf(seConnectionEstablished));
  ChecaOk('achou a estabelecida', FCap.Primeiro(seConnectionEstablished, LEv));
  ChecaOk('com endereco do peer', LEv.RemoteAddr <> '');
  ChecaOk('e com id de conexao', LEv.ConnectionId > 0);
  // The established one is born BEFORE the handshake: user and vhost do not exist yet,
  // and D34 says that an unpopulated field comes out ZEROED.
  ChecaStr('sem usuario ainda', '', LEv.Username);
  ChecaStr('sem vhost ainda', '', LEv.VHost);

  ChecaOk('achou a autenticada', FCap.Primeiro(seConnectionAuthenticated, LEv));
  ChecaStr('com o usuario autenticado', 'guest', LEv.Username);
  ChecaStr('e com o vhost aberto', '/', LEv.VHost);
  ChecaInt('evento de conexao vai no canal 0', 0, LEv.ChannelNumber);
end;

procedure TServerEventTests.Canal_EmiteAberturaEFechamento;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
begin
  SobeEConecta(True);
  LCh := FConn.CreateChannel;
  try
    LCh.Close;
  finally
    // A channel CLOSED by the client leaves its owner (the connection) -- whoever closes, frees.
    LCh.Free;
  end;
  ChecaOk('chegou o fechamento', FCap.Espera(seChannelClosed, 1, 3000));

  ChecaOk('achou a abertura', FCap.Primeiro(seChannelOpened, LEv));
  ChecaOk('com numero de canal', LEv.ChannelNumber > 0);
  ChecaInt('um fechamento', 1, FCap.CountOf(seChannelClosed));
  ChecaOk('achou o fechamento', FCap.Primeiro(seChannelClosed, LEv));
  ChecaOk('no mesmo canal', LEv.ChannelNumber > 0);
end;

procedure TServerEventTests.Publish_EmiteComTamanhoEChave;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LCorpo: string;
begin
  SobeEConecta(True);
  LCh := FConn.CreateChannel;
  DeclaraFila(LCh, 'obs.q');
  LCorpo := 'doze bytes!!';
  LCh.PublishText('', 'obs.q', LCorpo);
  ChecaOk('chegou o publish', FCap.Espera(seMessagePublished, 1, 3000));

  ChecaOk('achou o publish', FCap.Primeiro(seMessagePublished, LEv));
  ChecaStr('exchange default', '', LEv.ExchangeName);
  ChecaStr('routing key', 'obs.q', LEv.RoutingKey);
  // D34 in one assertion: the SIZE goes, never the body.
  ChecaInt('tamanho do corpo', Length(LCorpo), Integer(LEv.MessageSize));
  ChecaStr('roteou, entao sem motivo', '', LEv.Reason);
end;

procedure TServerEventTests.PublishSemRota_MarcaUnroutable;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
begin
  SobeEConecta(True);
  LCh := FConn.CreateChannel;
  LCh.PublishText('', 'fila.que.nao.existe', 'x');
  ChecaOk('chegou o publish', FCap.Espera(seMessagePublished, 1, 3000));

  // No route is NOT a refused publish: it is a successful publish that found no
  // queue (it gets an ack, and a Return if mandatory). Confusing the two would make
  // the observer count an error where there is none.
  ChecaInt('nenhum publish recusado', 0,
    FCap.CountOf(seMessagePublishRejected));
  ChecaOk('achou o publish', FCap.Primeiro(seMessagePublished, LEv));
  ChecaStr('marcado como sem rota', 'unroutable', LEv.Reason);
end;

procedure TServerEventTests.ConsumoEAck_Emitem;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LMsg: TAMQPGetResult;
begin
  SobeEConecta(True);
  LCh := FConn.CreateChannel;
  DeclaraFila(LCh, 'obs.ack');
  LCh.PublishText('', 'obs.ack', 'um');
  LMsg := LCh.BasicGet('obs.ack', False);
  ChecaOk('veio mensagem', LMsg.Found);
  LCh.Ack(LMsg.DeliveryTag, False);
  ChecaOk('chegou o ack', FCap.Espera(seMessageAcked, 1, 3000));

  ChecaInt('um ack', 1, FCap.CountOf(seMessageAcked));
  ChecaOk('achou o ack', FCap.Primeiro(seMessageAcked, LEv));
  ChecaOk('com delivery tag', LEv.DeliveryTag > 0);
  ChecaNao('sem multiple', LEv.Multiple);
  ChecaInt('resolveu uma entrega', 1, Integer(LEv.Count));
end;

procedure TServerEventTests.Conexao_EmiteFechamentoNoTeardown;
begin
  SobeEConecta(True);
  FConn.Close;
  FreeAndNil(FConn);
  ChecaOk('chegou o fechamento',
    FCap.Espera(seConnectionClosed, 1, 3000));

  // The close is born in the read thread's teardown -- and it only arrives because
  // the notifier is the LAST one to go down (see TAMQPServer.Stop).
  ChecaInt('uma conexao fechada', 1, FCap.CountOf(seConnectionClosed));
end;

procedure TServerEventTests.CanalMorreComAConexao_EmiteFechamento;
var
  LEv: TAMQPServerEvent;
begin
  // A channel opened and NEVER closed by the client: whoever closes it is the
  // connection's teardown. Without emitting here, an observer that counts opens
  // against closes would report a channel leak on every dropped connection -- that
  // is what the sample's race against the SmokeTest showed (3 opens, 2
  // closes), and it is the mutation this test exists to catch.
  SobeEConecta(True);
  FConn.CreateChannel;
  ChecaOk('chegou a abertura', FCap.Espera(seChannelOpened, 1, 3000));

  FConn.Close;
  FreeAndNil(FConn);
  ChecaOk('chegou o fechamento do canal',
    FCap.Espera(seChannelClosed, 1, 3000));
  ChecaOk('achou o fechamento', FCap.Primeiro(seChannelClosed, LEv));
  ChecaStr('e vem marcado como teardown', 'connection teardown', LEv.Reason);
end;

procedure TServerEventTests.SemAssinante_BrokerNaoEmiteNada;
var
  LCh: TAMQPChannel;
begin
  // The ANCHOR of D29: with the whole broker exercised and NOBODY subscribed,
  // no record is built and nothing enters the ring. The mutation that has to
  // kill this test is emitting without checking Wants.
  SobeEConecta(False);
  LCh := FConn.CreateChannel;
  DeclaraFila(LCh, 'obs.silencio');
  LCh.PublishText('', 'obs.silencio', 'nada disso deve virar evento');
  try
    LCh.Close; // round-trip: the broker HAS processed everything above when it returns
  finally
    LCh.Free;
  end;
  // Here Drain is the right barrier: the assertion is of ABSENCE, and the
  // Close-Ok round-trip guarantees the broker has already gone through every
  // emission point.
  ChecaOk('drenou', FBroker.DrainEvents(3000));

  ChecaInt('nada emitido', 0, Integer(FBroker.EventsEmitted));
  ChecaInt('e nada descartado', 0, Integer(FBroker.EventsDropped));
  ChecaInt('e nenhum handler falhou', 0, Integer(FBroker.EventsFailed));
end;

procedure TServerEventTests.Enqueue_EDelivery_EmitemDoAtor;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LMsg: TAMQPGetResult;
begin
  // These two are born in a worker of the actors' pool -- not on the read
  // thread. That they arrive is the proof that the D30 marshalling holds
  // for both origins, with the SAME contract.
  SobeEConecta(True);
  LCh := FConn.CreateChannel;
  DeclaraFila(LCh, 'obs.ator');
  LCh.PublishText('', 'obs.ator', 'do ator');
  ChecaOk('chegou o enqueue', FCap.Espera(seMessageEnqueued, 1, 3000));

  ChecaOk('achou o enqueue', FCap.Primeiro(seMessageEnqueued, LEv));
  ChecaStr('com a fila', 'obs.ator', LEv.QueueName);
  ChecaStr('e o vhost', '/', LEv.VHost);
  ChecaInt('e o tamanho', 7, Integer(LEv.MessageSize));
  // The actor lives on the other side of the mailbox: it knows neither connection
  // nor user, and D34 says an unpopulated field comes out ZEROED instead of garbage.
  ChecaInt('ator nao conhece a conexao', 0, Integer(LEv.ConnectionId));
  ChecaStr('nem o usuario', '', LEv.Username);

  LMsg := LCh.BasicGet('obs.ator', True);
  ChecaOk('veio mensagem', LMsg.Found);
end;

procedure TServerEventTests.Consumo_EmiteEntregaComTag;
var
  LCh: TAMQPChannel;
  LEv: TAMQPServerEvent;
  LTag: string;
begin
  SobeEConecta(True);
  LCh := FConn.CreateChannel;
  DeclaraFila(LCh, 'obs.entrega');
  LTag := LCh.Consume('obs.entrega', ConsumidorDeTeste);
  ChecaOk('consumidor registrado', LTag <> '');
  LCh.PublishText('', 'obs.entrega', 'entrega');
  ChecaOk('chegou a entrega', FCap.Espera(seMessageDelivered, 1, 3000));

  ChecaOk('achou a entrega', FCap.Primeiro(seMessageDelivered, LEv));
  ChecaStr('com a fila', 'obs.entrega', LEv.QueueName);
  ChecaStr('e a tag do consumidor', LTag, LEv.ConsumerTag);
  ChecaOk('e a tag de entrega', LEv.DeliveryTag > 0);
  ChecaInt('e o tamanho', 7, Integer(LEv.MessageSize));
end;

procedure TServerEventTests.TtlVencido_EmiteExpirada;
var
  LCh: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
  LEv: TAMQPServerEvent;
begin
  SobeEConecta(True);
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

  ChecaOk('chegou a expiracao', FCap.Espera(seMessageExpired, 1, 5000));
  ChecaOk('achou a expiracao', FCap.Primeiro(seMessageExpired, LEv));
  ChecaStr('na fila certa', 'obs.ttl', LEv.QueueName);
  ChecaStr('com a razao do x-death', 'expired', LEv.Reason);
end;

procedure TServerEventTests.TetoDeFila_EmiteDescarte;
var
  LCh: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
  LEv: TAMQPServerEvent;
  I: Integer;
begin
  SobeEConecta(True);
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

  ChecaOk('chegou o descarte', FCap.Espera(seMessageDropped, 1, 3000));
  ChecaOk('achou o descarte', FCap.Primeiro(seMessageDropped, LEv));
  ChecaStr('na fila certa', 'obs.teto', LEv.QueueName);
  ChecaStr('com a razao do x-death', 'maxlen', LEv.Reason);
end;

procedure TServerEventTests.HandlerLento_NaoTravaOAtor;
var
  LCh: TAMQPChannel;
  LLerdo: TLerdo;
  I, LEnfileiradas: Integer;
  LStats: TAMQPQueueStats;
  LFila: TAMQPServerQueue;
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
  LLerdo := TLerdo.Create;
  try
    FBroker.Subscribe(LLerdo.Handle, [seMessageEnqueued]);
    FBroker.Start;
    ConectaCliente;

    LCh := FConn.CreateChannel;
    DeclaraFila(LCh, 'obs.lenta');
    for I := 1 to 200 do
      LCh.PublishText('', 'obs.lenta', 'carga');

    // The queue is the witness: if the actor had waited for the handler, the 200
    // would not be in there within this deadline (200 x 40 ms = 8 s of handler alone).
    LFila := nil;
    LEnfileiradas := 0;
    LFila := FBroker.Engine.FindQueue('/', 'obs.lenta');
    ChecaOk('a fila existe', LFila <> nil);
    for I := 1 to 100 do
    begin
      LStats := LFila.Stats;
      LEnfileiradas := LStats.MessageCount;
      if LEnfileiradas >= 200 then
        Break;
      Sleep(20);
    end;
    ChecaInt('o ator enfileirou tudo sem esperar o handler', 200,
      LEnfileiradas);

    // And the price of best-effort shows up: with the handler at 40 ms per event and 8
    // slots, there were drops. This is D31 collecting its due, not a defect.
    ChecaOk('e houve descarte contado', FBroker.EventsDropped > 0);
  finally
    FBroker.Unsubscribe(LLerdo.Handle);
    // Stop the broker BEFORE freeing the slow handler: Stop drains the ring, and
    // draining calls the handler. Freeing it first would be the AV that D33 describes.
    FBroker.Stop;
    LLerdo.Free;
  end;
end;

procedure TServerEventTests.JournalDuravel_EmiteFlush;
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
  FBroker.DataDir := DirTempDeTeste;
  SobeEConecta(True);
  LCh := FConn.CreateChannel;
  // The classic rule of three (D20): DURABLE queue + PERSISTENT message +
  // broker with DataDir. With any one missing, nothing touches the journal.
  LDecl := TAMQPQueueDeclare.Create('obs.wal', True);
  LCh.DeclareQueue(LDecl);
  LCh.PublishText('', 'obs.wal', 'vai pro disco', True);

  ChecaOk('chegou o flush', FCap.Espera(seJournalFlushed, 1, 5000));
  ChecaOk('achou o flush', FCap.Primeiro(seJournalFlushed, LEv));
  ChecaOk('com a marca d''agua', LEv.Lsn > 0);
  ChecaOk('e com registros no lote', LEv.Count > 0);
  // An event from the journal thread has neither queue nor connection: it speaks of the BATCH.
  ChecaStr('sem fila', '', LEv.QueueName);
  ChecaInt('e sem conexao', 0, Integer(LEv.ConnectionId));
end;

initialization
  TDUnitX.RegisterTestFixture(TEventBusTests);
  TDUnitX.RegisterTestFixture(TServerEventTests);

end.
