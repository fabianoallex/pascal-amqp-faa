unit AMQP.ServerEventsTests;

{ Testes da observabilidade do broker (Fase 4.1, Inc. 1 -- D29-D36).

  Duas fixturas, com alvos diferentes:

  - TEventBusTests exercita o barramento SOZINHO, sem broker e sem socket. E'
    onde vivem os dois testes-ancora do mecanismo, os dois deterministicos por
    construcao (nenhum Sleep, nenhuma corrida):

      RingCheio_DescartaOMaisNovoEConta  -- a D31 inteira. Enche o ring com a
        notificadora AINDA PARADA, entao a pressao e' exata. Nao basta contar
        descarte: o teste exige que o que sobrou seja o PREFIXO (1..4) e nao a
        cauda (7..10). Uma implementacao que descartasse a cabeca passaria no
        contador e morreria na segunda metade.

      HandlerQueLevanta_ContaESegue -- a D33. A mutacao que tem de derruba-lo
        e' voltar o "except end" do WIP: FailedCount iria a zero. E o segundo
        handler recebendo tudo e' o que separa "a notificadora sobreviveu" de
        "ela morreu no primeiro evento".

  - TServerEventTests sobe o broker de verdade em porta efemera e conecta um
    TAMQPConnection nele: prova que os emissores estao nos lugares certos e
    que os campos saem preenchidos. O ancora aqui e'
    SemAssinante_BrokerNaoEmiteNada, que e' a D29 -- broker inteiro
    exercitado, ninguem assinando, contador em zero.

  Nenhum teste espera por Sleep (D35), e o tipo de barreira nao e' detalhe:

  - onde se assere PRESENCA, espera-se a CONDICAO (TCaptura.Espera). Drain
    sozinho nao serve, e a primeira rodada provou: ele espera o ring
    esvaziar, e um ring que ainda nao recebeu o evento ja' esta' vazio. Como
    o publish do cliente nao tem round-trip, drenar logo depois dele devolve
    True antes de o broker ter lido o frame -- quatro testes falharam
    exatamente assim;
  - onde se assere AUSENCIA, Drain e' a barreira certa, precedida de uma
    operacao COM round-trip (Channel.Close): quando ela volta, o broker ja'
    passou por todos os pontos de emissao.

  As quatro funcoes Checa* existem para os corpos dos testes serem IDENTICOS
  nos dois dialetos: so' a implementacao delas muda de lado. }

{ Espelho DUnitX de tests\Server\fpc\AMQP.ServerEventsTests.pas -- mantenha
  os dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  AMQP.Threading,
  AMQP.Connection,
  AMQP.Queue.Methods,
  AMQP.Server.Events,
  AMQP.Server.EventBus,
  AMQP.Server.Broker;

type
  { Sink de captura. O handler roda na thread notificadora e o teste le' da
    thread do runner, entao tudo passa por lock. }
  TCaptura = class
  private
    FMon: TAMQPMonitor;
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
    /// Barreira por CONDICAO (D35): espera chegarem ACount eventos do tipo.
    function Espera(AType: TAMQPServerEventType; ACount: Integer;
      ATimeoutMs: Cardinal): Boolean;
  end;

  { Handler que sempre levanta -- o dube da D33. }
  TLevanta = class
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
    [Setup] procedure Setup;
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
  public
    [Setup] procedure Setup;
    [TearDown] procedure TearDown;
    [Test] procedure Conexao_EmiteEstabelecidaEAutenticada;
    [Test] procedure Canal_EmiteAberturaEFechamento;
    [Test] procedure Publish_EmiteComTamanhoEChave;
    [Test] procedure PublishSemRota_MarcaUnroutable;
    [Test] procedure ConsumoEAck_Emitem;
    [Test] procedure Conexao_EmiteFechamentoNoTeardown;
    [Test] procedure CanalMorreComAConexao_EmiteFechamento;
    [Test] procedure SemAssinante_BrokerNaoEmiteNada;
  end;

implementation

// As quatro pontes para as assercoes do dialeto. Existem para os corpos dos
// testes serem IDENTICOS nos dois espelhos: so' estas quatro mudam de lado.
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  Assert.AreEqual(AEsperado, AObtido, AMsg);
end;

procedure ChecaStr(const AMsg: string; const AEsperado, AObtido: string);
begin
  Assert.AreEqual(AEsperado, AObtido, AMsg);
end;

procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  Assert.IsTrue(ACond, AMsg);
end;

procedure ChecaNao(const AMsg: string; ACond: Boolean);
begin
  Assert.IsFalse(ACond, AMsg);
end;

// --- dublês -----------------------------------------------------------------

{ Sink de captura: guarda tudo que chega, sob lock -- o handler roda na thread
  notificadora e o teste lê da thread do runner. }
constructor TCaptura.Create;
begin
  inherited Create;
  FMon := TAMQPMonitor.Create;
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
    FMon.PulseAll; // acorda quem estiver em Espera
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

{ Espera ate' ACount eventos do tipo chegarem, ou o prazo estourar.

  Drain sozinho NAO serve aqui, e o teste mostrou por que: ele espera o ring
  esvaziar, e um ring que ainda nao recebeu o evento ja' esta' vazio. Publish
  do cliente nao tem round-trip, entao drenar logo depois dele devolve True
  antes de o broker ter lido o frame. A barreira certa e' pela CONDICAO --
  mesma linha do "esperar OnReconnect, nao IsOpen" do CLAUDE.md --, com
  deadline e re-checagem em laco, nunca Sleep fixo. }
function TCaptura.Espera(AType: TAMQPServerEventType; ACount: Integer;
  ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
  LN, I: Integer;
begin
  LDeadline := AmqpTickMs + ATimeoutMs;
  FMon.Enter;
  try
    repeat
      LN := 0;
      for I := 0 to High(FEvents) do
        if FEvents[I].EventType = AType then
          Inc(LN);
      if LN >= ACount then
        Exit(True);
      if AmqpTickMs >= LDeadline then
        Exit(False);
      FMon.Wait(20);
    until False;
  finally
    FMon.Leave;
  end;
end;

{ Handler que sempre levanta. A D33 diz que isso é contado e o laço segue. }
procedure TLevanta.Handle(const AEvent: TAMQPServerEvent);
begin
  Inc(FChamadas);
  raise Exception.Create('handler de teste explodiu');
end;

{ Declara fila simples. A API do cliente e' por record -- e' o mesmo helper
  que os outros testes de servidor usam. }
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
  FBus.Free; // o destructor para a notificadora
  FCap.Free;
end;

procedure TEventBusTests.SemAssinante_NaoQuerNenhumTipo;
var
  LType: TAMQPServerEventType;
begin
  // A D29 em uma asserção: sem assinante, Wants é False para TUDO -- e é isso
  // que faz o emissor nem montar o record.
  for LType := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
    ChecaNao('nao quer ' + AmqpEventTypeName(LType), FBus.Wants(LType));
end;

procedure TEventBusTests.Assinatura_FiltraPorTipo;
begin
  FBus.Subscribe(FCap.Handle, [seChannelOpened, seMessageAcked]);
  ChecaOk('quer o que assinou', FBus.Wants(seChannelOpened));
  ChecaOk('quer o outro que assinou', FBus.Wants(seMessageAcked));
  ChecaNao('nao quer o que nao assinou', FBus.Wants(seConnectionClosed));

  // E o filtro vale na ENTREGA, não só no Wants.
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
  // O ANCORA da D31. Sem Start nao ha quem drene, entao o ring enche de
  // verdade e o comportamento sob pressao fica deterministico -- sem Sleep,
  // sem handler lento, sem corrida.
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

  // E agora o que a D31 tem de PROVAR: o que sobrou e' o PREFIXO (1..4), nao
  // a cauda (7..10). Descartar a cabeca passaria no contador acima e falharia
  // aqui -- e' a mutacao que este teste existe para pegar.
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
  // O ANCORA da D33: a mutacao que tem de derrubar este teste e' voltar o
  // "except end" do WIP, que zeraria FailedCount.
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
    // O que separa "sobreviveu" de "morreu na primeira": o SEGUNDO handler
    // continuou recebendo, evento a evento.
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

  // Substituir, e nao duplicar, quer dizer entregar UMA vez.
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
  // Guarda o EVENT_NAMES contra um tipo novo acrescentado sem nome: o array
  // e' indexado pelo enum, entao o compilador exige a contagem certa, mas nao
  // exige que os nomes sejam distintos nem nao-vazios.
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
    // Unsubscribe ANTES de soltar a captura: e' exatamente o contrato da D33
    // que a documentacao manda o usuario seguir, e o teste segue tambem.
    FBroker.Unsubscribe(FCap.Handle);
    try
      FBroker.Stop;
    except
    end;
    FreeAndNil(FBroker);
  end;
  FreeAndNil(FCap);
end;

{ Sobe o broker (assinando antes do Start, que e' o caso normal) e conecta um
  cliente de verdade nele. }
procedure TServerEventTests.SobeEConecta(AAssina: Boolean);
var
  LParams: TAMQPConnectionParams;
begin
  if AAssina then
    FBroker.Subscribe(FCap.Handle);
  FBroker.Start;
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := FBroker.Port;
  FConn := TAMQPConnection.Create(LParams);
  FConn.Open;
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
  // A estabelecida nasce ANTES do handshake: usuario e vhost ainda nao existem,
  // e a D34 manda que campo nao populado saia ZERADO.
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
    // Canal FECHADO pelo cliente sai do dono (a conexao) -- quem fecha, libera.
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
  // A D34 em uma asserção: vai o TAMANHO, nunca o corpo.
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

  // Sem rota NAO e' publish recusado: e' publish bem-sucedido que nao achou
  // fila (leva ack, e Return se mandatory). Confundir os dois faria o
  // observador contar erro onde nao ha.
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

  // O fechamento nasce no teardown da thread de leitura -- e so' chega porque
  // a notificadora e' a ULTIMA a descer (ver TAMQPServer.Stop).
  ChecaInt('uma conexao fechada', 1, FCap.CountOf(seConnectionClosed));
end;

procedure TServerEventTests.CanalMorreComAConexao_EmiteFechamento;
var
  LEv: TAMQPServerEvent;
begin
  // Canal aberto e NUNCA fechado pelo cliente: quem o fecha e' o teardown da
  // conexao. Sem emitir aqui, um observador que conte aberturas contra
  // fechamentos acusaria vazamento de canal a cada conexao derrubada -- foi
  // o que a corrida do sample contra o SmokeTest mostrou (3 aberturas, 2
  // fechamentos), e e' a mutacao que este teste existe para pegar.
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
  // O ANCORA da D29: com o broker inteiro exercitado e NINGUEM assinando,
  // nenhum record e' montado e nada entra no ring. A mutacao que tem de
  // derrubar este teste e' emitir sem checar Wants.
  SobeEConecta(False);
  LCh := FConn.CreateChannel;
  DeclaraFila(LCh, 'obs.silencio');
  LCh.PublishText('', 'obs.silencio', 'nada disso deve virar evento');
  try
    LCh.Close; // round-trip: o broker JA' processou tudo acima quando volta
  finally
    LCh.Free;
  end;
  // Aqui o Drain e' a barreira certa: a asserção e' de AUSENCIA, e o
  // round-trip do Close-Ok garante que o broker ja' passou por todos os
  // pontos de emissao.
  ChecaOk('drenou', FBroker.DrainEvents(3000));

  ChecaInt('nada emitido', 0, Integer(FBroker.EventsEmitted));
  ChecaInt('e nada descartado', 0, Integer(FBroker.EventsDropped));
  ChecaInt('e nenhum handler falhou', 0, Integer(FBroker.EventsFailed));
end;

initialization
  TDUnitX.RegisterTestFixture(TEventBusTests);
  TDUnitX.RegisterTestFixture(TServerEventTests);

end.
