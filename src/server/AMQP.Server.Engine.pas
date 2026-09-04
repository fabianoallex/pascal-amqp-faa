unit AMQP.Server.Engine;

{$I amqp.inc}

{ A engine -- sub-modulo broker, WS5 da Fase 2.

  Junta as tres pecas que as frentes anteriores construiram separadas:
   - a TOPOLOGIA por vhost (WS2: exchanges, filas declaradas, bindings e o
     roteamento direct/fanout/topic/headers, sob RWLock);
   - as FILAS VIVAS (WS3: um ator por fila, com estoque, nao-confirmadas e
     consumidores);
   - a ENTREGA (WS4: o alvo por canal).

  A FSM da conexao fala SO' com esta classe. Ela nao conhece numero de canal,
  nao levanta excecao de protocolo e nao sabe reply-code nenhum: devolve
  TAMQPEngineResult, e quem traduz para 403/404/405/406/540 -- e para "erro de
  canal" ou "erro de conexao" -- e' a FSM, que e' quem tem o canal na mao.

  Descritor x fila viva
  ---------------------
  O TAMQPVHost (WS2) guarda TAMQPQueueDef -- o DESCRITOR (flags, argumentos,
  dono). O objeto com estoque e consumidores e' o TAMQPServerQueue (WS3), que
  vive aqui, indexado por 'vhost/nome'. Os dois nascem e morrem juntos, e e'
  esta unit que garante isso: nao ha caminho que crie um sem o outro.

  Publicacao
  ----------
  TAMQPEngine implementa IAMQPMessageSink, entao o caminho de conteudo da
  Fase 1 (canal remonta -> Sink.RouteMessage) continua identico: so' muda quem
  esta' plugado. O match de bindings roda na thread do PUBLICADOR, sobre a
  tabela de headers viva do canal ("Invariante nova" do CLAUDE.md); so' o
  enqueue atravessa para o ator da fila.

  Refcount da interface: _AddRef/_Release sao no-op (-1) de proposito -- quem
  e' dono da engine e' o TAMQPServer, e a referencia de interface guardada na
  config das conexoes nao pode decidir o tempo de vida dela. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Exchange.Methods,
  AMQP.Threading,
  AMQP.Server.Types,
  AMQP.Server.Message,
  AMQP.Server.Header,
  AMQP.Server.Resources,
  AMQP.Server.Queue,
  AMQP.Server.VHost;

type
  { O que aconteceu numa operacao da engine. A FSM (WS5) mapeia para
    reply-code; a engine nao conhece nenhum. }
  TAMQPEngineResult = (
    amqerOk,
    /// recurso citado nao existe -- 404 NOT_FOUND
    amqerNaoEncontrado,
    /// redeclare do mesmo nome com flags/argumentos diferentes -- 406
    amqerDivergente,
    /// tipo de exchange desconhecido, ou 'x-match' invalido -- 406
    amqerTipoInvalido,
    /// delete com if-unused e o recurso em uso -- 406
    amqerEmUso,
    /// delete com if-empty e a fila com mensagens -- 406
    amqerNaoVazia,
    /// nome reservado ('amq.') vindo do cliente -- 403 ACCESS_REFUSED
    amqerNomeReservado,
    /// fila exclusiva de OUTRA conexao -- 405 RESOURCE_LOCKED
    amqerExclusivaDeOutro
  );

  TAMQPEngine = class;

  { Liga uma fila ao dead-lettering da engine.

    Existe porque a IAMQPDeadLetterSink nao carrega vhost -- e nao deveria: a
    fila nao sabe em que vhost mora, e alargar a interface so' para carregar um
    dado que o ADAPTADOR pode fixar seria piorar o contrato para todo mundo.
    Um adaptador por vhost fixa o nome e a interface fica com o que interessa.

    Tempo de vida: e' TInterfacedObject e a fila segura a referencia, entao o
    adaptador vive enquanto a fila viver. O ponteiro para a engine e cru de
    propriedade -- a engine e' dona das filas e as para no proprio destrutor,
    entao nao ha janela em que uma fila viva veja uma engine morta. }
  TAMQPDeadLetterAdapter = class(TInterfacedObject, IAMQPDeadLetterSink)
  private
    FEngine: TAMQPEngine;
    FVHost: string;
  public
    constructor Create(AEngine: TAMQPEngine; const AVHost: string);
    procedure DeadLetter(const ADlx, ARoutingKey: string;
      AMessage: TAMQPMessage; APriority: Byte);
  end;

  TAMQPEngine = class(TInterfacedObject, IAMQPMessageSink)
  private
    FLock: TCriticalSection;
    FVHosts: TAMQPVHostSet;
    // Filas vivas, chaveadas por 'vhost/nome'. O descritor equivalente vive
    // no TAMQPVHost; os dois sao criados e destruidos juntos.
    FQueues: TDictionary<string, TAMQPServerQueue>;
    FMaxQueueLength: Integer;
    FPool: TAMQPThreadPool;
    FRouted: Integer;   // atomico -- publicacoes com ao menos uma rota
    FUnrouted: Integer; // atomico -- publicacoes sem rota
    function Key(const AVHost, AName: string): string;
    // Chamar SOB FLock.
    function FindQueueLocked(const AVHost, AName: string): TAMQPServerQueue;
    function CheckExclusive(ADef: TAMQPQueueDef;
      AOwnerId: NativeUInt): Boolean;
  protected
    // IAMQPMessageSink -- refcount desligado (ver cabecalho).
    function _AddRef: Integer; stdcall;
    function _Release: Integer; stdcall;
  public
    constructor Create;
    destructor Destroy; override;

    /// Garante que o vhost existe na topologia (com os exchanges
    /// predefinidos). Chamado quando uma conexao abre nele.
    procedure EnsureVHost(const AVHost: string);

    // --- exchanges ---
    function DeclareExchange(const AVHost, AName, AType: string;
      APassive, ADurable, AAutoDelete, AInternal: Boolean;
      AArguments: TAMQPFieldTable): TAMQPEngineResult;
    function DeleteExchange(const AVHost, AName: string;
      AIfUnused: Boolean): TAMQPEngineResult;
    function BindExchange(const AVHost, ADestination, ASource,
      ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPEngineResult;
    function UnbindExchange(const AVHost, ADestination, ASource,
      ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPEngineResult;
    function ExchangeExists(const AVHost, AName: string): Boolean;

    // --- filas ---
    /// Declara (ou verifica, com APassive) uma fila. AOwnerId identifica a
    /// conexao pedinte -- e' o que barra o uso de fila exclusiva alheia.
    /// Devolve as contagens do Declare-Ok.
    function DeclareQueue(const AVHost, AName: string;
      APassive, ADurable, AExclusive, AAutoDelete: Boolean;
      AArguments: TAMQPFieldTable; AOwnerId: NativeUInt;
      out AMessageCount, AConsumerCount: Integer): TAMQPEngineResult;
    function DeleteQueue(const AVHost, AName: string;
      AIfUnused, AIfEmpty: Boolean; AOwnerId: NativeUInt;
      out AMessageCount: Integer): TAMQPEngineResult;
    function PurgeQueue(const AVHost, AName: string; AOwnerId: NativeUInt;
      out AMessageCount: Integer): TAMQPEngineResult;
    function BindQueue(const AVHost, AQueue, AExchange, ARoutingKey: string;
      AArguments: TAMQPFieldTable; AOwnerId: NativeUInt): TAMQPEngineResult;
    function UnbindQueue(const AVHost, AQueue, AExchange, ARoutingKey: string;
      AArguments: TAMQPFieldTable; AOwnerId: NativeUInt): TAMQPEngineResult;
    function QueueExists(const AVHost, AName: string): Boolean;

    // --- consumo ---
    function AddConsumer(const AVHost, AQueue, AConsumerTag: string;
      ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget;
      AOwnerId: NativeUInt): TAMQPEngineResult;
    function RemoveConsumer(const AVHost, AQueue, AConsumerTag: string;
      AChannelId: NativeUInt): TAMQPEngineResult;
    /// Basic.Get. AFound=False com resultado amqerOk significa fila vazia
    /// (Get-Empty), nao erro. A referencia devolvida em AMessage e' do
    /// CHAMADOR.
    function GetMessage(const AVHost, AQueue: string; ANoAck: Boolean;
      ADeliveryTag: UInt64; AChannelId: NativeUInt; AOwnerId: NativeUInt;
      out AFound: Boolean; out AMessage: TAMQPMessage;
      out ARedelivered: Boolean; out AMessageCount: Integer): TAMQPEngineResult;

    // --- ciclo de vida automatico (WS7) ---
    /// Apaga a fila se ela for `auto-delete`, JA TIVER TIDO consumidor e nao
    /// tiver mais nenhum. Chamar DEPOIS de remover um consumidor -- e como o
    /// Stats e' comando sincrono e a caixa da fila e' FIFO, a leitura aqui ja
    /// enxerga a remocao que acabou de ser postada. No-op se a fila nao
    /// existe ou nao e' auto-delete.
    procedure MaybeAutoDeleteQueue(const AVHost, AQueue: string);
    /// Apaga o exchange se ele for `auto-delete` e nao for mais origem de
    /// binding nenhum. Chamar depois de todo unbind (inclusive os implicitos,
    /// como os que somem junto com uma fila apagada).
    procedure MaybeAutoDeleteExchange(const AVHost, AExchange: string);
    /// Varre os exchanges auto-delete e apaga os que ficaram sem binding.
    /// Existe porque apagar uma FILA leva junto os bindings dela, e nao ha
    /// como saber de fora quais exchanges ficaram orfaos -- exchange
    /// auto-delete e' raro, entao a varredura e' mais barata que rastrear.
    procedure SweepAutoDeleteExchanges(const AVHost: string);
    /// Apaga TODAS as filas exclusivas de AOwnerId (a conexao dona caiu).
    /// Devolve quantas foram.
    function DeleteExclusiveQueuesOf(const AVHost: string;
      AOwnerId: NativeUInt): Integer;

    /// A fila viva de AName, ou nil. Usada pelo canal para encaminhar
    /// ack/nack/reject e para o teardown -- o chamador NAO e' dono.
    function FindQueue(const AVHost, AName: string): TAMQPServerQueue;

    // --- publicacao (IAMQPMessageSink) ---
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage; out ARejeitada: Boolean): Boolean;

    /// Republica uma mensagem ja' montada (usada pelo dead-lettering). Roda na
    /// thread do ATOR da fila de origem: le a topologia sob RWLock e posta na
    /// caixa das filas de destino, sem I/O e sem espera (D17).
    procedure RepublishTo(const AVHost, AExchange, ARoutingKey: string;
      AMessage: TAMQPMessage; APriority: Byte);

    /// Cutuca todas as filas (caso degradado da D3 -- ver
    /// TAMQPServerQueue.PostDeliverTick). Chamado pela thread monitora.
    procedure DeliverTick;

    /// Apaga as filas com x-expires que passaram do prazo SEM USO (WS8).
    /// Chamado pela thread monitora, como o DeliverTick.
    ///
    /// Quem DETECTA e' esta varredura; quem EXECUTA e' o DeleteQueue de
    /// sempre -- o mesmo caminho do Queue.Delete vindo do cliente, com o
    /// mesmo cuidado de parar o ator antes de liberar. Duplicar o teardown
    /// aqui seria a receita do double-free que a Fase 1 ja pagou uma vez.
    procedure ExpireIdleQueues;

    /// Teto de mensagens prontas por fila (decisao D7). 0 = ilimitado. Vale
    /// para as filas criadas DEPOIS de atribuido.
    property MaxQueueLength: Integer read FMaxQueueLength
      write FMaxQueueLength;
    /// Pool onde os atores das filas sao agendados (nil = AmqpPool global).
    property Pool: TAMQPThreadPool read FPool write FPool;
    /// Publicacoes que acharam ao menos uma fila / que nao acharam nenhuma.
    function RoutedCount: Integer;
    function UnroutedCount: Integer;
  end;

implementation

{ TAMQPDeadLetterAdapter }

constructor TAMQPDeadLetterAdapter.Create(AEngine: TAMQPEngine;
  const AVHost: string);
begin
  inherited Create;
  FEngine := AEngine;
  FVHost := AVHost;
end;

procedure TAMQPDeadLetterAdapter.DeadLetter(const ADlx, ARoutingKey: string;
  AMessage: TAMQPMessage; APriority: Byte);
begin
  FEngine.RepublishTo(FVHost, ADlx, ARoutingKey, AMessage, APriority);
end;

{ TAMQPEngine }

constructor TAMQPEngine.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FVHosts := TAMQPVHostSet.Create;
  FQueues := TDictionary<string, TAMQPServerQueue>.Create;
end;

destructor TAMQPEngine.Destroy;
var
  LQ: TAMQPServerQueue;
begin
  // Ordem: parar TODOS os atores antes de liberar qualquer coisa -- um ator
  // vivo com a topologia ja liberada seria use-after-free.
  for LQ in FQueues.Values do
  begin
    try
      LQ.Stop;
    except
      on E: Exception do
        ; // ator preso: o proprio Stop ja decidiu nao liberar nada
    end;
    LQ.Free;
  end;
  FQueues.Free;
  FVHosts.Free;
  FLock.Free;
  inherited;
end;

function TAMQPEngine._AddRef: Integer; stdcall;
begin
  Result := -1; // sem refcount: o dono e' o TAMQPServer
end;

function TAMQPEngine._Release: Integer; stdcall;
begin
  Result := -1;
end;

function TAMQPEngine.Key(const AVHost, AName: string): string;
begin
  Result := AVHost + '/' + AName;
end;

procedure TAMQPEngine.EnsureVHost(const AVHost: string);
begin
  FVHosts.GetOrCreate(AVHost);
end;

function TAMQPEngine.FindQueueLocked(const AVHost,
  AName: string): TAMQPServerQueue;
begin
  if not FQueues.TryGetValue(Key(AVHost, AName), Result) then
    Result := nil;
end;

function TAMQPEngine.FindQueue(const AVHost, AName: string): TAMQPServerQueue;
begin
  FLock.Enter;
  try
    Result := FindQueueLocked(AVHost, AName);
  finally
    FLock.Leave;
  end;
end;

// True se AOwnerId pode usar a fila: ou ela nao e' exclusiva, ou o dono e'
// exatamente esta conexao. AOwnerId=0 (chamador interno) sempre passa.
function TAMQPEngine.CheckExclusive(ADef: TAMQPQueueDef;
  AOwnerId: NativeUInt): Boolean;
begin
  Result := (ADef = nil) or (not ADef.Exclusive) or (AOwnerId = 0)
    or (ADef.OwnerId = AOwnerId);
end;

{ --- exchanges --- }

function TAMQPEngine.DeclareExchange(const AVHost, AName, AType: string;
  APassive, ADurable, AAutoDelete, AInternal: Boolean;
  AArguments: TAMQPFieldTable): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LRes: TAMQPTopologyResult;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  if APassive then
  begin
    // Passive so' verifica existencia; nao declara nem valida flags.
    AArguments.Free;
    if LVHost.ExchangeExists(AName) then
      Result := amqerOk
    else
      Result := amqerNaoEncontrado;
    Exit;
  end;

  LRes := LVHost.DeclareExchange(AName, AType, ADurable, AAutoDelete,
    AInternal, AArguments); // o vhost toma posse dos argumentos
  case LRes of
    amqtrOk, amqtrEquivalente: Result := amqerOk;
    amqtrDivergente: Result := amqerDivergente;
    amqtrTipoInvalido: Result := amqerTipoInvalido;
  else
    Result := amqerNaoEncontrado;
  end;
end;

function TAMQPEngine.DeleteExchange(const AVHost, AName: string;
  AIfUnused: Boolean): TAMQPEngineResult;
var
  LRes: TAMQPTopologyResult;
begin
  LRes := FVHosts.GetOrCreate(AVHost).DeleteExchange(AName, AIfUnused);
  case LRes of
    amqtrOk: Result := amqerOk;
    amqtrEmUso: Result := amqerEmUso;
    // Idempotente pela mesma razao do DeleteQueue acima -- ver o comentario
    // la'. Manter os dois iguais importa: um setup que apaga antes de
    // redeclarar mexe nos dois.
    amqtrNaoEncontrado: Result := amqerOk;
  else
    Result := amqerOk;
  end;
end;

function TAMQPEngine.BindExchange(const AVHost, ADestination, ASource,
  ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPEngineResult;
var
  LRes: TAMQPTopologyResult;
begin
  LRes := FVHosts.GetOrCreate(AVHost).BindExchange(ADestination, ASource,
    ARoutingKey, AArguments);
  case LRes of
    amqtrOk, amqtrEquivalente: Result := amqerOk;
    amqtrTipoInvalido: Result := amqerTipoInvalido;
  else
    Result := amqerNaoEncontrado;
  end;
end;

function TAMQPEngine.UnbindExchange(const AVHost, ADestination, ASource,
  ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPEngineResult;
var
  LRes: TAMQPTopologyResult;
begin
  LRes := FVHosts.GetOrCreate(AVHost).UnbindExchange(ADestination, ASource,
    ARoutingKey, AArguments);
  if LRes in [amqtrOk, amqtrEquivalente] then
    Result := amqerOk
  else
    Result := amqerNaoEncontrado;
end;

function TAMQPEngine.ExchangeExists(const AVHost, AName: string): Boolean;
begin
  Result := FVHosts.GetOrCreate(AVHost).ExchangeExists(AName);
end;

{ --- filas --- }

function TAMQPEngine.DeclareQueue(const AVHost, AName: string;
  APassive, ADurable, AExclusive, AAutoDelete: Boolean;
  AArguments: TAMQPFieldTable; AOwnerId: NativeUInt;
  out AMessageCount, AConsumerCount: Integer): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LRes: TAMQPTopologyResult;
  LQueue: TAMQPServerQueue;
  LStats: TAMQPQueueStats;
  LDef: TAMQPQueueDef;
begin
  AMessageCount := 0;
  AConsumerCount := 0;
  LVHost := FVHosts.GetOrCreate(AVHost);

  if APassive then
  begin
    AArguments.Free;
    LDef := LVHost.QueueDef(AName);
    if LDef = nil then
      Exit(amqerNaoEncontrado);
    if not CheckExclusive(LDef, AOwnerId) then
      Exit(amqerExclusivaDeOutro);
    Result := amqerOk;
  end
  else
  begin
    LDef := LVHost.QueueDef(AName);
    if (LDef <> nil) and (not CheckExclusive(LDef, AOwnerId)) then
    begin
      AArguments.Free;
      Exit(amqerExclusivaDeOutro);
    end;
    LRes := LVHost.DeclareQueue(AName, ADurable, AExclusive, AAutoDelete,
      AArguments, AOwnerId);
    case LRes of
      amqtrOk, amqtrEquivalente: Result := amqerOk;
      amqtrDivergente: Exit(amqerDivergente);
    else
      Exit(amqerNaoEncontrado);
    end;
  end;

  // Fila viva: nasce junto com o descritor, some junto com ele.
  FLock.Enter;
  try
    LQueue := FindQueueLocked(AVHost, AName);
    if (LQueue = nil) and (not APassive) then
    begin
      // A fila viva nasce com a MESMA politica do descritor (WS2/WS4): o
      // descritor ja e' a fonte da verdade dos x-arguments, e re-parsear aqui
      // abriria espaco para os dois divergirem.
      LDef := LVHost.QueueDef(AName);
      if LDef <> nil then
        LQueue := TAMQPServerQueue.Create(AName, LDef.Policy, FMaxQueueLength,
          FPool)
      else
        LQueue := TAMQPServerQueue.Create(AName, FMaxQueueLength, FPool);
      // WS5: e' por aqui que a fila alcanca a topologia para republicar as
      // mortas. Sem sink, uma morte vira simples descarte.
      LQueue.DeadLetterSink := TAMQPDeadLetterAdapter.Create(Self, AVHost);
      FQueues.Add(Key(AVHost, AName), LQueue);
    end
    else if LQueue <> nil then
      // WS8: REDECLARE conta como uso e reinicia o relogio do x-expires --
      // e' um dos tres eventos da regra (os outros sao consumidor e
      // Basic.Get; publicar nao conta).
      LQueue.PostTouch();
  finally
    FLock.Leave;
  end;

  if LQueue <> nil then
  begin
    LStats := LQueue.Stats;
    AMessageCount := LStats.MessageCount;
    AConsumerCount := LStats.ConsumerCount;
  end;
end;

function TAMQPEngine.DeleteQueue(const AVHost, AName: string;
  AIfUnused, AIfEmpty: Boolean; AOwnerId: NativeUInt;
  out AMessageCount: Integer): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LQueue: TAMQPServerQueue;
  LDel: TAMQPQueueDeleteResult;
begin
  AMessageCount := 0;
  LVHost := FVHosts.GetOrCreate(AVHost);
  if not CheckExclusive(LVHost.QueueDef(AName), AOwnerId) then
    Exit(amqerExclusivaDeOutro);

  FLock.Enter;
  try
    LQueue := FindQueueLocked(AVHost, AName);
    if LQueue = nil then
      // DELETE E' IDEMPOTENTE: apagar fila que nao existe devolve Delete-Ok
      // com contagem zero, e nao 404. A letra da spec manda levantar, mas o
      // RabbitMQ deixou idempotente ha muito tempo e e' com isso que o
      // ecossistema conta -- inclusive o sample RetryDlqVcl deste repo, que
      // apaga antes de redeclarar com argumentos novos ("redeclarar com args
      // diferentes seria PRECONDITION_FAILED").
      //
      // Achado pela WS10: o SmokeTest com os passos de TTL+DLX passava contra
      // o RabbitMQ e batia 404 contra o broker embutido. E' exatamente o tipo
      // de divergencia que rodar os MESMOS passos contra os dois brokers
      // existe para pegar. Desvio deliberado da letra da spec, documentado.
      Exit(amqerOk);

    // O ator decide: if-unused/if-empty sao estado DELE.
    LDel := LQueue.Delete(AIfUnused, AIfEmpty, AMessageCount);
    case LDel of
      amqqdEmUso: Exit(amqerEmUso);
      amqqdNaoVazia: Exit(amqerNaoVazia);
    end;
    // amqqdOk: o Delete ja parou o ator (e drenou o estoque).
    FQueues.Remove(Key(AVHost, AName));
    LQueue.Free;
  finally
    FLock.Leave;
  end;

  LVHost.DeleteQueue(AName); // tira o descritor e os bindings pendurados
  // Os bindings que sumiram com a fila podem ter sido os ultimos de um
  // exchange auto-delete.
  SweepAutoDeleteExchanges(AVHost);
  Result := amqerOk;
end;

function TAMQPEngine.PurgeQueue(const AVHost, AName: string;
  AOwnerId: NativeUInt; out AMessageCount: Integer): TAMQPEngineResult;
var
  LQueue: TAMQPServerQueue;
begin
  AMessageCount := 0;
  if not CheckExclusive(FVHosts.GetOrCreate(AVHost).QueueDef(AName),
    AOwnerId) then
    Exit(amqerExclusivaDeOutro);
  LQueue := FindQueue(AVHost, AName);
  if LQueue = nil then
    Exit(amqerNaoEncontrado);
  AMessageCount := LQueue.Purge;
  Result := amqerOk;
end;

function TAMQPEngine.BindQueue(const AVHost, AQueue, AExchange,
  ARoutingKey: string; AArguments: TAMQPFieldTable;
  AOwnerId: NativeUInt): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LRes: TAMQPTopologyResult;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  if not CheckExclusive(LVHost.QueueDef(AQueue), AOwnerId) then
  begin
    AArguments.Free;
    Exit(amqerExclusivaDeOutro);
  end;
  LRes := LVHost.BindQueue(AExchange, AQueue, ARoutingKey, AArguments);
  case LRes of
    amqtrOk, amqtrEquivalente: Result := amqerOk;
    amqtrTipoInvalido: Result := amqerTipoInvalido;
  else
    Result := amqerNaoEncontrado;
  end;
end;

function TAMQPEngine.UnbindQueue(const AVHost, AQueue, AExchange,
  ARoutingKey: string; AArguments: TAMQPFieldTable;
  AOwnerId: NativeUInt): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LRes: TAMQPTopologyResult;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  if not CheckExclusive(LVHost.QueueDef(AQueue), AOwnerId) then
  begin
    AArguments.Free;
    Exit(amqerExclusivaDeOutro);
  end;
  LRes := LVHost.UnbindQueue(AExchange, AQueue, ARoutingKey, AArguments);
  if LRes in [amqtrOk, amqtrEquivalente] then
    Result := amqerOk
  else
    Result := amqerNaoEncontrado;
end;

function TAMQPEngine.QueueExists(const AVHost, AName: string): Boolean;
begin
  Result := FVHosts.GetOrCreate(AVHost).QueueExists(AName);
end;

{ --- consumo --- }

function TAMQPEngine.AddConsumer(const AVHost, AQueue, AConsumerTag: string;
  ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget;
  AOwnerId: NativeUInt): TAMQPEngineResult;
var
  LQueue: TAMQPServerQueue;
begin
  if not CheckExclusive(FVHosts.GetOrCreate(AVHost).QueueDef(AQueue),
    AOwnerId) then
    Exit(amqerExclusivaDeOutro);
  LQueue := FindQueue(AVHost, AQueue);
  if LQueue = nil then
    Exit(amqerNaoEncontrado);
  LQueue.PostAddConsumer(TAMQPServerConsumer.Create(AConsumerTag, ANoAck,
    AExclusive, ATarget));
  Result := amqerOk;
end;

function TAMQPEngine.RemoveConsumer(const AVHost, AQueue,
  AConsumerTag: string; AChannelId: NativeUInt): TAMQPEngineResult;
var
  LQueue: TAMQPServerQueue;
begin
  LQueue := FindQueue(AVHost, AQueue);
  if LQueue = nil then
    Exit(amqerNaoEncontrado);
  LQueue.PostRemoveConsumer(AChannelId, AConsumerTag);
  Result := amqerOk;
end;

function TAMQPEngine.GetMessage(const AVHost, AQueue: string;
  ANoAck: Boolean; ADeliveryTag: UInt64; AChannelId: NativeUInt;
  AOwnerId: NativeUInt; out AFound: Boolean; out AMessage: TAMQPMessage;
  out ARedelivered: Boolean; out AMessageCount: Integer): TAMQPEngineResult;
var
  LQueue: TAMQPServerQueue;
begin
  AFound := False;
  AMessage := nil;
  ARedelivered := False;
  AMessageCount := 0;
  if not CheckExclusive(FVHosts.GetOrCreate(AVHost).QueueDef(AQueue),
    AOwnerId) then
    Exit(amqerExclusivaDeOutro);
  LQueue := FindQueue(AVHost, AQueue);
  if LQueue = nil then
    Exit(amqerNaoEncontrado);

  AFound := LQueue.Get(AChannelId, ADeliveryTag, ANoAck, AMessage,
    ARedelivered);
  // message-count do Get-Ok: quantas SOBRARAM depois desta.
  AMessageCount := LQueue.Stats.MessageCount;
  Result := amqerOk;
end;

{ --- ciclo de vida automatico (WS7) --- }

procedure TAMQPEngine.MaybeAutoDeleteQueue(const AVHost, AQueue: string);
var
  LDef: TAMQPQueueDef;
  LQueue: TAMQPServerQueue;
  LStats: TAMQPQueueStats;
  LCount: Integer;
begin
  LDef := FVHosts.GetOrCreate(AVHost).QueueDef(AQueue);
  if (LDef = nil) or (not LDef.AutoDelete) then
    Exit;
  LQueue := FindQueue(AVHost, AQueue);
  if LQueue = nil then
    Exit;
  LStats := LQueue.Stats; // sincrono: barreira sobre a remocao ja postada
  // Uma fila auto-delete que NUNCA teve consumidor nao some -- senao ela
  // desapareceria entre o declare e o consume (mesma regra do RabbitMQ).
  if (not LStats.EverHadConsumer) or (LStats.ConsumerCount > 0) then
    Exit;
  DeleteQueue(AVHost, AQueue, False, False, 0, LCount);
end;

procedure TAMQPEngine.MaybeAutoDeleteExchange(const AVHost,
  AExchange: string);
var
  LVHost: TAMQPVHost;
  LDef: TAMQPExchangeDef;
begin
  if AExchange = '' then
    Exit; // o exchange default nunca some
  LVHost := FVHosts.GetOrCreate(AVHost);
  LDef := LVHost.ExchangeDef(AExchange);
  if (LDef = nil) or (not LDef.AutoDelete) then
    Exit;
  if LVHost.HasBindingsFrom(AExchange) then
    Exit;
  LVHost.DeleteExchange(AExchange, False);
end;

procedure TAMQPEngine.SweepAutoDeleteExchanges(const AVHost: string);
var
  LNomes: TArray<string>;
  I: Integer;
begin
  LNomes := FVHosts.GetOrCreate(AVHost).ExchangeNames;
  for I := 0 to High(LNomes) do
    MaybeAutoDeleteExchange(AVHost, LNomes[I]);
end;

function TAMQPEngine.DeleteExclusiveQueuesOf(const AVHost: string;
  AOwnerId: NativeUInt): Integer;
var
  LNomes: TArray<string>;
  I, LCount: Integer;
begin
  Result := 0;
  if AOwnerId = 0 then
    Exit;
  LNomes := FVHosts.GetOrCreate(AVHost).ExclusiveQueuesOf(AOwnerId);
  for I := 0 to High(LNomes) do
    // AOwnerId=0 no DeleteQueue: chamada interna, ja sabemos que o dono e'
    // esta conexao (foi assim que a fila entrou na lista).
    if DeleteQueue(AVHost, LNomes[I], False, False, 0, LCount) = amqerOk then
      Inc(Result);
end;

{ --- publicacao --- }

function TAMQPEngine.RouteMessage(const AVHost: string;
  const AMessage: TAMQPServerMessage; out ARejeitada: Boolean): Boolean;
var
  LVHost: TAMQPVHost;
  LDestinos: TArray<string>;
  LMsg: TAMQPMessage;
  LQueue: TAMQPServerQueue;
  I: Integer;
  LHeaders: TAMQPFieldTable;
  LPriority: Byte;
  LTtlMs: Int64;
begin
  ARejeitada := False;
  LVHost := FVHosts.GetOrCreate(AVHost);
  // Headers vivos do canal: o match roda AQUI, na thread do publicador
  // (invariante da Fase 2), nao depois do enqueue.
  LHeaders := nil;
  if AMessage.Properties.Has(bpHeaders) then
    LHeaders := AMessage.Properties.Headers;

  LDestinos := LVHost.Route(AMessage.Exchange, AMessage.RoutingKey, LHeaders);
  Result := Length(LDestinos) > 0;
  if not Result then
  begin
    AmqpAtomicInc(FUnrouted);
    Exit;
  end;
  AmqpAtomicInc(FRouted);

  // UMA mensagem para N filas: corpo e header cru compartilhados sem copia,
  // cada fila tirando a referencia dela (decisao de arquitetura da Fase 2).
  // Prioridade e 'expiration' sao lidas AQUI, na thread do publicador, pelo
  // mesmo motivo do match de headers: a TAMQPMessage guarda o header CRU
  // (decisao D1) e quem ainda tem as propriedades decodificadas e' quem
  // publicou. O ator recebe os dois numeros prontos.
  LPriority := 0;
  if AMessage.Properties.Has(bpPriority) then
    LPriority := AMessage.Properties.Priority;
  LTtlMs := -1;
  if AMessage.Properties.Has(bpExpiration) then
    // Malformada nao chega aqui (a FSM recusa com 406 antes de rotear); o
    // False so' aconteceria se este sink fosse chamado por outro caminho, e
    // ai' "sem TTL" e' o comportamento seguro.
    if not AmqpParseExpirationMs(AMessage.Properties.Expiration, LTtlMs) then
      LTtlMs := -1;

  LMsg := TAMQPMessage.FromServerMessage(AMessage);
  try
    for I := 0 to High(LDestinos) do
    begin
      LQueue := FindQueue(AVHost, LDestinos[I]);
      if LQueue = nil then
        Continue;
      // D15: so' a fila declarada com reject-publish paga o enqueue SINCRONO.
      // As outras seguem pelo caminho assincrono de sempre.
      if LQueue.Policy.Overflow = amqovRejectPublish then
      begin
        if not LQueue.TryPostMessage(LMsg, LPriority, LTtlMs) then
          ARejeitada := True;
      end
      else
        LQueue.PostMessage(LMsg, LPriority, LTtlMs); // faz o proprio AddRef
    end;
  finally
    LMsg.Release; // a referencia do publicador
  end;
end;

procedure TAMQPEngine.RepublishTo(const AVHost, AExchange,
  ARoutingKey: string; AMessage: TAMQPMessage; APriority: Byte);
var
  LVHost: TAMQPVHost;
  LDestinos: TArray<string>;
  LQueue: TAMQPServerQueue;
  LEd: TAMQPHeaderEditor;
  I: Integer;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  // Um DLX do tipo headers precisa da tabela decodificada para casar. Decodar
  // aqui e' CPU na thread do ator, o que a D17 permite -- e' o caminho FRIO
  // (uma mensagem morre uma vez), nao o do publish.
  LEd := nil;
  try
    try
      LEd := TAMQPHeaderEditor.Create(AMessage.HeaderPayload);
      LDestinos := LVHost.Route(AExchange, ARoutingKey, LEd.Headers);
    except
      // Header ilegivel nao pode derrubar o ator da fila de origem.
      on E: Exception do
        Exit;
    end;
  finally
    LEd.Free;
  end;

  if Length(LDestinos) = 0 then
  begin
    // DLX sem rota: a mensagem morre de vez. E' o mesmo destino que teria sem
    // DLX nenhum, e por isso nao e' erro -- so' nao ha para onde mandar.
    AmqpAtomicInc(FUnrouted);
    Exit;
  end;
  AmqpAtomicInc(FRouted);

  for I := 0 to High(LDestinos) do
  begin
    LQueue := FindQueue(AVHost, LDestinos[I]);
    if LQueue <> nil then
      // TTL da mensagem vai como -1: o 'expiration' original foi retirado pelo
      // dead-lettering (senao ela morreria de novo no ato na DLQ), e o prazo
      // que passa a valer e' o da fila de DESTINO.
      LQueue.PostMessage(AMessage, APriority, -1);
  end;
end;

procedure TAMQPEngine.ExpireIdleQueues;
var
  LPares: TArray<TPair<string, TAMQPServerQueue>>;
  LNomes: TArray<string>;
  LVHosts: TArray<string>;
  I, N, LBarra: Integer;
  LStats: TAMQPQueueStats;
  LMsgs: Integer;
  LChave: string;
begin
  // Snapshot sob o lock, decisao FORA dele: ler Stats e' comando SINCRONO no
  // ator, e segurar o lock da engine enquanto se espera um ator seria
  // convidar um travamento (o ator pode estar republicando um dead-letter,
  // que volta a pedir FindQueue).
  FLock.Enter;
  try
    LPares := FQueues.ToArray;
  finally
    FLock.Leave;
  end;

  SetLength(LNomes, 0);
  SetLength(LVHosts, 0);
  for I := 0 to High(LPares) do
  begin
    // So' fila que PEDIU x-expires paga o comando sincrono.
    if LPares[I].Value.Policy.ExpiresMs < 0 then
      Continue;
    try
      LStats := LPares[I].Value.Stats;
    except
      // Fila parando debaixo da varredura: nao e' erro, e' corrida normal.
      Continue;
    end;
    if LStats.IdleMs < LPares[I].Value.Policy.ExpiresMs then
      Continue;

    // A chave e' 'vhost/nome'; o nome pode conter barra, o vhost nao comeca
    // com uma, entao a PRIMEIRA barra e' o separador.
    LChave := LPares[I].Key;
    LBarra := Pos('/', LChave);
    if LBarra <= 0 then
      Continue;
    N := Length(LNomes);
    SetLength(LNomes, N + 1);
    SetLength(LVHosts, N + 1);
    LVHosts[N] := Copy(LChave, 1, LBarra - 1);
    LNomes[N] := Copy(LChave, LBarra + 1, MaxInt);
  end;

  for I := 0 to High(LNomes) do
    try
      // AOwnerId=0: chamador interno, passa pelo CheckExclusive. Sem
      // if-unused/if-empty: x-expires apaga mesmo com mensagem dentro, que e'
      // o comportamento do RabbitMQ -- o argumento fala de USO, nao de vazio.
      DeleteQueue(LVHosts[I], LNomes[I], False, False, 0, LMsgs);
    except
      // Outra thread pode ter apagado a fila entre a decisao e a execucao.
      ;
    end;
end;

procedure TAMQPEngine.DeliverTick;
var
  LTodas: TArray<TAMQPServerQueue>;
  I: Integer;
begin
  FLock.Enter;
  try
    LTodas := FQueues.Values.ToArray;
  finally
    FLock.Leave;
  end;
  for I := 0 to High(LTodas) do
    LTodas[I].PostDeliverTick;
end;

function TAMQPEngine.RoutedCount: Integer;
begin
  Result := AmqpAtomicGet(FRouted);
end;

function TAMQPEngine.UnroutedCount: Integer;
begin
  Result := AmqpAtomicGet(FUnrouted);
end;

end.
