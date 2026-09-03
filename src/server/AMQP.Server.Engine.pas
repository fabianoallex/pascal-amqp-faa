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

    /// A fila viva de AName, ou nil. Usada pelo canal para encaminhar
    /// ack/nack/reject e para o teardown -- o chamador NAO e' dono.
    function FindQueue(const AVHost, AName: string): TAMQPServerQueue;

    // --- publicacao (IAMQPMessageSink) ---
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage): Boolean;

    /// Cutuca todas as filas (caso degradado da D3 -- ver
    /// TAMQPServerQueue.PostDeliverTick). Chamado pela thread monitora.
    procedure DeliverTick;

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
    amqtrNaoEncontrado: Result := amqerNaoEncontrado;
  else
    Result := amqerNaoEncontrado;
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
      LQueue := TAMQPServerQueue.Create(AName, FMaxQueueLength, FPool);
      FQueues.Add(Key(AVHost, AName), LQueue);
    end;
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
      Exit(amqerNaoEncontrado);

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

{ --- publicacao --- }

function TAMQPEngine.RouteMessage(const AVHost: string;
  const AMessage: TAMQPServerMessage): Boolean;
var
  LVHost: TAMQPVHost;
  LDestinos: TArray<string>;
  LMsg: TAMQPMessage;
  LQueue: TAMQPServerQueue;
  I: Integer;
  LHeaders: TAMQPFieldTable;
begin
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
  LMsg := TAMQPMessage.FromServerMessage(AMessage);
  try
    for I := 0 to High(LDestinos) do
    begin
      LQueue := FindQueue(AVHost, LDestinos[I]);
      if LQueue <> nil then
        LQueue.PostMessage(LMsg); // faz o proprio AddRef
    end;
  finally
    LMsg.Release; // a referencia do publicador
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
