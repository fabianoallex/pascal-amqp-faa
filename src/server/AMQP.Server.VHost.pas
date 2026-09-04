unit AMQP.Server.VHost;

{$I amqp.inc}

{ Topologia por virtual host -- sub-modulo broker, WS2 da Fase 2 (engine de
  roteamento em memoria, ver CLAUDE.md).

  TAMQPVHost e' a dona dos exchanges, das filas (por ora so' os TAMQPQueueDef
  do WS1 -- o objeto de fila DE VERDADE, com ator e consumidores, chega na
  WS3) e dos bindings de um virtual host. Concorrencia: TMultiReadExclusiveWri-
  teSynchronizer (unit SysUtils, reexportada por Classes -- testado nos dois
  compiladores), leitura sob BeginRead/EndRead, toda mutacao sob
  BeginWrite/EndWrite -- e' o "lookup quase-imutavel sob RWLock" que o
  CLAUDE.md trava para exchanges/filas.

  Erros de protocolo NAO SAO LEVANTADOS aqui: cada operacao devolve um
  TAMQPTopologyResult, e quem sabe converter isso em reply-code (404/406/...)
  e' a WS5 -- esta unit nao conhece numero de canal nem o modelo de erro da
  conexao (EAMQPChannelError/EAMQPConnectionError vivem em AMQP.Server.Types,
  que esta unit nem importa).

  Posse dos argumentos: toda operacao Declare*/Bind*/Unbind* que recebe um
  TAMQPFieldTable de argumentos TOMA POSSE dele incondicionalmente, mesmo
  quando o resultado nao e' amqtrOk (ex.: redeclare divergente, tipo invalido)
  -- o chamador nunca precisa decidir se libera ou nao dependendo do
  resultado. Mesmo espirito de posse do WS1 (TAMQPExchangeDef/TAMQPQueueDef).

  IMPORTANTE -- nao confundir com TAMQPVirtualHostRegistry (AMQP.Server.
  Types): aquele e' so' o CONJUNTO DE NOMES que o handshake da FSM aceita no
  Connection.Open (existe desde a Fase 1, WS4). Este aqui e' a TOPOLOGIA
  (exchanges/filas/bindings) de CADA vhost. A unificacao dos dois -- um vhost
  do registry ganhar automaticamente um TAMQPVHost de topologia -- e' trabalho
  da WS5 (despacho real na FSM), que e' quem consome TAMQPVHostSet. Esta unit
  deliberadamente NAO importa AMQP.Server.Types nem mexe no registry. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Server.Resources,
  AMQP.Server.Routing;

const
  /// Profundidade maxima de encadeamento exchange->exchange (decisao D5,
  /// CLAUDE.md: "binding exchange->exchange suportado... com conjunto de
  /// visitados por publicacao e profundidade maxima: o grafo pode ter
  /// ciclo"). O conjunto de visitados ja corta ciclo (nunca reentra no mesmo
  /// exchange na mesma publicacao); esta constante e' um teto adicional pra
  /// cadeias LEGITIMAS (sem ciclo) nao crescerem sem limite -- 16 hops e'
  /// generoso pra qualquer topologia razoavel. Ao estourar, aquele caminho
  /// para de descer EM SILENCIO (sem levantar excecao): um publish nao pode
  /// falhar por causa de um teto de seguranca interno.
  AMQP_MAX_EXCHANGE_HOP_DEPTH = 16;

type
  { Resultado de uma operacao de topologia. Quem mapeia para reply-code AMQP
    (403/404/405/406/...) e' a WS5 -- esta unit so' relata O QUE aconteceu. }
  TAMQPTopologyResult = (
    amqtrOk,           // operacao concluida (criado/removido/ligado/etc.)
    amqtrEquivalente,  // redeclare do MESMO nome com flags/argumentos IGUAIS -- idempotente, nao mudou nada
    amqtrNaoEncontrado,// exchange/fila referenciada nao existe (delete/bind/unbind)
    amqtrDivergente,   // redeclare do MESMO nome com flags/argumentos DIFERENTES -- 406 na WS5
    amqtrTipoInvalido, // tipo de exchange desconhecido no declare, OU 'x-match' invalido no bind de um headers exchange -- 406 na WS5
    amqtrEmUso         // delete com if-unused=True e o recurso tem bindings -- 405 na WS5
  );

  { Um binding liga um exchange de ORIGEM (Source) a um destino, que e' uma
    fila (amqbkQueue) ou outro exchange (amqbkExchange, decisao D5). Dono de
    Arguments (mesmo espirito do WS1: quem declara o dono, sem clonar
    TValue-em-TValue). }
  TAMQPBindingKind = (amqbkQueue, amqbkExchange);

  TAMQPBinding = class
  private
    FKind: TAMQPBindingKind;
    FSource: string;
    FDestination: string;
    FRoutingKey: string;
    FArguments: TAMQPFieldTable;
  public
    /// AArguments: este objeto passa a ser dono (libera no destrutor); pode
    /// ser nil.
    constructor Create(AKind: TAMQPBindingKind;
      const ASource, ADestination, ARoutingKey: string;
      AArguments: TAMQPFieldTable);
    destructor Destroy; override;

    property Kind: TAMQPBindingKind read FKind;
    /// Exchange de onde a mensagem parte (spec: exchange do Queue.Bind, ou o
    /// 'source' do Exchange.Bind).
    property Source: string read FSource;
    /// Fila (Kind=amqbkQueue) ou exchange (Kind=amqbkExchange, spec:
    /// 'destination' do Exchange.Bind) de destino.
    property Destination: string read FDestination;
    property RoutingKey: string read FRoutingKey;
    property Arguments: TAMQPFieldTable read FArguments;
  end;

  { Topologia (exchanges + filas + bindings) de UM virtual host. }
  TAMQPVHost = class
  private
    FLock: TMultiReadExclusiveWriteSynchronizer;
    FExchanges: TDictionary<string, TAMQPExchangeDef>;
    FQueues: TDictionary<string, TAMQPQueueDef>;
    // Lista unica (nao indexada por origem, de proposito -- WS2 preza
    // simplicidade e correcao sobre performance; o volume de bindings por
    // vhost e' pequeno perto do volume de mensagens roteadas, que e' o que
    // realmente precisaria de indice). Route filtra por Source ao percorrer.
    FBindings: TObjectList<TAMQPBinding>;
    procedure SeedExchange(const AName, AExchangeType: string);
    procedure SeedPredefinedExchanges;
    // Devolve o binding EXATO (mesmo Kind/Source/Destination/RoutingKey/
    // Arguments) ja existente, ou nil. Usado para Bind idempotente e para
    // localizar o alvo de um Unbind.
    function FindBinding(AKind: TAMQPBindingKind;
      const ASource, ADestination, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPBinding;
    procedure RouteInto(const AExchange, ARoutingKey: string;
      AHeaders: TAMQPFieldTable; ADepth: Integer;
      AVisited, AResultSet: TDictionary<string, Boolean>);
  public
    constructor Create;
    destructor Destroy; override;

    /// Declara um exchange. Predefinidos ('', 'amq.*') ja existem desde o
    /// construtor -- redeclara-los com os MESMOS flags/tipo devolve
    /// amqtrEquivalente (o bloqueio de nome reservado 'amq.' vindo do
    /// CLIENTE e' responsabilidade da WS5, via AmqpIsReservedName; esta unit
    /// nao distingue "declarado pelo broker" de "declarado pelo cliente").
    function DeclareExchange(const AName, AExchangeType: string;
      ADurable, AAutoDelete, AInternal: Boolean;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    /// AIfUnused=True: recusa (amqtrEmUso) se o exchange for ORIGEM de algum
    /// binding. Remove tambem qualquer binding que aponte para AName como
    /// DESTINO (exchange->exchange), pra nao sobrar referencia pendurada.
    function DeleteExchange(const AName: string;
      AIfUnused: Boolean): TAMQPTopologyResult;

    /// Declara uma fila. Toda fila nova e' auto-ligada ao exchange default
    /// ('') com routing key = proprio nome (decisao do WS2, espelha o
    /// comportamento do RabbitMQ) -- e' o que faz o publish direto na fila
    /// funcionar sem bind explicito nenhum.
    function DeclareQueue(const AName: string;
      ADurable, AExclusive, AAutoDelete: Boolean;
      AArguments: TAMQPFieldTable;
      AOwnerId: NativeUInt = 0): TAMQPTopologyResult;
    /// Remove a fila e TODOS os bindings que apontam pra ela (o auto-bind no
    /// exchange default incluso).
    function DeleteQueue(const AName: string): TAMQPTopologyResult;

    function BindQueue(const AExchange, AQueue, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    function UnbindQueue(const AExchange, AQueue, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    /// ADestination/ASource na mesma ordem do Exchange.Bind da spec: mensagens
    /// que casam em ASource (o exchange de origem) sao encaminhadas pra
    /// ADestination.
    function BindExchange(const ADestination, ASource, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    function UnbindExchange(const ADestination, ASource, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;

    function ExchangeExists(const AName: string): Boolean;
    /// Nomes de todos os exchanges declarados (snapshot).
    function ExchangeNames: TArray<string>;
    /// True se ALGUM binding parte de AName (fila ou exchange de destino).
    /// E' o que decide se um exchange auto-delete ja perdeu o ultimo binding.
    function HasBindingsFrom(const AName: string): Boolean;
    /// Nomes das filas exclusivas de AOwnerId (WS7: elas somem com a conexao
    /// dona). Snapshot -- o chamador apaga uma a uma depois.
    function ExclusiveQueuesOf(AOwnerId: NativeUInt): TArray<string>;
    function QueueExists(const AName: string): Boolean;
    /// Devolve o descritor (referencia direta, NAO uma copia -- o chamador
    /// NAO e' dono e nao deve reter alem do uso imediato: um Delete
    /// concorrente libera o objeto). nil se nao existir.
    function ExchangeDef(const AName: string): TAMQPExchangeDef;
    function QueueDef(const AName: string): TAMQPQueueDef;

    /// Roteia uma publicacao em AExchange com ARoutingKey/AHeaders (AHeaders
    /// so' importa pra um exchange do tipo headers -- pode ser nil). Devolve
    /// os nomes de fila alcancados, DEDUPLICADOS (uma fila casada por N
    /// bindings diferentes aparece uma unica vez) -- array vazio se nao
    /// houver rota (exchange inexistente incluso: publish sem rota, nao
    /// erro). Percorre exchanges->exchanges (D5) com conjunto de visitados
    /// (corta ciclo) e AMQP_MAX_EXCHANGE_HOP_DEPTH (corta cadeia longa
    /// demais). So' faz leitura -- BeginRead/EndRead.
    function Route(const AExchange, ARoutingKey: string;
      AHeaders: TAMQPFieldTable): TArray<string>;
  end;

  { Todos os TAMQPVHost de um broker, indexados por nome. Thread-safe (varias
    conexoes fazem handshake/declaram recursos em paralelo). '/' existe desde
    o construtor -- mesma raiz default de TAMQPVirtualHostRegistry. }
  TAMQPVHostSet = class
  private
    FLock: TCriticalSection;
    // TDictionary comum com liberacao manual dos vhosts no destrutor. Um
    // TObjectDictionary com doOwnsValues tambem serviria (ele EXISTE no FPC
    // 3.2.2, medido) -- a liberacao explicita e' so' consistencia com o resto
    // da codebase, que sempre mostra quem e' dono de que.
    FHosts: TDictionary<string, TAMQPVHost>;
  public
    constructor Create;
    destructor Destroy; override;

    /// Devolve o TAMQPVHost de AName, criando um novo (vazio, so' com os
    /// predefinidos) se ainda nao existir.
    function GetOrCreate(const AName: string): TAMQPVHost;
    /// Devolve o TAMQPVHost de AName, ou nil se nao existir (nao cria).
    function Find(const AName: string): TAMQPVHost;
    function Contains(const AName: string): Boolean;
  end;

implementation

{ TAMQPBinding }

constructor TAMQPBinding.Create(AKind: TAMQPBindingKind;
  const ASource, ADestination, ARoutingKey: string;
  AArguments: TAMQPFieldTable);
begin
  inherited Create;
  FKind := AKind;
  FSource := ASource;
  FDestination := ADestination;
  FRoutingKey := ARoutingKey;
  FArguments := AArguments;
end;

destructor TAMQPBinding.Destroy;
begin
  FArguments.Free;
  inherited;
end;

{ TAMQPVHost }

constructor TAMQPVHost.Create;
begin
  inherited Create;
  FLock := TMultiReadExclusiveWriteSynchronizer.Create;
  FExchanges := TDictionary<string, TAMQPExchangeDef>.Create;
  FQueues := TDictionary<string, TAMQPQueueDef>.Create;
  FBindings := TObjectList<TAMQPBinding>.Create(True); // dono dos bindings
  SeedPredefinedExchanges;
end;

destructor TAMQPVHost.Destroy;
var
  LExDef: TAMQPExchangeDef;
  LQDef: TAMQPQueueDef;
begin
  FBindings.Free; // libera os TAMQPBinding (OwnsObjects=True)
  for LExDef in FExchanges.Values do
    LExDef.Free;
  FExchanges.Free;
  for LQDef in FQueues.Values do
    LQDef.Free;
  FQueues.Free;
  FLock.Free;
  inherited;
end;

procedure TAMQPVHost.SeedExchange(const AName, AExchangeType: string);
begin
  FExchanges.Add(AName,
    TAMQPExchangeDef.Create(AName, AExchangeType, True, False, False, nil));
end;

procedure TAMQPVHost.SeedPredefinedExchanges;
begin
  SeedExchange('', AMQP_EXCHANGE_TYPE_DIRECT); // exchange default
  SeedExchange('amq.direct', AMQP_EXCHANGE_TYPE_DIRECT);
  SeedExchange('amq.fanout', AMQP_EXCHANGE_TYPE_FANOUT);
  SeedExchange('amq.topic', AMQP_EXCHANGE_TYPE_TOPIC);
  SeedExchange('amq.headers', AMQP_EXCHANGE_TYPE_HEADERS);
  SeedExchange('amq.match', AMQP_EXCHANGE_TYPE_HEADERS); // alias historico de headers (RabbitMQ)
end;

function TAMQPVHost.FindBinding(AKind: TAMQPBindingKind;
  const ASource, ADestination, ARoutingKey: string;
  AArguments: TAMQPFieldTable): TAMQPBinding;
var
  LBind: TAMQPBinding;
begin
  Result := nil;
  for LBind in FBindings do
    if (LBind.Kind = AKind) and (LBind.Source = ASource) and
       (LBind.Destination = ADestination) and
       (LBind.RoutingKey = ARoutingKey) and
       AmqpArgsEqual(LBind.Arguments, AArguments) then
      Exit(LBind);
end;

function TAMQPVHost.DeclareExchange(const AName, AExchangeType: string;
  ADurable, AAutoDelete, AInternal: Boolean;
  AArguments: TAMQPFieldTable): TAMQPTopologyResult;
var
  LExisting, LCandidate: TAMQPExchangeDef;
begin
  if not AmqpIsValidExchangeType(AExchangeType) then
  begin
    AArguments.Free; // esta unit sempre e' dona do que recebe, mesmo recusando
    Exit(amqtrTipoInvalido);
  end;

  FLock.BeginWrite;
  try
    if FExchanges.TryGetValue(AName, LExisting) then
    begin
      // Descritor temporario SO' pra comparar -- nunca substitui o existente
      // nem entra no dicionario, seja o resultado equivalente ou divergente.
      LCandidate := TAMQPExchangeDef.Create(AName, AExchangeType, ADurable,
        AAutoDelete, AInternal, AArguments);
      try
        if LExisting.EquivalentTo(LCandidate) then
          Result := amqtrEquivalente
        else
          Result := amqtrDivergente;
      finally
        LCandidate.Free;
      end;
      Exit;
    end;

    FExchanges.Add(AName, TAMQPExchangeDef.Create(AName, AExchangeType,
      ADurable, AAutoDelete, AInternal, AArguments));
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.DeleteExchange(const AName: string;
  AIfUnused: Boolean): TAMQPTopologyResult;
var
  LDef: TAMQPExchangeDef;
  LHasBindings: Boolean;
  I: Integer;
begin
  FLock.BeginWrite;
  try
    if not FExchanges.TryGetValue(AName, LDef) then
      Exit(amqtrNaoEncontrado);

    if AIfUnused then
    begin
      LHasBindings := False;
      for I := 0 to FBindings.Count - 1 do
        if FBindings[I].Source = AName then
        begin
          LHasBindings := True;
          Break;
        end;
      if LHasBindings then
        Exit(amqtrEmUso);
    end;

    // Remove bindings onde AName e' origem, e onde e' destino de um
    // exchange->exchange (senao sobra referencia pendurada apontando pra um
    // exchange que nao existe mais).
    I := 0;
    while I < FBindings.Count do
    begin
      if (FBindings[I].Source = AName) or
         ((FBindings[I].Kind = amqbkExchange) and
          (FBindings[I].Destination = AName)) then
        FBindings.Delete(I) // OwnsObjects=True: libera o TAMQPBinding
      else
        Inc(I);
    end;

    FExchanges.Remove(AName);
    LDef.Free;
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.DeclareQueue(const AName: string;
  ADurable, AExclusive, AAutoDelete: Boolean; AArguments: TAMQPFieldTable;
  AOwnerId: NativeUInt): TAMQPTopologyResult;
var
  LExisting, LCandidate: TAMQPQueueDef;
begin
  FLock.BeginWrite;
  try
    if FQueues.TryGetValue(AName, LExisting) then
    begin
      LCandidate := TAMQPQueueDef.Create(AName, ADurable, AExclusive,
        AAutoDelete, AArguments);
      try
        if LExisting.EquivalentTo(LCandidate) then
          Result := amqtrEquivalente
        else
          Result := amqtrDivergente;
      finally
        LCandidate.Free;
      end;
      Exit;
    end;

    FQueues.Add(AName, TAMQPQueueDef.Create(AName, ADurable, AExclusive,
      AAutoDelete, AArguments, AOwnerId));
    // Auto-liga ao exchange default com routing key = nome da fila.
    FBindings.Add(TAMQPBinding.Create(amqbkQueue, '', AName, AName, nil));
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.DeleteQueue(const AName: string): TAMQPTopologyResult;
var
  LDef: TAMQPQueueDef;
  I: Integer;
begin
  FLock.BeginWrite;
  try
    if not FQueues.TryGetValue(AName, LDef) then
      Exit(amqtrNaoEncontrado);

    I := 0;
    while I < FBindings.Count do
    begin
      if (FBindings[I].Kind = amqbkQueue) and
         (FBindings[I].Destination = AName) then
        FBindings.Delete(I)
      else
        Inc(I);
    end;

    FQueues.Remove(AName);
    LDef.Free;
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.BindQueue(const AExchange, AQueue, ARoutingKey: string;
  AArguments: TAMQPFieldTable): TAMQPTopologyResult;
var
  LExDef: TAMQPExchangeDef;
  LMode: TAMQPHeadersMatch;
begin
  FLock.BeginWrite;
  try
    if not FExchanges.TryGetValue(AExchange, LExDef) then
    begin
      AArguments.Free;
      Exit(amqtrNaoEncontrado);
    end;
    if not FQueues.ContainsKey(AQueue) then
    begin
      AArguments.Free;
      Exit(amqtrNaoEncontrado);
    end;

    if (LExDef.ExchangeType = AMQP_EXCHANGE_TYPE_HEADERS) and
       (not AmqpParseHeadersMatch(AArguments, LMode)) then
    begin
      AArguments.Free;
      Exit(amqtrTipoInvalido); // 'x-match' invalido -- 406 na WS5
    end;

    if FindBinding(amqbkQueue, AExchange, AQueue, ARoutingKey,
       AArguments) <> nil then
    begin
      AArguments.Free; // binding identico ja existe -- bind idempotente
      Exit(amqtrOk);
    end;

    FBindings.Add(TAMQPBinding.Create(amqbkQueue, AExchange, AQueue,
      ARoutingKey, AArguments));
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.UnbindQueue(const AExchange, AQueue, ARoutingKey: string;
  AArguments: TAMQPFieldTable): TAMQPTopologyResult;
var
  LBind: TAMQPBinding;
begin
  FLock.BeginWrite;
  try
    LBind := FindBinding(amqbkQueue, AExchange, AQueue, ARoutingKey,
      AArguments);
    AArguments.Free; // so' serviu pra comparar -- esta unit sempre libera
    if LBind = nil then
      Exit(amqtrNaoEncontrado);
    FBindings.Remove(LBind); // OwnsObjects=True: libera o TAMQPBinding
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.BindExchange(const ADestination, ASource,
  ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPTopologyResult;
var
  LSrcDef: TAMQPExchangeDef;
  LMode: TAMQPHeadersMatch;
begin
  FLock.BeginWrite;
  try
    if not FExchanges.TryGetValue(ASource, LSrcDef) then
    begin
      AArguments.Free;
      Exit(amqtrNaoEncontrado);
    end;
    if not FExchanges.ContainsKey(ADestination) then
    begin
      AArguments.Free;
      Exit(amqtrNaoEncontrado);
    end;

    if (LSrcDef.ExchangeType = AMQP_EXCHANGE_TYPE_HEADERS) and
       (not AmqpParseHeadersMatch(AArguments, LMode)) then
    begin
      AArguments.Free;
      Exit(amqtrTipoInvalido);
    end;

    if FindBinding(amqbkExchange, ASource, ADestination, ARoutingKey,
       AArguments) <> nil then
    begin
      AArguments.Free;
      Exit(amqtrOk);
    end;

    FBindings.Add(TAMQPBinding.Create(amqbkExchange, ASource, ADestination,
      ARoutingKey, AArguments));
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.UnbindExchange(const ADestination, ASource,
  ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPTopologyResult;
var
  LBind: TAMQPBinding;
begin
  FLock.BeginWrite;
  try
    LBind := FindBinding(amqbkExchange, ASource, ADestination, ARoutingKey,
      AArguments);
    AArguments.Free;
    if LBind = nil then
      Exit(amqtrNaoEncontrado);
    FBindings.Remove(LBind);
    Result := amqtrOk;
  finally
    FLock.EndWrite;
  end;
end;

function TAMQPVHost.ExchangeExists(const AName: string): Boolean;
begin
  FLock.BeginRead;
  try
    Result := FExchanges.ContainsKey(AName);
  finally
    FLock.EndRead;
  end;
end;

function TAMQPVHost.ExchangeNames: TArray<string>;
var
  LPar: TPair<string, TAMQPExchangeDef>;
  LNomes: TList<string>;
begin
  LNomes := TList<string>.Create;
  try
    FLock.BeginRead;
    try
      // NAO usar FExchanges.Values aqui -- ver o comentario em
      // ExclusiveQueuesOf: e' uma escrita disfarcada de leitura.
      for LPar in FExchanges do
        LNomes.Add(LPar.Value.Name);
    finally
      FLock.EndRead;
    end;
    Result := LNomes.ToArray;
  finally
    LNomes.Free;
  end;
end;

function TAMQPVHost.HasBindingsFrom(const AName: string): Boolean;
var
  I: Integer;
begin
  Result := False;
  FLock.BeginRead;
  try
    for I := 0 to FBindings.Count - 1 do
      if FBindings[I].Source = AName then
        Exit(True);
  finally
    FLock.EndRead;
  end;
end;

function TAMQPVHost.ExclusiveQueuesOf(AOwnerId: NativeUInt): TArray<string>;
var
  LPar: TPair<string, TAMQPQueueDef>;
  LNomes: TList<string>;
begin
  LNomes := TList<string>.Create;
  try
    FLock.BeginRead;
    try
      // ATENCAO: `FQueues.Values` NAO pode ser usado sob BeginRead. O
      // TDictionary cria a colecao de valores SOB DEMANDA e a guarda num
      // campo interno (`if not Assigned(FValues) then FValues := ...`), ou
      // seja: e' uma ESCRITA disfarcada de leitura. Como o BeginRead admite
      // varios leitores ao mesmo tempo, duas conexoes caindo juntas criavam
      // duas colecoes e uma sobrescrevia a outra -- vazando 16 bytes em ~10%
      // das execucoes. Achado com heaptrc; ver CLAUDE.md. Iterar o
      // dicionario direto cria um enumerador NOVO a cada chamada (nada de
      // campo compartilhado) e e' seguro entre leitores.
      for LPar in FQueues do
        if LPar.Value.Exclusive and (LPar.Value.OwnerId = AOwnerId) then
          LNomes.Add(LPar.Value.Name);
    finally
      FLock.EndRead;
    end;
    Result := LNomes.ToArray;
  finally
    LNomes.Free;
  end;
end;

function TAMQPVHost.QueueExists(const AName: string): Boolean;
begin
  FLock.BeginRead;
  try
    Result := FQueues.ContainsKey(AName);
  finally
    FLock.EndRead;
  end;
end;

function TAMQPVHost.ExchangeDef(const AName: string): TAMQPExchangeDef;
begin
  FLock.BeginRead;
  try
    if not FExchanges.TryGetValue(AName, Result) then
      Result := nil;
  finally
    FLock.EndRead;
  end;
end;

function TAMQPVHost.QueueDef(const AName: string): TAMQPQueueDef;
begin
  FLock.BeginRead;
  try
    if not FQueues.TryGetValue(AName, Result) then
      Result := nil;
  finally
    FLock.EndRead;
  end;
end;

procedure TAMQPVHost.RouteInto(const AExchange, ARoutingKey: string;
  AHeaders: TAMQPFieldTable; ADepth: Integer;
  AVisited, AResultSet: TDictionary<string, Boolean>);
var
  LExDef: TAMQPExchangeDef;
  LBind: TAMQPBinding;
  LMode: TAMQPHeadersMatch;
  LMatches: Boolean;
  LCasouAlgum: Boolean;
begin
  if ADepth > AMQP_MAX_EXCHANGE_HOP_DEPTH then
    Exit; // cadeia longa demais -- para em silencio, sem levantar

  if AVisited.ContainsKey(AExchange) then
    Exit; // ja passamos por este exchange nesta publicacao -- corta o ciclo
  AVisited.Add(AExchange, True);

  if not FExchanges.TryGetValue(AExchange, LExDef) then
    Exit; // exchange nao existe (ex.: deletado entre o bind e este publish)

  LCasouAlgum := False;
  for LBind in FBindings do
  begin
    if LBind.Source <> AExchange then
      Continue;

    if LExDef.ExchangeType = AMQP_EXCHANGE_TYPE_FANOUT then
      LMatches := True
    else if LExDef.ExchangeType = AMQP_EXCHANGE_TYPE_TOPIC then
      LMatches := AmqpTopicMatches(LBind.RoutingKey, ARoutingKey)
    else if LExDef.ExchangeType = AMQP_EXCHANGE_TYPE_HEADERS then
    begin
      if AmqpParseHeadersMatch(LBind.Arguments, LMode) then
        LMatches := AmqpHeadersMatch(LBind.Arguments, AHeaders, LMode)
      else
        LMatches := False; // defensivo: bind com x-match invalido nao deveria ter sido aceito
    end
    else // direct, inclusive o exchange default ''
      LMatches := AmqpDirectMatches(LBind.RoutingKey, ARoutingKey);

    if not LMatches then
      Continue;
    // O alternate-exchange dispara quando NENHUM BINDING DESTE EXCHANGE casou
    // -- nao quando o resultado final ficou vazio. Um binding que casou e
    // levou a um exchange sem rota JA' roteou: quem deixa de rotear e' aquele
    // exchange, e o AE dele (se tiver) e' que responde por isso. Marcar aqui,
    // e nao depois de olhar o AResultSet, e' o que separa as duas coisas.
    LCasouAlgum := True;

    if LBind.Kind = amqbkQueue then
    begin
      if not AResultSet.ContainsKey(LBind.Destination) then
        AResultSet.Add(LBind.Destination, True);
    end
    else // amqbkExchange (D5): desce recursivamente
      RouteInto(LBind.Destination, ARoutingKey, AHeaders, ADepth + 1,
        AVisited, AResultSet);
  end;

  // WS7 da Fase 3: nada casou aqui -- se este exchange tem alternate-exchange,
  // a mensagem desce por ele. O conjunto de visitados e o teto de profundidade
  // sao os MESMOS do caminho exchange->exchange, entao um AE que aponta para si
  // mesmo, ou um ciclo X->AE->X, para sozinho. AE inexistente tambem para
  // sozinho, no TryGetValue la' de cima -- e' o que faz declarar um AE antes de
  // o exchange existir nao ser erro.
  if (not LCasouAlgum) and LExDef.HasAlternateExchange then
    RouteInto(LExDef.AlternateExchange, ARoutingKey, AHeaders, ADepth + 1,
      AVisited, AResultSet);
end;

function TAMQPVHost.Route(const AExchange, ARoutingKey: string;
  AHeaders: TAMQPFieldTable): TArray<string>;
var
  LVisited, LResultSet: TDictionary<string, Boolean>;
  LKey: string;
  I: Integer;
begin
  Result := nil;
  LVisited := TDictionary<string, Boolean>.Create;
  LResultSet := TDictionary<string, Boolean>.Create;
  try
    FLock.BeginRead;
    try
      RouteInto(AExchange, ARoutingKey, AHeaders, 0, LVisited, LResultSet);
    finally
      FLock.EndRead;
    end;

    SetLength(Result, LResultSet.Count);
    I := 0;
    for LKey in LResultSet.Keys do
    begin
      Result[I] := LKey;
      Inc(I);
    end;
  finally
    LVisited.Free;
    LResultSet.Free;
  end;
end;

{ TAMQPVHostSet }

constructor TAMQPVHostSet.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FHosts := TDictionary<string, TAMQPVHost>.Create;
  FHosts.Add('/', TAMQPVHost.Create);
end;

destructor TAMQPVHostSet.Destroy;
var
  LHost: TAMQPVHost;
begin
  for LHost in FHosts.Values do
    LHost.Free;
  FHosts.Free;
  FLock.Free;
  inherited;
end;

function TAMQPVHostSet.GetOrCreate(const AName: string): TAMQPVHost;
begin
  FLock.Enter;
  try
    if not FHosts.TryGetValue(AName, Result) then
    begin
      Result := TAMQPVHost.Create;
      FHosts.Add(AName, Result);
    end;
  finally
    FLock.Leave;
  end;
end;

function TAMQPVHostSet.Find(const AName: string): TAMQPVHost;
begin
  FLock.Enter;
  try
    if not FHosts.TryGetValue(AName, Result) then
      Result := nil;
  finally
    FLock.Leave;
  end;
end;

function TAMQPVHostSet.Contains(const AName: string): Boolean;
begin
  FLock.Enter;
  try
    Result := FHosts.ContainsKey(AName);
  finally
    FLock.Leave;
  end;
end;

end.
