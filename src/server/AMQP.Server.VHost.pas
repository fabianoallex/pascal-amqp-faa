unit AMQP.Server.VHost;

{$I amqp.inc}

{ Per-virtual-host topology -- broker sub-module, Phase 2 WS2 (in-memory
  routing engine, see CLAUDE.md).

  TAMQPVHost owns the exchanges, the queues (for now only WS1's
  TAMQPQueueDef -- the REAL queue object, with actor and consumers, arrives in
  WS3) and the bindings of one virtual host. Concurrency: TMultiReadExclusiveWri-
  teSynchronizer (SysUtils unit, re-exported by Classes -- tested on both
  compilers), reads under BeginRead/EndRead, every mutation under
  BeginWrite/EndWrite -- it is the "almost-immutable lookup under an RWLock"
  that CLAUDE.md locks in for exchanges/queues.

  Protocol errors are NOT RAISED here: each operation returns a
  TAMQPTopologyResult, and the one that knows how to turn it into a reply-code
  (404/406/...) is WS5 -- this unit knows neither channel numbers nor the
  connection error model (EAMQPChannelError/EAMQPConnectionError live in
  AMQP.Server.Types, which this unit does not even import).

  Ownership of arguments: every Declare*/Bind*/Unbind* operation that receives
  a TAMQPFieldTable of arguments TAKES OWNERSHIP of it unconditionally, even
  when the result is not amqtrOk (e.g. divergent redeclare, invalid type)
  -- the caller never has to decide whether to free it depending on the
  result. Same ownership spirit as WS1 (TAMQPExchangeDef/TAMQPQueueDef).

  IMPORTANT -- do not confuse with TAMQPVirtualHostRegistry (AMQP.Server.
  Types): that one is only the SET OF NAMES the FSM handshake accepts on
  Connection.Open (it exists since Phase 1, WS4). This one is the TOPOLOGY
  (exchanges/queues/bindings) of EACH vhost. Unifying the two -- a registry
  vhost automatically getting a topology TAMQPVHost -- is the job of WS5
  (real dispatch in the FSM), which is the consumer of TAMQPVHostSet. This
  unit deliberately does NOT import AMQP.Server.Types nor touch the registry. }

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
  /// Maximum exchange->exchange chaining depth (decision D5, CLAUDE.md:
  /// "exchange->exchange binding supported... with a per-publish visited set
  /// and a maximum depth: the graph may have cycles"). The visited set
  /// already cuts cycles (never re-enters the same exchange in the same
  /// publish); this constant is an extra cap so LEGITIMATE (acyclic) chains
  /// do not grow without bound -- 16 hops is generous for any reasonable
  /// topology. When exceeded, that path stops descending SILENTLY (no
  /// exception raised): a publish must not fail because of an internal
  /// safety cap.
  AMQP_MAX_EXCHANGE_HOP_DEPTH = 16;

type
  { Result of a topology operation. Mapping it to an AMQP reply-code
    (403/404/405/406/...) is WS5's job -- this unit only reports WHAT happened. }
  TAMQPTopologyResult = (
    amqtrOk,           // operation completed (created/removed/bound/etc.)
    amqtrEquivalent,  // redeclare of the SAME name with EQUAL flags/arguments -- idempotent, nothing changed
    amqtrNotFound,// referenced exchange/queue does not exist (delete/bind/unbind)
    amqtrPreconditionFailed,   // redeclare of the SAME name with DIFFERENT flags/arguments -- 406 in WS5
    amqtrCommandInvalid, // unknown exchange type in the declare, OR invalid 'x-match' in the bind of a headers exchange -- 406 in WS5
    amqtrInUse         // delete with if-unused=True and the resource has bindings -- 405 in WS5
  );

  { A binding links a SOURCE exchange (Source) to a destination, which is a
    queue (amqbkQueue) or another exchange (amqbkExchange, decision D5). Owns
    Arguments (same spirit as WS1: the declarer is the owner, without cloning
    TValue-in-TValue). }
  TAMQPBindingKind = (amqbkQueue, amqbkExchange);

  TAMQPBinding = class
  private
    FKind: TAMQPBindingKind;
    FSource: string;
    FDestination: string;
    FRoutingKey: string;
    FArguments: TAMQPFieldTable;
  public
    /// AArguments: this object becomes its owner (frees it in the
    /// destructor); may be nil.
    constructor Create(AKind: TAMQPBindingKind;
      const ASource, ADestination, ARoutingKey: string;
      AArguments: TAMQPFieldTable);
    destructor Destroy; override;

    property Kind: TAMQPBindingKind read FKind;
    /// Exchange the message leaves from (spec: the exchange of Queue.Bind, or
    /// the 'source' of Exchange.Bind).
    property Source: string read FSource;
    /// Destination queue (Kind=amqbkQueue) or exchange (Kind=amqbkExchange,
    /// spec: the 'destination' of Exchange.Bind).
    property Destination: string read FDestination;
    property RoutingKey: string read FRoutingKey;
    property Arguments: TAMQPFieldTable read FArguments;
  end;

  { Topology (exchanges + queues + bindings) of ONE virtual host. }
  TAMQPVHost = class
  private
    FLock: TMultiReadExclusiveWriteSynchronizer;
    FExchanges: TDictionary<string, TAMQPExchangeDef>;
    FQueues: TDictionary<string, TAMQPQueueDef>;
    // A single list (not indexed by source, on purpose -- WS2 values
    // simplicity and correctness over performance; the number of bindings per
    // vhost is small next to the volume of routed messages, which is what
    // would really need an index). Route filters by Source while walking it.
    FBindings: TObjectList<TAMQPBinding>;
    procedure SeedExchange(const AName, AExchangeType: string);
    procedure SeedPredefinedExchanges;
    // Returns the already existing EXACT binding (same Kind/Source/Destination/
    // RoutingKey/Arguments), or nil. Used for idempotent Bind and to locate
    // the target of an Unbind.
    function FindBinding(AKind: TAMQPBindingKind;
      const ASource, ADestination, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPBinding;
    procedure RouteInto(const AExchange, ARoutingKey: string;
      AHeaders: TAMQPFieldTable; ADepth: Integer;
      AVisited, AResultSet: TDictionary<string, Boolean>);
  public
    constructor Create;
    destructor Destroy; override;

    /// Declares an exchange. The predefined ones ('', 'amq.*') exist since the
    /// constructor -- redeclaring them with the SAME flags/type returns
    /// amqtrEquivalent (blocking the reserved 'amq.' name when it comes from
    /// the CLIENT is WS5's responsibility, via AmqpIsReservedName; this unit
    /// does not tell "declared by the broker" from "declared by the client").
    function DeclareExchange(const AName, AExchangeType: string;
      ADurable, AAutoDelete, AInternal: Boolean;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    /// AIfUnused=True: refuses (amqtrInUse) if the exchange is the SOURCE of
    /// any binding. Also removes any binding that points to AName as
    /// DESTINATION (exchange->exchange), so no dangling reference is left.
    function DeleteExchange(const AName: string;
      AIfUnused: Boolean): TAMQPTopologyResult;

    /// Declares a queue. Every new queue is auto-bound to the default exchange
    /// ('') with routing key = its own name (WS2's decision, mirrors
    /// RabbitMQ's behaviour) -- it is what makes publishing straight to the
    /// queue work without any explicit bind.
    function DeclareQueue(const AName: string;
      ADurable, AExclusive, AAutoDelete: Boolean;
      AArguments: TAMQPFieldTable;
      AOwnerId: NativeUInt = 0): TAMQPTopologyResult;
    /// Removes the queue and ALL the bindings that point to it (the auto-bind
    /// on the default exchange included).
    function DeleteQueue(const AName: string): TAMQPTopologyResult;

    function BindQueue(const AExchange, AQueue, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    function UnbindQueue(const AExchange, AQueue, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    /// ADestination/ASource in the same order as the spec's Exchange.Bind:
    /// messages that match on ASource (the source exchange) are forwarded to
    /// ADestination.
    function BindExchange(const ADestination, ASource, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;
    function UnbindExchange(const ADestination, ASource, ARoutingKey: string;
      AArguments: TAMQPFieldTable): TAMQPTopologyResult;

    function ExchangeExists(const AName: string): Boolean;
    /// Names of all declared exchanges (snapshot).
    function ExchangeNames: TArray<string>;
    /// True if ANY binding leaves from AName (destination queue or exchange).
    /// It is what decides whether an auto-delete exchange lost its last binding.
    function HasBindingsFrom(const AName: string): Boolean;
    /// Names of AOwnerId's exclusive queues (WS7: they go away with the owning
    /// connection). Snapshot -- the caller deletes them one by one afterwards.
    function ExclusiveQueuesOf(AOwnerId: NativeUInt): TArray<string>;
    function QueueExists(const AName: string): Boolean;
    /// Returns the descriptor (a direct reference, NOT a copy -- the caller
    /// is NOT the owner and must not keep it beyond immediate use: a
    /// concurrent Delete frees the object). nil if it does not exist.
    function ExchangeDef(const AName: string): TAMQPExchangeDef;
    function QueueDef(const AName: string): TAMQPQueueDef;

    /// Routes a publish on AExchange with ARoutingKey/AHeaders (AHeaders only
    /// matters for a headers-type exchange -- may be nil). Returns the names
    /// of the queues reached, DEDUPLICATED (a queue matched by N different
    /// bindings appears only once) -- an empty array if there is no route
    /// (non-existent exchange included: an unroutable publish, not an error).
    /// Walks exchanges->exchanges (D5) with a visited set (cuts cycles) and
    /// AMQP_MAX_EXCHANGE_HOP_DEPTH (cuts chains that are too long). Read-only
    /// -- BeginRead/EndRead.
    function Route(const AExchange, ARoutingKey: string;
      AHeaders: TAMQPFieldTable): TArray<string>;
  end;

  { All of a broker's TAMQPVHost, indexed by name. Thread-safe (several
    connections handshake/declare resources in parallel). '/' exists since
    the constructor -- the same default root as TAMQPVirtualHostRegistry. }
  TAMQPVHostSet = class
  private
    FLock: TCriticalSection;
    // Plain TDictionary with the vhosts freed by hand in the destructor. A
    // TObjectDictionary with doOwnsValues would also do (it DOES exist in FPC
    // 3.2.2, measured) -- the explicit freeing is just consistency with the
    // rest of the codebase, which always shows who owns what.
    FHosts: TDictionary<string, TAMQPVHost>;
  public
    constructor Create;
    destructor Destroy; override;

    /// Returns AName's TAMQPVHost, creating a new one (empty, with only the
    /// predefined exchanges) if it does not exist yet.
    function GetOrCreate(const AName: string): TAMQPVHost;
    /// Returns AName's TAMQPVHost, or nil if it does not exist (does not create).
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
  FBindings := TObjectList<TAMQPBinding>.Create(True); // owns the bindings
  SeedPredefinedExchanges;
end;

destructor TAMQPVHost.Destroy;
var
  LExDef: TAMQPExchangeDef;
  LQDef: TAMQPQueueDef;
begin
  FBindings.Free; // frees the TAMQPBinding objects (OwnsObjects=True)
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
  SeedExchange('', AMQP_EXCHANGE_TYPE_DIRECT); // default exchange
  SeedExchange('amq.direct', AMQP_EXCHANGE_TYPE_DIRECT);
  SeedExchange('amq.fanout', AMQP_EXCHANGE_TYPE_FANOUT);
  SeedExchange('amq.topic', AMQP_EXCHANGE_TYPE_TOPIC);
  SeedExchange('amq.headers', AMQP_EXCHANGE_TYPE_HEADERS);
  SeedExchange('amq.match', AMQP_EXCHANGE_TYPE_HEADERS); // historical alias of headers (RabbitMQ)
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
    AArguments.Free; // this unit always owns what it receives, even when refusing
    Exit(amqtrCommandInvalid);
  end;

  FLock.BeginWrite;
  try
    if FExchanges.TryGetValue(AName, LExisting) then
    begin
      // Temporary descriptor ONLY for comparing -- it never replaces the
      // existing one nor enters the dictionary, whether equivalent or divergent.
      LCandidate := TAMQPExchangeDef.Create(AName, AExchangeType, ADurable,
        AAutoDelete, AInternal, AArguments);
      try
        if LExisting.EquivalentTo(LCandidate) then
          Result := amqtrEquivalent
        else
          Result := amqtrPreconditionFailed;
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
      Exit(amqtrNotFound);

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
        Exit(amqtrInUse);
    end;

    // Removes bindings where AName is the source, and where it is the
    // destination of an exchange->exchange one (otherwise a dangling reference
    // is left pointing to an exchange that no longer exists).
    I := 0;
    while I < FBindings.Count do
    begin
      if (FBindings[I].Source = AName) or
         ((FBindings[I].Kind = amqbkExchange) and
          (FBindings[I].Destination = AName)) then
        FBindings.Delete(I) // OwnsObjects=True: frees the TAMQPBinding
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
          Result := amqtrEquivalent
        else
          Result := amqtrPreconditionFailed;
      finally
        LCandidate.Free;
      end;
      Exit;
    end;

    FQueues.Add(AName, TAMQPQueueDef.Create(AName, ADurable, AExclusive,
      AAutoDelete, AArguments, AOwnerId));
    // Auto-binds to the default exchange with routing key = queue name.
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
      Exit(amqtrNotFound);

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
      Exit(amqtrNotFound);
    end;
    if not FQueues.ContainsKey(AQueue) then
    begin
      AArguments.Free;
      Exit(amqtrNotFound);
    end;

    if (LExDef.ExchangeType = AMQP_EXCHANGE_TYPE_HEADERS) and
       (not AmqpParseHeadersMatch(AArguments, LMode)) then
    begin
      AArguments.Free;
      Exit(amqtrCommandInvalid); // invalid 'x-match' -- 406 in WS5
    end;

    if FindBinding(amqbkQueue, AExchange, AQueue, ARoutingKey,
       AArguments) <> nil then
    begin
      AArguments.Free; // identical binding already exists -- idempotent bind
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
    AArguments.Free; // it was only for comparing -- this unit always frees it
    if LBind = nil then
      Exit(amqtrNotFound);
    FBindings.Remove(LBind); // OwnsObjects=True: frees the TAMQPBinding
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
      Exit(amqtrNotFound);
    end;
    if not FExchanges.ContainsKey(ADestination) then
    begin
      AArguments.Free;
      Exit(amqtrNotFound);
    end;

    if (LSrcDef.ExchangeType = AMQP_EXCHANGE_TYPE_HEADERS) and
       (not AmqpParseHeadersMatch(AArguments, LMode)) then
    begin
      AArguments.Free;
      Exit(amqtrCommandInvalid);
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
      Exit(amqtrNotFound);
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
  LNames: TList<string>;
begin
  LNames := TList<string>.Create;
  try
    FLock.BeginRead;
    try
      // Do NOT use FExchanges.Values here -- see the comment in
      // ExclusiveQueuesOf: it is a write disguised as a read.
      for LPar in FExchanges do
        LNames.Add(LPar.Value.Name);
    finally
      FLock.EndRead;
    end;
    Result := LNames.ToArray;
  finally
    LNames.Free;
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
  LPair: TPair<string, TAMQPQueueDef>;
  LNames: TList<string>;
begin
  LNames := TList<string>.Create;
  try
    FLock.BeginRead;
    try
      // WARNING: `FQueues.Values` must NOT be used under BeginRead. The
      // TDictionary creates the values collection ON DEMAND and keeps it in
      // an internal field (`if not Assigned(FValues) then FValues := ...`),
      // that is: it is a WRITE disguised as a read. Since BeginRead admits
      // several readers at once, two connections going down together created
      // two collections and one overwrote the other -- leaking 16 bytes in
      // ~10% of the runs. Found with heaptrc; see CLAUDE.md. Iterating the
      // dictionary directly creates a NEW enumerator on each call (no shared
      // field) and is safe among readers.
      for LPair in FQueues do
        if LPair.Value.Exclusive and (LPair.Value.OwnerId = AOwnerId) then
          LNames.Add(LPair.Value.Name);
    finally
      FLock.EndRead;
    end;
    Result := LNames.ToArray;
  finally
    LNames.Free;
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
  LAnyMatched: Boolean;
begin
  if ADepth > AMQP_MAX_EXCHANGE_HOP_DEPTH then
    Exit; // chain too long -- stops silently, without raising

  if AVisited.ContainsKey(AExchange) then
    Exit; // already went through this exchange in this publish -- cuts the cycle
  AVisited.Add(AExchange, True);

  if not FExchanges.TryGetValue(AExchange, LExDef) then
    Exit; // exchange does not exist (e.g. deleted between the bind and this publish)

  LAnyMatched := False;
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
        LMatches := False; // defensive: a bind with an invalid x-match should not have been accepted
    end
    else // direct, including the default exchange ''
      LMatches := AmqpDirectMatches(LBind.RoutingKey, ARoutingKey);

    if not LMatches then
      Continue;
    // The alternate-exchange fires when NO BINDING OF THIS EXCHANGE matched
    // -- not when the final result came out empty. A binding that matched and
    // led to an exchange with no route HAS ALREADY routed: the one failing to
    // route is that exchange, and its AE (if it has one) is what answers for
    // it. Marking here, and not after looking at AResultSet, is what keeps the
    // two things apart.
    LAnyMatched := True;

    if LBind.Kind = amqbkQueue then
    begin
      if not AResultSet.ContainsKey(LBind.Destination) then
        AResultSet.Add(LBind.Destination, True);
    end
    else // amqbkExchange (D5): descends recursively
      RouteInto(LBind.Destination, ARoutingKey, AHeaders, ADepth + 1,
        AVisited, AResultSet);
  end;

  // Phase 3 WS7: nothing matched here -- if this exchange has an
  // alternate-exchange, the message descends through it. The visited set and
  // the depth cap are the SAME as on the exchange->exchange path, so an AE
  // that points to itself, or an X->AE->X cycle, stops on its own. A
  // non-existent AE also stops on its own, at the TryGetValue up above -- it
  // is what makes declaring an AE before that exchange exists not an error.
  if (not LAnyMatched) and LExDef.HasAlternateExchange then
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
