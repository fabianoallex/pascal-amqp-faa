unit AMQP.Server.Engine;

{$I amqp.inc}

{ The engine -- broker sub-module, WS5 of Phase 2.

  Joins the three pieces the earlier workstreams built separately:
   - the per-vhost TOPOLOGY (WS2: exchanges, declared queues, bindings and
     direct/fanout/topic/headers routing, under an RWLock);
   - the LIVE QUEUES (WS3: one actor per queue, with stock, unacked messages
     and consumers);
   - DELIVERY (WS4: the per-channel target).

  The connection FSM talks ONLY to this class. It knows no channel number,
  raises no protocol exception and knows no reply-code at all: it returns
  TAMQPEngineResult, and the translation to 403/404/405/406/540 -- and to
  "channel error" or "connection error" -- is done by the FSM, which is the
  one holding the channel.

  Descriptor x live queue
  -----------------------
  TAMQPVHost (WS2) keeps TAMQPQueueDef -- the DESCRIPTOR (flags, arguments,
  owner). The object with stock and consumers is TAMQPServerQueue (WS3), which
  lives here, indexed by 'vhost/name'. Both are born and die together, and this
  unit is what guarantees it: there is no path that creates one without the
  other.

  Publishing
  ----------
  TAMQPEngine implements IAMQPMessageSink, so Phase 1's content path (channel
  reassembles -> Sink.RouteMessage) stays identical: only what is plugged in
  changes. Binding matching runs on the PUBLISHER's thread, over the channel's
  live headers table ("new invariant" in CLAUDE.md); only the enqueue crosses
  over to the queue actor.

  Interface refcount: _AddRef/_Release are no-ops (-1) on purpose -- the
  engine's owner is TAMQPServer, and the interface reference kept in the
  connections' config must not decide its lifetime. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Exchange.Methods,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Threading,
  AMQP.Server.Types,
  AMQP.Server.Message,
  AMQP.Server.Header,
  AMQP.Server.Resources,
  AMQP.Server.Queue,
  AMQP.Server.VHost,
  AMQP.Server.Events,
  AMQP.Server.Journal,
  AMQP.Server.Records,
  AMQP.Server.Recovery;

type
  { What happened in an engine operation. The FSM (WS5) maps it to a
    reply-code; the engine knows none. }
  TAMQPEngineResult = (
    amqerOk,
    /// referenced resource does not exist -- 404 NOT_FOUND
    amqerNotFound,
    /// redeclare of the same name with different flags/arguments -- 406
    amqerPreconditionFailed,
    /// unknown exchange type, or invalid 'x-match' -- 406
    amqerCommandInvalid,
    /// delete with if-unused and the resource in use -- 406
    amqerInUse,
    /// delete with if-empty and the queue holding messages -- 406
    amqerNotEmpty,
    /// reserved name ('amq.') coming from the client -- 403 ACCESS_REFUSED
    amqerAccessRefused,
    /// exclusive queue of ANOTHER connection -- 405 RESOURCE_LOCKED
    amqerResourceLocked,
    /// The operation touched DURABLE topology and the journal could not
    /// record it (Phase 4, WS3). Becomes a CONNECTION 541, not a channel
    /// error: a broker that can no longer persist is broken as a whole, and
    /// accepting durable declares would be lying about durability.
    amqerNoDurability
  );

  TAMQPEngine = class;

  { Connects a queue to the engine's dead-lettering.

    It exists because IAMQPDeadLetterSink does not carry the vhost -- and it
    should not: the queue does not know which vhost it lives in, and widening
    the interface just to carry a value the ADAPTER can pin would make the
    contract worse for everyone. One adapter per vhost pins the name and the
    interface keeps what matters.

    Lifetime: it is a TInterfacedObject and the queue holds the reference, so
    the adapter lives as long as the queue. The pointer to the engine is raw
    on purpose -- the engine owns the queues and stops them in its own
    destructor, so there is no window in which a live queue sees a dead engine. }
  TAMQPDeadLetterAdapter = class(TInterfacedObject, IAMQPDeadLetterSink)
  private
    FEngine: TAMQPEngine;
    FVHost: string;
  public
    constructor Create(AEngine: TAMQPEngine; const AVHost: string);
    procedure DeadLetter(const ADlx, ARoutingKey: string;
      AMessage: TAMQPMessage; APriority: Byte; AContentId: UInt64);
  end;

  TAMQPEngine = class(TInterfacedObject, IAMQPMessageSink)
  private
    FLock: TCriticalSection;
    FVHosts: TAMQPVHostSet;
    // Live queues, keyed by 'vhost/name'. The matching descriptor lives in
    // TAMQPVHost; both are created and destroyed together.
    FQueues: TDictionary<string, TAMQPServerQueue>;
    FMaxQueueLength: Integer;
    FJournal: TAMQPJournal;
    FEvents: IAMQPEventSink;
    FNextId: UInt64;
    /// True while Recupera is applying the log. While it is True the journal
    /// is MUTED: what is declared and enqueued now CAME from the log, and
    /// rewriting it would make the WAL grow one copy per boot, forever.
    FRecovering: Boolean;
    FPool: TPcThreadPool;
    FRouted: Integer;   // atomic -- publishes with at least one route
    FUnrouted: Integer; // atomic -- publishes with no route
    function Key(const AVHost, AName: string): string;
    // Call UNDER FLock.
    function FindQueueLocked(const AVHost, AName: string): TAMQPServerQueue;
    function CheckExclusive(ADef: TAMQPQueueDef;
      AOwnerId: NativeUInt): Boolean;
  protected
    // IAMQPMessageSink -- refcount turned off (see header).
    // THE IInterface CALLING CONVENTION DEPENDS ON THE PLATFORM on FPC:
    // stdcall on Windows, cdecl on Unix. Declaring a fixed stdcall compiles on
    // Windows and fails on Linux with "No matching implementation for interface
    // method _AddRef:LongInt; CDecl" -- and that is how it was, from WS5 of
    // Phase 2 until WS3 of Phase 4 compiled the broker in the container for the
    // first time. The earlier Linux validations were all of the CLIENT.
    function _AddRef: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
    function _Release: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
  public
    constructor Create;
    destructor Destroy; override;

    /// Ensures the vhost exists in the topology (with the predefined
    /// exchanges). Called when a connection opens on it.
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

    // --- queues ---
    /// Declares (or checks, with APassive) a queue. AOwnerId identifies the
    /// requesting connection -- it is what blocks use of someone else's
    /// exclusive queue. Returns the Declare-Ok counts.
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

    // --- consuming ---
    function AddConsumer(const AVHost, AQueue, AConsumerTag: string;
      ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget;
      AOwnerId: NativeUInt): TAMQPEngineResult;
    function RemoveConsumer(const AVHost, AQueue, AConsumerTag: string;
      AChannelId: NativeUInt): TAMQPEngineResult;
    /// Basic.Get. AFound=False with result amqerOk means an empty queue
    /// (Get-Empty), not an error. The reference returned in AMessage belongs
    /// to the CALLER.
    function GetMessage(const AVHost, AQueue: string; ANoAck: Boolean;
      ADeliveryTag: UInt64; AChannelId: NativeUInt; AOwnerId: NativeUInt;
      out AFound: Boolean; out AMessage: TAMQPMessage;
      out ARedelivered: Boolean; out AMessageCount: Integer): TAMQPEngineResult;

    // --- automatic lifecycle (WS7) ---
    /// Deletes the queue if it is `auto-delete`, HAS EVER HAD a consumer and
    /// has none left. Call AFTER removing a consumer -- and since Stats is a
    /// synchronous command and the queue's mailbox is FIFO, the read here
    /// already sees the removal just posted. No-op if the queue does not
    /// exist or is not auto-delete.
    /// True if this operation has to go to disk: there is a journal (D19) and
    /// the resource is durable (D20). A predefined exchange ('' and 'amq.*')
    /// NEVER goes -- EnsureVHost recreates it on every boot, and writing it
    /// would only fill the log with records recovery would have to ignore.
    function PersistExchange(const AName: string; ADurable: Boolean): Boolean;
    function PersistQueue(ADurable, AExclusive: Boolean): Boolean;
    /// Really durable: the descriptor exists AND is marked durable. Used for
    /// bindings, which are only persisted when BOTH ends are durable -- a
    /// binding is as durable as the more fragile of its sides.
    function IsExchangeDurable(AVHost: TAMQPVHost; const AName: string): Boolean;
    function IsQueueDurable(AVHost: TAMQPVHost; const AName: string): Boolean;
    /// Writes a topology record and WAITS for it to become durable. False =
    /// it did not work, and the caller returns amqerSemDurabilidade.
    function WriteTopology(AKind: Byte; const APayload: TBytes): Boolean;
    /// Next content/placement identifier. Monotonic and unique in the
    /// broker; under the same lock that already guards the live queues.
    /// Written when there was no portable 64-bit atomic increment
    /// (pascal-common-faa has PcAtomicInc64; the lock stayed because the
    /// caller already holds it), and a 32-bit counter would wrap around in a
    /// long-lived broker.
    function NextId: UInt64;
    function WriteEnqueue(AQueue: TAMQPServerQueue; const AVHost: string;
      AMessage: TAMQPMessage; APriority: Byte; AMessageTtlMs: Int64;
      var AContentId: UInt64; AWaitVacancy: Boolean;
      out ALsn: UInt64): UInt64;
    /// True if this live queue will persist anything (D20).
    function DoesQueuePersist(AQueue: TAMQPServerQueue): Boolean;
    procedure MaybeAutoDeleteQueue(const AVHost, AQueue: string);
    /// Deletes the exchange if it is `auto-delete` and is no longer the source
    /// of any binding. Call after every unbind (including the implicit ones,
    /// such as those that vanish together with a deleted queue).
    procedure MaybeAutoDeleteExchange(const AVHost, AExchange: string);
    /// Sweeps the auto-delete exchanges and deletes those left without a
    /// binding. It exists because deleting a QUEUE takes its bindings along,
    /// and there is no way to know from outside which exchanges were orphaned
    /// -- auto-delete exchanges are rare, so the sweep is cheaper than tracking.
    procedure SweepAutoDeleteExchanges(const AVHost: string);
    /// Deletes ALL exclusive queues of AOwnerId (the owning connection
    /// dropped). Returns how many.
    function DeleteExclusiveQueuesOf(const AVHost: string;
      AOwnerId: NativeUInt): Integer;

    /// The live queue for AName, or nil. Used by the channel to forward
    /// ack/nack/reject and for teardown -- the caller is NOT the owner.
    function FindQueue(const AVHost, AName: string): TAMQPServerQueue;

    // --- publishing (IAMQPMessageSink) ---
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage; out ARejected: Boolean;
      out ALsn: UInt64): Boolean;

    /// Republishes an already built message (used by dead-lettering). Runs on
    /// the source queue's ACTOR thread: reads the topology under the RWLock
    /// and posts to the destination queues' mailboxes, with no I/O and no
    /// waiting (D17).
    procedure RepublishTo(const AVHost, AExchange, ARoutingKey: string;
      AMessage: TAMQPMessage; APriority: Byte; AContentId: UInt64);

    /// Nudges all queues (D3's degraded case -- see
    /// TAMQPServerQueue.PostDeliverTick). Called by the monitor thread.
    procedure DeliverTick;

    /// Deletes the queues with x-expires that went past their deadline UNUSED
    /// (WS8). Called by the monitor thread, like DeliverTick.
    ///
    /// This sweep DETECTS; the usual DeleteQueue EXECUTES -- the same path as
    /// a Queue.Delete from the client, with the same care to stop the actor
    /// before freeing. Duplicating the teardown here would be the recipe for
    /// the double-free Phase 1 already paid for once.
    procedure ExpireIdleQueues;

    /// Cap of ready messages per queue (decision D7). 0 = unlimited. Applies
    /// to queues created AFTER it is set.
    /// Durability journal (Phase 4). nil = broker without DataDir, and then
    /// NO topology goes to disk (D19) -- the path stays identical to Phase
    /// 3's. Injected by TAMQPServer in Start; the engine does not own it.
    property Journal: TAMQPJournal read FJournal write FJournal;
    /// Observability sink passed on to EACH new queue (Phase 4.1, Inc. 2).
    /// The engine emits nothing on its own -- the actor is the emitter.
    property Events: IAMQPEventSink read FEvents write FEvents;

    /// Applies to the broker the state the replay read. Runs INSIDE Start,
    /// before the listening socket opens: nobody may talk to a half-recovered
    /// broker.
    ///
    /// With the journal MUTED (see FRecovering): what is declared here came
    /// from the log, and rewriting it would make the WAL grow one copy per boot.
    procedure Recover(AState: TAMQPRecoveredState);

    property MaxQueueLength: Integer read FMaxQueueLength
      write FMaxQueueLength;
    /// Pool where the queue actors are scheduled (nil = PcPool). TAMQPServer
    /// always passes its own pool; nil is left for the standalone engine of
    /// the tests.
    property Pool: TPcThreadPool read FPool write FPool;
    /// Publishes that found at least one queue / that found none.
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
  AMessage: TAMQPMessage; APriority: Byte; AContentId: UInt64);
begin
  FEngine.RepublishTo(FVHost, ADlx, ARoutingKey, AMessage, APriority,
    AContentId);
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
  // Order: stop ALL actors before freeing anything -- a live actor with the
  // topology already freed would be use-after-free.
  for LQ in FQueues.Values do
  begin
    try
      LQ.Stop;
    except
      on E: Exception do
        ; // stuck actor: Stop itself already decided not to free anything
    end;
    LQ.Free;
  end;
  FQueues.Free;
  FVHosts.Free;
  FLock.Free;
  inherited;
end;

function TAMQPEngine._AddRef: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
begin
  Result := -1; // no refcount: the owner is TAMQPServer
end;

function TAMQPEngine._Release: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
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

// True if AOwnerId may use the queue: either it is not exclusive, or the owner
// is exactly this connection. AOwnerId=0 (internal caller) always passes.
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
  LDef: TAMQPExchangeDef;
  LRec: TAMQPRecExchange;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  if APassive then
  begin
    // Passive only checks existence; it neither declares nor validates flags.
    AArguments.Free;
    if LVHost.ExchangeExists(AName) then
      Result := amqerOk
    else
      Result := amqerNotFound;
    Exit;
  end;

  LRes := LVHost.DeclareExchange(AName, AType, ADurable, AAutoDelete,
    AInternal, AArguments); // the vhost takes ownership of the arguments
  case LRes of
    amqtrOk, amqtrEquivalent: Result := amqerOk;
    amqtrPreconditionFailed: Result := amqerPreconditionFailed;
    amqtrCommandInvalid: Result := amqerCommandInvalid;
  else
    Result := amqerNotFound;
  end;

  // Only a declare that actually CREATED goes to disk: amqtrEquivalente is an
  // idempotent redeclare, and writing it would fill the log with records that
  // recovery would only have the trouble of reapplying over themselves.
  if (LRes = amqtrOk) and PersistExchange(AName, ADurable) then
  begin
    LDef := LVHost.ExchangeDef(AName);
    if LDef <> nil then
    begin
      // The arguments come from the DESCRIPTOR, not from AArguments: ownership
      // has already passed to the vhost, and the descriptor is the source of truth.
      LRec.VHost := AVHost;
      LRec.Name := LDef.Name;
      LRec.ExchangeType := LDef.ExchangeType;
      LRec.Durable := LDef.Durable;
      LRec.AutoDelete := LDef.AutoDelete;
      LRec.Internal := LDef.Internal;
      LRec.Arguments := LDef.Arguments;
      if not WriteTopology(AMQP_REC_EXCHANGE_DECLARE,
        AmqpEncodeRecExchange(LRec)) then
        Result := amqerNoDurability;
    end;
  end;
end;

function TAMQPEngine.DeleteExchange(const AVHost, AName: string;
  AIfUnused: Boolean): TAMQPEngineResult;
var
  LRes: TAMQPTopologyResult;
  LVHost: TAMQPVHost;
  LWasDurable: Boolean;
  LName: TAMQPRecName;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  // BEFORE deleting: afterwards the descriptor no longer exists and there is no
  // way to know whether it was durable.
  LWasDurable := (FJournal <> nil) and IsExchangeDurable(LVHost, AName);
  LRes := LVHost.DeleteExchange(AName, AIfUnused);
  case LRes of
    amqtrOk: Result := amqerOk;
    amqtrInUse: Result := amqerInUse;
    // Idempotent for the same reason as DeleteQueue above -- see the comment
    // there. Keeping both the same matters: a setup that deletes before
    // redeclaring touches both.
    amqtrNotFound: Result := amqerOk;
  else
    Result := amqerOk;
  end;

  if (LRes = amqtrOk) and LWasDurable then
  begin
    LName.VHost := AVHost;
    LName.Name := AName;
    if not WriteTopology(AMQP_REC_EXCHANGE_DELETE, AmqpEncodeRecName(LName)) then
      Result := amqerNoDurability;
  end;
end;

function TAMQPEngine.BindExchange(const AVHost, ADestination, ASource,
  ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPEngineResult;
var
  LRes: TAMQPTopologyResult;
  LVHost: TAMQPVHost;
  LPersist: Boolean;
  LPayload: TBytes;
  LRec: TAMQPRecBinding;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  // BOTH ends durable, and the encoding done BEFORE the bind: afterwards the
  // vhost owns AArguments.
  LPersist := (FJournal <> nil) and IsExchangeDurable(LVHost, ASource)
    and IsExchangeDurable(LVHost, ADestination);
  if LPersist then
  begin
    LRec.VHost := AVHost;
    LRec.Source := ASource;
    LRec.Destination := ADestination;
    LRec.RoutingKey := ARoutingKey;
    LRec.Arguments := AArguments;
    LPayload := AmqpEncodeRecBinding(LRec);
  end;
  LRes := LVHost.BindExchange(ADestination, ASource,
    ARoutingKey, AArguments);
  case LRes of
    amqtrOk, amqtrEquivalent: Result := amqerOk;
    amqtrCommandInvalid: Result := amqerCommandInvalid;
  else
    Result := amqerNotFound;
  end;
  if (LRes = amqtrOk) and LPersist then
    if not WriteTopology(AMQP_REC_EXCHANGE_BIND, LPayload) then
      Result := amqerNoDurability;
end;

function TAMQPEngine.UnbindExchange(const AVHost, ADestination, ASource,
  ARoutingKey: string; AArguments: TAMQPFieldTable): TAMQPEngineResult;
var
  LRes: TAMQPTopologyResult;
  LVHost: TAMQPVHost;
  LPersist: Boolean;
  LPayload: TBytes;
  LRec: TAMQPRecBinding;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  LPersist := (FJournal <> nil) and IsExchangeDurable(LVHost, ASource)
    and IsExchangeDurable(LVHost, ADestination);
  if LPersist then
  begin
    LRec.VHost := AVHost;
    LRec.Source := ASource;
    LRec.Destination := ADestination;
    LRec.RoutingKey := ARoutingKey;
    LRec.Arguments := AArguments;
    LPayload := AmqpEncodeRecBinding(LRec);
  end;
  LRes := LVHost.UnbindExchange(ADestination, ASource,
    ARoutingKey, AArguments);
  if LRes in [amqtrOk, amqtrEquivalent] then
    Result := amqerOk
  else
    Result := amqerNotFound;
  if (LRes = amqtrOk) and LPersist then
    if not WriteTopology(AMQP_REC_EXCHANGE_UNBIND, LPayload) then
      Result := amqerNoDurability;
end;

function TAMQPEngine.ExchangeExists(const AVHost, AName: string): Boolean;
begin
  Result := FVHosts.GetOrCreate(AVHost).ExchangeExists(AName);
end;

{ --- queues --- }

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
  LRecQ: TAMQPRecQueue;
  LNoDurability: Boolean;
begin
  AMessageCount := 0;
  AConsumerCount := 0;
  LNoDurability := False;
  LVHost := FVHosts.GetOrCreate(AVHost);

  if APassive then
  begin
    AArguments.Free;
    LDef := LVHost.QueueDef(AName);
    if LDef = nil then
      Exit(amqerNotFound);
    if not CheckExclusive(LDef, AOwnerId) then
      Exit(amqerResourceLocked);
    Result := amqerOk;
  end
  else
  begin
    LDef := LVHost.QueueDef(AName);
    if (LDef <> nil) and (not CheckExclusive(LDef, AOwnerId)) then
    begin
      AArguments.Free;
      Exit(amqerResourceLocked);
    end;
    LRes := LVHost.DeclareQueue(AName, ADurable, AExclusive, AAutoDelete,
      AArguments, AOwnerId);
    case LRes of
      amqtrOk, amqtrEquivalent: Result := amqerOk;
      amqtrPreconditionFailed: Exit(amqerPreconditionFailed);
    else
      Exit(amqerNotFound);
    end;

    // Only a declare that actually CREATED goes to disk (an idempotent
    // redeclare produces no record). Note the result does NOT leave via Exit
    // here: the live queue still has to be born below, or the descriptor would
    // have no actor. If the journal failed, the connection drops with 541 --
    // and the divergence between memory and disk until restart is exactly what
    // that 541 communicates.
    if (LRes = amqtrOk) and PersistQueue(ADurable, AExclusive) then
    begin
      LDef := LVHost.QueueDef(AName);
      if LDef <> nil then
      begin
        LRecQ.VHost := AVHost;
        LRecQ.Name := LDef.Name;
        LRecQ.Durable := LDef.Durable;
        LRecQ.AutoDelete := LDef.AutoDelete;
        LRecQ.Arguments := LDef.Arguments;
        if not WriteTopology(AMQP_REC_QUEUE_DECLARE, AmqpEncodeRecQueue(LRecQ)) then
          LNoDurability := True;
      end;
    end;
  end;

  // Live queue: born together with the descriptor, gone together with it.
  FLock.Enter;
  try
    LQueue := FindQueueLocked(AVHost, AName);
    if (LQueue = nil) and (not APassive) then
    begin
      // The live queue is born with the SAME policy as the descriptor
      // (WS2/WS4): the descriptor is already the source of truth for the
      // x-arguments, and re-parsing here would leave room for the two to diverge.
      LDef := LVHost.QueueDef(AName);
      if LDef <> nil then
        LQueue := TAMQPServerQueue.Create(AName, LDef.Policy, FMaxQueueLength,
          FPool)
      else
        LQueue := TAMQPServerQueue.Create(AName, FMaxQueueLength, FPool);
      // Phase 4: the live queue needs to know WHERE it lives and WHETHER it
      // persists. Durable and Exclusive come from the descriptor, which is
      // already the source of truth -- rereading the declare's flags here would
      // leave room for the two to diverge (the same reason the policy comes
      // from there).
      LQueue.VHostName := AVHost;
      LQueue.Journal := FJournal;
      LQueue.Events := FEvents;
      if LDef <> nil then
        LQueue.Durable := LDef.Durable and (not LDef.Exclusive)
      else
        LQueue.Durable := False;
      // WS5: this is how the queue reaches the topology to republish dead
      // messages. Without a sink, a death becomes a plain drop.
      LQueue.DeadLetterSink := TAMQPDeadLetterAdapter.Create(Self, AVHost);
      FQueues.Add(Key(AVHost, AName), LQueue);
    end
    else if LQueue <> nil then
      // WS8: a REDECLARE counts as use and restarts the x-expires clock --
      // it is one of the rule's three events (the others are a consumer and
      // Basic.Get; publishing does not count).
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
  if LNoDurability then
    Result := amqerNoDurability;
end;

function TAMQPEngine.DeleteQueue(const AVHost, AName: string;
  AIfUnused, AIfEmpty: Boolean; AOwnerId: NativeUInt;
  out AMessageCount: Integer): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LQueue: TAMQPServerQueue;
  LDel: TAMQPQueueDeleteResult;
  LWasDurable: Boolean;
  LName: TAMQPRecName;
begin
  AMessageCount := 0;
  LVHost := FVHosts.GetOrCreate(AVHost);
  if not CheckExclusive(LVHost.QueueDef(AName), AOwnerId) then
    Exit(amqerResourceLocked);
  // BEFORE deleting: afterwards the descriptor does not exist and there is no
  // way to know whether it was durable. Applies to the explicit delete AND to
  // auto-delete, which arrives here through MaybeAutoDeleteQueue -- a DURABLE
  // auto-delete queue that vanished has to record that, or recovery revives it.
  LWasDurable := (FJournal <> nil) and IsQueueDurable(LVHost, AName);

  FLock.Enter;
  try
    LQueue := FindQueueLocked(AVHost, AName);
    if LQueue = nil then
      // DELETE IS IDEMPOTENT: deleting a queue that does not exist returns
      // Delete-Ok with a zero count, not 404. The letter of the spec says to
      // raise, but RabbitMQ made it idempotent long ago and that is what the
      // ecosystem relies on -- including this repo's RetryDlqVcl sample, which
      // deletes before redeclaring with new arguments ("redeclaring with
      // different args would be PRECONDITION_FAILED").
      //
      // Found by WS10: the SmokeTest with the TTL+DLX steps passed against
      // RabbitMQ and hit 404 against the embedded broker. That is exactly the
      // kind of divergence that running the SAME steps against both brokers
      // exists to catch. Deliberate deviation from the letter of the spec,
      // documented.
      Exit(amqerOk);

    // The actor decides: if-unused/if-empty are ITS state.
    LDel := LQueue.Delete(AIfUnused, AIfEmpty, AMessageCount);
    case LDel of
      amqqdInUse: Exit(amqerInUse);
      amqqdNotEmpty: Exit(amqerNotEmpty);
    end;
    // amqqdOk: Delete already stopped the actor (and drained the stock).
    FQueues.Remove(Key(AVHost, AName));
    LQueue.Free;
  finally
    FLock.Leave;
  end;

  LVHost.DeleteQueue(AName); // removes the descriptor and the bindings hanging on it
  // The bindings that vanished with the queue may have been the last ones of
  // an auto-delete exchange.
  SweepAutoDeleteExchanges(AVHost);
  Result := amqerOk;

  // A single delete record: the queue's bindings vanish together with it in
  // recovery, just as they vanish here, so writing an unbind for each would
  // be writing what is already implicit.
  if LWasDurable then
  begin
    LName.VHost := AVHost;
    LName.Name := AName;
    if not WriteTopology(AMQP_REC_QUEUE_DELETE, AmqpEncodeRecName(LName)) then
      Result := amqerNoDurability;
  end;
end;

function TAMQPEngine.PurgeQueue(const AVHost, AName: string;
  AOwnerId: NativeUInt; out AMessageCount: Integer): TAMQPEngineResult;
var
  LQueue: TAMQPServerQueue;
begin
  AMessageCount := 0;
  if not CheckExclusive(FVHosts.GetOrCreate(AVHost).QueueDef(AName),
    AOwnerId) then
    Exit(amqerResourceLocked);
  LQueue := FindQueue(AVHost, AName);
  if LQueue = nil then
    Exit(amqerNotFound);
  AMessageCount := LQueue.Purge;
  Result := amqerOk;
end;

function TAMQPEngine.BindQueue(const AVHost, AQueue, AExchange,
  ARoutingKey: string; AArguments: TAMQPFieldTable;
  AOwnerId: NativeUInt): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LRes: TAMQPTopologyResult;
  LPersist: Boolean;
  LPayload: TBytes;
  LRec: TAMQPRecBinding;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  if not CheckExclusive(LVHost.QueueDef(AQueue), AOwnerId) then
  begin
    AArguments.Free;
    Exit(amqerResourceLocked);
  end;
  LPersist := (FJournal <> nil) and IsExchangeDurable(LVHost, AExchange)
    and IsQueueDurable(LVHost, AQueue);
  if LPersist then
  begin
    // Encoded BEFORE the bind: afterwards the vhost owns AArguments.
    LRec.VHost := AVHost;
    LRec.Source := AExchange;
    LRec.Destination := AQueue;
    LRec.RoutingKey := ARoutingKey;
    LRec.Arguments := AArguments;
    LPayload := AmqpEncodeRecBinding(LRec);
  end;
  LRes := LVHost.BindQueue(AExchange, AQueue, ARoutingKey, AArguments);
  case LRes of
    amqtrOk, amqtrEquivalent: Result := amqerOk;
    amqtrCommandInvalid: Result := amqerCommandInvalid;
  else
    Result := amqerNotFound;
  end;
  if (LRes = amqtrOk) and LPersist then
    if not WriteTopology(AMQP_REC_QUEUE_BIND, LPayload) then
      Result := amqerNoDurability;
end;

function TAMQPEngine.UnbindQueue(const AVHost, AQueue, AExchange,
  ARoutingKey: string; AArguments: TAMQPFieldTable;
  AOwnerId: NativeUInt): TAMQPEngineResult;
var
  LVHost: TAMQPVHost;
  LRes: TAMQPTopologyResult;
  LPersist: Boolean;
  LPayload: TBytes;
  LRec: TAMQPRecBinding;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  if not CheckExclusive(LVHost.QueueDef(AQueue), AOwnerId) then
  begin
    AArguments.Free;
    Exit(amqerResourceLocked);
  end;
  LPersist := (FJournal <> nil) and IsExchangeDurable(LVHost, AExchange)
    and IsQueueDurable(LVHost, AQueue);
  if LPersist then
  begin
    LRec.VHost := AVHost;
    LRec.Source := AExchange;
    LRec.Destination := AQueue;
    LRec.RoutingKey := ARoutingKey;
    LRec.Arguments := AArguments;
    LPayload := AmqpEncodeRecBinding(LRec);
  end;
  LRes := LVHost.UnbindQueue(AExchange, AQueue, ARoutingKey, AArguments);
  if LRes in [amqtrOk, amqtrEquivalent] then
    Result := amqerOk
  else
    Result := amqerNotFound;
  if (LRes = amqtrOk) and LPersist then
    if not WriteTopology(AMQP_REC_QUEUE_UNBIND, LPayload) then
      Result := amqerNoDurability;
end;

function TAMQPEngine.QueueExists(const AVHost, AName: string): Boolean;
begin
  Result := FVHosts.GetOrCreate(AVHost).QueueExists(AName);
end;

{ --- consuming --- }

function TAMQPEngine.AddConsumer(const AVHost, AQueue, AConsumerTag: string;
  ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget;
  AOwnerId: NativeUInt): TAMQPEngineResult;
var
  LQueue: TAMQPServerQueue;
begin
  if not CheckExclusive(FVHosts.GetOrCreate(AVHost).QueueDef(AQueue),
    AOwnerId) then
    Exit(amqerResourceLocked);
  LQueue := FindQueue(AVHost, AQueue);
  if LQueue = nil then
    Exit(amqerNotFound);
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
    Exit(amqerNotFound);
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
    Exit(amqerResourceLocked);
  LQueue := FindQueue(AVHost, AQueue);
  if LQueue = nil then
    Exit(amqerNotFound);

  AFound := LQueue.Get(AChannelId, ADeliveryTag, ANoAck, AMessage,
    ARedelivered);
  // Get-Ok's message-count: how many are LEFT after this one.
  AMessageCount := LQueue.Stats.MessageCount;
  Result := amqerOk;
end;

{ --- automatic lifecycle (WS7) --- }

function TAMQPEngine.PersistExchange(const AName: string;
  ADurable: Boolean): Boolean;
begin
  Result := (FJournal <> nil) and ADurable and (AName <> '')
    and (not SameText(Copy(AName, 1, 4), 'amq.'));
end;

function TAMQPEngine.PersistQueue(ADurable, AExclusive: Boolean): Boolean;
begin
  // D20: an exclusive queue is NEVER persisted -- it dies with the owning
  // connection, and after a restart there is no owning connection.
  Result := (FJournal <> nil) and ADurable and (not AExclusive);
end;

function TAMQPEngine.IsExchangeDurable(AVHost: TAMQPVHost;
  const AName: string): Boolean;
var
  LDef: TAMQPExchangeDef;
begin
  LDef := AVHost.ExchangeDef(AName);
  Result := (LDef <> nil) and LDef.Durable
    and (AName <> '') and (not SameText(Copy(AName, 1, 4), 'amq.'));
end;

function TAMQPEngine.IsQueueDurable(AVHost: TAMQPVHost;
  const AName: string): Boolean;
var
  LDef: TAMQPQueueDef;
begin
  LDef := AVHost.QueueDef(AName);
  Result := (LDef <> nil) and LDef.Durable and (not LDef.Exclusive);
end;

// Writes the placement (and, if asked, the CONTENT along with it in the SAME
// batch) and returns the placement identifier. Zero = nothing was written.
//
// Runs on the thread of WHOEVER PUBLISHES -- the publisher's on publish, the
// source queue actor's on dead-letter. It is D24: only who already routed
// knows which placements need to be durable, and so it is the one that writes
// the batch.
function TAMQPEngine.WriteEnqueue(AQueue: TAMQPServerQueue;
  const AVHost: string; AMessage: TAMQPMessage; APriority: Byte;
  AMessageTtlMs: Int64; var AContentId: UInt64; AWaitVacancy: Boolean;
  out ALsn: UInt64): UInt64;
var
  LRecs: TAMQPJournalRecords;
  LContent: TAMQPRecContent;
  LEnq: TAMQPRecEnqueue;
  LN: Integer;
begin
  Result := 0;
  ALsn := 0;
  // Same reason as GravaTopo: during replay the placement is ALREADY in the
  // log -- rewriting it would duplicate the message on every boot.
  if FRecovering or (not DoesQueuePersist(AQueue)) then
    Exit;

  LN := 0;
  SetLength(LRecs, 2);
  if AContentId = 0 then
  begin
    // First placement of this message: the body goes ONCE (D22). The others
    // -- other queues of the same publish, or the dead-letter derivative --
    // reuse this identifier and do not rewrite the body.
    AContentId := NextId;
    LContent.ContentId := AContentId;
    LContent.UserId := AMessage.UserId;
    LContent.Body := AMessage.Body;
    LRecs[0].Kind := AMQP_REC_CONTENT;
    LRecs[0].Payload := AmqpEncodeRecContent(LContent);
    LN := 1;
  end;

  Result := NextId;
  LEnq.VHost := AVHost;
  LEnq.Queue := AQueue.Name;
  LEnq.EntryId := Result;
  LEnq.ContentId := AContentId;
  LEnq.Exchange := AMessage.Exchange;
  LEnq.RoutingKey := AMessage.RoutingKey;
  // The EFFECTIVE numbers, the same ones the actor will keep -- the queue is
  // the one that computes them, so there are not two copies of the rule.
  LEnq.Priority := AQueue.EffectivePriority(APriority);
  LEnq.EnqueuedAtWall := AmqpWallMs; // D21: wall clock on disk, never monotonic
  LEnq.TtlMs := AQueue.EffectiveTtl(AMessageTtlMs);
  LEnq.HeaderPayload := AMessage.HeaderPayload;
  LRecs[LN].Kind := AMQP_REC_ENQUEUE;
  LRecs[LN].Payload := AmqpEncodeRecEnqueue(LEnq);
  SetLength(LRecs, LN + 1);

  try
    if AWaitVacancy then
      // Publisher thread: MAY block on backpressure (D24, corollary c).
      ALsn := FJournal.Submit(LRecs)
    else
      // ACTOR thread (dead-letter): D2 forbids waiting on I/O.
      ALsn := FJournal.SubmitNoWait(LRecs);
  except
    on E: EAMQPJournal do
    begin
      // Journal failed: nothing was written, so the placement has no identity
      // and no retirement will be written for it later.
      Result := 0;
      ALsn := 0;
    end;
  end;
end;

function TAMQPEngine.NextId: UInt64;
begin
  FLock.Enter;
  try
    Inc(FNextId);
    Result := FNextId;
  finally
    FLock.Leave;
  end;
end;

function TAMQPEngine.DoesQueuePersist(AQueue: TAMQPServerQueue): Boolean;
begin
  Result := (FJournal <> nil) and (AQueue <> nil) and AQueue.Durable;
end;

function TAMQPEngine.WriteTopology(AKind: Byte; const APayload: TBytes): Boolean;
var
  LRecs: TAMQPJournalRecords;
  LLsn: UInt64;
begin
  if FJournal = nil then
    Exit(True); // without a journal there is nothing to write, and that is not a failure
  // MUTED during recovery: what we are declaring now CAME from the log.
  // Without this, each restart would rewrite the whole topology -- the WAL
  // would grow one copy per boot, forever, and WS7's compaction would never
  // catch up.
  if FRecovering then
    Exit(True);
  Result := False;
  SetLength(LRecs, 1);
  LRecs[0].Kind := AKind;
  LRecs[0].Payload := APayload;
  try
    // WAITS for the fsync before the *-Ok goes out. Topology operations are
    // rare and off the hot path, and a client that got Declare-Ok for a
    // DURABLE queue is entitled to assume it survived -- answering before the
    // fsync would be a half-millisecond lie that only shows up on restart.
    LLsn := FJournal.Submit(LRecs);
    Result := FJournal.WaitDurable(LLsn, AMQP_JOURNAL_SUBMIT_TIMEOUT_MS);
  except
    on E: EAMQPJournal do
      Result := False;
  end;
end;

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
  LStats := LQueue.Stats; // synchronous: barrier over the removal already posted
  // An auto-delete queue that NEVER had a consumer does not vanish -- or it
  // would disappear between the declare and the consume (same rule as RabbitMQ).
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
    Exit; // the default exchange never goes away
  LVHost := FVHosts.GetOrCreate(AVHost);
  LDef := LVHost.ExchangeDef(AExchange);
  if (LDef = nil) or (not LDef.AutoDelete) then
    Exit;
  if LVHost.HasBindingsFrom(AExchange) then
    Exit;
  // Through the ENGINE's method, not the vhost's: the engine's is the one that
  // writes the delete record (Phase 4, WS3). A DURABLE auto-delete exchange
  // that vanished has to record that, or recovery revives it -- and calling
  // the vhost directly here was exactly that hole.
  //
  // The result has nowhere to go (this path is automatic, with no client
  // waiting): if the journal has failed, the exchange vanishes from memory and
  // comes back on restart. It is a known consequence of a broken journal, and
  // the next durable declare drops the connection with 541 anyway.
  DeleteExchange(AVHost, AExchange, False);
end;

procedure TAMQPEngine.SweepAutoDeleteExchanges(const AVHost: string);
var
  LNames: TArray<string>;
  I: Integer;
begin
  LNames := FVHosts.GetOrCreate(AVHost).ExchangeNames;
  for I := 0 to High(LNames) do
    MaybeAutoDeleteExchange(AVHost, LNames[I]);
end;

function TAMQPEngine.DeleteExclusiveQueuesOf(const AVHost: string;
  AOwnerId: NativeUInt): Integer;
var
  LNames: TArray<string>;
  I, LCount: Integer;
begin
  Result := 0;
  if AOwnerId = 0 then
    Exit;
  LNames := FVHosts.GetOrCreate(AVHost).ExclusiveQueuesOf(AOwnerId);
  for I := 0 to High(LNames) do
    // AOwnerId=0 in DeleteQueue: internal call, we already know the owner is
    // this connection (that is how the queue got into the list).
    if DeleteQueue(AVHost, LNames[I], False, False, 0, LCount) = amqerOk then
      Inc(Result);
end;

{ --- recovery (WS6 of Phase 4) --- }

// Rebuilds the deadline per D21: on disk lives WALL CLOCK (enqueue instant
// + effective TTL in ms); in memory, monotonic. The conversion happens only
// here and on write -- that is what keeps an NTP jump from touching a live queue.
//
// Returns the REMAINING TTL in ms, or -1 for "no deadline". Zero means
// "already expired": the message goes in and dies through the NORMAL
// expiration path, dead-letter and all, on the actor's first touch.
function TtlRestante(const AEnq: TAMQPRecEnqueue; ANowWall: Int64): Int64;
var
  LElapsed: Int64;
begin
  if AEnq.TtlMs < 0 then
    Exit(-1);
  LElapsed := ANowWall - AEnq.EnqueuedAtWall;
  if LElapsed < 0 then
    LElapsed := 0; // the clock went backwards between the two boots
  Result := AEnq.TtlMs - LElapsed;
  if Result < 0 then
    Result := 0;
end;

procedure TAMQPEngine.Recover(AState: TAMQPRecoveredState);
var
  I: Integer;
  LEx: TAMQPRecExchange;
  LQ: TAMQPRecQueue;
  LB: TAMQPRecoveredBinding;
  LEnq: TAMQPRecEnqueue;
  LCount: TAMQPRecContent;
  LQueue: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LMsgCount, LConsCount: Integer;
  LNowWall: Int64;
begin
  if AState = nil then
    Exit;
  FRecovering := True;
  try
    // 1) TOPOLOGY. Exchanges before queues, and binds last: a bind needs both
    //    ends up.
    for I := 0 to AState.Exchanges.Count - 1 do
    begin
      LEx := AState.Exchanges[I];
      EnsureVHost(LEx.VHost);
      DeclareExchange(LEx.VHost, LEx.Name, LEx.ExchangeType, False,
        LEx.Durable, LEx.AutoDelete, LEx.Internal, LEx.Arguments);
      // OWNERSHIP OF THE ARGUMENTS HAS PASSED. The engine frees AArguments on
      // EVERY path -- including the refusing ones --, so the state must not
      // free them again in its destructor. Nil-ing item by item (rather than
      // a flag on the whole state) keeps the count exact even if this loop
      // stops halfway.
      LEx.Arguments := nil;
      AState.Exchanges[I] := LEx;
    end;

    for I := 0 to AState.Queues.Count - 1 do
    begin
      LQ := AState.Queues[I];
      EnsureVHost(LQ.VHost);
      // AExclusive=False ALWAYS: an exclusive queue was never persisted (D20),
      // and a recovered one has no live owner to be exclusive to.
      // AOwnerId=0 for the same reason. And the queue is born with
      // EverHadConsumer=False, which is what stops Phase 2's auto-delete guard
      // from deleting it at boot before anyone gets a chance to consume.
      DeclareQueue(LQ.VHost, LQ.Name, False, LQ.Durable, False, LQ.AutoDelete,
        LQ.Arguments, 0, LMsgCount, LConsCount);
      LQ.Arguments := nil; // ownership has passed -- see the comment above
      AState.Queues[I] := LQ;
    end;

    for I := 0 to AState.Bindings.Count - 1 do
    begin
      LB := AState.Bindings[I];
      if LB.DestinationIsExchange then
        BindExchange(LB.Binding.VHost, LB.Binding.Destination,
          LB.Binding.Source, LB.Binding.RoutingKey, LB.Binding.Arguments)
      else
        BindQueue(LB.Binding.VHost, LB.Binding.Destination,
          LB.Binding.Source, LB.Binding.RoutingKey, LB.Binding.Arguments, 0);
      LB.Binding.Arguments := nil; // ownership has passed -- see the comment above
      AState.Bindings[I] := LB;
    end;

    // 2) THE ID COUNTER continues AFTER the largest id the log already used.
    //    Restarting from zero would make a new placement collide with a
    //    recovered one, and the DEQ of one would retire the other -- silent
    //    and fatal.
    FLock.Enter;
    try
      if AState.MaxId > FNextId then
        FNextId := AState.MaxId;
    finally
      FLock.Leave;
    end;

    // 3) THE PLACEMENTS, IN LOG ORDER -- which is enqueue order. The priority
    //    bucket comes in the record, so each message lands exactly where
    //    D12's RequeueFront would put it. Nothing to sort.
    LNowWall := AmqpWallMs;
    for I := 0 to AState.Entries.Count - 1 do
    begin
      LEnq := AState.Entries[I];
      LQueue := FindQueue(LEnq.VHost, LEnq.Queue);
      if LQueue = nil then
        Continue; // queue deleted in the log; the replay should already have removed it
      if not AState.Contents.TryGetValue(LEnq.ContentId, LCount) then
        Continue;
      LMsg := TAMQPMessage.Create(LEnq.Exchange, LEnq.RoutingKey, LCount.UserId,
        LEnq.HeaderPayload, LCount.Body);
      try
        // The TTL is passed as the REMAINING one, not the recorded effective
        // one: the actor's CalculaPrazo applies "the smaller wins" again, and
        // the remaining is by construction <= the effective, so the minimum
        // returns the remaining. One rule, one implementation -- there is no
        // separate enqueue path for recovery.
        LQueue.PostMessage(LMsg, LEnq.Priority, TtlRestante(LEnq, LNowWall),
          LEnq.EntryId, LEnq.ContentId, True); // True = D27, comes back redelivered
      finally
        LMsg.Release; // the queue did its own AddRef
      end;
    end;
  finally
    FRecovering := False;
  end;
end;

{ --- publishing --- }

function TAMQPEngine.RouteMessage(const AVHost: string;
  const AMessage: TAMQPServerMessage; out ARejected: Boolean;
  out ALsn: UInt64): Boolean;
var
  LVHost: TAMQPVHost;
  LDestinations: TArray<string>;
  LMsg: TAMQPMessage;
  LQueue: TAMQPServerQueue;
  I: Integer;
  LHeaders: TAMQPFieldTable;
  LPriority: Byte;
  LTtlMs: Int64;
  LPersistent: Boolean;
  LContentId, LEntryId, LLsn: UInt64;
begin
  ARejected := False;
  ALsn := 0;
  LVHost := FVHosts.GetOrCreate(AVHost);
  // The channel's live headers: matching runs HERE, on the publisher's thread
  // (Phase 2 invariant), not after the enqueue.
  LHeaders := nil;
  if AMessage.Properties.Has(bpHeaders) then
    LHeaders := AMessage.Properties.Headers;

  LDestinations := LVHost.Route(AMessage.Exchange, AMessage.RoutingKey, LHeaders);
  Result := Length(LDestinations) > 0;
  if not Result then
  begin
    PcAtomicInc(FUnrouted);
    Exit;
  end;
  PcAtomicInc(FRouted);

  // ONE message for N queues: body and raw header shared without copying,
  // each queue taking its own reference (Phase 2 architecture decision).
  // Priority and 'expiration' are read HERE, on the publisher's thread, for
  // the same reason as headers matching: TAMQPMessage keeps the RAW header
  // (decision D1) and whoever still has the decoded properties is the
  // publisher. The actor gets both numbers ready.
  LPriority := 0;
  if AMessage.Properties.Has(bpPriority) then
    LPriority := AMessage.Properties.Priority;
  LTtlMs := -1;
  if AMessage.Properties.Has(bpExpiration) then
    // A malformed one does not get here (the FSM rejects it with 406 before
    // routing); False would only happen if this sink were called by another
    // path, and then "no TTL" is the safe behavior.
    if not AmqpParseExpirationMs(AMessage.Properties.Expiration, LTtlMs) then
      LTtlMs := -1;

  // D20: only a message with delivery-mode 2 goes to disk, and only in durable
  // queues. Read HERE, on the publisher's thread, for the same reason as
  // priority and 'expiration' (decision D1): whoever has the decoded
  // properties in hand is the publisher.
  LPersistent := AMessage.Properties.Has(bpDeliveryMode)
    and (AMessage.Properties.DeliveryMode = 2); // 2 = persistent (spec)

  // DISK CAP (D26): when exceeded, the PERSISTENT publish is refused -- and
  // refused BEFORE writing or posting anything, so there is no half-done
  // publish. The memory cap drops from the head (D7); the disk cap refuses,
  // because dropping here would mean deleting data the broker already
  // confirmed as durable. A transient publish is not affected: it does not
  // touch the journal.
  if LPersistent and (FJournal <> nil) and FJournal.IsFull then
  begin
    FJournal.IncrementRefused;
    ARejected := True;
    Exit(False);
  end;

  LMsg := TAMQPMessage.FromServerMessage(AMessage);
  try
    LContentId := 0;
    for I := 0 to High(LDestinations) do
    begin
      LQueue := FindQueue(AVHost, LDestinations[I]);
      if LQueue = nil then
        Continue;
      LEntryId := 0;
      if LPersistent then
      begin
        LEntryId := WriteEnqueue(LQueue, AVHost, LMsg, LPriority, LTtlMs,
          LContentId, True, LLsn);
        // WS5/D24: this publish's confirm waits for the LARGEST LSN it wrote.
        // Since the journal assigns increasing LSNs and each queue's batch is
        // indivisible, the largest is the last queue's that wrote -- but
        // comparing is free and does not depend on that order staying true.
        if LLsn > ALsn then
          ALsn := LLsn;
      end;
      // D15: only a queue declared with reject-publish pays for the
      // SYNCHRONOUS enqueue. The others take the usual asynchronous path.
      if LQueue.Policy.Overflow = amqovRejectPublish then
      begin
        if not LQueue.TryPostMessage(LMsg, LPriority, LTtlMs, LEntryId,
          LContentId) then
          ARejected := True;
      end
      else
        // does its own AddRef
        LQueue.PostMessage(LMsg, LPriority, LTtlMs, LEntryId, LContentId);
    end;
  finally
    LMsg.Release; // the publisher's reference
  end;
end;

procedure TAMQPEngine.RepublishTo(const AVHost, AExchange,
  ARoutingKey: string; AMessage: TAMQPMessage; APriority: Byte;
  AContentId: UInt64);
var
  LLsnIgn: UInt64;
  LEntryId: UInt64;
  LVHost: TAMQPVHost;
  LDestinations: TArray<string>;
  LQueue: TAMQPServerQueue;
  LEd: TAMQPHeaderEditor;
  I: Integer;
begin
  LVHost := FVHosts.GetOrCreate(AVHost);
  // A headers-type DLX needs the decoded table to match. Decoding here is CPU
  // on the actor thread, which D17 allows -- it is the COLD path (a message
  // dies once), not the publish one.
  LEd := nil;
  try
    try
      LEd := TAMQPHeaderEditor.Create(AMessage.HeaderPayload);
      LDestinations := LVHost.Route(AExchange, ARoutingKey, LEd.Headers);
    except
      // An unreadable header must not bring down the source queue's actor.
      on E: Exception do
        Exit;
    end;
  finally
    LEd.Free;
  end;

  if Length(LDestinations) = 0 then
  begin
    // DLX with no route: the message dies for good. It is the same fate it
    // would have with no DLX at all, so it is not an error -- there is just
    // nowhere to send it.
    PcAtomicInc(FUnrouted);
    Exit;
  end;
  PcAtomicInc(FRouted);

  for I := 0 to High(LDestinations) do
  begin
    LQueue := FindQueue(AVHost, LDestinations[I]);
    if LQueue = nil then
      Continue;
    // D23: the NEW placement is written HERE, and the old one is only retired
    // afterwards, in the MorreCom that called us. Failing halfway produces a
    // duplicate, never a loss. The body is NOT rewritten -- AContentId already
    // identifies what is on disk.
    LEntryId := 0;
    if AContentId <> 0 then
      // The LSN is discarded on purpose: dead-letter has no publisher seq-no
      // waiting for it -- the original publisher was already confirmed when
      // the message entered the source queue.
      LEntryId := WriteEnqueue(LQueue, AVHost, AMessage, APriority, -1,
        AContentId, False, LLsnIgn); // in the actor: without waiting for room (D2)
    // The message TTL goes as -1: the original 'expiration' was removed by
    // dead-lettering (or it would die again on the spot in the DLQ), and the
    // deadline that applies now is the DESTINATION queue's.
    LQueue.PostMessage(AMessage, APriority, -1, LEntryId, AContentId);
  end;
end;

procedure TAMQPEngine.ExpireIdleQueues;
var
  LPairs: TArray<TPair<string, TAMQPServerQueue>>;
  LNames: TArray<string>;
  LVHosts: TArray<string>;
  I, N, LBarra: Integer;
  LStats: TAMQPQueueStats;
  LMsgs: Integer;
  LKey: string;
begin
  // Snapshot under the lock, decision OUTSIDE it: reading Stats is a
  // SYNCHRONOUS command on the actor, and holding the engine lock while
  // waiting on an actor would invite a deadlock (the actor may be
  // republishing a dead-letter, which asks for FindQueue again).
  FLock.Enter;
  try
    LPairs := FQueues.ToArray;
  finally
    FLock.Leave;
  end;

  SetLength(LNames, 0);
  SetLength(LVHosts, 0);
  for I := 0 to High(LPairs) do
  begin
    // Only a queue that ASKED for x-expires pays for the synchronous command.
    if LPairs[I].Value.Policy.ExpiresMs < 0 then
      Continue;
    try
      LStats := LPairs[I].Value.Stats;
    except
      // Queue stopping under the sweep: not an error, a normal race.
      Continue;
    end;
    if LStats.IdleMs < LPairs[I].Value.Policy.ExpiresMs then
      Continue;

    // The key is 'vhost/name'; the name may contain a slash, the vhost does
    // not start with one, so the FIRST slash is the separator.
    LKey := LPairs[I].Key;
    LBarra := Pos('/', LKey);
    if LBarra <= 0 then
      Continue;
    N := Length(LNames);
    SetLength(LNames, N + 1);
    SetLength(LVHosts, N + 1);
    LVHosts[N] := Copy(LKey, 1, LBarra - 1);
    LNames[N] := Copy(LKey, LBarra + 1, MaxInt);
  end;

  for I := 0 to High(LNames) do
    try
      // AOwnerId=0: internal caller, passes CheckExclusive. No
      // if-unused/if-empty: x-expires deletes even with messages inside, which
      // is RabbitMQ's behavior -- the argument is about USE, not emptiness.
      DeleteQueue(LVHosts[I], LNames[I], False, False, 0, LMsgs);
    except
      // Another thread may have deleted the queue between decision and execution.
      ;
    end;
end;

// Posts under FLock (v0.5.0): DeleteQueue frees the queue under FLock, so a
// queue taken from FQueues is only alive while FLock is held. Posting after
// leaving it put a tick into a queue freed in between (found by heaptrc: one
// leaked command, once in 16 runs of the durable acceptance pass). Post never
// waits (the queue's monitor, briefly), and FLock -> queue monitor is the
// order DeleteQueue already takes.
procedure TAMQPEngine.DeliverTick;
var
  LQueue: TAMQPServerQueue;
begin
  FLock.Enter;
  try
    for LQueue in FQueues.Values do
      LQueue.PostDeliverTick;
  finally
    FLock.Leave;
  end;
end;

function TAMQPEngine.RoutedCount: Integer;
begin
  Result := PcAtomicGet(FRouted);
end;

function TAMQPEngine.UnroutedCount: Integer;
begin
  Result := PcAtomicGet(FUnrouted);
end;

end.
