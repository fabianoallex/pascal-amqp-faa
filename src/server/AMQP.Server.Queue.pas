unit AMQP.Server.Queue;

{$I amqp.inc}

{ The queue as an ACTOR -- broker sub-module, WS3 of Phase 2 (in-memory
  routing engine, see CLAUDE.md).

  Decision D2 (locked): the actor does NOT have its own thread. It is a work
  item scheduled on a TPcThreadPool -- N idle queues must not become N threads.
  The rule this imposes: NO command may block the worker waiting for I/O
  (that is why WS4's delivery is a non-blocking post and the slow consumer
  leaves the round-robin, decision D3).

  THE GOLDEN RULE: no queue state field (stock, unacked, consumers, counters)
  is read or written outside the actor -- including the counts that go to the
  client in Queue.Declare-Ok/Purge-Ok/Delete-Ok. Whoever is outside talks to
  the queue ONLY through commands.

  Serial command mailbox
  ----------------------
  Post enqueues under the lock and, if FScheduled is false, sets it and
  schedules ONE work item. The runner loops: under the lock, if the mailbox is
  empty it clears FScheduled and exits; otherwise it takes a command and runs
  it OUTSIDE the lock. FScheduled guarantees at most one scheduled work item
  per queue -- whoever is in the loop is the only executor. The subtle point:
  the runner only declares the mailbox empty UNDER THE LOCK, which closes the
  window between "saw it empty" and "cleared the flag" (a concurrent Post that
  sees FScheduled=True does not schedule, and the current runner finds the
  command on the next turn). Cap of AMQP_QUEUE_CMD_BATCH commands per round --
  when exceeded, it reschedules instead of monopolizing a pool worker.

  Asynchronous vs synchronous commands
  ------------------------------------
  Asynchronous (enqueue, add/remove consumer, ack/nack/reject): the actor frees
  them in the finally. Synchronous (Get, Purge, Delete, Stats): they carry a
  completion event and result fields, because the connection thread needs the
  answer to build the *-Ok. The caller waits with a generous timeout -- the
  actor does no I/O, so exceeding it is OUR bug, not peer slowness.

  The command is REFCOUNTED (two references: the caller and the mailbox), not
  "owned by the caller": if a synchronous one exceeds the timeout, the owner
  freeing the object while the actor still holds it would be a use-after-free
  stacked on top of a bug that is already bad. It costs four lines and wipes
  out the whole class of crash.

  Live guard: the actor records the thread-id while it runs, and posting a
  SYNCHRONOUS command from that thread raises EAMQPQueueActor right away,
  instead of hanging silently (the actor would be waiting for itself).

  Destruction order
  -----------------
  Stop marks the stop under the lock (refuses new posts and new scheduling),
  waits for the actor to leave the loop (FScheduled=False) with a deadline and
  ONLY THEN drains the stock inline. NEVER free the queue without Stop having
  returned -- it is the same family of error that cost a double free in Phase 1.
  Stop does NOT use a "stop command": a pool shutting down frees WITHOUT running
  the item it receives after Destroy starts (what was already queued it does
  run -- TPcThreadPool's Destroy never discarded the queue; this note said the
  opposite until F8), so a command posted there would never run and Stop would
  only return on the timeout.

  WS3 scope -- what is NOT here
  -----------------------------
  DELIVERY is WS4. This unit registers the consumers (tag, channel, no-ack,
  target) and keeps the round-robin index, but does not dispatch Basic.Deliver,
  does not apply prefetch and does not look at Channel.Flow. The seam is
  IAMQPDeliveryTarget: WS4 implements it (in the channel/connection) and passes
  it in the consumer. Note also that today's TAMQPFrameWriter.PostFrames BLOCKS
  when the outbound queue fills up -- incompatible with D2/D3; the
  TryPostFrames that delivery needs comes in WS4, not here. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Server.Events,
  AMQP.Server.Message,
  AMQP.Server.Header,
  AMQP.Server.Resources,
  AMQP.Server.Journal,
  AMQP.Server.Records;

const
  /// How many commands the actor runs per round before rescheduling. Without
  /// a cap, a very busy queue would monopolize a pool worker as long as there
  /// was work.
  AMQP_QUEUE_CMD_BATCH = 64;

  /// Wait for a synchronous command. Generous on purpose: the actor never does
  /// I/O, so exceeding this is not a slow peer -- it is our bug (or a saturated
  /// pool, see the EAMQPQueueActor message).
  AMQP_QUEUE_SYNC_TIMEOUT_MS = 15000;

  /// How long Stop waits for the actor to leave the loop before giving up.
  AMQP_QUEUE_STOP_TIMEOUT_MS = 15000;

  /// Cap on dead-letter hops (decision D14). Same spirit as the
  /// AMQP_MAX_EXCHANGE_HOP_DEPTH that Route already uses: the DLX graph may
  /// have a cycle, and cutting by DEPTH is cheaper than carrying the set of
  /// visited queues, with the same practical effect. When exceeded, the message
  /// is dropped and counted -- there is no path in which it circulates forever.
  AMQP_MAX_DEADLETTER_HOPS = 16;

type
  { Misuse of the actor: a synchronous command posted from inside the actor
    itself, a command on an already stopped queue, or a synchronous command
    timeout. Always a programming bug or pool saturation -- never a protocol error. }
  EAMQPQueueActor = class(Exception);

  { Result of a Queue.Delete with if-unused/if-empty. Mapping to a reply-code
    (405/406) is WS5's job -- same spirit as TAMQPTopologyResult. }
  TAMQPQueueDeleteResult = (amqqdOk, amqqdInUse, amqqdNotEmpty);

  { Counts the client sees (Queue.Declare-Ok, Purge-Ok, Delete-Ok) plus the
    internal metrics. Only the actor produces them. }
  TAMQPQueueStats = record
    /// Ready messages (does not count the delivered and not yet acked ones).
    MessageCount: Integer;
    ConsumerCount: Integer;
    /// Delivered and awaiting ack/nack/reject.
    UnackedCount: Integer;
    /// Dropped by MaxLength (D7) or by a post on an already stopped queue.
    /// MANDATORY metric: without it, messages vanish without anyone knowing.
    DroppedCount: Int64;
    /// Delivered to consumers since creation (does not count Basic.Get).
    DeliveredCount: Int64;
    /// How long the queue has been UNUSED (WS8 of Phase 3), in ms. Zero while
    /// there is a consumer: a queue with a consumer is in use by definition,
    /// and the x-expires clock only starts when the last one leaves.
    ///
    /// "Use" is a consumer, Basic.Get and a redeclare -- PUBLISHING DOES NOT
    /// COUNT. That is RabbitMQ's rule, and its reason is the argument's purpose:
    /// a queue that only receives and that nobody reads is exactly the one to reclaim.
    IdleMs: Int64;
    /// Dead messages that were republished to a DLX. Not added to ExpiredCount
    /// nor to DroppedCount: those count the REASON, this one counts the DESTINATION.
    DeadLetteredCount: Int64;
    /// Expired by TTL (the queue's x-message-ttl or the message's 'expiration').
    /// MANDATORY metric for the same reason as DroppedCount: without it,
    /// messages vanish without anyone knowing. In WS5 these start going to the
    /// DLX instead of being dropped -- the count still holds.
    ExpiredCount: Int64;
    /// True if this queue HAS EVER HAD at least one consumer. That is what
    /// separates "auto-delete that lost its last consumer" (goes away) from
    /// "auto-delete that never had one" (stays) -- same rule as RabbitMQ.
    EverHadConsumer: Boolean;
  end;

  { Dead-lettering seam (WS5). The engine implements it, being the only one
    that sees the topology; the queue only keeps the reference.

    It exists as an interface for the same reason as IAMQPDeliveryTarget:
    without it, AMQP.Server.Queue would have to see AMQP.Server.Engine, which
    already sees the queue -- a unit cycle.

    IT MUST NOT BLOCK OR DO I/O (D17): republishing is reading the topology
    under an RWLock and posting to other queues' mailboxes, both fast and
    without waiting. If it ever needs to wait for something, dead-lettering
    leaves the actor. }
  IAMQPDeadLetterSink = interface
    ['{2B9F1C74-5E30-4A88-9D61-7C0E3F5A82B4}']
    /// Republishes AMessage to ADlx with ARoutingKey. The reference still
    /// belongs to the CALLER (the implementation AddRefs it if it wants to keep it).
    /// AContentId identifies the BODY already written: the new placement reuses
    /// it (D22), which is why the body is never rewritten on a dead-letter.
    /// Zero = the message was not persisted.
    procedure DeadLetter(const ADlx, ARoutingKey: string;
      AMessage: TAMQPMessage; APriority: Byte; AContentId: UInt64);
  end;

  { Delivery seam (WS4). The channel/connection implements it; the queue only
    keeps the reference in the consumer.

    TryDeliver MUST NOT BLOCK (D2/D3): it returns False when the writer's
    outbound queue is full or the channel died, and in that case the consumer
    leaves the round-robin until it drains -- blocking the actor would be
    head-of-line blocking for all the other consumers of the same queue. }
  IAMQPDeliveryTarget = interface
    ['{7C4E1F82-3A65-4D19-B0E7-5F2A8C93D604}']
    /// Channel identity (NativeUInt and not a reference, for the same reason
    /// as TAMQPQueueDef.OwnerId: not to create a cycle with AMQP.Server.Connection).
    function ChannelId: NativeUInt;
    /// False if the channel/connection has already died -- the actor drops the consumer.
    function IsAlive: Boolean;
    /// Tries to write Basic.Deliver + content-header + body frames as ONE
    /// indivisible batch. False = it did not fit NOW (outbound queue full,
    /// channel with Channel.Flow off, prefetch exceeded or dead channel) --
    /// without blocking and without raising. The message still belongs to the caller.
    ///
    /// The delivery-tag is MONOTONIC PER CHANNEL, so the one generating it is
    /// the target (the channel), not the queue: a queue delivers to N channels
    /// and a channel receives from N queues. It comes out in ADeliveryTag and is
    /// only consumed when the delivery actually enters the write queue -- a
    /// refusal does not open a gap in the sequence.
    ///
    /// ANoAck comes from the consumer because it changes two things on the
    /// target: a no-ack delivery does not count toward the prefetch and creates
    /// no unacked entry.
    function TryDeliver(const AConsumerTag, AQueueName: string;
      ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
      out ADeliveryTag: UInt64): Boolean;
    /// The queue vanished from under the consumer (Queue.Delete, or
    /// auto-delete): sends Basic.Cancel so the client knows. Like TryDeliver, IT
    /// MUST NOT BLOCK -- False = it did not fit now, and the consumer dies
    /// silently (the client finds out on next use). There is no retry: the
    /// queue is being destroyed.
    function TryCancel(const AConsumerTag: string): Boolean;
  end;

  { A registered consumer (Basic.Consume). The queue owns these objects
    after PostAddConsumer. }
  TAMQPServerConsumer = class
  private
    FConsumerTag: string;
    FChannelId: NativeUInt;
    FNoAck: Boolean;
    FExclusive: Boolean;
    FTarget: IAMQPDeliveryTarget;
    FSuspended: Boolean;
  public
    /// ATarget may NOT be nil: a consumer without a target would have no way
    /// to receive anything. The ChannelId comes FROM IT -- it is the same
    /// identity the queue keys the unacked messages by and with which the
    /// connection teardown asks for PostRemoveChannel; keeping an independent
    /// copy would leave room for the two to diverge and the requeue of a dead
    /// channel to miss its target.
    constructor Create(const AConsumerTag: string;
      ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget);
    property ConsumerTag: string read FConsumerTag;
    property ChannelId: NativeUInt read FChannelId;
    property NoAck: Boolean read FNoAck;
    property Exclusive: Boolean read FExclusive;
    property Target: IAMQPDeliveryTarget read FTarget;
    /// D3: refused the delivery IN THIS round (outbound queue full, prefetch
    /// exceeded, flow off) and therefore leaves the round-robin until the end
    /// of it. It is not persistent state: every delivery round starts over with
    /// all consumers eligible and simply tries again -- an attempt costs a lock
    /// and a counter comparison, and this way there is no "suspended" state to
    /// resynchronize when the channel drains.
    property Suspended: Boolean read FSuspended write FSuspended;
  end;

  { A message in the stock. Redelivered is per QUEUE (the same TAMQPMessage
    may be in N queues with different histories), which is why it lives in
    the entry and not in the message object. }
  TAMQPQueueEntry = record
    Msg: TAMQPMessage;
    Redelivered: Boolean;
    /// Priority level ALREADY LIMITED to the queue's cap. The one reading the
    /// message's 'priority' property is the publisher thread (Phase 2
    /// invariant: the message keeps the RAW header, decision D1) -- the actor
    /// only receives the ready-made number.
    Priority: Byte;
    /// Absolute instant (same base as NowTick) at which the message expires.
    /// 0 = never expires.
    ExpiresAt: UInt64;
    /// Identity of this PLACEMENT in the journal (Phase 4, D22). 0 = the message
    /// was not persisted -- non-durable queue, non-persistent message, or a
    /// broker without DataDir. When it is 0, no retirement record is written.
    JournalId: UInt64;
    /// Identity of the BODY in the journal. Several placements (and the one
    /// derived by dead-lettering) share it -- that is what makes the body be
    /// written only once.
    ContentId: UInt64;
  end;

  { A delivery awaiting confirmation, keyed by (channel, delivery-tag). }
  TAMQPUnackedEntry = record
    ChannelId: NativeUInt;
    DeliveryTag: UInt64;
    Msg: TAMQPMessage;
    /// Kept because the requeue must return the message TO THE HEAD OF ITS OWN
    /// BUCKET, with the original deadline -- not to the absolute head nor with
    /// the clock reset.
    Priority: Byte;
    ExpiresAt: UInt64;
    /// The same as the stock entry's: the placement stays ALIVE in the journal
    /// while it is here (an unacked delivery is not retired -- that is what
    /// makes D27 come for free in the replay).
    JournalId: UInt64;
    ContentId: UInt64;
  end;

  TAMQPQueueCmdKind = (
    amqqcEnqueue, amqqcAddConsumer, amqqcRemoveConsumer, amqqcRemoveChannel,
    amqqcAck, amqqcNack, amqqcReject, amqqcDeliverTick,
    amqqcEnqueueSync, amqqcTouch, amqqcGet, amqqcPurge, amqqcDelete,
    amqqcStats);

  { A command in the mailbox. Public fields on purpose: it is a pass-through
    record between the poster and the actor, not a domain object.

    Refcounted (caller + mailbox) -- see the unit header. }
  TAMQPQueueCommand = class
  private
    FRefCount: Integer;
    FKind: TAMQPQueueCmdKind;
    FEvent: TEvent; // nil for asynchronous ones
    function GetIsSync: Boolean;
  public
    // input
    Msg: TAMQPMessage;             // amqqcEnqueue (the queue's OWN reference)
    Consumer: TAMQPServerConsumer; // amqqcAddConsumer (the queue becomes the owner)
    ConsumerTag: string;
    ChannelId: NativeUInt;
    DeliveryTag: UInt64;
    NoAck: Boolean;
    Priority: Byte;        // amqqcEnqueue
    MessageTtlMs: Int64;   // amqqcEnqueue (-1 = no 'expiration')
    JournalId: UInt64;     // amqqcEnqueue (0 = not persisted)
    ContentId: UInt64;     // amqqcEnqueue
    /// amqqcEnqueue: entry coming from RECOVERY (WS6). D27 says every
    /// recovered entry comes back with redelivered=True -- persisting the flag
    /// would cost a write on the hottest path there is, and would be false
    /// precision: after a crash the broker does not know whether the consumer
    /// got to see the message.
    Restored: Boolean;   // amqqcEnqueue
    Multiple: Boolean;
    Requeue: Boolean;
    IfUnused: Boolean;
    IfEmpty: Boolean;
    // output (synchronous)
    ResOk: Boolean;
    ResCount: Integer;
    ResMsg: TAMQPMessage;
    ResRedelivered: Boolean;
    ResStats: TAMQPQueueStats;
    ResDelete: TAMQPQueueDeleteResult;

    constructor Create(AKind: TAMQPQueueCmdKind; ASync: Boolean);
    destructor Destroy; override;
    function AddRef: Integer;
    /// The last Release frees the object (and, with it, the message references
    /// left in Msg/ResMsg -- the drop path).
    function Release: Integer;
    procedure Signal;
    function WaitFor(ATimeoutMs: Cardinal): Boolean;

    property Kind: TAMQPQueueCmdKind read FKind;
    property IsSync: Boolean read GetIsSync;
  end;

  TAMQPServerQueue = class;

  { The work item that runs one actor round on a pool worker. }
  TAMQPQueueWork = class(TPcWorkItem)
  private
    FQueue: TAMQPServerQueue;
  public
    constructor Create(AQueue: TAMQPServerQueue);
    procedure Execute; override;
  end;

  { The queue. Every interaction from outside is via Post*/synchronous command. }
  TAMQPServerQueue = class
  private
    FName: string;
    FMaxLength: Integer;      // EFFECTIVE count cap (global + queue)
    FMaxBytes: Int64;         // EFFECTIVE byte cap (-1 = no cap)
    FReadyBytes: Int64;     // sum of the READY bodies (actor only)
    FPool: TPcThreadPool;
    // --- mailbox (under FMon) ---
    FMon: TPcMonitor;
    FMailbox: TQueue<TAMQPQueueCommand>;
    FScheduled: Boolean;
    FStopping: Boolean;
    FStopped: Boolean;
    FActorThread: TThreadID;
    FPolicy: TAMQPQueuePolicy;
    FMaxPriority: Integer; // 0 = queue without priority (a single bucket)
    // --- state, actor ONLY ---
    // ONE BUCKET PER priority LEVEL (decision D12: buckets, not a heap). A queue
    // without x-max-priority has FMaxPriority=0, so a single bucket and the same
    // code path as before -- no conditional branch scattered around.
    FStock: array of TQueue<TAMQPQueueEntry>;
    FRequeued: array of TQueue<TAMQPQueueEntry>; // logical head of each bucket
    FUnacked: TList<TAMQPUnackedEntry>;
    FConsumers: TObjectList<TAMQPServerConsumer>;
    FRoundRobin: Integer; // next consumer to serve (round-robin)
    FEverHadConsumer: Boolean;
    FDropped: Int64;
    FDelivered: Int64;
    FExpired: Int64;
    FDeadLettered: Int64;
    FDeadLetterSink: IAMQPDeadLetterSink;
    FJournal: TAMQPJournal;
    FEvents: IAMQPEventSink;
    FVHostName: string;
    FDurable: Boolean;
    /// One-way latch: becomes True on the first enqueue with a deadline and
    /// never goes back. It only serves to let a queue WITHOUT any expirable
    /// message skip the expiration loop on every round (D13: the sweep only
    /// costs in a queue that has a TTL). Overestimating is harmless -- at most
    /// it sweeps a queue that no longer has anything with a deadline.
    FHasExpiration: Boolean;
    /// Instant of the last USE (see Stats.IdleMs). Actor only.
    FLastUsed: UInt64;

    procedure ScheduleLocked;
    procedure Post(ACmd: TAMQPQueueCommand);
    { --- observability (Phase 4.1, Inc. 2) ---
      Emitting from the ACTOR is only legitimate because Emit does not block
      and does not raise (D31): it copies into the ring and returns. That was
      the doubt D30 settled -- calling a user handler from here would block the
      pool worker, which is the SAME pool as the other actors'.

      The actor knows no connection, user or address: who publishes and who
      consumes stay on the other side of the mailbox, on purpose. So an
      actor-origin event goes out with ConnectionId=0 and empty
      RemoteAddr/Username, and what identifies it is VHost+QueueName
      (+ConsumerTag on delivery). Whoever needs to tie a consumer to a
      connection joins by ConsumerTag, which Inc. 1's seConsumerRegistered
      carries together with the connection. }
    function WantsEvent(AType: TAMQPServerEventType): Boolean;
    procedure EmitMessageEvent(AType: TAMQPServerEventType;
      const AEntry: TAMQPQueueEntry; const AReason: string);
    procedure PostSync(ACmd: TAMQPQueueCommand);
    // --- actor helpers (never called from outside it) ---
    function ReadyCount: Integer;
    function TakeReady(out AEntry: TAMQPQueueEntry): Boolean;
    function PeekReady(out AEntry: TAMQPQueueEntry): Boolean;
    /// Expires what is past its deadline, FROM THE HEAD of each bucket backward.
    /// See the implementation's comment for the limitation (deliberate, and the
    /// same as RabbitMQ's) of a message with its own deadline in the middle of the queue.
    procedure ExpireMessages;
    /// Single point through which an expired message leaves. In WS4 it only
    /// drops the reference and counts; WS5 swaps its body for "send to the DLX".
    procedure DiscardExpired(AEntry: TAMQPQueueEntry);
    /// Sends the entry to the queue's DLX, if there is one; otherwise only drops
    /// the reference. Returns True if it actually republished. ALWAYS consumes
    /// the entry's reference -- the caller does not drop it again.
    function DeadLetterWith(AEntry: TAMQPQueueEntry; const AReason: string): Boolean;
    /// Absolute deadline of a message, combining the QUEUE's TTL with the
    /// MESSAGE's TTL (the smaller wins). 0 = never.
    function CalculateExpiration(AMessageTtlMs: Int64): UInt64;
    /// Limits the requested priority to the queue's cap.
    function LimitPriority(APriority: Byte): Byte;
    function NextEligible: TAMQPServerConsumer;
    procedure DeliverPending;
    procedure RequeueFront(AMsg: TAMQPMessage; APriority: Byte;
      AExpiresAt: UInt64; AJournalId: UInt64 = 0; AContentId: UInt64 = 0);
    procedure EnforceMaxLength;
    /// Adds/subtracts an entry's body in the ready-bytes accumulator.
    /// Every entry that ENTERS or LEAVES the stock goes through here -- if a
    /// path forgets, the accumulator becomes garbage and the byte cap stops
    /// holding (with no immediate symptom).
    /// The SINGLE point through which a placement is retired on disk. Called
    /// from inside the actor, with SubmitNoWait: D2 forbids the actor from
    /// waiting for I/O, and a retirement record can never be refused (D24, corollary c).
    ///
    /// There is no retirement when the WHOLE queue is deleted: the topology
    /// delete record already takes its placements along, the same way it takes
    /// the bindings -- writing a DEQ per entry would be writing what is
    /// already implicit.
    procedure Retire(AJournalId: UInt64);
    procedure TrackBytes(const AEntry: TAMQPQueueEntry; ASignal: Integer);
    /// True if the queue is full RIGHT NOW (by count or by bytes). Only makes
    /// sense in a queue with x-overflow reject-publish.
    function IsFull: Boolean;
    /// ARejected separates ACK from NACK/REJECT-without-requeue: both arrive
    /// here with ARequeue=False, but only the second is a death (reason 'rejected').
    /// Confusing the two would send every acked message to the DLX.
    procedure ResolveUnacked(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple, ARequeue, ARejected: Boolean);
    function CurrentStats: TAMQPQueueStats;
    procedure DrainState;
  protected
    /// The exact point between "the command loop ended" and "the end-of-round
    /// lock was taken" -- the WINDOW the mailbox design closes. A no-op in
    /// production; it exists because that window is unreachable by an ordinary
    /// stress test (measured: a version with the empty-mailbox check OUTSIDE
    /// the lock passes the 16,000 concurrent posts, because a lost command is
    /// resurrected by the next post -- the bug is only fatal on the LAST post
    /// before idleness). The test overrides this to post from another thread
    /// exactly here. MUST NOT RAISE.
    procedure ActorRoundEnding; virtual;
    /// The actor's clock (decision D13). Virtual because a TTL test cannot
    /// depend on Sleep: the suite overrides this and moves time by hand, which
    /// makes expiration deterministic and instantaneous. In production it is
    /// PcTickMs.
    function NowTick: UInt64; virtual;
    /// One actor round. Public only for the work item; never call it from outside.
    procedure RunActor;
    /// Runs ONE command, always on the actor thread and outside the lock.
    /// Virtual only so the tests can peek from inside the actor (that is how
    /// the synchronous command guard is exercised).
    procedure ExecuteCommand(ACmd: TAMQPQueueCommand); virtual;
  public
    /// AMaxLength = 0: no limit (D7's default). > 0: when exceeded, drops
    /// from the HEAD and counts in Stats.DroppedCount.
    /// APool = nil: PcPool, the process pool (the letter of D2). The broker does
    /// NOT use nil: TAMQPServer passes its own pool since F8, because PcPool has
    /// a cap and is shared with consumer callbacks -- of this client and of the
    /// other *-faa libs -- that may block on I/O; measured in
    /// TPoolIsolationTests, the actor waited there until the 15 s timeout.
    constructor Create(const AName: string; AMaxLength: Integer = 0;
      APool: TPcThreadPool = nil); overload;
    /// With the x-arguments already parsed (WS2). This is how the engine
    /// creates the queue; the overload above exists for whoever has no policy
    /// at all (pure state tests) and is equivalent to passing TAMQPQueuePolicy.Empty.
    constructor Create(const AName: string; const APolicy: TAMQPQueuePolicy;
      AMaxLength: Integer = 0; APool: TPcThreadPool = nil); overload;
    destructor Destroy; override;

    // --- asynchronous commands ---
    /// The queue takes ITS OWN reference (AddRef): whoever publishes to N
    /// queues still owns its reference and Releases it after the fan-out.
    /// On an already stopped queue, the message is dropped and counted in
    /// DroppedCount (does not raise -- a publish does not fail because of a
    /// teardown race).
    /// APriority and AMessageTtlMs come DECODED by the publisher thread (the
    /// message keeps the raw header -- decision D1 --, so whoever reads the
    /// properties is whoever still has the channel's live table at hand). The
    /// queue limits the priority to its own cap and combines the message's TTL
    /// with the queue's. AMessageTtlMs < 0 = the message carried no 'expiration'.
    /// AJournalId/AContentId come from whoever WROTE the placement record --
    /// the publisher thread (D24) --, and are not generated here: the queue is
    /// the last link, not the first. Zero = nothing was persisted.
    procedure PostMessage(AMessage: TAMQPMessage; APriority: Byte = 0;
      AMessageTtlMs: Int64 = -1; AJournalId: UInt64 = 0;
      AContentId: UInt64 = 0; ARestored: Boolean = False);
    /// The queue becomes the owner of AConsumer.
    procedure PostAddConsumer(AConsumer: TAMQPServerConsumer);
    procedure PostRemoveConsumer(AChannelId: NativeUInt;
      const AConsumerTag: string);
    /// A channel (or connection) went down: removes all its consumers and returns
    /// its unacked messages to the queue, with redelivered. WS7 uses it.
    procedure PostRemoveChannel(AChannelId: NativeUInt);
    /// Re-examines delivery with no other event. It exists for D3's degraded
    /// case: consumers that refused (outbound queue full) and no ack arriving --
    /// typical of a no-ack consumer slower than the producer. In that case
    /// nothing else would wake the actor, so the broker's monitor thread nudges
    /// the queues periodically. On the normal path delivery comes from the
    /// enqueue/ack itself, without depending on this tick.
    procedure PostDeliverTick;
    /// Marks the queue as USED now (WS8). The declare/redeclare calls this --
    /// it is one of the three events that restart the x-expires clock.
    procedure PostTouch;
    procedure PostAck(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple: Boolean);
    procedure PostNack(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple, ARequeue: Boolean);
    procedure PostReject(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      ARequeue: Boolean);

    // --- synchronous commands (raise EAMQPQueueActor if the queue has already
    //     stopped, if they are posted from inside the actor itself, or on timeout) ---
    /// SYNCHRONOUS enqueue, for a queue declared with x-overflow reject-publish
    /// (decision D15). Returns False when the queue is full and the message was
    /// REJECTED -- it is the only path in which a queue says no to a publish.
    ///
    /// Why synchronous: the publisher needs the answer BEFORE the Basic.Ack, and
    /// PostMessage is asynchronous and returns nothing. Blocking the publisher
    /// here is not a side effect -- it is literally the semantics reject-publish
    /// asks for. The command does no I/O, so the 15 s cap is a net against our
    /// own bug, not against a slow peer.
    ///
    /// It is the ONLY point of Phase 3 designed to be replaced: when Phase 4
    /// has the pending-confirms registry, this becomes asynchronous.
    function TryPostMessage(AMessage: TAMQPMessage; APriority: Byte = 0;
      AMessageTtlMs: Int64 = -1; AJournalId: UInt64 = 0;
      AContentId: UInt64 = 0): Boolean;

    /// Basic.Get. ADeliveryTag is the tag the CHANNEL will use (the queue does
    /// not generate tags -- they are monotonic per channel). With ANoAck=False
    /// the message goes to the unacked ones. The reference returned in AMessage
    /// belongs to the CALLER (Release it after use). False = empty queue.
    function Get(AChannelId: NativeUInt; ADeliveryTag: UInt64; ANoAck: Boolean;
      out AMessage: TAMQPMessage; out ARedelivered: Boolean): Boolean;
    /// Queue.Purge: drops the ready messages (does not touch the unacked ones)
    /// and returns how many there were.
    function Purge: Integer;
    function Stats: TAMQPQueueStats;
    /// Queue.Delete. On amqqdOk the queue is left stopped and empty -- the
    /// owner still has to free the object.
    function Delete(AIfUnused, AIfEmpty: Boolean;
      out AMessageCount: Integer): TAMQPQueueDeleteResult;

    /// Stops the actor and frees the stock. Idempotent. Called by the destructor.
    /// Raises EAMQPQueueActor if the actor does not leave the loop within the
    /// deadline -- in that case NOTHING is freed (leaking is better than freeing
    /// from under an actor that is still running).
    procedure Stop;

    property Name: string read FName;
    property MaxLength: Integer read FMaxLength;
    /// Priority cap in force (0 = queue without priority).
    property MaxPriority: Integer read FMaxPriority;

    /// Effective priority of this message IN THIS queue (limited to the cap) and
    /// effective TTL in ms (the smaller of the queue's and the message's; -1 = no deadline).
    ///
    /// These are PURE reads of the policy, which is immutable after creation,
    /// and so they may be called from OUTSIDE the actor -- and they must be:
    /// whoever writes the placement record is the publisher thread (D24), and it
    /// has to write the same numbers the actor will keep.
    function EffectivePriority(APriority: Byte): Byte;
    function EffectiveTtl(AMessageTtlMs: Int64): Int64;
    /// Who republishes dead messages. nil = no dead-letter (the message is
    /// simply dropped, as in WS4). The engine injects it at creation.
    property DeadLetterSink: IAMQPDeadLetterSink read FDeadLetterSink
      write FDeadLetterSink;
    property Policy: TAMQPQueuePolicy read FPolicy;

    /// Durability journal (Phase 4). nil = nothing from this queue goes to disk.
    /// The queue does NOT own it; the engine injects it at creation.
    property Journal: TAMQPJournal read FJournal write FJournal;
    /// Where the actor emits observability events. nil = off (D29), and then
    /// not even the record is built. Seeded by the engine.
    property Events: IAMQPEventSink read FEvents write FEvents;
    /// vhost this queue lives in -- it goes into the placement records, because
    /// the queue alone is not an identity (two vhosts may both have 'q').
    property VHostName: string read FVHostName write FVHostName;
    /// D20: only a DURABLE queue writes. A transient queue does not touch the
    /// journal, and its hot path stays identical to Phase 3's.
    property Durable: Boolean read FDurable write FDurable;
  end;

implementation

{ TAMQPServerConsumer }

constructor TAMQPServerConsumer.Create(const AConsumerTag: string;
  ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget);
begin
  inherited Create;
  if ATarget = nil then
    raise EAMQPQueueActor.CreateFmt('consumer "%s" without delivery target',
      [AConsumerTag]);
  FConsumerTag := AConsumerTag;
  FChannelId := ATarget.ChannelId;
  FNoAck := ANoAck;
  FExclusive := AExclusive;
  FTarget := ATarget;
end;

{ TAMQPQueueCommand }

constructor TAMQPQueueCommand.Create(AKind: TAMQPQueueCmdKind; ASync: Boolean);
begin
  inherited Create;
  FRefCount := 1;
  FKind := AKind;
  if ASync then
    FEvent := TEvent.Create(nil, True, False, ''); // manual-reset
end;

destructor TAMQPQueueCommand.Destroy;
begin
  // Drop path (a command that never ran, or a synchronous one that exceeded the
  // timeout after the actor filled in the result): the message references
  // left over are ours and must leave from here.
  if Msg <> nil then
    Msg.Release;
  if ResMsg <> nil then
    ResMsg.Release;
  Consumer.Free;
  FEvent.Free;
  inherited;
end;

function TAMQPQueueCommand.GetIsSync: Boolean;
begin
  Result := FEvent <> nil;
end;

function TAMQPQueueCommand.AddRef: Integer;
begin
  Result := PcAtomicInc(FRefCount);
end;

function TAMQPQueueCommand.Release: Integer;
begin
  Result := PcAtomicDec(FRefCount);
  if Result = 0 then
    Free;
end;

procedure TAMQPQueueCommand.Signal;
begin
  if FEvent <> nil then
    FEvent.SetEvent;
end;

function TAMQPQueueCommand.WaitFor(ATimeoutMs: Cardinal): Boolean;
begin
  Result := (FEvent <> nil) and (FEvent.WaitFor(ATimeoutMs) = wrSignaled);
end;

{ TAMQPQueueWork }

constructor TAMQPQueueWork.Create(AQueue: TAMQPServerQueue);
begin
  inherited Create;
  FQueue := AQueue;
end;

procedure TAMQPQueueWork.Execute;
begin
  FQueue.RunActor;
end;

{ TAMQPServerQueue }

constructor TAMQPServerQueue.Create(const AName: string; AMaxLength: Integer;
  APool: TPcThreadPool);
begin
  Create(AName, TAMQPQueuePolicy.Empty, AMaxLength, APool);
end;

constructor TAMQPServerQueue.Create(const AName: string;
  const APolicy: TAMQPQueuePolicy; AMaxLength: Integer;
  APool: TPcThreadPool);
var
  I: Integer;
begin
  inherited Create;
  FName := AName;
  FPolicy := APolicy;
  // An absent MaxPriority (-1) becomes 0: a single bucket, which is the usual queue.
  if APolicy.MaxPriority > 0 then
    FMaxPriority := APolicy.MaxPriority
  else
    FMaxPriority := 0;
  // CAP COMPOSITION (WS6): the broker's global cap
  // (TAMQPServer.MaxQueueLength, decision D7) and the QUEUE's x-max-length are
  // both an upper bound, so the SMALLER wins. The reason is one of role, not of
  // arithmetic: the global one is a safety net of the HOST PROCESS -- the
  // broker runs inside the user's app --, and an argument coming from the
  // client must not loosen a safety net. 0/absent = no cap on that side, and
  // then the other one applies.
  if AMaxLength < 0 then
    AMaxLength := 0;
  FMaxLength := AMaxLength;
  if APolicy.MaxLength >= 0 then
    if (FMaxLength = 0) or (APolicy.MaxLength < FMaxLength) then
      FMaxLength := Integer(APolicy.MaxLength);
  // There is no global BYTE cap: x-max-length-bytes only comes from the queue.
  FMaxBytes := APolicy.MaxLengthBytes;
  if APool <> nil then
    FPool := APool
  else
    FPool := PcPool;
  FMon := TPcMonitor.Create;
  FMailbox := TQueue<TAMQPQueueCommand>.Create;
  FLastUsed := NowTick; // the queue is born "used now"
  SetLength(FStock, FMaxPriority + 1);
  SetLength(FRequeued, FMaxPriority + 1);
  for I := 0 to FMaxPriority do
  begin
    FStock[I] := TQueue<TAMQPQueueEntry>.Create;
    FRequeued[I] := TQueue<TAMQPQueueEntry>.Create;
  end;
  FUnacked := TList<TAMQPUnackedEntry>.Create;
  FConsumers := TObjectList<TAMQPServerConsumer>.Create(True);
end;

destructor TAMQPServerQueue.Destroy;
var
  I: Integer;
begin
  try
    Stop;
  except
    // Stuck actor: Stop has already freed nothing, on purpose. Leaking is the
    // lesser evil -- freeing the structures from under a running actor would be
    // exactly the double free Phase 1 already paid for once. And exceptions do
    // not escape destructors.
    on E: Exception do
      ;
  end;
  FMailbox.Free;
  for I := 0 to High(FStock) do
  begin
    FStock[I].Free;
    FRequeued[I].Free;
  end;
  FUnacked.Free;
  FConsumers.Free;
  FMon.Free;
  inherited;
end;

procedure TAMQPServerQueue.ActorRoundEnding;
begin
  // no-op: see the declaration's comment.
end;

{ --- command mailbox --- }

// Under FMon. Schedules a round if none is scheduled yet.
procedure TAMQPServerQueue.ScheduleLocked;
begin
  if FScheduled or FStopping then
    Exit;
  FScheduled := True;
  FPool.Queue(TAMQPQueueWork.Create(Self));
end;

// Asynchronous: enqueues and returns. The caller's reference becomes the mailbox's.
procedure TAMQPServerQueue.Post(ACmd: TAMQPQueueCommand);
var
  LDiscard: Boolean;
begin
  LDiscard := False;
  FMon.Enter;
  try
    if FStopping then
    begin
      // Stopped/deleted queue: drops silently, but COUNTS (nothing vanishes
      // without a metric). The actor no longer runs, so nobody competes with
      // this write made under the lock.
      if ACmd.Kind = amqqcEnqueue then
        Inc(FDropped);
      LDiscard := True;
    end
    else
    begin
      FMailbox.Enqueue(ACmd);
      ScheduleLocked;
    end;
  finally
    FMon.Leave;
  end;
  if LDiscard then
    ACmd.Release; // outside the lock: the destructor releases message/consumer
end;

// Synchronous: enqueues, waits for the event and returns with the result filled in.
// The CALLER's reference stays with it (the mailbox gets its own).
procedure TAMQPServerQueue.PostSync(ACmd: TAMQPQueueCommand);
begin
  FMon.Enter;
  try
    if (FActorThread <> 0) and (FActorThread = TThread.CurrentThread.ThreadID) then
      raise EAMQPQueueActor.CreateFmt(
        'synchronous command (%d) posted from inside the queue actor "%s"'
        + ' -- the actor would wait for itself', [Ord(ACmd.Kind), FName]);
    if FStopping then
      raise EAMQPQueueActor.CreateFmt(
        'synchronous command (%d) posted to an already stopped queue ("%s")',
        [Ord(ACmd.Kind), FName]);
    ACmd.AddRef; // the mailbox has its own reference
    FMailbox.Enqueue(ACmd);
    ScheduleLocked;
  finally
    FMon.Leave;
  end;

  if not ACmd.WaitFor(AMQP_QUEUE_SYNC_TIMEOUT_MS) then
    raise EAMQPQueueActor.CreateFmt(
      'timeout of %d ms waiting for synchronous command (%d) of queue "%s"'
      + ' -- the actor was not scheduled; check thread pool saturation',
      [AMQP_QUEUE_SYNC_TIMEOUT_MS, Ord(ACmd.Kind), FName]);
end;


// One actor round, on a pool worker.
procedure TAMQPServerQueue.RunActor;
var
  LCmd: TAMQPQueueCommand;
  LCount: Integer;
begin
  FMon.Enter;
  FActorThread := TThread.CurrentThread.ThreadID;
  FMon.Leave;
  try
    LCount := 0;
    while True do
    begin
      LCmd := nil;
      FMon.Enter;
      if (not FStopping) and (LCount < AMQP_QUEUE_CMD_BATCH)
        and (FMailbox.Count > 0) then
        LCmd := FMailbox.Dequeue;
      FMon.Leave;
      if LCmd = nil then
        Break;
      try
        try
          ExecuteCommand(LCmd);
        except
          // A command should never raise; if it does, whoever waits on the
          // other side cannot be left stuck forever -- we signal anyway, with
          // ResOk=False, and move on.
          on E: Exception do
            LCmd.ResOk := False;
        end;
      finally
        LCmd.Signal;
        LCmd.Release; // the mailbox's reference
      end;
      Inc(LCount);
    end;
  finally
    try
      ActorRoundEnding; // test seam (no-op in production)
    except
      on E: Exception do
        ; // a hook that raises must not leave FScheduled stuck
    end;
    FMon.Enter;
    try
      FActorThread := 0;
      if (not FStopping) and (FMailbox.Count > 0) then
        // Round cap exceeded (or a command arrived in the window): stays
        // scheduled, only the work item changes -- FScheduled stays True.
        FPool.Queue(TAMQPQueueWork.Create(Self))
      else
      begin
        FScheduled := False;
        FMon.PulseAll; // Stop waits for exactly this
      end;
    finally
      FMon.Leave;
    end;
  end;
end;

procedure TAMQPServerQueue.Stop;
var
  LDeadline: UInt64;
begin
  FMon.Enter;
  try
    if FStopped then
      Exit;
    FStopping := True; // nobody posts or schedules anymore
    LDeadline := PcTickMs + AMQP_QUEUE_STOP_TIMEOUT_MS;
    while FScheduled and (PcTickMs < LDeadline) do
      FMon.Wait(100);
    if FScheduled then
      raise EAMQPQueueActor.CreateFmt(
        'queue actor "%s" did not finish within %d ms -- state NOT released',
        [FName, AMQP_QUEUE_STOP_TIMEOUT_MS]);
    // From here on we are the only executor: the actor left the loop and no
    // new scheduling is possible.
    DrainState;
    FStopped := True;
  finally
    FMon.Leave;
  end;
end;

// Frees everything. Only Stop calls it, with the actor already stopped.
procedure TAMQPServerQueue.DrainState;
var
  LCmd: TAMQPQueueCommand;
  LEntry: TAMQPQueueEntry;
  I: Integer;
begin
  while FMailbox.Count > 0 do
  begin
    LCmd := FMailbox.Dequeue;
    LCmd.Signal; // releases whoever is waiting (it will see ResOk=False)
    LCmd.Release;
  end;
  for I := 0 to High(FStock) do
  begin
    while FRequeued[I].Count > 0 do
    begin
      LEntry := FRequeued[I].Dequeue;
      LEntry.Msg.Release;
    end;
    while FStock[I].Count > 0 do
    begin
      LEntry := FStock[I].Dequeue;
      LEntry.Msg.Release;
    end;
  end;
  FReadyBytes := 0;
  for I := 0 to FUnacked.Count - 1 do
    FUnacked[I].Msg.Release;
  FUnacked.Clear;
  FConsumers.Clear; // OwnsObjects
end;

{ --- asynchronous API --- }

procedure TAMQPServerQueue.PostMessage(AMessage: TAMQPMessage;
  APriority: Byte; AMessageTtlMs: Int64; AJournalId, AContentId: UInt64;
  ARestored: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcEnqueue, False);
  AMessage.AddRef; // the QUEUE's reference; the publisher keeps its own
  LCmd.Msg := AMessage;
  LCmd.Priority := APriority;
  LCmd.MessageTtlMs := AMessageTtlMs;
  LCmd.JournalId := AJournalId;
  LCmd.ContentId := AContentId;
  LCmd.Restored := ARestored;
  Post(LCmd);
end;

function TAMQPServerQueue.TryPostMessage(AMessage: TAMQPMessage;
  APriority: Byte; AMessageTtlMs: Int64;
  AJournalId, AContentId: UInt64): Boolean;
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcEnqueueSync, True);
  try
    AMessage.AddRef; // the QUEUE's reference, as in PostMessage
    LCmd.Msg := AMessage;
    LCmd.Priority := APriority;
    LCmd.MessageTtlMs := AMessageTtlMs;
    LCmd.JournalId := AJournalId;
    LCmd.ContentId := AContentId;
    PostSync(LCmd);
    Result := LCmd.ResOk;
  finally
    // If the actor refused, the reference this command carried stays in it
    // and leaves in the destructor -- that is why the command's Release suffices.
    LCmd.Release;
  end;
end;

procedure TAMQPServerQueue.PostAddConsumer(AConsumer: TAMQPServerConsumer);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcAddConsumer, False);
  LCmd.Consumer := AConsumer;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostRemoveConsumer(AChannelId: NativeUInt;
  const AConsumerTag: string);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcRemoveConsumer, False);
  LCmd.ChannelId := AChannelId;
  LCmd.ConsumerTag := AConsumerTag;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostRemoveChannel(AChannelId: NativeUInt);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcRemoveChannel, False);
  LCmd.ChannelId := AChannelId;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostTouch;
begin
  Post(TAMQPQueueCommand.Create(amqqcTouch, False));
end;

procedure TAMQPServerQueue.PostDeliverTick;
begin
  Post(TAMQPQueueCommand.Create(amqqcDeliverTick, False));
end;

procedure TAMQPServerQueue.PostAck(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; AMultiple: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcAck, False);
  LCmd.ChannelId := AChannelId;
  LCmd.DeliveryTag := ADeliveryTag;
  LCmd.Multiple := AMultiple;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostNack(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; AMultiple, ARequeue: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcNack, False);
  LCmd.ChannelId := AChannelId;
  LCmd.DeliveryTag := ADeliveryTag;
  LCmd.Multiple := AMultiple;
  LCmd.Requeue := ARequeue;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostReject(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; ARequeue: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcReject, False);
  LCmd.ChannelId := AChannelId;
  LCmd.DeliveryTag := ADeliveryTag;
  LCmd.Requeue := ARequeue;
  Post(LCmd);
end;

{ --- synchronous API --- }

function TAMQPServerQueue.Get(AChannelId: NativeUInt; ADeliveryTag: UInt64;
  ANoAck: Boolean; out AMessage: TAMQPMessage;
  out ARedelivered: Boolean): Boolean;
var
  LCmd: TAMQPQueueCommand;
begin
  AMessage := nil;
  ARedelivered := False;
  LCmd := TAMQPQueueCommand.Create(amqqcGet, True);
  try
    LCmd.ChannelId := AChannelId;
    LCmd.DeliveryTag := ADeliveryTag;
    LCmd.NoAck := ANoAck;
    PostSync(LCmd);
    Result := LCmd.ResOk;
    AMessage := LCmd.ResMsg;
    LCmd.ResMsg := nil; // the reference now belongs to the caller
    ARedelivered := LCmd.ResRedelivered;
  finally
    LCmd.Release;
  end;
end;

function TAMQPServerQueue.Purge: Integer;
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcPurge, True);
  try
    PostSync(LCmd);
    Result := LCmd.ResCount;
  finally
    LCmd.Release;
  end;
end;

function TAMQPServerQueue.Stats: TAMQPQueueStats;
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcStats, True);
  try
    PostSync(LCmd);
    Result := LCmd.ResStats;
  finally
    LCmd.Release;
  end;
end;

function TAMQPServerQueue.Delete(AIfUnused, AIfEmpty: Boolean;
  out AMessageCount: Integer): TAMQPQueueDeleteResult;
var
  LCmd: TAMQPQueueCommand;
begin
  AMessageCount := 0;
  LCmd := TAMQPQueueCommand.Create(amqqcDelete, True);
  try
    LCmd.IfUnused := AIfUnused;
    LCmd.IfEmpty := AIfEmpty;
    PostSync(LCmd);
    Result := LCmd.ResDelete;
    AMessageCount := LCmd.ResCount;
  finally
    LCmd.Release;
  end;
  if Result = amqqdOk then
    Stop; // a deleted queue does not serve again
end;

{ --- state: actor helpers --- }

function TAMQPServerQueue.ReadyCount: Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := 0 to High(FStock) do
    Inc(Result, FRequeued[I].Count + FStock[I].Count);
end;

// Takes the next ready one: from the HIGHEST priority bucket that has something,
// and within it the returned ones (requeue) first, in the original order. FIFO
// within a level is a tested invariant -- priority reorders between levels,
// never within one.
function TAMQPServerQueue.TakeReady(out AEntry: TAMQPQueueEntry): Boolean;
var
  I: Integer;
begin
  for I := High(FStock) downto 0 do
  begin
    if FRequeued[I].Count > 0 then
      AEntry := FRequeued[I].Dequeue
    else if FStock[I].Count > 0 then
      AEntry := FStock[I].Dequeue
    else
      Continue;
    TrackBytes(AEntry, -1); // TakeReady is the common exit path
    Exit(True);
  end;
  Result := False;
end;

// Peeks at the next ready one WITHOUT taking it from the stock -- delivery only
// takes it after the target accepts the frame batch; if it refuses, the message
// must stay exactly where it was. Same order as TakeReady.
function TAMQPServerQueue.PeekReady(out AEntry: TAMQPQueueEntry): Boolean;
var
  I: Integer;
begin
  for I := High(FStock) downto 0 do
  begin
    if FRequeued[I].Count > 0 then
      AEntry := FRequeued[I].Peek
    else if FStock[I].Count > 0 then
      AEntry := FStock[I].Peek
    else
      Continue;
    Exit(True);
  end;
  Result := False;
end;

function TAMQPServerQueue.NowTick: UInt64;
begin
  Result := PcTickMs;
end;

function TAMQPServerQueue.LimitPriority(APriority: Byte): Byte;
begin
  if APriority > FMaxPriority then
    Result := Byte(FMaxPriority)
  else
    Result := APriority;
end;

// Combines the QUEUE's TTL (x-message-ttl) with the MESSAGE's ('expiration'):
// the SMALLER wins, which is what RabbitMQ's spec actually does. With both
// absent, the message never expires (0).
function TAMQPServerQueue.EffectiveTtl(AMessageTtlMs: Int64): Int64;
begin
  Result := -1;
  if FPolicy.MessageTtlMs >= 0 then
    Result := FPolicy.MessageTtlMs;
  if AMessageTtlMs >= 0 then
    if (Result < 0) or (AMessageTtlMs < Result) then
      Result := AMessageTtlMs;
end;

function TAMQPServerQueue.EffectivePriority(APriority: Byte): Byte;
begin
  Result := LimitPriority(APriority);
end;

function TAMQPServerQueue.CalculateExpiration(AMessageTtlMs: Int64): UInt64;
var
  LTtl: Int64;
begin
  // A single implementation of the "smaller wins" rule: the record the
  // publisher thread writes must say EXACTLY what the actor will keep, and two
  // copies of the rule would be two chances to diverge.
  LTtl := EffectiveTtl(AMessageTtlMs);
  if LTtl < 0 then
    Exit(0);
  Result := NowTick + UInt64(LTtl);
end;

// SINGLE exit point of an expired message. In WS4 it only drops the reference
// and counts; WS5 (dead-lettering) swaps its body for "rewrite the header with
// x-death and republish to the DLX". It exists as its own method precisely so
// that swap is local.
procedure TAMQPServerQueue.DiscardExpired(AEntry: TAMQPQueueEntry);
begin
  Inc(FExpired);
  // BEFORE DeadLetterWith, which consumes the entry's reference: after it
  // AEntry.Msg may already have been freed.
  EmitMessageEvent(seMessageExpired, AEntry, AMQP_DEATH_EXPIRED);
  DeadLetterWith(AEntry, AMQP_DEATH_EXPIRED);
end;

// Runs ON THE ACTOR THREAD, and so is only CPU (rewriting the header) plus a
// post to another queue's mailbox (D17). Nothing here waits for I/O.
function TAMQPServerQueue.DeadLetterWith(AEntry: TAMQPQueueEntry;
  const AReason: string): Boolean;
var
  LEd: TAMQPHeaderEditor;
  LNew: TAMQPMessage;
  LDlx, LKey, LOrigExpiration: string;
  LPayload: TBytes;
  LEv: TAMQPServerEvent;
begin
  Result := False;
  // The outer try/FINALLY exists for a specific reason: `Exit` inside a
  // try/EXCEPT leaves the FUNCTION and skips the code after the block. The
  // first version here freed the reference AFTER a try/except that had an Exit
  // on the "no DLX" path -- and on that path the Release never ran. A WS3 test
  // caught it (Nack_WithoutRequeue_Drops) and heaptrc confirmed it with 21
  // leaked blocks.
  try
    try
      if (FDeadLetterSink = nil) or (not FPolicy.HasDeadLetterExchange) then
        Exit;

      LEd := nil;
      LNew := nil;
      try
        LEd := TAMQPHeaderEditor.Create(AEntry.Msg.HeaderPayload);

        // Cycle guard (D14) BEFORE touching anything: if it has already gone
        // around too many times, the message dies here and does not become new traffic.
        if AmqpDeathHops(LEd.Headers) >= AMQP_MAX_DEADLETTER_HOPS then
          Exit;

        // A message killed by TTL cannot carry the SAME deadline to the DLQ:
        // it would die on arrival and the retry-with-delay pattern would not
        // work. The old value is recorded in the x-death entry.
        LOrigExpiration := LEd.TakeExpiration;
        AmqpAddDeath(LEd, FName, AReason, AEntry.Msg.Exchange,
          AEntry.Msg.RoutingKey, LOrigExpiration);
        LPayload := LEd.BuildPayload(AEntry.Msg.BodySize);

        LDlx := FPolicy.DeadLetterExchange;
        // Without x-dead-letter-routing-key, the ORIGINAL key is kept -- that
        // is what makes a topic DLX keep routing by the same criterion.
        if FPolicy.HasDeadLetterRoutingKey then
          LKey := FPolicy.DeadLetterRoutingKey
        else
          LKey := AEntry.Msg.RoutingKey;

        LNew := AmqpDeriveMessage(AEntry.Msg, LPayload, LDlx, LKey);
        FDeadLetterSink.DeadLetter(LDlx, LKey, LNew, AEntry.Priority,
          AEntry.ContentId);
        Inc(FDeadLettered);
        // Only here, and not at the top of DeadLetterWith: the event says the
        // message WAS republished to the DLX. A queue without a DLX, or a
        // republish that failed, is a drop -- and goes out as seMessageDropped, in the caller.
        if WantsEvent(seMessageDeadLettered) then
        begin
          LEv := AmqpNewEvent(seMessageDeadLettered);
          LEv.VHost := FVHostName;
          LEv.QueueName := FName;
          LEv.Priority := AEntry.Priority;
          LEv.Redelivered := AEntry.Redelivered;
          LEv.Reason := AReason;
          LEv.MessageSize := UInt64(AEntry.Msg.BodySize);
          // Where it WENT -- that is what distinguishes this event from a drop.
          LEv.ExchangeName := LDlx;
          LEv.RoutingKey := LKey;
          FEvents.Emit(LEv);
        end;
        Result := True;
      finally
        if LNew <> nil then
          LNew.Release; // the deriver's reference; the sink took its own
        LEd.Free;
      end;
    except
      // Malformed header, a DLX that vanished, whatever: a death that cannot be
      // republished becomes a drop. It must NOT bring the actor down --
      // raising here would kill the round and leave the queue half-processed.
      on E: Exception do
        Result := False;
    end;
  finally
    // D23, ORDER: the new placement has already been written in there (by the
    // sink, which runs on this same thread), and only NOW is the old one
    // retired. Failing in the middle produces a DUPLICATE, never a loss -- and
    // at-least-once already admits duplicates. Holds for all three death
    // reasons, because they all go through here.
    Retire(AEntry.JournalId);
    // The entry's reference is ALWAYS consumed, whether it republished or not.
    AEntry.Msg.Release;
  end;
end;

// Expires what is past its deadline, FROM THE HEAD of each bucket backward.
//
// DELIBERATE LIMITATION, the same as RabbitMQ's: with PER-MESSAGE 'expiration'
// the entries are not in expiration order, so a short-deadline message behind
// a long-deadline one is only collected when it reaches the head. Sweeping the
// whole queue on every tick would be O(n) per tick to fix a rare case. With
// the QUEUE's x-message-ttl (the common case) there is no problem at all: all
// have the same TTL and FIFO order already is expiration order.
procedure TAMQPServerQueue.ExpireMessages;
var
  I: Integer;
  LNow: UInt64;
  LEntry: TAMQPQueueEntry;

  function HeadExpired(AQueue: TQueue<TAMQPQueueEntry>): Boolean;
  begin
    Result := (AQueue.Count > 0) and (AQueue.Peek.ExpiresAt > 0)
      and (AQueue.Peek.ExpiresAt <= LNow);
  end;

begin
  // D13: a queue that never received a message with a deadline pays nothing for this.
  if not FHasExpiration then
    Exit;
  LNow := NowTick;
  for I := 0 to High(FStock) do
  begin
    while HeadExpired(FRequeued[I]) do
    begin
      LEntry := FRequeued[I].Dequeue;
      TrackBytes(LEntry, -1);
      DiscardExpired(LEntry);
    end;
    while HeadExpired(FStock[I]) do
    begin
      LEntry := FStock[I].Dequeue;
      TrackBytes(LEntry, -1);
      DiscardExpired(LEntry);
    end;
  end;
end;

// Next consumer in the round-robin that has not refused in this round yet.
// nil = all refused (or there is no consumer).
function TAMQPServerQueue.NextEligible: TAMQPServerConsumer;
var
  I, N, LIdx: Integer;
begin
  Result := nil;
  N := FConsumers.Count;
  if N = 0 then
    Exit;
  if (FRoundRobin < 0) or (FRoundRobin >= N) then
    FRoundRobin := 0;
  for I := 0 to N - 1 do
  begin
    LIdx := (FRoundRobin + I) mod N;
    if not FConsumers[LIdx].Suspended then
    begin
      Result := FConsumers[LIdx];
      // The pointer advances even when this consumer ends up refusing: whoever
      // cannot receive has no reason to hold its turn.
      FRoundRobin := (LIdx + 1) mod N;
      Exit;
    end;
  end;
end;

// One delivery ROUND. Runs on the actor thread, outside the mailbox lock, and
// NEVER blocks (D2): the target returns False instead of waiting, and the
// consumer that refused leaves the round-robin until the end of this round
// (D3) -- the others keep receiving, which is exactly the head-of-line D3 avoids.
procedure TAMQPServerQueue.DeliverPending;
var
  I: Integer;
  LCons: TAMQPServerConsumer;
  LEntry: TAMQPQueueEntry;
  LUnacked: TAMQPUnackedEntry;
  LTag: UInt64;
  LDelivEv: TAMQPServerEvent;
begin
  // BEFORE anything else, and BEFORE the early exit for lack of a consumer:
  // a queue with no consumer at all must also expire on time (D13), and it is
  // the monitor thread's tick that wakes it.
  ExpireMessages;

  // A channel/connection that has already died does not come back: the
  // consumer leaves here. Its unacked messages are returned by
  // amqqcRemoveChannel, which the connection teardown posts (WS7).
  for I := FConsumers.Count - 1 downto 0 do
    if not FConsumers[I].Target.IsAlive then
      FConsumers.Delete(I);
  if FConsumers.Count = 0 then
    Exit;

  for I := 0 to FConsumers.Count - 1 do
    FConsumers[I].Suspended := False; // new round: everyone eligible again

  while ReadyCount > 0 do
  begin
    LCons := NextEligible;
    if LCons = nil then
      Break; // all refused in this round
    if not PeekReady(LEntry) then
      Break;

    if LCons.Target.TryDeliver(LCons.ConsumerTag, FName, LCons.NoAck,
      LEntry.Redelivered, LEntry.Msg, LTag) then
    begin
      TakeReady(LEntry); // only leaves the stock after the batch is accepted
      Inc(FDelivered);
      if WantsEvent(seMessageDelivered) then
      begin
        LDelivEv := AmqpNewEvent(seMessageDelivered);
        LDelivEv.VHost := FVHostName;
        LDelivEv.QueueName := FName;
        LDelivEv.ConsumerTag := LCons.ConsumerTag;
        LDelivEv.DeliveryTag := LTag;
        LDelivEv.Priority := LEntry.Priority;
        LDelivEv.Redelivered := LEntry.Redelivered;
        LDelivEv.MessageSize := UInt64(LEntry.Msg.BodySize);
        LDelivEv.ExchangeName := LEntry.Msg.Exchange;
        LDelivEv.RoutingKey := LEntry.Msg.RoutingKey;
        // NoAck: the delivery already ends the message's life here.
        if LCons.NoAck then
          LDelivEv.Reason := 'no-ack';
        FEvents.Emit(LDelivEv);
      end;
      if LCons.NoAck then
      begin
        // Without an ack there is no way back: the placement ends here, on disk too.
        Retire(LEntry.JournalId);
        LEntry.Msg.Release; // the queue drops its reference now
      end
      else
      begin
        // The stock's reference passes to the unacked ones.
        //
        // Priority and ExpiraEm MUST come along: they are what the requeue uses
        // to return the message to the right bucket with the original deadline
        // (D12/D13). They were missing here -- the Basic.Get path already copied
        // them, this one did not --, and the local record held stack garbage.
        // Effect: every message delivered to a CONSUMER and then returned
        // (Nack/Reject with requeue, or a channel going down) went back to an
        // arbitrary bucket with an arbitrary deadline. It stayed invisible
        // because the requeue tests used Basic.Get, and because a queue without
        // priority has a single bucket and a queue without TTL does not even
        // sweep deadlines.
        LUnacked.ChannelId := LCons.Target.ChannelId;
        LUnacked.DeliveryTag := LTag;
        LUnacked.Msg := LEntry.Msg;
        LUnacked.Priority := LEntry.Priority;
        LUnacked.ExpiresAt := LEntry.ExpiresAt;
        LUnacked.JournalId := LEntry.JournalId;
        LUnacked.ContentId := LEntry.ContentId;
        FUnacked.Add(LUnacked);
      end;
    end
    else
      LCons.Suspended := True;
  end;
end;

// Returns to the HEAD of the queue, marked as redelivered (spec: Basic.Nack/
// Reject with requeue return the message to its original position).
// Returns to the HEAD OF ITS OWN BUCKET, with the original deadline. Going back
// to the absolute head would let a low-priority message jump the queue of the
// high ones; resetting the deadline would give extra life to one that should already have died.
procedure TAMQPServerQueue.RequeueFront(AMsg: TAMQPMessage; APriority: Byte;
  AExpiresAt: UInt64; AJournalId, AContentId: UInt64);
var
  LEntry: TAMQPQueueEntry;
begin
  LEntry.Msg := AMsg;
  LEntry.Redelivered := True;
  LEntry.Priority := LimitPriority(APriority);
  LEntry.ExpiresAt := AExpiresAt;
  // The placement is NOT retired on a requeue: it stays the same on disk, it
  // only went back to the stock. That is what makes D27 ("unacked comes back
  // ready") come for free in the replay -- there is no record at all to write.
  LEntry.JournalId := AJournalId;
  LEntry.ContentId := AContentId;
  TrackBytes(LEntry, +1);
  FRequeued[LEntry.Priority].Enqueue(LEntry);
end;

// D7: global memory cap. Drops from the HEAD (the oldest) -- the broker runs
// INSIDE the user's app and a queue without a consumer must not bring down the
// host process. Counts only the ready ones (the unacked ones are already with
// a consumer).
// The byte accumulator exists because summing the bodies on every check would
// be O(n) per publish. The price is that EVERY entry into and exit from the
// stock must go through here -- a forgotten path gives no symptom at the time,
// just a cap that stops holding.
procedure TAMQPServerQueue.Retire(AJournalId: UInt64);
var
  LRecs: TAMQPJournalRecords;
  LDeq: TAMQPRecDequeue;
begin
  if (AJournalId = 0) or (FJournal = nil) or (not FDurable) then
    Exit;
  LDeq.VHost := FVHostName;
  LDeq.Queue := FName;
  LDeq.EntryId := AJournalId;
  SetLength(LRecs, 1);
  LRecs[0].Kind := AMQP_REC_DEQUEUE;
  LRecs[0].Payload := AmqpEncodeRecDequeue(LDeq);
  try
    // SubmitNoWait, never Submit: we are ON the actor thread.
    FJournal.SubmitNoWait(LRecs);
  except
    // A failing journal has already warned whoever needed it (WS5's sink). The
    // actor must not die because of it -- the in-memory queue is still correct.
    on E: Exception do
      ;
  end;
end;

procedure TAMQPServerQueue.TrackBytes(const AEntry: TAMQPQueueEntry;
  ASignal: Integer);
begin
  if AEntry.Msg <> nil then
    Inc(FReadyBytes, Int64(ASignal) * AEntry.Msg.BodySize);
end;

function TAMQPServerQueue.IsFull: Boolean;
begin
  Result := ((FMaxLength > 0) and (ReadyCount >= FMaxLength))
    or ((FMaxBytes >= 0) and (FReadyBytes >= FMaxBytes));
end;

procedure TAMQPServerQueue.EnforceMaxLength;
var
  LEntry: TAMQPQueueEntry;
  I: Integer;
  LRemoved: Boolean;
begin
  if (FMaxLength <= 0) and (FMaxBytes < 0) then
    Exit;
  // Drops while EITHER of the two caps is exceeded. The test is ">" and not
  // ">=": the cap is what the queue MAY hold, so it only drops after going
  // past it.
  while ((FMaxLength > 0) and (ReadyCount > FMaxLength))
    or ((FMaxBytes >= 0) and (FReadyBytes > FMaxBytes)) do
  begin
    // D12: starts with the LOWEST priority bucket. Dropping from the absolute
    // head (which is the HIGHEST priority one) would throw away exactly what
    // the client marked as most important. Within the bucket, the head -- the oldest.
    LRemoved := False;
    for I := 0 to High(FStock) do
    begin
      if FRequeued[I].Count > 0 then
        LEntry := FRequeued[I].Dequeue
      else if FStock[I].Count > 0 then
        LEntry := FStock[I].Dequeue
      else
        Continue;
      TrackBytes(LEntry, -1);
      Inc(FDropped);
      // BEFORE DeadLetterWith, which consumes the reference.
      EmitMessageEvent(seMessageDropped, LEntry, AMQP_DEATH_MAXLEN);
      // 'maxlen': dropping by cap is also a death, and goes to the DLX like the
      // others. MorreCom consumes the reference.
      DeadLetterWith(LEntry, AMQP_DEATH_MAXLEN);
      LRemoved := True;
      Break;
    end;
    if not LRemoved then
      Break; // nothing to drop (only unacked ones)
  end;
end;

// Ack (ARequeue=False), Nack/Reject with or without requeue. AMultiple: all
// tags <= ADeliveryTag OF THAT channel.
procedure TAMQPServerQueue.ResolveUnacked(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; AMultiple, ARequeue, ARejected: Boolean);
var
  I: Integer;
  LEntry: TAMQPUnackedEntry;
  LQueueEntry: TAMQPQueueEntry;
  LMatched: Boolean;
begin
  I := 0;
  while I < FUnacked.Count do
  begin
    LEntry := FUnacked[I];
    if AMultiple then
      LMatched := (LEntry.ChannelId = AChannelId)
        and (LEntry.DeliveryTag <= ADeliveryTag)
    else
      LMatched := (LEntry.ChannelId = AChannelId)
        and (LEntry.DeliveryTag = ADeliveryTag);
    if LMatched then
    begin
      FUnacked.Delete(I);
      if ARequeue then
        RequeueFront(LEntry.Msg, LEntry.Priority, LEntry.ExpiresAt,
          LEntry.JournalId, LEntry.ContentId)
      else if ARejected then
      begin
        // 'rejected': nack/reject WITHOUT requeue. The ack falls into the else
        // below -- confusing the two would send every acked message to the DLX.
        LQueueEntry.Msg := LEntry.Msg;
        LQueueEntry.Redelivered := True;
        LQueueEntry.Priority := LEntry.Priority;
        LQueueEntry.ExpiresAt := LEntry.ExpiresAt;
        LQueueEntry.JournalId := LEntry.JournalId;
        LQueueEntry.ContentId := LEntry.ContentId;
        DeadLetterWith(LQueueEntry, AMQP_DEATH_REJECTED);
      end
      else
      begin
        // ACK: end of the line. The placement leaves the disk here -- it is the
        // common case, and the only one in which the message goes away without having died.
        Retire(LEntry.JournalId);
        LEntry.Msg.Release;
      end;
    end
    else
      Inc(I);
  end;
  if ARequeue then
    EnforceMaxLength;
end;

function TAMQPServerQueue.CurrentStats: TAMQPQueueStats;
begin
  Result.MessageCount := ReadyCount;
  Result.ConsumerCount := FConsumers.Count;
  Result.UnackedCount := FUnacked.Count;
  Result.DroppedCount := FDropped;
  Result.DeliveredCount := FDelivered;
  Result.ExpiredCount := FExpired;
  Result.DeadLetteredCount := FDeadLettered;
  if FConsumers.Count > 0 then
    Result.IdleMs := 0
  else
    Result.IdleMs := Int64(NowTick - FLastUsed);
  Result.EverHadConsumer := FEverHadConsumer;
end;

{ --- observability (Phase 4.1, Inc. 2) ------------------------------------ }

function TAMQPServerQueue.WantsEvent(AType: TAMQPServerEventType): Boolean;
begin
  Result := (FEvents <> nil) and FEvents.Wants(AType);
end;

procedure TAMQPServerQueue.EmitMessageEvent(AType: TAMQPServerEventType;
  const AEntry: TAMQPQueueEntry; const AReason: string);
var
  LEv: TAMQPServerEvent;
begin
  if not WantsEvent(AType) then
    Exit;
  LEv := AmqpNewEvent(AType);
  LEv.VHost := FVHostName;
  LEv.QueueName := FName;
  LEv.Priority := AEntry.Priority;
  LEv.Redelivered := AEntry.Redelivered;
  LEv.Reason := AReason;
  // The SIZE, never the body (D34): carrying the body would put a buffer copy
  // on the hottest path there is and would break D1's COW.
  if AEntry.Msg <> nil then
  begin
    LEv.MessageSize := UInt64(AEntry.Msg.BodySize);
    LEv.ExchangeName := AEntry.Msg.Exchange;
    LEv.RoutingKey := AEntry.Msg.RoutingKey;
  end;
  FEvents.Emit(LEv);
end;

{ --- command dispatch, on the actor thread --- }

procedure TAMQPServerQueue.ExecuteCommand(ACmd: TAMQPQueueCommand);
var
  LEntry: TAMQPQueueEntry;
  LUnacked: TAMQPUnackedEntry;
  I, LCount: Integer;
begin
  case ACmd.Kind of
    amqqcEnqueue:
      begin
        LEntry.Msg := ACmd.Msg;
        LEntry.Redelivered := ACmd.Restored; // D27
        LEntry.Priority := LimitPriority(ACmd.Priority);
        LEntry.ExpiresAt := CalculateExpiration(ACmd.MessageTtlMs);
        LEntry.JournalId := ACmd.JournalId;
        LEntry.ContentId := ACmd.ContentId;
        if LEntry.ExpiresAt > 0 then
          FHasExpiration := True;
        ACmd.Msg := nil; // the reference passed to the stock
        TrackBytes(LEntry, +1);
        FStock[LEntry.Priority].Enqueue(LEntry);
        EmitMessageEvent(seMessageEnqueued, LEntry, '');
        // ORDER matters: expire before applying the cap, otherwise an already
        // expired message would take up a slot and cause a live one to be dropped.
        ExpireMessages;
        EnforceMaxLength;
      end;

    amqqcEnqueueSync:
      begin
        // Expire BEFORE deciding: an expired message taking up a slot would
        // cause a live publish to be rejected -- same reason as the order in the async enqueue.
        ExpireMessages;
        if IsFull then
        begin
          // The reference stays in the command and leaves in its destructor.
          ACmd.ResOk := False;
          // COMPENSATION. The placement record has already been written by the
          // publisher thread BEFORE getting here (D24) -- it is the one that
          // knows the LSN. If the queue rejects (x-overflow reject-publish, D15),
          // the placement must be retired, otherwise recovery would resurrect a
          // message the broker REJECTED. Writing the DEQ is simpler and safer
          // than deferring the ENQ: deferring would open the window for the
          // message to be consumed and retired BEFORE it exists in the log.
          Retire(ACmd.JournalId);
        end
        else
        begin
          LEntry.Msg := ACmd.Msg;
          LEntry.Redelivered := False;
          LEntry.Priority := LimitPriority(ACmd.Priority);
          LEntry.ExpiresAt := CalculateExpiration(ACmd.MessageTtlMs);
          LEntry.JournalId := ACmd.JournalId;
          LEntry.ContentId := ACmd.ContentId;
          if LEntry.ExpiresAt > 0 then
            FHasExpiration := True;
          ACmd.Msg := nil;
          TrackBytes(LEntry, +1);
          FStock[LEntry.Priority].Enqueue(LEntry);
          ACmd.ResOk := True;
        end;
      end;

    amqqcTouch:
      FLastUsed := NowTick;

    amqqcAddConsumer:
      begin
        FLastUsed := NowTick;
        FConsumers.Add(ACmd.Consumer);
        ACmd.Consumer := nil; // the queue owns it now
        FEverHadConsumer := True;
      end;

    amqqcRemoveConsumer:
      begin
        // Leaving is ALSO use: it is from here that the x-expires clock starts
        // counting, not from the instant the consumer joined.
        FLastUsed := NowTick;
        for I := FConsumers.Count - 1 downto 0 do
          if (FConsumers[I].ChannelId = ACmd.ChannelId)
            and (FConsumers[I].ConsumerTag = ACmd.ConsumerTag) then
            FConsumers.Delete(I);
      end;

    amqqcRemoveChannel:
      begin
        for I := FConsumers.Count - 1 downto 0 do
          if FConsumers[I].ChannelId = ACmd.ChannelId then
            FConsumers.Delete(I);
        // Unacked messages of a dead channel go back to the queue: the client
        // never acked them, so the delivery has to happen again.
        I := 0;
        while I < FUnacked.Count do
        begin
          LUnacked := FUnacked[I];
          if LUnacked.ChannelId = ACmd.ChannelId then
          begin
            FUnacked.Delete(I);
            RequeueFront(LUnacked.Msg, LUnacked.Priority, LUnacked.ExpiresAt,
              LUnacked.JournalId, LUnacked.ContentId);
          end
          else
            Inc(I);
        end;
        EnforceMaxLength;
      end;

    amqqcAck: // an ack is NOT a death: ARejected=False
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, ACmd.Multiple,
        False, False);

    amqqcNack:
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, ACmd.Multiple,
        ACmd.Requeue, not ACmd.Requeue);

    amqqcReject: // Basic.Reject has no 'multiple'
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, False, ACmd.Requeue,
        not ACmd.Requeue);

    amqqcGet:
      begin
      FLastUsed := NowTick; // Basic.Get is use (RabbitMQ's de facto spec)
      // A Basic.Get must not return an already expired message -- lazy
      // expiration runs on the delivery path, and Get is the other path.
      ExpireMessages;
      if TakeReady(LEntry) then
      begin
        ACmd.ResOk := True;
        ACmd.ResRedelivered := LEntry.Redelivered;
        ACmd.ResMsg := LEntry.Msg;
        if not ACmd.NoAck then
        begin
          // The caller takes ONE reference and the queue keeps its own in
          // the unacked ones.
          LEntry.Msg.AddRef;
          LUnacked.ChannelId := ACmd.ChannelId;
          LUnacked.DeliveryTag := ACmd.DeliveryTag;
          LUnacked.Msg := LEntry.Msg;
          LUnacked.Priority := LEntry.Priority;
          LUnacked.ExpiresAt := LEntry.ExpiresAt;
          LUnacked.JournalId := LEntry.JournalId;
          LUnacked.ContentId := LEntry.ContentId;
          FUnacked.Add(LUnacked);
        end
        else
          // Get in no-ack: the placement ends here, as in delivery without ack.
          Retire(LEntry.JournalId);
        // With no-ack the stock's reference simply passes to the caller.
      end
      else
        ACmd.ResOk := False;
      end;

    amqqcPurge:
      begin
        LCount := 0;
        while TakeReady(LEntry) do
        begin
          Retire(LEntry.JournalId);
          LEntry.Msg.Release; // a purge is not a drop by limit: not counted
          Inc(LCount);
        end;
        ACmd.ResOk := True;
        ACmd.ResCount := LCount;
      end;

    amqqcDelete:
      begin
        if ACmd.IfUnused and (FConsumers.Count > 0) then
          ACmd.ResDelete := amqqdInUse
        else if ACmd.IfEmpty and (ReadyCount > 0) then
          ACmd.ResDelete := amqqdNotEmpty
        else
        begin
          ACmd.ResCount := ReadyCount;
          ACmd.ResDelete := amqqdOk;
          // Warns whoever was consuming BEFORE the state goes away: the queue
          // vanished from under them and the client needs to know (spec 1.7.2.9 --
          // "the server MUST cancel all consumers"). Best-effort and without
          // blocking: the queue is being destroyed, there is no retry.
          for I := 0 to FConsumers.Count - 1 do
            try
              FConsumers[I].Target.TryCancel(FConsumers[I].ConsumerTag);
            except
              on E: Exception do
                ; // channel dying along with it: does not get in the way of the delete
            end;
          // The state leaves in the DrainState of the Stop the Delete triggers --
          // freeing it here and then again there would be a double free.
        end;
        ACmd.ResOk := ACmd.ResDelete = amqqdOk;
      end;

    amqqcStats:
      begin
        ACmd.ResStats := CurrentStats;
        ACmd.ResOk := True;
      end;

    amqqcDeliverTick:
      ; // only exists to trigger the delivery round below
  end;

  // A delivery round after every command that may have UNBLOCKED delivery:
  // a new message, a new consumer, ack/nack/reject (which release prefetch or
  // return a message) and the tick of D3's degraded case. Get, Purge, Stats,
  // Delete and a consumer leaving never create a delivery opportunity, so they
  // do not even try.
  if ACmd.Kind in [amqqcEnqueue, amqqcEnqueueSync, amqqcAddConsumer, amqqcAck, amqqcNack,
    amqqcReject, amqqcRemoveChannel, amqqcDeliverTick] then
    DeliverPending;
end;

end.
