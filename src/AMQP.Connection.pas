unit AMQP.Connection;

{$I amqp.inc}

{ AMQP 0-9-1 connection over a TCP socket, with a dedicated read thread.

  Concurrency architecture (roadmap item 3):
  - The handshake (Open) is done synchronously, reading the socket inline, BEFORE
    any thread exists.
  - After Open-Ok, a single read thread (TAMQPReaderThread) becomes
    the ONLY one that reads the socket. All it does: read frame -> demultiplex by channel
    -> deliver. It never runs user code nor blocks.
  - ALL writes to the socket go through FWriteLock (a TCriticalSection), so
    that RPCs, publishes, acks and the read thread's own replies
    (Close-Ok) do not interleave.
  - RPC (declare/bind/get/close/...) is event-driven: the caller registers what
    it expects, sends the method and waits on a TEvent that the read thread signals
    when delivering the reply.

  Invariants (so as not to introduce a race/deadlock):
  - Only the read thread reads the socket after the handshake.
  - Lock order always "outside in": FChannelsLock -> FWriteLock and
    FRpcLock -> FWriteLock. FWriteLock never wraps FChannelsLock/FRpcLock.
  - The confirms monitor (FConfirmMon, publisher confirms) is the INNERMOST lock:
    it is acquired alone (WaitForConfirm) or inside FWriteLock (Publish) or
    inside FChannelsLock (when resolving acks on the read thread); no other lock
    is ever acquired while holding it.
  - The read thread writes the RPC slot and calls SetEvent; the caller only reads
    the slot after WaitFor returns (the event is the synchronization barrier).

  Consumption (Basic.Consume + dispatch to the thread pool) and heartbeat come in the
  next increments. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Threading,
  PascalCommon.Transport,
  AMQP.Transport,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  AMQP.Connection.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  /// Connection-level error: handshake, Open/Close, or misuse
  /// (publish/CreateChannel with the connection closed or reconnecting). An
  /// EAMQPConnection does not close the channel or the connection by itself — the
  /// caller decides what to do (retry, give up).
  EAMQPConnection = class(Exception);
  /// Channel-level error: an RPC (declare/bind/get/consume/close) that
  /// received Channel.Close from the broker, timed out, or was called with the
  /// channel already closed — or misuse (WaitForConfirm outside confirm
  /// mode). See also EAMQPConnection (errors of the connection as a whole).
  EAMQPChannel = class(Exception);

  TAMQPConnectionParams = record
    Host: string;
    Port: Word;
    VirtualHost: string;
    User: string;
    Password: string;
    // Client tune preferences (0 = no limit; see NegotiateTune).
    ChannelMax: Word;
    FrameMax: Cardinal;
    Heartbeat: Word;
    // Automatic reconnection (opt-in).
    AutoReconnect: Boolean;
    ReconnectDelayMs: Cardinal;    // wait between attempts (default 2000)
    MaxReconnectAttempts: Integer; // 0 = infinite
    ConnectionName: string;        // optional; goes in client-properties
    // Automatic resend (opt-in): in confirm mode, publishes left
    // unconfirmed by a drop are republished on reconnection (new seq-nos).
    // At-least-once — may duplicate. Cost: keeps the body until confirmed.
    RepublishUnconfirmedOnReconnect: Boolean;
    /// Budget for the TCP connect (every address of the name included), in
    /// ms. 0 = pascal-common-faa's default, 10 s. Before 0.6.0 there was no
    /// budget of our own (the OS's: ~21 s on Windows, minutes on Linux).
    ConnectTimeoutMs: Cardinal;
    // TLS (amqps://). Opt-in.
    UseTls: Boolean;               // True => does a TLS handshake before AMQP
    TlsVerifyPeer: Boolean;        // True (default): validates chain + hostname
    TlsServerName: string;         // '' => uses Host (SNI / name for validation)
    /// Private CA (PEM, both backends): trust THIS CA for the server instead
    /// of the system store. '' = the system store.
    TlsCaFile: string;
    /// Client certificate for mTLS. OpenSSL: a PEM pair (TlsCertFile +
    /// TlsKeyFile). SChannel: a PFX in TlsCertFile with TlsCertPassword.
    /// '' = no client certificate.
    TlsCertFile: string;
    TlsKeyFile: string;
    TlsCertPassword: string;
    /// Which TLS implementation, chosen at run time. tbDefault = SChannel on
    /// Windows (OpenSSL with -dAMQP_OPENSSL), OpenSSL elsewhere; see
    /// AmqpDefaultTlsBackend. tbSChannel off Windows fails with EAMQPTls.
    TlsBackend: TPcTlsBackend;
    /// Default parameters: localhost:5672, vhost '/', guest/guest (no TLS).
    class function Localhost: TAMQPConnectionParams; static;
    /// Like Localhost, but TLS on port 5671 with validation OFF
    /// (TlsVerifyPeer=False) — a convenience for a dev broker with a self-signed cert.
    class function LocalhostTls: TAMQPConnectionParams; static;
  end;

  { Result of Basic.Get. If Found=False, the queue was empty. If
    Properties has Headers, the caller owns that table (free it). }
  TAMQPGetResult = record
    Found: Boolean;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    MessageCount: Cardinal;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  TAMQPConnection = class;
  TAMQPChannel = class;

  { Callback for connection events (disconnect / reconnect). Runs on an internal
    thread; keep it short and non-blocking. }
  TAMQPConnectionEvent = procedure(AConnection: TAMQPConnection) of object;

  { Callback for a failed reconnection attempt: AAttempt counts from 1 within the
    current drop and AError is the exception message (e.g. the 404 of a
    basic.consume on a queue nobody redeclared). Runs on the reconnect
    thread, like OnDisconnect/OnReconnect; the next attempt comes after
    ReconnectDelayMs. }
  TAMQPReconnectErrorEvent = procedure(AConnection: TAMQPConnection;
    AAttempt: Integer; const AError: string) of object;

  { Callback for Connection.Blocked (RabbitMQ extension): the broker entered a
    resource alarm (memory/disk) and stopped accepting publishes; AReason carries the
    reason. Dispatched on a pool thread (does not block the read thread). The
    Unblocked counterpart uses TAMQPConnectionEvent. }
  TAMQPConnectionBlockedEvent = procedure(AConnection: TAMQPConnection;
    const AReason: string) of object;

  { Message delivered to a consumer (Basic.Deliver). If Properties has
    Headers, the table is freed automatically after the callback returns. }
  TAMQPDelivery = record
    ConsumerTag: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  { Consumer callback. Runs on a pool thread, so it does NOT block the
    read thread. Use AChannel.Ack(ADelivery.DeliveryTag) to acknowledge.
    If it raises, the exception goes to AChannel.OnConsumerError and the delivery
    not yet acknowledged gets a Nack without requeue (see TAMQPConsumerErrorAction). }
  TAMQPConsumerCallback = procedure(AChannel: TAMQPChannel;
    const ADelivery: TAMQPDelivery) of object;

  { What the lib does with a delivery whose callback raised an exception. Only
    applied if the delivery has not been acknowledged yet (by Ack/Nack in the callback or
    in OnConsumerError itself). If the connection dropped during the callback, the Nack
    fails silently: the broker already returned the delivery to the queue, and it comes
    back with Redelivered=True after reconnection. }
  TAMQPConsumerErrorAction = (
    ceaReject,  // Nack without requeue: goes to the queue's dead-letter exchange, if any; otherwise the broker drops it
    ceaRequeue, // Nack with requeue: goes back to the queue (beware of a message that always fails: loop)
    ceaNone);   // the lib does not acknowledge (the application acknowledges later, or there is nothing to acknowledge)

  { Callback for an exception raised by the consumer callback. Runs on the same
    pool thread, right after the exception, with the message's 'process' span
    still current (with tracing on: the log written here carries its trace id;
    the span ends afterwards, with the callback's error). AAction arrives as ceaReject if the
    delivery is still pending, or ceaNone if it was already acknowledged or the
    consumer is no-ack; change it to decide. The exception belongs to the lib: do not free it
    or re-raise it. An exception raised by the handler itself is ignored and
    AAction stands as it was. }
  TAMQPConsumerErrorEvent = procedure(AChannel: TAMQPChannel;
    const ADelivery: TAMQPDelivery; AError: Exception;
    var AAction: TAMQPConsumerErrorAction) of object;

  { Consumer registered on the channel: the queue (for the span name 'process
    <queue>'; Basic.Deliver does not carry it) and the no-ack mode (a no-ack delivery
    has nothing to acknowledge when the callback raises). }
  TAMQPConsumerInfo = record
    Queue: string;
    NoAck: Boolean;
  end;

  { Message returned by the broker (Basic.Return): a `mandatory` publish that
    could not be routed to any queue. If Properties has Headers, the table
    is freed automatically after the callback returns. }
  TAMQPReturnedMessage = record
    ReplyCode: Word;
    ReplyText: string;
    Exchange: string;
    RoutingKey: string;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  { Basic.Return callback. Runs on a pool thread (TTask), like the
    consumer callback — does not block the read thread. }
  TAMQPBasicReturnCallback = procedure(AChannel: TAMQPChannel;
    const AReturned: TAMQPReturnedMessage) of object;

  { Publisher confirm callback: the broker ack'ed or nack'ed the publish
    numbered ASeqNo (the value returned by Publish). Runs on a pool thread,
    like the other callbacks — does not block the read thread. Fires only on
    real confirms from the broker; a connection drop does NOT fire OnConfirm (the
    pending publish is reported as unconfirmed via WaitForConfirm). }
  TAMQPConfirmCallback = procedure(AChannel: TAMQPChannel;
    ASeqNo: UInt64; AAck: Boolean) of object;

  { Replay phase a recorded action belongs to. Reconnection replays the
    WHOLE connection phase by phase, not channel by channel: every exchange of every
    channel, then every queue, then every bind, finally every consumer. Channel by
    channel, a consumer on a consume-only channel ran before the declare made
    on another channel and got a 404 (non-durable queue, which the broker restart
    deleted) — and the attempt failed the same way forever. Within a phase, the
    order is that of the channels (ChannelId) and, within each channel, the recording order. }
  TAMQPRecoveryPhase = (rpChannel, rpExchange, rpQueue, rpBind, rpConsume);

  { Topology action recorded for replay after reconnection. We keep the
    already serialized method payload (qos/declare/bind/consume) — so arguments and
    flags come for free. Consumers also keep the tag and the callback. }
  TAMQPRecoveryAction = record
    Payload: TBytes;
    AwaitReply: Boolean;    // True: waits for the -Ok (CallRpc); False: only sends
    Phase: TAMQPRecoveryPhase; // derived from the payload's method (AddRecovery)
    IsConsume: Boolean;
    ConsumerTag: string;
    Callback: TAMQPConsumerCallback;
  end;

  TAMQPRpcKind = (rkNone, rkMethod, rkMessage, rkError);

  // Content assembly state (method + content header + body frames).
  // Named type (not inline anonymous) for FPC compatibility.
  TAMQPAsmState = (asIdle, asHeader, asBody);

  { Already serialized frames of a publish, kept for resending after reconnection
    (internal use; see RepublishUnconfirmedOnReconnect). Method/Header are the
    frame payloads; Body is the whole body (re-sliced on resend). }
  TAMQPRawPublish = record
    Method: TBytes;
    Header: TBytes;
    Body: TBytes;
  end;

  { Data channel. Created via TAMQPConnection.CreateChannel (which already opens it).
    The caller owns the channel and must free it (Free). }
  TAMQPChannel = class
  private
    FConnection: TAMQPConnection;
    FChannelId: Word;
    FIsOpen: Boolean;
    FClosed: Boolean;
    // Channel closing or closed (v0.4.2): set by Close before
    // Channel.Close and by the read thread before the Close-Ok of a
    // broker-initiated close. Under FAckLock, which Ack/Nack hold from the check until the
    // frame goes out: every accepted Ack goes on the wire BEFORE Channel.Close(-Ok),
    // and none goes out after — a Basic.Ack on a closed channel is a CONNECTION error
    // (504) and would bring down every channel. Order: FAckLock outside
    // FSettleLock and FWriteLock; nothing acquires them in the reverse order.
    FClosing: Boolean;
    FAckLock: TCriticalSection;
    // --- RPC (one call at a time on this channel) ---
    FRpcLock: TCriticalSection;
    FRpcEvent: TEvent;
    FRpcKind: TAMQPRpcKind;
    FRpcMethodPayload: TBytes;
    FRpcMessage: TAMQPGetResult;
    FRpcError: string;
    // --- content assembly (written only by the read thread) ---
    FAsmState: TAMQPAsmState;
    FAsmMethodId: TAMQPMethodId;
    FAsmGetOk: TAMQPBasicGetOk;
    FAsmDeliver: TAMQPBasicDeliver;
    FAsmReturn: TAMQPBasicReturn;
    FAsmProps: TAMQPBasicProperties;
    FAsmBody: TBytes;
    FAsmRemaining: UInt64;
    // --- consumers ---
    FConsumers: TDictionary<string, TAMQPConsumerCallback>;
    // consumer-tag -> queue and no-ack (see TAMQPConsumerInfo). Under
    // FConsumersLock; reconnection does not clear it: the consumers come back with the same
    // tag, the same queue and the same mode.
    FConsumerInfo: TDictionary<string, TAMQPConsumerInfo>;
    FConsumersLock: TCriticalSection;
    FConsumerCounter: Integer;
    // --- deliveries not yet acknowledged (for OnConsumerError) ---
    // Delivery-tags of manual-ack consumers, from dispatch (read
    // thread, BEFORE queueing: an Ack never arrives before the registration) until
    // Ack/Nack or the end of the callback. A delivery from before a reconnection
    // is never acknowledged with another one's tag: reconnection drains every old
    // callback (DrainAllChannels) before reopening the channel, and the Nack of an
    // old callback falls on the dead socket. FSettleLock only protects the set;
    // no other lock is ever acquired with it.
    FUnsettled: TDictionary<UInt64, Boolean>;
    FSettleLock: TCriticalSection;
    FOnConsumerError: TAMQPConsumerErrorEvent;
    FOnBasicReturn: TAMQPBasicReturnCallback;
    FInFlight: Integer; // callbacks running on the pool (atomic)
    // Private pool (1 worker) when the channel uses a dedicated thread; nil => uses
    // the global PcPool (default behavior). See CreateChannel.
    FDispatchPool: TPcThreadPool;
    // --- publisher confirms ---
    // FConfirmMon is lock + condition variable (TPcMonitor) protecting
    // FUnconfirmed/FNacked/FPublishSeqNo/FConfirmBase and waking WaitForConfirm(s).
    // Lock order: it is the INNERMOST lock — acquired only alone or inside
    // FWriteLock (Publish); no other lock is ever acquired while holding it.
    FConfirmMode: Boolean;
    FPublishSeqNo: UInt64;   // user seq-no, monotonic (NOT reset on reconnection)
    FConfirmBase: UInt64;    // session offset: userSeqNo = FConfirmBase + broker's wireTag
    FConfirmMon: TPcMonitor;
    FUnconfirmed: TDictionary<UInt64, Boolean>; // seq-nos awaiting confirm
    FNacked: TDictionary<UInt64, Boolean>;      // nack'ed seq-nos (or lost in the drop)
    FOnConfirm: TAMQPConfirmCallback;
    // Automatic resend (opt-in via params): buffer of the content of pending
    // publishes, by seq-no, to republish after reconnection. Protected by
    // FConfirmMon, like FUnconfirmed.
    FRepublish: Boolean;
    FResendBuffer: TDictionary<UInt64, TAMQPRawPublish>;
    // --- topology recorded for reconnection ---
    FRecovery: TList<TAMQPRecoveryAction>;
    FRecoveryLock: TCriticalSection;
    procedure AddRecovery(const APayload: TBytes; AAwaitReply: Boolean;
      AIsConsume: Boolean = False; const AConsumerTag: string = '';
      const ACallback: TAMQPConsumerCallback = nil);
    /// Removes a consumer's recovery record (when cancelling it).
    procedure RemoveConsumerRecovery(const AConsumerTag: string);
    /// Removes from the recorded topology the bind equivalent to (queue, exchange, route),
    /// so that an unbind is not undone by a reconnection. Matches binds without
    /// Arguments (the common case), in both no-wait variants.
    procedure RemoveBindRecovery(const AQueue, AExchange, ARoutingKey: string);
    /// Same for exchange->exchange bindings (see UnbindExchange).
    procedure RemoveExchangeBindRecovery(const ADestination, ASource, ARoutingKey: string);
    /// Removes from the recorded topology the queue.declare of queue AQueue (called by
    /// DeleteQueue), so a reconnection does not recreate the deleted queue. Decodes the
    /// header of each recorded payload — independent of the declare's flags/Arguments.
    procedure RemoveQueueDeclareRecovery(const AQueue: string);
    /// Removes from recovery every action whose payload matches one of the candidates.
    procedure RemoveRecoveryMatching(const ACandidates: array of TBytes);
    /// Reconnection, in three steps called by the connection (RecoverAllChannels):
    /// BeginRecover takes FRpcLock (user RPCs wait for the replay), reopens
    /// the channel and replays the rpChannel phase (qos, confirm.select); ReplayRecovery
    /// replays one topology phase (and, after the last one, the connection calls
    /// ResendUnconfirmed); EndRecover releases the lock, on success or not. If
    /// BeginRecover raises, the lock has already been released and EndRecover is not called.
    procedure BeginRecover;
    procedure ReplayRecovery(APhase: TAMQPRecoveryPhase);
    procedure EndRecover;
    procedure Open;
    /// Sends a method and waits for the reply (payload of the reply method).
    function CallRpc(const ARequest: TBytes): TBytes;
    // signalers called by the read thread:
    procedure SignalMethod(const APayload: TBytes);
    procedure SignalMessage(const AMessage: TAMQPGetResult);
    procedure SignalError(const AMessage: string);
    procedure CompleteContent;
    /// Queues a work item on the channel's dedicated pool (if any) or on the
    /// global PcPool. Do not call it "Dispatch": it would collide with
    /// TObject.Dispatch used by the message dispatch mechanism.
    procedure DispatchToPool(AItem: TPcWorkItem);
    /// Dispatches a delivery to the consumer callback, on the thread pool.
    procedure DispatchDelivery(const ADeliver: TAMQPBasicDeliver;
      const AProps: TAMQPBasicProperties; const ABody: TBytes);
    /// Dispatches a Basic.Return to OnBasicReturn, on the thread pool.
    procedure DispatchReturn(const AReturn: TAMQPBasicReturn;
      const AProps: TAMQPBasicProperties; const ABody: TBytes);
    /// Dispatches a confirm (broker ack/nack) to OnConfirm, on the thread pool.
    procedure DispatchConfirm(ASeqNo: UInt64; AAck: Boolean);
    /// Resolves the pending seq-nos covered by (tag, multiple); returns the resolved
    /// list (to dispatch OnConfirm outside the lock). Runs on the read thread.
    function ResolveConfirms(ATag: UInt64; AMultiple, AAck: Boolean): TArray<UInt64>;
    /// Marks every pending publish as unconfirmed (connection drop).
    procedure FailAllUnconfirmed;
    /// Sends a publish's frames under FWriteLock, assigning the seq-no (in
    /// confirm mode) and buffering for resend (if enabled). Basis of Publish
    /// and of the post-reconnection resend. Returns the seq-no (0 outside confirm mode).
    function DoSendPublish(const AMethod, AHeader, ABody: TBytes): UInt64;
    /// Republishes the publishes left unconfirmed (called in Recover,
    /// if RepublishUnconfirmedOnReconnect). Resends in the original order, with new
    /// seq-nos; the old ones were already reported as unconfirmed.
    procedure ResendUnconfirmed;
    /// The Basic.Get without a span (BasicGet opens the span around it).
    function DoBasicGet(const AQueue: string; ANoAck: Boolean): TAMQPGetResult;
    /// Removes the tag from the pending set (or, with AMultiple, every tag up to
    /// it). Called by Ack/Nack, before sending.
    procedure SettleDeliveries(ADeliveryTag: UInt64; AMultiple: Boolean);
    /// Sets FClosing under FAckLock: Ack/Nack stop going out on the wire.
    procedure MarkClosing;
    /// Removes the tag from the set; True if it was there (the delivery is still
    /// unacknowledged). Atomic: it is what keeps the lib from acknowledging a
    /// delivery the application already acknowledged.
    function TakeUnsettled(ADeliveryTag: UInt64): Boolean;
    /// Only checks (without removing); used for the initial action of OnConsumerError.
    function IsUnsettled(ADeliveryTag: UInt64): Boolean;
    /// Handles the exception raised by a consumer callback: OnConsumerError
    /// and, if the delivery is still pending, the Nack of the chosen action.
    procedure HandleConsumerError(const ADelivery: TAMQPDelivery;
      AError: Exception);
    /// Waits for the in-flight callbacks to finish (used when closing the channel).
    procedure DrainInFlight;
    /// Handles a frame delivered by the read thread (runs ON the read thread).
    procedure HandleFrame(const AFrame: TAMQPFrame);
  public
    constructor Create(AConnection: TAMQPConnection; AChannelId: Word);
    destructor Destroy; override;

    procedure DeclareExchange(const ADeclare: TAMQPExchangeDeclare);
    function DeclareQueue(const ADeclare: TAMQPQueueDeclare): TAMQPQueueDeclareOk;
    procedure BindQueue(const ABind: TAMQPQueueBind);
    /// Undoes a queue->exchange binding (queue.unbind). Always waits for Unbind-Ok.
    /// Also removes the equivalent bind from the topology recorded for reconnection, so
    /// it is not recreated on an eventual reconnection (see AddRecovery/Recover). Binds
    /// with non-trivial Arguments are not reconciled automatically.
    procedure UnbindQueue(const AUnbind: TAMQPQueueUnbind);
    /// Binds one exchange to another (exchange->exchange binding, RabbitMQ extension):
    /// Destination starts receiving what is routed from Source by RoutingKey.
    procedure BindExchange(const ABind: TAMQPExchangeBinding);
    /// Undoes an exchange->exchange binding and removes the equivalent bind from the
    /// recovery topology (same reconciliation as UnbindQueue).
    procedure UnbindExchange(const AUnbind: TAMQPExchangeBinding);
    /// Deletes a queue (queue.delete). Returns the number of dropped messages
    /// (0 with NoWait). In RabbitMQ, deleting a nonexistent queue is a no-op that
    /// returns 0 — usable to recreate topology from scratch. Also removes
    /// this queue's declares from the topology recorded for reconnection.
    function DeleteQueue(const ADelete: TAMQPQueueDelete): Cardinal;

    /// Puts the channel in publisher confirms mode (confirm.select). From then on
    /// each Publish gets a seq-no and the broker confirms it via Basic.Ack/Nack —
    /// use OnConfirm (asynchronous) and/or WaitForConfirm/WaitForConfirms. Idempotent.
    procedure ConfirmSelect;

    /// Publishes ABody with properties AProps. Without confirm mode: fire-and-forget
    /// and returns 0. In confirm mode: returns the assigned seq-no (1, 2, 3, ...),
    /// which identifies this publish in OnConfirm/WaitForConfirm.
    function Publish(const AExchange, ARoutingKey: string; const ABody: TBytes;
      const AProps: TAMQPBasicProperties; AMandatory: Boolean = False): UInt64;
    /// Convenience: publishes a UTF-8 text as content-type text/plain.
    /// Returns the seq-no like Publish (0 outside confirm mode).
    function PublishText(const AExchange, ARoutingKey, AText: string;
      APersistent: Boolean = True): UInt64;

    /// Blocks until the broker confirms publish ASeqNo. Result=True if ack'ed;
    /// False if nack'ed, if the connection dropped before the confirm, or on timeout.
    /// Requires confirm mode. ASeqNo must be a value returned by Publish.
    function WaitForConfirm(ASeqNo: UInt64; ATimeoutMs: Cardinal = 5000): Boolean;
    /// Blocks until every pending publish is confirmed. Result=True
    /// if all were ack'ed within the timeout; False if any was nack'ed (or
    /// lost in a drop) or the timeout expired. Requires confirm mode.
    /// Consumes the batch's nack state — do not mix with WaitForConfirm(seqno)
    /// for the same publishes (use one or the other per batch).
    function WaitForConfirms(ATimeoutMs: Cardinal = 5000): Boolean;

    /// Fetches a message from the queue. Result.Found=False if it was empty.
    function BasicGet(const AQueue: string; ANoAck: Boolean = True): TAMQPGetResult;
    procedure Ack(ADeliveryTag: UInt64; AMultiple: Boolean = False);
    procedure Nack(ADeliveryTag: UInt64; ARequeue: Boolean = True;
      AMultiple: Boolean = False);

    /// Limits how many unacknowledged messages the server delivers at a time
    /// (prefetch). Useful to split the load among concurrent callbacks.
    procedure Qos(APrefetchCount: Word; AGlobal: Boolean = False);
    /// Starts consuming the queue. Each message arrives in ACallback, dispatched
    /// on a pool thread. Returns the consumer-tag (use it in Cancel). With
    /// ANoAck=False (default), acknowledge with AChannel.Ack in the callback.
    function Consume(const AQueue: string; const ACallback: TAMQPConsumerCallback;
      ANoAck: Boolean = False; AExclusive: Boolean = False): string;
    /// Cancels a consumer by tag.
    procedure Cancel(const AConsumerTag: string);

    /// Closes the channel (Channel.Close + waits for Close-Ok).
    ///
    /// With active consumers (v0.4.2): first cancels each one (Basic.Cancel
    /// + Cancel-Ok — the broker stops delivering), then waits for the callbacks
    /// already dispatched to finish with the channel still open (their Ack/Nack
    /// counts), and only then sends Channel.Close. Ack/Nack after that — from a
    /// late callback or from another thread — are ignored: the broker has already
    /// returned to the queue whatever was left unacked. The same holds when it is the BROKER
    /// that closes the channel. Do not call Close from inside a callback of the
    /// channel itself: it waits for the callbacks, including the one that called it.
    ///
    /// MIND the ownership: the channel is still YOURS after Close — free it.
    /// Closing UNREGISTERS the channel from the connection (that is what makes a late
    /// server frame be discarded instead of landing on an already closed channel), and the
    /// connection's destructor only collects the channels that are still registered.
    /// In other words: forgetting the Free of a channel you NEVER closed is forgiven
    /// by the connection's destructor; forgetting the Free after a Close leaks the
    /// object. The right pair is always `Ch.Close; Ch.Free;` (or just `Ch.Free`,
    /// which closes before freeing).
    ///
    /// Close and Free wait, with no deadline, for this channel's callbacks counted from
    /// QUEUEING — including those still in the pool's queue. On a
    /// regular channel the pool is PcPool, the whole process's: if another lib
    /// saturates it, closing waits for its queue to move. CreateChannel(True) does not
    /// depend on it (see DrainInFlight).
    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'OK');

    property ChannelId: Word read FChannelId;
    property IsOpen: Boolean read FIsOpen;
    /// Fired when the broker returns an unroutable `mandatory` publish
    /// (Basic.Return). Runs on a pool thread (does not block reading).
    property OnBasicReturn: TAMQPBasicReturnCallback read FOnBasicReturn write FOnBasicReturn;
    /// Fired when the broker confirms (ack/nack) a publish in confirm mode.
    /// Runs on a pool thread. Set it before publishing. Optional — you can
    /// use only WaitForConfirm/WaitForConfirms.
    property OnConfirm: TAMQPConfirmCallback read FOnConfirm write FOnConfirm;
    /// Fired when a consumer callback of this channel raises an exception
    /// (see TAMQPConsumerErrorEvent). Without a handler, or if it does not change the
    /// action, a delivery not yet acknowledged gets a Nack WITHOUT requeue — it goes to
    /// the queue's dead-letter exchange, if any. Never acknowledges twice:
    /// if the callback already did Ack/Nack before raising, the lib does nothing.
    property OnConsumerError: TAMQPConsumerErrorEvent read FOnConsumerError write FOnConsumerError;
  end;

  TAMQPReaderThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  { Heartbeat thread: wakes up periodically (interruptible wait via TEvent,
    NOT TTimer) to send a heartbeat when sending is idle and to detect a
    dead connection (no frame received in 2x the negotiated interval). }
  TAMQPHeartbeatThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  TAMQPConnection = class
  private
    FParams: TAMQPConnectionParams;
    FEndpoint: TPcEndpoint; // TCP or TLS (pascal-common-faa); owned
    FStream: TStream;       // TAMQPEndpointStream over FEndpoint
    FIsOpen: Boolean;
    FNegotiated: TAMQPConnectionTune;
    FNextChannel: Word;
    FWriteLock: TCriticalSection;
    FChannelsLock: TCriticalSection;
    FChannels: TDictionary<Word, TAMQPChannel>;
    FReadThread: TAMQPReaderThread;
    FCloseOkEvent: TEvent;
    // --- heartbeat ---
    FHeartbeatThread: TAMQPHeartbeatThread;
    FHbStopEvent: TEvent;
    FLastWriteTick: UInt64; // updated on every frame sent
    FLastReadTick: UInt64;  // updated on every frame received
    // --- reconnection ---
    FDeliberateClose: Boolean; // True when the user closed (do not reconnect)
    FReconnecting: Boolean;
    // Thread of the reconnection in progress (or of the last one, already finished), joined in Close
    // or before creating the next one. Protected by FReconnectLock, which also
    // makes "decide to reconnect" (read thread) and "close on
    // purpose" (Close) atomic. FReconnectWake is signaled by Close and wakes the
    // wait between attempts immediately.
    FReconnectLock: TCriticalSection;
    FReconnectWake: TEvent;
    FReconnectThread: TThread;
    FOnDisconnect: TAMQPConnectionEvent;
    FOnReconnect: TAMQPConnectionEvent;
    FOnReconnectFailed: TAMQPConnectionEvent;
    FOnReconnectError: TAMQPReconnectErrorEvent;
    // --- write state and reasons (so the publish exception says WHY) ---
    // FWritable (0/1, atomic, lock-free): 1 only between the end of the handshake and the
    // drop/close. Outside that, SendFrameNoLock raises EAMQPConnection without
    // touching the stream — before, a publish on a dead connection wrote to the stream
    // and leaked the RTL's EWriteError. FLostReason (why the connection dropped) and
    // FLastReconnectError (why the last attempt failed) live under
    // FStateLock, the innermost lock of all: it only protects the two strings.
    FWritable: Integer;
    FStateLock: TCriticalSection;
    FLostReason: string;
    FServerCloseReason: string; // the broker's Connection.Close; read thread only
    FLastReconnectError: string;
    // --- Connection.Blocked/Unblocked (broker resource alarm) ---
    FOnBlocked: TAMQPConnectionBlockedEvent;
    FOnUnblocked: TAMQPConnectionEvent;
    FInFlightConn: Integer; // connection callbacks running on the pool (atomic)
    // --- sending (serialized by FWriteLock) ---
    /// Writes the frame without looking at FWritable (the handshake writes before the
    /// connection becomes writable). A write failure becomes EAMQPConnection.
    procedure WriteFrameNoLock(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendFrameNoLock(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendFrame(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendMethod(AChannel: Word; const APayload: TBytes);
    procedure SendHandshakeMethod(const APayload: TBytes);
    function UnavailableMessage: string;
    function GetLastReconnectError: string;
    // --- synchronous reading, only during the handshake ---
    function NextFrame: TAMQPFrame;
    function NextMethodOn(AExpectChannel: Word; out AId: TAMQPMethodId): TAMQPReader;
    function ExpectMethod(AExpectChannel, AClassId, AMethodId: Word): TAMQPReader;
    function BuildClientProperties: TAMQPFieldTable;
    procedure Handshake;
    procedure EstablishConnection;
    procedure CloseSocketStream;
    // --- threads ---
    procedure StartReadThread;
    procedure StopReadThread;
    procedure StartHeartbeatThread;
    procedure StopHeartbeatThread;
    procedure HeartbeatTick;
    procedure DispatchFrame(const AFrame: TAMQPFrame);
    procedure HandleConnectionFrame(const AFrame: TAMQPFrame);
    /// Dispatches Connection.Blocked/Unblocked to the pool (does not block reading).
    procedure DispatchBlocked(const AReason: string);
    procedure DispatchUnblocked;
    /// Waits for in-flight connection callbacks to finish (used when destroying).
    procedure DrainConnCallbacks;
    procedure ReadThreadFinished(const AError: string);
    procedure UnregisterChannel(AChannelId: Word);
    // --- reconnection ---
    procedure RunReconnect;
    procedure RecoverAllChannels;
    procedure DrainAllChannels;
    procedure StopReconnect;
  public
    constructor Create(const AParams: TAMQPConnectionParams);
    destructor Destroy; override;

    /// Connects the socket and runs the handshake. Raises EAMQPConnection on failure.
    procedure Open;
    /// Opens a new channel (already open). The caller owns it and must free it
    /// (see the ownership note in TAMQPChannel.Close: closing does not free it, and takes
    /// the channel out of the connection destructor's safety net).
    /// ADedicatedConsumerThread: if True, this channel's deliveries/returns/confirms
    /// are dispatched to a single thread of the channel's own (guaranteed
    /// order, never concurrent) instead of the shared global pool — and
    /// closing/freeing the channel does not depend on PcPool being free.
    function CreateChannel(ADedicatedConsumerThread: Boolean = False): TAMQPChannel;
    /// Sends Connection.Close, waits for Close-Ok and closes the socket.
    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'Goodbye');
    /// Closes the socket abruptly to simulate a network drop (for tests).
    procedure DropConnectionForTest;

    property IsOpen: Boolean read FIsOpen;
    property NegotiatedTune: TAMQPConnectionTune read FNegotiated;
    /// Fired (on an internal thread) when the connection drops and reconnection starts.
    property OnDisconnect: TAMQPConnectionEvent read FOnDisconnect write FOnDisconnect;
    /// Fired after reconnecting and restoring the topology successfully.
    property OnReconnect: TAMQPConnectionEvent read FOnReconnect write FOnReconnect;
    /// Fired when reconnection runs out of attempts.
    property OnReconnectFailed: TAMQPConnectionEvent read FOnReconnectFailed write FOnReconnectFailed;
    /// Fired on each failed reconnection attempt, with the reason (broker
    /// still down, 404 in the topology replay...). With
    /// MaxReconnectAttempts = 0 reconnection tries forever: this (or
    /// LastReconnectError) is how you find out it is not making progress.
    property OnReconnectError: TAMQPReconnectErrorEvent read FOnReconnectError write FOnReconnectError;
    /// Message of the last failed reconnection attempt; '' after a
    /// successful reconnection. It is also what the EAMQPConnection of a publish
    /// made during reconnection carries in its text.
    property LastReconnectError: string read GetLastReconnectError;
    /// Fired when the broker signals Connection.Blocked (resource alarm:
    /// memory/disk full) and stops accepting publishes. Runs on a pool thread.
    /// Typical: set an atomic flag to pause publishing until OnUnblocked.
    property OnBlocked: TAMQPConnectionBlockedEvent read FOnBlocked write FOnBlocked;
    /// Fired when the broker leaves the resource alarm (Connection.Unblocked) and
    /// accepts publishes again. Runs on a pool thread.
    property OnUnblocked: TAMQPConnectionEvent read FOnUnblocked write FOnUnblocked;
  end;

/// How many reconnect threads exist in the process right now (for tests):
/// drops to zero only when the last one leaves Execute, after its last access to the
/// connection. It is what proves that Close/Destroy wait for reconnection to actually exit.
function AmqpReconnectThreadsAlive: Integer;

/// The TLS options the client uses for AParams: CA, client certificate,
/// verification and backend (tbDefault resolved by AmqpDefaultTlsBackend).
/// The handshake deadline is the common's default (15 s). Public for the
/// unit tests of the mapping.
function AmqpClientTlsOptions(const AParams: TAMQPConnectionParams): TPcTlsOptions;

implementation

uses
  AMQP.Channel.Methods,
  PascalCommon.Tracing,
  AMQP.Tracing,
  PascalCommon.Transport.Tcp,
  PascalCommon.Transport.Tls;

const
  AMQP_RPC_TIMEOUT_MS = 30000; // maximum time waiting for an RPC reply

type
  { Dedicated reconnection thread (replaces TThread.CreateAnonymousThread,
    which does not exist with method pointers / in FPC). It does NOT free itself: the connection
    keeps the reference and joins it (StopReconnect, ReadThreadFinished) — that is how
    Close knows it actually exited. }
  TAMQPReconnectThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  { Work items dispatched to the pool (PcPool, from pascal-common-faa,
    or the channel's own pool). They replace the
    TTask.Run closures: each item carries the captured data in fields and the
    "in flight" counter is decremented in the finally of Execute — the same
    contract as the original closures. The pool frees the item after Execute. }
  TAMQPDeliveryWork = class(TPcWorkItem)
  private
    FChannel: TAMQPChannel;
    FCallback: TAMQPConsumerCallback;
    FDelivery: TAMQPDelivery;
    FQueue: string;
  public
    constructor Create(AChannel: TAMQPChannel;
      const ACallback: TAMQPConsumerCallback; const ADelivery: TAMQPDelivery;
      const AConsumer: TAMQPConsumerInfo);
    procedure Execute; override;
  end;

  TAMQPReturnWork = class(TPcWorkItem)
  private
    FChannel: TAMQPChannel;
    FCallback: TAMQPBasicReturnCallback;
    FReturned: TAMQPReturnedMessage;
  public
    constructor Create(AChannel: TAMQPChannel;
      const ACallback: TAMQPBasicReturnCallback;
      const AReturned: TAMQPReturnedMessage);
    procedure Execute; override;
  end;

  TAMQPConfirmWork = class(TPcWorkItem)
  private
    FChannel: TAMQPChannel;
    FCallback: TAMQPConfirmCallback;
    FSeqNo: UInt64;
    FAck: Boolean;
  public
    constructor Create(AChannel: TAMQPChannel;
      const ACallback: TAMQPConfirmCallback; ASeqNo: UInt64; AAck: Boolean);
    procedure Execute; override;
  end;

  TAMQPBlockedWork = class(TPcWorkItem)
  private
    FConnection: TAMQPConnection;
    FCallback: TAMQPConnectionBlockedEvent;
    FReason: string;
  public
    constructor Create(AConnection: TAMQPConnection;
      const ACallback: TAMQPConnectionBlockedEvent; const AReason: string);
    procedure Execute; override;
  end;

  TAMQPUnblockedWork = class(TPcWorkItem)
  private
    FConnection: TAMQPConnection;
    FCallback: TAMQPConnectionEvent;
  public
    constructor Create(AConnection: TAMQPConnection;
      const ACallback: TAMQPConnectionEvent);
    procedure Execute; override;
  end;

{ TAMQPReconnectThread }

var
  GReconnectThreadsAlive: Integer = 0;

function AmqpReconnectThreadsAlive: Integer;
begin
  Result := PcAtomicGet(GReconnectThreadsAlive);
end;

function AmqpClientTlsOptions(const AParams: TAMQPConnectionParams): TPcTlsOptions;
begin
  Result := Default(TPcTlsOptions);
  Result.CaFile := AParams.TlsCaFile;
  Result.CertFile := AParams.TlsCertFile;
  Result.KeyFile := AParams.TlsKeyFile;
  Result.CertPassword := AParams.TlsCertPassword;
  Result.SkipServerVerification := not AParams.TlsVerifyPeer;
  Result.Backend := AmqpResolveTlsBackend(AParams.TlsBackend);
end;

constructor TAMQPReconnectThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := False;
  PcAtomicInc(GReconnectThreadsAlive); // before the thread can start
  inherited Create(False);
end;

procedure TAMQPReconnectThread.Execute;
begin
  try
    FConnection.RunReconnect;
  finally
    PcAtomicDec(GReconnectThreadsAlive);
  end;
end;

{ TAMQPDeliveryWork }

constructor TAMQPDeliveryWork.Create(AChannel: TAMQPChannel;
  const ACallback: TAMQPConsumerCallback; const ADelivery: TAMQPDelivery;
  const AConsumer: TAMQPConsumerInfo);
begin
  inherited Create;
  FChannel := AChannel;
  FCallback := ACallback;
  FDelivery := ADelivery;
  FQueue := AConsumer.Queue;
end;

procedure TAMQPDeliveryWork.Execute;
var
  LSpan: IPcSpan;
begin
  try
    // 'process <queue>' span, child of the message's traceparent (nil with the
    // tracer off). Current during the callback; it is born and ends here,
    // on the pool thread.
    LSpan := AmqpStartProcessSpan(FQueue, FDelivery.Properties,
      FDelivery.RoutingKey, Length(FDelivery.Body), FDelivery.DeliveryTag,
      FDelivery.Redelivered, FChannel.FConnection.FParams.Host,
      FChannel.FConnection.FParams.Port);
    try
      FCallback(FChannel, FDelivery);
    except
      on E: Exception do
      begin
        // Does not re-raise: the pool would swallow the exception silently, and the delivery
        // would stay unacked until the connection dropped (with a low Qos, the whole
        // consumer stopped). Here it becomes OnConsumerError + Nack — with
        // 'process' still current, so the handler's log comes out with the message's
        // trace id (up to 0.4.0 the span closed before). The span ends
        // afterwards, with the callback's error.
        try
          FChannel.HandleConsumerError(FDelivery, E);
        finally
          AmqpFinishSpan(LSpan, E);
          LSpan := nil;
        end;
      end;
    end;
    AmqpFinishSpan(LSpan, nil); // nil-safe: on the exception path it is already closed
    LSpan := nil;
  finally
    // A callback that returned without acknowledging (ack deferred by the application) leaves the
    // set here: the set only matters while the callback runs. (The tags
    // belong to the channel, no-ack or not: removing one that never went in is harmless.)
    FChannel.TakeUnsettled(FDelivery.DeliveryTag);
    if FDelivery.Properties.Has(bpHeaders) and Assigned(FDelivery.Properties.Headers) then
      FDelivery.Properties.Headers.Free;
    PcAtomicDec(FChannel.FInFlight);
  end;
end;

{ TAMQPReturnWork }

constructor TAMQPReturnWork.Create(AChannel: TAMQPChannel;
  const ACallback: TAMQPBasicReturnCallback;
  const AReturned: TAMQPReturnedMessage);
begin
  inherited Create;
  FChannel := AChannel;
  FCallback := ACallback;
  FReturned := AReturned;
end;

procedure TAMQPReturnWork.Execute;
begin
  try
    FCallback(FChannel, FReturned);
  finally
    if FReturned.Properties.Has(bpHeaders) and Assigned(FReturned.Properties.Headers) then
      FReturned.Properties.Headers.Free;
    PcAtomicDec(FChannel.FInFlight);
  end;
end;

{ TAMQPConfirmWork }

constructor TAMQPConfirmWork.Create(AChannel: TAMQPChannel;
  const ACallback: TAMQPConfirmCallback; ASeqNo: UInt64; AAck: Boolean);
begin
  inherited Create;
  FChannel := AChannel;
  FCallback := ACallback;
  FSeqNo := ASeqNo;
  FAck := AAck;
end;

procedure TAMQPConfirmWork.Execute;
begin
  try
    FCallback(FChannel, FSeqNo, FAck);
  finally
    PcAtomicDec(FChannel.FInFlight);
  end;
end;

{ TAMQPBlockedWork }

constructor TAMQPBlockedWork.Create(AConnection: TAMQPConnection;
  const ACallback: TAMQPConnectionBlockedEvent; const AReason: string);
begin
  inherited Create;
  FConnection := AConnection;
  FCallback := ACallback;
  FReason := AReason;
end;

procedure TAMQPBlockedWork.Execute;
begin
  try
    FCallback(FConnection, FReason);
  finally
    PcAtomicDec(FConnection.FInFlightConn);
  end;
end;

{ TAMQPUnblockedWork }

constructor TAMQPUnblockedWork.Create(AConnection: TAMQPConnection;
  const ACallback: TAMQPConnectionEvent);
begin
  inherited Create;
  FConnection := AConnection;
  FCallback := ACallback;
end;

procedure TAMQPUnblockedWork.Execute;
begin
  try
    FCallback(FConnection);
  finally
    PcAtomicDec(FConnection.FInFlightConn);
  end;
end;

{ TAMQPGetResult }

function TAMQPGetResult.BodyAsText: string;
begin
  Result := AmqpUtf8Decode(Body);
end;

{ TAMQPDelivery }

function TAMQPDelivery.BodyAsText: string;
begin
  Result := AmqpUtf8Decode(Body);
end;

{ TAMQPReturnedMessage }

function TAMQPReturnedMessage.BodyAsText: string;
begin
  Result := AmqpUtf8Decode(Body);
end;

{ TAMQPConnectionParams }

class function TAMQPConnectionParams.Localhost: TAMQPConnectionParams;
begin
  Result.Host := 'localhost';
  Result.Port := 5672;
  Result.VirtualHost := '/';
  Result.User := 'guest';
  Result.Password := 'guest';
  Result.ChannelMax := 2047;
  Result.FrameMax := 131072;
  Result.Heartbeat := 60;
  Result.AutoReconnect := False;
  Result.ReconnectDelayMs := 2000;
  Result.MaxReconnectAttempts := 0; // infinite
  Result.ConnectionName := '';
  Result.RepublishUnconfirmedOnReconnect := False;
  Result.ConnectTimeoutMs := 0;
  Result.UseTls := False;
  Result.TlsVerifyPeer := True;
  Result.TlsServerName := '';
  Result.TlsCaFile := '';
  Result.TlsCertFile := '';
  Result.TlsKeyFile := '';
  Result.TlsCertPassword := '';
  Result.TlsBackend := tbDefault;
end;

class function TAMQPConnectionParams.LocalhostTls: TAMQPConnectionParams;
begin
  Result := Localhost;
  Result.Port := 5671;
  Result.UseTls := True;
  Result.TlsVerifyPeer := False; // dev: accepts a self-signed cert
end;

{ TAMQPReaderThread }

constructor TAMQPReaderThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := False;
  inherited Create(False);
end;

procedure TAMQPReaderThread.Execute;
var
  LFrame: TAMQPFrame;
begin
  try
    while not Terminated do
    begin
      LFrame := TAMQPFrame.ReadFrom(FConnection.FStream);
      // atomic, for the heartbeat thread
      PcAtomicWrite64(FConnection.FLastReadTick, PcTickMs);
      FConnection.DispatchFrame(LFrame);
    end;
    FConnection.ReadThreadFinished('');
  except
    on E: Exception do
      // end of stream (socket closed) or protocol error: ends the thread.
      FConnection.ReadThreadFinished(E.Message);
  end;
end;

{ TAMQPHeartbeatThread }

constructor TAMQPHeartbeatThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := False;
  inherited Create(False);
end;

procedure TAMQPHeartbeatThread.Execute;
var
  LWaitMs: Cardinal;
begin
  // Wakes up every half interval (minimum 1s); the wait is interruptible
  // via FHbStopEvent (we do not use TTimer).
  LWaitMs := (Cardinal(FConnection.FNegotiated.Heartbeat) * 1000) div 2;
  if LWaitMs < 1000 then
    LWaitMs := 1000;
  while not Terminated do
  begin
    if FConnection.FHbStopEvent.WaitFor(LWaitMs) = wrSignaled then
      Break; // stop requested
    if Terminated then
      Break;
    FConnection.HeartbeatTick;
  end;
end;

{ TAMQPConnection }

constructor TAMQPConnection.Create(const AParams: TAMQPConnectionParams);
begin
  inherited Create;
  FParams := AParams;
  FWriteLock := TCriticalSection.Create;
  FChannelsLock := TCriticalSection.Create;
  FChannels := TDictionary<Word, TAMQPChannel>.Create;
  FCloseOkEvent := TEvent.Create(nil, True, False, '');
  FHbStopEvent := TEvent.Create(nil, True, False, '');
  FReconnectLock := TCriticalSection.Create;
  FReconnectWake := TEvent.Create(nil, True, False, '');
  FStateLock := TCriticalSection.Create;
end;

destructor TAMQPConnection.Destroy;
var
  LChan: TAMQPChannel;
begin
  // Close is idempotent: aborts reconnection, stops threads and closes the socket.
  try
    Close;
  except
  end;
  DrainConnCallbacks; // in-flight Blocked/Unblocked callbacks captured Self
  // Frees channels the user did not free (each Free calls UnregisterChannel,
  // which removes it from the dictionary; that is why we iterate over a copy).
  if Assigned(FChannels) then
  begin
    for LChan in FChannels.Values.ToArray do
      LChan.Free;
    FChannels.Free;
  end;
  FChannelsLock.Free;
  FWriteLock.Free;
  FCloseOkEvent.Free;
  FHbStopEvent.Free;
  FReconnectWake.Free;
  FReconnectLock.Free;
  FStateLock.Free;
  inherited;
end;

function TAMQPConnection.UnavailableMessage: string;
var
  LLost, LAttempt: string;
begin
  FStateLock.Enter;
  try
    LLost := FLostReason;
    LAttempt := FLastReconnectError;
  finally
    FStateLock.Leave;
  end;
  if FDeliberateClose then
    Result := 'connection was closed by the application'
  else if FParams.AutoReconnect and (LLost <> '') then
  begin
    // The start of the text is the old one: some code matches on it.
    Result := 'connection unavailable right now (reconnecting; ' + LLost;
    if LAttempt <> '' then
      Result := Result + '; last attempt failed: ' + LAttempt;
    Result := Result + ')';
  end
  else if LLost <> '' then
    Result := LLost
  else
    Result := 'connection is not open';
end;

function TAMQPConnection.GetLastReconnectError: string;
begin
  FStateLock.Enter;
  try
    Result := FLastReconnectError;
  finally
    FStateLock.Leave;
  end;
end;

procedure TAMQPConnection.WriteFrameNoLock(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
var
  LFrame: TAMQPFrame;
begin
  if not Assigned(FStream) then
    raise EAMQPConnection.Create(UnavailableMessage);
  LFrame := TAMQPFrame.Create(AFrameType, AChannel, APayload);
  try
    LFrame.WriteTo(FStream);
  except
    // A drop not yet noticed by the read thread arrives here as a stream
    // error (the RTL's EWriteError, EAMQPTransport, EAMQPTls...), different on
    // each compiler and backend. For the caller it is always the same thing.
    on E: EAMQPConnection do
      raise;
    on E: Exception do
      raise EAMQPConnection.CreateFmt('connection write failed (%s: %s)',
        [E.ClassName, E.Message]);
  end;
  // Atomic: read by the heartbeat thread; on Win32 a 64-bit store is not
  // atomic and could be "torn" (see PcAtomicRead64 in HeartbeatTick).
  PcAtomicWrite64(FLastWriteTick, PcTickMs);
end;

procedure TAMQPConnection.SendFrameNoLock(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
begin
  // Assumes FWriteLock already acquired (sending a group of frames, e.g. Publish).
  if PcAtomicGet(FWritable) = 0 then
    raise EAMQPConnection.Create(UnavailableMessage);
  WriteFrameNoLock(AFrameType, AChannel, APayload);
end;

procedure TAMQPConnection.SendHandshakeMethod(const APayload: TBytes);
begin
  FWriteLock.Enter;
  try
    WriteFrameNoLock(AMQP_FRAME_METHOD, AMQP_CHANNEL_CONNECTION, APayload);
  finally
    FWriteLock.Leave;
  end;
end;

procedure TAMQPConnection.SendFrame(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
begin
  FWriteLock.Enter;
  try
    SendFrameNoLock(AFrameType, AChannel, APayload);
  finally
    FWriteLock.Leave;
  end;
end;

procedure TAMQPConnection.SendMethod(AChannel: Word; const APayload: TBytes);
begin
  SendFrame(AMQP_FRAME_METHOD, AChannel, APayload);
end;

{ --- Synchronous reading (handshake only) --- }

function TAMQPConnection.NextFrame: TAMQPFrame;
begin
  repeat
    Result := TAMQPFrame.ReadFrom(FStream);
  until not Result.IsHeartbeat;
end;

function TAMQPConnection.NextMethodOn(AExpectChannel: Word;
  out AId: TAMQPMethodId): TAMQPReader;
var
  LFrame: TAMQPFrame;
  LConnClose: TAMQPConnectionClose;
begin
  LFrame := NextFrame;
  if not LFrame.IsMethod then
    raise EAMQPConnection.CreateFmt(
      'unexpected frame (type %d, channel %d)', [LFrame.FrameType, LFrame.Channel]);

  Result := TAMQPReader.Create(LFrame.Payload);
  try
    AId := ReadMethodHeader(Result);
    if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
    begin
      LConnClose := DecodeClose(Result);
      raise EAMQPConnection.CreateFmt('connection refused by server: %d %s',
        [LConnClose.ReplyCode, LConnClose.ReplyText]);
    end;
    if LFrame.Channel <> AExpectChannel then
      raise EAMQPConnection.CreateFmt('unexpected channel response: %d (expected %d)',
        [LFrame.Channel, AExpectChannel]);
  except
    Result.Free;
    raise;
  end;
end;

function TAMQPConnection.ExpectMethod(AExpectChannel, AClassId,
  AMethodId: Word): TAMQPReader;
var
  LId: TAMQPMethodId;
begin
  Result := NextMethodOn(AExpectChannel, LId);
  try
    if not LId.Matches(AClassId, AMethodId) then
      raise EAMQPConnection.CreateFmt(
        'unexpected response: method %d/%d (expected %d/%d)',
        [LId.ClassId, LId.MethodId, AClassId, AMethodId]);
  except
    Result.Free;
    raise;
  end;
end;

procedure TAMQPConnection.Handshake;
var
  LReader: TAMQPReader;
  LStart: TAMQPConnectionStart;
  LServerTune: TAMQPConnectionTune;
  LProps: TAMQPFieldTable;
begin
  WriteProtocolHeader(FStream);

  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START);
  try
    LStart := DecodeStart(LReader);
  finally
    LReader.Free;
  end;

  try
    if not LStart.SupportsMechanism(AMQP_AUTH_PLAIN) then
      raise EAMQPConnection.CreateFmt(
        'server does not offer mechanism %s (offers: %s)',
        [AMQP_AUTH_PLAIN, LStart.Mechanisms]);

    LProps := BuildClientProperties;
    try
      SendHandshakeMethod(BuildStartOk(LProps, AMQP_AUTH_PLAIN,
        PlainAuthResponse(FParams.User, FParams.Password), AMQP_LOCALE_DEFAULT));
    finally
      LProps.Free;
    end;
  finally
    LStart.ServerProperties.Free;
  end;

  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE);
  try
    LServerTune := DecodeTune(LReader);
  finally
    LReader.Free;
  end;

  FNegotiated := NegotiateTune(LServerTune,
    FParams.ChannelMax, FParams.FrameMax, FParams.Heartbeat);
  SendHandshakeMethod(BuildTuneOk(FNegotiated));

  SendHandshakeMethod(BuildOpen(FParams.VirtualHost));
  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK);
  try
    DecodeOpenOk(LReader);
  finally
    LReader.Free;
  end;
end;

function TAMQPConnection.BuildClientProperties: TAMQPFieldTable;
begin
  Result := DefaultClientProperties;
  if FParams.ConnectionName <> '' then
    Result.Put('connection_name', FParams.ConnectionName);
end;

procedure TAMQPConnection.EstablishConnection;
var
  LTarget: string;
begin
  // Creates the socket, does the handshake (synchronous, inline) and starts the threads.
  // Used both by the initial Open and by reconnection.
  try
    if FParams.UseTls then
    begin
      LTarget := FParams.TlsServerName;
      if LTarget = '' then
        LTarget := FParams.Host;
      // The TLS handshake runs here, on this thread, with the common's
      // default deadline (15 s); the deadline is removed once it's done.
      // SNI and the hostname check use LTarget, the TCP connect uses Host.
      FEndpoint := PcTlsWrap(
        PcTcpConnect(FParams.Host, FParams.Port,
          AmqpTcpOptions(FParams.ConnectTimeoutMs)),
        LTarget, AmqpClientTlsOptions(FParams));
    end
    else
      FEndpoint := PcTcpConnect(FParams.Host, FParams.Port,
        AmqpTcpOptions(FParams.ConnectTimeoutMs));
  except
    // T6: no EPcTransport* leaves the library. PcTlsWrap owns (and frees)
    // the TCP endpoint also when the handshake fails.
    on E: EPcTransport do
      AmqpRaiseTransportError(E);
  end;
  FStream := TAMQPEndpointStream.Create(FEndpoint);

  Handshake; // before any thread (now over TLS, if enabled)

  FLastWriteTick := PcTickMs;
  FLastReadTick := FLastWriteTick;
  FServerCloseReason := '';
  PcAtomicSet(FWritable, 1); // before the read thread: it replies Close-Ok
  StartReadThread;     // from here on, only the thread reads the socket
  StartHeartbeatThread;
  FIsOpen := True;
end;

procedure TAMQPConnection.CloseSocketStream;
begin
  PcAtomicSet(FWritable, 0);
  if Assigned(FEndpoint) then
    try
      FEndpoint.CloseAbort;
    except
    end;
  FreeAndNil(FStream);
  FreeAndNil(FEndpoint);
end;

procedure TAMQPConnection.Open;
begin
  if FIsOpen then
    raise EAMQPConnection.Create('connection is already open');
  // Before the connection exists: a drop right after Open records its
  // reason, which must not be erased here.
  FStateLock.Enter;
  try
    FLostReason := '';
  finally
    FStateLock.Leave;
  end;
  EstablishConnection;
end;

procedure TAMQPConnection.DropConnectionForTest;
begin
  if Assigned(FEndpoint) then
    try
      FEndpoint.CloseAbort; // the read thread will notice and trigger reconnection
    except
    end;
end;

{ --- Read thread --- }

procedure TAMQPConnection.StartReadThread;
begin
  FReadThread := TAMQPReaderThread.Create(Self);
end;

procedure TAMQPConnection.StopReadThread;
begin
  if Assigned(FReadThread) then
  begin
    FReadThread.Terminate;
    if Assigned(FEndpoint) then
      try
        FEndpoint.CloseAbort; // unblocks the thread's ReadFrom
      except
      end;
    FReadThread.WaitFor;
    FreeAndNil(FReadThread);
  end;
end;

{ --- Heartbeat thread --- }

procedure TAMQPConnection.StartHeartbeatThread;
begin
  if FNegotiated.Heartbeat = 0 then
    Exit; // heartbeat disabled in the negotiation
  FHbStopEvent.ResetEvent;
  FHeartbeatThread := TAMQPHeartbeatThread.Create(Self);
end;

procedure TAMQPConnection.StopHeartbeatThread;
begin
  if Assigned(FHeartbeatThread) then
  begin
    FHeartbeatThread.Terminate;
    FHbStopEvent.SetEvent; // interrupts the wait immediately
    FHeartbeatThread.WaitFor;
    FreeAndNil(FHeartbeatThread);
  end;
end;

procedure TAMQPConnection.HeartbeatTick;
var
  LIntervalMs: UInt64;
  LNow, LLastRead, LLastWrite: UInt64;
begin
  LIntervalMs := UInt64(FNegotiated.Heartbeat) * 1000;
  if LIntervalMs = 0 then
    Exit;
  LNow := PcTickMs;
  // Atomic reads (the ticks are written by other threads; on Win32 a
  // 64-bit load can be "torn" and produce an absurd delta).
  LLastRead := PcAtomicRead64(FLastReadTick);
  LLastWrite := PcAtomicRead64(FLastWriteTick);

  // Dead connection: no frame received in 2x the interval (the server also
  // sends heartbeats). Closes the socket to unblock the read thread.
  if (LNow - LLastRead) > (2 * LIntervalMs) then
  begin
    if Assigned(FEndpoint) then
      try
        FEndpoint.CloseAbort;
      except
      end;
    Exit;
  end;

  // Sending idle for >= half the interval: send a heartbeat.
  if (LNow - LLastWrite) >= (LIntervalMs div 2) then
    try
      SendFrame(AMQP_FRAME_HEARTBEAT, AMQP_CHANNEL_CONNECTION, nil);
    except
      // write error: the read thread will notice the dropped socket
    end;
end;

procedure TAMQPConnection.ReadThreadFinished(const AError: string);
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
  LMsg: string;
begin
  if FServerCloseReason <> '' then
    LMsg := 'connection closed by server: ' + FServerCloseReason
  else if AError <> '' then
    LMsg := 'connection closed: ' + AError
  else
    LMsg := 'connection closed';
  // The reason BEFORE the flags: whoever sees IsOpen = False or FWritable = 0 must
  // find the reason already recorded (the reverse order gave, on Linux, a publish with
  // "connection is not open" right after IsOpen dropped). During
  // reconnection, whoever stops this thread is the reconnection itself tearing down an
  // attempt that failed: the reason that matters is still the original drop's.
  FStateLock.Enter;
  try
    if (not FReconnecting) or (FLostReason = '') then
      FLostReason := LMsg;
  finally
    FStateLock.Leave;
  end;
  PcAtomicSet(FWritable, 0); // from here on, publish raises with the reason above
  FIsOpen := False;
  // Wakes whoever is waiting for Close-Ok and any pending RPC on the channels.
  FCloseOkEvent.SetEvent;
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.SignalError(LMsg);

  // Unexpected drop: triggers reconnection (on a thread of its own, since this is the
  // read thread that is finishing). Under FReconnectLock: either Close already
  // set FDeliberateClose and nothing is created, or the thread is born first and Close
  // finds it to join.
  FReconnectLock.Enter;
  try
    if FParams.AutoReconnect and (not FDeliberateClose) and (not FReconnecting) then
    begin
      // The thread of the previous (successful) reconnection already cleared FReconnecting,
      // which is the last thing it does: it is exiting, and the join is short.
      if Assigned(FReconnectThread) then
      begin
        FReconnectThread.WaitFor;
        FreeAndNil(FReconnectThread);
      end;
      FReconnecting := True;
      FReconnectThread := TAMQPReconnectThread.Create(Self);
    end;
  finally
    FReconnectLock.Leave;
  end;
end;

procedure TAMQPConnection.DrainAllChannels;
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
begin
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.DrainInFlight;
end;

procedure TAMQPConnection.RecoverAllChannels;
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
  LPhase: TAMQPRecoveryPhase;
  I, J, LEntered: Integer;
begin
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  // Creation order (ChannelId), not the dictionary's, which is the hash's —
  // measured: with channels 1, 2 and 3 the TDictionary returned 3 first.
  // Insertion sort: TArray.Sort<T> does not exist in FPC, and there are few channels.
  for I := 1 to High(LChannels) do
  begin
    LChan := LChannels[I];
    J := I - 1;
    while (J >= 0) and (LChannels[J].ChannelId > LChan.ChannelId) do
    begin
      LChannels[J + 1] := LChannels[J];
      Dec(J);
    end;
    LChannels[J + 1] := LChan;
  end;

  // Phase by phase over the whole connection (see TAMQPRecoveryPhase): the declare of
  // one channel must exist before the bind/consume of another. Each channel holds
  // its own FRpcLock from BeginRecover to EndRecover; only this thread holds
  // more than one, always in ChannelId order, and whoever uses a channel takes only
  // that one — there is no cycle.
  LEntered := 0;
  try
    for I := 0 to High(LChannels) do
    begin
      LChannels[I].BeginRecover;
      LEntered := I + 1;
    end;
    for LPhase := rpExchange to rpConsume do
      for I := 0 to High(LChannels) do
        LChannels[I].ReplayRecovery(LPhase);
    // Topology restored and confirm mode re-armed: republishes what was left
    // unconfirmed by the drop (if enabled).
    for I := 0 to High(LChannels) do
      LChannels[I].ResendUnconfirmed;
  finally
    for I := LEntered - 1 downto 0 do
      LChannels[I].EndRecover;
  end;
end;

procedure TAMQPConnection.StopReconnect;
var
  LThread: TThread;
begin
  // Marks the deliberate close, wakes the wait between attempts and JOINS the
  // reconnect thread: returns only when it has actually exited, because after this
  // Close touches the socket and Destroy frees the object it uses. No deadline
  // on purpose: the wait between attempts ends immediately (FReconnectWake);
  // what can take time is an attempt already in progress (connect/handshake/replay),
  // bounded by its own timeouts. Giving up earlier would be a use-after-free.
  FReconnectLock.Enter;
  try
    FDeliberateClose := True;
    FReconnectWake.SetEvent;
    LThread := FReconnectThread;
    // Close called from a callback running ON the reconnect thread
    // (OnDisconnect, OnReconnect, OnReconnectFailed): it cannot join
    // itself. That is left to the next Close/Destroy, from another thread; the
    // reconnection loop sees FDeliberateClose and exits.
    if Assigned(LThread) and (LThread.ThreadID = TThread.CurrentThread.ThreadID) then
      LThread := nil
    else
      FReconnectThread := nil; // the join below is the only one (on Unix it is not idempotent)
  finally
    FReconnectLock.Leave;
  end;
  if Assigned(LThread) then
  begin
    LThread.WaitFor;
    LThread.Free;
  end;
end;

procedure TAMQPConnection.RunReconnect;
var
  LAttempt: Integer;
  LDelay: Cardinal;
begin
  // Runs on TAMQPReconnectThread; FReconnecting is already True.
  try
    StopHeartbeatThread;
    StopReadThread;    // waits for the old read thread to finish
    DrainAllChannels;  // old callbacks finish (acks fail on the dead socket)
    CloseSocketStream;

    if Assigned(FOnDisconnect) then
      try FOnDisconnect(Self); except end;

    LDelay := FParams.ReconnectDelayMs;
    if LDelay = 0 then
      LDelay := 2000;

    LAttempt := 0;
    while not FDeliberateClose do
    begin
      // Not a Sleep: Close signals FReconnectWake and the wait ends immediately.
      FReconnectWake.WaitFor(LDelay);
      if FDeliberateClose then
        Break;
      Inc(LAttempt);
      try
        EstablishConnection; // new socket + handshake + threads (FIsOpen := True)
        RecoverAllChannels;  // reopens channels and replays the recorded topology
        // FLostReason stays: it is only read with the connection not writable, and the next
        // drop records its own before the connection stops being writable.
        FStateLock.Enter;
        try
          FLastReconnectError := '';
        finally
          FStateLock.Leave;
        end;
        if Assigned(FOnReconnect) then
          try FOnReconnect(Self); except end;
        Exit; // success
      except
        on E: Exception do
        begin
          // Attempt failed: clears the partial state, records the reason and tries
          // again. The reason is NOT swallowed: it goes to LastReconnectError,
          // to the text of the publish's EAMQPConnection and to OnReconnectError
          // — before, a 404 in the replay repeated forever with no signal at all.
          FIsOpen := False;
          StopHeartbeatThread;
          StopReadThread;
          CloseSocketStream;
          FStateLock.Enter;
          try
            FLastReconnectError := E.Message;
          finally
            FStateLock.Leave;
          end;
          if Assigned(FOnReconnectError) then
            try FOnReconnectError(Self, LAttempt, E.Message); except end;
          if (FParams.MaxReconnectAttempts > 0) and
             (LAttempt >= FParams.MaxReconnectAttempts) then
          begin
            if Assigned(FOnReconnectFailed) then
              try FOnReconnectFailed(Self); except end;
            Break;
          end;
        end;
      end;
    end;
  finally
    FReconnecting := False;
  end;
end;

procedure TAMQPConnection.DispatchFrame(const AFrame: TAMQPFrame);
var
  LChan: TAMQPChannel;
begin
  if AFrame.IsHeartbeat then
    Exit; // heartbeat handled in item 4

  if AFrame.Channel = AMQP_CHANNEL_CONNECTION then
  begin
    HandleConnectionFrame(AFrame);
    Exit;
  end;

  FChannelsLock.Enter;
  try
    if not FChannels.TryGetValue(AFrame.Channel, LChan) then
      LChan := nil;
    if Assigned(LChan) then
      LChan.HandleFrame(AFrame);
  finally
    FChannelsLock.Leave;
  end;
end;

procedure TAMQPConnection.HandleConnectionFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LClose: TAMQPConnectionClose;
begin
  if not AFrame.IsMethod then
    Exit;
  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);
    if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK) then
    begin
      FCloseOkEvent.SetEvent;
    end
    else if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
    begin
      LClose := DecodeClose(LReader);
      FIsOpen := False;
      FServerCloseReason := Format('%d %s', [LClose.ReplyCode, LClose.ReplyText]);
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildCloseOk);
      FCloseOkEvent.SetEvent;
      // (channels will be signaled when the thread ends)
    end
    else if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED) then
      DispatchBlocked(DecodeBlocked(LReader))
    else if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_UNBLOCKED) then
      DispatchUnblocked;
  finally
    LReader.Free;
  end;
end;

procedure TAMQPConnection.DispatchBlocked(const AReason: string);
var
  LCallback: TAMQPConnectionBlockedEvent;
begin
  LCallback := FOnBlocked;
  if not Assigned(LCallback) then
    Exit;
  // Separate pool: the read thread does NOT run the user's callback.
  PcAtomicInc(FInFlightConn);
  PcPool.Queue(TAMQPBlockedWork.Create(Self, LCallback, AReason));
end;

procedure TAMQPConnection.DispatchUnblocked;
var
  LCallback: TAMQPConnectionEvent;
begin
  LCallback := FOnUnblocked;
  if not Assigned(LCallback) then
    Exit;
  PcAtomicInc(FInFlightConn);
  PcPool.Queue(TAMQPUnblockedWork.Create(Self, LCallback));
end;

procedure TAMQPConnection.DrainConnCallbacks;
begin
  // Waits for the in-flight Blocked/Unblocked callbacks (they captured Self) to finish
  // before the object is freed — same rationale as the channel's DrainInFlight.
  while PcAtomicGet(FInFlightConn) > 0 do
    Sleep(10);
end;

procedure TAMQPConnection.UnregisterChannel(AChannelId: Word);
begin
  FChannelsLock.Enter;
  try
    FChannels.Remove(AChannelId);
  finally
    FChannelsLock.Leave;
  end;
end;

function TAMQPConnection.CreateChannel(ADedicatedConsumerThread: Boolean): TAMQPChannel;
var
  LChan: TAMQPChannel;
begin
  if not FIsOpen then
    raise EAMQPConnection.Create('connection is not open');

  // Id allocation + dictionary insertion under the same lock (keeps two threads from
  // generating the same channel-id and the second Add raising/leaking the channel).
  FChannelsLock.Enter;
  try
    if (FNegotiated.ChannelMax > 0) and (FNextChannel >= FNegotiated.ChannelMax) then
      raise EAMQPConnection.CreateFmt(
        'channel limit reached (%d); channel reuse is not yet implemented',
        [FNegotiated.ChannelMax]);
    Inc(FNextChannel);
    LChan := TAMQPChannel.Create(Self, FNextChannel);
    try
      if ADedicatedConsumerThread then
        LChan.FDispatchPool := TPcThreadPool.Create(1);
      FChannels.Add(LChan.ChannelId, LChan);
    except
      LChan.Free;
      raise;
    end;
  finally
    FChannelsLock.Leave;
  end;

  try
    LChan.Open; // RPC: uses the read thread already running (outside the lock)
  except
    UnregisterChannel(LChan.ChannelId);
    LChan.Free;
    raise;
  end;
  Result := LChan;
end;

procedure TAMQPConnection.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPConnectionClose;
begin
  // Idempotent. Signals a deliberate close and waits for any reconnection in
  // progress to end before touching the socket/threads (avoids a race).
  StopReconnect;

  if FIsOpen then
  begin
    FIsOpen := False;
    StopHeartbeatThread; // do not send heartbeats while closing
    LClose.ReplyCode := AReplyCode;
    LClose.ReplyText := AReplyText;
    LClose.ClassId := 0;
    LClose.MethodId := 0;
    try
      FCloseOkEvent.ResetEvent;
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildClose(LClose));
      FCloseOkEvent.WaitFor(3000);
    except
    end;
  end;

  StopHeartbeatThread;
  StopReadThread; // ends the thread and closes the socket
  CloseSocketStream;
end;

{ TAMQPChannel }

constructor TAMQPChannel.Create(AConnection: TAMQPConnection; AChannelId: Word);
begin
  inherited Create;
  FConnection := AConnection;
  FChannelId := AChannelId;
  FRpcLock := TCriticalSection.Create;
  FRpcEvent := TEvent.Create(nil, True, False, '');
  FConsumers := TDictionary<string, TAMQPConsumerCallback>.Create;
  FConsumerInfo := TDictionary<string, TAMQPConsumerInfo>.Create;
  FConsumersLock := TCriticalSection.Create;
  FUnsettled := TDictionary<UInt64, Boolean>.Create;
  FSettleLock := TCriticalSection.Create;
  FAckLock := TCriticalSection.Create;
  FConfirmMon := TPcMonitor.Create;
  FUnconfirmed := TDictionary<UInt64, Boolean>.Create;
  FNacked := TDictionary<UInt64, Boolean>.Create;
  FResendBuffer := TDictionary<UInt64, TAMQPRawPublish>.Create;
  FRepublish := AConnection.FParams.RepublishUnconfirmedOnReconnect;
  FRecovery := TList<TAMQPRecoveryAction>.Create;
  FRecoveryLock := TCriticalSection.Create;
  FAsmState := asIdle;
end;

destructor TAMQPChannel.Destroy;
begin
  if FIsOpen and (not FClosed) and FConnection.IsOpen then
    try
      Close;
    except
    end;
  // Always unregisters: Close (which also unregisters) does not run when the channel
  // was already closed — the typical case, the server having sent Channel.Close for a
  // channel error. Without this the connection's dictionary keeps a dangling
  // pointer and its destructor blows up. Removing a missing key is a no-op.
  FConnection.UnregisterChannel(FChannelId);
  DrainInFlight; // makes sure no callback still uses this channel
  FDispatchPool.Free; // nil-safe; if assigned, there are no more items in flight
  FRecovery.Free;
  FRecoveryLock.Free;
  FUnconfirmed.Free;
  FNacked.Free;
  FResendBuffer.Free;
  FConfirmMon.Free;
  FConsumers.Free;
  FConsumerInfo.Free;
  FConsumersLock.Free;
  FUnsettled.Free;
  FSettleLock.Free;
  FAckLock.Free;
  FRpcEvent.Free;
  FRpcLock.Free;
  inherited;
end;

function RecoveryPhaseOf(const APayload: TBytes): TAMQPRecoveryPhase;
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LReader := TAMQPReader.Create(APayload);
  try
    LId := ReadMethodHeader(LReader);
  finally
    LReader.Free;
  end;
  if LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE) then
    Result := rpExchange
  else if LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE) then
    Result := rpQueue
  else if LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_BIND) or
          LId.Matches(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_BIND) then
    Result := rpBind
  else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CONSUME) then
    Result := rpConsume
  else
    Result := rpChannel; // qos, confirm.select: the channel's own state
end;

procedure TAMQPChannel.AddRecovery(const APayload: TBytes; AAwaitReply: Boolean;
  AIsConsume: Boolean; const AConsumerTag: string;
  const ACallback: TAMQPConsumerCallback);
var
  LAction: TAMQPRecoveryAction;
begin
  LAction.Payload := APayload;
  LAction.AwaitReply := AAwaitReply;
  LAction.Phase := RecoveryPhaseOf(APayload);
  LAction.IsConsume := AIsConsume;
  LAction.ConsumerTag := AConsumerTag;
  LAction.Callback := ACallback;
  FRecoveryLock.Enter;
  try
    FRecovery.Add(LAction);
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.RemoveConsumerRecovery(const AConsumerTag: string);
var
  I: Integer;
begin
  FRecoveryLock.Enter;
  try
    for I := FRecovery.Count - 1 downto 0 do
      if FRecovery[I].IsConsume and (FRecovery[I].ConsumerTag = AConsumerTag) then
        FRecovery.Delete(I);
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.RemoveRecoveryMatching(const ACandidates: array of TBytes);

  function SameBytes(const A, B: TBytes): Boolean;
  begin
    Result := (Length(A) = Length(B)) and
      ((Length(A) = 0) or CompareMem(@A[0], @B[0], Length(A)));
  end;

var
  I, J: Integer;
begin
  FRecoveryLock.Enter;
  try
    for I := FRecovery.Count - 1 downto 0 do
      for J := 0 to High(ACandidates) do
        if SameBytes(FRecovery[I].Payload, ACandidates[J]) then
        begin
          FRecovery.Delete(I);
          Break;
        end;
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.RemoveBindRecovery(const AQueue, AExchange,
  ARoutingKey: string);
var
  LBind: TAMQPQueueBind;
  LCandFalse, LCandTrue: TBytes;
begin
  // Recovery keeps the bind's serialized payload; we rebuild the candidates
  // (in both no-wait variants, without Arguments) and remove the ones that match.
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := AQueue;
  LBind.ExchangeName := AExchange;
  LBind.RoutingKey := ARoutingKey;
  LBind.NoWait := False;
  LCandFalse := BuildQueueBind(LBind);
  LBind.NoWait := True;
  LCandTrue := BuildQueueBind(LBind);
  RemoveRecoveryMatching([LCandFalse, LCandTrue]);
end;

procedure TAMQPChannel.RemoveExchangeBindRecovery(const ADestination, ASource,
  ARoutingKey: string);
var
  LBind: TAMQPExchangeBinding;
  LCandFalse, LCandTrue: TBytes;
begin
  LBind := Default(TAMQPExchangeBinding);
  LBind.Destination := ADestination;
  LBind.Source := ASource;
  LBind.RoutingKey := ARoutingKey;
  LBind.NoWait := False;
  LCandFalse := BuildExchangeBind(LBind);
  LBind.NoWait := True;
  LCandTrue := BuildExchangeBind(LBind);
  RemoveRecoveryMatching([LCandFalse, LCandTrue]);
end;

procedure TAMQPChannel.BeginRecover;
begin
  // Runs on the reconnect thread. Holds FRpcLock until EndRecover so that
  // user RPCs on the channel wait for recovery to finish. FRpcLock is
  // recursive (critical section), so the internal CallRpc works.
  FRpcLock.Enter;
  try
    FClosed := False;
    FAckLock.Enter;
    try
      FClosing := False; // channel reopened in the new session
    finally
      FAckLock.Leave;
    end;
    FAsmState := asIdle;
    // New session: the broker restarts the WIRE numbering at 1. We keep the
    // USER seq-no (FPublishSeqNo) monotonic and store the session offset to
    // convert the broker's tags (see ResolveConfirms) — new publishes
    // get numbers ABOVE the old ones, with no collision. FNacked is NOT cleared: the
    // publishes lost in the drop remain reportable as unconfirmed
    // (FailAllUnconfirmed already moved them from FUnconfirmed to FNacked).
    FConfirmMon.Enter;
    try
      FConfirmBase := FPublishSeqNo;
      FUnconfirmed.Clear;
      FConfirmMon.PulseAll;
    finally
      FConfirmMon.Leave;
    end;
    // The new session's tags restart at 1. The set should already be
    // empty — RunReconnect drains every old callback (DrainAllChannels)
    // before getting here, and each one removes its own tag —, but a leftover
    // old tag would be mistaken for that of a new delivery.
    FSettleLock.Enter;
    try
      FUnsettled.Clear;
    finally
      FSettleLock.Leave;
    end;
    Open; // Channel.Open on the new socket (sets FIsOpen := True)

    FConsumersLock.Enter;
    try
      FConsumers.Clear; // old session's records; they come back in the rpConsume phase
    finally
      FConsumersLock.Leave;
    end;

    ReplayRecovery(rpChannel);
  except
    FRpcLock.Leave;
    raise;
  end;
end;

procedure TAMQPChannel.ReplayRecovery(APhase: TAMQPRecoveryPhase);
var
  LActions: TArray<TAMQPRecoveryAction>;
  LAction: TAMQPRecoveryAction;
begin
  FRecoveryLock.Enter;
  try
    LActions := FRecovery.ToArray;
  finally
    FRecoveryLock.Leave;
  end;

  for LAction in LActions do
  begin
    if LAction.Phase <> APhase then
      Continue;
    if LAction.IsConsume then
    begin
      FConsumersLock.Enter;
      try
        FConsumers.AddOrSetValue(LAction.ConsumerTag, LAction.Callback);
      finally
        FConsumersLock.Leave;
      end;
    end;
    if LAction.AwaitReply then
      CallRpc(LAction.Payload)
    else
      FConnection.SendMethod(FChannelId, LAction.Payload);
  end;
end;

procedure TAMQPChannel.EndRecover;
begin
  FRpcLock.Leave;
end;

function TAMQPChannel.CallRpc(const ARequest: TBytes): TBytes;
begin
  FRpcLock.Enter;
  try
    if FClosed then
      raise EAMQPChannel.Create('channel is closed');
    FRpcKind := rkNone;
    FRpcError := '';
    FRpcEvent.ResetEvent;
    FConnection.SendMethod(FChannelId, ARequest);
    if FRpcEvent.WaitFor(AMQP_RPC_TIMEOUT_MS) <> wrSignaled then
      raise EAMQPChannel.Create('timeout waiting for server response');
    case FRpcKind of
      rkMethod:
        Result := FRpcMethodPayload;
      rkError:
        raise EAMQPChannel.Create(FRpcError);
    else
      raise EAMQPChannel.Create('unexpected RPC response');
    end;
  finally
    FRpcLock.Leave;
  end;
end;

procedure TAMQPChannel.SignalMethod(const APayload: TBytes);
begin
  FRpcMethodPayload := APayload;
  FRpcKind := rkMethod;
  FRpcEvent.SetEvent;
end;

procedure TAMQPChannel.SignalMessage(const AMessage: TAMQPGetResult);
begin
  FRpcMessage := AMessage;
  FRpcKind := rkMessage;
  FRpcEvent.SetEvent;
end;

procedure TAMQPChannel.SignalError(const AMessage: string);
begin
  FClosed := True;
  FIsOpen := False;
  FRpcError := AMessage;
  FRpcKind := rkError;
  FRpcEvent.SetEvent;
  FailAllUnconfirmed; // pending publishes become "unconfirmed"
end;

procedure TAMQPChannel.Open;
var
  LPayload: TBytes;
  LReader: TAMQPReader;
begin
  LPayload := CallRpc(BuildChannelOpen);
  LReader := TAMQPReader.Create(LPayload);
  try
    ReadMethodHeader(LReader);
    DecodeChannelOpenOk(LReader);
  finally
    LReader.Free;
  end;
  FIsOpen := True;
end;

procedure TAMQPChannel.DeclareExchange(const ADeclare: TAMQPExchangeDeclare);
var
  LPayload: TBytes;
begin
  LPayload := BuildExchangeDeclare(ADeclare);
  if ADeclare.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Declare-Ok (no args, discarded)
  AddRecovery(LPayload, not ADeclare.NoWait);
end;

function TAMQPChannel.DeclareQueue(const ADeclare: TAMQPQueueDeclare): TAMQPQueueDeclareOk;
var
  LPayload: TBytes;
  LReader: TAMQPReader;
begin
  LPayload := BuildQueueDeclare(ADeclare);
  if ADeclare.NoWait then
  begin
    FConnection.SendMethod(FChannelId, LPayload);
    Result := Default(TAMQPQueueDeclareOk);
    Result.QueueName := ADeclare.QueueName;
  end
  else
  begin
    LReader := TAMQPReader.Create(CallRpc(LPayload));
    try
      ReadMethodHeader(LReader);
      Result := DecodeQueueDeclareOk(LReader);
    finally
      LReader.Free;
    end;
  end;
  AddRecovery(LPayload, not ADeclare.NoWait);
end;

procedure TAMQPChannel.BindQueue(const ABind: TAMQPQueueBind);
var
  LPayload: TBytes;
begin
  LPayload := BuildQueueBind(ABind);
  if ABind.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Bind-Ok (no args, discarded)
  AddRecovery(LPayload, not ABind.NoWait);
end;

procedure TAMQPChannel.UnbindQueue(const AUnbind: TAMQPQueueUnbind);
begin
  CallRpc(BuildQueueUnbind(AUnbind)); // Unbind-Ok (no args, discarded)
  RemoveBindRecovery(AUnbind.QueueName, AUnbind.ExchangeName, AUnbind.RoutingKey);
end;

procedure TAMQPChannel.RemoveQueueDeclareRecovery(const AQueue: string);
var
  I: Integer;
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LMatch: Boolean;
begin
  FRecoveryLock.Enter;
  try
    for I := FRecovery.Count - 1 downto 0 do
    begin
      LMatch := False;
      LReader := TAMQPReader.Create(FRecovery[I].Payload);
      try
        LId := ReadMethodHeader(LReader);
        if LId.Matches(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE) then
        begin
          LReader.ReadShortUInt; // reserved-1 (ticket)
          LMatch := LReader.ReadShortStr = AQueue;
        end;
      finally
        LReader.Free;
      end;
      if LMatch then
        FRecovery.Delete(I);
    end;
  finally
    FRecoveryLock.Leave;
  end;
end;

function TAMQPChannel.DeleteQueue(const ADelete: TAMQPQueueDelete): Cardinal;
var
  LPayload: TBytes;
  LReader: TAMQPReader;
begin
  LPayload := BuildQueueDelete(ADelete);
  if ADelete.NoWait then
  begin
    FConnection.SendMethod(FChannelId, LPayload);
    Result := 0;
  end
  else
  begin
    LReader := TAMQPReader.Create(CallRpc(LPayload));
    try
      ReadMethodHeader(LReader);
      Result := DecodeQueueDeleteOk(LReader);
    finally
      LReader.Free;
    end;
  end;
  RemoveQueueDeclareRecovery(ADelete.QueueName);
end;

procedure TAMQPChannel.BindExchange(const ABind: TAMQPExchangeBinding);
var
  LPayload: TBytes;
begin
  LPayload := BuildExchangeBind(ABind);
  if ABind.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Bind-Ok (no args, discarded)
  AddRecovery(LPayload, not ABind.NoWait);
end;

procedure TAMQPChannel.UnbindExchange(const AUnbind: TAMQPExchangeBinding);
var
  LPayload: TBytes;
begin
  LPayload := BuildExchangeUnbind(AUnbind);
  if AUnbind.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Unbind-Ok (no args, discarded)
  RemoveExchangeBindRecovery(AUnbind.Destination, AUnbind.Source, AUnbind.RoutingKey);
end;

function TAMQPChannel.DoSendPublish(const AMethod, AHeader, ABody: TBytes): UInt64;
var
  LMaxBody, LOffset, LLen: Integer;
  LRaw: TAMQPRawPublish;
begin
  if FConnection.FNegotiated.FrameMax = 0 then
    LMaxBody := 131072 - 8
  else
    LMaxBody := Integer(FConnection.FNegotiated.FrameMax) - 8;
  if LMaxBody < 1 then
    LMaxBody := 1;

  Result := 0;
  // A message's frames (method + header + body) must go out together, with no
  // frames from another thread interleaving — hence a single lock.
  FConnection.FWriteLock.Enter;
  try
    if FConfirmMode then
    begin
      // The seq-no follows the send order on the wire: assigned under FWriteLock,
      // which serializes the publishes. We register the pending entry BEFORE sending
      // (via FConfirmMon, the innermost lock) so as never to miss an ack that
      // arrives too fast — the same pattern as registering the consumer before Consume.
      FConfirmMon.Enter;
      try
        Inc(FPublishSeqNo);
        Result := FPublishSeqNo;
        FUnconfirmed.AddOrSetValue(Result, True);
        if FRepublish then
        begin
          // Keeps the content for resending if the connection drops before the ack. Copy
          // of the body: the caller may reuse/free the array after returning.
          LRaw.Method := AMethod;
          LRaw.Header := AHeader;
          LRaw.Body := Copy(ABody, 0, Length(ABody));
          FResendBuffer.AddOrSetValue(Result, LRaw);
        end;
      finally
        FConfirmMon.Leave;
      end;
    end;

    FConnection.SendFrameNoLock(AMQP_FRAME_METHOD, FChannelId, AMethod);
    FConnection.SendFrameNoLock(AMQP_FRAME_HEADER, FChannelId, AHeader);

    LOffset := 0;
    while LOffset < Length(ABody) do
    begin
      if (Length(ABody) - LOffset) < LMaxBody then
        LLen := Length(ABody) - LOffset
      else
        LLen := LMaxBody;
      FConnection.SendFrameNoLock(AMQP_FRAME_BODY, FChannelId,
        Copy(ABody, LOffset, LLen));
      Inc(LOffset, LLen);
    end;
  finally
    FConnection.FWriteLock.Leave;
  end;
end;

function TAMQPChannel.Publish(const AExchange, ARoutingKey: string;
  const ABody: TBytes; const AProps: TAMQPBasicProperties; AMandatory: Boolean): UInt64;
var
  LSpan: IPcSpan;
  LHeader: TBytes;
begin
  // Producer span (nil with the tracer off); its traceparent goes in the
  // headers, only in the bytes. It ends here, on the thread that opened it: the confirm
  // arrives later, on another thread (see AMQP.Tracing).
  LSpan := AmqpStartPublishSpan(AExchange, ARoutingKey, Length(ABody), AProps,
    FConnection.FParams.Host, FConnection.FParams.Port);
  if LSpan = nil then
  begin
    Result := DoSendPublish(
      BuildBasicPublish(AExchange, ARoutingKey, AMandatory, False),
      BuildContentHeader(UInt64(Length(ABody)), AProps),
      ABody);
    Exit;
  end;
  try
    LHeader := BuildContentHeader(UInt64(Length(ABody)), AProps,
      LSpan.TraceParent, LSpan.TraceState);
    Result := DoSendPublish(
      BuildBasicPublish(AExchange, ARoutingKey, AMandatory, False),
      LHeader, ABody);
    if Result > 0 then
      LSpan.SetIntAttribute('pascalamqp.publish.sequence_number', Int64(Result));
  except
    on E: Exception do
    begin
      AmqpFinishSpan(LSpan, E);
      raise;
    end;
  end;
  AmqpFinishSpan(LSpan, nil);
end;

procedure TAMQPChannel.ResendUnconfirmed;

  // Local insertion sort: TArray.Sort<T> does not exist in FPC's
  // Generics.Collections and the resend buffer is small (publishes pending from one drop).
  procedure SortKeys(var AKeys: TArray<UInt64>);
  var
    I, J: Integer;
    LKey: UInt64;
  begin
    for I := 1 to High(AKeys) do
    begin
      LKey := AKeys[I];
      J := I - 1;
      while (J >= 0) and (AKeys[J] > LKey) do
      begin
        AKeys[J + 1] := AKeys[J];
        Dec(J);
      end;
      AKeys[J + 1] := LKey;
    end;
  end;

var
  LKeys: TArray<UInt64>;
  LSnapshot: TArray<TAMQPRawPublish>;
  I: Integer;
begin
  if not (FConfirmMode and FRepublish) then
    Exit;
  // Takes an ordered snapshot (original publish order) and clears the buffer
  // under the lock; resends OUTSIDE the lock (DoSendPublish re-acquires FWriteLock/FConfirmMon
  // and re-buffers under the new seq-nos).
  FConfirmMon.Enter;
  try
    LKeys := FResendBuffer.Keys.ToArray;
    if Length(LKeys) = 0 then
      Exit;
    SortKeys(LKeys);
    SetLength(LSnapshot, Length(LKeys));
    for I := 0 to High(LKeys) do
      LSnapshot[I] := FResendBuffer[LKeys[I]];
    FResendBuffer.Clear;
  finally
    FConfirmMon.Leave;
  end;

  for I := 0 to High(LSnapshot) do
    DoSendPublish(LSnapshot[I].Method, LSnapshot[I].Header, LSnapshot[I].Body);
end;

function TAMQPChannel.PublishText(const AExchange, ARoutingKey, AText: string;
  APersistent: Boolean): UInt64;
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  if APersistent then
    LProps.SetPersistent;
  Result := Publish(AExchange, ARoutingKey, AmqpUtf8Encode(AText), LProps);
end;

function TAMQPChannel.BasicGet(const AQueue: string; ANoAck: Boolean): TAMQPGetResult;
var
  LSpan: IPcSpan;
begin
  LSpan := AmqpStartReceiveSpan(AQueue, FConnection.FParams.Host,
    FConnection.FParams.Port);
  if LSpan = nil then
  begin
    Result := DoBasicGet(AQueue, ANoAck);
    Exit;
  end;
  try
    Result := DoBasicGet(AQueue, ANoAck);
    AmqpSetReceiveResult(LSpan, Result.Found, Result.Properties, Result.RoutingKey,
      Length(Result.Body), Result.DeliveryTag, Result.Redelivered);
  except
    on E: Exception do
    begin
      AmqpFinishSpan(LSpan, E);
      raise;
    end;
  end;
  AmqpFinishSpan(LSpan, nil);
end;

function TAMQPChannel.DoBasicGet(const AQueue: string; ANoAck: Boolean): TAMQPGetResult;
begin
  FRpcLock.Enter;
  try
    if FClosed then
      raise EAMQPChannel.Create('channel is closed');
    FRpcKind := rkNone;
    FRpcError := '';
    FRpcEvent.ResetEvent;
    FConnection.SendMethod(FChannelId, BuildBasicGet(AQueue, ANoAck));
    if FRpcEvent.WaitFor(AMQP_RPC_TIMEOUT_MS) <> wrSignaled then
      raise EAMQPChannel.Create('timeout waiting for server response');
    case FRpcKind of
      rkMessage:
        Result := FRpcMessage;      // Get-Ok (Found=True came from the assembly)
      rkMethod:
        begin                       // Get-Empty
          Result := Default(TAMQPGetResult);
          Result.Found := False;
        end;
      rkError:
        raise EAMQPChannel.Create(FRpcError);
    else
      raise EAMQPChannel.Create('unexpected RPC response');
    end;
  finally
    FRpcLock.Leave;
  end;
end;

procedure TAMQPChannel.Ack(ADeliveryTag: UInt64; AMultiple: Boolean);
begin
  // Channel closing or closed: nothing goes on the wire (see FClosing). The broker
  // already returned to the queue whatever was left unacked when the channel closed.
  FAckLock.Enter;
  try
    SettleDeliveries(ADeliveryTag, AMultiple);
    if not FClosing then
      FConnection.SendMethod(FChannelId, BuildBasicAck(ADeliveryTag, AMultiple));
  finally
    FAckLock.Leave;
  end;
end;

procedure TAMQPChannel.Nack(ADeliveryTag: UInt64; ARequeue, AMultiple: Boolean);
begin
  FAckLock.Enter;
  try
    SettleDeliveries(ADeliveryTag, AMultiple);
    if not FClosing then
      FConnection.SendMethod(FChannelId,
        BuildBasicNack(ADeliveryTag, AMultiple, ARequeue));
  finally
    FAckLock.Leave;
  end;
end;

procedure TAMQPChannel.MarkClosing;
begin
  FAckLock.Enter;
  try
    FClosing := True;
  finally
    FAckLock.Leave;
  end;
end;

procedure TAMQPChannel.SettleDeliveries(ADeliveryTag: UInt64; AMultiple: Boolean);
var
  LTags: TArray<UInt64>;
  LTag: UInt64;
begin
  FSettleLock.Enter;
  try
    if not AMultiple then
      FUnsettled.Remove(ADeliveryTag)
    else
    begin
      // The set only has the in-flight callbacks (and the queued ones): it is small.
      LTags := FUnsettled.Keys.ToArray;
      for LTag in LTags do
        if LTag <= ADeliveryTag then
          FUnsettled.Remove(LTag);
    end;
  finally
    FSettleLock.Leave;
  end;
end;

function TAMQPChannel.TakeUnsettled(ADeliveryTag: UInt64): Boolean;
begin
  FSettleLock.Enter;
  try
    // Remove is a procedure in Delphi (and in FPC): check and remove separately,
    // still under the same lock.
    Result := FUnsettled.ContainsKey(ADeliveryTag);
    if Result then
      FUnsettled.Remove(ADeliveryTag);
  finally
    FSettleLock.Leave;
  end;
end;

function TAMQPChannel.IsUnsettled(ADeliveryTag: UInt64): Boolean;
begin
  FSettleLock.Enter;
  try
    Result := FUnsettled.ContainsKey(ADeliveryTag);
  finally
    FSettleLock.Leave;
  end;
end;

procedure TAMQPChannel.HandleConsumerError(const ADelivery: TAMQPDelivery;
  AError: Exception);
var
  LAction: TAMQPConsumerErrorAction;
  LHandler: TAMQPConsumerErrorEvent;
begin
  // Runs on the pool thread, inside the callback's except (AError is still alive).
  // A no-ack delivery never enters the set: it falls into ceaNone here.
  if IsUnsettled(ADelivery.DeliveryTag) then
    LAction := ceaReject
  else
    LAction := ceaNone;

  LHandler := FOnConsumerError;
  if Assigned(LHandler) then
    try
      LHandler(Self, ADelivery, AError, LAction);
    except
      // An error handler that raises must not leave the delivery hanging,
      // which is the defect it exists to fix: the action still stands.
    end;

  // Checks AGAIN, removing: the handler may have done Ack/Nack itself.
  // Acknowledging the same tag twice is 406 PRECONDITION_FAILED and brings down the channel.
  if (LAction = ceaNone) or (not TakeUnsettled(ADelivery.DeliveryTag)) then
    Exit;
  try
    FConnection.SendMethod(FChannelId, BuildBasicNack(ADelivery.DeliveryTag,
      False, LAction = ceaRequeue));
  except
    // Connection or channel dropped: the broker returns the delivery to the queue by itself.
  end;
end;

procedure TAMQPChannel.ConfirmSelect;
begin
  if FConfirmMode then
    Exit;
  CallRpc(BuildConfirmSelect(False)); // Confirm.Select-Ok (no args)
  FConfirmMon.Enter;
  try
    FUnconfirmed.Clear;
    FNacked.Clear;
    FResendBuffer.Clear;
    FPublishSeqNo := 0; // the first publish from here on gets seq-no 1
    FConfirmBase := 0;
  finally
    FConfirmMon.Leave;
  end;
  FConfirmMode := True; // last: from here on Publish starts numbering
  AddRecovery(BuildConfirmSelect(False), True); // re-arms confirm mode on reconnection
end;

function TAMQPChannel.WaitForConfirm(ASeqNo: UInt64; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
  LRemaining: Int64;
begin
  if not FConfirmMode then
    raise EAMQPChannel.Create('channel is not in confirm mode (call ConfirmSelect)');
  if ASeqNo = 0 then
    raise EAMQPChannel.Create('invalid seq-no (0) in WaitForConfirm');
  LDeadline := PcTickMs + ATimeoutMs;
  FConfirmMon.Enter;
  try
    while True do
    begin
      if FNacked.ContainsKey(ASeqNo) then
      begin
        FNacked.Remove(ASeqNo);
        Exit(False); // nack'ed (or lost in a connection drop)
      end;
      if not FUnconfirmed.ContainsKey(ASeqNo) then
        // neither pending nor nack'ed: ack'ed (if already published) or a
        // seq-no that never existed (> last assigned => False, no pointless wait).
        Exit(ASeqNo <= FPublishSeqNo);
      LNow := PcTickMs;
      if LNow >= LDeadline then
        Exit(False);
      LRemaining := Int64(LDeadline) - Int64(LNow);
      FConfirmMon.Wait(Cardinal(LRemaining));
    end;
  finally
    FConfirmMon.Leave;
  end;
end;

function TAMQPChannel.WaitForConfirms(ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
  LRemaining: Int64;
begin
  if not FConfirmMode then
    raise EAMQPChannel.Create('channel is not in confirm mode (call ConfirmSelect)');
  LDeadline := PcTickMs + ATimeoutMs;
  FConfirmMon.Enter;
  try
    while FUnconfirmed.Count > 0 do
    begin
      LNow := PcTickMs;
      if LNow >= LDeadline then
        Exit(False);
      LRemaining := Int64(LDeadline) - Int64(LNow);
      FConfirmMon.Wait(Cardinal(LRemaining));
    end;
    // All pending ones resolved: success iff NONE was nack'ed. FNacked
    // keeps the nacks not yet consumed — including those that arrived BEFORE
    // this call. By clearing it, we consume this batch's result.
    Result := FNacked.Count = 0;
    FNacked.Clear;
  finally
    FConfirmMon.Leave;
  end;
end;

procedure TAMQPChannel.Qos(APrefetchCount: Word; AGlobal: Boolean);
var
  LPayload: TBytes;
begin
  LPayload := BuildBasicQos(APrefetchCount, AGlobal);
  CallRpc(LPayload); // Qos-Ok (no args)
  AddRecovery(LPayload, True);
end;

function TAMQPChannel.Consume(const AQueue: string;
  const ACallback: TAMQPConsumerCallback; ANoAck, AExclusive: Boolean): string;
var
  LTag: string;
  LConsume: TAMQPBasicConsume;
  LPayload: TBytes;
  LInfo: TAMQPConsumerInfo;
begin
  // We generate the consumer-tag on the client and register the callback BEFORE sending,
  // so as not to lose deliveries that arrive between Consume-Ok and the registration.
  LTag := Format('ctag-%d-%d', [FChannelId, PcAtomicInc(FConsumerCounter)]);
  LInfo.Queue := AQueue;
  LInfo.NoAck := ANoAck;
  FConsumersLock.Enter;
  try
    FConsumers.AddOrSetValue(LTag, ACallback);
    FConsumerInfo.AddOrSetValue(LTag, LInfo);
  finally
    FConsumersLock.Leave;
  end;

  LConsume := TAMQPBasicConsume.Create(AQueue, LTag, ANoAck);
  LConsume.Exclusive := AExclusive;
  LPayload := BuildBasicConsume(LConsume);
  try
    CallRpc(LPayload); // Consume-Ok (returns the same tag)
  except
    FConsumersLock.Enter;
    try
      FConsumers.Remove(LTag);
      FConsumerInfo.Remove(LTag);
    finally
      FConsumersLock.Leave;
    end;
    raise;
  end;
  // Records for replay after reconnection (same tag and callback).
  AddRecovery(LPayload, True, True, LTag, ACallback);
  Result := LTag;
end;

procedure TAMQPChannel.Cancel(const AConsumerTag: string);
begin
  // The local cleanup runs even if CallRpc fails (channel dropped / timeout): otherwise the
  // consumer would stay in FRecovery and be resurrected on an eventual reconnection.
  try
    CallRpc(BuildBasicCancel(AConsumerTag, False)); // Cancel-Ok
  finally
    FConsumersLock.Enter;
    try
      FConsumers.Remove(AConsumerTag);
      FConsumerInfo.Remove(AConsumerTag);
    finally
      FConsumersLock.Leave;
    end;
    RemoveConsumerRecovery(AConsumerTag);
  end;
end;

procedure TAMQPChannel.DispatchToPool(AItem: TPcWorkItem);
begin
  if Assigned(FDispatchPool) then
    FDispatchPool.Queue(AItem)
  else
    PcPool.Queue(AItem);
end;

procedure TAMQPChannel.DispatchDelivery(const ADeliver: TAMQPBasicDeliver;
  const AProps: TAMQPBasicProperties; const ABody: TBytes);
var
  LCallback: TAMQPConsumerCallback;
  LDelivery: TAMQPDelivery;
  LInfo: TAMQPConsumerInfo;
begin
  FConsumersLock.Enter;
  try
    if not FConsumers.TryGetValue(ADeliver.ConsumerTag, LCallback) then
      LCallback := nil;
    if not FConsumerInfo.TryGetValue(ADeliver.ConsumerTag, LInfo) then
      LInfo := Default(TAMQPConsumerInfo);
  finally
    FConsumersLock.Leave;
  end;

  LDelivery := Default(TAMQPDelivery);
  LDelivery.ConsumerTag := ADeliver.ConsumerTag;
  LDelivery.DeliveryTag := ADeliver.DeliveryTag;
  LDelivery.Redelivered := ADeliver.Redelivered;
  LDelivery.Exchange := ADeliver.Exchange;
  LDelivery.RoutingKey := ADeliver.RoutingKey;
  LDelivery.Properties := AProps;
  LDelivery.Body := ABody;

  if not Assigned(LCallback) then
  begin
    // no consumer (cancelled): frees any Headers and discards
    if LDelivery.Properties.Has(bpHeaders) and Assigned(LDelivery.Properties.Headers) then
      LDelivery.Properties.Headers.Free;
    Exit;
  end;

  // Registers the delivery as pending BEFORE queueing: an Ack from the callback
  // (or an Ack with multiple from another callback) never arrives before the registration.
  if not LInfo.NoAck then
  begin
    FSettleLock.Enter;
    try
      FUnsettled.AddOrSetValue(LDelivery.DeliveryTag, True);
    finally
      FSettleLock.Leave;
    end;
  end;

  // Dispatches to the pool; the read thread does NOT run the callback.
  PcAtomicInc(FInFlight);
  DispatchToPool(TAMQPDeliveryWork.Create(Self, LCallback, LDelivery, LInfo));
end;

procedure TAMQPChannel.DispatchReturn(const AReturn: TAMQPBasicReturn;
  const AProps: TAMQPBasicProperties; const ABody: TBytes);
var
  LCallback: TAMQPBasicReturnCallback;
  LReturned: TAMQPReturnedMessage;
begin
  LCallback := FOnBasicReturn;

  LReturned := Default(TAMQPReturnedMessage);
  LReturned.ReplyCode := AReturn.ReplyCode;
  LReturned.ReplyText := AReturn.ReplyText;
  LReturned.Exchange := AReturn.Exchange;
  LReturned.RoutingKey := AReturn.RoutingKey;
  LReturned.Properties := AProps;
  LReturned.Body := ABody;

  if not Assigned(LCallback) then
  begin
    // no handler: frees any Headers and discards (same behavior
    // as before Basic.Return was handled).
    if LReturned.Properties.Has(bpHeaders) and Assigned(LReturned.Properties.Headers) then
      LReturned.Properties.Headers.Free;
    Exit;
  end;

  // Dispatches to the pool; the read thread does NOT run the callback.
  PcAtomicInc(FInFlight);
  DispatchToPool(TAMQPReturnWork.Create(Self, LCallback, LReturned));
end;

procedure TAMQPChannel.DispatchConfirm(ASeqNo: UInt64; AAck: Boolean);
var
  LCallback: TAMQPConfirmCallback;
begin
  LCallback := FOnConfirm;
  if not Assigned(LCallback) then
    Exit;
  // Dispatches to the pool; the read thread does NOT run the callback.
  PcAtomicInc(FInFlight);
  DispatchToPool(TAMQPConfirmWork.Create(Self, LCallback, ASeqNo, AAck));
end;

function TAMQPChannel.ResolveConfirms(ATag: UInt64;
  AMultiple, AAck: Boolean): TArray<UInt64>;
var
  LList: TList<UInt64>;
  LKey, LUserTag: UInt64;
begin
  // Runs on the read thread. ATag is the broker's delivery-tag (WIRE
  // numbering, restarts per session); we add the session offset to get the user's
  // seq-no. `multiple` covers "this one and all previous ones still pending",
  // so we resolve every seq-no <= tag.
  LList := TList<UInt64>.Create;
  try
    FConfirmMon.Enter;
    try
      LUserTag := FConfirmBase + ATag;
      for LKey in FUnconfirmed.Keys do
        if (AMultiple and (LKey <= LUserTag)) or (LKey = LUserTag) then
          LList.Add(LKey);
      for LKey in LList do
      begin
        FUnconfirmed.Remove(LKey);
        FResendBuffer.Remove(LKey); // confirmed (broker ack or nack): no resend
        if not AAck then
          FNacked.AddOrSetValue(LKey, True);
      end;
      FConfirmMon.PulseAll; // wakes WaitForConfirm(s)
    finally
      FConfirmMon.Leave;
    end;
    Result := LList.ToArray;
  finally
    LList.Free;
  end;
end;

procedure TAMQPChannel.FailAllUnconfirmed;
var
  LKey: UInt64;
begin
  // Called when the connection drops (SignalError): pending publishes are left in an
  // ambiguous state — we report them as unconfirmed (WaitForConfirm => False).
  // Does NOT fire OnConfirm (which is only for real confirms from the broker).
  if not FConfirmMode then
    Exit;
  FConfirmMon.Enter;
  try
    for LKey in FUnconfirmed.Keys do
      FNacked.AddOrSetValue(LKey, True);
    FUnconfirmed.Clear;
    FConfirmMon.PulseAll;
  finally
    FConfirmMon.Leave;
  end;
end;

procedure TAMQPChannel.DrainInFlight;
begin
  // Waits WITHOUT a timeout: freeing the channel with an item still running or still
  // in the pool's QUEUE would be a use-after-free (the item holds Self and decrements
  // FInFlight at the end). A well-behaved callback always finishes — even if
  // it does 5s+ of IO (the target use case). Do not call Close from inside the
  // callback itself (self-wait).
  //
  // The counter goes up at queueing time, so on a regular channel this wait
  // depends on PcPool moving — and PcPool belongs to the whole process. Measured
  // (TPoolIsolationTests): PcPool saturated for 1.5 s, Free of the channel with one
  // delivery in the queue = 1.5 s; with CreateChannel(True), 29 ms. A deadline here would
  // have no safe way out (the queued item still points to the channel); the way out is the
  // channel's own pool.
  while PcAtomicGet(FInFlight) > 0 do
    Sleep(10);
end;

procedure TAMQPChannel.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPCloseInfo;
  LTags: TArray<string>;
  LTag: string;
begin
  if FClosed or (not FIsOpen) then
    Exit;

  // (1) Cancels the consumers: after Cancel-Ok the broker delivers nothing more
  // for these tags. If one failed (connection down, timeout), do not insist:
  // the others would give the same error, each with its own timeout.
  FConsumersLock.Enter;
  try
    LTags := FConsumers.Keys.ToArray;
  finally
    FConsumersLock.Leave;
  end;
  for LTag in LTags do
    try
      Cancel(LTag);
    except
      Break; // keeps closing; the Channel.Close below meets the same fate
    end;
  // (2) With the channel still open, the callbacks already dispatched finish and
  // their Ack counts: closing does not redeliver what was already processed.
  DrainInFlight;
  // (3) From here on Ack/Nack do not go out (see FClosing).
  MarkClosing;

  LClose.ReplyCode := AReplyCode;
  LClose.ReplyText := AReplyText;
  LClose.ClassId := 0;
  LClose.MethodId := 0;
  try
    CallRpc(BuildChannelClose(LClose)); // waits for Channel.Close-Ok
  except
    // if it fails (connection dropped etc.), keeps closing locally
  end;
  FClosed := True;
  FIsOpen := False;
  // Whatever arrived between step (2) and the Close-Ok (a Return, a confirm):
  // waits for it to finish before the object can be freed.
  DrainInFlight;
  FConnection.UnregisterChannel(FChannelId);
end;

{ TAMQPChannel — reception (runs on the read thread) }

procedure TAMQPChannel.CompleteContent;
var
  LMsg: TAMQPGetResult;
begin
  FAsmState := asIdle;
  if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK) then
  begin
    LMsg := Default(TAMQPGetResult);
    LMsg.Found := True;
    LMsg.DeliveryTag := FAsmGetOk.DeliveryTag;
    LMsg.Redelivered := FAsmGetOk.Redelivered;
    LMsg.Exchange := FAsmGetOk.Exchange;
    LMsg.RoutingKey := FAsmGetOk.RoutingKey;
    LMsg.MessageCount := FAsmGetOk.MessageCount;
    LMsg.Properties := FAsmProps;
    LMsg.Body := FAsmBody;
    SignalMessage(LMsg);
  end
  else if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER) then
    DispatchDelivery(FAsmDeliver, FAsmProps, FAsmBody)
  else if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN) then
    DispatchReturn(FAsmReturn, FAsmProps, FAsmBody)
  else
  begin
    // Unknown method arriving through this path: we discard the content.
    // Since nobody takes ownership of the Properties here, we free the
    // Headers table if it came (otherwise it would leak).
    if FAsmProps.Has(bpHeaders) and Assigned(FAsmProps.Headers) then
    begin
      FAsmProps.Headers.Free;
      FAsmProps.Headers := nil;
    end;
  end;
  FAsmBody := nil;
end;

procedure TAMQPChannel.HandleFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LClose: TAMQPCloseInfo;
  LHeader: TAMQPContentHeader;
  LTag: string;
  LAck: TAMQPBasicAck;
  LNack: TAMQPBasicNack;
  LResolved: TArray<UInt64>;
  LSeq: UInt64;
begin
  case FAsmState of
    asHeader:
      begin
        if AFrame.FrameType <> AMQP_FRAME_HEADER then
          Exit; // out-of-order frame: ignore (protocol)
        LReader := TAMQPReader.Create(AFrame.Payload);
        try
          LHeader := DecodeContentHeader(LReader);
        finally
          LReader.Free;
        end;
        FAsmProps := LHeader.Properties;
        FAsmBody := nil;
        FAsmRemaining := LHeader.BodySize;
        if FAsmRemaining = 0 then
          CompleteContent
        else
          FAsmState := asBody;
        Exit;
      end;

    asBody:
      begin
        if AFrame.FrameType <> AMQP_FRAME_BODY then
          Exit;
        FAsmBody := FAsmBody + AFrame.Payload;
        if UInt64(Length(AFrame.Payload)) >= FAsmRemaining then
          FAsmRemaining := 0
        else
          Dec(FAsmRemaining, Length(AFrame.Payload));
        if FAsmRemaining = 0 then
          CompleteContent;
        Exit;
      end;
  end;

  // asIdle: expects a method frame.
  if not AFrame.IsMethod then
    Exit;

  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);

    if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK) then
    begin
      FAsmMethodId := LId;
      FAsmGetOk := DecodeBasicGetOk(LReader);
      FAsmState := asHeader;
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER) then
    begin
      FAsmMethodId := LId;
      FAsmDeliver := DecodeBasicDeliver(LReader);
      FAsmState := asHeader; // content comes in the next frames
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN) then
    begin
      // unrouted mandatory publish: content comes in the next frames.
      FAsmMethodId := LId;
      FAsmReturn := DecodeBasicReturn(LReader);
      FAsmState := asHeader;
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL) then
    begin
      // server cancelled the consumer (e.g. queue deleted).
      LTag := DecodeBasicCancel(LReader);
      FConsumersLock.Enter;
      try
        FConsumers.Remove(LTag);
        FConsumerInfo.Remove(LTag);
      finally
        FConsumersLock.Leave;
      end;
      RemoveConsumerRecovery(LTag);
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK) then
    begin
      // publisher confirm: ack of one or more publishes (pure method frame,
      // no content). Resolves the pending ones and dispatches OnConfirm outside the lock.
      LAck := DecodeBasicAck(LReader);
      LResolved := ResolveConfirms(LAck.DeliveryTag, LAck.Multiple, True);
      for LSeq in LResolved do
        DispatchConfirm(LSeq, True);
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK) then
    begin
      // publisher confirm: nack (the broker could not handle the publish).
      LNack := DecodeBasicNack(LReader);
      LResolved := ResolveConfirms(LNack.DeliveryTag, LNack.Multiple, False);
      for LSeq in LResolved do
        DispatchConfirm(LSeq, False);
    end
    else if LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE) then
    begin
      LClose := DecodeChannelClose(LReader);
      // Sets FClosing BEFORE the Close-Ok, under the same lock as Ack: an Ack from an
      // in-flight callback goes out before the Close-Ok (the broker discards it, as the
      // spec says) or does not go out — never after, which would be a connection 504.
      FAckLock.Enter;
      try
        FClosing := True;
        FConnection.SendMethod(FChannelId, BuildChannelCloseOk);
      finally
        FAckLock.Leave;
      end;
      SignalError(Format('channel %d closed by server: %d %s',
        [FChannelId, LClose.ReplyCode, LClose.ReplyText]));
    end
    else
    begin
      // RPC reply (Open-Ok, Declare-Ok, Bind-Ok, Get-Empty, Close-Ok, ...)
      SignalMethod(AFrame.Payload);
    end;
  finally
    LReader.Free;
  end;
end;

end.
