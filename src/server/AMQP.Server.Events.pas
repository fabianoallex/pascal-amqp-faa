unit AMQP.Server.Events;

{$I amqp.inc}

{ The broker's observability vocabulary (Phase 4.1, D29-D36).

  This unit holds ONLY types: what an event is, which types exist and how
  one is built. TAMQPEventBus (AMQP.Server.EventBus) transports them;
  TAMQPServer exposes them. It uses nothing from the sub-module on purpose
  -- it is the bottom of the stack, and AMQP.Server.Types depends on it to
  declare IAMQPEventSink.

  WHAT THIS MECHANISM IS (D29): a READ-ONLY observer. No handler influences
  routing, authentication, ack or drop -- whoever needs to DECIDE uses
  IAMQPAuthorizer. With no subscriber at all the hot path pays one mask read
  and does not even build the record.

  WHAT IT IS NOT (D36): an audit log. Delivery is best-effort and D31 admits
  loss by design, counted in DroppedCount. Whoever needs everything keeps
  everything inside the handler -- and even then loses what is dropped under
  pressure.

  WHY THE RECORD IS FLAT (D34): because EVERY event is delivered by another
  thread (D30). An event is a copy BY VALUE, without a single pointer to a
  connection, channel or message -- any of them may die between emission and
  delivery, and with a flat record that is nobody's problem. It is the same
  reason as D1 (no TValue, whose pain in FPC is already catalogued in
  CLAUDE.md).

  Three constraints of D34, each with a reason:

  - The MESSAGE BODY NEVER GOES INTO the event, only MessageSize. Carrying it
    would break D1's COW and put a buffer copy on the hottest path there is.
  - The wall clock is WallMs (Int64, epoch ms UTC) via AmqpWallMs, and NOT
    TDateTime: D21's DateTimeToUnix(LocalTimeToUniversal(...)) conversion
    converts twice and the error is invisible on a UTC machine -- that is,
    invisible in every container where the suite runs. TickMs (monotonic) is
    only for measuring latency, and does not survive a restart.
  - Each type populates a FIXED set of fields; the rest comes out zeroed,
    because AmqpNewEvent starts from a zeroed record. The field-by-type table
    is in docs/observability.md and the tests assert it.

  Adding a field to the record is the compatible direction (existing handlers
  keep compiling); removing a field is not. }

interface

uses
  SysUtils,
  PascalCommon.Threading,
  AMQP.Threading; // AmqpWallMs

type
  { What happened. THE ORDER OF THIS ENUM IS API: the ordinal value becomes a
    bit in the subscription mask (D32), so a new type always goes AT THE END.

    CAP OF 32 TYPES (D32): the mask is a Cardinal, read lock-free on the hot
    path. Going past 32 means changing the mask's type -- a conscious
    decision, not an enum value added absent-mindedly. 18 used, 14 spare.

    The [thread] column says where the event is BORN, which is what D30
    exists to make irrelevant: all of them are delivered by the notifier. }
  TAMQPServerEventType = (
    { --- connection lifecycle [read thread] --- }
    seConnectionEstablished,   // socket accepted, before any AMQP byte
    seConnectionAuthenticated, // SASL PLAIN OK and vhost opened (Open-Ok)
    seConnectionClosed,        // read thread exited (Code = reply-code)

    { --- channel lifecycle [read thread] --- }
    seChannelOpened,
    seChannelClosed,           // Code = reply-code (0 = clean close)

    { --- publishing [read thread] --- }
    seMessagePublished,        // content reassembled and routed
    seMessagePublishRejected,  // no route (mandatory) or reject-publish

    { --- consumers [read thread] --- }
    seConsumerRegistered,      // Basic.Consume accepted
    seConsumerCancelled,       // Basic.Cancel, or cancellation by the server

    { --- acknowledgement [read thread] --- }
    seMessageAcked,
    seMessageNacked,
    seMessageRejected,

    { --- message lifecycle [queue actor -- Inc. 2] --- }
    seMessageEnqueued,
    seMessageDelivered,
    seMessageExpired,          // TTL ran out (Reason = 'expired')
    seMessageDeadLettered,     // went to the DLX (Reason = x-death reason)
    seMessageDropped,          // dropped by a cap (Reason = 'maxlen'...)

    { --- durability [journal thread -- Inc. 2] --- }
    seJournalFlushed           // a BATCH became durable (D25: never per record)
  );

  { Set of types a subscriber wants. Becomes a Cardinal mask inside the
    bus -- see AmqpEventMask. }
  TAMQPServerEventTypes = set of TAMQPServerEventType;

  { A broker fact, flattened. See the unit header for the reason behind each
    constraint; docs/observability.md has the table of who populates what.

    Every field the event type does not populate comes out ZEROED (empty
    string, numeric 0, Boolean False). }
  TAMQPServerEvent = record
    EventType: TAMQPServerEventType;

    { Clocks. WallMs correlates with external logs; TickMs measures latency
      (monotonic, does not survive a restart). }
    WallMs: Int64;
    TickMs: Int64;

    { Connection context. ConnectionId is the same identifier an exclusive
      queue uses to know whose it is. }
    ConnectionId: UInt64;
    RemoteAddr: string;
    Username: string;
    VHost: string;

    { Channel context. 0 = connection event. }
    ChannelNumber: Word;
    { AMQP reply-code when the event is a close (0 = clean). }
    Code: Word;

    { Queue / routing context. }
    QueueName: string;
    ConsumerTag: string;
    ExchangeName: string;
    RoutingKey: string;

    { Message. NEVER the body (D34) -- only its size. }
    DeliveryTag: UInt64;
    MessageSize: UInt64;
    Priority: Byte;
    Redelivered: Boolean;
    Mandatory: Boolean;  // publish asked for Basic.Return if unroutable
    Multiple: Boolean;   // ack/nack with multiple=true
    Requeue: Boolean;    // nack/reject with requeue=true

    { Journal LSN (seJournalFlushed) and a generic count -- how many records
      the batch carried, how many messages the drop ate. }
    Lsn: UInt64;
    Count: Int64;

    { Why, when the type alone does not say. }
    Reason: string;
  end;

  { Where the observability events go (Phase 4.1, D32).

    Lives HERE, and not in AMQP.Server.Types as D32 first said, because the
    queue actor and the journal thread also emit (Inc. 2) and neither of
    those units uses Types -- declaring it there would force Queue and
    Journal to pull in Auth, FrameIO and Basic.Methods to reach an
    interface. The house rule points here anyway: just as IAMQPMessageSink
    lives next to TAMQPServerMessage, this one lives next to
    TAMQPServerEvent.

    It is the house shape -- an interface here, like IAMQPMessageSink and
    IAMQPConfirmRegistry --, and not the TObject with a cast that the WIP
    used to dodge a unit cycle. nil = observability off, exactly as
    Confirms = nil means "no durability".

    Implemented by TAMQPEventBus (AMQP.Server.EventBus), which is the one
    that knows D31's delivery contract. A CALLER only needs to know two
    things:

    - Wants is very cheap (atomic mask read) and is there to avoid building
      the record when nobody wants the type. Calling Emit without Wants is
      not an error, only waste;
    - Emit NEVER blocks, NEVER raises and MAY DROP. No caller needs to handle
      an error, and no caller can count on delivery. }
  IAMQPEventSink = interface
    ['{7B3C1D48-0A62-4F95-8E17-2C5D9B4A6E03}']
    /// True if some subscriber wants this event type.
    function Wants(AType: TAMQPServerEventType): Boolean;
    /// Enqueues the event for the notifier thread. See the contract above.
    procedure Emit(const AEvent: TAMQPServerEvent);
  end;

  { Subscriber. An object method (FPC 3.2 has no closures -- CLAUDE.md).

    HANDLER CONTRACT, in three lines:
    - runs on the notifier thread, NEVER on the read thread nor on a pool
      worker (D30). Do not assume which one it is;
    - may take its time: while it runs, the ring fills up and starts
      DROPPING the newest (D31). A slow handler loses events, it never holds
      up the broker;
    - if it raises, the exception is counted and the loop goes on (D33). No
      taking the notifier down, no silent swallowing. }
  TAMQPServerEventHandler = procedure(const AEvent: TAMQPServerEvent) of object;

const
  /// All types -- the default for whoever subscribes without filtering.
  AMQP_EVENT_ALL_TYPES = [Low(TAMQPServerEventType)..High(TAMQPServerEventType)];

  /// Size of the bus ring, in EVENTS (D31). Beyond this the emitter drops
  /// the newest and counts; it never blocks and never raises.
  ///
  /// 4096 is generous on purpose: the cost is an array of records, and the
  /// only thing the cap protects is the memory of a broker whose handler
  /// stopped draining. Whoever wants another value sets
  /// TAMQPServer.EventQueueCapacity BEFORE Start.
  AMQP_EVENT_QUEUE_CAPACITY = 4096;

/// Zeroed record, already carrying the type and both clocks. EVERY emitter
/// starts here -- it is what guarantees D34's promise that an unpopulated
/// field comes out zeroed, without relying on the emitter remembering to zero it.
function AmqpNewEvent(AType: TAMQPServerEventType): TAMQPServerEvent;

/// Set -> bit mask. The enum ordinal is the bit number, which is why the
/// enum order is API.
function AmqpEventMask(const ATypes: TAMQPServerEventTypes): Cardinal;

/// Short type name, for logs and test messages. Without the 'se' prefix --
/// 'ConnectionEstablished', not 'seConnectionEstablished'.
function AmqpEventTypeName(AType: TAMQPServerEventType): string;

implementation

const
  { Mirrors the enum. A new type with no entry here is caught by the test's
    AssertNames, which compares the count with Ord(High(...)) + 1. }
  EVENT_NAMES: array[TAMQPServerEventType] of string = (
    'ConnectionEstablished',
    'ConnectionAuthenticated',
    'ConnectionClosed',
    'ChannelOpened',
    'ChannelClosed',
    'MessagePublished',
    'MessagePublishRejected',
    'ConsumerRegistered',
    'ConsumerCancelled',
    'MessageAcked',
    'MessageNacked',
    'MessageRejected',
    'MessageEnqueued',
    'MessageDelivered',
    'MessageExpired',
    'MessageDeadLettered',
    'MessageDropped',
    'JournalFlushed'
  );

function AmqpNewEvent(AType: TAMQPServerEventType): TAMQPServerEvent;
begin
  // Finalize + FillChar would be faster, and it would be WRONG: Result may come
  // from a slot with live strings. A local record zeroed by Default/Init is the
  // portable way to release the old references on both compilers.
  Result := Default(TAMQPServerEvent);
  Result.EventType := AType;
  Result.WallMs := AmqpWallMs;
  Result.TickMs := Int64(PcTickMs);
end;

function AmqpEventMask(const ATypes: TAMQPServerEventTypes): Cardinal;
var
  LType: TAMQPServerEventType;
begin
  Result := 0;
  for LType := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
    if LType in ATypes then
      Result := Result or (Cardinal(1) shl Ord(LType));
end;

function AmqpEventTypeName(AType: TAMQPServerEventType): string;
begin
  Result := EVENT_NAMES[AType];
end;

end.
