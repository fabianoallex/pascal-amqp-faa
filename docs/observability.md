# Broker observability

An embedded broker is a black box inside your application: it accepts connections, routes, delivers and drops without any of that showing up in the app's log. Observability opens that box — `TAMQPServer` emits structured events, and you subscribe to what interests you.

The architecture decisions are locked in `CLAUDE.md` as **D29–D36**; this document is the user manual. If you only want the summary: **it is opt-in, it is read-only, it is best-effort, and the handler runs on a thread of its own.**

## Getting started

```pascal
uses AMQP.Server.Events, AMQP.Server.Broker;

type
  TMeuLog = class
  public
    procedure AoEvento(const AEvento: TAMQPServerEvent);
  end;

procedure TMeuLog.AoEvento(const AEvento: TAMQPServerEvent);
begin
  // Runs on the broker's notifier thread. Only one, always the same.
  Writeln(AmqpEventTypeName(AEvento.EventType), ' conn=', AEvento.ConnectionId);
end;

// ...
Broker := TAMQPServer.Create;
Log := TMeuLog.Create;
Broker.Subscribe(Log.AoEvento);   // before Start: you want the 1st event
Broker.Start;
// ...
Broker.Stop;
Broker.Unsubscribe(Log.AoEvento); // BEFORE freeing Log — see "Pitfalls"
```

Without any `Subscribe`, the broker is byte for byte the usual one: the hot path tests a mask and does not even build the event record. Nobody pays for not observing.

To filter by type — which avoids building records you are going to throw away:

```pascal
Broker.Subscribe(Log.AoEvento,
  [seConnectionAuthenticated, seMessagePublished, seMessageAcked]);
```

Subscribing the **same method** again **replaces** the previous mask; it does not duplicate delivery.

Full sample, with a drop report at the end: `samples/Server/AMQPServer.dpr` (`lazbuild samples\Server\AMQPServer.lpi`, or the `.dproj` in the Delphi IDE).

## The contract, in four lines

1. **Read-only.** No handler influences routing, authentication, ack or drop. Whoever needs to *decide* implements `IAMQPAuthorizer` — which exists exactly for that. Raising an exception inside the handler cancels nothing; it is only counted.
2. **Asynchronous, always.** *Every* event is copied into a bounded ring and delivered by a dedicated thread — including those born on the connection's read thread. Your handler never runs on the read thread nor on a pool worker.
3. **Best-effort: it can lose.** Ring full ⇒ the **newest** event is dropped and counted. The emitter never blocks and never raises.
4. **Order per source.** Events from the same connection (or the same queue) arrive in the order they happened. Across different connections, the relative order is not an API promise.

### Why the handler does not run inline

Because there is no safe half. The three alternatives to the ring were measured against the architecture and each one breaks something different:

| where to run the handler | what breaks |
|---|---|
| pool worker | the pool of the queue actors (the broker's own) — a slow handler starves the actor; `PcPool` belongs to the whole process, and the handler would compete for workers with other libs' callbacks |
| monitor thread | puts heartbeat, Close-Ok deadline, TTL sweep and reap behind the handler |
| inline on the read thread | stalls frame processing for that connection, **heartbeat included**, until the client drops it |

What changes between them is the blast radius, not whether there is damage. A dedicated notifier thread gives one contract, one order and one exception policy — and since the event record is a copy by value, asynchrony costs a copy, not a dangling-pointer risk.

### This is not an audit log

Loss under pressure is by design, not for lack of care. If your handler cannot keep up with the load, you lose events — and the only way to know is to look at the counter:

```pascal
if Broker.EventsDropped > 0 then
  ...; // the observer did not see everything
```

For real auditing, the handler has to be fast (queue into a buffer of your own and return) and you still need to treat `EventsDropped > 0` as a known gap. Growing the ring postpones the problem; it does not remove it:

```pascal
Broker.EventQueueCapacity := 65536; // only before Start
```

## The event types

| type | when |
|---|---|
| `seConnectionEstablished` | socket accepted, before any AMQP byte (and before the TLS handshake) |
| `seConnectionAuthenticated` | SASL PLAIN accepted **and** vhost opened (the `Open-Ok` went out) |
| `seConnectionClosed` | the read thread exited, after the whole teardown |
| `seChannelOpened` | `Channel.Open-Ok` sent |
| `seChannelClosed` | channel closed — by the client, by an error, or by the connection teardown |
| `seMessagePublished` | content reassembled and routed (including when no queue was found) |
| `seMessagePublishRejected` | rejected by a full queue with `x-overflow: reject-publish` |
| `seConsumerRegistered` | `Basic.Consume` accepted |
| `seConsumerCancelled` | `Basic.Cancel` of an existing consumer |
| `seMessageAcked` / `seMessageNacked` / `seMessageRejected` | `Basic.Ack` / `.Nack` / `.Reject` received |
| `seMessageEnqueued` | the message entered a queue |
| `seMessageDelivered` | it left the queue for a consumer (does not count `Basic.Get`) |
| `seMessageExpired` | expired by TTL (message or queue — the smaller wins) |
| `seMessageDeadLettered` | it was republished to a dead-letter exchange |
| `seMessageDropped` | dropped by a queue cap (`x-max-length`/`-bytes`) |
| `seJournalFlushed` | a WAL **batch** became durable (only with `DataDir`) |

### Where each event is born, and why that shows in the fields

The events come from three different places in the broker, and what each one **knows** is different:

| source | types | what it knows |
|---|---|---|
| connection read thread | connection, channel, consumer, publish, ack/nack/reject | everything: connection, user, vhost, channel |
| **queue actor** (pool worker) | enqueue, delivered, expired, dead-lettered, dropped | queue and vhost — **not** connection nor user |
| journal thread | `seJournalFlushed` | the batch: LSN and how many records |

The actor lives on the other side of the command mailbox, on purpose: that is what lets it never wait for I/O. It does not know who published nor who consumes, so **an actor-origin event goes out with `ConnectionId = 0` and empty `RemoteAddr`/`Username`**. That is not a field missing by oversight — it is the separation the architecture imposes, and D34 requires the unpopulated part to go out zeroed instead of as garbage.

To tie a delivery to a connection, join on the **`ConsumerTag`**: `seConsumerRegistered` (born on the read thread) carries the tag ↔ connection pair, and `seMessageDelivered` carries the tag.

One event per journal **batch**, never per record: group commit exists because `fsync` is expensive, and one event per record would cost more than the `fsync` it observes.

> The **cap is 32 types** (18 used). The subscription mask is a `Cardinal` read without a lock on the hot path; going past 32 means changing the mask type, and is a conscious decision. The **enum order is API**: a new type goes at the end.

## Which fields each type fills

The record is **flat** and the emitter always starts from a zeroed record, so **whatever the row does not list goes out zeroed** — empty string, numeric 0, `False`. It is not "undefined": it is zero, and the tests assert that.

All events carry `EventType`, `WallMs`, `TickMs`, `ConnectionId`, `RemoteAddr`, `Username` and `VHost` — with the caveat that `seConnectionEstablished` is born before the handshake, so in it `Username` and `VHost` are still empty.

| type | besides the connection context |
|---|---|
| `seConnectionEstablished` | — (`Username`/`VHost` empty: no handshake yet) |
| `seConnectionAuthenticated` | — (this is where `Username` and `VHost` become valid) |
| `seConnectionClosed` | `Reason` = error that brought the connection down (empty = clean close) |
| `seChannelOpened` | `ChannelNumber` |
| `seChannelClosed` | `ChannelNumber`; `Reason` = `connection teardown` when the channel died together with the connection, empty when it was the client that closed it |
| `seMessagePublished` | `ChannelNumber`, `ExchangeName`, `RoutingKey`, `MessageSize`, `Mandatory`, `Lsn`; `Reason` = `unroutable` if no queue was found |
| `seMessagePublishRejected` | `ChannelNumber`, `ExchangeName`, `RoutingKey`, `MessageSize`, `Mandatory`; `Reason` = `reject-publish` |
| `seConsumerRegistered` | `ChannelNumber`, `QueueName`, `ConsumerTag` |
| `seConsumerCancelled` | `ChannelNumber`, `QueueName`, `ConsumerTag` |
| `seMessageAcked` | `ChannelNumber`, `DeliveryTag`, `Multiple`, `Count` (how many deliveries the ack settled) |
| `seMessageNacked` | same, plus `Requeue` |
| `seMessageRejected` | `ChannelNumber`, `DeliveryTag`, `Requeue`, `Count` |
| `seMessageEnqueued` | `QueueName`, `MessageSize`, `Priority`, `Redelivered`, `ExchangeName`, `RoutingKey` |
| `seMessageDelivered` | same, plus `ConsumerTag` and `DeliveryTag`; `Reason` = `no-ack` when the consumer does not ack |
| `seMessageExpired` | `QueueName`, `MessageSize`, `Priority`, `ExchangeName`, `RoutingKey`; `Reason` = `expired` |
| `seMessageDeadLettered` | `QueueName` (the **source** one), `MessageSize`, `Priority`; `ExchangeName`/`RoutingKey` = **where it went** (the DLX); `Reason` = the `x-death` reason |
| `seMessageDropped` | `QueueName`, `MessageSize`, `Priority`, `ExchangeName`, `RoutingKey`; `Reason` = `maxlen` |
| `seJournalFlushed` | `Lsn` (watermark that became durable) and `Count` (records in the batch). No queue, no connection |

In the five actor-origin ones, `VHost` and `QueueName` are valid and the connection context goes out zeroed — see the previous section.

Two constraints that hold for all of them:

- **the message body never goes into the event**, only `MessageSize`. Carrying it would put a buffer copy on the hottest path there is;
- **`WallMs` is epoch in ms UTC** (`Int64`), not `TDateTime` — to correlate with an external log. `TickMs` is monotonic and serves to measure latency; it does **not** survive a restart.

**Dying into the DLX is not the same as being dropped.** A message that expires, or that is pushed out by the cap, first emits the event for the reason (`seMessageExpired` or `seMessageDropped`) and, **if the queue has a dead-letter exchange and the republish succeeds**, also emits `seMessageDeadLettered`. Without a DLX — or with a republish that failed — only the first one goes out. Counting `seMessageDeadLettered` to measure loss underestimates; counting `seMessageExpired` + `seMessageDropped` measures what actually left the queue on its own.

**Unroutable is not a rejected publish.** A message that found no queue is a successful publish: it gets an `ack`, and a `Basic.Return` if it was `mandatory`. It goes out as `seMessagePublished` with `Reason = 'unroutable'`. `seMessagePublishRejected` is something else — a full queue declared with `x-overflow: reject-publish`, which gets a `Basic.Nack`. Counting the two together gives a number that means nothing.

## Pitfalls

**Unsubscribe before destroying.** The handler is an object method — a (code, instance) pair. If you free the instance without `Unsubscribe`, the notifier calls a dead pointer: it becomes an `EAccessViolation`, which is caught and counted, but the event is lost and the symptom shows up far from the cause.

```pascal
Broker.Unsubscribe(Log.AoEvento);
Log.Free;
```

**Do not call `DrainEvents` from inside a handler.** It waits for the current handler to finish — and that handler would be you.

**An exception in the handler does not propagate.** It is caught, counted in `EventsFailed`, recorded in `LastEventFailure`, and the loop moves on to the next handler and the next event. The notifier never dies because of a subscriber. If your handler can fail, check the counter:

```pascal
if Broker.EventsFailed > 0 then
  Writeln('handler falhou: ', Broker.LastEventFailure);
```

**No lock is needed inside the handler** for state only it touches: the notifier is a single thread, so the handler never runs concurrently with itself. State shared with *other* threads of your app, that, yes, is your responsibility.

## In tests: wait for the condition, not for the clock

Delivery is asynchronous, so `Publish` followed by "did the event arrive?" is a race. Two barriers, and the choice between them matters:

- to assert **presence**, wait for the event (your capture sink counts and signals). `DrainEvents` alone **does not work**: it waits for the ring to empty, and a ring that has not yet received the event is already empty — a client publish has no round-trip, so draining right after it returns `True` before the broker has read the frame;
- to assert **absence**, `DrainEvents` is the right barrier, preceded by an operation **with** a round-trip (a `Channel.Close`, for example): when it returns, the broker has already gone through every emission point.

`tests\Server\AMQP.ServerEventsTests.pas` (and the FPCUnit mirror) show both forms.

## API reference

| `TAMQPServer` member | what it does |
|---|---|
| `Subscribe(AHandler)` | subscribes to all types |
| `Subscribe(AHandler, ATypes)` | subscribes to the types in the set; re-subscribing replaces the mask |
| `Unsubscribe(AHandler)` | removes — mandatory before destroying the method's owner |
| `EventQueueCapacity` | ring size, in events (default 4096). Only before `Start` |
| `DrainEvents(ATimeoutMs)` | waits for the ring to empty and the current handler to finish. `False` = the deadline ran out |
| `EventsEmitted` | how many entered the ring |
| `EventsDropped` | how many were dropped because the ring was full |
| `EventsFailed` | how many handler exceptions were caught |
| `LastEventFailure` | class and message of the last one (`''` if none) |

Functions in `AMQP.Server.Events`: `AmqpNewEvent` (zeroed record with type and clocks — use it if you emit events of your own), `AmqpEventMask` (set → mask) and `AmqpEventTypeName` (short type name, for logging).
