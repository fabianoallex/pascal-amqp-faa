# Transactional outbox through an AMQP broker (`src/outbox`)

`AMQP.Outbox` connects the transactional outbox of [pascal-db-faa](https://github.com/fabianoallex/pascal-db-faa)
(`PascalDb.Outbox`, 0.18.0 or newer) to an AMQP broker: the **sink** publishes each event the
relay delivers, and the **consumer** runs a handler on its own queue, with retry through a
waiting queue, a dead queue and deduplication in the database before the ack. It landed in **v0.5.0** (2026-10-31).

It came from pascal-agenda-faa (`Agenda.Outbox.Amqp`, decision 11 of its M7), with the same API: what
was costly to copy is the retry queue with TTL, the attempt count through `x-death`, the dead
queue and the deduplication before the ack. The only change was the clock: there it was
pascal-api-infra-faa's `TJwt.UnixNow`, used only as a clock; here it is pascal-common-faa's `TClock`,
and the bridge does not pull in a third lib.

## A bridge, outside the packages

The unit lives in `src/outbox`, **outside** `pascal_amqp_faa.lpk` and `pascal_amqp_faa_server.lpk`,
like pascal-api-infra-faa's `src/redis`: it needs pascal-db-faa, which the client and the broker
do not. Whoever does not use the outbox neither compiles nor links anything of it.

| Build | What to add |
|---|---|
| Lazarus | the `pascal_db_faa` package (from the copy the application provides) in the project's required packages; `src/outbox` in *Other unit files* and `src` in *Include files* (the unit starts with `{$I amqp.inc}`) |
| Plain FPC | `-Fu<amqp>/src/outbox -Fi<amqp>/src -Fu<pascal-db-faa>/src -Fi<pascal-db-faa>/src` |
| Delphi | `src\outbox` and pascal-db-faa's `src\` in the search path |

Like pascal-common-faa, pascal-db-faa belongs to the **application**: this repository's
`external/pascal-db-faa` submodule serves only the tests. pascal-db-faa 0.18.0 requires
pascal-common-faa 1.17 or newer, so whoever uses the bridge needs that one, even though the client
alone accepts 1.8.

## The message

| Event field (`TOutboxEvent`) | In the message |
|---|---|
| `Payload` | the body, in UTF-8, `content-type: application/json` |
| `Id` | `message-id` |
| `Kind` | the `type` property and the **routing key** |
| `CreatedAt` | `timestamp` |
| `TenantId` | the `tenant-id` header (`OUTBOX_AMQP_TENANT_HEADER`) |
| `TraceParent` | not sent as a field: the relay's span is the current one when the sink publishes, and the client's `publish` puts the `traceparent` in the headers (see [tracing](tracing.md)) |

Every message goes out **persistent** (`delivery-mode 2`), on a `topic` exchange.

## The sink: `TOutboxAmqpSink`

```pascal
uses PascalDb.Outbox, AMQP.Connection, AMQP.Outbox;

OutboxAmqpDeclareExchange(Channel, 'shop.events');
Relay := TOutboxRelay.Create(Factory,
  TOutboxAmqpSink.Create(RelayChannel, 'shop.events'),
  PdbOutboxNewOwner('relay'), TOutboxOptions.Default);
// in a timer or in a worker thread:
Relay.RunOnce(NowInUnixSeconds);
```

- The constructor puts the channel in **confirm mode**, and each `Deliver` waits for the broker's ack
  (`AConfirmTimeoutMs`, default 5 s). Without an ack, or with the connection down, it raises, and the relay leaves the
  event pending for the next pass. The relay does not give up on a sink: a broker that is down comes back.
- `Deliver` runs **inside the relay's transaction** (the `IOutboxSink` contract): a slow broker
  holds that transaction (in SQLite, the write lock of the whole database).
- The channel does not belong to the sink (whoever creates it frees it) and is used by one thread at a time, the relay's.
- An event whose `Kind` matches no binding is accepted by the broker and dropped: the confirm goes out
  all the same. That is how a new queue only receives what was published after it exists.

## The consumer: `TOutboxAmqpConsumer`

```pascal
Queue := TOutboxAmqpQueue.Create('shop.email', 'shop.events', 'order.#');
Queue.RetryDelayMs := 30000; // default
Queue.MaxAttempts := 10;     // default (OUTBOX_DEFAULT_MAX_ATTEMPTS)
OutboxAmqpDeclareQueues(Channel, Queue);

Consumer := TOutboxAmqpConsumer.Create(Factory, ConsumerChannel, Queue, 'email', EmailHandler);
Consumer.OnError := Log.OnOutboxError; // a method
Consumer.Start;
...
Consumer.Stop;           // before closing the channel and freeing the consumer
ConsumerChannel.Free;
Consumer.Free;
```

### Three queues per consumer

`OutboxAmqpDeclareQueues` declares, all durable:

```
exchange (topic) --binding--> shop.email --nack--> shop.email.retry --TTL--> shop.email
                                  |                (no consumer)
                                  +--(MaxAttempts)--> shop.email.dead
```

- **`shop.email`**, the work queue, bound to the exchange by the binding key, with dead-letter to
  the retry queue (through the default exchange, which routes by queue name).
- **`shop.email.retry`**, with no consumer, with `x-message-ttl = RetryDelayMs` and dead-letter back
  to the work queue. It is the **queue's** TTL, not a per-message one: a message's `expiration`
  only expires when the message reaches the head of the queue.
- **`shop.email.dead`**, where the message that used up its attempts stays parked, as it arrived.

Changing `RetryDelayMs` changes the retry queue's arguments, and redeclaring a queue with other
arguments is `PRECONDITION_FAILED` (406): delete the queue first.

### What each delivery turns into

1. If the handler does not accept the `Kind` (another type under the same binding key): **ack**, nothing runs.
2. If the event is already in `PROCESSED_EVENTS` for this consumer (a redelivery by the broker, or the
   relay that published twice after losing the lease): **ack**, the handler does not run again.
3. Otherwise it runs the handler, records the event in `PROCESSED_EVENTS` and **acks**. It is at-least-once: a
   crash between the handler and the recording runs the handler again.
4. A handler that raises: `OnError`, and **`Nack` without requeue**. The work queue's dead-letter takes
   the message to the retry queue, and it comes back after the TTL.
5. The attempts already made are the `rejected` entries **of the work queue** in the `x-death` header
   (the broker does not count attempts by itself). On the failing attempt number `MaxAttempts`, the consumer
   publishes the message to the dead queue and acks it.
6. A message that is not an outbox event (no `message-id`, `type` or `tenant-id`) goes straight to the
   dead queue.

The callbacks run **in parallel** on the process pool (`PcPool`), up to the prefetch
(`TOutboxAmqpQueue.Prefetch`, default 10): the handler has to accept that. `InFlight` counts the
callbacks in progress, for whoever waits for the consumer to settle.

`Stop` cancels the consumer and waits (up to `AWaitMs`) for the callbacks in progress; a callback that
starts after that neither runs the handler nor acks, and closing the channel returns the message to the
queue. Call it before closing the channel (no new work while the process shuts down) and before
freeing the consumer.

`OutboxAmqpPending` sums the ready messages in the work and retry queues (not the ones delivered
but not yet acked), and `OutboxAmqpEvent` reads the event from a delivery, with the attempts.

### Metrics

In pascal-common-faa's `PcMetrics`, counted after the ack, the nack or the parking:

| Metric | Labels |
|---|---|
| `outbox.consumer.messages` | `outbox.consumer`, `outbox.event.kind` (`other` for a type the handler does not accept, so that whoever publishes to the exchange does not inflate the series), `outbox.result` (`done`, `duplicate`, `ignored`, `retried`, `parked`) |

## The SQL belongs to the application

Like `PascalDb.Outbox`'s, the SQL is read **by name** from the factory's loader, and the table belongs to the
application:

| Constant | Name | Parameters |
|---|---|---|
| `PROCESSED_SQL_EXISTS` | `PROCESSED.EXISTS` | `CONSUMER`, `TENANT_ID`, `EVENT_ID`; one row if already processed |
| `PROCESSED_SQL_INSERT` | `PROCESSED.INSERT` | `CONSUMER`, `TENANT_ID`, `EVENT_ID`, `NOW` (Unix seconds) |
| `PROCESSED_SQL_PURGE` | `PROCESSED.PURGE` | `BEFORE` (Unix seconds); called by `OutboxAmqpPurgeProcessed` |

Reference (**tested on SQLite**, by the `tests/Outbox` suite; the same form works on PostgreSQL,
which is what pascal-agenda-faa runs):

```sql
CREATE TABLE PROCESSED_EVENTS (
  CONSUMER VARCHAR(60) NOT NULL,
  TENANT_ID VARCHAR(40) NOT NULL,
  EVENT_ID VARCHAR(32) NOT NULL,
  PROCESSED_AT BIGINT NOT NULL,
  CONSTRAINT PK_PROCESSED_EVENTS PRIMARY KEY (CONSUMER, TENANT_ID, EVENT_ID));
CREATE INDEX IX_PROCESSED_EVENTS_AT ON PROCESSED_EVENTS (PROCESSED_AT);

-- PROCESSED.EXISTS
SELECT 1 AS FOUND FROM PROCESSED_EVENTS
 WHERE CONSUMER = :CONSUMER AND TENANT_ID = :TENANT_ID AND EVENT_ID = :EVENT_ID

-- PROCESSED.INSERT
INSERT INTO PROCESSED_EVENTS (CONSUMER, TENANT_ID, EVENT_ID, PROCESSED_AT)
VALUES (:CONSUMER, :TENANT_ID, :EVENT_ID, :NOW)
ON CONFLICT (CONSUMER, TENANT_ID, EVENT_ID) DO NOTHING

-- PROCESSED.PURGE
DELETE FROM PROCESSED_EVENTS WHERE PROCESSED_AT < :BEFORE
```

`ON CONFLICT ... DO NOTHING` covers two consumers of the same queue that received the same
message (a redelivery while the first was still running). On a database without that form, a plain `INSERT`
works too: the primary key rejects the second one, the callback treats it as a failure, and the
next redelivery falls into step 2 (already processed) and gets acked. `PROCESSED_AT` is
pascal-common-faa's `TClock` in Unix seconds (UTC), replaceable in tests (`TClock.SetClock`).

Purging `PROCESSED_EVENTS` is up to the application, at whatever pace it wants: a row only needs to live
while the broker can still redeliver that event.

## Tests

`tests/Outbox` (DUnitX `AMQP.OutboxTests.dproj`, FPCUnit `fpc/AMQPOutboxTestsFpc.lpi`, generated
mirror): 8 tests against the **embedded broker** on an ephemeral port, with an SQLite file in the
temp directory (`AMQP.OutboxTestDb`: SQLdb on FPC, FireDAC on Delphi). No RabbitMQ.

- the published message and its fields, with confirm;
- handled once and recorded; redelivered and not run again; another type, ack without running;
- a failure redone from the retry queue; parked after the attempts;
- the purge by age; `PROCESSED_AT` coming from `TClock` (the mutation "read `SysUtils.Now` instead of
  `TClock`" kills this one).

FPC loads the SQLite library: on Windows, the `sqlite3.dll` **from sqlite.org** (Python's
lacks the metadata functions SQLdb uses; see pascal-db-faa's gotcha 23) next to the runner,
or in `AMQP_SQLITE_CLIENT`; on Linux, `libsqlite3.so.0` (`apt install libsqlite3-0`). FireDAC
links SQLite into the program. On Linux, `SUITES=outbox sh tools/test_fpc_docker.sh`.
