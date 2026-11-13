# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/). While the version is 0.x,
a minor version may change the API; every such change shows up here.

## [Unreleased]

The repository is English-only: comments, docs, identifiers, string literals and tool
interfaces. The client's public API is unchanged.

### Changed

- **Broker:** `TAMQPWal.BytesDescartados` is now `DiscardedBytes`, and the minimum-version
  build error reads `pascal-amqp-faa needs pascal-common-faa 1.27.0 or newer`.
- **Samples** moved to English folders and names: `PostoAutomacao` → `FuelStation` (with
  `pos`/`server`/`common`, and its AMQP/JSON contract in English), `PublicadorConfiavelVcl` →
  `ReliablePublisherVcl`, `EventosTopicVcl` → `TopicEventsVcl`, `EventosHeadersVcl` →
  `HeadersEventsVcl`, `PrioridadeVcl` → `PriorityVcl`, `ConsultaStatusVcl` → `StatusQueryVcl`,
  `Retaguarda(Vcl)` → `BackOffice(Vcl)`, `AutorizadorSim(Vcl)` → `AuthorizerSim(Vcl)`. Queue,
  exchange and header names used by the samples changed with them.
- **Tools:** `AMQP_ACEITACAO_DURAVEL` → `AMQP_ACCEPTANCE_DURABLE`, `tests/tools/matriz_de_queda.py`
  → `crash_matrix.py`, `tests/tools/verifica_espelhos.py` → `check_mirrors.py`; CLI options
  `--porta`/`--segundos`/`--tipos todos`/`--dedicado` → `--port`/`--seconds`/`--types all`/`--dedicated`.

## [0.6.0] - 2026-11-02

TCP and TLS through **pascal-common-faa 1.27.0** (step 5 of the "Migration order" in
`docs/transport-design.md` there): the client connects with `PcTcpConnect`/`PcTlsWrap`, the broker listens
with `PcTcpListen`/`PcTlsListen`, and both read and write through a `TAMQPEndpointStream`. The three
copies of the transport (amqp, redis, pipes) became one.

### Added

- **Private CA and mTLS in the client** (`TAMQPConnectionParams`): `TlsCaFile` (PEM, both backends:
  trusts THIS CA for the server instead of the system store), `TlsCertFile` + `TlsKeyFile`
  (PEM pair on OpenSSL) or `TlsCertFile` + `TlsCertPassword` (PFX on SChannel) for the client
  certificate. Before there was only the system store and no client certificate.
- **TLS backend chosen at run time**: `TAMQPConnectionParams.TlsBackend` and
  `TAMQPServer.TlsBackend` (`tbDefault`, `tbSChannel`, `tbOpenSsl`). On Windows both backends
  are in every build; elsewhere, OpenSSL is the only one.
- **Broker: server TLS on SChannel too** (Windows, PFX certificate with
  `TlsCertPassword`) and **mTLS** (`TAMQPServer.TlsCaFile`: requires a client certificate issued by
  that CA). Before, server TLS required `-dAMQP_OPENSSL`.
- `TAMQPConnectionParams.ConnectTimeoutMs` (0 = the common's default, 10 s).
- In `AMQP.Transport`: `TAMQPEndpointStream`, `AmqpTcpOptions`, `AmqpDefaultTlsBackend`,
  `AmqpResolveTlsBackend`, `AmqpRaiseTransportError` and overloads of `AmqpTlsBackendName`/
  `AmqpTlsBackendInfo` that take the backend. In `AMQP.Connection`: `AmqpClientTlsOptions`.
- Tests: `AMQP.TransportTests` (unit, 14: the exception boundary and the parameter
  mapping); the broker's TLS fixture runs in every build, with private CA, wrong name and mTLS
  accepted/refused, and on Windows also with OpenSSL on both sides and crossed (9 tests × 4); the PKI is
  pascal-common-faa's `tests/pki`, through the submodule.

### Changed

- **Requires pascal-common-faa 1.27.0** (check in `AMQP.Threading` and `MinVersion` of both
  `.lpk`; it was 1.8.0). Submodule at tag v1.27.0.
- **Transport error messages in English**, pascal-common-faa's (message text is not a
  contract). The classes that leave the lib are still `EAMQPTransport` and `EAMQPTls` (no
  `EPcTransport*` leaves), and `EAMQPTls` is still outside the `EAMQPTransport` hierarchy, so that an old
  `except on EAMQPTransport` does not start catching TLS errors.
- **Failure to connect** raises `EAMQPTransport`. Before, the RTL's exception came out (`ESocketError` from
  `ssockets` on FPC, from `System.Net.Socket` on Delphi).
- `-dAMQP_OPENSSL` now **only switches the default backend** to OpenSSL (`AmqpDefaultTlsBackend`). The
  `openssl` build modes still apply.
- **Deadlines**: connect has a budget (`ConnectTimeoutMs`, 10 s by default; before it was the OS's) and the
  TLS handshake has the common's default deadline, 15 s, on both sides; it is removed as soon as the handshake
  ends. Read and write still have **no deadline** (AMQP has heartbeat). TCP_NODELAY is still
  on for every socket.
- **Broker: the certificate is loaded at `Start`** (once, by the listener), and a wrong file or password
  raises `EAMQPTls` there, with the broker stopped. Before, each handshake failed. A `Start`
  that cannot listen (port in use, certificate) undoes what it had already brought up (journal,
  event notifier).
- Broker: the TLS handshake still runs on the connection's thread (`TPcEndpoint.Handshake`), never on the
  accept thread. `BindAddress` accepts `'::'` (all IPv6 interfaces).
- `TAMQPServerConnection.Create` takes a `TPcEndpoint`; `TAMQPServerConnConfig` lost
  `TlsCertFile`/`TlsKeyFile` (the certificate belongs to the listener).
- `TAMQPServer.Backlog` is **ignored** (the common's listener uses its own); it stays so as not to break
  whoever assigns it.

### Removed

- `TAMQPTcpSocket`, `TAMQPTcpListener`, `TAMQPSocketStream` and `AmqpSetTlsBackendDetail`
  (`AMQP.Transport`), `TAMQPSchannelStream` (`AMQP.Transport.Tls`) and `TAMQPOpenSslStream`
  (`AMQP.Transport.OpenSSL`). The two TLS units stay empty, only pulling in the common's, for one
  minor version; they go away in the next.

### Known

- A **non-TLS** client against the TLS port of an **SChannel** broker only drops after the handshake
  deadline (15 s): SChannel waits for a complete TLS record, and the AMQP protocol-header is 8
  bytes. With OpenSSL the refusal is immediate. Recorded for pascal-common-faa in
  `.ci/transport-findings-for-pascal-common-faa.md`.

## [0.5.0] - 2026-10-31

pascal-db-faa's transactional outbox through an AMQP broker: the bridge pascal-agenda-faa used
(`Agenda.Outbox.Amqp`, decision 11 of its M7) moved here. Manual in `docs/outbox.md`.

### Added

- **`AMQP.Outbox`** (`src/outbox`), **outside the packages**: it needs pascal-db-faa 0.18.0 or
  newer (`PascalDb.Outbox`), which the client and the broker do not. The API is pascal-agenda-faa's:
  - `TOutboxAmqpSink`, the `IOutboxSink` that publishes each event (persistent, on a `topic` exchange,
    routing key = type, `tenant-id` header) on a channel in confirm mode and waits for the ack; without an ack,
    it raises, and the relay tries again.
  - `TOutboxAmqpConsumer`, which runs an `IOutboxHandler` on its own queue: it records the event in
    `PROCESSED_EVENTS` before the ack and does not run an already recorded event again; a failure becomes a `Nack`
    without requeue, which the dead-letter takes to the `.retry` queue (TTL) and back; the attempts come
    from `x-death`, and the last one parks the message in the `.dead` queue.
  - `TOutboxAmqpQueue`, `OutboxAmqpDeclareExchange`, `OutboxAmqpDeclareQueues`,
    `OutboxAmqpPending`, `OutboxAmqpEvent`, `OutboxAmqpPurgeProcessed`, and the
    `outbox.consumer.messages` metric.
  - The SQL (`PROCESSED.EXISTS`, `PROCESSED.INSERT`, `PROCESSED.PURGE`) belongs to the application, read by name
    from the factory's loader; reference table and commands in `docs/outbox.md`.
- `tests/Outbox` suite (8 tests, mirrored DUnitX and FPCUnit): the sink and the consumer against the
  embedded broker, with an SQLite file (SQLdb on FPC, FireDAC on Delphi). No RabbitMQ.
  `tools/test_fpc_docker.sh` gained the `outbox` suite.

### Changed

- The `PROCESSED_AT` recorded by the consumer comes from pascal-common-faa's `TClock` (it was
  pascal-api-infra-faa's `TJwt.UnixNow`, used only as a clock): the bridge does not pull in a third lib,
  and the tests swap the clock with `TClock.SetClock`.
- Test submodules: pascal-common-faa 1.12.0 → **1.26.0**, and **pascal-db-faa 0.18.0** (new,
  only for the outbox suite). The minimum the client and the broker require is still 1.8.0; whoever uses the
  bridge needs what pascal-db-faa requires (1.17).

### Fixed

- **Broker: the monitor thread's delivery tick could post to an already freed queue.**
  `TAMQPEngine.DeliverTick` copied the queues under the engine lock, released the lock and only then
  posted to each one; a `Queue.Delete` in that window freed the queue (`DeleteQueue` frees under the
  same lock), and the command landed on a dead object. Found by heaptrc in this round: one
  leaked command (200 bytes), once in 16 runs of the acceptance durable pass. Now the posts go out
  with the lock held (`Post` never waits). The same pattern — a queue pointer used after
  releasing the lock — exists in the users of `TAMQPEngine.FindQueue`; it is recorded in the worklog for a
  dedicated broker session.

## [0.4.2] - 2026-10-27

Defect found by pascal-agenda-faa (its `docs/design.md`, section "M4: the broker, the compose
file, the trace"), in the test where the consumer stopped right after the handler started.

### Fixed

- **Closing a channel with a consumer callback in flight brought down the whole connection.** `Close`
  sent `Channel.Close`, waited for `Close-Ok` and only then waited for the callbacks. A callback
  that finished in that window and called `Ack` sent `Basic.Ack` on an already closed channel, and per the spec
  that is a connection error: `504 CHANNEL_ERROR`, all channels go down. The channel's `Free` went
  through the same path. Measured before changing, against RabbitMQ 3
  (`504 CHANNEL_ERROR - expected 'channel.open'`) and against the embedded broker. Now `Close`
  cancels the channel's consumers, waits for the callbacks with the channel still open (their `Ack`
  counts, and nothing already processed is redelivered) and only then sends `Channel.Close`.
- **The same 504 when it was the broker that closed the channel** (a 404 or 406 with a callback in flight).
  `Ack`/`Nack` on a closed or closing channel no longer go to the wire and no longer raise: the
  broker has already returned to the queue, as `redelivered`, whatever was left unacked. After a connection
  drop nothing changes: `Ack`/`Nack` raise `EAMQPConnection`, as before.

### For users

- The workaround the consumer did before closing the channel (cancel the consumer and wait for the
  callbacks in flight) is no longer necessary. A flag of your own for "do not handle after Stop"
  is still useful if the callback must not start new work during shutdown.

## [0.4.1] - 2026-10-15

### Fixed

- **`OnConsumerError` ran without a current span**, and the log written in it went out without the
  message's trace id (with the `try` inside the callback, as up to 0.3.0, it went out with it). The `process <queue>` span
  was closed before the handler was called. Now it stays current during the handler and the
  `Nack`, and ends afterwards, marked with the callback's error (`error.type` and status, as before).
  Found by pascal-api-infra-faa in `samples/04-messaging`; measured here with a test that reads
  `TPcTracing.Current` inside the handler (empty in 0.4.0).

## [0.4.0] - 2026-10-15

Defect found while measuring pascal-api-infra-faa's `docs/guide/dtos-json.md` guide (FPC
3.2.2/Windows against RabbitMQ 3).

### Fixed

- **An exception in a consumer callback vanished, and the delivery was left unacked.**
  `TAMQPDeliveryWork` re-raised the exception, and the pool (`TPcThreadPool`) swallows it on purpose: nothing
  was logged, the message stayed unacked until the connection dropped, came back on every process restart
  and got stuck again, and with `Qos(1)` the whole consumer stopped. Measured: 7 messages with the 4th
  raising left 0 ready and 1 unacked; with `Qos(1)` and the 1st of 5 raising, 1 callback
  in 8 s and 4 ready forever. Now the pending delivery gets a `Nack` without requeue (it goes to the
  queue's dead-letter exchange, if any; without a DLX, the broker drops it), and the 4 go through.
  **Behavior change:** whoever counted on the message coming back after a drop now
  sees it in the DLX (or dropped); for the old behavior, `AAction := ceaNone` in the handler.

### Added

- **`TAMQPChannel.OnConsumerError(AChannel, ADelivery, AError, var AAction)`**, called when a consumer
  callback raises, on the same pool thread. `AAction` (`TAMQPConsumerErrorAction`)
  arrives as `ceaReject` if the delivery is pending, or `ceaNone` if it was already settled or if the
  consumer is no-ack; the handler can change it to `ceaRequeue` (Nack with requeue) or `ceaNone`
  (the application settles later). The lib never settles twice: if the callback or the handler already
  called `Ack`/`Nack`, nothing is sent (settling the same tag again is `406` and closes the channel).

## [0.3.0] - 2026-10-15

Two defects found while measuring pascal-api-infra-faa's messaging guide (FPC 3.2.2/Windows
against RabbitMQ 3).

### Fixed

- **Automatic reconnection did not recover when the queue was declared only on another channel.** The
  topology replay was channel by channel, in the order of the channels' `TDictionary` (the hash order, not the
  creation order: with channels 1, 2 and 3, 3 came first). With the non-durable queue declared on
  channel 1 and consumed on 3, the broker restart deleted it, channel 3's `basic.consume` got
  `404` before channel 1's declare came back, and every attempt failed the same way, forever, without
  any signal. Now the replay is of the whole connection, phase by phase: exchanges, queues, bindings and only
  then consumers, in channel creation order. Measured with `docker restart`: before, every
  attempt with the broker up got the same `404` until the end of the measurement (90 s in the guide's);
  after, recovered on the first attempt with the broker up.
- **Publish on a dead connection raised `EWriteError` ("Stream write error") instead of
  `EAMQPConnection`.** The connection now knows when it is not writable (before the handshake, after the
  drop) and raises `EAMQPConnection` without touching the stream, with the reason in the text
  (`connection closed by server: 320 CONNECTION_FORCED - ...`); a write that fails before the
  drop is noticed also becomes `EAMQPConnection` (`connection write failed (...)`), the same on both
  compilers and on every transport backend.

### Added

- **`TAMQPConnection.OnReconnectError(Conn, Attempt, Reason)`**, fired on each reconnection
  attempt that fails, and **`TAMQPConnection.LastReconnectError`**, the message of the last one (cleared
  when the reconnection succeeds). The `EAMQPConnection` of a publish made during the reconnection carries
  the reason for the drop and that of the last attempt; the start of the text
  (`connection unavailable right now (reconnecting`) is the same as before.
- `AMQP_TEST_HOST`/`AMQP_TEST_PORT` variables to point the integration suite at another
  RabbitMQ (the default is still `localhost:5672`).

## [0.2.0] - 2026-10-12

Phase D of pascal-api-infra-faa's observability design: OpenTelemetry spans in the
client, on top of pascal-common-faa's `PascalCommon.Tracing`. Details and decisions in
[`docs/tracing.md`](docs/tracing.md).

### Added

- **Spans per publish and per consumed message**, opened by the channel itself when the
  application turns the tracer on (`TPcTracing.Start`): `publish <exchange>` (producer, child of the current
  span), `process <queue>` around the `Consume` callback (consumer, child of the message's
  `traceparent` and current span during the callback) and `receive <queue>` on
  `BasicGet` (client). Attributes from the OpenTelemetry messaging conventions. With the tracer
  off nothing changes: no span, no header, the same bytes.
- **`traceparent`/`tracestate` in the published message's headers**, injected only into the
  content header bytes: the caller's header table is not changed, and a `traceparent` the
  application already set wins.
- Unit `AMQP.Tracing`: `AmqpTraceParentOf`, `AmqpStartProcessSpan` (to process the result
  of a `BasicGet` in the producer's trace), `AmqpFinishSpan` and the other pieces.
- `BuildContentHeader` with trace context and `TAMQPWriter.WriteFieldTable` with extra pairs
  (overloads; the old forms stay the same).

### Changed

- **The minimum pascal-common-faa version goes up from 1.1.3 to 1.8.0**, which brings
  `PascalCommon.Tracing`; it is the same one the other `*-faa` libs require. An older copy
  stops the build with `pascal-amqp-faa needs pascal-common-faa 1.8.0 or newer`; both
  `.lpk` require `MinVersion` 1.8.
- Submodule `external/pascal-common-faa` (tests and samples only) from v1.2.0 to v1.8.0.

## [0.1.2] - 2026-10-05

### Changed

- **The minimum pascal-common-faa version goes up from 1.0 to 1.1.3.** Before 1.1.3 `PcPool`
  did not grow during a burst of items: with one idle worker, deliveries whose callbacks block
  ran one at a time, and a callback waiting for another one queued behind it only moved
  when another item arrived (found here, fixed there). The client's callbacks run on
  `PcPool`, so an older copy is a production defect. A copy older than 1.1.3 stops the
  build with `pascal-amqp-faa needs pascal-common-faa 1.1.3 or newer`; both `.lpk`
  require `MinVersion` 1.1.3.
- Submodule `external/pascal-common-faa` (tests and samples only) from v1.1.2 to v1.2.0, which
  also counts the real cores on FPC/Linux (`PcPool`'s ceiling there was always 16).

### Fixed

- The intermittent failure of `ConsumesAll_WithAck_And_Concurrency` (concurrency peak 1) was the
  burst above; it goes away with pascal-common-faa 1.1.3 or newer.

## [0.1.1] - 2026-10-05

Fixes coming from the migration of pascal-dfe-broker to v0.1.0 (findings 4 and 6 of its F10),
plus what this round's validation found. No API change besides one diagnostic
function.

### Fixed

- **`Close`/`Free` of the connection during reconnection waited for the whole `ReconnectDelayMs`.** The
  wait between attempts was a `Sleep`; now it is an event that `Close` signals, and closing is
  immediate (measured: 3000 ms → 38 ms).
- **With `ReconnectDelayMs` above 12 s, `Close`/`Destroy` freed the connection with the
  reconnection thread still alive**, which then woke up reading the freed object. The wait gave up
  at a 12 s cap; now the thread is joined, and `Close`/`Destroy` only return after it exits.
  An attempt already in progress (connect, handshake, topology replay) finishes first, bounded
  by its timeouts.
- `Close` called from inside `OnDisconnect`/`OnReconnect`/`OnReconnectFailed` (which run on the
  reconnection thread) stayed stuck for 12 s waiting for its own thread; now it returns at once.
- Broker: **generated queue names (`amq.gen-…`) repeated across connections.** The sequence was
  per connection, and two connections in the same millisecond generated the same name; with the first queue
  exclusive and alive, the second one's declare got `405`. The sequence is now per process.
- The test suites compile on **Delphi Win64** (assertions that compared `Integer` with the
  `NativeInt` of `Length`/`Count`); all four pass on Win32 and Win64.

### Added

- `AmqpReconnectThreadsAlive` (`AMQP.Connection`): how many reconnection threads exist in the
  process. Serves tests and diagnostics.

### Documented

- `Close`/`Free` of a channel also wait for the callbacks **still queued** on the pool; on a regular
  channel the pool is `PcPool`, the whole process's, and a `PcPool` saturated by another lib delays
  the close (measured: 1.5 s of saturation, 1.5 s of `Free`). No deadline, on purpose;
  `CreateChannel(True)` does not depend on `PcPool` (29 ms in the same scenario). In both READMEs.

### Changed

- Submodule `external/pascal-common-faa` (tests and samples only) from v1.0.1 to v1.1.2. The minimum
  required of the application is still 1.0.

## [0.1.0] - 2026-10-04

First published version. AMQP 0-9-1 client for Delphi 12 and FPC 3.2.2/Lazarus (Windows and
Linux x86_64/ARM64) in a single codebase, and an AMQP 0-9-1 broker embeddable in the same process as the
application (separate package).

### Added

- **Client**: connection and channels, declare/bind/delete of exchanges and queues, publish with
  properties, push consumption and `Basic.Get`, ack/nack/reject, `Basic.Qos`, publisher confirms,
  `mandatory` with `Basic.Return`, automatic reconnection with replay of topology and consumers,
  heartbeats, TLS (SChannel on Windows; OpenSSL on any platform with `-dAMQP_OPENSSL`),
  `Connection.Blocked`/`Unblocked`, channel with a dedicated worker (`CreateChannel(True)`).
- **Embedded broker** (`pascal_amqp_faa_server.lpk`): SASL PLAIN handshake, exchanges
  direct/fanout/topic/headers and exchange→exchange, queues as actors, confirms, `mandatory`,
  exclusive/auto-delete queues; queue and message TTL, dead-lettering with `x-death`,
  priority, `x-max-length`/`x-max-length-bytes` with `x-overflow`, `alternate-exchange`,
  `x-expires`; opt-in durability through `DataDir` (WAL with group commit, recovery at `Start`,
  confirm tied to the watermark, compaction, disk cap that refuses); opt-in observability
  (events through a notifier thread, best-effort). Deliberate deviations from RabbitMQ listed in the
  README.
- Messaging pattern samples (dual VCL/LCL GUI) and `FuelStation`; unit,
  integration, broker and acceptance suites (the client's integration suite against the embedded
  broker), mirrored in DUnitX and FPCUnit.

### Changed

- **New dependency: [pascal-common-faa](https://github.com/fabianoallex/pascal-common-faa)
  1.0 or newer** (phase F8 of its plan). Atomics, ticks, monitor and thread pool left
  `AMQP.Threading`, without aliases: `AmqpAtomic*` → `PcAtomic*`, `AmqpTickMs` → `PcTickMs`,
  `TAMQPMonitor` → `TPcMonitor`, `TAMQPWorkItem` → `TPcWorkItem`, `TAMQPThreadPool` →
  `TPcThreadPool`, `AmqpPool` → `PcPool`, `AMQP_WAIT_INFINITE` → `PC_WAIT_INFINITE`
  (`TAMQPCondGen` and `TAMQPPoolWorker` became private types there). `AmqpWallMs` stays in
  `AMQP.Threading`. The application provides the single copy of pascal-common-faa; the packages require it
  by name, and a copy older than the minimum stops the build with a clear message.
- The client's callbacks run on `PcPool`, **one pool for the whole process**, shared with
  the other `*-faa` libs.
- **The broker runs the queue actors on a pool of its own** (the `TAMQPServer`'s), not on the
  process pool: with the process pool saturated by callbacks that block, `Queue.Declare`
  waited 15 s and the connection dropped; with its own pool, it answers in milliseconds.

### Fixed

- Broker: double free of the argument table in `Queue.Declare` when the queue actor did not
  answer in time (the channel freed the table the queue descriptor already owned) — access
  violation in the broker's `Destroy`.
- Journal rotation test that assumed the rotation was visible right after the watermark (it failed
  under load).

[Unreleased]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fabianoallex/pascal-amqp-faa/releases/tag/v0.1.0
