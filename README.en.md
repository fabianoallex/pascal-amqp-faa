# pascal-amqp-faa

> 🇧🇷 Este documento também está disponível em [português](README.md) — a versão em português é a canônica; em caso de divergência, ela prevalece.

**AMQP 0-9-1** client (RabbitMQ) for **Free Pascal / Lazarus** and **Delphi**, from a single codebase. Cross-platform port of [delphi-amqp-faa](https://github.com/fabianoallex/delphi-amqp-faa) (same author, MIT), which was Delphi/Windows-only.

## Features

- Full handshake (`Start/Tune/Open`) with correct `channel-max`/`frame-max`/`heartbeat` negotiation (compatible with RabbitMQ 3.13+).
- Publish/consume with **manual ack** (at-least-once); consumer callbacks dispatched on the library's **own thread pool** — the read thread never runs user code.
- **Publisher confirms** (`confirm.select`): `Publish` returns a seq-no, asynchronous `OnConfirm`, `WaitForConfirm`/`WaitForConfirms`.
- `Basic.Return` (unroutable `mandatory` publish), `Connection.Blocked/Unblocked` (broker resource alarm).
- **Heartbeat** on a dedicated thread (dead-connection detection + sending when idle).
- **Automatic reconnection** (opt-in) with topology recovery (exchanges, queues, bindings, qos, confirm mode and consumers) and, optionally, republishing of unconfirmed publishes (`RepublishUnconfirmedOnReconnect`).
- **TLS (amqps)** with two backends behind the same API: native **SChannel** on Windows (automatic, zero dependencies) and **OpenSSL** on any platform (opt-in via `-dAMQP_OPENSSL`) — see the [TLS (amqps)](#tls-amqps) section.
- `Queue.Unbind`, `Exchange.Bind/Unbind` (RabbitMQ extension), `Basic.Get`, `Qos`.

## Compatibility

| Compiler | Status |
|---|---|
| FPC 3.2.2 (Lazarus 4.0), Win64 | Compiles; smoke test, FPCUnit suite (80 unit + 28 integration) and the 4 samples pass against a real RabbitMQ |
| Delphi (tested on 12 / Athens) | Same codebase; DUnitX suite (80 unit + 28 integration) and the 4 samples validated through the IDE (Community Edition has no command-line compiler) |
| FPC 3.2.2, Linux x86_64 (Debian, container) | Compiles; smoke test (plain and `--tls` with `-dAMQP_OPENSSL`), FPCUnit suite (80 unit + 27 integration, TLS included via OpenSSL) and the 4 samples (console and GUI/LCL-GTK2) pass against a real RabbitMQ |
| FPC 3.2.2, Linux ARM64 (Debian, container/QEMU) | Same coverage as x86_64: smoke test plain and `--tls` (aarch64 OpenSSL) and FPCUnit suite 80 + 27 pass against a real RabbitMQ |

Porting decisions (see `CLAUDE.md` for details):

- **Callbacks are `procedure ... of object`** (not `reference to`), because stable FPC has no anonymous methods. In Delphi, use methods of your own class instead of lambdas.
- No `System.Threading`/`TTask` and no `System.TMonitor`: the library ships `AMQP.Threading` (portable thread pool + monitor/condvar + atomics).
- Socket layer in `AMQP.Transport` (`System.Net.Socket` on Delphi, `ssockets` on FPC).

## Quick start

```pascal
uses AMQP.Connection, AMQP.Exchange.Methods, AMQP.Queue.Methods;

type
  TMyConsumer = class
    procedure OnMsg(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  end;

procedure TMyConsumer.OnMsg(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
begin
  // runs on a pool thread; process and acknowledge:
  WriteLn(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
end;

var
  Conn: TAMQPConnection;
  Chan: TAMQPChannel;
  Consumer: TMyConsumer;
begin
  Conn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  Conn.Open;
  Chan := Conn.CreateChannel;
  Chan.DeclareQueue(TAMQPQueueDeclare.Create('my-queue'));

  Chan.ConfirmSelect; // publisher confirms (optional)
  Chan.PublishText('', 'my-queue', 'hello');
  Chan.WaitForConfirms(5000);

  Consumer := TMyConsumer.Create;
  Chan.Qos(10);
  Chan.Consume('my-queue', Consumer.OnMsg);
  // ... Chan.Close; Chan.Free; Conn.Close; Conn.Free; Consumer.Free;
end;
```

With FPC on **Linux**, add `cthreads` as the first unit of the program. For non-ASCII strings on FPC outside Lazarus, call `SetMultiByteConversionCodePage(CP_UTF8)` (Lazarus applications already default to UTF-8).

> `TAMQPConnection` owns the internal threads (read + heartbeat). Free the channels before the connection (or let the connection's `Free` take care of them).

## Usage in detail

### Declaring queue / exchange / binding

```pascal
// Named durable queue:
Chan.DeclareQueue(TAMQPQueueDeclare.Create('invoice.responses', True));

// Topic exchange + binding:
var
  LBind: TAMQPQueueBind;
begin
  Chan.DeclareExchange(TAMQPExchangeDeclare.Create('invoice', AMQP_EXCHANGE_TYPE_TOPIC));
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := 'invoice.responses';
  LBind.ExchangeName := 'invoice';
  LBind.RoutingKey := 'response.#';
  Chan.BindQueue(LBind);
end;
```

### Publishing with properties

```pascal
uses AMQP.Wire, AMQP.Basic.Methods;

var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  LProps.SetPersistent;                       // delivery-mode 2
  LProps.SetCorrelationId('Invoice3524...9012');
  LProps.SetMessageId('req-42');
  Chan.Publish('', 'invoice.responses',
    AmqpUtf8Encode('{"status":"authorized"}'), LProps);
end;
```

The empty exchange (`''`) routes by *routing key* = queue name (default exchange). Without confirm mode, publishing is fire-and-forget and `Publish` returns 0. `AmqpUtf8Encode` (unit `AMQP.Wire`) is the portable replacement for `TEncoding.UTF8.GetBytes`.

### Consuming one message (pull)

```pascal
var
  LMsg: TAMQPGetResult;
begin
  LMsg := Chan.BasicGet('invoice.responses', True {no-ack});
  if LMsg.Found then
    WriteLn(LMsg.BodyAsText);
end;
```

For continuous consumption (push, concurrent, with manual ack), see `Consume` in the [Quick start](#quick-start) — and the [concurrency and ordering](#concurrency-and-message-ordering) section below.

### Publisher confirms in detail

`ConfirmSelect` puts the channel in confirm mode: from then on each `Publish` gets a *seq-no* (1, 2, 3, ...) and the broker confirms (`ack`) or rejects (`nack`) it. You can handle it asynchronously with `OnConfirm` and/or block until confirmation with `WaitForConfirm` (one publish) or `WaitForConfirms` (all pending):

```pascal
type
  TMyPublisher = class
    procedure WhenConfirmed(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
  end;

procedure TMyPublisher.WhenConfirmed(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
begin
  if not AAck then
    Log(Format('publish %d was NACKed by the broker', [ASeqNo]));
end;

// ...
Chan.ConfirmSelect;
Chan.OnConfirm := Publisher.WhenConfirmed; // asynchronous, on a pool thread

// Synchronous: blocks until the broker confirms this publish.
LSeq := Chan.Publish('', 'invoice.responses', LBody, LProps);
if Chan.WaitForConfirm(LSeq, 5000) then
  WriteLn('confirmed')
else
  WriteLn('not confirmed (nack, connection drop or timeout)');

// Or publish a batch and wait for all at once:
Chan.Publish('', 'invoice.responses', LBody1, LProps);
Chan.Publish('', 'invoice.responses', LBody2, LProps);
if not Chan.WaitForConfirms(5000) then
  WriteLn('some publish was not confirmed');
```

If the connection drops before the confirmation, the pending publish is reported as **not confirmed** (`WaitForConfirm` returns `False`); `OnConfirm` fires only for real broker confirmations. After a reconnection the seq-nos remain **monotonic** (they do not restart).

**Automatic republish (opt-in)**: with `RepublishUnconfirmedOnReconnect := True` in the connection parameters (together with `AutoReconnect`), publishes left unconfirmed by a drop are **automatically republished on reconnection**, with new seq-nos (observable via `OnConfirm`). It is *at-least-once* — duplicates can happen when the broker received the message but the `ack` was lost in the drop; the original seq-nos still report "not confirmed". Cost: the body of each pending publish is kept until confirmation.

### `Basic.Return` (unroutable `mandatory` publish)

To know whether a `mandatory` publish was not routed to any queue, handle `OnBasicReturn` (fires on a pool thread, like the consumer callback):

```pascal
procedure TMyPublisher.WhenUnroutable(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
begin
  Log(Format('unrouted message: %s (%d) exchange=%s rk=%s',
    [AReturned.ReplyText, AReturned.ReplyCode, AReturned.Exchange, AReturned.RoutingKey]));
end;

// ...
Chan.OnBasicReturn := Publisher.WhenUnroutable;
Chan.Publish('invoice', 'response.nonexistent', LBody, LProps, True {mandatory});
```

### Automatic reconnection

```pascal
LParams := TAMQPConnectionParams.Localhost;
LParams.AutoReconnect := True;
LParams.ReconnectDelayMs := 2000;   // backoff between attempts
LParams.MaxReconnectAttempts := 0;  // 0 = unlimited

Conn := TAMQPConnection.Create(LParams);
Conn.OnReconnect := MyApp.WhenReconnected; // fires after reconnecting AND restoring the topology
Conn.Open;
```

On a drop, the library reconnects and **restores the topology** declared on that channel (queues, exchanges, bindings, Qos, confirm mode) and **re-registers the consumers** — your callback receives messages again with no intervention. Since *delivery-tags* restart on each session, unconfirmed messages are redelivered: design your handlers to be **idempotent** (at-least-once). For tests/synchronization, wait for `OnReconnect` (which fires after the full recovery), not `IsOpen` (it turns `True` before the topology replay).

### The consumer callback must always finish on its own

The channel's `Close`/`Destroy` wait (without timeout, on purpose) for in-flight callbacks to finish before releasing the object — slow I/O is fine, but a callback that blocks indefinitely waiting for user interaction, or for an event only another application thread signals, stalls that shutdown; if the `Free` runs on the main thread of a VCL/LCL app, the UI freezes with it (deadlock). If the flow depends on human approval, prefer **not blocking**: store the *delivery-tag* and the content in your own structure, return, and confirm later (`Ack`/`Nack` can be called from any thread). If you choose to block on a `TEvent`, the shutdown must wake **all** the waits and also cover deliveries arriving *during* the disconnection — a nack+requeue can be redelivered immediately to the same consumer until `Cancel` completes (`samples/RetaguardaVcl` shows the pattern with a shutdown flag).

## Architecture (summary)

- **One read thread** is the only one reading the socket after the handshake; it demultiplexes frames per channel and **dispatches consumer callbacks to the thread pool** (`AmqpPool`, from `AMQP.Threading`) — it never runs user code and never blocks.
- All **writes** are serialized by a lock; the frames of one message (method + header + body) go out together.
- **RPC** (declare/bind/get/consume/close) is event-based: send and wait for the read thread to deliver the response.
- **Heartbeat** and **reconnection** run on their own threads with interruptible waits (`TEvent`).

## Errors and exceptions

All descend directly from `Exception` (no common base class). `EAMQPConnection`/`EAMQPChannel` (`AMQP.Connection`) are the ones an application normally handles; the other four belong to internal layers and in practice usually only surface during `Open` (wrapped or not).

| Exception | Raised when |
|---|---|
| `EAMQPConnection` | Handshake/`Open` failure (broker refusal, response on an unexpected channel); `Publish`/`CreateChannel` called with the connection closed or mid-reconnect. |
| `EAMQPChannel` | A channel RPC (declare/bind/get/consume/close) got `Channel.Close` from the broker, timed out, or was called with the channel already closed; API misuse (e.g. `WaitForConfirm` outside confirm mode). |
| `EAMQPTransport` | Plain socket error (unreachable host/port, dropped connection) outside the TLS handshake. |
| `EAMQPTls` | TLS handshake failure, rejected certificate (`TlsVerifyPeer=True`), `UseTls=True` with no TLS backend compiled, or `libssl`/`libcrypto` not found at runtime. See [TLS (amqps)](#tls-amqps). |
| `EAMQPFrame` | Malformed frame, or the connection closed mid-frame — usually a symptom of a dropped connection, not an application bug. |
| `EAMQPWire` | Encode/decode outside protocol limits (shortstr > 255 bytes, unsupported `TValue` kind in a `TAMQPFieldTable`) — generally points to an application error building `Arguments`/`Headers`. |

Practical notes:

- **`Open`** is where you most expect to catch a synchronous exception: `EAMQPConnection`, and — if `UseTls=True` — `EAMQPTransport`/`EAMQPTls`.
- **Channel calls** (`DeclareQueue`, `BindQueue`, `Publish`, `Consume`, etc.) can raise `EAMQPChannel` synchronously when the broker refuses the request or the channel already dropped.
- **After a successful `Open`, a connection drop does not turn into an exception on the application thread** — it is reported through the `OnDisconnect`/`OnReconnect`/`OnReconnectFailed` callbacks (see [Automatic reconnection](#automatic-reconnection)); without `AutoReconnect`, the next RPC call on that channel fails with `EAMQPChannel` ("canal fechado").
- **A publisher-confirm failure is not an exception** — it's `WaitForConfirm`/`WaitForConfirms` returning `False`, or `AAck=False` in `OnConfirm` (see [Publisher confirms in detail](#publisher-confirms-in-detail)).

## Concurrency and message ordering

Each delivery is dispatched to the **thread pool** (`AmqpPool`, see `AMQP.Threading`) as an independent work item — there is no single per-channel/consumer queue being drained in order. It is a deliberate design choice, different from the common pattern in other languages:

- **RabbitMQ Java/.NET client**: each channel is processed sequentially by a single worker taken from a shared pool — different channels run in parallel with each other, but within a channel order is preserved by default.
- **pika (Python) / node-amqplib**: single-threaded, event-loop oriented — all callbacks run on the same thread/loop; parallelism is opt-in, on the user's side.
- **amqp091-go**: exposes a Go channel with the deliveries; the consumer decides whether to process serially or spawn goroutines.

Here the default is the opposite: **parallelism by default, ordering by opt-in**. The real throughput gain shows up when the callback does something blocking (network I/O, database, disk) — several messages are processed at the same time instead of one waiting for the other to finish. If the callback is light/CPU-bound, the gain is marginal (the hand-off to the pool costs more than processing inline). Either way, the side effect is the same: **two deliveries of the same consumer can finish processing out of order.**

### How to get ordering when it matters

**1. `Qos(1)` + ack only at the end of processing** — the simplest option, uses only the existing public API. The broker won't deliver the next message of that consumer while the previous one is unconfirmed:

```pascal
Chan.Qos(1);
Chan.Consume('my-queue', Consumer.OnMsg);
// inside OnMsg: only call Ack after finishing the processing
```

Serializes **everything** for that consumer — no parallelism at all. A good choice when strict ordering is mandatory and volume is not the bottleneck.

**2. Channel with a dedicated worker (`CreateChannel(True)`)** — native library option: instead of the global `AmqpPool`, the channel gets its own thread (`TAMQPThreadPool.Create(1)` internally) that processes deliveries/returns/confirms one at a time, in arrival order. Unlike `Qos(1)`, the broker keeps delivering up to the configured prefetch — only client-side processing is serialized, not the network flow. Zero application code; other channels on the same connection stay concurrent as usual. See `samples/Retaguarda` (`--dedicado` flag) and `samples/RetaguardaVcl` (the "Thread dedicada" checkbox).

**3. Application-owned queue + one dedicated thread** — the callback only enqueues the delivery into a thread-safe queue (e.g. `TCriticalSection` + `TQueue`); a single dedicated thread drains and processes in arrival order, calling `Ack` at the end of each item. Only worth it over option 2 when you need something the library's dedicated worker doesn't offer — e.g. your own backpressure (bounding the queue size) or sharing a single processing queue across more than one channel/consumer.

**4. Sharding by key** — a variation of option 3 with N queues of one thread each, chosen by hashing some domain key (e.g. order id, customer id). Ordering guaranteed *per key*, parallelism between different keys — a good middle ground when only "per-entity ordering" matters, not global ordering.

Option 2 is built-in (`CreateChannel(ADedicatedConsumerThread: Boolean)`); options 3 and 4 are implemented entirely in the application, with the existing public API (`Consume`/`Ack`/`Qos`/`TAMQPDelivery`) — they require no change in the library. Two gotchas to watch on options 3 and 4:

- **`ADelivery.Properties.Headers` is freed by the library as soon as the callback returns** (the caller owns it, but the pool frees it automatically after `OnMsg` returns). If the callback only enqueues the delivery and returns right away, `Headers` will already be invalid when the dedicated thread processes it later — extract whatever you need from the headers **before** returning from the callback; do not keep the `TAMQPFieldTable` reference for later use.
- **`Channel.Close`/`Free` doesn't know about your own queue.** Since the callback returns as soon as it enqueues, the library's internal "in-flight" counter already counts that message as done. When shutting the application down, it is the responsibility of whoever implemented the own-queue to wait for it to drain before closing the channel — otherwise messages already taken from the library but not yet processed/acked can be lost.

Why start from the parallel side instead of the serialized one: adding ordering on top of a *concurrency-first* library is additive and contained (the options above). The opposite path — adding parallelism to a library that serializes by default — usually requires rebuilding in the application what already comes ready here: guaranteeing that channel operations are safe across multiple threads (serialized-by-default libraries usually don't guarantee that, forcing one channel per worker), your own backpressure, and safe draining on shutdown.

## Building

**Lazarus**: open/install `packages/pascal_amqp_faa.lpk` (or `lazbuild packages\pascal_amqp_faa.lpk`).

**Plain FPC**:

```
fpc -Fusrc -Fisrc your_program.pas
```

**Delphi**: add `src\` to the project search path (unit scope names `System;Winapi`, which is the default). Ready-made example in `samples\SmokeTest\SmokeTest.dproj`.

## TLS (amqps)

Two backends behind the **same API** — the application code doesn't change, only the build:

| Backend | Platforms | How to enable | Runtime dependency |
|---|---|---|---|
| **SChannel** (`AMQP.Transport.Tls`) | Windows (FPC and Delphi) | No step — automatic | None (Windows' own `secur32.dll`) |
| **OpenSSL** (`AMQP.Transport.OpenSSL`) | Any (validated on Linux x86_64) | `AMQP_OPENSSL` define (opt-in) | `libssl`/`libcrypto` **3.x** or **1.1.1** installed |

### Using

```pascal
var
  Params: TAMQPConnectionParams;
begin
  Params := TAMQPConnectionParams.Localhost;
  Params.Port := 5671;
  Params.UseTls := True;
  Params.TlsVerifyPeer := True; // default: validates chain + hostname
  Params.TlsServerName := '';   // '' => uses Host (SNI / validated name)
  Conn := TAMQPConnection.Create(Params);
```

For a dev broker with a self-signed certificate, `TAMQPConnectionParams.LocalhostTls` comes ready (port 5671, `TlsVerifyPeer=False`).

### Enabling the OpenSSL backend (`AMQP_OPENSSL`)

The define is **opt-in** on purpose: SChannel is guaranteed to exist on Windows, but OpenSSL depends on `libssl` being present on the machine — so it is never enabled automatically. With the define set, OpenSSL is used on **any** platform (including Windows, replacing SChannel). Without it, nothing changes from the current behavior: outside Windows, `UseTls` raises an exception explaining how to enable it.

- **FPC command line**: add `-dAMQP_OPENSSL`:

  ```
  fpc -dAMQP_OPENSSL -Fusrc -Fisrc your_program.pas
  ```

- **Lazarus**: `SmokeTest.lpi` and the two FPCUnit runners ship with an **`openssl` build mode** — pick it in the IDE's build-mode dropdown, or on the command line: `lazbuild -B --build-mode=openssl samples\SmokeTest\SmokeTest.lpi`. For other projects, remember the library builds through the package and project defines do **not** recompile package units — the define has to reach it via `Project Options → Compiler Options → Additions and Overrides` (Custom Option `-dAMQP_OPENSSL`, which applies to the project's packages) or directly in `packages/pascal_amqp_faa.lpk` (`Options → Custom Options`, then applying to every project using the package).
- **Delphi (IDE)**: `SmokeTest.dproj` and the two test projects ship with an **`OpenSSL`** *build configuration* (child of Debug) — just activate it in the Project Manager. For other projects: `Project → Options → Building → Delphi Compiler → Conditional defines` → add `AMQP_OPENSSL`. *(Pending validation on a Linux target: the Community Edition doesn't have it — the same source is the one validated by FPC/Linux.)*

The libraries are loaded **dynamically on the first TLS connection** (`dlopen`/`LoadLibrary` + name list: `libssl.so.3` → `libssl.so.1.1` → `libssl.so` on Linux; `libssl-3-x64.dll` etc. on Windows). The executable gains no link-time dependency: if `libssl` is not installed, the error only happens when calling `Open` with `UseTls=True` — with a message listing which names were tried.

To know **which engine an executable is using** (useful in UI/log — the VCL/LCL samples show it in the caption and status): `AmqpTlsBackendName` (unit `AMQP.Transport`) returns the build's backend (`OpenSSL`/`SChannel`/`nenhum` [none]); `AmqpTlsBackendInfo` adds, after the first TLS connection with OpenSSL, the version and library actually loaded (e.g. `OpenSSL 3.5.2 ... (libssl-3.dll)`). The SmokeTest `--tls` prints it in step 1.

Scope (same on both backends): server authentication with TLS 1.2+; validation via the system trust store; no mTLS/client-cert and no server-initiated renegotiation.

### Dev broker with TLS

The `docker/docker-compose.tls.yml` overlay adds the 5671 listener to the main compose broker — the file header has the step-by-step to generate the self-signed certificate (including the mandatory `chmod 644` on the key):

```
cd docker
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
```

## Smoke test (integration)

Start RabbitMQ (`docker compose -f docker/docker-compose.yml up -d`) and:

```
cd samples\SmokeTest
fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
SmokeTest.exe
```

It exercises handshake, topology, confirms, `Basic.Get`, concurrent consume with ack and automatic reconnection with recovery. Exits with code 0 on success.

With the `--tls` argument, it runs the same steps over TLS (needs the broker with the `docker-compose.tls.yml` overlay, see the TLS section above):

```
SmokeTest.exe --tls
```

On Windows that uses SChannel; to exercise the OpenSSL backend, build with `-dAMQP_OPENSSL` — `SmokeTest.lpi` ships the `openssl` build mode ready for that (on Linux, OpenSSL is the only TLS option).

## Samples

The flow that motivated the library: the authorizer publishes the response of an NFe (Brazilian electronic invoice) to a queue; the **back office** consumes that queue and answers the polling of **several simultaneous POS terminals**. Thread-pool consumption + manual ack serves that directly — each response is processed in parallel, correlated by the invoice key (`CorrelationId` or a header), and only confirmed after processing. The samples implement this flow (POS → authorizer → back office), each pair compiling from the same source on both compilers:

- **`samples/AutorizadorSim`** / **`samples/Retaguarda`** (console) — the authorizer publishes N simulated invoice responses; the back office consumes concurrently, with manual ack and a status command. Run `Retaguarda` and then `AutorizadorSim`: the `[worker N] iniciando...` lines of different invoices show up interleaved, confirming parallel processing. With `Retaguarda --dedicado`, the channel uses its own worker (see [option 2](#how-to-get-ordering-when-it-matters)) — the same lines show up in order, one invoice at a time.
- **`samples/AutorizadorSimVcl`** / **`samples/RetaguardaVcl`** (GUI — VCL on Delphi, LCL on Lazarus, from the same `.pas`/`.dfm`/`.lfm`) — same idea with a window: editable connection, publish on demand, and in `RetaguardaVcl` a manual confirmation mode (approve/reject an invoice from the list) besides the automatic one, and a "Thread dedicada" checkbox to switch between the two dispatch modes.
- **`samples/ConsultaStatusVcl`** (GUI, same dual scheme) — **RPC request/reply**, the classic pattern with `ReplyTo` + `CorrelationId`: the client publishes an invoice status query with `ReplyTo` pointing to the instance's exclusive/auto-delete reply queue and `Expiration` = timeout (a request that grows stale in the queue is dropped by the broker); the server consumes the request queue, simulates the lookup and replies echoing the correlation id. One window plays both roles (separate channels on the same connection) — run two instances for the truly distributed scenario. Client-side timeout via a timer, and a reply arriving **after** the timeout is discarded with a log notice. The reply queue uses a fixed per-instance name instead of a broker-generated one: the reconnection topology replay would re-send the declare and the broker would generate a new name, orphaning the recorded consumer. A **"Usar Direct Reply-to"** checkbox switches the client to the `amq.rabbitmq.reply-to` pseudo-address — the pattern [this README recommends](#recovery-of-server-named-queues) for RPC: with no queue declared at all, the client just consumes the pseudo-queue in no-ack mode (on the same channel as the publish) and the broker routes the reply straight back over a fast path; the server side is identical. Shows both patterns side by side.
- **`samples/EventosTopicVcl`** (GUI, same dual scheme) — **pub/sub with a topic exchange**: dynamic subscribers, each with its own exclusive/auto-delete queue bound to the exchange by a binding key (`*` matches one word, `#` matches zero or more) — the same event reaches **every** matching subscriber, unlike the work queues of the other samples (one message, one consumer). Publishing uses `mandatory` + `OnBasicReturn`: an event matching no subscriber is returned by the broker and shows up in the log instead of vanishing silently. Removing a subscriber undoes things in order — `Cancel` → `UnbindQueue` → `DeleteQueue` — reconciling the recovery topology (without that, a reconnection would replay a bind to a nonexistent queue). The only sample exercising `DeclareExchange`/`BindQueue`/`UnbindQueue`.
- **`samples/RetryDlqVcl`** (GUI, same dual scheme) — **dead-letter + retry with backoff + DLQ**, the reprocessing pattern built with queue arguments alone (`TAMQPFieldTable`): the work queue has a DLX pointing to a consumer-less wait queue (`x-message-ttl` = backoff, DLX back), so a `Nack` without requeue "schedules" the retry inside the broker itself; the consumer reads the current attempt from the `x-death` header and, once the configured maximum is exhausted, moves the message to the DLQ (the consumer decides — the broker does not count attempts on its own). The backoff uses **queue** TTL (per-message TTL only expires at the head of the queue). Connecting recreates the topology from scratch via `DeleteQueue` — redeclaring an existing queue with different arguments is `PRECONDITION_FAILED`.
- **`samples/PublicadorConfiavelVcl`** (GUI, same dual scheme) — a showcase of **publisher confirms**: the channel enters confirm mode on connect and every publish becomes a row in the list, resolved live by `OnConfirm` (ack/nack by seq-no). The batch is published with a configurable interval by its own thread, on purpose: stop the broker mid-batch (`docker stop` on the RabbitMQ container) and watch the whole story — send failure, automatic reconnection, topology replay and, with "Reenviar não confirmadas na reconexão" checked (`RepublishUnconfirmedOnReconnect`), the automatic republish of whatever was left pending (under new seq-nos, counted separately). It also has a `mandatory` publish button targeting a nonexistent route (the broker **returns** it via `Basic.Return` and still **confirms** it — a confirm means "I took responsibility", not "I routed it") and a `WaitForConfirms` at the end of each batch, with the outcome in the log.

Start the broker (`docker compose -f docker/docker-compose.yml up -d`) and open the corresponding `.dproj`/`.lpi` — or `AMQP.groupproj`/`AMQP.lpg` to open them all together.

## Tests

- **Delphi (DUnitX)**: open `AMQP.groupproj` in RAD Studio.
- **FPC/Lazarus (FPCUnit)**: open `AMQP.lpg` (Project Group — requires the optional `LazProjectGroups` package installed in the IDE) or each `.lpi` individually.

In both: `AMQP.UnitTests` / `AMQPUnitTestsFpc` (80 tests, no broker needed — frame/method/content-header encode/decode, tune negotiation) and `AMQP.IntegrationTests` / `AMQPIntegrationTestsFpc` (28 tests, needs RabbitMQ up: `docker compose -f docker/docker-compose.yml up -d`, TLS included via `docker-compose.tls.yml`). The 5 TLS tests (publish/fetch, verify-peer, 300KB payload, concurrent consume and handshake against the plain port) self-ignore if the TLS broker is down — and, outside Windows, if the runner was not built with `-dAMQP_OPENSSL`. On FPCUnit they show up as truly ignored (`Number of ignored tests: 5` in the report); on DUnitX, which has no runtime *skip*, they still count as Passed, but the console log shows `Success. : IGNORADO: broker TLS (5671) indisponível...` on each — if the 5 appear without that message, they really connected.

The FPCUnit runner decides by itself via `ParamCount`: with no arguments it opens the GUI (test tree + green/red bar); with arguments (`--all --format=plain`) it runs in console mode. Running from the Lazarus IDE, the switch is in `Run → Run Parameters → Command line parameters` — the `.lpi` files come with `--all --format=plain` saved (console mode); clear the field to make F9 open the GUI. In console mode via F9 the window closes when done: either check *Use launching application* with `C:\Windows\System32\cmd.exe /K $(TargetCmdLine)`, or run the executable directly in a terminal.

## Known limitations

- Transactions (`tx.*`) **not implemented — by design decision**, not technical difficulty (the `tx.*` protocol is trivial). An AMQP transaction is *stateful and per-channel* ("everything I published/acked since the last commit"), which fits poorly with this library's *concurrency-first* model: several threads publishing on the same channel would all fall into the same transaction, with no per-thread scope. Besides, `tx.*` is synchronous and slow, and RabbitMQ itself recommends **publisher confirms** instead — which are already implemented (`ConfirmSelect`). If a real need for atomic batches arises, the tractable subset is `Tx.Select/Commit/Rollback` for serial use on a dedicated channel.
- Publisher confirms + reconnection: publishes unconfirmed before the drop are reported as **not confirmed**; republishing on reconnection is **opt-in** (`RepublishUnconfirmedOnReconnect`, at-least-once) — without it, resend at your layer if you need an end-to-end guarantee. See [Publisher confirms in detail](#publisher-confirms-in-detail).
- **Topology recovery with server-named queues** — see the section below.
- TLS: server authentication only — no mTLS/client-cert, no manual version/cipher-suite selection (see [TLS (amqps)](#tls-amqps)).

### Recovery of server-named queues

When declaring `QueueName = ''`, the server **generates** the name (`amq.gen-XXXX`). Topology recovery on reconnection **assumes named queues**: it stores the already-serialized `declare`/`bind`/`consume` payloads and re-executes them. Since redeclaring with an empty name generates a **new** name and the recorded `bind`/`consume` carry the **old** one, the replay breaks (`basic.consume` on a nonexistent queue → `404` → the server closes the channel).

**Workaround (recommended):** if you need a temporary queue **and** reconnection, **generate the name on the client** instead of using `''`:

```pascal
var
  LGuid: TGUID;
begin
  CreateGUID(LGuid);
  LDecl.QueueName := 'reply-' + GUIDToString(LGuid);
  LDecl.Exclusive := True;   // goes away when the connection closes
  LDecl.AutoDelete := True;
end;
```

That way the queue is temporary with a **stable, known name**, and recovery works like any named queue's. For request/reply RPC, prefer **Direct Reply-to** (`amq.rabbitmq.reply-to`) — a fixed magic name, no queue declared per request, which also doesn't suffer from this problem. (For anyone forking and needing real server-named recovery, the detailed path is documented in the [delphi-amqp-faa](https://github.com/fabianoallex/delphi-amqp-faa#recupera%C3%A7%C3%A3o-de-filas-com-nome-gerado-pelo-servidor) README — the design is the same.)

## Roadmap

- ~~Linux validation~~ — done: x86_64 and ARM64, TLS/OpenSSL included, GUI samples validated on LCL/GTK2 (see the compatibility table).
- Validation of the OpenSSL backend compiled by Delphi on Linux (the Community Edition doesn't have the target; the same source is the one validated by FPC/Linux).
- mTLS/client-cert.

## License

MIT — see [LICENSE](LICENSE).
