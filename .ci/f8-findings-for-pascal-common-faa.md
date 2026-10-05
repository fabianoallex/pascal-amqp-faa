# F8 (pascal-amqp-faa) — findings for pascal-common-faa

Migration of pascal-amqp-faa to pascal-common-faa v1.0.1, done 2026-10-04. Nothing in
pascal-common-faa was changed; these are the points to bring back. Most important first.

## 1. `migrating.md`: a library whose pool work answers synchronous requests should not share `PcPool`

**What happened.** The amqp broker runs one actor per queue as work items on a pool (its
decision D2), and those actors answer **synchronous** commands: the connection thread posts
`Stats`/`Get`/`Purge` and waits (15 s deadline) to build `Queue.Declare-Ok`, `Basic.Get-Ok`...
Before F8 the actors ran on `AmqpPool`; a straight rename would put them on `PcPool`, which is
also where consumer callbacks run — amqp's own client, and pipes/redis in the same process —
and those callbacks may block on I/O for seconds. `PcPool` has a ceiling
(`max(16, 4 × cores)`), so blocking work can starve work that someone else is waiting on.

**Measured** (amqp test `TPoolIsolationTests.PcPoolSaturado_BrokerContinuaRespondendo`: queue
`4 × ProcessorCount + 16` items that block on an event, wait until `QueueDepth > 0`, then
declare/publish/get against the embedded broker):

- actors on `PcPool`: `Queue.Declare` waited the full 15 s, the actor command timed out and the
  broker dropped the connection;
- actors on a `TPcThreadPool` owned by the broker: 26 ms.

amqp now gives the broker its own pool (amqp decision D37); the client stays on `PcPool`.

**Suggestion** for "Behavior to know about": `PcPool` is for work that may block (user
callbacks). Work that other threads wait on synchronously — an actor, a dispatcher whose
items answer requests — belongs on its own `TPcThreadPool`, or one library's slow callbacks
become another library's timeouts. The ceiling makes this a correctness issue, not only a
latency one.

## 2. Possible additive API (1.x): `TPcThreadPool.MaxWorkers` (read-only)

To saturate `PcPool` deterministically, the amqp test had to re-derive the ceiling
(`4 × ProcessorCount + 16` is "at least the ceiling" for `max(16, 4 × cores)`), and detect
saturation as "every item started or still queued, and `QueueDepth > 0`". A read-only
`MaxWorkers` would let such a test queue exactly `MaxWorkers + 1` items, and let a consumer log
or assert the effective ceiling. Not required; nice to have.

## 3. Name map: amqp needs word boundaries (`TAMQPMonitorThread`)

amqp has `TAMQPMonitorThread` (the broker's monitor thread), a different type from
`TAMQPMonitor`. A recipe written as `s/TAMQPMonitor/TPcMonitor/` (no `\b`) would rename it to
`TPcMonitorThread`. The `perl -pi` example in `migrating.md` is for pascal-db-faa and already uses
`\b`; worth one line saying "always `\b`: amqp has `TAMQPMonitorThread`".

## 4. Destroy runs the whole queue — amqp's docs were half right

`AMQP.Server.Queue`'s header said "the finalization of AMQP.Threading frees queued items
WITHOUT running them". Wrong for items already queued (Destroy runs them, as F3 found), right
only for an item queued *after* Destroy started (freed without running). Fixed in amqp. The
second half is worth stating in `migrating.md` next to the first: it is the reason amqp's queue
`Stop` never posts a "stop" command to the pool.

## 5. Finalization: nothing to wait for in amqp (no action)

After the migration the only amqp finalization left frees `GLibLock` in
`AMQP.Transport.OpenSSL`, touched only when opening a TLS connection; no amqp work item opens
connections (reconnection is its own `TThread`). Client work items only touch channel/connection
objects the user frees, and the channel already waits for its in-flight items counting from
queue time. The broker's pool is freed in `TAMQPServer.Destroy`, after the engine stops every
actor. So the "wait for your own items" pattern from pipes was not needed here.

## 6. Debian's FPC heaptrc is silent at exit without `log=` (known; maybe a gotcha)

`tools/test_fpc_docker.sh` already says it in a comment. Measured again here: on Debian bookworm
FPC 3.2.2, a program built with `-gh` that leaks 10 bytes prints nothing at exit; an explicit
`DumpHeap` prints, and `HEAPTRC=log=<file>` writes the exit dump to the file. amqp's own docs
claimed the runners print `N unfreed memory blocks` "as the last line" everywhere, which made
earlier Linux leak checks unobserved. Since the acceptance criterion of every consumer is
"0 leaks on Linux", it may deserve a numbered gotcha in `docs/gotchas.md`, not only a script
comment.

## 7. Things that worked as documented (no action)

- `.lpk` requiring `pascal_common_faa` by name with `MinVersion Major="1"` (both amqp packages),
  and 21 test/sample `.lpi` listing it first with `DefaultFilename` into `external/` and
  `Prefer="True"`: lazbuild built everything against the submodule copy (checked in the build
  log: `-Fu...\external\pascal-common-faa\packages\lib\x86_64-win64`).
- Version check after the `uses` that brings in `PascalCommon.Version` (in `AMQP.Threading`, which
  `AMQP.Connection` and `AMQP.Server.Engine` keep in their `uses` for that reason): raising the
  minimum stops FPC 3.2.2 with `Fatal: User defined: pascal-amqp-faa precisa da
  pascal-common-faa 1.0.0 ou mais nova`.
- 64-bit counters: all 21 `PcAtomicRead64`/`PcAtomicWrite64` call sites target `UInt64` fields
  (ticks, LSN, journal bytes); the `var` parameter binds only the `UInt64` overload.
