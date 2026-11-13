# Fuel Station — architecture

A sample that serves as the base for a real application: a **fuel-pump
automation server** with the embedded AMQP broker, and **POS terminals**
on the store network that consume the fuel deliveries to put them into sales.

This document is the canonical reference for the design. The history of the
conversation that produced it is not reloaded — what counts is what is here.

---

## 1. Goal

- The automation receives fuel deliveries from the pumps (here: a timer-driven simulator).
- Every completed fuel delivery becomes **visible to all POS terminals** on the network,
  almost in real time.
- When the customer reaches a POS, the fuel delivery **is already there**.
- From the moment a POS includes the fuel delivery in a sale, **no other POS**
  can use it. A fuel delivery belongs to **exactly one sale**.
- If the item is removed from the sale, or the sale is cancelled, the fuel
  delivery **becomes available again** to the others.
- A POS that starts later receives the fuel deliveries **still pending**.

### Non-goals of v1

- Persistence of the fuel-delivery register (memory only; a server restart
  loses what was pending). It comes later as an isolated increment.
- Automatic release of a reservation when a POS drops (see §10, R1/R2).
- Multi-station / more than one broker. One broker per store.
- Real fiscal work (issuing NF-e/SAT). "Finalize sale" here is only the fuel
  delivery's state transition.
- Aggregation / ready-made metrics in the panel — raw numbers, not histograms.

---

## 2. Principle: who owns the state

This library's broker is **transport, not a database**: in-memory queues, no
consumer state, no management API. Therefore:

> **The authoritative state of the fuel deliveries lives in the automation
> server**, which already owns the state of the pumps. The broker carries two
> things: **events** (server → POS terminals, one-to-many) and **commands** (POS → server,
> request/reply).

The rule "one fuel delivery in exactly one sale" is a distributed lock with **a
single arbiter** — the server. AMQP doesn't give you that by itself; the server
does, by processing commands **serially per fuel-delivery id**.

This also lines up with the philosophy of the broker's Phase 4 (D23): **a
duplicate is the acceptable, reconcilable failure mode; losing a fuel delivery
is not.** The server never drops a fuel delivery on its own.

---

## 3. Roles

### Server (one process)

Automation + embedded broker + authoritative register + GUI panel. It brings up
the `TAMQPServer` on a TCP port of the store network; the POS terminals connect to it.

### POS (one process per checkout)

A front-of-store checkout GUI app (this sample's slice: only the fuel-delivery
part). It has **its own local database** and **keeps working even cut off from
the network** (offline operation): it adds items and closes sales offline, and
syncs when the network comes back.

---

## 4. Fuel-delivery states

```pascal
TFuelingState = (fsAvailable, fsPosting, fsPosted);
```

| State | Meaning | Blocks other POS terminals? |
|---|---|---|
| `fsAvailable` | free; any POS can add it | no |
| `fsPosting` | item included in a sale **in progress** on a POS | **yes** |
| `fsPosted` | sale **finalized** with the fuel delivery | **yes** |

`fsPosting` and `fsPosted` keep `pos`, `sale` and the instant of the transition.
**The sale in progress already locks the fuel delivery** — there is no "reserve
the whole sale"; each item added locks its fuel delivery right away.

### Transitions

Every transition increments the global `version` (the server's monotonic
counter) and publishes an event.

| From | To | Trigger | Command |
|---|---|---|---|
| — | `fsAvailable` | pump finished fueling | internal (simulator) |
| `fsAvailable` | `fsPosting` | operator includes the fuel delivery in the sale | `post` |
| `fsPosting` | `fsAvailable` | removes the item / cancels the sale in progress | `unpost` |
| `fsPosting` | `fsPosted` | operator finalizes the sale | `finish` |
| `fsPosted` | `fsAvailable` | cancellation of an already-finalized sale/invoice | `cancel_finished` |
| blocked | `fsAvailable` | supervisor forces the release | `force_release` |
| `fsAvailable` | (removed) | supervisor discards (calibration/pump test) | `discard` |

---

## 5. AMQP topology

| Object | Type | Use |
|---|---|---|
| `station.events` | `topic` exchange | events server → all POS terminals |
| queue per POS | exclusive + auto-delete, bind `fueling.#` | pub/sub; every POS sees every event |
| `station.commands` | queue (non-durable) | RPC request queue; consumed by the server |
| RPC reply | exclusive queue per POS (`reply-to` + `correlation-id`) | the server's reply to the POS |

- **vhost**: `/` (the broker's default).
- **payload**: JSON in the *body*, `content-type = application/json`. (A minimal
  codec of its own in `common/FuelStation.Json.pas` — `fpjson` and `System.JSON` differ
  between the compilers, and the `snapshot` carries an array of objects, which
  in a `TAMQPFieldTable` would fall into FPC's `TValue`-in-`TValue`.)

---

## 6. Message contract

### 6.1 POS identity

The POS authenticates with the broker with **its own user** `pos-<checkout>`
(e.g. `pos-03`), with a shared or per-checkout password (server config via
`TAMQPStaticAuthenticator`). That gives **two things for free**:

1. per-checkout authentication;
2. the `Username` arrives in the broker's connection events (`seConnectionAuthenticated`
   / `seConnectionClosed`), so the **Watcher** (§11) maps connection → POS without
   any extra handshake.

The server uses the `automation` user for its own publishing connection.

### 6.2 Commands — RPC via `station.commands`

Request: `reply-to` = the POS terminal's exclusive queue, `correlation-id` = GUID,
`type` = command name, body = JSON. Reply: a JSON body with the same
`correlation-id`.

```
snapshot { "cmd":"snapshot" }
  → { "version": N,
      "available": [ Fueling, ... ],
      "locked":    [ { "id":"…", "state":"posting|posted",
                       "pos":"pos-03", "sale":"…", "since": epochMs }, ... ] }

post { "cmd":"post", "id":"…", "pos":"pos-03", "sale":"S-20261113-101500-0001" }   // ONE AT A TIME
  → { "ok": true,  "version": N }
  | { "ok": false, "reason": "ALREADY_POSTING|ALREADY_POSTED|NOT_FOUND|DISCARDED",
      "currentPos":"pos-05", "currentSale":"…" }

unpost { "cmd":"unpost", "id":"…", "pos":"pos-03", "sale":"S-20261113-101500-0001" }
  → { "ok": true, "version": N }          // idempotent; validates the owner (pos+sale)
  | { "ok": false, "reason": "NOT_OWNER|NOT_FOUND" }

finish { "cmd":"finish", "pos":"pos-03", "sale":"S-20261113-101500-0001", "ids":["…","…"] }   // BATCH
  → { "ok": true, "version": N,
      "items": [ { "id":"…", "ok": true }, { "id":"…", "ok": false,
                   "reason":"NOT_OWNER" } ] }

cancel_finished { "cmd":"cancel_finished", "pos":"pos-03", "sale":"…", "operator":"F. Arndt", "ids":[…] }
  → { "ok": true, "version": N, "items": [ … ] }

force_release { "cmd":"force_release", "id":"…", "operator":"Supervisor", "reason":"POS out of order" }
  → { "ok": true, "version": N, "previousState":"posting", "previousPos":"pos-07" }

discard { "cmd":"discard", "id":"…", "operator":"Supervisor", "reason":"pump 3 calibration" }
  → { "ok": true, "version": N }
```

A request the server cannot parse is refused with `"reason":"BAD_FORMAT"`, and an
unknown `cmd` with `"reason":"UNKNOWN_CMD"`.

- `post` is **one at a time**, at the moment the item is included — that is
  what makes the sale in progress block item by item.
- `finish` is **a batch** (the whole sale, at a single moment). The server
  applies each id and returns the result per item; an item that fails (e.g. the
  supervisor released it in the meantime) doesn't stop the others — the POS
  flags the conflict.

### 6.3 Events — exchange `station.events` (topic)

```
fueling.new        { "fueling": Fueling, "version": N }
fueling.posting    { "id":"…", "pos":"pos-03", "sale":"…", "version": N }
fueling.posted     { "id":"…", "pos":"pos-03", "sale":"…", "version": N }
fueling.available  { "id":"…", "reason":"ITEM_UNPOST|CANCEL_SALE|CANCEL_INVOICE|MANUAL_RELEASE",
                     "version": N }
fueling.discarded  { "id":"…", "version": N }
```

### 6.4 The `Fueling` object

```json
{
  "id": "P03-N2-0001234609",
  "pump": 3, "nozzle": 2,
  "product": "GC",
  "liters": 42.317,
  "pricePerLiter": 5.899,
  "totalAmount": 249.66,
  "startTotalizer": 1234567.11,
  "endTotalizer": 1234609.42,
  "timestampMs": 1757260330000,
  "kind": "SALE"
}
```

`kind`: `SALE` (normal) or `CALIBRATION` (pump test — the simulator emits one
now and then; the supervisor discards it from the panel).
`id`: stable and unique — `pump + nozzle + end totalizer`, formatted
`P%.2d-N%d-%.10d`. The dedup key
everywhere.

---

## 7. POS synchronization

The classic pub/sub problem: a POS that connects **later** has missed all the
earlier events. Solution — snapshot + replay with a version number:

1. The POS connects, declares and **binds** the event queue, starts
   **consuming but buffers** (doesn't apply to the screen yet).
2. Calls `snapshot` → receives `version` + lists.
3. Applies the snapshot as the baseline; then **reprocesses the buffer,
   discarding events with `version <= snapshotVersion`**.
4. From then on it applies live, keeping the last version applied.
5. **Version gap** (`version > lastVersion + 1`) → triggers a **re-snapshot**.
   This also heals an event lost for any reason (the POS terminal's queue filled up,
   etc.) — the mechanism is self-healing.

On **reconnection** (the library's `OnReconnect`, which fires after the topology
recovery): re-`snapshot` + reconciliation (§9).

---

## 8. Normal flow

```
Pump → Server: fueling finished
Server: records fsAvailable, version++, publishes fueling.new
  → every POS shows it in the pending list

The operator of POS-A includes the fuel delivery in the sale
POS-A → Server: post(id, pos-A, S-...-0001)
Server: fsAvailable → fsPosting, version++, replies ok
Server: publishes fueling.posting(id, pos-A)
  → the other POS terminals remove it from the list

  ── sale closed ──                       ── item removed / sale cancelled ──
  POS-A → Server: finish(...)             POS-A → Server: unpost(id, pos-A, S-...-0001)
  Server: fsPosting → fsPosted            Server: fsPosting → fsAvailable
  publishes fueling.posted                publishes fueling.available(ITEM_UNPOST)
                                            → the other POS terminals show it again
```

---

## 9. Offline operation

The POS may be **closing the sale offline** with the fuel delivery it reserved.
That is why the server **cannot release a reservation on its own** — releasing
automatically would risk a double sale.

### The POS terminal's outbox

`post` / `unpost` / `finish` done offline (or whose RPC timed out)
go into a **local queue persisted in the POS terminal's database**. On reconnection, the
POS **replays the outbox in order** and handles each result.

- Online, `post` is synchronous: `ok` → the item goes into the sale;
  `ALREADY_POSTING` → "in use at POS pos-N", the item does
  **not** go in.
- Offline, `post` is **optimistic**: the operator includes the fuel delivery
  (which is already in the local list, frozen from before the drop), the POS
  marks it locally and enqueues the `post` in the outbox. The screen shows a
  "syncing" marker on the item.

### Reconciliation on reconnection

For each fuel delivery the POS believes it reserved in an **open** sale, it
checks the snapshot:

- the server shows `fsPosting` by this POS → ok, carry on.
- the server shows `fsAvailable` → re-send `post`.
- the server shows `fsPosting`/`fsPosted` by **another** POS → **conflict**:
  the offline sale already went out; the item is marked "fuel delivery sold at
  another checkout — check with the supervisor". The server records the conflict
  in the panel log.

### The conflict the system does not prevent, only detects

POS-A and POS-B both offline, both with fuel delivery X in their frozen list,
both operators add X and print. On reconnection the first `post` wins; the
second gets `ALREADY_POSTING` → supervisor reconciliation (void/adjustment).
Inherent to offline operation. The system's role is to make this **glaring**,
not to pretend it doesn't happen.

---

## 10. Failure rules

| # | Rule |
|---|---|
| R1 | **A reservation does not expire.** It only leaves `fsPosting`/`fsPosted` through `unpost`, `finish`, `cancel_finished` or a manual release. A POS disconnecting releases **nothing**. |
| R2 | **Releasing a disconnected POS terminal's reservation is a deliberate supervisor action** (`force_release` from the panel). Logged; emits `fueling.available` with `reason=MANUAL_RELEASE`. |
| R3 | **The Watcher (observability) feeds the panel, it doesn't act.** It shows "pos-03 DISCONNECTED" and for how long so the supervisor can decide whether it was a network drop or a broken checkout. |
| R4 | **The POS has a local outbox.** Offline operations are enqueued and replayed on reconnection, in order. |
| R5 | **A conflict is detected, logged and flagged — never resolved silently.** |
| R6 | **The server never drops a fuel delivery by itself.** No TTL, no `x-max-length`. Only the supervisor discards (calibration). |

---

## 11. Server panel and Watcher

### Panel (dual VCL/LCL GUI)

- Fuel-delivery grid: id, product, liters, value, **state**, **owning POS**,
  **sale**, time in the state.
- POS list: user, **connected / disconnected since HH:MM**.
- Buttons: **force release** (selected fuel delivery), **discard**.
- Log of conflicts and supervisor actions.

### Watcher — `FuelStation.Server.Watcher`

Subscribes to `TAMQPServer.Subscribe` with
`[seConnectionAuthenticated, seConnectionClosed]`. It keeps a table
`Username → TPresenceInfo { Connected: Boolean; SinceMs: Int64; ... }` that the panel reads.
**No automatic action** on reservations (R3). The handler runs on the broker's
notifier thread — it only updates the table under a lock and marks the UI dirty.

---

## 12. Persistence

**v1: memory only.** `FuelStation.Server.Registry` is an in-memory dictionary. A
server restart loses the pending fuel deliveries; the POS terminals, on reconnecting,
redo `snapshot` and adjust (fuel deliveries that disappeared while in an open
sale become conflicts to resolve).

Next increment (outside v1): the server persists its own register in a file of
its own (JSON or a simple WAL), **separate** from the broker. The broker's
`DataDir` is an AMQP guarantee (events/commands are not lost in transport),
nearly orthogonal to this.

---

## 13. File layout

```
samples/FuelStation/
  ARCHITECTURE.md                      (this file)
  README.md                            build + walkthrough
  common/
    FuelStation.Json.pas                     DOM JSON codec (writer + recursive parser)
    FuelStation.Fueling.pas               TFueling, TFuelingState, TFuelingType, REF_*/RSN_*
    FuelStation.Contracts.pas                topology/command/event names + TFueling<->Json + encoders
  server/
    FuelStation.Server.Registry.pas        authoritative state + state machine + version (single lock)
    FuelStation.Server.Auth.pas            SASL PLAIN: automation + pos-* (no pre-registration)
    FuelStation.Server.Events.pas         publishing to station.events (channel + lock)
    FuelStation.Server.Pumps.pas          pump simulator (thread + timer)
    FuelStation.Server.Commands.pas        consumer of station.commands; applies the transition; detects conflicts
    FuelStation.Server.Watcher.pas           observability sink → POS presence table
    FuelStation.Server.App.pas             UI-less assembly (broker+register+commands+Watcher+pumps)
    uServerMain.pas / .dfm / .lfm    dual GUI panel (a shell over TFuelStationServerApp)
    FuelStationServer.dpr / .lpi / .dproj
  pos/
    FuelStation.POS.Client.pas              connection + synchronous RPC (exclusive reply queue)
    FuelStation.POS.Sync.pas            buffer + snapshot + replay + version-gap detection
    FuelStation.POS.Model.pas               sale + items + outbox + reconciliation (ops on PcPool workers)
    uPosMain.pas / .dfm / .lfm         dual GUI checkout front end
    FuelStationPos.dpr / .lpi / .dproj
  smoke/
    FuelStationSmoke.dpr / .lpi / .dproj    dual console smoke test (in-process broker + 2 POS terminals, exercises the whole path)
```

Conventions: a single source for both compilers; `{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}`
at the top of the units (the GUI samples don't use `{$I amqp.inc}`); `of object`
callbacks (no closures). New units in UTF-8 **with BOM**.

**UI built in code** (`BuildUI`), not in the designer — the `.dfm`/`.lfm` are
empty shells (only the form + `OnCreate`/`OnClose`). It is the most robust dual
path: one layout source, nothing to keep in sync between the two formats.

**No `TThread.Queue` in either screen**: threads (pump, command consumer, the
broker's notifier, connection callbacks) only touch state under a lock and
enqueue log lines / set an atomic flag; a `TTimer` on the UI thread drains the
log and rebuilds the lists. This avoids the gotcha of `TThread.Queue` discarded
by a thread that dies (CLAUDE.md) without needing the hop through `PcPool`.

---

## 14. Manual test walkthrough

1 server + 2 POS terminals (`pos-01`, `pos-02`) on the same machine, local ports.

1. The simulator generates a fuel delivery → **it shows up on both POS terminals**.
2. `post` on POS-01 → **it disappears from POS-02**.
3. `unpost` on POS-01 → **it comes back on both**.
4. `post` + `finish` on POS-01 → it disappears for good; POS-02 never sees it again.
5. Bring up **POS-03 now** → it receives the pending ones through `snapshot`.
6. POS-02 adds a fuel delivery and **drops** (closes the app) without finalizing →
   the server panel shows POS-02 disconnected; the fuel delivery **stays `fsPosting`**.
7. Supervisor in the panel: **force release** → the fuel delivery comes back on
   the others; the log records the action.
8. Offline operation: bring the network down (stop the server), POS-01 and
   POS-02 add the **same** fuel delivery offline and finalize; the network comes
   back → the first `post` from the outbox wins, the second becomes a
   **conflict** on the POS and in the panel log.

---

## 15. Backlog (post-v1)

- Persistence of the register on the server (a file of its own).
- An automated test suite for the sample (in-process host + two test clients).
- Event filter by pump/nozzle (`fueling.<pump>.<event>`), for a POS
  dedicated to one island.
- Panel: history and totals per pump/shift.
- Security: per-checkout password, `guest` out, rate-limit after an auth failure.
