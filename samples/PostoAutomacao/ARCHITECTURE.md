# Posto Automação — architecture

A sample that serves as the base for a real application: a **fuel-pump
automation server** with the embedded AMQP broker, and **POS terminals** (PDVs)
on the store network that consume the fuel deliveries to put them into sales.

This document is the canonical reference for the design. The history of the
conversation that produced it is not reloaded — what counts is what is here.

---

## 1. Goal

- The automation receives fuel deliveries from the pumps (here: a timer-driven simulator).
- Every completed fuel delivery becomes **visible to all PDVs** on the network,
  almost in real time.
- When the customer reaches a PDV, the fuel delivery **is already there**.
- From the moment a PDV includes the fuel delivery in a sale, **no other PDV**
  can use it. A fuel delivery belongs to **exactly one sale**.
- If the item is removed from the sale, or the sale is cancelled, the fuel
  delivery **becomes available again** to the others.
- A PDV that starts later receives the fuel deliveries **still pending**.

### Non-goals of v1

- Persistence of the fuel-delivery register (memory only; a server restart
  loses what was pending). It comes later as an isolated increment.
- Automatic release of a reservation when a PDV drops (see §10, R1/R2).
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
> things: **events** (server → PDVs, one-to-many) and **commands** (PDV → server,
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
the `TAMQPServer` on a TCP port of the store network; the PDVs connect to it.

### PDV (one process per checkout)

A front-of-store checkout GUI app (this sample's slice: only the fuel-delivery
part). It has **its own local database** and **keeps working even cut off from
the network** (offline operation): it adds items and closes sales offline, and
syncs when the network comes back.

---

## 4. Fuel-delivery states

```pascal
TEstadoAbastecida = (eaDisponivel, eaLancando, eaLancado);
```

| State | Meaning | Blocks other PDVs? |
|---|---|---|
| `eaDisponivel` | free; any PDV can add it | no |
| `eaLancando` | item included in a sale **in progress** on a PDV | **yes** |
| `eaLancado` | sale **finalized** with the fuel delivery | **yes** |

`eaLancando` and `eaLancado` keep `pdv`, `venda` and the instant of the transition.
**The sale in progress already locks the fuel delivery** — there is no "reserve
the whole sale"; each item added locks its fuel delivery right away.

### Transitions

Every transition increments the global `versao` (the server's monotonic
counter) and publishes an event.

| From | To | Trigger | Command |
|---|---|---|---|
| — | `eaDisponivel` | pump finished fueling | internal (simulator) |
| `eaDisponivel` | `eaLancando` | operator includes the fuel delivery in the sale | `lancar` |
| `eaLancando` | `eaDisponivel` | removes the item / cancels the sale in progress | `estornar` |
| `eaLancando` | `eaLancado` | operator finalizes the sale | `finalizar` |
| `eaLancado` | `eaDisponivel` | cancellation of an already-finalized sale/invoice | `cancelar_finalizada` |
| blocked | `eaDisponivel` | supervisor forces the release | `liberar_forcado` |
| `eaDisponivel` | (removed) | supervisor discards (calibration/pump test) | `descartar` |

---

## 5. AMQP topology

| Object | Type | Use |
|---|---|---|
| `posto.eventos` | `topic` exchange | events server → all PDVs |
| queue per PDV | exclusive + auto-delete, bind `abastecida.#` | pub/sub; every PDV sees every event |
| `posto.comandos` | queue (non-durable) | RPC request queue; consumed by the server |
| RPC reply | exclusive queue per PDV (`reply-to` + `correlation-id`) | the server's reply to the PDV |

- **vhost**: `/` (the broker's default).
- **payload**: JSON in the *body*, `content-type = application/json`. (A minimal
  codec of its own in `comum/Posto.Json.pas` — `fpjson` and `System.JSON` differ
  between the compilers, and the `snapshot` carries an array of objects, which
  in a `TAMQPFieldTable` would fall into FPC's `TValue`-in-`TValue`.)

---

## 6. Message contract

### 6.1 PDV identity

The PDV authenticates with the broker with **its own user** `pdv-<checkout>`
(e.g. `pdv-03`), with a shared or per-checkout password (server config via
`TAMQPStaticAuthenticator`). That gives **two things for free**:

1. per-checkout authentication;
2. the `Username` arrives in the broker's connection events (`seConnectionAuthenticated`
   / `seConnectionClosed`), so the **Vigia** (§11) maps connection → PDV without
   any extra handshake.

The server uses the `automacao` user for its own publishing connection.

### 6.2 Commands — RPC via `posto.comandos`

Request: `reply-to` = the PDV's exclusive queue, `correlation-id` = GUID,
`type` = command name, body = JSON. Reply: a JSON body with the same
`correlation-id`.

```
snapshot {}
  → { "versao": N,
      "disponiveis": [ Abastecida, ... ],
      "bloqueadas":  [ { "id":"…", "estado":"lancando|lancado",
                         "pdv":"pdv-03", "venda":"…", "desde": epochMs }, ... ] }

lancar { "id":"…", "pdv":"pdv-03", "venda":"V-2026-0001" }   // ONE AT A TIME
  → { "ok": true,  "versao": N }
  | { "ok": false, "motivo": "JA_LANCANDO|JA_LANCADO|NAO_EXISTE|DESCARTADA",
      "pdvAtual":"pdv-05", "vendaAtual":"…" }

estornar { "id":"…", "pdv":"pdv-03", "venda":"V-2026-0001" }
  → { "ok": true, "versao": N }          // idempotent; validates the owner (pdv+venda)
  | { "ok": false, "motivo": "NAO_E_DONO|NAO_EXISTE" }

finalizar { "pdv":"pdv-03", "venda":"V-2026-0001", "ids":["…","…"] }   // BATCH
  → { "ok": true, "versao": N,
      "itens": [ { "id":"…", "ok": true }, { "id":"…", "ok": false,
                   "motivo":"NAO_E_DONO" } ] }

cancelar_finalizada { "pdv":"pdv-03", "venda":"…", "ids":[…], "operador":"F. Arndt" }
  → { "ok": true, "versao": N, "itens": [ … ] }

liberar_forcado { "id":"…", "operador":"Supervisor", "motivo":"PDV inoperante" }
  → { "ok": true, "versao": N, "estadoAnterior":"lancando", "pdvAnterior":"pdv-07" }

descartar { "id":"…", "operador":"Supervisor", "motivo":"aferição bomba 3" }
  → { "ok": true, "versao": N }
```

- `lancar` is **one at a time**, at the moment the item is included — that is
  what makes the sale in progress block item by item.
- `finalizar` is **a batch** (the whole sale, at a single moment). The server
  applies each id and returns the result per item; an item that fails (e.g. the
  supervisor released it in the meantime) doesn't stop the others — the PDV
  flags the conflict.

### 6.3 Events — exchange `posto.eventos` (topic)

```
abastecida.nova        { "abastecida": Abastecida, "versao": N }
abastecida.lancando    { "id":"…", "pdv":"pdv-03", "venda":"…", "versao": N }
abastecida.lancado     { "id":"…", "pdv":"pdv-03", "venda":"…", "versao": N }
abastecida.disponivel  { "id":"…", "motivo":"ESTORNO_ITEM|CANCEL_VENDA|CANCEL_NOTA|LIBERACAO_MANUAL",
                         "versao": N }
abastecida.descartada  { "id":"…", "versao": N }
```

### 6.4 The `Abastecida` object

```json
{
  "id": "B03-BC2-000148271",
  "bomba": 3, "bico": 2,
  "produto": "GC",
  "litros": 42.317,
  "valorLitro": 5.899,
  "valorTotal": 249.66,
  "encerranteInicio": 1234567.11,
  "encerranteFim": 1234609.42,
  "dataHoraMs": 1757260330000,
  "tipo": "VENDA"
}
```

`tipo`: `VENDA` (normal) or `AFERICAO` (pump test — the simulator emits one
now and then; the supervisor discards it from the panel).
`id`: stable and unique — `pump + nozzle + totalizer`. The dedup key
everywhere.

---

## 7. PDV synchronization

The classic pub/sub problem: a PDV that connects **later** has missed all the
earlier events. Solution — snapshot + replay with a version number:

1. The PDV connects, declares and **binds** the event queue, starts
   **consuming but buffers** (doesn't apply to the screen yet).
2. Calls `snapshot` → receives `versao` + lists.
3. Applies the snapshot as the baseline; then **reprocesses the buffer,
   discarding events with `versao <= versaoSnapshot`**.
4. From then on it applies live, keeping `ultimaVersao`.
5. **Version gap** (`versao > ultimaVersao + 1`) → triggers a **re-snapshot**.
   This also heals an event lost for any reason (the PDV's queue filled up,
   etc.) — the mechanism is self-healing.

On **reconnection** (the library's `OnReconnect`, which fires after the topology
recovery): re-`snapshot` + reconciliation (§9).

---

## 8. Normal flow

```
Pump → Server: fueling finished
Server: records eaDisponivel, versao++, publishes abastecida.nova
  → every PDV shows it in the pending list

The operator of PDV-A includes the fuel delivery in the sale
PDV-A → Server: lancar(id, pdv-A, V-0001)
Server: eaDisponivel → eaLancando, versao++, replies ok
Server: publishes abastecida.lancando(id, pdv-A)
  → the other PDVs remove it from the list (or show "being served at PDV-A")

  ── sale closed ──                       ── item removed / sale cancelled ──
  PDV-A → Server: finalizar(...)          PDV-A → Server: estornar(id, pdv-A, V-0001)
  Server: eaLancando → eaLancado          Server: eaLancando → eaDisponivel
  publishes abastecida.lancado            publishes abastecida.disponivel(ESTORNO_ITEM)
                                            → the other PDVs show it again
```

---

## 9. Offline operation

The PDV may be **closing the sale offline** with the fuel delivery it reserved.
That is why the server **cannot release a reservation on its own** — releasing
automatically would risk a double sale.

### The PDV's outbox

`lancar` / `estornar` / `finalizar` done offline (or whose RPC timed out)
go into a **local queue persisted in the PDV's database**. On reconnection, the
PDV **replays the outbox in order** and handles each result.

- Online, `lancar` is synchronous: `ok` → the item goes into the sale;
  `JA_LANCANDO` → "fuel delivery being served at checkout N", the item does
  **not** go in.
- Offline, `lancar` is **optimistic**: the operator includes the fuel delivery
  (which is already in the local list, frozen from before the drop), the PDV
  marks it locally and enqueues the `lancar` in the outbox. The screen shows a
  "syncing" marker on the item.

### Reconciliation on reconnection

For each fuel delivery the PDV believes it reserved in an **open** sale, it
checks the snapshot:

- the server shows `eaLancando` by this PDV → ok, carry on.
- the server shows `eaDisponivel` → re-send `lancar`.
- the server shows `eaLancando`/`eaLancado` by **another** PDV → **conflict**:
  the offline sale already went out; the item is marked "fuel delivery sold at
  another checkout — check with the supervisor". The server records the conflict
  in the panel log.

### The conflict the system does not prevent, only detects

PDV-A and PDV-B both offline, both with fuel delivery X in their frozen list,
both operators add X and print. On reconnection the first `lancar` wins; the
second gets `JA_LANCANDO` → supervisor reconciliation (void/adjustment).
Inherent to offline operation. The system's role is to make this **glaring**,
not to pretend it doesn't happen.

---

## 10. Failure rules

| # | Rule |
|---|---|
| R1 | **A reservation does not expire.** It only leaves `eaLancando`/`eaLancado` through `estornar`, `finalizar`, `cancelar_finalizada` or a manual release. A PDV disconnecting releases **nothing**. |
| R2 | **Releasing a disconnected PDV's reservation is a deliberate supervisor action** (`liberar_forcado` from the panel). Logged; emits `abastecida.disponivel` with `motivo=LIBERACAO_MANUAL`. |
| R3 | **The Vigia (observability) feeds the panel, it doesn't act.** It shows "PDV-03: disconnected for 14 min" so the supervisor can decide whether it was a network drop or a broken checkout. |
| R4 | **The PDV has a local outbox.** Offline operations are enqueued and replayed on reconnection, in order. |
| R5 | **A conflict is detected, logged and flagged — never resolved silently.** |
| R6 | **The server never drops a fuel delivery by itself.** No TTL, no `x-max-length`. Only the supervisor discards (calibration). |

---

## 11. Server panel and Vigia

### Panel (dual VCL/LCL GUI)

- Fuel-delivery grid: id, product, liters, value, **state**, **owning PDV**,
  **sale**, time in the state.
- PDV list: user, **connected / disconnected since HH:MM**.
- Buttons: **force release** (selected fuel delivery), **discard**.
- Log of conflicts and supervisor actions.

### Vigia — `Posto.Servidor.Vigia`

Subscribes to `TAMQPServer.Subscribe` with
`[seConnectionAuthenticated, seConnectionClosed]`. It keeps a table
`Username → { conectado: Boolean; desde: TDateTime }` that the panel reads.
**No automatic action** on reservations (R3). The handler runs on the broker's
notifier thread — it only updates the table under a lock and marks the UI dirty.

---

## 12. Persistence

**v1: memory only.** `Posto.Servidor.Registro` is an in-memory dictionary. A
server restart loses the pending fuel deliveries; the PDVs, on reconnecting,
redo `snapshot` and adjust (fuel deliveries that disappeared while in an open
sale become conflicts to resolve).

Next increment (outside v1): the server persists its own register in a file of
its own (JSON or a simple WAL), **separate** from the broker. The broker's
`DataDir` is an AMQP guarantee (events/commands are not lost in transport),
nearly orthogonal to this.

---

## 13. File layout

```
samples/PostoAutomacao/
  ARCHITECTURE.md                      (this file)
  README.md                            build + walkthrough
  comum/
    Posto.Json.pas                     DOM JSON codec (writer + recursive parser)
    Posto.Abastecida.pas               TAbastecida, TEstadoAbastecida, TTipoAbastecida, REC_*/MOT_*
    Posto.Contratos.pas                topology/command/event names + Abastecida<->Json + encoders
  servidor/
    Posto.Servidor.Registro.pas        authoritative state + state machine + versao (single lock)
    Posto.Servidor.Auth.pas            SASL PLAIN: automacao + pdv-* (no pre-registration)
    Posto.Servidor.Eventos.pas         publishing to posto.eventos (channel + lock)
    Posto.Servidor.Bombas.pas          pump simulator (thread + timer)
    Posto.Servidor.Comandos.pas        consumer of posto.comandos; applies the transition; detects conflicts
    Posto.Servidor.Vigia.pas           observability sink → PDV presence table
    Posto.Servidor.App.pas             UI-less assembly (broker+register+commands+Vigia+pumps)
    uServidorMain.pas / .dfm / .lfm    dual GUI panel (a shell over TPostoServidorApp)
    ServidorPosto.dpr / .lpi / .dproj
  pdv/
    Posto.PDV.Cliente.pas              connection + synchronous RPC (exclusive reply queue)
    Posto.PDV.Sincronia.pas            buffer + snapshot + replay + version-gap detection
    Posto.PDV.Modelo.pas               sale + items + outbox + reconciliation (ops on PcPool workers)
    uPdvMain.pas / .dfm / .lfm         dual GUI checkout front end
    PdvPosto.dpr / .lpi / .dproj
  smoke/
    SmokePosto.dpr / .lpi / .dproj    dual console smoke test (in-process broker + 2 PDVs, exercises the whole path)
```

Conventions: a single source for both compilers; `{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}`
at the top of the units (the GUI samples don't use `{$I amqp.inc}`); `of object`
callbacks (no closures). New units in UTF-8 **with BOM**.

**UI built in code** (`ConstroiUI`), not in the designer — the `.dfm`/`.lfm` are
empty shells (only the form + `OnCreate`/`OnClose`). It is the most robust dual
path: one layout source, nothing to keep in sync between the two formats.

**No `TThread.Queue` in either screen**: threads (pump, command consumer, the
broker's notifier, connection callbacks) only touch state under a lock and
enqueue log lines / set an atomic flag; a `TTimer` on the UI thread drains the
log and rebuilds the lists. This avoids the gotcha of `TThread.Queue` discarded
by a thread that dies (CLAUDE.md) without needing the hop through `PcPool`.

---

## 14. Manual test walkthrough

1 server + 2 PDVs (`pdv-01`, `pdv-02`) on the same machine, local ports.

1. The simulator generates a fuel delivery → **it shows up on both PDVs**.
2. `lancar` on PDV-01 → **it disappears from PDV-02** (or becomes "being served").
3. `estornar` on PDV-01 → **it comes back on both**.
4. `lancar` + `finalizar` on PDV-01 → it disappears for good; PDV-02 never sees it again.
5. Bring up **PDV-03 now** → it receives the pending ones through `snapshot`.
6. PDV-02 adds a fuel delivery and **drops** (closes the app) without finalizing →
   the server panel shows PDV-02 disconnected; the fuel delivery **stays `eaLancando`**.
7. Supervisor in the panel: **force release** → the fuel delivery comes back on
   the others; the log records the action.
8. Offline operation: bring the network down (stop the server), PDV-01 and
   PDV-02 add the **same** fuel delivery offline and finalize; the network comes
   back → the first `lancar` from the outbox wins, the second becomes a
   **conflict** on the PDV and in the panel log.

---

## 15. Backlog (post-v1)

- Persistence of the register on the server (a file of its own).
- An automated test suite for the sample (in-process host + two test clients).
- Event filter by pump/nozzle (`abastecida.<bomba>.<evento>`), for a PDV
  dedicated to one island.
- Panel: history and totals per pump/shift.
- Security: per-checkout password, `guest` out, rate-limit after an auth failure.
