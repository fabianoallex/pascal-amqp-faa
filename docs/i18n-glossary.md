# PT→EN glossary and message convention (`feat/i18n-en` effort)

Reference for the effort of translating the lib into English. **As of 2026-11-12 the
author decided the library is English only** for code comments, descriptions and
documentation. Identifiers stay as they were (the client's public API was never
renamed; the server's were renamed by this effort, see below), and the string literals
in tests and samples (assertion messages, sample UI text) stay in Portuguese, with no
code changes. Until that date comments stayed in Portuguese; what this effort changed
first were identifiers (server) and the text of exception/error messages (client and server).

Not loaded automatically — consult it when renaming or reviewing the effort.

## Scope (locked decision)

| Layer | What is translated | What is NOT translated |
|---|---|---|
| Client (`src/AMQP.*.pas`) | exception message text; comments (since 2026-11-12) | **public identifiers** — the API's type/method/property names stay as they are (it is a distributed lib; renaming is a breaking change and would diverge from the sibling `delphi-amqp-faa`) |
| Server (`src/server/AMQP.Server.*.pas`) | identifiers (not stable public API) **and** message text; comments (since 2026-11-12) | test names (`Route_ExchangeToExchange_Delivers` etc. — they are descriptions, not symbols) |
| Tests | follow the rename of server identifiers; comments (since 2026-11-12) | assertion messages and test names stay in Portuguese |
| Samples | comments (since 2026-11-12) | identifiers and UI text (string literals) stay in Portuguese |
| Documentation (`docs/`, README) | everything, prose included (since 2026-11-12; before that the prose stayed in Portuguese and only references to renamed identifiers were updated) | Portuguese identifiers quoted in backticks stay verbatim |

## Exception message convention

- **lowercase initial**, **no final period**, no line break.
  Rationale: the original PT ones were lowercase (`'conexão já está aberta'`,
  `'frame-max negociado inválido'`) and the client's translation already followed that; it is
  also the style of the sibling libs.
- An acronym, identifier or qualified name at the start **keeps its case**:
  `'TLS handshake failed'`, `'LSN %d is not greater…'`, `'DataDir cannot be…'`,
  `'Connection.Close not answered…'`, `'TAMQPReader: range exceeds buffer'`.
- Placeholders `%s`/`%d`/`%u` inline, never alone at the end.
- A `Class: ` prefix when it helps locate the source (`'TAMQPSocketStream: …'`).

## Term glossary

| PT | EN | Note |
|---|---|---|
| fila | queue | |
| exchange | exchange | not translated (protocol term) |
| conexão | connection | |
| canal | channel | |
| durável | durable | |
| divergente (redeclare) | precondition failed | name of AMQP reply-code 406 |
| equivalente (redeclare) | equivalent | |
| tipo inválido / comando inválido | command invalid | AMQP reply-code 503/406 |
| não encontrado | not found | AMQP reply-code 404 |
| em uso | in use | |
| não vazia | not empty | |
| nome reservado / acesso recusado | access refused | AMQP reply-code 403 |
| exclusiva de outro | resource locked | AMQP reply-code 405 |
| sem durabilidade | no durability | connection-level 541 (local deviation) |
| recuperação (do WAL) | recovery | see the `Recovery` vs `Recover` note below |
| persiste / grava | persist / write | |
| colocação (ENQ) | enqueue | |
| conteúdo | content | |
| marca (d'água) | mark / watermark | |
| aposentar (registro) | retire | |
| segmento | segment | |
| rotação (de segmento) | rotation | |
| compactar | compact | |
| cheio (journal no teto) | is full | |
| recusado (publish) | rejected / refused | `rejected` = overflow nack; `refused` = disk cap |
| órfã de fila | orphaned from queue | |
| duplicada | duplicate | |
| tinha headers | had headers | |
| para exchange (destino é exchange) | destination is exchange | **not** "stop exchange" |
| frame writer parado/falhou | frame writer stopped / failed | |

### Enum → AMQP reply-code constant

The rename of `TAMQPEngineResult` aligned the values with the names of the
0-9-1 spec constants:

| Old | New | Reply-code |
|---|---|---|
| `amqerNaoEncontrado` | `amqerNotFound` | 404 |
| `amqerDivergente` | `amqerPreconditionFailed` | 406 |
| `amqerTipoInvalido` | `amqerCommandInvalid` | 503/406 |
| `amqerEmUso` | `amqerInUse` | 406 |
| `amqerNaoVazia` | `amqerNotEmpty` | 406 |
| `amqerNomeReservado` | `amqerAccessRefused` | 403 |
| `amqerExclusivaDeOutro` | `amqerResourceLocked` | 405 |
| `amqerSemDurabilidade` | `amqerNoDurability` | 541 (connection) |

The same table applies to the `amqtr*` prefix (`TAMQPTopologyResult`).

## Status

Every runtime string and every identifier of the lib is in English and follows
the convention. **Validated on both compilers:** FPC (client `plain` and
`-dAMQP_OPENSSL`, server FPCUnit suite 435/435) and Delphi (IDE — everything
compiling, all suites green). Only item 9 below is left.

## Open items of this effort

1. ~~Server translation incomplete.~~ **Done.** The server's PT strings
   (`FrameIO`, `Header`, `Journal`, `Records`, `Wal`) and the client's TLS
   backend (`AMQP.Transport.OpenSSL.pas` — 13 messages — plus two in
   `AMQP.Connection.pas` and the `'nenhum'`→`'none'` of `AmqpTlsBackendName`)
   were translated. **No Portuguese runtime string left**
   (only comments, which at the time stayed in PT by convention; translated since 2026-11-12).
2. ~~Server message case split.~~ **Done.** The 31 capitalized
   handshake/journal/frame-writer messages were changed to a
   lowercase initial. These keep their case because they are an identifier/acronym:
   `'AmqpDeriveMessage: …'`, `'CreateNew called …'`, `'LSN %d is not …'`,
   `'DataDir cannot be changed …'`. The whole lib now follows the convention.
3. ~~`frame writer stopped/failed` with inconsistent case.~~ **Done** together
   with item 2 — the four occurrences (two in each `FrameIO` routine)
   are lowercase and identical.
4. ~~`Recovery` is a noun in a method name.~~ **Done.** `TAMQPEngine.Recovery`
   → `Recover` and `TAMQPJournal.OpenOrRecovery` → `OpenOrRecover`.
   `TAMQPRecoveredState` / `AMQP.Server.Recovery` (unit) / `RecoveryStats`
   stay — "recovery" as a noun in a type/unit/property name is
   correct.
5. ~~Incomplete renames of locals.~~ **Done.** Sweep of `src/server/`:
   `AError`→`AError`, `LQueue`→`LQueueName`, `LConteudo`→`LContent`,
   `LText`→`LText`, `LKey`→`LKey`, `ASource`→`ASource`,
   `ADetalhe`→`ADetail`, `ADestination`→`ADestination`, `LNames`→`LNames`,
   `LExpOriginal`→`LOrigExpiration`, functions `ErroArg`→`ArgError` and
   `LerNumerico`→`ReadNumeric`, comment `FRecuperando`→`FRecovering`.
   **No Portuguese identifier left in the server.**
6. **Typos already fixed**: `suport`→`support`, `bind in`→`bind on`,
   `'Unexpected RPC response'`→lowercase.
7. ~~`CLAUDE.md` + `docs/broker-worklog.md` cite old identifiers.~~
   **Done.** `CLAUDE.md`: `TAMQPEngine.Recupera`→`Recover`. `broker-worklog.md`:
   code backticks updated + a note in the header pointing to this
   glossary (the historical prose may keep old terms, it is a record).
8. ~~README.en.md.~~ **Checked — clean.** No message text or
   renamed identifier in the two READMEs (only the word "Recuperação"
   in section titles, which is prose). (The English README has since become `README.md`.)
9. **Copied units** (`pascal-pipes-faa`, `pascal-redis-faa` — renamed copies
   of Threading/Transport): the message changes need a manual port,
   or accepting the divergence. **Deferred — reassess some other time**
   (the author's decision, does not block this effort).
