# Glossário PT→EN e convenção de mensagens (frente `feat/i18n-en`)

Referência da frente de tradução da lib para inglês. **Comentários permanecem
em português**; o que muda são identificadores (server) e o texto das mensagens
de exceção/erro (cliente e server).

Não é carregado automaticamente — consulte ao renomear ou ao revisar a frente.

## Escopo (decisão travada)

| Camada | O que traduz | O que NÃO traduz |
|---|---|---|
| Cliente (`src/AMQP.*.pas`) | texto das mensagens de exceção | **identificadores públicos** — nomes de tipo/método/propriedade da API ficam como estão (é lib distribuída; renomear é breaking change e divergiria da irmã `delphi-amqp-faa`) |
| Server (`src/server/AMQP.Server.*.pas`) | identificadores (não são API pública estável) **e** texto das mensagens | nomes de teste (`Route_ExchangeParaExchange_Entrega` etc. — são descrição, não símbolo) |
| Testes | acompanham o rename dos identificadores do server | asserções e nomes de teste ficam em português |
| `CLAUDE.md`, `docs/broker-worklog.md` | referências a identificadores renomeados | o corpo em prosa fica em português |

## Convenção de mensagem de exceção

- **minúscula inicial**, **sem ponto final**, sem quebra de linha.
  Rationale: as originais em PT eram minúsculas (`'conexão já está aberta'`,
  `'frame-max negociado inválido'`) e a tradução do cliente já seguiu isso; é
  também o estilo das libs irmãs.
- Acrônimo, identificador ou nome qualificado no início **mantém a caixa**:
  `'TLS handshake failed'`, `'LSN %d is not greater…'`, `'DataDir cannot be…'`,
  `'Connection.Close not answered…'`, `'TAMQPReader: range exceeds buffer'`.
- Placeholders `%s`/`%d`/`%u` inline, nunca sozinhos no fim.
- Prefixo com `Classe: ` quando ajuda a localizar (`'TAMQPSocketStream: …'`).

## Glossário de termos

| PT | EN | Nota |
|---|---|---|
| fila | queue | |
| exchange | exchange | não traduz (termo do protocolo) |
| conexão | connection | |
| canal | channel | |
| durável | durable | |
| divergente (redeclare) | precondition failed | nome do reply-code AMQP 406 |
| equivalente (redeclare) | equivalent | |
| tipo inválido / comando inválido | command invalid | reply-code AMQP 503/406 |
| não encontrado | not found | reply-code AMQP 404 |
| em uso | in use | |
| não vazia | not empty | |
| nome reservado / acesso recusado | access refused | reply-code AMQP 403 |
| exclusiva de outro | resource locked | reply-code AMQP 405 |
| sem durabilidade | no durability | 541 de conexão (desvio local) |
| recuperação (do WAL) | recovery | ver nota `Recovery` vs `Recover` abaixo |
| persiste / grava | persist / write | |
| colocação (ENQ) | enqueue | |
| conteúdo | content | |
| marca (d'água) | mark / watermark | |
| aposentar (registro) | retire | |
| segmento | segment | |
| rotação (de segmento) | rotation | |
| compactar | compact | |
| cheio (journal no teto) | is full | |
| recusado (publish) | rejected / refused | `rejected` = nack por overflow; `refused` = teto de disco |
| órfã de fila | orphaned from queue | |
| duplicada | duplicate | |
| tinha headers | had headers | |
| para exchange (destino é exchange) | destination is exchange | **não** "stop exchange" |
| frame writer parado/falhou | frame writer stopped / failed | |

### Enum → constante do reply-code AMQP

O rename de `TAMQPEngineResult` alinhou os valores aos nomes das constantes da
spec 0-9-1:

| Antigo | Novo | Reply-code |
|---|---|---|
| `amqerNaoEncontrado` | `amqerNotFound` | 404 |
| `amqerDivergente` | `amqerPreconditionFailed` | 406 |
| `amqerTipoInvalido` | `amqerCommandInvalid` | 503/406 |
| `amqerEmUso` | `amqerInUse` | 406 |
| `amqerNaoVazia` | `amqerNotEmpty` | 406 |
| `amqerNomeReservado` | `amqerAccessRefused` | 403 |
| `amqerExclusivaDeOutro` | `amqerResourceLocked` | 405 |
| `amqerSemDurabilidade` | `amqerNoDurability` | 541 (conexão) |

Mesma tabela vale para o prefixo `amqtr*` (`TAMQPTopologyResult`).

## Estado

Toda string de runtime e todo identificador da lib estão em inglês e seguem
a convenção. **Validado nos dois compiladores:** FPC (cliente `plain` e
`-dAMQP_OPENSSL`, suíte FPCUnit do server 435/435) e Delphi (IDE — tudo
compilando, todas as suítes verdes). Falta só o item 9 abaixo.

## Pendências desta frente

1. ~~Tradução do server incompleta.~~ **Feito.** As strings PT do server
   (`FrameIO`, `Header`, `Journal`, `Records`, `Wal`) e o backend TLS do
   cliente (`AMQP.Transport.OpenSSL.pas` — 13 mensagens — mais duas em
   `AMQP.Connection.pas` e o `'nenhum'`→`'none'` de `AmqpTlsBackendName`)
   foram traduzidos. **Nenhuma string de runtime em português restante**
   (só comentários, que ficam em PT por convenção).
2. ~~Caixa das mensagens do server dividida.~~ **Feito.** As 31 mensagens
   capitalizadas do handshake/journal/frame-writer foram passadas para
   minúscula inicial. Mantêm a caixa por serem identificador/acrônimo:
   `'AmqpDeriveMessage: …'`, `'CreateNew called …'`, `'LSN %d is not …'`,
   `'DataDir cannot be changed …'`. Toda a lib segue agora a convenção.
3. ~~`frame writer stopped/failed` com caixa divergente.~~ **Feito** junto
   com o item 2 — as quatro ocorrências (duas em cada rotina de `FrameIO`)
   estão minúsculas e idênticas.
4. ~~`Recovery` é substantivo num método.~~ **Feito.** `TAMQPEngine.Recovery`
   → `Recover` e `TAMQPJournal.OpenOrRecovery` → `OpenOrRecover`.
   `TAMQPRecoveredState` / `AMQP.Server.Recovery` (unit) / `RecoveryStats`
   ficam — "recovery" como substantivo em nome de tipo/unit/propriedade é
   correto.
5. ~~Renames incompletos em locais.~~ **Feito.** Varredura de `src/server/`:
   `AErro`→`AError`, `LFila`→`LQueueName`, `LConteudo`→`LContent`,
   `LTexto`→`LText`, `LChave`→`LKey`, `AOrigem`→`ASource`,
   `ADetalhe`→`ADetail`, `ADestino`→`ADestination`, `LNomes`→`LNames`,
   `LExpOriginal`→`LOrigExpiration`, funções `ErroArg`→`ArgError` e
   `LerNumerico`→`ReadNumeric`, comentário `FRecuperando`→`FRecovering`.
   **Nenhum identificador em português restante no server.**
6. **Typos já corrigidos**: `suport`→`support`, `bind in`→`bind on`,
   `'Unexpected RPC response'`→minúscula.
7. ~~`CLAUDE.md` + `docs/broker-worklog.md` citam identificadores antigos.~~
   **Feito.** `CLAUDE.md`: `TAMQPEngine.Recupera`→`Recover`. `broker-worklog.md`:
   backticks de código atualizados + nota no cabeçalho apontando este
   glossário (a prosa histórica pode manter termos antigos, é registro).
8. ~~README.en.md.~~ **Conferido — limpo.** Nenhum texto de mensagem nem
   identificador renomeado nos dois READMEs (só a palavra "Recuperação"
   em títulos de seção, que é prosa).
9. **Units copiadas** (`pascal-pipes-faa`, `pascal-redis-faa` — cópias
   renomeadas de Threading/Transport): as mudanças de mensagem precisam de port
   manual, ou aceitar a divergência. **Adiado — reavaliar em outro momento**
   (decisão do autor, não bloqueia esta frente).
