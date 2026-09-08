# Posto Automação — arquitetura

Sample que serve de base para uma aplicação real: um **servidor de automação de
bombas de combustível** com o broker AMQP embutido, e **PDVs** na rede da loja
que consomem as abastecidas para lançá-las em vendas.

Este documento é a referência canônica do desenho. O histórico da conversa que
o produziu não é recarregado — o que vale é o que está aqui.

---

## 1. Objetivo

- A automação recebe abastecidas das bombas (aqui: um simulador por timer).
- Toda abastecida concluída fica **visível para todos os PDVs** da rede, quase
  em tempo real.
- Quando o cliente chega a um PDV, a abastecida **já está lá**.
- A partir do momento em que um PDV inclui a abastecida numa venda, **nenhum
  outro PDV** pode usá-la. Uma abastecida pertence a **uma venda só**.
- Se o item é removido da venda, ou a venda é cancelada, a abastecida **volta a
  ficar disponível** para os demais.
- Um PDV que liga depois recebe as abastecidas **ainda pendentes**.

### Não-objetivos do v1

- Persistência do registro de abastecidas (memória só; reinício do servidor
  perde o que estava pendente). Entra como incremento isolado depois.
- Liberação automática de reserva por queda de PDV (ver §10, R1/R2).
- Multi-posto / mais de um broker. Um broker por loja.
- Fiscal real (emissão de NF-e/SAT). O "finalizar venda" aqui é só a transição
  de estado da abastecida.
- Agregação / métricas prontas no painel — número cru, não histograma.

---

## 2. Princípio: quem é dono do estado

O broker desta lib é **transporte, não banco**: filas em memória, sem estado de
consumidor, sem management API. Logo:

> **O estado autoritativo das abastecidas vive no servidor de automação**, que
> já é dono do estado das bombas. O broker carrega duas coisas: **eventos**
> (servidor → PDVs, um-para-muitos) e **comandos** (PDV → servidor,
> request/reply).

A regra "uma abastecida em uma venda só" é um lock distribuído com **um único
árbitro** — o servidor. O AMQP não dá isso sozinho; o servidor dá, processando
comandos **serialmente por id de abastecida**.

Isto também alinha com a filosofia da Fase 4 do broker (D23): **duplicata é o
modo de falha aceitável e reconciliável; perda de abastecida não é.** O servidor
nunca descarta uma abastecida por conta própria.

---

## 3. Papéis

### Servidor (um processo)

Automação + broker embutido + registro autoritativo + painel GUI. Sobe o
`TAMQPServer` numa porta TCP da rede da loja; os PDVs conectam nela.

### PDV (um processo por caixa)

App GUI de frente de caixa (recorte deste sample: só a parte de abastecidas).
Tem **banco local próprio** e **opera mesmo isolado da rede** (contingência):
lança e fecha vendas offline, sincroniza quando a rede volta.

---

## 4. Estados da abastecida

```pascal
TEstadoAbastecida = (eaDisponivel, eaLancando, eaLancado);
```

| Estado | Significado | Bloqueia outros PDVs? |
|---|---|---|
| `eaDisponivel` | livre; qualquer PDV pode lançar | não |
| `eaLancando` | item incluído numa venda **em andamento** num PDV | **sim** |
| `eaLancado` | venda **finalizada** com a abastecida | **sim** |

`eaLancando` e `eaLancado` guardam `pdv`, `venda` e o instante da transição.
**A venda em andamento já tranca a abastecida** — não existe "reservar a venda
inteira"; cada item lançado tranca a sua abastecida na hora.

### Transições

Toda transição incrementa a `versao` global (contador monotônico do servidor) e
publica um evento.

| De | Para | Gatilho | Comando |
|---|---|---|---|
| — | `eaDisponivel` | bomba concluiu abastecimento | interno (simulador) |
| `eaDisponivel` | `eaLancando` | operador inclui a abastecida na venda | `lancar` |
| `eaLancando` | `eaDisponivel` | remove o item / cancela venda em andamento | `estornar` |
| `eaLancando` | `eaLancado` | operador finaliza a venda | `finalizar` |
| `eaLancado` | `eaDisponivel` | cancelamento da venda/nota já finalizada | `cancelar_finalizada` |
| bloqueado | `eaDisponivel` | supervisor força liberação | `liberar_forcado` |
| `eaDisponivel` | (removida) | supervisor descarta (aferição/teste de bomba) | `descartar` |

---

## 5. Topologia AMQP

| Objeto | Tipo | Uso |
|---|---|---|
| `posto.eventos` | exchange `topic` | eventos servidor → todos os PDVs |
| fila por PDV | exclusive + auto-delete, bind `abastecida.#` | pub/sub; todo PDV vê todo evento |
| `posto.comandos` | fila (não durável) | fila de requisição RPC; consumida pelo servidor |
| resposta RPC | fila exclusiva por PDV (`reply-to` + `correlation-id`) | resposta do servidor ao PDV |

- **vhost**: `/` (default do broker).
- **payload**: JSON no *body*, `content-type = application/json`. (Codec mínimo
  próprio em `comum/Posto.Json.pas` — `fpjson` e `System.JSON` divergem entre os
  compiladores, e o `snapshot` carrega array de objetos, que no
  `TAMQPFieldTable` cairia no `TValue`-em-`TValue` do FPC.)

---

## 6. Contrato de mensagens

### 6.1 Identidade do PDV

O PDV autentica no broker com **usuário próprio** `pdv-<caixa>` (ex.:
`pdv-03`), senha compartilhada ou por caixa (config do servidor via
`TAMQPStaticAuthenticator`). Isso dá **duas coisas de graça**:

1. autenticação por caixa;
2. o `Username` chega nos eventos de conexão do broker (`seConnectionAuthenticated`
   / `seConnectionClosed`), então o **Vigia** (§11) mapeia conexão → PDV sem
   nenhum handshake extra.

O servidor usa o usuário `automacao` para a própria conexão de publicação.

### 6.2 Comandos — RPC via `posto.comandos`

Requisição: `reply-to` = fila exclusiva do PDV, `correlation-id` = GUID,
`type` = nome do comando, body = JSON. Resposta: body JSON com o mesmo
`correlation-id`.

```
snapshot {}
  → { "versao": N,
      "disponiveis": [ Abastecida, ... ],
      "bloqueadas":  [ { "id":"…", "estado":"lancando|lancado",
                         "pdv":"pdv-03", "venda":"…", "desde": epochMs }, ... ] }

lancar { "id":"…", "pdv":"pdv-03", "venda":"V-2026-0001" }   // UMA A UMA
  → { "ok": true,  "versao": N }
  | { "ok": false, "motivo": "JA_LANCANDO|JA_LANCADO|NAO_EXISTE|DESCARTADA",
      "pdvAtual":"pdv-05", "vendaAtual":"…" }

estornar { "id":"…", "pdv":"pdv-03", "venda":"V-2026-0001" }
  → { "ok": true, "versao": N }          // idempotente; valida dono (pdv+venda)
  | { "ok": false, "motivo": "NAO_E_DONO|NAO_EXISTE" }

finalizar { "pdv":"pdv-03", "venda":"V-2026-0001", "ids":["…","…"] }   // LOTE
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

- `lancar` é **uma a uma**, no momento da inclusão do item — é o que faz a venda
  em andamento bloquear item a item.
- `finalizar` é **em lote** (a venda inteira, num momento só). O servidor aplica
  cada id e devolve o resultado por item; um item que falhar (ex.: supervisor
  liberou no meio) não impede os outros — o PDV sinaliza o conflito.

### 6.3 Eventos — exchange `posto.eventos` (topic)

```
abastecida.nova        { "abastecida": Abastecida, "versao": N }
abastecida.lancando    { "id":"…", "pdv":"pdv-03", "venda":"…", "versao": N }
abastecida.lancado     { "id":"…", "pdv":"pdv-03", "venda":"…", "versao": N }
abastecida.disponivel  { "id":"…", "motivo":"ESTORNO_ITEM|CANCEL_VENDA|CANCEL_NOTA|LIBERACAO_MANUAL",
                         "versao": N }
abastecida.descartada  { "id":"…", "versao": N }
```

### 6.4 Objeto `Abastecida`

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

`tipo`: `VENDA` (normal) ou `AFERICAO` (teste de bomba — o simulador emite uma
de vez em quando; o supervisor descarta pelo painel).
`id`: estável e único — `bomba + bico + encerrante`. Chave de dedup em toda
parte.

---

## 7. Sincronização do PDV

O problema clássico do pub/sub: um PDV que conecta **depois** perdeu todos os
eventos anteriores. Solução — snapshot + replay com número de versão:

1. PDV conecta, declara e faz **bind** da fila de eventos, começa a **consumir
   mas bufferiza** (não aplica na tela ainda).
2. Chama `snapshot` → recebe `versao` + listas.
3. Aplica o snapshot como baseline; então **reprocessa o buffer descartando
   eventos com `versao <= versaoSnapshot`**.
4. Daí em diante aplica ao vivo, guardando `ultimaVersao`.
5. **Salto de versão** (`versao > ultimaVersao + 1`) → dispara **re-snapshot**.
   Isso também cura evento perdido por qualquer motivo (fila do PDV encheu
   etc.) — o mecanismo é auto-recuperável.

Na **reconexão** (`OnReconnect` da lib, que dispara após o recovery da
topologia): re-`snapshot` + reconciliação (§9).

---

## 8. Fluxo normal

```
Bomba → Servidor: abastecimento concluído
Servidor: registra eaDisponivel, versao++, publica abastecida.nova
  → todos os PDVs exibem na lista de pendentes

Operador do PDV-A inclui a abastecida na venda
PDV-A → Servidor: lancar(id, pdv-A, V-0001)
Servidor: eaDisponivel → eaLancando, versao++, responde ok
Servidor: publica abastecida.lancando(id, pdv-A)
  → demais PDVs removem da lista (ou mostram "em atendimento no PDV-A")

  ── venda fechada ──                    ── item removido / venda cancelada ──
  PDV-A → Servidor: finalizar(...)        PDV-A → Servidor: estornar(id, pdv-A, V-0001)
  Servidor: eaLancando → eaLancado        Servidor: eaLancando → eaDisponivel
  publica abastecida.lancado              publica abastecida.disponivel(ESTORNO_ITEM)
                                            → demais PDVs voltam a exibir
```

---

## 9. Operação em contingência

O PDV pode estar **fechando a venda offline** com a abastecida que reservou.
Por isso o servidor **não pode liberar reserva por conta própria** — liberar
automaticamente seria risco de venda dupla.

### Outbox do PDV

`lancar` / `estornar` / `finalizar` feitos offline (ou cujo RPC deu timeout)
vão para uma **fila local persistida no banco do PDV**. Na reconexão, o PDV
**reproduz o outbox em ordem** e trata cada resultado.

- Online, `lancar` é síncrono: `ok` → item entra na venda; `JA_LANCANDO` →
  "abastecida em atendimento no caixa N", item **não** entra.
- Offline, `lancar` é **otimista**: o operador inclui a abastecida (que já está
  na lista local, congelada de antes da queda), o PDV marca localmente e
  enfileira o `lancar` no outbox. A tela mostra um marcador "sincronizando" no
  item.

### Reconciliação na reconexão

Para cada abastecida que o PDV acha que reservou numa venda **aberta**, confere
no snapshot:

- servidor mostra `eaLancando` por este PDV → ok, segue.
- servidor mostra `eaDisponivel` → re-envia `lancar`.
- servidor mostra `eaLancando`/`eaLancado` por **outro** PDV → **conflito**:
  a venda offline já saiu; o item é marcado "abastecida vendida em outro caixa —
  verificar com supervisor". O servidor registra o conflito no log do painel.

### O conflito que o sistema não previne, só detecta

PDV-A e PDV-B ambos offline, ambos com a abastecida X na lista congelada, ambos
os operadores lançam X e imprimem. Na reconexão o primeiro `lancar` ganha; o
segundo recebe `JA_LANCANDO` → reconciliação do supervisor (estorno/ajuste).
Inerente à contingência. O papel do sistema é deixar isso **gritante**, não
fingir que não acontece.

---

## 10. Regras de falha

| # | Regra |
|---|---|
| R1 | **Reserva não expira.** Só sai de `eaLancando`/`eaLancado` por `estornar`, `finalizar`, `cancelar_finalizada` ou liberação manual. Desconexão do PDV **não** libera nada. |
| R2 | **Liberar reserva de PDV desconectado é ação deliberada do supervisor** (`liberar_forcado` pelo painel). Logada; emite `abastecida.disponivel` com `motivo=LIBERACAO_MANUAL`. |
| R3 | **O Vigia (observabilidade) alimenta o painel, não age.** Mostra "PDV-03: desconectado há 14 min" para o supervisor decidir se foi queda de rede ou caixa inoperante. |
| R4 | **O PDV tem outbox local.** Operações offline são enfileiradas e reproduzidas na reconexão, em ordem. |
| R5 | **Conflito é detectado, logado e sinalizado — nunca resolvido em silêncio.** |
| R6 | **O servidor nunca descarta abastecida sozinho.** Sem TTL, sem `x-max-length`. Só o supervisor descarta (aferição). |

---

## 11. Painel do servidor e Vigia

### Painel (GUI dual VCL/LCL)

- Grade de abastecidas: id, produto, litros, valor, **estado**, **PDV dono**,
  **venda**, tempo no estado.
- Lista de PDVs: usuário, **conectado / desconectado desde HH:MM**.
- Botões: **liberar forçado** (abastecida selecionada), **descartar**.
- Log de conflitos e de ações de supervisor.

### Vigia — `Posto.Servidor.Vigia`

Assina `TAMQPServer.Subscribe` com
`[seConnectionAuthenticated, seConnectionClosed]`. Mantém uma tabela
`Username → { conectado: Boolean; desde: TDateTime }` que o painel lê.
**Nenhuma ação automática** sobre reservas (R3). O handler roda na thread
notificadora do broker — só atualiza a tabela sob lock e marca a UI como suja.

---

## 12. Persistência

**v1: memória só.** O `Posto.Servidor.Registro` é um dicionário em memória. O
reinício do servidor perde as abastecidas pendentes; os PDVs, ao reconectar,
re-fazem `snapshot` e ajustam (abastecidas que sumiram e estavam numa venda
aberta viram conflito a resolver).

Incremento seguinte (fora do v1): o servidor persiste o próprio registro num
arquivo próprio (JSON ou WAL simples), **separado** do broker. `DataDir` do
broker é garantia AMQP (evento/comando não se perde no transporte), quase
ortogonal a isto.

---

## 13. Estrutura de arquivos

```
samples/PostoAutomacao/
  ARQUITETURA.md                       (este arquivo)
  README.md                            build + roteiro
  comum/
    Posto.Json.pas                     codec JSON DOM (writer + parser recursivo)
    Posto.Abastecida.pas               TAbastecida, TEstadoAbastecida, TTipoAbastecida, REC_*/MOT_*
    Posto.Contratos.pas                nomes de topologia/comando/evento + Abastecida<->Json + encoders
  servidor/
    Posto.Servidor.Registro.pas        estado autoritativo + máquina de estados + versao (lock único)
    Posto.Servidor.Auth.pas            SASL PLAIN: automacao + pdv-* (sem pré-cadastro)
    Posto.Servidor.Eventos.pas         publicação em posto.eventos (canal + lock)
    Posto.Servidor.Bombas.pas          simulador de bombas (thread + timer)
    Posto.Servidor.Comandos.pas        consumidor de posto.comandos; aplica transição; detecta conflito
    Posto.Servidor.Vigia.pas           sink de observabilidade → tabela de presença dos PDVs
    Posto.Servidor.App.pas             montagem sem UI (broker+registro+comandos+Vigia+bombas)
    uServidorMain.pas / .dfm / .lfm    painel GUI dual (casca sobre TPostoServidorApp)
    ServidorPosto.dpr / .lpi / .dproj
  pdv/
    Posto.PDV.Cliente.pas              conexão + RPC síncrono (fila de resposta exclusiva)
    Posto.PDV.Sincronia.pas            buffer + snapshot + replay + detecção de salto de versão
    Posto.PDV.Modelo.pas               venda + itens + outbox + reconciliação (ops em workers do AmqpPool)
    uPdvMain.pas / .dfm / .lfm         frente de caixa GUI dual
    PdvPosto.dpr / .lpi / .dproj
  smoke/
    SmokePosto.dpr / .lpi / .dproj    smoke test console dual (broker in-process + 2 PDVs, exercita o caminho todo)
```

Convenções: um fonte só para os dois compiladores; `{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}`
no topo das units (os samples GUI não usam `{$I amqp.inc}`); callbacks `of object`
(sem closure). Units novas em UTF-8 **com BOM**.

**UI construída em código** (`ConstroiUI`), não no designer — os `.dfm`/`.lfm` são
cascas vazias (só a form + `OnCreate`/`OnClose`). É o caminho dual mais robusto:
uma fonte de layout, nada para manter em sincronia entre os dois formatos.

**Sem `TThread.Queue` nas duas telas**: threads (bomba, consumidor de comandos,
notificadora do broker, callbacks de conexão) só mexem em estado com lock e
enfileiram log / marcam uma flag atômica; um `TTimer` na thread da UI drena o
log e reconstrói as listas. Isso evita o gotcha do `TThread.Queue` descartado
por thread que morre (CLAUDE.md) sem precisar do salto pelo `AmqpPool`.

---

## 14. Roteiro de teste manual

1 servidor + 2 PDVs (`pdv-01`, `pdv-02`) na mesma máquina, portas locais.

1. Simulador gera abastecida → **aparece nos dois PDVs**.
2. `lancar` no PDV-01 → **some do PDV-02** (ou vira "em atendimento").
3. `estornar` no PDV-01 → **volta nos dois**.
4. `lancar` + `finalizar` no PDV-01 → some definitivamente; PDV-02 nunca a viu de volta.
5. Sobe **PDV-03 agora** → recebe as pendentes por `snapshot`.
6. PDV-02 lança uma abastecida e **cai** (fecha o app) sem finalizar → painel do
   servidor mostra PDV-02 desconectado; a abastecida **continua `eaLancando`**.
7. Supervisor no painel: **liberar forçado** → abastecida volta nos demais;
   log registra a ação.
8. Contingência: derruba a rede (para o servidor), PDV-01 e PDV-02 lançam a
   **mesma** abastecida offline e finalizam; volta a rede → primeiro `lancar`
   do outbox ganha, segundo vira **conflito** no PDV e no log do painel.

---

## 15. Backlog (pós-v1)

- Persistência do registro no servidor (arquivo próprio).
- Suíte de teste automatizada do sample (host in-process + dois clientes de teste).
- Filtro de eventos por bomba/bico (`abastecida.<bomba>.<evento>`), para PDV
  dedicado a uma ilha.
- Painel: histórico e totais por bomba/turno.
- Segurança: senha por caixa, `guest` fora, rate-limit pós-falha de auth.
