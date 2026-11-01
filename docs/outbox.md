# Outbox transacional por um broker AMQP (`src/outbox`)

`AMQP.Outbox` liga o outbox transacional da [pascal-db-faa](https://github.com/fabianoallex/pascal-db-faa)
(`PascalDb.Outbox`, 0.18.0 ou mais nova) a um broker AMQP: o **sink** publica cada evento que o
relay entrega, e o **consumidor** roda um handler numa fila própria, com retry por fila de
espera, fila de mortos e deduplicação no banco antes do ack. Entrou na **v0.5.0** (2026-10-31).

Veio do pascal-agenda-faa (`Agenda.Outbox.Amqp`, decisão 11 da M7 de lá), a mesma API: o que
custava copiar é a fila de retry com TTL, a contagem de tentativas pelo `x-death`, a fila de
mortos e a deduplicação antes do ack. A única troca foi o relógio: lá era o `TJwt.UnixNow` da
pascal-api-infra-faa, usado só como relógio; aqui é o `TClock` da pascal-common-faa, e a ponte não
puxa uma terceira lib.

## Uma ponte, fora dos pacotes

A unit fica em `src/outbox`, **fora** de `pascal_amqp_faa.lpk` e de `pascal_amqp_faa_server.lpk`,
como o `src/redis` da pascal-api-infra-faa: ela precisa da pascal-db-faa, que o cliente e o broker
não precisam. Quem não usa o outbox não compila nem linka nada dela.

| Build | O que somar |
|---|---|
| Lazarus | o pacote `pascal_db_faa` (da cópia que a aplicação fornece) nos pacotes requeridos do projeto; `src/outbox` em *Other unit files* e `src` em *Include files* (a unit começa com `{$I amqp.inc}`) |
| FPC puro | `-Fu<amqp>/src/outbox -Fi<amqp>/src -Fu<pascal-db-faa>/src -Fi<pascal-db-faa>/src` |
| Delphi | `src\outbox` e o `src\` da pascal-db-faa no search path |

Como a pascal-common-faa, a pascal-db-faa é da **aplicação**: o submódulo
`external/pascal-db-faa` deste repositório serve só aos testes. A pascal-db-faa 0.18.0 exige a
pascal-common-faa 1.17 ou mais nova, então quem usa a ponte precisa dessa, mesmo que o cliente
sozinho aceite a 1.8.

## A mensagem

| Campo do evento (`TOutboxEvent`) | Na mensagem |
|---|---|
| `Payload` | o corpo, em UTF-8, `content-type: application/json` |
| `Id` | `message-id` |
| `Kind` | a propriedade `type` e a **routing key** |
| `CreatedAt` | `timestamp` |
| `TenantId` | o header `tenant-id` (`OUTBOX_AMQP_TENANT_HEADER`) |
| `TraceParent` | não vai como campo: o span do relay é o corrente quando o sink publica, e o `publish` do cliente põe o `traceparent` nos headers (ver [tracing](tracing.md)) |

Toda mensagem sai **persistente** (`delivery-mode 2`), num exchange `topic`.

## O sink: `TOutboxAmqpSink`

```pascal
uses PascalDb.Outbox, AMQP.Connection, AMQP.Outbox;

OutboxAmqpDeclareExchange(Canal, 'loja.eventos');
Relay := TOutboxRelay.Create(Factory,
  TOutboxAmqpSink.Create(CanalDoRelay, 'loja.eventos'),
  PdbOutboxNewOwner('relay'), TOutboxOptions.Default);
// num timer ou numa thread de trabalho:
Relay.RunOnce(AgoraEmSegundosUnix);
```

- O construtor põe o canal em **confirm mode**, e cada `Deliver` espera o ack do broker
  (`AConfirmTimeoutMs`, padrão 5 s). Sem ack, ou com a conexão caída, levanta, e o relay deixa o
  evento pendente para a próxima passada. O relay não desiste de um sink: broker fora volta.
- O `Deliver` roda **dentro da transação do relay** (o contrato do `IOutboxSink`): um broker lento
  segura essa transação (no SQLite, a trava de escrita do banco inteiro).
- O canal não é do sink (quem o cria o libera) e é usado por uma thread de cada vez, a do relay.
- Um evento cujo `Kind` não casa binding nenhum é aceito pelo broker e descartado: o confirm sai do
  mesmo jeito. É assim que uma fila nova só recebe o que foi publicado depois de ela existir.

## O consumidor: `TOutboxAmqpConsumer`

```pascal
Fila := TOutboxAmqpQueue.Create('loja.email', 'loja.eventos', 'pedido.#');
Fila.RetryDelayMs := 30000; // padrão
Fila.MaxAttempts := 10;     // padrão (OUTBOX_DEFAULT_MAX_ATTEMPTS)
OutboxAmqpDeclareQueues(Canal, Fila);

Consumidor := TOutboxAmqpConsumer.Create(Factory, CanalDoConsumidor, Fila, 'email', HandlerDeEmail);
Consumidor.OnError := Log.OnOutboxError; // um método
Consumidor.Start;
...
Consumidor.Stop;           // antes de fechar o canal e de liberar o consumidor
CanalDoConsumidor.Free;
Consumidor.Free;
```

### Três filas por consumidor

`OutboxAmqpDeclareQueues` declara, todas duráveis:

```
exchange (topic) --binding--> loja.email --nack--> loja.email.retry --TTL--> loja.email
                                  |                (sem consumidor)
                                  +--(MaxAttempts)--> loja.email.dead
```

- **`loja.email`**, a fila de trabalho, ligada ao exchange pela binding key, com dead-letter para
  a fila de retry (pelo exchange padrão, que roteia pelo nome da fila).
- **`loja.email.retry`**, sem consumidor, com `x-message-ttl = RetryDelayMs` e dead-letter de volta
  para a fila de trabalho. É o TTL **da fila**, não um por mensagem: o `expiration` de uma
  mensagem só vence quando ela chega na cabeça da fila.
- **`loja.email.dead`**, onde a mensagem que esgotou as tentativas fica parada, como chegou.

Mudar o `RetryDelayMs` muda os argumentos da fila de retry, e redeclarar uma fila com outros
argumentos é `PRECONDITION_FAILED` (406): apague a fila antes.

### O que cada entrega vira

1. Se o handler não aceita o `Kind` (outro tipo debaixo da mesma binding key): **ack**, nada roda.
2. Se o evento já está em `PROCESSED_EVENTS` para este consumidor (uma reentrega do broker, ou o
   relay que publicou duas vezes depois de perder a lease): **ack**, o handler não roda de novo.
3. Senão roda o handler, grava o evento em `PROCESSED_EVENTS` e dá **ack**. É at-least-once: uma
   queda entre o handler e a gravação roda o handler de novo.
4. Handler que levanta: `OnError`, e **`Nack` sem requeue**. O dead-letter da fila de trabalho leva
   a mensagem para a fila de retry, e ela volta depois do TTL.
5. As tentativas já feitas são as entradas `rejected` **da fila de trabalho** no header `x-death`
   (o broker não conta tentativas sozinho). Na tentativa `MaxAttempts` que falha, o consumidor
   publica a mensagem na fila de mortos e dá ack nela.
6. Mensagem que não é evento do outbox (sem `message-id`, `type` ou `tenant-id`) vai direto para a
   fila de mortos.

Os callbacks rodam **em paralelo** no pool do processo (`PcPool`), até o prefetch
(`TOutboxAmqpQueue.Prefetch`, padrão 10): o handler tem de aceitar isso. `InFlight` conta os
callbacks em andamento, para quem espera o consumidor sossegar.

`Stop` cancela o consumer e espera (até `AWaitMs`) os callbacks em andamento; um callback que
começa depois disso não roda o handler nem dá ack, e o fechamento do canal devolve a mensagem à
fila. Chame antes de fechar o canal (nada de trabalho novo enquanto o processo encerra) e antes de
liberar o consumidor.

`OutboxAmqpPending` soma as mensagens prontas nas filas de trabalho e de retry (não as entregues
ainda sem ack), e `OutboxAmqpEvent` lê o evento de uma entrega, com as tentativas.

### Métricas

No `PcMetrics` da pascal-common-faa, contado depois do ack, do nack ou do estacionamento:

| Métrica | Labels |
|---|---|
| `outbox.consumer.messages` | `outbox.consumer`, `outbox.event.kind` (`other` para um tipo que o handler não aceita, para quem publica no exchange não inflar as séries), `outbox.result` (`done`, `duplicate`, `ignored`, `retried`, `parked`) |

## O SQL é da aplicação

Como o do `PascalDb.Outbox`, o SQL é lido **pelo nome** no loader da factory, e a tabela é da
aplicação:

| Constante | Nome | Parâmetros |
|---|---|---|
| `PROCESSED_SQL_EXISTS` | `PROCESSED.EXISTS` | `CONSUMER`, `TENANT_ID`, `EVENT_ID`; uma linha se já processado |
| `PROCESSED_SQL_INSERT` | `PROCESSED.INSERT` | `CONSUMER`, `TENANT_ID`, `EVENT_ID`, `NOW` (segundos Unix) |
| `PROCESSED_SQL_PURGE` | `PROCESSED.PURGE` | `BEFORE` (segundos Unix); chamado por `OutboxAmqpPurgeProcessed` |

Referência (**testada no SQLite**, pela suíte `tests/Outbox`; a mesma forma vale no PostgreSQL,
que é o que o pascal-agenda-faa roda):

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

O `ON CONFLICT ... DO NOTHING` cobre dois consumidores da mesma fila que receberam a mesma
mensagem (uma reentrega enquanto o primeiro ainda rodava). Num banco sem essa forma, um `INSERT`
simples também serve: a chave primária recusa o segundo, o callback trata como falha, e a
reentrega seguinte cai no passo 2 (já processado) e leva ack. O `PROCESSED_AT` é o `TClock` da
pascal-common-faa em segundos Unix (UTC), trocável nos testes (`TClock.SetClock`).

O purge de `PROCESSED_EVENTS` é da aplicação, no ritmo que ela quiser: uma linha só precisa viver
enquanto o broker ainda pode reentregar aquele evento.

## Testes

`tests/Outbox` (DUnitX `AMQP.OutboxTests.dproj`, FPCUnit `fpc/AMQPOutboxTestsFpc.lpi`, espelho
gerado): 8 testes contra o **broker embutido** em porta efêmera, com um arquivo SQLite no
diretório temporário (`AMQP.OutboxTestDb`: SQLdb no FPC, FireDAC no Delphi). Nenhum RabbitMQ.

- a mensagem publicada e seus campos, com confirm;
- tratado uma vez e gravado; reentregue e não rodado de novo; outro tipo, ack sem rodar;
- uma falha refeita a partir da fila de retry; estacionado depois das tentativas;
- o purge por idade; o `PROCESSED_AT` vindo do `TClock` (a mutação "ler `SysUtils.Now` em vez do
  `TClock`" derruba este).

O FPC carrega a biblioteca do SQLite: no Windows, a `sqlite3.dll` **do sqlite.org** (a do Python
não tem as funções de metadado que o SQLdb usa; ver gotcha 23 da pascal-db-faa) ao lado do runner,
ou em `AMQP_SQLITE_CLIENT`; no Linux, `libsqlite3.so.0` (`apt install libsqlite3-0`). O FireDAC
linka o SQLite no programa. No Linux, `SUITES=outbox sh tools/test_fpc_docker.sh`.
