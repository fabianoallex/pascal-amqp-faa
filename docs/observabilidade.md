# Observabilidade do broker

Um broker embutido é uma caixa-preta dentro da sua aplicação: ele aceita conexões, roteia, entrega e descarta sem que nada disso apareça no log da app. A observabilidade abre essa caixa — o `TAMQPServer` emite eventos estruturados, e você assina o que interessa.

As decisões de arquitetura estão travadas no `CLAUDE.md` como **D29–D36**; este documento é o manual de uso. Se você só quer o resumo: **é opt-in, é read-only, é best-effort, e o handler roda numa thread só dele.**

## Começando

```pascal
uses AMQP.Server.Events, AMQP.Server.Broker;

type
  TMeuLog = class
  public
    procedure AoEvento(const AEvento: TAMQPServerEvent);
  end;

procedure TMeuLog.AoEvento(const AEvento: TAMQPServerEvent);
begin
  // Roda na thread notificadora do broker. Uma só, sempre a mesma.
  Writeln(AmqpEventTypeName(AEvento.EventType), ' conn=', AEvento.ConnectionId);
end;

// ...
Broker := TAMQPServer.Create;
Log := TMeuLog.Create;
Broker.Subscribe(Log.AoEvento);   // antes do Start: você quer o 1º evento
Broker.Start;
// ...
Broker.Stop;
Broker.Unsubscribe(Log.AoEvento); // ANTES de liberar o Log — ver "Armadilhas"
```

Sem nenhum `Subscribe`, o broker é byte a byte o de sempre: o caminho quente testa uma máscara e nem chega a montar o registro do evento. Ninguém paga por não observar.

Para filtrar por tipo — o que evita montar registro que você vai descartar:

```pascal
Broker.Subscribe(Log.AoEvento,
  [seConnectionAuthenticated, seMessagePublished, seMessageAcked]);
```

Assinar o **mesmo método** de novo **substitui** a máscara anterior; não duplica a entrega.

Sample completo, com relatório de descartes no fim: `samples/Server/AMQPServer.dpr` (`lazbuild samples\Server\AMQPServer.lpi`, ou o `.dproj` no IDE Delphi).

## O contrato, em quatro linhas

1. **Read-only.** Nenhum handler influencia roteamento, autenticação, ack ou descarte. Quem precisa *decidir* implementa `IAMQPAuthorizer` — que existe exatamente para isso. Levantar exceção dentro do handler não cancela coisa alguma; só é contado.
2. **Assíncrono, sempre.** *Todo* evento é copiado para um ring limitado e entregue por uma thread dedicada — inclusive os que nascem na thread de leitura da conexão. Seu handler nunca roda na thread de leitura nem num worker do pool.
3. **Best-effort: pode perder.** Ring cheio ⇒ o evento **mais novo** é descartado e contado. O emissor nunca bloqueia e nunca levanta.
4. **Ordem por origem.** Eventos da mesma conexão (ou da mesma fila) chegam na ordem em que aconteceram. Entre conexões diferentes, a ordem relativa não é promessa de API.

### Por que o handler não roda inline

Porque não existe metade segura. As três alternativas ao ring foram medidas contra a arquitetura e cada uma quebra algo diferente:

| onde rodar o handler | o que quebra |
|---|---|
| worker do `AmqpPool` | é o **mesmo pool** que roda os atores das filas — handler lento starva o ator |
| thread monitora | põe heartbeat, prazo de Close-Ok, varredura de TTL e reap atrás do handler |
| inline na thread de leitura | trava o processamento de frames daquela conexão, **heartbeat incluso**, até o cliente derrubá-la |

O que muda entre elas é o raio do estrago, não se há estrago. Uma thread notificadora dedicada dá um contrato, uma ordem e uma política de exceção — e como o registro do evento é uma cópia por valor, o assíncrono custa uma cópia, não um risco de ponteiro solto.

### Isto não é log de auditoria

A perda sob pressão é por desenho, não por falta de capricho. Se o seu handler não acompanha a carga, você perde eventos — e o único jeito de saber é olhar o contador:

```pascal
if Broker.EventsDropped > 0 then
  ...; // o observador não viu tudo
```

Para auditoria de verdade, o handler tem de ser rápido (enfileirar num buffer seu e voltar) e você ainda precisa tratar `EventsDropped > 0` como lacuna conhecida. Aumentar o ring adia o problema; não o remove:

```pascal
Broker.EventQueueCapacity := 65536; // só antes do Start
```

## Os tipos de evento

| tipo | quando |
|---|---|
| `seConnectionEstablished` | socket aceito, antes de qualquer byte de AMQP (e antes do handshake TLS) |
| `seConnectionAuthenticated` | SASL PLAIN aceito **e** vhost aberto (o `Open-Ok` saiu) |
| `seConnectionClosed` | a thread de leitura saiu, depois de todo o teardown |
| `seChannelOpened` | `Channel.Open-Ok` enviado |
| `seChannelClosed` | canal fechado — pelo cliente, por erro, ou pelo teardown da conexão |
| `seMessagePublished` | conteúdo remontado e roteado (inclusive quando não achou fila) |
| `seMessagePublishRejected` | recusado por fila cheia com `x-overflow: reject-publish` |
| `seConsumerRegistered` | `Basic.Consume` aceito |
| `seConsumerCancelled` | `Basic.Cancel` de um consumidor existente |
| `seMessageAcked` / `seMessageNacked` / `seMessageRejected` | `Basic.Ack` / `.Nack` / `.Reject` recebido |
| `seMessageEnqueued` | a mensagem entrou numa fila |
| `seMessageDelivered` | saiu da fila para um consumidor (não conta `Basic.Get`) |
| `seMessageExpired` | venceu por TTL (de mensagem ou de fila — o menor vence) |
| `seMessageDeadLettered` | foi republicada num dead-letter exchange |
| `seMessageDropped` | descartada por teto de fila (`x-max-length`/`-bytes`) |
| `seJournalFlushed` | um **lote** do WAL ficou durável (só com `DataDir`) |

### De onde cada evento nasce, e por que isso aparece nos campos

Os eventos vêm de três lugares diferentes do broker, e o que cada um **sabe** é diferente:

| origem | tipos | o que sabe |
|---|---|---|
| thread de leitura da conexão | conexão, canal, consumidor, publish, ack/nack/reject | tudo: conexão, usuário, vhost, canal |
| **ator da fila** (worker do pool) | enqueue, delivered, expired, dead-lettered, dropped | fila e vhost — **não** conexão nem usuário |
| thread do journal | `seJournalFlushed` | o lote: LSN e quantos registros |

O ator vive do outro lado da caixa de comandos, de propósito: é o que permite que ele nunca espere por I/O. Ele não conhece quem publicou nem quem consome, então **evento de origem-ator sai com `ConnectionId = 0` e `RemoteAddr`/`Username` vazios**. Isso não é campo faltando por descuido — é a separação que a arquitetura impõe, e a D34 manda que o não-populado saia zerado em vez de lixo.

Para amarrar uma entrega a uma conexão, junte pelo **`ConsumerTag`**: `seConsumerRegistered` (que nasce na thread de leitura) traz o par tag ↔ conexão, e `seMessageDelivered` traz a tag.

Um evento por **lote** de journal, nunca por registro: o group commit existe porque o `fsync` é caro, e um evento por registro custaria mais que o `fsync` que ele observa.

> O **teto é de 32 tipos** (18 usados). A máscara de assinatura é um `Cardinal` lido sem lock no caminho quente; passar de 32 é mudar o tipo da máscara, e é decisão consciente. A **ordem do enum é API**: tipo novo entra no fim.

## Que campos cada tipo preenche

O registro é **flat** e o emissor sempre parte de um registro zerado, então **o que a linha não lista sai zerado** — string vazia, numérico 0, `False`. Não é "indefinido": é zero, e os testes asseram isso.

Todos os eventos trazem `EventType`, `WallMs`, `TickMs`, `ConnectionId`, `RemoteAddr`, `Username` e `VHost` — com a ressalva de que `seConnectionEstablished` nasce antes do handshake, então nele `Username` e `VHost` ainda estão vazios.

| tipo | além do contexto de conexão |
|---|---|
| `seConnectionEstablished` | — (`Username`/`VHost` vazios: ainda não houve handshake) |
| `seConnectionAuthenticated` | — (é aqui que `Username` e `VHost` passam a valer) |
| `seConnectionClosed` | `Reason` = erro que derrubou a conexão (vazio = fechamento limpo) |
| `seChannelOpened` | `ChannelNumber` |
| `seChannelClosed` | `ChannelNumber`; `Reason` = `connection teardown` quando o canal morreu junto com a conexão, vazio quando foi o cliente que o fechou |
| `seMessagePublished` | `ChannelNumber`, `ExchangeName`, `RoutingKey`, `MessageSize`, `Mandatory`, `Lsn`; `Reason` = `unroutable` se não achou fila |
| `seMessagePublishRejected` | `ChannelNumber`, `ExchangeName`, `RoutingKey`, `MessageSize`, `Mandatory`; `Reason` = `reject-publish` |
| `seConsumerRegistered` | `ChannelNumber`, `QueueName`, `ConsumerTag` |
| `seConsumerCancelled` | `ChannelNumber`, `QueueName`, `ConsumerTag` |
| `seMessageAcked` | `ChannelNumber`, `DeliveryTag`, `Multiple`, `Count` (quantas entregas o ack resolveu) |
| `seMessageNacked` | idem, mais `Requeue` |
| `seMessageRejected` | `ChannelNumber`, `DeliveryTag`, `Requeue`, `Count` |
| `seMessageEnqueued` | `QueueName`, `MessageSize`, `Priority`, `Redelivered`, `ExchangeName`, `RoutingKey` |
| `seMessageDelivered` | idem, mais `ConsumerTag` e `DeliveryTag`; `Reason` = `no-ack` quando o consumidor não confirma |
| `seMessageExpired` | `QueueName`, `MessageSize`, `Priority`, `ExchangeName`, `RoutingKey`; `Reason` = `expired` |
| `seMessageDeadLettered` | `QueueName` (a de **origem**), `MessageSize`, `Priority`; `ExchangeName`/`RoutingKey` = para **onde foi** (o DLX); `Reason` = a razão do `x-death` |
| `seMessageDropped` | `QueueName`, `MessageSize`, `Priority`, `ExchangeName`, `RoutingKey`; `Reason` = `maxlen` |
| `seJournalFlushed` | `Lsn` (marca d'água que ficou durável) e `Count` (registros no lote). Sem fila, sem conexão |

Nos cinco de origem-ator, `VHost` e `QueueName` valem e o contexto de conexão sai zerado — ver a seção anterior.

Duas amarras que valem para todos:

- **o corpo da mensagem nunca entra no evento**, só `MessageSize`. Carregá-lo poria uma cópia de buffer no caminho mais quente que existe;
- **`WallMs` é epoch em ms UTC** (`Int64`), não `TDateTime` — para correlacionar com log externo. `TickMs` é monotônico e serve para medir latência; ele **não** sobrevive a um restart.

**Morrer no DLX não é o mesmo que ser descartada.** Uma mensagem que vence, ou que é empurrada para fora pelo teto, emite primeiro o evento do motivo (`seMessageExpired` ou `seMessageDropped`) e, **se a fila tiver um dead-letter exchange e a republicação der certo**, emite também `seMessageDeadLettered`. Sem DLX — ou com uma republicação que falhou — só o primeiro sai. Contar `seMessageDeadLettered` para medir perda subestima; contar `seMessageExpired` + `seMessageDropped` mede o que de fato saiu da fila por conta própria.

**Sem rota não é publish recusado.** Uma mensagem que não achou fila é um publish bem-sucedido: leva `ack`, e leva `Basic.Return` se foi `mandatory`. Ela sai como `seMessagePublished` com `Reason = 'unroutable'`. `seMessagePublishRejected` é outra coisa — fila cheia declarada com `x-overflow: reject-publish`, que leva `Basic.Nack`. Contar as duas juntas dá um número que não significa nada.

## Armadilhas

**Desassine antes de destruir.** O handler é um método de objeto — um par (código, instância). Se você liberar a instância sem `Unsubscribe`, a notificadora chama um ponteiro morto: vira `EAccessViolation`, que é capturada e contada, mas o evento se perde e o sintoma aparece longe da causa.

```pascal
Broker.Unsubscribe(Log.AoEvento);
Log.Free;
```

**Não chame `DrainEvents` de dentro de um handler.** Ele espera o handler corrente terminar — e esse handler seria você.

**Exceção no handler não sobe.** Ela é capturada, contada em `EventsFailed`, registrada em `LastEventFailure`, e o laço segue para o próximo handler e para o próximo evento. A notificadora nunca morre por causa de um assinante. Se o seu handler pode falhar, verifique o contador:

```pascal
if Broker.EventsFailed > 0 then
  Writeln('handler falhou: ', Broker.LastEventFailure);
```

**Não é preciso lock dentro do handler** para estado que só ele toca: a notificadora é uma thread só, então o handler nunca roda concorrente consigo mesmo. Estado compartilhado com *outras* threads da sua app, aí sim, é sua responsabilidade.

## Em teste: espere pela condição, não pelo relógio

A entrega é assíncrona, então `Publish` seguido de "o evento chegou?" é uma corrida. Duas barreiras, e a escolha entre elas importa:

- para asserir **presença**, espere o evento (o seu sink de captura conta e sinaliza). `DrainEvents` sozinho **não serve**: ele espera o ring esvaziar, e um ring que ainda não recebeu o evento já está vazio — publish do cliente não tem round-trip, então drenar logo depois dele devolve `True` antes de o broker ter lido o frame;
- para asserir **ausência**, `DrainEvents` é a barreira certa, precedida de uma operação **com** round-trip (um `Channel.Close`, por exemplo): quando ela volta, o broker já passou por todos os pontos de emissão.

`tests\Server\AMQP.ServerEventsTests.pas` (e o espelho FPCUnit) mostram as duas formas.

## Referência da API

| membro de `TAMQPServer` | o que faz |
|---|---|
| `Subscribe(AHandler)` | assina todos os tipos |
| `Subscribe(AHandler, ATypes)` | assina os tipos do conjunto; re-assinar substitui a máscara |
| `Unsubscribe(AHandler)` | remove — obrigatório antes de destruir o dono do método |
| `EventQueueCapacity` | tamanho do ring, em eventos (default 4096). Só antes do `Start` |
| `DrainEvents(ATimeoutMs)` | espera o ring esvaziar e o handler corrente terminar. `False` = estourou o prazo |
| `EventsEmitted` | quantos entraram no ring |
| `EventsDropped` | quantos foram descartados por ring cheio |
| `EventsFailed` | quantas exceções de handler foram capturadas |
| `LastEventFailure` | classe e mensagem da última (`''` se nenhuma) |

Funções de `AMQP.Server.Events`: `AmqpNewEvent` (registro zerado com tipo e relógios — use se for emitir eventos seus), `AmqpEventMask` (conjunto → máscara) e `AmqpEventTypeName` (nome curto do tipo, para log).
