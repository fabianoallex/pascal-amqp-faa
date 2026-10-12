# Tracing (OpenTelemetry) no cliente

Fase D do design de observabilidade da pascal-api-infra-faa (`docs/observability-design.md`
de lá): as libs irmãs abrem spans que entram na trace da aplicação sem depender da lib de
API, usando os contratos que foram para a pascal-common-faa (`PascalCommon.Tracing`,
1.7/1.8). A pascal-db-faa (0.13.0) e a pascal-redis-faa (0.2.0) fizeram a parte delas antes;
esta é a do AMQP. Entrou na **v0.2.0** (2026-10-12).

## Uso

A aplicação liga o tracer uma vez, com o exportador que quiser (o `TOtlpHttpExporter` da
pascal-api-infra-faa, ou qualquer `IPcSpanExporter`), e a lib faz o resto:

```pascal
uses PascalCommon.Tracing;

TPcTracing.Start(TPcTracingOptions.FromEnvironment('pedidos', '1.0.0'), MeuExportador);
...
TPcTracing.Shutdown; // antes de o programa terminar, para exportar os últimos spans
```

| Onde | Span | Kind | Pai |
|---|---|---|---|
| `Publish` / `PublishText` | `publish <exchange>` (`publish amq.default` no exchange padrão) | producer | o span corrente da thread |
| callback do `Consume` | `process <fila>` | consumer | o `traceparent` da mensagem |
| `BasicGet` | `receive <fila>` | client | o span corrente da thread |

O `traceparent` (e o `tracestate`, quando houver) do span de publish vai nos **headers da
mensagem**, como longstr. Do outro lado, o callback do consumidor roda com o span `process`
como span corrente, então o que ele fizer (uma query, um comando Redis, outro publish) entra
na mesma trace. Uma requisição HTTP que publica e um worker que consome aparecem como **uma
trace só**: `GET /pedidos` → `publish amq.default` → `process pedidos` → ...

Quem processa o resultado de um `BasicGet` na trace do produtor abre o span com as
propriedades da mensagem:

```pascal
R := Ch.BasicGet('pedidos', False);
if R.Found then
begin
  S := AmqpStartProcessSpan('pedidos', R.Properties, R.RoutingKey, Length(R.Body),
    R.DeliveryTag, R.Redelivered);
  try
    Processa(R);
    AmqpFinishSpan(S, nil);
  except
    on E: Exception do
    begin
      AmqpFinishSpan(S, E);
      raise;
    end;
  end;
end;
```

Atributos (convenções semânticas de mensageria do OpenTelemetry): `messaging.system`
(`rabbitmq`), `messaging.operation.type` (`send`/`process`/`receive`),
`messaging.operation.name`, `messaging.destination.name` (o exchange no publish, a fila no
consumo), `messaging.rabbitmq.destination.routing_key`, `messaging.message.body.size`,
`messaging.message.id` (o `MessageId`), `messaging.message.conversation_id` (o
`CorrelationId`), `messaging.rabbitmq.message.delivery_tag`, `server.address`, `server.port`,
`messaging.destination.temporary` (fila `amq.gen-...`). Próprios da lib, com prefixo:
`pascalamqp.publish.sequence_number` (o seq-no do confirm), `pascalamqp.message.redelivered`
e `pascalamqp.get.empty`. Callback que levanta deixa o `process` com status de erro, a
mensagem e `error.type` (a classe).

## Decisões (2026-10-12)

As três primeiras foram do usuário; as demais, como recomendadas.

1. **Só o cliente abre span; o broker embutido não.** O broker repassa o content header byte
   a byte (D1), então o `traceparent` atravessa intacto, inclusive no dead-letter, que
   reescreve o header (`x-death`) e mantém o resto (testado). Span no broker esbarraria na
   D2/D30 (nada de trabalho a mais no ator ou na thread de leitura) e o broker já tem os
   eventos da Fase 4.1 para a observabilidade dele. Fica como trabalho futuro.
2. **Injeção automática, sem sobrescrever.** Com o tracer ligado, todo publish leva o
   contexto do seu span. Se a tabela de headers da aplicação já tem `traceparent`, o dela
   vence: nada é acrescentado (nem o `tracestate`). Um `tracestate` solto na tabela, sem
   `traceparent`, fica o do chamador, porque field-table não admite nome repetido.
3. **O pai do `process` é o `traceparent` da mensagem** (`StartSpanFromParent`). Sem um
   válido, o `process` é raiz de uma trace nova. A pascal-common-faa não tem links de span, e
   o OpenTelemetry aceita o contexto de criação como pai num processamento de mensagem
   única.
4. **A injeção acontece na codificação; a tabela do chamador nunca é alterada.**
   `BuildContentHeader` ganhou um overload que escreve os pares no fim da field-table, só nos
   bytes (`TAMQPWriter.WriteFieldTable` com pares extras). O `Publish` recebe `const AProps`
   com uma tabela que pertence a quem chamou, que pode reusá-la em vários publishes: mexer
   nela seria efeito colateral, e cloná-la custaria um dicionário de `TValue` por publish.
5. **O publish termina quando os frames saem, não no confirm.** O confirm chega noutra
   thread (a de leitura, e o callback no pool), e um span que virou o corrente só pode
   terminar na thread que o abriu (regra de threads do `PascalCommon.Tracing`). O seq-no vai
   no atributo, o que permite casar o span com o `OnConfirm` se alguém quiser.
6. **O `process` nasce e termina no work item da entrega**, na thread do pool, em volta do
   callback. Não mede o tempo até o ack (o ack pode vir depois, de outra thread, ou nunca, com
   `noAck`). Ack/nack/reject, `Basic.Return`, confirms e declares ficam fora da trace.
7. **`Basic.Get` é `receive`, filho do span corrente**: o pull faz parte do trabalho de quem
   chamou. Ligar à trace do produtor é decisão de quem processa (o `AmqpStartProcessSpan`
   acima), porque só a aplicação sabe se aquele Get é um "processamento".
8. **Nome de span com cardinalidade baixa.** Fila gerada pelo broker (`amq.gen-...`) vira
   `(temporary)` no nome e fica inteira em `messaging.destination.name`. A routing key, que
   num topic pode ter cardinalidade alta, vai só no atributo. O nome da fila do `process` vem
   de um mapa consumer-tag → fila mantido pelo canal (o `Basic.Deliver` não traz a fila).
9. **Desligado, custo zero.** Com `TPcTracing.Enabled` falso (leitura sem lock), nenhuma
   função de `AMQP.Tracing` cria span, ids ou atributos, e o publish monta o header pelo
   caminho de sempre, com os mesmos bytes (testado).
10. **O reenvio pós-reconexão não abre span.** O `ResendUnconfirmed` reenvia os bytes
    guardados, que já carregam o `traceparent` do publish original.
11. **A versão mínima da pascal-common-faa sobe para 1.8.0.** A 1.7 já tinha o que se usa
    aqui, mas é a 1.8 que as libs irmãs exigem, e assim uma aplicação não fica com duas
    exigências diferentes para o mesmo pacote (a mesma escolha da pascal-redis-faa).

## Testes

- `tests/Unit/*/AMQP.TracingTests.pas` (10 testes, sem broker): a injeção nos bytes, a tabela
  do chamador intacta, o `traceparent` da aplicação vencendo, o `tracestate` sem nome
  repetido, a leitura do contexto, os nomes de destino, nada aberto com o tracer desligado.
- `tests/Server/*/AMQP.ServerTracingTests.pas` (9 testes): o cliente real contra o broker
  embutido, com um exportador que só guarda os spans. A cadeia `pai → publish → process`
  numa trace só, o header visto pelo consumidor, o `traceparent` da aplicação, o publish
  feito dentro do callback como filho do `process`, callback que levanta, o `receive` do Get
  (cheio e vazio), o dead-letter preservando o `traceparent`, a fila gerada e o tracer
  desligado sem header. Mutação: tirar a injeção derruba 4 dos 9.
- Medido também contra o RabbitMQ 3.13.7 (uma sonda em FPC/Linux, fora da suíte): o
  `process` do consumidor saiu filho do `publish`, na trace do span de servidor, com o header
  recebido igual ao contexto do publish.
