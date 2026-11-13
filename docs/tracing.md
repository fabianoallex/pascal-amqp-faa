# Tracing (OpenTelemetry) in the client

Phase D of pascal-api-infra-faa's observability design (`docs/observability-design.md`
there): the sibling libs open spans that join the application's trace without depending on the
API lib, using the contracts that went into pascal-common-faa (`PascalCommon.Tracing`,
1.7/1.8). pascal-db-faa (0.13.0) and pascal-redis-faa (0.2.0) did their part before;
this is AMQP's. It landed in **v0.2.0** (2026-10-12).

## Usage

The application turns the tracer on once, with whatever exporter it wants (pascal-api-infra-faa's
`TOtlpHttpExporter`, or any `IPcSpanExporter`), and the lib does the rest:

```pascal
uses PascalCommon.Tracing;

TPcTracing.Start(TPcTracingOptions.FromEnvironment('orders', '1.0.0'), MyExporter);
...
TPcTracing.Shutdown; // before the program ends, to export the last spans
```

| Where | Span | Kind | Parent |
|---|---|---|---|
| `Publish` / `PublishText` | `publish <exchange>` (`publish amq.default` on the default exchange) | producer | the thread's current span |
| `Consume` callback | `process <queue>` | consumer | the message's `traceparent` |
| `BasicGet` | `receive <queue>` | client | the thread's current span |

The publish span's `traceparent` (and `tracestate`, when there is one) goes in the **message
headers**, as a longstr. On the other side, the consumer callback runs with the `process` span
as the current span, so whatever it does (a query, a Redis command, another publish) joins
the same trace. An HTTP request that publishes and a worker that consumes show up as **a single
trace**: `GET /orders` → `publish amq.default` → `process orders` → ...

Whoever processes the result of a `BasicGet` in the producer's trace opens the span with the
message properties:

```pascal
R := Ch.BasicGet('orders', False);
if R.Found then
begin
  S := AmqpStartProcessSpan('orders', R.Properties, R.RoutingKey, Length(R.Body),
    R.DeliveryTag, R.Redelivered);
  try
    Process(R);
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

Attributes (OpenTelemetry messaging semantic conventions): `messaging.system`
(`rabbitmq`), `messaging.operation.type` (`send`/`process`/`receive`),
`messaging.operation.name`, `messaging.destination.name` (the exchange on publish, the queue on
consume), `messaging.rabbitmq.destination.routing_key`, `messaging.message.body.size`,
`messaging.message.id` (the `MessageId`), `messaging.message.conversation_id` (the
`CorrelationId`), `messaging.rabbitmq.message.delivery_tag`, `server.address`, `server.port`,
`messaging.destination.temporary` (`amq.gen-...` queue). The lib's own, with a prefix:
`pascalamqp.publish.sequence_number` (the confirm seq-no), `pascalamqp.message.redelivered`
and `pascalamqp.get.empty`. A callback that raises leaves `process` with error status, the
message and `error.type` (the class). The channel's `OnConsumerError` runs with `process` still
current, so the handler's log carries the message's trace id; the span ends after it and after the
`Nack` (up to 0.4.0 it closed before, and the handler ran without a current span).

## Decisions (2026-10-12)

The first three were the user's; the rest, as recommended.

1. **Only the client opens spans; the embedded broker does not.** The broker passes the content
   header through byte for byte (D1), so the `traceparent` crosses intact, including on dead-letter,
   which rewrites the header (`x-death`) and keeps the rest (tested). A span in the broker would run
   into D2/D30 (no extra work in the actor or in the read thread) and the broker already has the
   Phase 4.1 events for its own observability. Left as future work.
2. **Automatic injection, without overwriting.** With the tracer on, every publish carries its
   span's context. If the application's header table already has `traceparent`, the application's
   wins: nothing is added (not even `tracestate`). A loose `tracestate` in the table, without
   `traceparent`, stays the caller's, because a field-table does not allow a repeated name.
3. **The parent of `process` is the message's `traceparent`** (`StartSpanFromParent`). Without a
   valid one, `process` is the root of a new trace. pascal-common-faa has no span links, and
   OpenTelemetry accepts the creation context as parent when processing a single
   message.
4. **Injection happens at encoding; the caller's table is never changed.**
   `BuildContentHeader` gained an overload that writes the pairs at the end of the field-table, only
   in the bytes (`TAMQPWriter.WriteFieldTable` with extra pairs). `Publish` takes `const AProps`
   with a table that belongs to the caller, who may reuse it across several publishes: touching
   it would be a side effect, and cloning it would cost a `TValue` dictionary per publish.
5. **The publish ends when the frames go out, not on the confirm.** The confirm arrives on another
   thread (the read thread, and the callback on the pool), and a span that became the current one
   can only end on the thread that opened it (the threading rule of `PascalCommon.Tracing`). The
   seq-no goes in an attribute, which lets someone match the span with `OnConfirm` if they want.
6. **`process` starts and ends in the delivery work item**, on the pool thread, around the
   callback and around `OnConsumerError`, when the callback raises. It does not measure the time
   until the ack (the ack may come later, from another thread, or never, with `noAck`).
   Ack/nack/reject, `Basic.Return`, confirms and declares stay out of the trace.
7. **`Basic.Get` is `receive`, child of the current span**: the pull is part of the caller's
   work. Linking it to the producer's trace is the decision of whoever processes it (the
   `AmqpStartProcessSpan` above), because only the application knows whether that Get is a "processing".
8. **Low-cardinality span names.** A broker-generated queue (`amq.gen-...`) becomes
   `(temporary)` in the name and goes whole into `messaging.destination.name`. The routing key, which
   on a topic exchange may have high cardinality, goes only in the attribute. The queue name of
   `process` comes from a consumer-tag → queue map kept by the channel (`Basic.Deliver` does not
   carry the queue).
9. **Off, zero cost.** With `TPcTracing.Enabled` false (lock-free read), no
   `AMQP.Tracing` function creates spans, ids or attributes, and the publish builds the header the
   usual way, with the same bytes (tested).
10. **Resending after a reconnection does not open a span.** `ResendUnconfirmed` resends the stored
    bytes, which already carry the original publish's `traceparent`.
11. **The minimum pascal-common-faa version goes up to 1.8.0.** 1.7 already had what is used
    here, but 1.8 is what the sibling libs require, so an application does not end up with two
    different requirements for the same package (the same choice pascal-redis-faa made).

## Tests

- `tests/Unit/*/AMQP.TracingTests.pas` (10 tests, no broker): injection into the bytes, the
  caller's table intact, the application's `traceparent` winning, `tracestate` without a repeated
  name, reading the context, the destination names, nothing opened with the tracer off.
- `tests/Server/*/AMQP.ServerTracingTests.pas` (9 tests): the real client against the embedded
  broker, with an exporter that only stores the spans. The `parent → publish → process` chain
  in a single trace, the header seen by the consumer, the application's `traceparent`, a publish
  made inside the callback as a child of `process`, a callback that raises, the Get's `receive`
  (full and empty), dead-letter preserving the `traceparent`, the generated queue and the tracer
  off with no header. Mutation: removing the injection breaks 4 of the 9.
- Also measured against RabbitMQ 3.13.7 (a probe in FPC/Linux, outside the suite): the
  consumer's `process` came out as a child of `publish`, in the server span's trace, with the
  received header equal to the publish context.
