UTF-8
# Observabilidade do Broker (Fase 4.1)

## Visão geral

A partir da Fase 4.1, o broker AMQP embarcado fornece um **sistema de eventos estruturado** para capturar tudo que acontece internamente. Essencial para:

- **Auditoria**: rastrear quem publicou, quem consumiu, quando, de onde
- **Debugging**: diagnosticar problemas sem quebrar a production
- **Compliance**: atender requisitos regulatórios (rastreabilidade de operações)
- **Observabilidade em tempo real**: métricas, alertas, correlação de eventos

## Arquitetura

O sistema é **observer pattern** sem bloquear:

1. Você registra um **handler** (procedure com assinatura `TAMQPServerEventHandler`)
2. O broker dispara **eventos estruturados** (`TAMQPServerEvent`) em pontos críticos
3. O handler roda **fora de locks internos** — nunca trava o engine
4. A ordem dos eventos é **preservada por thread de origem** (thread-safe por conexão)

### Thread-safety

- `Subscribe`/`Unsubscribe` são thread-safe (usam lock interno)
- Handlers são chamados **fora do lock de subscribers** — vários handlers podem rodar em paralelo
- Um handler que levanta exceção não afeta os outros

### Zero overhead quando desativado

Sem nenhum subscriber registrado, o broker tem **zero overhead**. O `NotifyEvent` é inlined e rápido em runtime.

## Tipos de evento

```pascal
type
  TAMQPServerEventType = (
    { Ciclo de vida da conexão }
    seConnectionEstablished,    { Nova conexão TCP aceita }
    seConnectionAuthenticated,  { SASL OK }
    seConnectionClosed,         { Conexão fechada }

    { Ciclo de vida do canal }
    seChannelOpened,
    seChannelClosed,

    { Publish }
    seMessagePublished,         { Mensagem chegou via Basic.Publish }
    seMessagePublishRejected,   { Publish recusado (fila cheia, sem rota) }

    { Enqueue }
    seMessageEnqueued,          { Mensagem entrou em fila }

    { Consume }
    seConsumerRegistered,       { Basic.Consume OK }
    seConsumerCancelled,        { Basic.Cancel OK }
    seMessageDelivered,         { Mensagem saiu da fila pro consumer }

    { Reconhecimento }
    seMessageAcked,             { Basic.Ack recebido }
    seMessageNacked,            { Basic.Nack recebido }
    seMessageRejected,          { Basic.Reject recebido }

    { Ciclo de vida especial }
    seMessageExpired,           { TTL expirou }
    seMessageDeadLettered,      { Entrou em DLX }
    seMessageDropped,           { Descarte por teto }

    { Durabilidade }
    seJournalFlushed            { Lote fsync'd (Fase 4) }
  );
```

## Campos de `TAMQPServerEvent`

```pascal
type
  TAMQPServerEvent = record
    EventType: TAMQPServerEventType;

    { Relógio }
    WallTime: TDateTime;   { Hora UTC (para correlação com logs externos) }
    TickMs: Int64;         { Monotônico em ms (para latência intra-broker) }

    { Contexto de conexão }
    ConnectionId: UInt64;  { Identificador único }
    RemoteAddr: string;    { IP:porta do cliente }
    Username: string;      { Quem se autenticou }
    VHost: string;         { Virtual host }

    { Contexto de canal }
    ChannelNumber: Word;   { 0 = conexão; N = canal específico }

    { Contexto de fila/mensagem }
    QueueName: string;
    ConsumerTag: string;
    DeliveryTag: UInt64;   { Numbering para ack/nack }
    ExchangeName: string;
    RoutingKey: string;

    { Dados da mensagem }
    MessageSize: UInt64;
    MessagePriority: Byte;
    Redelivered: Boolean;
    Mandatory: Boolean;

    { TTL (em ms; 0 = sem limite) }
    MessageExpirationMs: UInt32;
    QueueExpirationMs: UInt32;

    { Correlação }
    CorrelationId: string;
    ReplyTo: string;

    { Razão do evento (quando aplicável) }
    Reason: string;

    { Durabilidade: LSN do último write }
    JournalLsn: UInt64;
  end;
```

## Uso básico

```pascal
procedure TMyApp.OnAMQPEvent(const Event: TAMQPServerEvent);
begin
  WriteLn(Format('%s: %s',
    [DateTimeToStr(Event.WallTime),
     GetEnumName(TypeInfo(TAMQPServerEventType), Ord(Event.EventType))]));
end;

procedure TMyApp.Start;
begin
  FServer := TAMQPServer.Create;
  FServer.Subscribe(OnAMQPEvent);  { Registra ANTES do Start }
  FServer.Start;
end;

procedure TMyApp.Stop;
begin
  if FServer.Running then
  begin
    FServer.Stop;
    FServer.Unsubscribe(OnAMQPEvent);  { Opcional: pode unsubscribe antes }
  end;
end;
```

## Caso de uso: Auditoria de combustível

Para o sistema de abastecidas em postos:

```pascal
procedure TAuditLog.OnAMQPEvent(const Event: TAMQPServerEvent);
begin
  case Event.EventType of
    seMessagePublished:
      LogSQL('INSERT INTO audit_log (timestamp, event, pdv, queue, size, user) '
        + 'VALUES (?, ?, ?, ?, ?, ?)',
        [Event.WallTime, 'PUBLISH', Event.RemoteAddr, Event.QueueName,
         Event.MessageSize, Event.Username]);

    seMessageDelivered:
      begin
        LogSQL('INSERT INTO audit_log ... VALUES (?, ?, ?, ?, ?, ?)',
          [Event.WallTime, 'DELIVER', Event.ConsumerTag, Event.QueueName, 0,
           '']);
        { Marca que esta abastecida foi consumida por este PDV. }
        FConsumedBy[Event.DeliveryTag] := Event.ConsumerTag;
      end;

    seMessageAcked:
      begin
        LogSQL('INSERT INTO audit_log ... VALUES (?, ?, ?, ?, ?, ?)',
          [Event.WallTime, 'ACK', FConsumedBy[Event.DeliveryTag],
           Event.QueueName, 0, '']);
        { A abastecida foi confirmada — não pode ser reutilizada. }
        FAbastecidaUsada[Event.DeliveryTag] := True;
      end;

    seMessageNacked:
      begin
        LogSQL('INSERT INTO audit_log ... VALUES (?, ?, ?, ?, ?, ?)',
          [Event.WallTime, 'NACK', FConsumedBy[Event.DeliveryTag],
           Event.QueueName, 0, Event.Reason]);
        { A abastecida volta disponível. }
        FConsumedBy.Remove(Event.DeliveryTag);
      end;

    seMessageExpired:
      Alert(Format('Abastecida expirou sem confirma: %s', [Event.QueueName]));

    seConnectionClosed:
      Alert(Format('PDV desconectou abruptamente: %s', [Event.RemoteAddr]));
  end;
end;
```

## Padrões comuns

### 1. Logging estruturado

Escrever em arquivo ou banco de dados:

```pascal
procedure TLogger.OnEvent(const Event: TAMQPServerEvent);
var
  LJson: string;
begin
  { Serializar para JSON ou formato estruturado. }
  LJson := Format(
    '{"timestamp":"%s","event":"%s","connection":"%s","queue":"%s",'
    + '"user":"%s","delivery_tag":%d}',
    [FormatDateTime('yyyy-mm-dd hh:mm:ss.zzz', Event.WallTime),
     GetEnumName(TypeInfo(TAMQPServerEventType), Ord(Event.EventType)),
     Event.ConnectionId, Event.QueueName, Event.Username, Event.DeliveryTag]);
  AppendToLogFile('/var/log/broker.log', LJson);
end;
```

### 2. Rastreamento de latência

Medir tempo entre eventos para diagnosticar gargalos:

```pascal
procedure TLatencyTracker.OnEvent(const Event: TAMQPServerEvent);
begin
  case Event.EventType of
    seMessagePublished:
      FPublishTime[Event.DeliveryTag] := Event.TickMs;

    seMessageDelivered:
      begin
        if FPublishTime.ContainsKey(Event.DeliveryTag) then
        begin
          LDelta := Event.TickMs - FPublishTime[Event.DeliveryTag];
          WriteLn(Format('Latencia publish -> deliver: %d ms', [LDelta]));
        end;
      end;
  end;
end;
```

### 3. Detecção de anomalias

Identificar padrões estranhos:

```pascal
procedure TAnomalyDetector.OnEvent(const Event: TAMQPServerEvent);
begin
  if Event.EventType = seMessageNacked then
  begin
    Inc(FNackCount[Event.ConsumerTag]);
    if FNackCount[Event.ConsumerTag] > 10 then
      Alert(Format('Consumidor %s rejeitando muitas mensagens',
        [Event.ConsumerTag]));
  end;

  if Event.EventType = seMessageDelivered then
    if Event.Redelivered then
      Inc(FRedeliveryCount[Event.QueueName]);

  if Event.EventType = seMessageExpired then
  begin
    Alert(Format('Mensagem em %s expirou — aumentar TTL?',
      [Event.QueueName]));
  end;
end;
```

### 4. Métricas em tempo real

Coletar estatísticas:

```pascal
procedure TMetrics.OnEvent(const Event: TAMQPServerEvent);
begin
  case Event.EventType of
    seMessagePublished:
      begin
        Inc(FTotalPublished);
        FTotalBytes := FTotalBytes + Event.MessageSize;
      end;

    seMessageAcked:
      Inc(FTotalAcked);

    seMessageNacked, seMessageRejected:
      Inc(FTotalNacked);

    seMessageDropped:
      begin
        Inc(FDropped);
        Alert(Format('Mensagem descartada por teto: %s', [Event.Reason]));
      end;
  end;
end;

procedure TMetrics.PrintStats;
begin
  WriteLn(Format(
    'Total published: %d | Acked: %d | Nacked: %d | Dropped: %d | Bytes: %d',
    [FTotalPublished, FTotalAcked, FTotalNacked, FDropped, FTotalBytes]));
end;
```

## Performance

- **Sem subscribers**: zero overhead (a chamada `NotifyEvent` é inlined)
- **Com 1 subscriber**: ~1–2 µs por evento (snapshot de handlers + uma chamada de procedure)
- **Com 10 subscribers**: ~10–20 µs (linear com número de handlers)
- **Handler lento**: não trava o engine (rodam fora do lock)

## Limitações atuais (Fase 4.1)

1. Os eventos disparam **na thread de origem** (thread de leitura da conexão, thread do ator da fila, thread de journal). Você é responsável por sincronização se o handler compartilhar estado.

2. **Sem filtragem built-in**: você recebe TODOS os eventos. Filter inside your handler:

   ```pascal
   if Event.EventType <> seJournalFlushed then  { skip boring events }
     LogEvent(Event);
   ```

3. **Sem buffering**: cada evento é despachado immediately. Se seu handler for muito lento, ele bloqueia a thread de origem. Use uma fila assíncrona:

   ```pascal
   procedure TAsyncLogger.OnEvent(const Event: TAMQPServerEvent);
   begin
     FQueue.Enqueue(Event);  { post async, handler sai rápido }
   end;
   ```

4. **Sem persistência integrada**: você é responsável por armazenar. Use banco de dados, arquivo, Elasticsearch, etc.

## Roadmap futuro

- Fase 4.2: Filtros built-in (só observar certos tipos de evento ou filas)
- Fase 4.3: Ring buffer circular de últimos N eventos (sem I/O)
- Fase 4.4: Integração com OpenTelemetry (traces distribuídos)

## Veja também

- `AMQP.Server.Events` — unit com os tipos
- `samples\Server\ObservabilityExample.pas` — exemplo completo
- `tests\Server\AMQP.Server.Events.Test.pas` — testes unitários
- `CLAUDE.md` — decisões travadas (D19–D28, Fase 4)
