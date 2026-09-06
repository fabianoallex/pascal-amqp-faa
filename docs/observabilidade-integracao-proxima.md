UTF-8
# Próximas Etapas: Integração de NotifyEvent (Fase 4.2)

A Fase 4.1 fornece o framework de observabilidade. Agora é preciso **disparar eventos reais** nos 5 pontos críticos do broker.

## Ponto 1: Conexão estabelecida

**Arquivo**: `AMQP.Server.Connection.pas`

**Método**: `TAMQPServerConnection.RunReadLoop` (logo após o protocol-header ser recebido)

**Evento**: `seConnectionEstablished`

```pascal
// Em RunReadLoop, após ler o protocol-header com sucesso:
var
  LEvent: TAMQPServerEvent;
begin
  LEvent := Default(TAMQPServerEvent);
  LEvent.EventType := seConnectionEstablished;
  LEvent.WallTime := Now;
  LEvent.TickMs := AmqpTickMs;
  LEvent.ConnectionId := FConnId;
  LEvent.RemoteAddr := FPeer;
  LEvent.ChannelNumber := 0;  { conexão }
  LEvent.Reason := 'Protocol header received';
  
  { Precisa de referência ao broker para chamar NotifyEvent. }
  if FConfig.EventSink <> nil then
    FConfig.EventSink.NotifyEvent(LEvent);
end;
```

## Ponto 2: Autenticação OK

**Arquivo**: `AMQP.Server.Connection.pas`

**Método**: `TAMQPServerConnection.RunReadLoop` (após `Connection.Open-Ok` ser enviado)

**Evento**: `seConnectionAuthenticated`

```pascal
LEvent := Default(TAMQPServerEvent);
LEvent.EventType := seConnectionAuthenticated;
LEvent.WallTime := Now;
LEvent.TickMs := AmqpTickMs;
LEvent.ConnectionId := FConnId;
LEvent.RemoteAddr := FPeer;
LEvent.Username := FUserId;
LEvent.VHost := FVirtualHost;
LEvent.ChannelNumber := 0;
LEvent.Reason := 'Connection.Open-Ok sent';

if FConfig.EventSink <> nil then
  FConfig.EventSink.NotifyEvent(LEvent);
```

## Ponto 3: Publish OK

**Arquivo**: `AMQP.Server.Channel.pas`

**Método**: `IAMQPMessageSink.RouteMessage` (após o routing ter aceitado a mensagem)

**Evento**: `seMessagePublished` ou `seMessagePublishRejected`

```pascal
procedure TEngine.RouteMessage(const AMessage: TAMQPServerMessage;
  out ARejeitada, ALsn: UInt64): Boolean;
var
  LEvent: TAMQPServerEvent;
begin
  // ... routing logic ...
  
  LEvent := Default(TAMQPServerEvent);
  LEvent.WallTime := Now;
  LEvent.TickMs := AmqpTickMs;
  LEvent.ExchangeName := AMessage.Exchange;
  LEvent.RoutingKey := AMessage.RoutingKey;
  LEvent.MessageSize := Length(AMessage.Body);
  LEvent.Mandatory := AMessage.Mandatory;
  LEvent.MessagePriority := AMessage.Properties.Priority;
  LEvent.CorrelationId := AMessage.Properties.CorrelationId;
  LEvent.ReplyTo := AMessage.Properties.ReplyTo;
  LEvent.MessageExpirationMs := AMessage.Properties.Expiration;
  
  if Result then
  begin
    LEvent.EventType := seMessagePublished;
    LEvent.Reason := Format('Routed to %d queue(s)', [routed_count]);
  end
  else if ARejeitada then
  begin
    LEvent.EventType := seMessagePublishRejected;
    LEvent.Reason := 'Queue full (x-overflow: reject-publish)';
  end
  else
  begin
    LEvent.EventType := seMessagePublishRejected;
    LEvent.Reason := 'No matching route';
  end;
  
  NotifyEvent(LEvent);  { Engine já tem acesso ao EventSink }
end;
```

## Ponto 4: Enqueue OK

**Arquivo**: `AMQP.Server.Queue.pas`

**Método**: `TAMQPServerQueue.Enqueue` (após a mensagem ser adicionada à fila)

**Evento**: `seMessageEnqueued` ou `seMessageDropped`

```pascal
procedure TAMQPServerQueue.Enqueue(const AMessage: TAMQPMessage; 
  const ADeliveryId: NativeUInt; const ADeliveryTag: UInt64);
var
  LEvent: TAMQPServerEvent;
begin
  // ... enqueue logic ...
  
  LEvent := Default(TAMQPServerEvent);
  LEvent.EventType := seMessageEnqueued;
  LEvent.WallTime := Now;
  LEvent.TickMs := AmqpTickMs;
  LEvent.QueueName := Self.Name;
  LEvent.MessageSize := Length(AMessage.Body);
  LEvent.DeliveryTag := ADeliveryTag;
  LEvent.MessagePriority := AMessage.Priority;
  LEvent.MessageExpirationMs := AMessage.EffectiveTtlMs;
  
  { Se descartou por teto: }
  if WasDropped then
  begin
    LEvent.EventType := seMessageDropped;
    LEvent.Reason := Format('Queue full: %d/%d messages', [Count, MaxLength]);
  end;
  
  NotifyEvent(LEvent);
end;
```

## Ponto 5: Deliver OK

**Arquivo**: `AMQP.Server.Queue.pas`

**Método**: `TAMQPServerQueue.TryDeliver` (após enviar para o consumer)

**Evento**: `seMessageDelivered`, `seMessageExpired`, `seMessageDeadLettered`

```pascal
function TAMQPServerQueue.TryDeliver(ATarget: IAMQPDeliveryTarget): Boolean;
var
  LEvent: TAMQPServerEvent;
  LMessage: TAMQPMessage;
begin
  // ... delivery logic ...
  
  LEvent := Default(TAMQPServerEvent);
  LEvent.WallTime := Now;
  LEvent.TickMs := AmqpTickMs;
  LEvent.QueueName := Self.Name;
  LEvent.DeliveryTag := CurrentTag;
  LEvent.MessageSize := Length(LMessage.Body);
  LEvent.Redelivered := LMessage.Redelivered;
  
  if IsExpired then
  begin
    LEvent.EventType := seMessageExpired;
    LEvent.MessageExpirationMs := LMessage.EffectiveTtlMs;
    LEvent.Reason := Format('TTL exceeded: %d ms', [Elapsed]);
  end
  else if IsGoingToDLX then
  begin
    LEvent.EventType := seMessageDeadLettered;
    LEvent.Reason := Format('Dead-lettered: %s', [DLXReason]);
  end
  else
  begin
    LEvent.EventType := seMessageDelivered;
    { ConsumerTag vem de ATarget? Precisa de investigação. }
  end;
  
  NotifyEvent(LEvent);
  Result := Delivered;
end;
```

## Ponto 6: Ack/Nack

**Arquivo**: `AMQP.Server.Connection.pas`

**Método**: `TAMQPServerConnection.ProcessBasicAck` (após executar o ack)

**Evento**: `seMessageAcked`, `seMessageNacked`, `seMessageRejected`

```pascal
procedure ProcessBasicAck(const AMethod: TAMQPBasicAck; AChannel: Word);
var
  LEvent: TAMQPServerEvent;
begin
  // ... ack logic ...
  
  LEvent := Default(TAMQPServerEvent);
  LEvent.WallTime := Now;
  LEvent.TickMs := AmqpTickMs;
  LEvent.ConnectionId := FConnId;
  LEvent.ChannelNumber := AChannel;
  LEvent.DeliveryTag := AMethod.DeliveryTag;
  
  if AIsReject then
    LEvent.EventType := seMessageRejected
  else if ANack then
    LEvent.EventType := seMessageNacked
  else
    LEvent.EventType := seMessageAcked;
  
  LEvent.Reason := IntToStr(AMethod.DeliveryTag);
  
  NotifyEvent(LEvent);
end;
```

## Ponto 7: Journal Flushed

**Arquivo**: `AMQP.Server.Journal.pas` ou `AMQP.Server.Confirm.pas`

**Método**: `IAMQPDurabilitySink.Release` (após fsync bem-sucedido)

**Evento**: `seJournalFlushed`

```pascal
procedure TConfirmRegistry.Release(AMarca: UInt64);
var
  LEvent: TAMQPServerEvent;
begin
  // ... confirm release logic ...
  
  LEvent := Default(TAMQPServerEvent);
  LEvent.EventType := seJournalFlushed;
  LEvent.WallTime := Now;
  LEvent.TickMs := AmqpTickMs;
  LEvent.JournalLsn := AMarca;
  LEvent.Reason := Format('Durability watermark advanced to LSN %d', [AMarca]);
  
  NotifyEvent(LEvent);
end;
```

## Arquitetura de notificação

Para evitar ciclo de dependências e manter a separação de camadas:

1. **EventSink no ConnConfig**: adicionar `EventSink: TAMQPServer` (referência fraca, ou a interface)
2. **Ou**: padrão inverse — a Engine/Queue/Connection chamam um callback registrado
3. **Melhor ainda**: criar `IAMQPEventNotifier` + implementação no Broker

### Opção recomendada: Interface `IAMQPEventNotifier`

```pascal
{ em AMQP.Server.Types }
type
  IAMQPEventNotifier = interface
    ['{5A0F6E31-1C5D-4E8B-9F27-6D3B0A5C4E19}']
    procedure NotifyEvent(const AEvent: TAMQPServerEvent);
  end;

{ em TAMQPServerConnConfig }
type
  TAMQPServerConnConfig = record
    // ... existing fields ...
    EventNotifier: IAMQPEventNotifier;  { nil = sem observabilidade }
  end;
```

Assim:
- Broker implementa a interface
- Passa para cada conexão via ConnConfig
- Conexão/Engine/Queue chamar `if Config.EventNotifier <> nil then ...`
- Zero overhead quando nil
- Sem ciclo de unidades

## Checklist de integração

- [ ] Adicionar `IAMQPEventNotifier` a `AMQP.Server.Types`
- [ ] Adicionar campo `EventNotifier` a `TAMQPServerConnConfig`
- [ ] Adicionar campo `FEventNotifier` a `TAMQPServer`, implementar interface
- [ ] Preencher `EventNotifier` em `TAMQPServer.ConnConfig`
- [ ] Adicionar `seConnectionEstablished` em `TAMQPServerConnection`
- [ ] Adicionar `seConnectionAuthenticated` em `TAMQPServerConnection`
- [ ] Adicionar `seMessagePublished` + `seMessagePublishRejected` em `TAMQPEngine`
- [ ] Adicionar `seMessageEnqueued` + `seMessageDropped` em `TAMQPServerQueue`
- [ ] Adicionar `seMessageDelivered` em `TAMQPServerQueue.TryDeliver`
- [ ] Adicionar `seMessageExpired` em `TAMQPServerQueue` (expiração)
- [ ] Adicionar `seMessageDeadLettered` em `TAMQPServerQueue` (DLX)
- [ ] Adicionar `seMessageAcked` + `seMessageNacked` + `seMessageRejected` em Connection
- [ ] Adicionar `seJournalFlushed` em Journal/Confirm (Fase 4)
- [ ] Adicionar `seConnectionClosed` em `TAMQPServerConnection.RunReadLoop` (saída)
- [ ] Testar com `ObservabilityExample.pas`
- [ ] Atualizar `docs/observabilidade.md` com eventos reais

## Performance esperada

Após integração completa:
- ~1–2 µs por evento (framework + snapshot)
- Zero overhead quando sem subscribers
- ~10 µs com 10 subscribers
- Handler lento não trava engine (rodam fora do lock)

## Backlog de futuras fases

- **Fase 4.3**: Filtros built-in (só observar certos tipos)
- **Fase 4.4**: Ring buffer circular de últimos N eventos
- **Fase 4.5**: OpenTelemetry exporter
- **Fase 5.0**: Integração com observabilidade distribuída (trace parent/baggage)
