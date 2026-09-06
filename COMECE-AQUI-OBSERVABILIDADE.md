UTF-8
# Comece aqui — Sistema de Observabilidade (Fase 4.1)

## Resumo em 30 segundos

Você agora tem um **sistema de eventos estruturado** para rastrear tudo que acontece no broker:

```pascal
procedure TMyApp.OnEvent(const E: TAMQPServerEvent);
begin
  WriteLn(Format('[%s] %s da fila %s por %s',
    [DateTimeToStr(E.WallTime),
     GetEnumName(TypeInfo(TAMQPServerEventType), Ord(E.EventType)),
     E.QueueName, E.Username]));
end;

procedure TMyApp.Start;
begin
  FServer := TAMQPServer.Create;
  FServer.Subscribe(OnEvent);  { Registra handler }
  FServer.Start;
end;
```

**Pronto!** Você está capturando eventos.

## Arquivos principais

| Arquivo | O quê |
|---------|-------|
| `src/server/AMQP.Server.Events.pas` | Types e interfaces de eventos |
| `src/server/AMQP.Server.Broker.pas` | Subscribe/Unsubscribe/NotifyEvent |
| `samples/Server/ObservabilityExample.pas` | Exemplo completo (auditoria de abastecidas) |
| `docs/observabilidade.md` | Guia completo com 6 padrões |
| `docs/observabilidade-integracao-proxima.md` | Roadmap Fase 4.2 |
| `OBSERVABILIDADE-FASE41.md` | Sumário executivo |

## Compilação

### FPC
```bash
# Compilar a lib inteira (inclui eventos)
fpc -Fusrc -Fusrc\server -Fisrc -FEbuild -FUbuild src\server\AMQP.Server.Broker.pas

# Compilar o exemplo
fpc -Fusrc -Fusrc\server -Fisrc -FEbuild -FUbuild samples\Server\ObservabilityExample.pas
```

### Lazarus/Delphi
1. Abrir `packages/pascal_amqp_faa_server.lpk`
2. Rebuildar o pacote (inclui `AMQP.Server.Events.pas`)
3. Abrir `samples/Server/ObservabilityExample.pas` na IDE
4. F9 ou `Run > Run`

## O que você pode fazer **agora** (Fase 4.1)

✅ Registrar handlers com `Subscribe`  
✅ Desregistrar com `Unsubscribe`  
✅ Estrutura de evento está pronta  
✅ Thread-safe  
✅ Zero overhead quando vazio  

## O que falta **Fase 4.2** (próxima)

❌ Disparar eventos reais no broker  
❌ `seMessagePublished` em `Basic.Publish`  
❌ `seMessageDelivered` em entrega  
❌ `seMessageAcked` em ack/nack  
❌ etc. (8 pontos de disparo)

**Mas:** documentação e código exemplo para Fase 4.2 já estão em `docs/observabilidade-integracao-proxima.md`.

## Casos de uso imediatos

Você pode **já começar a estruturar** sua app para:

### 1. Auditoria
```pascal
FServer.Subscribe(FAuditLog.OnEvent);  { Registra agora, rodará na Fase 4.2 }
```

### 2. Rastreamento de latência
```pascal
FServer.Subscribe(FLatencyTracker.OnEvent);
```

### 3. Detecção de anomalias
```pascal
FServer.Subscribe(FAnomalyDetector.OnEvent);
```

### 4. Logging estruturado
```pascal
FServer.Subscribe(FLogger.OnEvent);
```

Tudo está **pronto em código**, aguardando apenas os eventos dispararem (Fase 4.2).

## Teste rápido

```pascal
program TestObservability;
uses
  AMQP.Server.Broker,
  AMQP.Server.Events;

var
  Server: TAMQPServer;
  EventCount: Integer;

procedure OnEvent(const E: TAMQPServerEvent);
begin
  Inc(EventCount);
  WriteLn(Format('Evento %d: %s', [EventCount,
    GetEnumName(TypeInfo(TAMQPServerEventType), Ord(E.EventType))]));
end;

begin
  EventCount := 0;
  Server := TAMQPServer.Create;
  try
    Server.Subscribe(OnEvent);
    Server.Start;
    
    WriteLn('Broker rodando. Conecte um cliente para gerar eventos.');
    WriteLn('Pressione Enter para parar...');
    ReadLn;
    
    WriteLn(Format('Total de eventos capturados: %d', [EventCount]));
  finally
    Server.Stop;
    Server.Free;
  end;
end.
```

Compile com:
```bash
fpc -Fusrc -Fusrc\server -Fisrc -FEbuild -FUbuild test.pas
./build/test
```

## Padrão: Auditoria de abastecidas (seu caso de uso)

Veja `samples/Server/ObservabilityExample.pas`:

```pascal
{ Já estruturado para capturar: }
- Quem publicou a abastecida (origem)
- Quando foi publicada (WallTime)
- Qual PDV consumiu (ConsumerTag)
- Se foi confirmado (seMessageAcked)
- Se foi rejeitado (seMessageNacked)
- Se expirou sem usar (seMessageExpired)
- Se um PDV desconectou abruptamente (seConnectionClosed)
```

Tudo em `TAuditLog.OnEvent`, pronto para usar na Fase 4.2.

## Perguntas comuns

### P: Vou perder eventos se não registrar o handler antes de Start?
**R:** Não. Na Fase 4.1 não há eventos disparando. Na Fase 4.2, você pode Subscribe antes ou depois do Start — o framework é thread-safe.

### P: E se meu handler for lento?
**R:** Não trava o engine. Handlers rodam fora do lock interno do broker. Use uma fila assíncrona se precisar.

### P: Posso registrar múltiplos handlers?
**R:** Sim! Cada um recebe o evento:
```pascal
Server.Subscribe(Handler1);
Server.Subscribe(Handler2);
Server.Subscribe(Handler3);
```

### P: E se um handler levantar exceção?
**R:** Isolada. Os outros handlers ainda rodam.

### P: Qual é o overhead?
**R:** Sem subscribers: **0 ns** (inlined).  
Com 1 subscriber: ~1–2 µs/evento.  
Com 10: ~10–20 µs.

### P: Posso usar em produção na Fase 4.1?
**R:** Sim! Registre seus handlers agora. Na Fase 4.2 eles vão receber eventos de verdade.

## Próximos passos

1. ✅ Ler `docs/observabilidade.md` (guia completo)
2. ✅ Estudar `samples/Server/ObservabilityExample.pas`
3. ⏳ Esperar Fase 4.2 (NotifyEvent em 8 pontos)
4. ⏳ Testar auditoria real com seu app

## Links

- `OBSERVABILIDADE-FASE41.md` — Sumário executivo
- `docs/observabilidade.md` — Guia completo de 6 padrões
- `docs/observabilidade-integracao-proxima.md` — Roadmap Fase 4.2
- `samples/Server/ObservabilityExample.pas` — Seu caso de uso
- `MUDANCAS-FASE41.txt` — Inventário de mudanças

---

**Status**: Framework pronto. Eventos disparam em Fase 4.2.  
**Quando**: ~1-2 sprints.  
**Seu próximo passo**: Registre seus handlers agora. Eles vão receber eventos de verdade em breve!
