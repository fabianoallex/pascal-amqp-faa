# pascal-amqp-faa

Cliente **AMQP 0-9-1** (RabbitMQ) para **Free Pascal / Lazarus** e **Delphi**, a partir de uma única codebase. Porte multiplataforma da [delphi-amqp-faa](../delphi-amqp-faa) (mesmo autor, MIT), que era exclusiva de Delphi/Windows.

## Recursos

- Handshake completo (`Start/Tune/Open`) com negociação correta de `channel-max`/`frame-max`/`heartbeat` (compatível com RabbitMQ 3.13+).
- Publish/consume com **ack manual** (at-least-once); callbacks de consumer despachados em **thread pool próprio** — a thread de leitura nunca roda código do usuário.
- **Publisher confirms** (`confirm.select`): `Publish` devolve seq-no, `OnConfirm` assíncrono, `WaitForConfirm`/`WaitForConfirms`.
- `Basic.Return` (publish `mandatory` não roteável), `Connection.Blocked/Unblocked` (resource alarm do broker).
- **Heartbeat** em thread dedicada (detecção de conexão morta + envio quando ocioso).
- **Reconexão automática** (opt-in) com recuperação de topologia (exchanges, filas, binds, qos, confirm mode e consumers) e, opcionalmente, reenvio de publishes não confirmados (`RepublishUnconfirmedOnReconnect`).
- **TLS (amqps)** via SChannel nativo — somente Windows por enquanto (FPC e Delphi); OpenSSL para Linux está no roadmap.
- `Queue.Unbind`, `Exchange.Bind/Unbind` (extensão RabbitMQ), `Basic.Get`, `Qos`.

## Compatibilidade

| Compilador | Status |
|---|---|
| FPC 3.2.2 (Lazarus 4.0), Win64 | Compila e passa o smoke test contra RabbitMQ real |
| Delphi (testado na base 12 / Athens) | Mesma codebase; requer namespaces `System;Winapi` no projeto (padrão do IDE) |
| FPC em Linux | Deve funcionar (socket via `ssockets`, sem dependência de Windows fora do TLS) — ainda não validado |

Decisões do porte (ver `CLAUDE.md` para detalhes):

- **Callbacks são `procedure ... of object`** (não `reference to`), porque o FPC estável não tem métodos anônimos. No Delphi, use métodos de uma classe sua em vez de lambdas.
- Nada de `System.Threading`/`TTask` nem `System.TMonitor`: a lib traz `AMQP.Threading` (thread pool + monitor/condvar + atomics portáveis).
- Socket em `AMQP.Transport` (`System.Net.Socket` no Delphi, `ssockets` no FPC).

## Uso rápido

```pascal
uses AMQP.Connection, AMQP.Exchange.Methods, AMQP.Queue.Methods;

type
  TMeuConsumidor = class
    procedure OnMsg(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  end;

procedure TMeuConsumidor.OnMsg(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
begin
  // roda numa thread do pool; processe e confirme:
  WriteLn(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
end;

var
  Conn: TAMQPConnection;
  Chan: TAMQPChannel;
  Consumidor: TMeuConsumidor;
begin
  Conn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  Conn.Open;
  Chan := Conn.CreateChannel;
  Chan.DeclareQueue(TAMQPQueueDeclare.Create('minha-fila'));

  Chan.ConfirmSelect; // publisher confirms (opcional)
  Chan.PublishText('', 'minha-fila', 'ola');
  Chan.WaitForConfirms(5000);

  Consumidor := TMeuConsumidor.Create;
  Chan.Qos(10);
  Chan.Consume('minha-fila', Consumidor.OnMsg);
  // ... Chan.Close; Chan.Free; Conn.Close; Conn.Free; Consumidor.Free;
end;
```

No FPC em **Linux**, adicione `cthreads` como primeira unit do programa. Para strings não-ASCII no FPC fora do Lazarus, configure `SetMultiByteConversionCodePage(CP_UTF8)` (aplicações Lazarus já usam UTF-8 por padrão).

## Compilando

**Lazarus**: abra/instale `packages/pascal_amqp_faa.lpk` (ou `lazbuild packages\pascal_amqp_faa.lpk`).

**FPC puro**:

```
fpc -Fusrc -Fisrc seu_programa.pas
```

**Delphi**: adicione `src\` ao search path do projeto (unit scope names `System;Winapi`, que é o padrão). Exemplo pronto em `samples\SmokeTest\SmokeTest.dproj`.

## Smoke test (integração)

Suba o RabbitMQ (`docker compose -f docker/docker-compose.yml up -d`) e:

```
cd samples\SmokeTest
fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
SmokeTest.exe
```

Exercita handshake, topologia, confirms, `Basic.Get`, consume concorrente com ack e reconexão automática com recovery. Sai com código 0 em sucesso.

## Roadmap

- TLS multiplataforma (OpenSSL via FCL) para Linux.
- Porte da suíte de testes unitários (FPCUnit) da lib original.
- Validação em Linux (x86_64/ARM).
- mTLS/client-cert (Windows: exige `crypt32`).

## Licença

MIT — ver [LICENSE](LICENSE).
