# pascal-amqp-faa

Cliente **AMQP 0-9-1** (RabbitMQ) para **Free Pascal / Lazarus** e **Delphi**, a partir de uma única codebase. Porte multiplataforma da [delphi-amqp-faa](../delphi-amqp-faa) (mesmo autor, MIT), que era exclusiva de Delphi/Windows.

## Recursos

- Handshake completo (`Start/Tune/Open`) com negociação correta de `channel-max`/`frame-max`/`heartbeat` (compatível com RabbitMQ 3.13+).
- Publish/consume com **ack manual** (at-least-once); callbacks de consumer despachados em **thread pool próprio** — a thread de leitura nunca roda código do usuário.
- **Publisher confirms** (`confirm.select`): `Publish` devolve seq-no, `OnConfirm` assíncrono, `WaitForConfirm`/`WaitForConfirms`.
- `Basic.Return` (publish `mandatory` não roteável), `Connection.Blocked/Unblocked` (resource alarm do broker).
- **Heartbeat** em thread dedicada (detecção de conexão morta + envio quando ocioso).
- **Reconexão automática** (opt-in) com recuperação de topologia (exchanges, filas, binds, qos, confirm mode e consumers) e, opcionalmente, reenvio de publishes não confirmados (`RepublishUnconfirmedOnReconnect`).
- **TLS (amqps)** com dois backends, mesma API: **SChannel** nativo no Windows (automático, sem dependências) e **OpenSSL** em qualquer plataforma (opt-in via `-dAMQP_OPENSSL`) — ver a seção [TLS (amqps)](#tls-amqps).
- `Queue.Unbind`, `Exchange.Bind/Unbind` (extensão RabbitMQ), `Basic.Get`, `Qos`.

## Compatibilidade

| Compilador | Status |
|---|---|
| FPC 3.2.2 (Lazarus 4.0), Win64 | Compila; smoke test, suíte FPCUnit (80 unitários + 24 integração) e os 4 samples passam contra RabbitMQ real |
| Delphi (testado na base 12 / Athens) | Mesma codebase; suíte DUnitX (80 unitários + 24 integração) e os 4 samples validados via IDE (Community Edition não compila por linha de comando) |
| FPC 3.2.2, Linux x86_64 (Debian, container) | Compila; smoke test (plain e `--tls` com `-dAMQP_OPENSSL`), suíte FPCUnit (80 unitários + 24 integração, TLS incluso via OpenSSL) e os samples console (`AutorizadorSim`/`Retaguarda`) passam contra RabbitMQ real. ARM e samples VCL/LCL ainda não validados |

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

## Concorrência e ordenação de mensagens

Cada entrega é despachada pro **thread pool** (`AmqpPool`, ver `AMQP.Threading`) como um item de trabalho independente — não existe uma fila única por canal/consumer sendo drenada em ordem. É uma escolha de design deliberada, diferente do padrão comum em outras linguagens:

- **RabbitMQ Java/.NET client**: cada canal é processado sequencialmente por um único worker tirado de um pool compartilhado — canais diferentes rodam em paralelo entre si, mas dentro de um canal a ordem é preservada por padrão.
- **pika (Python) / node-amqplib**: single-threaded, orientado a event loop — todos os callbacks rodam na mesma thread/loop; paralelismo é opt-in, por conta do código do usuário.
- **amqp091-go**: expõe um channel de Go com as entregas; quem consome decide se processa serial ou dispara goroutines.

Aqui o padrão é o oposto: **paralelismo por padrão, ordem por opt-in**. O ganho real de throughput aparece quando o callback faz algo bloqueante (I/O de rede, banco, disco) — várias mensagens processam ao mesmo tempo em vez de uma esperar a outra terminar. Se o callback for leve/CPU-bound, o ganho é marginal (o hand-off pro pool custa mais que processar inline). Em qualquer um dos dois casos, o efeito colateral não muda: **duas entregas do mesmo consumer podem terminar de processar fora de ordem.**

### Como obter ordem quando ela importa

**1. `Qos(1)` + ack só ao final do processamento** — a opção mais simples, usa só API pública já existente. O broker não entrega a próxima mensagem daquele consumer enquanto a anterior não for confirmada:

```pascal
Chan.Qos(1);
Chan.Consume('minha-fila', Consumidor.OnMsg);
// dentro de OnMsg: só chame Ack depois de terminar o processamento
```

Serializa **tudo** daquele consumer — sem paralelismo algum. Boa escolha quando ordem estrita é obrigatória e o volume não é o gargalo.

**2. Fila própria da aplicação + uma thread dedicada** — a callback só empilha a entrega numa fila thread-safe (ex. `TCriticalSection` + `TQueue`); uma única thread dedicada drena e processa em ordem de chegada, chamando `Ack` no fim de cada item. Preserva ordem real de chegada e deixa o `Qos`/prefetch livre pra continuar recebendo em paralelo do broker enquanto sua fila processa.

**3. Sharding por chave** — variação da opção 2 com N filas de uma thread cada, escolhida por hash de alguma chave de domínio (ex. id do pedido, id do cliente). Ordem garantida *por chave*, paralelismo entre chaves diferentes — bom meio-termo quando só importa "ordem por entidade", não ordem global.

As opções 2 e 3 são implementadas inteiramente na aplicação, com a API pública já existente (`Consume`/`Ack`/`Qos`/`TAMQPDelivery`) — não exigem nenhuma mudança na lib. Duas pegadinhas a observar:

- **`ADelivery.Properties.Headers` é liberado pela lib assim que a callback retorna** (o dono é o chamador, mas o pool libera automaticamente depois que `OnMsg` volta). Se a callback só empilha a entrega e retorna na hora, `Headers` já estará inválido quando a thread dedicada for processar depois — extraia o que precisar dos headers **antes** de retornar da callback; não guarde a referência ao `TAMQPFieldTable` pra usar depois.
- **`Channel.Close`/`Free` não sabe da sua fila própria.** Como a callback retorna assim que empilha, o contador interno de "em voo" da lib já dá aquela mensagem como concluída. Ao fechar a aplicação, é responsabilidade de quem implementou a fila própria esperá-la esvaziar antes de fechar o canal — senão mensagens já retiradas da lib mas ainda não processadas/ackadas podem se perder.

Por que começar do lado paralelo em vez do serializado: adicionar ordem sobre uma lib *concurrency-first* é aditivo e contido (as opções acima). O caminho inverso — adicionar paralelismo numa lib que serializa por padrão — normalmente exige reconstruir na aplicação o que aqui já vem pronto: garantir que operações no canal são seguras por múltiplas threads (bibliotecas serializadas por padrão costumam não garantir isso, forçando um canal por worker), backpressure própria e drenagem segura no encerramento.

## Compilando

**Lazarus**: abra/instale `packages/pascal_amqp_faa.lpk` (ou `lazbuild packages\pascal_amqp_faa.lpk`).

**FPC puro**:

```
fpc -Fusrc -Fisrc seu_programa.pas
```

**Delphi**: adicione `src\` ao search path do projeto (unit scope names `System;Winapi`, que é o padrão). Exemplo pronto em `samples\SmokeTest\SmokeTest.dproj`.

## TLS (amqps)

Dois backends atrás da **mesma API** — o código da aplicação não muda, só o build:

| Backend | Plataformas | Como habilitar | Dependência em runtime |
|---|---|---|---|
| **SChannel** (`AMQP.Transport.Tls`) | Windows (FPC e Delphi) | Nenhum passo — automático | Nenhuma (`secur32.dll` do próprio Windows) |
| **OpenSSL** (`AMQP.Transport.OpenSSL`) | Qualquer uma (validado em Linux x86_64) | Diretiva `AMQP_OPENSSL` (opt-in) | `libssl`/`libcrypto` **3.x** ou **1.1.1** instaladas |

### Usando

```pascal
var
  Params: TAMQPConnectionParams;
begin
  Params := TAMQPConnectionParams.Localhost;
  Params.Port := 5671;
  Params.UseTls := True;
  Params.TlsVerifyPeer := True; // padrão: valida cadeia + hostname
  Params.TlsServerName := '';   // '' => usa Host (SNI / nome validado)
  Conn := TAMQPConnection.Create(Params);
```

Para broker de dev com certificado self-signed, `TAMQPConnectionParams.LocalhostTls` já vem pronto (porta 5671, `TlsVerifyPeer=False`).

### Habilitando o backend OpenSSL (`AMQP_OPENSSL`)

A diretiva é **opt-in** de propósito: SChannel é garantido existir no Windows, mas OpenSSL depende de `libssl` presente na máquina — então nunca é ligado automaticamente. Com a diretiva definida, o OpenSSL é usado em **qualquer** plataforma (inclusive no Windows, no lugar do SChannel). Sem ela, nada muda em relação ao comportamento atual: fora do Windows, `UseTls` levanta exceção explicando como habilitar.

- **FPC por linha de comando**: acrescente `-dAMQP_OPENSSL`:

  ```
  fpc -dAMQP_OPENSSL -Fusrc -Fisrc seu_programa.pas
  ```

- **Lazarus (IDE)**: a lib compila via pacote, então a diretiva vai **no pacote, não no projeto** (defines do projeto não recompilam as units do pacote): `Package → Open Package File → packages/pascal_amqp_faa.lpk → Options → Compiler Options → Custom Options` → adicione `-dAMQP_OPENSSL` e recompile. Vale para qualquer projeto que dependa do pacote (samples GUI, runners de teste).
- **Delphi (IDE)**: `Project → Options → Building → Delphi Compiler → Conditional defines` → adicione `AMQP_OPENSSL`. *(Pendente de validação em alvo Linux: a Community Edition não tem esse target — o mesmo fonte é o validado pelo FPC/Linux.)*

As bibliotecas são carregadas **dinamicamente na primeira conexão TLS** (`dlopen`/`LoadLibrary` + lista de nomes: `libssl.so.3` → `libssl.so.1.1` → `libssl.so` no Linux; `libssl-3-x64.dll` etc. no Windows). O executável não ganha dependência de link: se a `libssl` não estiver instalada, o erro só acontece ao chamar `Open` com `UseTls=True` — com mensagem dizendo quais nomes foram tentados.

Para saber **qual motor um executável está usando** (útil em UI/log — os samples VCL/LCL mostram no caption e no status): `AmqpTlsBackendName` (unit `AMQP.Transport`) devolve o backend do build (`OpenSSL`/`SChannel`/`nenhum`); `AmqpTlsBackendInfo` acrescenta, depois da primeira conexão TLS com OpenSSL, a versão e a biblioteca carregadas de fato (ex.: `OpenSSL 3.5.2 ... (libssl-3.dll)`). O SmokeTest `--tls` imprime isso no passo 1.

Escopo (igual nos dois backends): autenticação de servidor com TLS 1.2+; validação via trust store do sistema; sem mTLS/client-cert e sem renegociação iniciada pelo servidor.

### Broker de dev com TLS

O overlay `docker/docker-compose.tls.yml` adiciona o listener 5671 ao broker do compose principal — o cabeçalho do arquivo tem o passo a passo para gerar o certificado self-signed (incluindo o `chmod 644` na chave, que é obrigatório):

```
cd docker
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
```

## Smoke test (integração)

Suba o RabbitMQ (`docker compose -f docker/docker-compose.yml up -d`) e:

```
cd samples\SmokeTest
fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
SmokeTest.exe
```

Exercita handshake, topologia, confirms, `Basic.Get`, consume concorrente com ack e reconexão automática com recovery. Sai com código 0 em sucesso.

Com o argumento `--tls`, roda os mesmos passos sobre TLS (precisa do broker com o overlay `docker-compose.tls.yml`, ver seção TLS acima):

```
SmokeTest.exe --tls
```

No Windows isso usa SChannel; para exercitar o backend OpenSSL, compile com `-dAMQP_OPENSSL` (no Linux, é a única opção de TLS).

## Samples

Fluxo de exemplo (PDV → autorizador → retaguarda), cada par compilando do mesmo fonte nos dois compiladores:

- **`samples/AutorizadorSim`** / **`samples/Retaguarda`** (console) — o autorizador publica N retornos simulados de NFe; a retaguarda consome concorrentemente, com ack manual e um comando de status.
- **`samples/AutorizadorSimVcl`** / **`samples/RetaguardaVcl`** (GUI — VCL no Delphi, LCL no Lazarus, a partir do mesmo `.pas`/`.dfm`/`.lfm`) — mesma ideia com tela: conexão editável, publish sob demanda, e no `RetaguardaVcl` um modo de confirmação manual (aprovar/rejeitar nota pela lista) além do automático.

Suba o broker (`docker compose -f docker/docker-compose.yml up -d`) e abra o `.dproj`/`.lpi` correspondente — ou `AMQP.groupproj`/`AMQP.lpg` pra abrir todos juntos.

## Testes

- **Delphi (DUnitX)**: abra `AMQP.groupproj` no RAD Studio.
- **FPC/Lazarus (FPCUnit)**: abra `AMQP.lpg` (Project Group — requer o pacote opcional `LazProjectGroups` instalado na IDE) ou cada `.lpi` individualmente.

Em ambos: `AMQP.UnitTests` / `AMQPUnitTestsFpc` (80 testes, não precisa de broker — encode/decode de frames, métodos, content header, negociação de tune) e `AMQP.IntegrationTests` / `AMQPIntegrationTestsFpc` (27 testes, precisa do RabbitMQ no ar: `docker compose -f docker/docker-compose.yml up -d`, TLS incluso via `docker-compose.tls.yml`). Os 5 testes de TLS (publish/busca, verify-peer, payload de 300KB, consumo concorrente e handshake contra a porta plain) se auto-ignoram se o broker TLS estiver fora do ar — e, fora do Windows, se o runner não tiver sido compilado com `-dAMQP_OPENSSL`. No FPCUnit eles aparecem como ignorados de verdade (`Number of ignored tests: 5` no relatório); no DUnitX, que não tem *skip* em runtime, continuam contando como Passed, mas o log de console mostra `Success. : IGNORADO: broker TLS (5671) indisponível...` em cada um — se os 5 aparecem sem essa mensagem, conectaram de fato.

O runner FPCUnit decide sozinho pelo `ParamCount`: sem argumentos abre a GUI (árvore de testes + barra verde/vermelha); com argumentos (`--all --format=plain`) roda em modo console. Rodando pela IDE do Lazarus, o chaveamento é em `Run → Run Parameters → Command line parameters` — os `.lpi` vêm com `--all --format=plain` salvo (modo console); limpe o campo para o F9 abrir a GUI. No modo console via F9 a janela fecha ao terminar: ou marque *Use launching application* com `C:\Windows\System32\cmd.exe /K $(TargetCmdLine)`, ou rode o executável direto num terminal.

## Roadmap

- Validação em Linux — x86_64 feito, TLS/OpenSSL incluso (ver tabela de compatibilidade); falta ARM e os samples VCL/LCL.
- Validação do backend OpenSSL compilado pelo Delphi em Linux (a Community Edition não tem o target; o mesmo fonte é validado pelo FPC/Linux).
- mTLS/client-cert.

## Licença

MIT — ver [LICENSE](LICENSE).
