# pascal-amqp-faa

> 🇬🇧 This document is also available in [English](README.en.md).

Cliente **AMQP 0-9-1** (RabbitMQ) para **Free Pascal / Lazarus** e **Delphi**, a partir de uma única codebase. Porte multiplataforma da [delphi-amqp-faa](https://github.com/fabianoallex/delphi-amqp-faa) (mesmo autor, MIT), que era exclusiva de Delphi/Windows.

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
| FPC 3.2.2 (Lazarus 4.0), Win64 | Compila; smoke test, suíte FPCUnit (80 unitários + 28 integração) e os 4 samples passam contra RabbitMQ real |
| Delphi (testado na base 12 / Athens) | Mesma codebase; suíte DUnitX (80 unitários + 28 integração) e os 4 samples validados via IDE (Community Edition não compila por linha de comando) |
| FPC 3.2.2, Linux x86_64 (Debian, container) | Compila; smoke test (plain e `--tls` com `-dAMQP_OPENSSL`), suíte FPCUnit (80 unitários + 27 integração, TLS incluso via OpenSSL) e os 4 samples (console e GUI/LCL-GTK2) passam contra RabbitMQ real |
| FPC 3.2.2, Linux ARM64 (Debian, container/QEMU) | Mesma cobertura do x86_64: smoke test plain e `--tls` (OpenSSL aarch64) e suíte FPCUnit 80 + 27 passam contra RabbitMQ real |

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

> O `TAMQPConnection` é dono das threads internas (leitura + heartbeat). Libere os canais antes da conexão (ou deixe o `Free` da conexão cuidar deles).

## Uso em detalhe

### Declarar fila / exchange / binding

```pascal
// Fila durável nomeada:
Chan.DeclareQueue(TAMQPQueueDeclare.Create('nfe.respostas', True));

// Exchange topic + binding:
var
  LBind: TAMQPQueueBind;
begin
  Chan.DeclareExchange(TAMQPExchangeDeclare.Create('nfe', AMQP_EXCHANGE_TYPE_TOPIC));
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := 'nfe.respostas';
  LBind.ExchangeName := 'nfe';
  LBind.RoutingKey := 'resposta.#';
  Chan.BindQueue(LBind);
end;
```

### Publicar com propriedades

```pascal
uses AMQP.Wire, AMQP.Basic.Methods;

var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  LProps.SetPersistent;                       // delivery-mode 2
  LProps.SetCorrelationId('NFe3524...9012');
  LProps.SetMessageId('req-42');
  Chan.Publish('', 'nfe.respostas',
    AmqpUtf8Encode('{"status":"autorizada"}'), LProps);
end;
```

O exchange vazio (`''`) roteia pela *routing key* = nome da fila (default exchange). Sem confirm mode, publicar é fire-and-forget e `Publish` retorna 0. `AmqpUtf8Encode` (unit `AMQP.Wire`) é o substituto portável de `TEncoding.UTF8.GetBytes`.

### Consumir uma mensagem (pull)

```pascal
var
  LMsg: TAMQPGetResult;
begin
  LMsg := Chan.BasicGet('nfe.respostas', True {no-ack});
  if LMsg.Found then
    WriteLn(LMsg.BodyAsText);
end;
```

Para consumo contínuo (push, concorrente, com ack manual), veja o `Consume` do [Uso rápido](#uso-rápido) — e a seção de [concorrência e ordenação](#concorrência-e-ordenação-de-mensagens) abaixo.

### Publisher confirms em detalhe

`ConfirmSelect` coloca o canal em modo confirm: a partir daí cada `Publish` recebe um *seq-no* (1, 2, 3, ...) e o broker o confirma (`ack`) ou rejeita (`nack`). Dá para tratar de forma assíncrona com `OnConfirm` e/ou bloquear até a confirmação com `WaitForConfirm` (um publish) ou `WaitForConfirms` (todos os pendentes):

```pascal
type
  TMeuPublicador = class
    procedure QuandoConfirmar(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
  end;

procedure TMeuPublicador.QuandoConfirmar(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
begin
  if not AAck then
    Log(Format('publish %d foi NACK-ado pelo broker', [ASeqNo]));
end;

// ...
Chan.ConfirmSelect;
Chan.OnConfirm := Publicador.QuandoConfirmar; // assíncrono, numa thread do pool

// Síncrono: bloqueia até o broker confirmar este publish.
LSeq := Chan.Publish('', 'nfe.respostas', LBody, LProps);
if Chan.WaitForConfirm(LSeq, 5000) then
  WriteLn('confirmado')
else
  WriteLn('não confirmado (nack, queda de conexão ou timeout)');

// Ou publique em lote e espere todos de uma vez:
Chan.Publish('', 'nfe.respostas', LBody1, LProps);
Chan.Publish('', 'nfe.respostas', LBody2, LProps);
if not Chan.WaitForConfirms(5000) then
  WriteLn('algum publish não foi confirmado');
```

Se a conexão cair antes da confirmação, o publish pendente é reportado como **não confirmado** (`WaitForConfirm` retorna `False`); `OnConfirm` dispara apenas para confirmações reais do broker. Após uma reconexão os seq-nos seguem **monotônicos** (não reiniciam).

**Reenvio automático (opt-in)**: com `RepublishUnconfirmedOnReconnect := True` nos parâmetros da conexão (junto de `AutoReconnect`), os publishes que ficaram sem confirmação numa queda são **re-publicados automaticamente na reconexão**, com seq-nos novos (observáveis via `OnConfirm`). É *at-least-once* — pode haver duplicatas quando o broker recebeu a mensagem mas o `ack` se perdeu na queda; os seq-nos originais seguem reportando "não confirmado". Custo: guarda o corpo de cada publish pendente até a confirmação.

### `Basic.Return` (publish `mandatory` não roteável)

Para saber se um publish `mandatory` não foi roteado a nenhuma fila, trate `OnBasicReturn` (dispara numa thread do pool, como o callback de consumer):

```pascal
procedure TMeuPublicador.QuandoNaoRotear(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
begin
  Log(Format('mensagem não roteada: %s (%d) exchange=%s rk=%s',
    [AReturned.ReplyText, AReturned.ReplyCode, AReturned.Exchange, AReturned.RoutingKey]));
end;

// ...
Chan.OnBasicReturn := Publicador.QuandoNaoRotear;
Chan.Publish('nfe', 'resposta.inexistente', LBody, LProps, True {mandatory});
```

### Reconexão automática

```pascal
LParams := TAMQPConnectionParams.Localhost;
LParams.AutoReconnect := True;
LParams.ReconnectDelayMs := 2000;   // backoff entre tentativas
LParams.MaxReconnectAttempts := 0;  // 0 = infinitas

Conn := TAMQPConnection.Create(LParams);
Conn.OnReconnect := MeuApp.QuandoReconectar; // dispara após reconectar E restaurar a topologia
Conn.Open;
```

Na queda, a lib reconecta e **restaura a topologia** declarada naquele canal (filas, exchanges, bindings, Qos, confirm mode) e **re-registra os consumers** — o seu callback volta a receber mensagens sem intervenção. Como *delivery-tags* reiniciam a cada sessão, mensagens não confirmadas são reentregues: projete os handlers para serem **idempotentes** (at-least-once). Em testes/sincronização, espere o `OnReconnect` (que dispara após o recovery completo), não `IsOpen` (fica `True` antes do replay da topologia).

### O callback de consumer precisa sempre terminar sozinho

`Close`/`Destroy` do canal esperam (sem timeout, de propósito) os callbacks em voo terminarem antes de liberar o objeto — I/O demorado é ok, mas um callback que bloqueia indefinidamente esperando interação do usuário ou um evento que só outra thread da aplicação sinaliza trava esse fechamento; se o `Free` roda na thread principal de uma app VCL/LCL, a UI congela junto (deadlock). Se o fluxo depende de aprovação humana, prefira **não bloquear**: guarde o *delivery-tag* e o conteúdo numa estrutura própria, retorne, e confirme depois (`Ack`/`Nack` podem ser chamados de qualquer thread). Se optar por bloquear num `TEvent`, o encerramento precisa acordar **todas** as esperas e também cobrir entregas que cheguem *durante* a desconexão — um nack+requeue pode ser reentregue imediatamente ao mesmo consumer até o `Cancel` completar (`samples/RetaguardaVcl` mostra o padrão com flag de encerramento).

## Arquitetura (resumo)

- **Uma thread de leitura** é a única que lê o socket após o handshake; ela demultiplexa frames por canal e **despacha callbacks de consumer para o thread pool** (`AmqpPool`, de `AMQP.Threading`) — nunca roda código do usuário nem bloqueia.
- Todas as **escritas** são serializadas por um lock; os frames de uma mensagem (método + header + corpo) saem juntos.
- **RPC** (declare/bind/get/consume/close) é feito por evento: envia e aguarda a thread de leitura entregar a resposta.
- **Heartbeat** e **reconexão** rodam em threads próprias com espera interrompível (`TEvent`).

## Erros e exceções

Todas descendem direto de `Exception` (sem classe-base comum). `EAMQPConnection`/`EAMQPChannel` (`AMQP.Connection`) são as que a aplicação normalmente trata; as outras quatro são de camadas internas e costumam só aparecer na prática durante `Open` (encapsuladas ou não).

| Exceção | Quando é levantada |
|---|---|
| `EAMQPConnection` | Falha no handshake/`Open` (recusa do broker, resposta em canal inesperado); `Publish`/`CreateChannel` chamado com a conexão fechada ou em processo de reconexão. |
| `EAMQPChannel` | RPC de canal (declare/bind/get/consume/close) recebeu `Channel.Close` do broker, deu timeout, ou foi chamado com o canal já fechado; uso indevido da API (ex. `WaitForConfirm` fora de confirm mode). |
| `EAMQPTransport` | Erro de socket plain (host/porta inalcançável, conexão derrubada) fora do handshake TLS. |
| `EAMQPTls` | Falha de handshake TLS, certificado rejeitado (`TlsVerifyPeer=True`), `UseTls=True` sem backend TLS compilado, ou `libssl`/`libcrypto` não encontrada em runtime. Ver [TLS (amqps)](#tls-amqps). |
| `EAMQPFrame` | Frame malformado ou conexão encerrada no meio de um frame — normalmente sintoma de queda de conexão, não bug de aplicação. |
| `EAMQPWire` | Encode/decode fora dos limites do protocolo (shortstr > 255 bytes, tipo de `TValue` não suportado num `TAMQPFieldTable`) — geralmente aponta erro da aplicação ao montar `Arguments`/`Headers`. |

Pontos práticos:

- **`Open`** é onde mais se espera capturar exceção síncrona: `EAMQPConnection`, e — se `UseTls=True` — `EAMQPTransport`/`EAMQPTls`.
- **Chamadas de canal** (`DeclareQueue`, `BindQueue`, `Publish`, `Consume`, etc.) podem levantar `EAMQPChannel` de forma síncrona quando o broker recusa o pedido ou o canal já caiu.
- **Depois de `Open` bem-sucedido, uma queda de conexão não vira exceção na thread da aplicação** — ela é reportada pelos callbacks `OnDisconnect`/`OnReconnect`/`OnReconnectFailed` (ver [Reconexão automática](#reconexão-automática)); sem `AutoReconnect`, a próxima chamada de RPC naquele canal falha com `EAMQPChannel` ("canal fechado").
- **Falha de publisher confirm não é exceção** — é `WaitForConfirm`/`WaitForConfirms` retornando `False`, ou `AAck=False` no `OnConfirm` (ver [Publisher confirms em detalhe](#publisher-confirms-em-detalhe)).

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

**2. Canal com worker dedicado (`CreateChannel(True)`)** — opção nativa da lib: em vez do `AmqpPool` global, o canal ganha uma thread própria (`TAMQPThreadPool.Create(1)` internamente) que processa deliveries/returns/confirms um de cada vez, na ordem de chegada. Diferente do `Qos(1)`, o broker continua entregando até o prefetch configurado — só o processamento no cliente é serializado, não o fluxo de rede. Zero código de aplicação; outros canais da mesma conexão continuam concorrentes normalmente. Ver `samples/Retaguarda` (flag `--dedicado`) e `samples/RetaguardaVcl` (checkbox "Thread dedicada").

**3. Fila própria da aplicação + uma thread dedicada** — a callback só empilha a entrega numa fila thread-safe (ex. `TCriticalSection` + `TQueue`); uma única thread dedicada drena e processa em ordem de chegada, chamando `Ack` no fim de cada item. Só vale a pena sobre a opção 2 quando você precisa de algo que o worker dedicado da lib não oferece — por exemplo, backpressure própria (limitar o tamanho da fila) ou compartilhar uma única fila de processamento entre mais de um canal/consumer.

**4. Sharding por chave** — variação da opção 3 com N filas de uma thread cada, escolhida por hash de alguma chave de domínio (ex. id do pedido, id do cliente). Ordem garantida *por chave*, paralelismo entre chaves diferentes — bom meio-termo quando só importa "ordem por entidade", não ordem global.

A opção 2 é built-in (`CreateChannel(ADedicatedConsumerThread: Boolean)`); as opções 3 e 4 são implementadas inteiramente na aplicação, com a API pública já existente (`Consume`/`Ack`/`Qos`/`TAMQPDelivery`) — não exigem nenhuma mudança na lib. Duas pegadinhas a observar nas opções 3 e 4:

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

- **Lazarus**: o `SmokeTest.lpi` e os dois runners FPCUnit já vêm com um **build mode `openssl`** — selecione no dropdown de build modes da IDE, ou por linha de comando: `lazbuild -B --build-mode=openssl samples\SmokeTest\SmokeTest.lpi`. Para outros projetos, lembre que a lib compila via pacote e defines do projeto **não** recompilam as units do pacote — a diretiva precisa chegar lá por `Project Options → Compiler Options → Additions and Overrides` (Custom Option `-dAMQP_OPENSSL`, que vale para os pacotes do projeto) ou direto no `packages/pascal_amqp_faa.lpk` (`Options → Custom Options`, valendo então para todo projeto que use o pacote).
- **Delphi (IDE)**: o `SmokeTest.dproj` e os dois projetos de teste já vêm com uma *build configuration* **`OpenSSL`** (filha da Debug) — basta ativá-la no Project Manager. Para outros projetos: `Project → Options → Building → Delphi Compiler → Conditional defines` → adicione `AMQP_OPENSSL`. *(Pendente de validação em alvo Linux: a Community Edition não tem esse target — o mesmo fonte é o validado pelo FPC/Linux.)*

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

No Windows isso usa SChannel; para exercitar o backend OpenSSL, compile com `-dAMQP_OPENSSL` — o `SmokeTest.lpi` traz o build mode `openssl` pronto pra isso (no Linux, OpenSSL é a única opção de TLS).

## Samples

O fluxo que motivou a lib: o autorizador publica a resposta da NFe numa fila; a **retaguarda** consome essa fila e responde ao polling de **vários PDVs simultâneos**. O consumo com thread pool + ack manual atende isso diretamente — cada resposta é processada em paralelo, correlacionada pela chave da NFe (`CorrelationId` ou um header), e só é confirmada após o processamento. Os samples implementam esse fluxo (PDV → autorizador → retaguarda), cada par compilando do mesmo fonte nos dois compiladores:

- **`samples/AutorizadorSim`** / **`samples/Retaguarda`** (console) — o autorizador publica N retornos simulados de NFe; a retaguarda consome concorrentemente, com ack manual e um comando de status. Rode a `Retaguarda` e depois o `AutorizadorSim`: as linhas `[worker N] iniciando...` de notas diferentes aparecem intercaladas, confirmando o processamento em paralelo. Com `Retaguarda --dedicado`, o canal usa worker próprio (ver [opção 2](#como-obter-ordem-quando-ela-importa)) — as mesmas linhas aparecem em ordem, uma nota de cada vez.
- **`samples/AutorizadorSimVcl`** / **`samples/RetaguardaVcl`** (GUI — VCL no Delphi, LCL no Lazarus, a partir do mesmo `.pas`/`.dfm`/`.lfm`) — mesma ideia com tela: conexão editável, publish sob demanda, e no `RetaguardaVcl` um modo de confirmação manual (aprovar/rejeitar nota pela lista) além do automático, e um checkbox "Thread dedicada" pra alternar entre os dois modos de despacho.
- **`samples/ConsultaStatusVcl`** (GUI, mesmo esquema dual) — **RPC request/reply**, o padrão clássico com `ReplyTo` + `CorrelationId`: o cliente publica a consulta de status de uma nota com `ReplyTo` apontando para uma fila de respostas exclusiva/auto-delete da instância e `Expiration` = timeout (pedido que envelhecer na fila é descartado pelo broker); o servidor consome a fila de pedidos, simula a busca e responde ecoando a correlação. Uma janela faz os dois papéis (canais separados na mesma conexão) — rode duas instâncias para o cenário distribuído. Timeout no cliente via timer, e resposta que chega **depois** do timeout é descartada com aviso no log. A fila de respostas usa nome fixo por instância em vez de nome gerado pelo broker: o replay de topologia da reconexão reenviaria o declare e o broker geraria um nome novo, órfão do consumer gravado.
- **`samples/EventosTopicVcl`** (GUI, mesmo esquema dual) — **pub/sub com exchange topic**: assinantes dinâmicos, cada um com fila exclusiva/auto-delete própria ligada ao exchange pela binding key (`*` casa uma palavra, `#` casa zero ou mais) — o mesmo evento chega a **todo** assinante que casar, diferente da fila de trabalho dos outros samples (uma mensagem, um consumidor). Publica com `mandatory` + `OnBasicReturn`: evento sem nenhum assinante casando é devolvido pelo broker e aparece no log em vez de sumir em silêncio. Remover um assinante desfaz na ordem `Cancel` → `UnbindQueue` → `DeleteQueue`, reconciliando a topologia de recovery (sem isso, uma reconexão replayaria um bind para fila inexistente). Único sample que exercita `DeclareExchange`/`BindQueue`/`UnbindQueue`.
- **`samples/RetryDlqVcl`** (GUI, mesmo esquema dual) — **dead-letter + retry com backoff + DLQ**, o padrão de reprocessamento montado só com argumentos de fila (`TAMQPFieldTable`): a fila de trabalho tem DLX apontando para uma fila de espera sem consumidor (`x-message-ttl` = backoff, DLX de volta), então `Nack` sem requeue "agenda" o retry no próprio broker; o consumidor lê a tentativa atual do header `x-death` e, ao esgotar o máximo configurado, move a mensagem para a DLQ (é o consumidor quem decide — o broker não conta tentativas sozinho). O backoff usa TTL **da fila** (TTL por mensagem só expira na cabeça da fila). Conectar recria a topologia do zero via `DeleteQueue` — redeclarar fila existente com argumentos diferentes é `PRECONDITION_FAILED`.
- **`samples/PublicadorConfiavelVcl`** (GUI, mesmo esquema dual) — vitrine dos **publisher confirms**: o canal entra em confirm mode ao conectar e cada publish vira uma linha na lista, resolvida ao vivo pelo `OnConfirm` (ack/nack por seq-no). O lote é publicado com intervalo configurável por uma thread própria, de propósito: derrube o broker no meio (`docker stop` no container do RabbitMQ) e assista à sequência completa — falha de envio, reconexão automática, replay da topologia e, com "Reenviar não confirmadas na reconexão" marcado (`RepublishUnconfirmedOnReconnect`), o reenvio automático do que ficou pendente (com seq-nos novos, contados à parte). Tem ainda um botão de publish `mandatory` para rota inexistente (o broker **devolve** via `Basic.Return` e mesmo assim **confirma** — confirm é "assumi a responsabilidade", não "roteei") e um `WaitForConfirms` ao fim de cada lote, com o resultado no log.

Suba o broker (`docker compose -f docker/docker-compose.yml up -d`) e abra o `.dproj`/`.lpi` correspondente — ou `AMQP.groupproj`/`AMQP.lpg` pra abrir todos juntos.

## Testes

- **Delphi (DUnitX)**: abra `AMQP.groupproj` no RAD Studio.
- **FPC/Lazarus (FPCUnit)**: abra `AMQP.lpg` (Project Group — requer o pacote opcional `LazProjectGroups` instalado na IDE) ou cada `.lpi` individualmente.

Em ambos: `AMQP.UnitTests` / `AMQPUnitTestsFpc` (80 testes, não precisa de broker — encode/decode de frames, métodos, content header, negociação de tune) e `AMQP.IntegrationTests` / `AMQPIntegrationTestsFpc` (28 testes, precisa do RabbitMQ no ar: `docker compose -f docker/docker-compose.yml up -d`, TLS incluso via `docker-compose.tls.yml`). Os 5 testes de TLS (publish/busca, verify-peer, payload de 300KB, consumo concorrente e handshake contra a porta plain) se auto-ignoram se o broker TLS estiver fora do ar — e, fora do Windows, se o runner não tiver sido compilado com `-dAMQP_OPENSSL`. No FPCUnit eles aparecem como ignorados de verdade (`Number of ignored tests: 5` no relatório); no DUnitX, que não tem *skip* em runtime, continuam contando como Passed, mas o log de console mostra `Success. : IGNORADO: broker TLS (5671) indisponível...` em cada um — se os 5 aparecem sem essa mensagem, conectaram de fato.

O runner FPCUnit decide sozinho pelo `ParamCount`: sem argumentos abre a GUI (árvore de testes + barra verde/vermelha); com argumentos (`--all --format=plain`) roda em modo console. Rodando pela IDE do Lazarus, o chaveamento é em `Run → Run Parameters → Command line parameters` — os `.lpi` vêm com `--all --format=plain` salvo (modo console); limpe o campo para o F9 abrir a GUI. No modo console via F9 a janela fecha ao terminar: ou marque *Use launching application* com `C:\Windows\System32\cmd.exe /K $(TargetCmdLine)`, ou rode o executável direto num terminal.

## Limitações conhecidas

- Transações (`tx.*`) **não implementadas — por decisão de design**, não por dificuldade técnica (o protocolo `tx.*` é trivial). Uma transação AMQP é *stateful e por canal* ("tudo que publiquei/dei ack desde o último commit"), o que casa mal com o modelo *concurrency-first* desta lib: várias threads publicando no mesmo canal cairiam todas na mesma transação, sem escopo por-thread. Além disso, `tx.*` é síncrono e lento, e a própria RabbitMQ recomenda **publisher confirms** no lugar — que já estão implementados (`ConfirmSelect`). Se surgir necessidade real de lote atômico, o subconjunto tratável é `Tx.Select/Commit/Rollback` para uso serial em canal dedicado.
- Publisher confirms + reconexão: os publishes não confirmados antes da queda são reportados como **não confirmados**; o reenvio na reconexão é **opt-in** (`RepublishUnconfirmedOnReconnect`, at-least-once) — sem ele, reenvie na sua camada se precisar de garantia ponta a ponta. Ver [Publisher confirms em detalhe](#publisher-confirms-em-detalhe).
- **Recuperação de topologia com filas de nome gerado pelo servidor** — ver a seção abaixo.
- TLS: autenticação de servidor apenas — sem mTLS/client-cert, sem escolha manual de versão/cipher suite (ver [TLS (amqps)](#tls-amqps)).

### Recuperação de filas com nome gerado pelo servidor

Ao declarar `QueueName = ''`, o servidor **gera** o nome (`amq.gen-XXXX`). A recuperação de topologia na reconexão **assume filas nomeadas**: ela guarda os payloads de `declare`/`bind`/`consume` já serializados e os re-executa. Como o redeclare com nome vazio gera um nome **novo** e os `bind`/`consume` gravados carregam o nome **antigo**, o replay quebra (`basic.consume` na fila inexistente → `404` → o servidor fecha o canal).

**Workaround (recomendado):** se você precisa de fila temporária **e** de reconexão, **gere o nome no cliente** em vez de usar `''`:

```pascal
var
  LGuid: TGUID;
begin
  CreateGUID(LGuid);
  LDecl.QueueName := 'reply-' + GUIDToString(LGuid);
  LDecl.Exclusive := True;   // some quando a conexão fecha
  LDecl.AutoDelete := True;
end;
```

Assim a fila é temporária com nome **estável e conhecido**, e a recuperação funciona como a de qualquer fila nomeada. Para RPC request/reply, prefira o **Direct Reply-to** (`amq.rabbitmq.reply-to`) — um nome-mágico fixo, sem declarar fila por requisição, que também não sofre desse problema. (Para quem for forkar e precisar de server-named real na recuperação, o caminho detalhado está documentado no README da [delphi-amqp-faa](https://github.com/fabianoallex/delphi-amqp-faa#recupera%C3%A7%C3%A3o-de-filas-com-nome-gerado-pelo-servidor) — o design é o mesmo.)

## Roadmap

- ~~Validação em Linux~~ — concluída: x86_64 e ARM64, TLS/OpenSSL incluso, samples GUI validados em LCL/GTK2 (ver tabela de compatibilidade).
- Validação do backend OpenSSL compilado pelo Delphi em Linux (a Community Edition não tem o target; o mesmo fonte é validado pelo FPC/Linux).
- mTLS/client-cert.

## Licença

MIT — ver [LICENSE](LICENSE).
