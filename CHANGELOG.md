# Changelog

O formato segue o [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/), e as versões
seguem o [Versionamento Semântico](https://semver.org/lang/pt-BR/). Enquanto a versão for 0.x,
uma versão minor pode mudar a API; toda mudança desse tipo aparece aqui.

## [Unreleased]

## [0.6.0] - 2026-11-02

TCP e TLS pela **pascal-common-faa 1.27.0** (passo 5 da "Migration order" do
`docs/transport-design.md` de lá): o cliente conecta com `PcTcpConnect`/`PcTlsWrap`, o broker escuta
com `PcTcpListen`/`PcTlsListen`, e os dois leem e escrevem por um `TAMQPEndpointStream`. As três
cópias do transporte (amqp, redis, pipes) viraram uma.

### Adicionado

- **CA privada e mTLS no cliente** (`TAMQPConnectionParams`): `TlsCaFile` (PEM, os dois backends:
  confia NESTA CA para o servidor em vez do repositório do sistema), `TlsCertFile` + `TlsKeyFile`
  (par PEM no OpenSSL) ou `TlsCertFile` + `TlsCertPassword` (PFX no SChannel) para o certificado do
  cliente. Antes só havia o repositório do sistema e nenhum certificado de cliente.
- **Backend TLS escolhido em tempo de execução**: `TAMQPConnectionParams.TlsBackend` e
  `TAMQPServer.TlsBackend` (`tbDefault`, `tbSChannel`, `tbOpenSsl`). No Windows os dois backends
  estão em todo build; fora dele, o OpenSSL é o único.
- **Broker: TLS de servidor também no SChannel** (Windows, certificado em PFX com
  `TlsCertPassword`) e **mTLS** (`TAMQPServer.TlsCaFile`: exige certificado de cliente emitido por
  essa CA). Antes o TLS de servidor exigia `-dAMQP_OPENSSL`.
- `TAMQPConnectionParams.ConnectTimeoutMs` (0 = o padrão da common, 10 s).
- Em `AMQP.Transport`: `TAMQPEndpointStream`, `AmqpTcpOptions`, `AmqpDefaultTlsBackend`,
  `AmqpResolveTlsBackend`, `AmqpRaiseTransportError` e sobrecargas de `AmqpTlsBackendName`/
  `AmqpTlsBackendInfo` que recebem o backend. Em `AMQP.Connection`: `AmqpClientTlsOptions`.
- Testes: `AMQP.TransportTests` (unitária, 14: a fronteira das exceções e o mapeamento dos
  parâmetros); a fixture TLS do broker roda em todo build, com CA privada, nome errado e mTLS
  aceito/recusado, e no Windows também com OpenSSL dos dois lados e cruzado (9 testes × 4); a PKI é
  a `tests/pki` da pascal-common-faa, pelo submódulo.

### Mudado

- **Exige a pascal-common-faa 1.27.0** (checagem em `AMQP.Threading` e `MinVersion` dos dois
  `.lpk`; era 1.8.0). Submódulo na tag v1.27.0.
- **Mensagens de erro de transporte em inglês**, as da pascal-common-faa (texto de mensagem não é
  contrato). As classes que saem da lib continuam `EAMQPTransport` e `EAMQPTls` (nenhuma
  `EPcTransport*` sai), e `EAMQPTls` continua fora da hierarquia de `EAMQPTransport`, para um
  `except on EAMQPTransport` antigo não passar a capturar erro de TLS.
- **Falha ao conectar** levanta `EAMQPTransport`. Antes saía a exceção da RTL (`ESocketError` do
  `ssockets` no FPC, do `System.Net.Socket` no Delphi).
- `-dAMQP_OPENSSL` agora **só troca o backend padrão** para OpenSSL (`AmqpDefaultTlsBackend`). Os
  build modes `openssl` continuam valendo.
- **Prazos**: o connect tem orçamento (`ConnectTimeoutMs`, 10 s por padrão; antes era o do SO) e o
  handshake TLS tem o prazo padrão da common, 15 s, nos dois lados; ele sai assim que o handshake
  termina. Leitura e escrita continuam **sem prazo** (o AMQP tem heartbeat). TCP_NODELAY continua
  ligado em todo socket.
- **Broker: o certificado é carregado no `Start`** (uma vez, pelo listener), e um arquivo ou senha
  errados levantam `EAMQPTls` ali, com o broker parado. Antes cada handshake falhava. Um `Start`
  que não consegue escutar (porta ocupada, certificado) desfaz o que já tinha subido (journal,
  notificadora de eventos).
- Broker: o handshake TLS continua na thread da conexão (`TPcEndpoint.Handshake`), nunca na de
  accept. `BindAddress` aceita `'::'` (todas as interfaces IPv6).
- `TAMQPServerConnection.Create` recebe um `TPcEndpoint`; `TAMQPServerConnConfig` perdeu
  `TlsCertFile`/`TlsKeyFile` (o certificado é do listener).
- `TAMQPServer.Backlog` é **ignorado** (o listener da common usa o próprio); fica para não quebrar
  quem o atribui.

### Removido

- `TAMQPTcpSocket`, `TAMQPTcpListener`, `TAMQPSocketStream` e `AmqpSetTlsBackendDetail`
  (`AMQP.Transport`), `TAMQPSchannelStream` (`AMQP.Transport.Tls`) e `TAMQPOpenSslStream`
  (`AMQP.Transport.OpenSSL`). As duas units de TLS ficam vazias, só puxando as da common, por uma
  versão minor; saem na próxima.

### Conhecido

- Um cliente **sem TLS** contra a porta TLS de um broker **SChannel** só cai depois do prazo do
  handshake (15 s): o SChannel espera um registro TLS completo, e o protocol-header do AMQP tem 8
  bytes. Com OpenSSL a recusa é imediata. Registrado para a pascal-common-faa em
  `.ci/transport-findings-for-pascal-common-faa.md`.

## [0.5.0] - 2026-10-31

O outbox transacional da pascal-db-faa por um broker AMQP: a ponte que o pascal-agenda-faa usava
(`Agenda.Outbox.Amqp`, decisão 11 da M7 de lá) veio para cá. Manual em `docs/outbox.md`.

### Adicionado

- **`AMQP.Outbox`** (`src/outbox`), **fora dos pacotes**: precisa da pascal-db-faa 0.18.0 ou mais
  nova (`PascalDb.Outbox`), que o cliente e o broker não precisam. A API é a do pascal-agenda-faa:
  - `TOutboxAmqpSink`, o `IOutboxSink` que publica cada evento (persistente, num exchange `topic`,
    routing key = tipo, header `tenant-id`) num canal em confirm mode e espera o ack; sem ack,
    levanta, e o relay tenta de novo.
  - `TOutboxAmqpConsumer`, que roda um `IOutboxHandler` numa fila própria: grava o evento em
    `PROCESSED_EVENTS` antes do ack e não roda de novo um evento já gravado; falha vira `Nack`
    sem requeue, que o dead-letter leva para a fila `.retry` (TTL) e de volta; as tentativas vêm
    do `x-death`, e a última estaciona a mensagem na fila `.dead`.
  - `TOutboxAmqpQueue`, `OutboxAmqpDeclareExchange`, `OutboxAmqpDeclareQueues`,
    `OutboxAmqpPending`, `OutboxAmqpEvent`, `OutboxAmqpPurgeProcessed`, e a métrica
    `outbox.consumer.messages`.
  - O SQL (`PROCESSED.EXISTS`, `PROCESSED.INSERT`, `PROCESSED.PURGE`) é da aplicação, lido pelo nome
    no loader da factory; tabela e comandos de referência em `docs/outbox.md`.
- Suíte `tests/Outbox` (8 testes, DUnitX e FPCUnit espelhados): o sink e o consumidor contra o
  broker embutido, com um arquivo SQLite (SQLdb no FPC, FireDAC no Delphi). Nenhum RabbitMQ.
  `tools/test_fpc_docker.sh` ganhou a suíte `outbox`.

### Mudado

- O `PROCESSED_AT` gravado pelo consumidor vem do `TClock` da pascal-common-faa (era o
  `TJwt.UnixNow` da pascal-api-infra-faa, usado só como relógio): a ponte não puxa uma terceira lib,
  e os testes trocam o relógio com `TClock.SetClock`.
- Submódulos de teste: pascal-common-faa 1.12.0 → **1.26.0**, e **pascal-db-faa 0.18.0** (novo,
  só para a suíte do outbox). O mínimo que o cliente e o broker exigem continua 1.8.0; quem usa a
  ponte precisa do que a pascal-db-faa exige (1.17).

### Corrigido

- **Broker: o tick de entrega da thread monitora podia postar numa fila já liberada.**
  `TAMQPEngine.DeliverTick` copiava as filas sob o lock da engine, soltava o lock e só então
  postava em cada uma; um `Queue.Delete` nessa janela liberava a fila (o `DeleteQueue` libera sob o
  mesmo lock), e o comando caía num objeto morto. Achado pelo heaptrc nesta rodada: um comando
  vazado (200 bytes), uma vez em 16 execuções da passada durável da aceitação. Agora os posts saem
  com o lock seguro (o `Post` nunca espera). O mesmo padrão — ponteiro de fila usado depois de
  soltar o lock — existe em quem usa `TAMQPEngine.FindQueue`; fica registrado no worklog para uma
  sessão própria do broker.

## [0.4.2] - 2026-10-27

Defeito achado pelo pascal-agenda-faa (`docs/design.md` de lá, seção "M4: the broker, the compose
file, the trace"), no teste em que o consumidor parava logo depois de o handler começar.

### Corrigido

- **Fechar um canal com callback de consumer em voo derrubava a conexão inteira.** O `Close`
  mandava o `Channel.Close`, esperava o `Close-Ok` e só então esperava os callbacks. Um callback
  que terminava nessa janela e dava `Ack` mandava `Basic.Ack` num canal já fechado, e pela spec
  isso é erro de conexão: `504 CHANNEL_ERROR`, todos os canais caem. O `Free` do canal passava
  pelo mesmo caminho. Medido antes de mudar, contra o RabbitMQ 3
  (`504 CHANNEL_ERROR - expected 'channel.open'`) e contra o broker embutido. Agora o `Close`
  cancela os consumers do canal, espera os callbacks com o canal ainda aberto (o `Ack` deles
  vale, e nada do que já foi processado é reentregue) e só então manda o `Channel.Close`.
- **O mesmo 504 quando era o broker que fechava o canal** (um 404 ou 406 com um callback em voo).
  `Ack`/`Nack` num canal fechado ou fechando não saem mais para o fio e não levantam exceção: o
  broker já devolveu à fila, como `redelivered`, o que ficou sem ack. Depois de uma queda de
  conexão nada muda: `Ack`/`Nack` levantam `EAMQPConnection`, como antes.

### Para quem usa

- O contorno que o consumidor fazia antes de fechar o canal (cancelar o consumer e esperar os
  callbacks em voo) deixa de ser necessário. Uma flag própria de "não tratar depois do Stop"
  continua útil se o callback não deve começar trabalho novo durante o encerramento.

## [0.4.1] - 2026-10-15

### Corrigido

- **O `OnConsumerError` rodava sem span corrente**, e o log escrito nele saía sem o trace id da
  mensagem (com o `try` dentro do callback, como até a 0.3.0, saía com). O span `process <fila>`
  era fechado antes de o handler ser chamado. Agora ele segue corrente durante o handler e o
  `Nack`, e termina depois, marcado com o erro do callback (`error.type` e status, como antes).
  Achado pela pascal-api-infra-faa no `samples/04-messaging`; medido aqui com um teste que lê
  `TPcTracing.Current` dentro do handler (vazio na 0.4.0).

## [0.4.0] - 2026-10-15

Defeito achado ao medir o guia `docs/guide/dtos-json.md` da pascal-api-infra-faa (FPC
3.2.2/Windows contra RabbitMQ 3).

### Corrigido

- **Exceção no callback de consumer sumia, e a entrega ficava sem ack.** O
  `TAMQPDeliveryWork` relançava a exceção, e o pool (`TPcThreadPool`) a engole de propósito: nada
  era logado, a mensagem ficava sem ack até a conexão cair, voltava a cada reinício do processo
  e travava de novo, e com `Qos(1)` o consumer inteiro parava. Medido: 7 mensagens com a 4ª
  levantando deixavam 0 prontas e 1 sem ack; com `Qos(1)` e a 1ª de 5 levantando, 1 callback
  em 8 s e 4 prontas para sempre. Agora a entrega pendente leva `Nack` sem requeue (vai para a
  dead-letter exchange da fila, se houver; sem DLX, o broker a descarta), e as 4 passam.
  **Mudança de comportamento:** quem contava com a mensagem voltando depois de uma queda passa a
  vê-la na DLX (ou descartada); para o comportamento de antes, `AAction := ceaNone` no handler.

### Adicionado

- **`TAMQPChannel.OnConsumerError(Canal, Entrega, Erro, var Acao)`**, chamado quando um
  callback de consumer levanta, na mesma thread do pool. `Acao` (`TAMQPConsumerErrorAction`)
  chega como `ceaReject` se a entrega está pendente, ou `ceaNone` se já foi confirmada ou se o
  consumer é no-ack; o handler pode trocá-la por `ceaRequeue` (Nack com requeue) ou `ceaNone`
  (o aplicativo confirma depois). A lib nunca confirma duas vezes: se o callback ou o handler já
  deu `Ack`/`Nack`, nada é enviado (confirmar o mesmo tag de novo é `406` e fecha o canal).

## [0.3.0] - 2026-10-15

Dois defeitos achados ao medir o guia de mensageria da pascal-api-infra-faa (FPC 3.2.2/Windows
contra RabbitMQ 3).

### Corrigido

- **A reconexão automática não se recuperava quando a fila era declarada só noutro canal.** O
  replay da topologia era canal a canal, na ordem do `TDictionary` dos canais (a do hash, não a
  de criação: com os canais 1, 2 e 3 o 3 vinha primeiro). Com a fila não durável declarada no
  canal 1 e consumida no 3, o restart do broker a apagava, o `basic.consume` do canal 3 levava
  `404` antes de o declare do canal 1 voltar, e toda tentativa falhava igual, para sempre, sem
  nenhum sinal. Agora o replay é da conexão inteira, fase a fase: exchanges, filas, bindings e só
  então consumers, na ordem de criação dos canais. Medido com `docker restart`: antes, toda
  tentativa com o broker de pé levou o mesmo `404` até o fim da medição (90 s na do guia);
  depois, recuperado na primeira tentativa com o broker de pé.
- **Publish numa conexão morta levantava `EWriteError` ("Stream write error") em vez de
  `EAMQPConnection`.** A conexão passa a saber quando não é gravável (antes do handshake, depois da
  queda) e levanta `EAMQPConnection` sem tocar o stream, com o motivo no texto
  (`connection closed by server: 320 CONNECTION_FORCED - ...`); uma escrita que falha antes de a
  queda ser percebida também vira `EAMQPConnection` (`connection write failed (...)`), igual nos
  dois compiladores e em todo backend de transporte.

### Adicionado

- **`TAMQPConnection.OnReconnectError(Conn, Tentativa, Motivo)`**, disparado a cada tentativa de
  reconexão que falha, e **`TAMQPConnection.LastReconnectError`**, a mensagem da última (zerada
  quando a reconexão dá certo). A `EAMQPConnection` de um publish feito durante a reconexão traz
  o motivo da queda e o da última tentativa; o começo do texto
  (`connection unavailable right now (reconnecting`) é o de antes.
- Variáveis `AMQP_TEST_HOST`/`AMQP_TEST_PORT` para apontar a suíte de integração para outro
  RabbitMQ (o default segue `localhost:5672`).

## [0.2.0] - 2026-10-12

Fase D do design de observabilidade da pascal-api-infra-faa: spans do OpenTelemetry no
cliente, sobre o `PascalCommon.Tracing` da pascal-common-faa. Detalhes e decisões em
[`docs/tracing.md`](docs/tracing.md).

### Adicionado

- **Spans por publish e por mensagem consumida**, abertos pelo próprio canal quando a
  aplicação liga o tracer (`TPcTracing.Start`): `publish <exchange>` (producer, filho do span
  corrente), `process <fila>` em volta do callback do `Consume` (consumer, filho do
  `traceparent` da mensagem e span corrente durante o callback) e `receive <fila>` no
  `BasicGet` (client). Atributos das convenções de mensageria do OpenTelemetry. Com o tracer
  desligado nada muda: nenhum span, nenhum header, os mesmos bytes.
- **`traceparent`/`tracestate` nos headers da mensagem publicada**, injetados só nos bytes do
  content header: a tabela de headers do chamador não é alterada, e um `traceparent` que a
  aplicação já pôs vence.
- Unit `AMQP.Tracing`: `AmqpTraceParentOf`, `AmqpStartProcessSpan` (para processar o resultado
  de um `BasicGet` na trace do produtor), `AmqpFinishSpan` e as demais peças.
- `BuildContentHeader` com contexto de trace e `TAMQPWriter.WriteFieldTable` com pares extras
  (overloads; as formas antigas seguem iguais).

### Mudado

- **A versão mínima da pascal-common-faa sobe de 1.1.3 para 1.8.0**, que traz o
  `PascalCommon.Tracing`; é a mesma que as outras libs `*-faa` exigem. Uma cópia mais velha
  para o build com `pascal-amqp-faa precisa da pascal-common-faa 1.8.0 ou mais nova`; os dois
  `.lpk` exigem `MinVersion` 1.8.
- Submódulo `external/pascal-common-faa` (só testes e samples) de v1.2.0 para v1.8.0.

## [0.1.2] - 2026-10-05

### Mudado

- **A versão mínima da pascal-common-faa sobe de 1.0 para 1.1.3.** Antes da 1.1.3 o `PcPool`
  não crescia numa rajada de itens: com um worker ocioso, entregas cujos callbacks bloqueiam
  rodavam uma de cada vez, e um callback que esperasse outro enfileirado atrás dele só andava
  quando chegasse outro item (achado aqui, corrigido lá). Os callbacks do cliente rodam no
  `PcPool`, então uma cópia mais velha é defeito em produção. Uma cópia anterior à 1.1.3 para o
  build com `pascal-amqp-faa precisa da pascal-common-faa 1.1.3 ou mais nova`; os dois `.lpk`
  exigem `MinVersion` 1.1.3.
- Submódulo `external/pascal-common-faa` (só testes e samples) de v1.1.2 para v1.2.0, que
  também conta os núcleos de verdade no FPC/Linux (o teto do `PcPool` lá era sempre 16).

### Corrigido

- A falha intermitente de `ConsomeTodas_ComAck_E_Concorrencia` (pico de concorrência 1) era a
  rajada acima; some com a pascal-common-faa 1.1.3 ou mais nova.

## [0.1.1] - 2026-10-05

Correções vindas da migração do pascal-dfe-broker para a v0.1.0 (achados 4 e 6 do F10 de lá),
mais o que a validação desta rodada encontrou. Nenhuma mudança de API além de uma função de
diagnóstico.

### Corrigido

- **`Close`/`Free` da conexão durante a reconexão esperavam o `ReconnectDelayMs` inteiro.** A
  espera entre tentativas era um `Sleep`; agora é um evento que o `Close` sinaliza, e fechar é
  imediato (medido: 3000 ms → 38 ms).
- **Com `ReconnectDelayMs` acima de 12 s, `Close`/`Destroy` liberavam a conexão com a thread
  de reconexão ainda viva**, que depois acordava lendo o objeto liberado. A espera desistia
  num teto de 12 s; agora a thread é unida, e `Close`/`Destroy` só voltam depois de ela sair.
  Uma tentativa já em curso (connect, handshake, replay da topologia) termina antes, limitada
  pelos timeouts dela.
- `Close` chamado de dentro de `OnDisconnect`/`OnReconnect`/`OnReconnectFailed` (que rodam na
  thread de reconexão) ficava preso 12 s esperando a própria thread; agora volta na hora.
- Broker: **nomes de fila gerados (`amq.gen-…`) se repetiam entre conexões.** A sequência era
  por conexão, e duas conexões no mesmo milissegundo geravam o mesmo nome; com a primeira fila
  exclusiva e viva, o declare da segunda levava `405`. A sequência agora é do processo.
- As suítes de teste compilam no **Delphi Win64** (asserções que comparavam `Integer` com o
  `NativeInt` de `Length`/`Count`); as quatro passam em Win32 e Win64.

### Adicionado

- `AmqpReconnectThreadsAlive` (`AMQP.Connection`): quantas threads de reconexão existem no
  processo. Serve a testes e diagnóstico.

### Documentado

- `Close`/`Free` de um canal esperam também os callbacks **ainda na fila** do pool; num canal
  comum o pool é o `PcPool`, do processo inteiro, e um `PcPool` saturado por outra lib atrasa
  o fechamento (medido: 1,5 s de saturação, 1,5 s de `Free`). Sem prazo, de propósito;
  `CreateChannel(True)` não depende do `PcPool` (29 ms no mesmo cenário). Nos dois READMEs.

### Mudado

- Submódulo `external/pascal-common-faa` (só testes e samples) de v1.0.1 para v1.1.2. O mínimo
  exigido da aplicação continua 1.0.

## [0.1.0] - 2026-10-04

Primeira versão publicada. Cliente AMQP 0-9-1 para Delphi 12 e FPC 3.2.2/Lazarus (Windows e
Linux x86_64/ARM64) numa codebase só, e um broker AMQP 0-9-1 embutível no mesmo processo da
aplicação (pacote separado).

### Adicionado

- **Cliente**: conexão e canais, declare/bind/delete de exchanges e filas, publish com
  propriedades, consumo push e `Basic.Get`, ack/nack/reject, `Basic.Qos`, publisher confirms,
  `mandatory` com `Basic.Return`, reconexão automática com replay de topologia e consumidores,
  heartbeats, TLS (SChannel no Windows; OpenSSL em qualquer plataforma com `-dAMQP_OPENSSL`),
  `Connection.Blocked`/`Unblocked`, canal com worker dedicado (`CreateChannel(True)`).
- **Broker embutido** (`pascal_amqp_faa_server.lpk`): handshake SASL PLAIN, exchanges
  direct/fanout/topic/headers e exchange→exchange, filas como atores, confirms, `mandatory`,
  filas exclusive/auto-delete; TTL de fila e de mensagem, dead-lettering com `x-death`,
  prioridade, `x-max-length`/`x-max-length-bytes` com `x-overflow`, `alternate-exchange`,
  `x-expires`; durabilidade opt-in por `DataDir` (WAL com group commit, recuperação no `Start`,
  confirm preso à marca d'água, compactação, teto de disco que recusa); observabilidade opt-in
  (eventos por thread notificadora, best-effort). Desvios deliberados do RabbitMQ listados no
  README.
- Samples de padrões de mensageria (GUI dual VCL/LCL) e o `PostoAutomacao`; suítes unitária,
  de integração, do broker e de aceitação (a suíte de integração do cliente contra o broker
  embutido), espelhadas em DUnitX e FPCUnit.

### Mudado

- **Dependência nova: [pascal-common-faa](https://github.com/fabianoallex/pascal-common-faa)
  1.0 ou mais nova** (fase F8 do plano dela). Atomics, ticks, monitor e thread pool saíram de
  `AMQP.Threading`, sem alias: `AmqpAtomic*` → `PcAtomic*`, `AmqpTickMs` → `PcTickMs`,
  `TAMQPMonitor` → `TPcMonitor`, `TAMQPWorkItem` → `TPcWorkItem`, `TAMQPThreadPool` →
  `TPcThreadPool`, `AmqpPool` → `PcPool`, `AMQP_WAIT_INFINITE` → `PC_WAIT_INFINITE`
  (`TAMQPCondGen` e `TAMQPPoolWorker` viraram tipos privados de lá). `AmqpWallMs` fica em
  `AMQP.Threading`. A aplicação fornece a cópia única da pascal-common-faa; os pacotes a exigem
  pelo nome, e uma cópia mais velha que a mínima para o build com mensagem clara.
- Os callbacks do cliente rodam no `PcPool`, **um pool para o processo inteiro**, dividido com
  as outras libs `*-faa`.
- **O broker roda os atores das filas num pool próprio** (do `TAMQPServer`), não no pool do
  processo: com o pool do processo saturado por callbacks que bloqueiam, o `Queue.Declare`
  esperava 15 s e a conexão caía; com o pool próprio, responde em milissegundos.

### Corrigido

- Broker: double free da tabela de argumentos no `Queue.Declare` quando o ator da fila não
  respondia a tempo (o canal liberava a tabela que o descritor da fila já possuía) — access
  violation no `Destroy` do broker.
- Teste de rotação do journal que supunha a rotação visível logo após a marca d'água (falhava
  sob carga).

[Unreleased]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fabianoallex/pascal-amqp-faa/releases/tag/v0.1.0
