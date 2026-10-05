# Changelog

O formato segue o [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/), e as versões
seguem o [Versionamento Semântico](https://semver.org/lang/pt-BR/). Enquanto a versão for 0.x,
uma versão minor pode mudar a API; toda mudança desse tipo aparece aqui.

## [Unreleased]

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

[Unreleased]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/fabianoallex/pascal-amqp-faa/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fabianoallex/pascal-amqp-faa/releases/tag/v0.1.0
