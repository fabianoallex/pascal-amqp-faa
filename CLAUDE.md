# pascal-amqp-faa — contexto do projeto

Cliente AMQP 0-9-1 dual-compiler (**FPC/Lazarus + Delphi**) numa única codebase, MIT. É o porte multiplataforma da `../delphi-amqp-faa` (mesmo autor); aquela permanece como projeto à parte, Delphi/Windows-only. A arquitetura, os invariantes de concorrência e as lições de protocolo estão documentados no `CLAUDE.md` de lá e valem aqui integralmente.

## Regra inegociável: proveniência do código (herdada da lib original)

**Nunca copiar, adaptar ou se basear em código-fonte de bibliotecas Pascal/Delphi AMQP de terceiros sem licença permissiva clara.** Este projeto deriva exclusivamente da `delphi-amqp-faa` (MIT, mesmo autor) e da especificação AMQP 0-9-1. Consultas a outras implementações são permitidas apenas como conhecimento de protocolo, nunca como fonte a transcrever.

## Regras da codebase dual (Delphi + FPC 3.2.2)

Toda unit começa com `{$I amqp.inc}` (ativa `{$MODE DELPHI}` no FPC e define `AMQP_WINDOWS`). O que NÃO usar, e o que usar no lugar:

| Proibido (não existe no FPC 3.2) | Usar |
|---|---|
| `reference to procedure` / métodos anônimos / `TThread.CreateAnonymousThread` | `procedure ... of object`; work items (`TAMQPWorkItem`) para capturar estado; threads nomeadas |
| `System.Threading` (`TTask.Run`) | `AmqpPool.Queue(...)` de `AMQP.Threading` |
| `System.TMonitor` (Enter/Wait/PulseAll) | `TAMQPMonitor` de `AMQP.Threading` |
| `TInterlocked.*` / `AtomicXxx` direto | `AmqpAtomicInc/Dec/Get/Read64/Write64` de `AMQP.Threading` |
| `TThread.GetTickCount64` | `AmqpTickMs` |
| `System.Net.Socket` direto | `TAMQPTcpSocket` de `AMQP.Transport` |
| `TEncoding.UTF8.GetBytes/GetString` | `AmqpUtf8Encode/Decode` de `AMQP.Wire` (no FPC respeitam o codepage dinâmico) |
| uses com namespace (`System.SysUtils`) | nome curto (`SysUtils`); no Delphi resolve via unit scope names `System;Winapi` |
| inline `var` em bloco | declarar no `var` da rotina |
| `TStringHelper` (`.Split` etc.) | rotinas manuais (`Pos`/`Copy`) |
| `TArray.Sort<T>` | ordenação manual (não existe no Generics.Collections do FPC) |

Gotchas de FPC já encontrados no porte:

- `TValue.From<TArray<TValue>>(x)` não parseia (o `>>` vira `shr` e nem com espaço passa) → usar o alias `TAMQPValueArray` (AMQP.Wire). `TValue.AsType<T>` não existe → `GetArrayLength/GetArrayElement`.
- `Boolean` tem `Kind = tkBool` no FPC (não `tkEnumeration`); `AnsiString` é `tkAString` — os `case AValue.Kind` do codec tratam os dois via `{$IFDEF FPC}`.
- Enum anônimo inline como campo de classe não compila → tipo nomeado (`TAMQPAsmState`).
- `PWideChar(string)` não existe (string é Ansi) → campos que vão para APIs wide são `UnicodeString` (ver `AMQP.Transport.Tls.FTargetName`).
- `Format('%x')` com `LongInt` negativo imprime 16 dígitos → passar `Cardinal(valor)`.
- Fontes em UTF-8 **com BOM**: Delphi exige o BOM para ler UTF-8; o FPC com BOM trata literais como UTF-8 corretamente. Manter o BOM ao criar units novas.
- FPC 3.2.2 trava com **erro interno** (`Internal error 2015071704` / `200510032`) em duas combinações específicas envolvendo `TValue`: (1) encadear `.Put(...)` de `TAMQPFieldTable` terminando num `TValue.From<T>(literal)` inline — separar em chamadas `.Put()` distintas resolve; (2) `Tabela['chave'].AsString` / `.AsExtended` / `.AsObject` encadeado direto no indexador — atribuir o resultado do indexador a uma variável `TValue` local antes de chamar o accessor resolve (`.AsBoolean`/`.AsInteger`/`.AsInt64` encadeados direto não têm esse problema). Achado portando `tests\Unit\fpc\AMQP.WireTests.pas`.
- Apps console FPC puro (fora do Lazarus/LCL) têm `DefaultSystemCodePage` diferente de UTF-8 por padrão — strings acentuadas literais (mesmo com o `.pas` em UTF-8 com BOM) saem transcodificadas errado ao passar por `AmqpUtf8Encode`. Chamar `SetMultiByteConversionCodePage(CP_UTF8)` no início do `program` resolve (ver `tests\Unit\fpc\AMQPUnitTestsFpc.lpr`).
- `TInterlocked` (System.SyncObjs) não existe no FPC → usar `AmqpAtomicInc/Dec/Get/Set/CompareExchange` de `AMQP.Threading` (`AmqpAtomicSet`/`AmqpAtomicCompareExchange` foram adicionados especificamente para portar os testes de integração, que usavam `TInterlocked.Exchange`/`.CompareExchange(x,new,old)`).
- `TTestCase.AssertException` do FPCUnit exige a classe **exata** da exceção (`E.ClassName = AExceptionClass.ClassName`) — não aceita subclasse, diferente do `Assert.WillRaise(proc, EAlgumaClasse)` do DUnitX (que aceita `is`/herança) e do `Assert.WillRaise(proc)` sem classe (qualquer exceção). Para "qualquer exceção", usar `try/except` manual com uma flag booleana + `AssertTrue` (não `Fail()` dentro do próprio `try`, que seria capturado pelo `except`, já que `EAssertionFailedError` também é `Exception`).
- `System.Threading` (`TParallel.For`) não existe no FPC — para testes que precisavam de N chamadas concorrentes, usar `AmqpPool.Queue(TAMQPWorkItem)` da própria lib. Cuidado ao compartilhar arrays entre o item de trabalho e o chamador: um parâmetro de construtor tipado como `array of T` é *open array* (semântica de valor, copia os elementos) — usar um tipo nomeado de dynamic array (`TFooArr = array of T`) como parâmetro para de fato compartilhar o mesmo buffer.
- Citar um diretivo tipo `{$IFDEF FPC}` dentro de um comentário de prosa `{ ... }` quebra a compilação: o `}` do próprio texto citado fecha o comentário mais cedo, e o resto vira código inválido. Escrever "condicional" ou similar em vez do `{$...}` literal ao documentar isso em comentários.
- Amostras VCL/LCL (samples `*Vcl`): `TThread.Queue`/`.Synchronize` do FPC só têm o overload `TThreadMethod = procedure of object` (sem parâmetros) — sem o overload de closure anônima do Delphi. Toda chamada `TThread.Queue(nil, procedure ... end)` (usada pra levar resultado de callback da lib, que roda numa thread do pool, de volta pra UI) precisa virar um método nomeado. Como várias threads do pool chamam isso concorrentemente, um campo compartilhado na form teria corrida — em vez disso, cada chamada cria um pequeno objeto de "marshal" descartável (dados da chamada + `procedure Execute; begin Form.Metodo(...); Free; end;`) e enfileira `TThread.Queue(nil, LMarshal.Execute)`. Ver `samples\RetaguardaVcl\uRetaguardaMain.pas` (`TRecebidaMarshal`/`TStatusMarshal`).
- `System.TMonitor` (usado nos samples VCL originais pra proteger `FPending`) não existe no FPC → `TCriticalSection` (`SyncObjs`), mesmo padrão já usado no resto do projeto.
- `ReportMemoryLeaksOnShutdown` (global do `System`) não existe no FPC → `{$IFNDEF FPC}` em volta da atribuição no `.dpr`.
- LCL não tem a propriedade `Font.Charset` nem `TForm.DesignSize` do jeito que o `.dfm` do Delphi grava — omitir essas linhas ao escrever o `.lfm` equivalente (as demais subpropriedades de `Font` e os `Anchors` de cada controle continuam idênticos nos dois).

## Peças novas em relação à lib original

- `AMQP.Threading`: atomics, `AmqpTickMs`, `TAMQPMonitor` (condvar por gerações de eventos — sem wakeups perdidos; waiters re-checam a condição em loop) e `TAMQPThreadPool` (cresce sob demanda até `max(16, 4×núcleos)`; workers persistentes; `AmqpPool` é o singleton global).
- `AMQP.Transport`: `TAMQPTcpSocket` (Delphi: `System.Net.Socket`; FPC: `ssockets.TInetSocket` + `fpshutdown` para desbloquear a thread de leitura sem corrida de FD).
- Work items em `AMQP.Connection` (implementation): `TAMQPDeliveryWork`, `TAMQPReturnWork`, `TAMQPConfirmWork`, `TAMQPBlockedWork`, `TAMQPUnblockedWork`, `TAMQPReconnectThread` — substituem os closures de `TTask.Run`; o contador in-flight é decrementado no `finally` do `Execute`.
- TLS (`AMQP.Transport.Tls`): SChannel, compila nos dois compiladores mas **só no Windows** (`AMQP_WINDOWS`); em outras plataformas `UseTls` levanta exceção. OpenSSL é roadmap.

## Build e testes

- FPC: `fpc -Fusrc -Fisrc -FEbuild -FUbuild src\AMQP.Connection.pas` compila a lib inteira. Pacote Lazarus: `lazbuild packages\pascal_amqp_faa.lpk`.
- Smoke test de integração: `samples\SmokeTest\SmokeTest.dpr` (mesmo fonte para FPC e Delphi) contra o RabbitMQ do `docker/docker-compose.yml` — cobre handshake, topologia, confirms, get, consume+ack e reconexão com recovery. **Rode-o após qualquer mudança na lib.**
- Delphi da máquina é Community Edition: **não compila por linha de comando** ("This version of the product does not support command line compiling") — validar o lado Delphi abrindo `samples\SmokeTest\SmokeTest.dproj` no IDE.
- Sincronização pós-reconexão em testes: esperar `OnReconnect` (dispara após o recovery), não `IsOpen` (fica True antes do replay da topologia).

## Convenções gerais

Mesmo padrão dos projetos irmãos (`delphi-amqp-faa`, `delphi-api-infra-faa`): licença MIT com copyright de Fabiano Arndt, commits em português, sem pushes/commits automáticos sem confirmação explícita do usuário.

## Roadmap

1. ~~Porte do núcleo + validação FPC/Win64 com smoke test (broker real).~~ **Concluído.**
2. ~~Validação Delphi via IDE (CE não tem CLI).~~ **Concluído** — `SmokeTest.exe` (Win32\Debug) rodou os 7 passos com PASS.
3. TLS multiplataforma via OpenSSL (FCL `opensslsockets`/handler próprio) para Linux.
4. ~~Porte dos testes unitários e de integração da lib original (Delphi/DUnitX e FPC/FPCUnit).~~ **Concluído.**
   - Fase Delphi/DUnitX **concluída** — `tests\Unit\AMQP.UnitTests.dproj` (80/80, 0 leaks) e `tests\Integration\AMQP.IntegrationTests.dproj` (24/24, 0 leaks, contra o RabbitMQ do `docker/docker-compose.yml`; TLS incluso). Nos testes de integração os callbacks anônimos (`OnBasicReturn`/`OnConfirm`/`OnReconnect`/`Consume`) viraram métodos nomeados na fixture, já que a lib usa `procedure ... of object` (regra do FPC). `AMQP.groupproj` na raiz abre os dois projetos juntos no IDE.
   - Fase FPC/FPCUnit dos **unitários concluída** — `tests\Unit\fpc\AMQPUnitTestsFpc.lpi` (mesma cobertura do `AMQP.UnitTests.dproj`, reescrita para a API do FPCUnit — `AssertEquals`/`AssertTrue` com mensagem primeiro, `AssertException(Classe, MetodoDeObjeto)` no lugar de `Assert.WillRaise`), 80/80, validado por `fpc` e `lazbuild` direto (sem precisar do IDE). Ver gotchas de FPCUnit/`TValue` acima. O runner decide sozinho pelo `ParamCount`: sem argumentos abre a GUI do FPCUnit (pacote `FPCUnitTestRunner`, árvore de testes + barra verde/vermelha); com argumentos (`--all --format=plain`) roda em modo console.
   - Fase FPC/FPCUnit de **integração concluída** — `tests\Integration\fpc\AMQPIntegrationTestsFpc.lpi`, 24/24 (0 erros, 0 falhas), rodado contra o RabbitMQ real do `docker/docker-compose.yml`, incluindo TLS de verdade (`docker-compose.tls.yml` + certs de `docker/certs`) — confirmado com o broker recriado com a porta 5671 ativa; os 2 testes de TLS conectaram e passaram de fato, não caíram no caminho de auto-ignorar por broker indisponível. Precisou de `AmqpAtomicSet`/`AmqpAtomicCompareExchange` novos em `AMQP.Threading` (substituem `TInterlocked`, que não existe no FPC) e de um `TAMQPWorkItem` customizado via `AmqpPool` no lugar de `TParallel.For` (`System.Threading` também não existe no FPC). Ver gotchas acima.
5. Validação em Linux (socket/threads já são portáveis; falta rodar).
