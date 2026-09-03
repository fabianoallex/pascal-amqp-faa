unit AMQP.TestTimeLogger;

{ Logger de console do DUnitX que imprime o TEMPO de cada teste.

  Por que existe
  --------------
  O TDUnitXConsoleLogger embutido não é configurável (só `quietMode` no
  construtor) e nunca imprime duração. Isso importa nesta codebase por um
  motivo concreto, descoberto na WS9 da Fase 2: o DUnitX **não tem balde de
  "ignorado"** — o auto-ignore usado aqui (testes de TLS sem broker, testes que
  dependem de recurso de Fase 3) é `Assert.Pass('IGNORADO: ...')`, que o
  relatório conta como **Passed**. Um resumo "28 Passed / 0 Ignored" pode
  esconder 5 testes que nem tocaram no alvo.

  O que distingue os dois é a **mensagem**: o auto-ignore daqui sempre passa um
  texto começando com `IGNORADO:` para o `Assert.Pass`, e é isso que este logger
  marca com `~`. (Tempo *quase* servia — auto-ignorado sai em ~0.000s — mas é
  heurística: contra o broker embutido há testes reais abaixo de 0,5 ms, e a
  primeira versão deste logger produziu 3 falsos positivos em 28. O campo
  `asserts` do XML também não serve: vem 0 mesmo em teste que rodou.)

  A duração vem de `ITestResult.Duration` (via `IResult`), exatamente a mesma
  fonte que o logger XML usa para o `time=` — os dois números batem.

  Como usar
  ---------
  `runner.AddLogger(TAMQPTestTimeLogger.Create);` **antes** do logger de console.
  A ordem importa: o runner percorre os loggers na ordem de registro, e o rodapé
  ("Done testing / Tests Found: ...") sai do `OnTestingEnds` do logger de
  console. Registrando este primeiro, a tabela de tempos aparece **entre** a
  saída detalhada dos testes e esse rodapé.

  Ele não imprime nada durante a execução: acumula em memória e despeja tudo de
  uma vez no fim. Assim a saída padrão do DUnitX (Setup/Executing/Teardown de
  cada teste) fica intacta, sem uma linha nossa intercalada a cada teste.

  A suíte FPCUnit já imprime tempo por teste nativamente — isto alinha os dois
  lados. }

interface

uses
  System.SysUtils,
  System.TimeSpan,
  System.Generics.Collections,
  DUnitX.TestFramework;

type
  /// Uma linha da tabela: o que foi medido de um teste.
  TAMQPTestTiming = record
    Nome: string;
    Segundos: Double;
    Resultado: string;
    Mensagem: string;
  end;

  TAMQPTestTimeLogger = class(TInterfacedObject, ITestLogger)
  private
    /// Prefixo que o auto-ignore desta codebase usa em `Assert.Pass` — é o
    /// sinal EXATO de "este teste não exercitou o alvo". A versão anterior
    /// deste logger deduzia isso do tempo (< 0,5 ms) e errava: contra o broker
    /// embutido, testes que rodam de verdade terminam em menos que isso —
    /// medido, 3 falsos positivos numa execução de 28. Tempo é heurística; a
    /// mensagem é fato.
    const MARCA_IGNORADO = 'IGNORADO';
  private
    FLinhas: TList<TAMQPTestTiming>;
    procedure Anota(const AResultado: string; const ATest: ITestResult);
    procedure Despeja;
  public
    constructor Create;
    destructor Destroy; override;

    // --- ITestLogger: só três destes fazem algo; o resto é obrigação da
    //     interface (o DUnitX não tem uma classe base "adapter"). ---
    procedure OnTestingStarts(const threadId: TThreadID;
      testCount, testActiveCount: Cardinal);
    procedure OnStartTestFixture(const threadId: TThreadID;
      const fixture: ITestFixtureInfo);
    procedure OnSetupFixture(const threadId: TThreadID;
      const fixture: ITestFixtureInfo);
    procedure OnEndSetupFixture(const threadId: TThreadID;
      const fixture: ITestFixtureInfo);
    procedure OnBeginTest(const threadId: TThreadID; const Test: ITestInfo);
    procedure OnSetupTest(const threadId: TThreadID; const Test: ITestInfo);
    procedure OnEndSetupTest(const threadId: TThreadID; const Test: ITestInfo);
    procedure OnExecuteTest(const threadId: TThreadID; const Test: ITestInfo);
    procedure OnTestSuccess(const threadId: TThreadID; const Test: ITestResult);
    procedure OnTestError(const threadId: TThreadID; const Error: ITestError);
    procedure OnTestFailure(const threadId: TThreadID; const Failure: ITestError);
    procedure OnTestIgnored(const threadId: TThreadID; const AIgnored: ITestResult);
    procedure OnTestMemoryLeak(const threadId: TThreadID; const Test: ITestResult);
    procedure OnLog(const logType: TLogLevel; const msg: string);
    procedure OnTeardownTest(const threadId: TThreadID; const Test: ITestInfo);
    procedure OnEndTeardownTest(const threadId: TThreadID; const Test: ITestInfo);
    procedure OnEndTest(const threadId: TThreadID; const Test: ITestResult);
    procedure OnTearDownFixture(const threadId: TThreadID;
      const fixture: ITestFixtureInfo);
    procedure OnEndTearDownFixture(const threadId: TThreadID;
      const fixture: ITestFixtureInfo);
    procedure OnEndTestFixture(const threadId: TThreadID;
      const results: IFixtureResult);
    procedure OnTestingEnds(const RunResults: IRunResults);
  end;

implementation

constructor TAMQPTestTimeLogger.Create;
begin
  inherited Create;
  FLinhas := TList<TAMQPTestTiming>.Create;
end;

destructor TAMQPTestTimeLogger.Destroy;
begin
  FLinhas.Free;
  inherited;
end;

// Guarda; não imprime. Ver "Como usar" no cabeçalho da unit.
procedure TAMQPTestTimeLogger.Anota(const AResultado: string;
  const ATest: ITestResult);
var
  LLinha: TAMQPTestTiming;
begin
  if ATest = nil then
    Exit;
  LLinha.Segundos := ATest.Duration.TotalSeconds;
  LLinha.Resultado := AResultado;
  LLinha.Nome := '';
  if ATest.Test <> nil then
    LLinha.Nome := ATest.Test.FullName;
  LLinha.Mensagem := ATest.Message;
  FLinhas.Add(LLinha);
end;

procedure TAMQPTestTimeLogger.Despeja;
var
  I: Integer;
  LLinha: TAMQPTestTiming;
  LMarca, LMsg: string;
  LTotal: Double;
  LSuspeitos: Integer;
begin
  if FLinhas.Count = 0 then
    Exit;

  System.Writeln;
  System.Writeln('tempo por teste');
  System.Writeln('---------------');

  LTotal := 0;
  LSuspeitos := 0;
  for I := 0 to FLinhas.Count - 1 do
  begin
    LLinha := FLinhas[I];
    LTotal := LTotal + LLinha.Segundos;
    // '~' marca o teste que se auto-ignorou -- pela MENSAGEM, não pelo tempo.
    if Pos(MARCA_IGNORADO, UpperCase(LLinha.Mensagem)) > 0 then
    begin
      LMarca := '~';
      Inc(LSuspeitos);
    end
    else
      LMarca := ' ';

    LMsg := LLinha.Mensagem;
    if LMsg <> '' then
      LMsg := '  <- ' + LMsg;

    System.Writeln(Format('%s%8.3fs  %-5s %s%s',
      [LMarca, LLinha.Segundos, LLinha.Resultado, LLinha.Nome, LMsg]));
  end;

  System.Writeln(Format('---------------  %d testes, %.3fs somados',
    [FLinhas.Count, LTotal]));
  if LSuspeitos = 1 then
    System.Writeln('1 auto-ignorado (~): NAO exercitou o alvo, apesar de contar '
      + 'como Passed no rodape -- ver a mensagem ao lado')
  else if LSuspeitos > 1 then
    System.Writeln(Format('%d auto-ignorados (~): NAO exercitaram o alvo, apesar '
      + 'de contarem como Passed no rodape -- ver a mensagem de cada um',
      [LSuspeitos]));
  FLinhas.Clear;
end;

procedure TAMQPTestTimeLogger.OnTestSuccess(const threadId: TThreadID;
  const Test: ITestResult);
begin
  Anota('ok', Test);
end;

procedure TAMQPTestTimeLogger.OnTestFailure(const threadId: TThreadID;
  const Failure: ITestError);
begin
  Anota('FALHA', Failure);
end;

procedure TAMQPTestTimeLogger.OnTestError(const threadId: TThreadID;
  const Error: ITestError);
begin
  Anota('ERRO', Error);
end;

procedure TAMQPTestTimeLogger.OnTestIgnored(const threadId: TThreadID;
  const AIgnored: ITestResult);
begin
  Anota('IGN', AIgnored);
end;

procedure TAMQPTestTimeLogger.OnTestMemoryLeak(const threadId: TThreadID;
  const Test: ITestResult);
begin
  Anota('LEAK', Test);
end;

procedure TAMQPTestTimeLogger.OnTestingEnds(const RunResults: IRunResults);
begin
  // Este logger é registrado ANTES do de console, então isto sai antes do
  // rodapé "Done testing / Tests Found: ..." -- que é exatamente onde a tabela
  // é útil: logo acima do resumo que ela qualifica.
  Despeja;
  System.Writeln(Format('tempo total da execucao: %.3fs',
    [RunResults.Duration.TotalSeconds]));
end;

{ --- daqui para baixo: exigências da interface, sem saída --- }

procedure TAMQPTestTimeLogger.OnTestingStarts(const threadId: TThreadID;
  testCount, testActiveCount: Cardinal);
begin
end;

procedure TAMQPTestTimeLogger.OnStartTestFixture(const threadId: TThreadID;
  const fixture: ITestFixtureInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnSetupFixture(const threadId: TThreadID;
  const fixture: ITestFixtureInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnEndSetupFixture(const threadId: TThreadID;
  const fixture: ITestFixtureInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnBeginTest(const threadId: TThreadID;
  const Test: ITestInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnSetupTest(const threadId: TThreadID;
  const Test: ITestInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnEndSetupTest(const threadId: TThreadID;
  const Test: ITestInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnExecuteTest(const threadId: TThreadID;
  const Test: ITestInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnLog(const logType: TLogLevel;
  const msg: string);
begin
end;

procedure TAMQPTestTimeLogger.OnTeardownTest(const threadId: TThreadID;
  const Test: ITestInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnEndTeardownTest(const threadId: TThreadID;
  const Test: ITestInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnEndTest(const threadId: TThreadID;
  const Test: ITestResult);
begin
  // A linha já saiu em OnTestSuccess/OnTestFailure/OnTestError/OnTestIgnored;
  // imprimir aqui também duplicaria.
end;

procedure TAMQPTestTimeLogger.OnTearDownFixture(const threadId: TThreadID;
  const fixture: ITestFixtureInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnEndTearDownFixture(const threadId: TThreadID;
  const fixture: ITestFixtureInfo);
begin
end;

procedure TAMQPTestTimeLogger.OnEndTestFixture(const threadId: TThreadID;
  const results: IFixtureResult);
begin
end;

end.
