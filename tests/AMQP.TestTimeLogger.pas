unit AMQP.TestTimeLogger;

{ DUnitX console logger that prints the TIME of each test.

  Why it exists
  -------------
  The built-in TDUnitXConsoleLogger is not configurable (only `quietMode` in
  the constructor) and never prints duration. That matters in this codebase for a
  concrete reason, discovered in WS9 of Phase 2: DUnitX **has no "ignored"
  bucket** — the auto-ignore used here (TLS tests without a broker, tests that
  depend on a Phase 3 feature) is `Assert.Pass('IGNORADO: ...')`, which the
  report counts as **Passed**. A "28 Passed / 0 Ignored" summary can
  hide 5 tests that never touched the target.

  What tells the two apart is the **message**: the auto-ignore here always passes
  `Assert.Pass` a text starting with `IGNORADO:`, and that is what this logger
  marks with `~`. (Time *almost* worked — auto-ignored finishes in ~0.000s — but it
  is a heuristic: against the embedded broker there are real tests under 0.5 ms, and the
  first version of this logger produced 3 false positives out of 28. The XML's
  `asserts` field does not work either: it is 0 even for a test that ran.)

  The duration comes from `ITestResult.Duration` (via `IResult`), exactly the same
  source the XML logger uses for `time=` — the two numbers match.

  How to use
  ----------
  `runner.AddLogger(TAMQPTestTimeLogger.Create);` **before** the console logger.
  The order matters: the runner walks the loggers in registration order, and the footer
  ("Done testing / Tests Found: ...") comes from the console logger's
  `OnTestingEnds`. Registering this one first, the timing table appears **between** the
  tests' detailed output and that footer.

  It prints nothing during the run: it accumulates in memory and dumps everything
  at once at the end. This way DUnitX's standard output (Setup/Executing/Teardown of
  each test) stays intact, without a line of ours interleaved with each test.

  The FPCUnit suite already prints time per test natively — this aligns both
  sides. }

interface

uses
  System.SysUtils,
  System.TimeSpan,
  System.Generics.Collections,
  DUnitX.TestFramework;

type
  /// One row of the table: what was measured for a test.
  TAMQPTestTiming = record
    Nome: string;
    Segundos: Double;
    Resultado: string;
    Mensagem: string;
  end;

  TAMQPTestTimeLogger = class(TInterfacedObject, ITestLogger)
  private
    /// Prefix this codebase's auto-ignore uses in `Assert.Pass` — it is the
    /// EXACT signal of "this test did not exercise the target". The previous version
    /// of this logger inferred it from the time (< 0.5 ms) and got it wrong: against the
    /// embedded broker, tests that really run finish in less than that —
    /// measured, 3 false positives in a run of 28. Time is a heuristic; the
    /// message is fact.
    const MARCA_IGNORADO = 'IGNORADO';
  private
    FLinhas: TList<TAMQPTestTiming>;
    procedure Anota(const AResultado: string; const ATest: ITestResult);
    procedure Despeja;
  public
    constructor Create;
    destructor Destroy; override;

    // --- ITestLogger: only three of these do anything; the rest is required by
    //     the interface (DUnitX has no "adapter" base class). ---
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

// Stores; does not print. See "How to use" in the unit header.
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
    // '~' marks the test that ignored itself -- by the MESSAGE, not by the time.
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
  // This logger is registered BEFORE the console one, so this comes out before the
  // "Done testing / Tests Found: ..." footer -- which is exactly where the table
  // is useful: right above the summary it qualifies.
  Despeja;
  System.Writeln(Format('tempo total da execucao: %.3fs',
    [RunResults.Duration.TotalSeconds]));
end;

{ --- from here down: interface requirements, no output --- }

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
  // The line already came out in OnTestSuccess/OnTestFailure/OnTestError/OnTestIgnored;
  // printing here too would duplicate it.
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
