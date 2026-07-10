program AMQP.UnitTests;

{$APPTYPE CONSOLE}
{$STRONGLINKTYPES ON}

uses
  System.SysUtils,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  DUnitX.TestFramework,
  AMQP.Protocol in '..\..\src\AMQP.Protocol.pas',
  AMQP.Wire in '..\..\src\AMQP.Wire.pas',
  AMQP.Frame in '..\..\src\AMQP.Frame.pas',
  AMQP.Method in '..\..\src\AMQP.Method.pas',
  AMQP.Connection.Methods in '..\..\src\AMQP.Connection.Methods.pas',
  AMQP.Channel.Methods in '..\..\src\AMQP.Channel.Methods.pas',
  AMQP.Exchange.Methods in '..\..\src\AMQP.Exchange.Methods.pas',
  AMQP.Queue.Methods in '..\..\src\AMQP.Queue.Methods.pas',
  AMQP.Basic.Methods in '..\..\src\AMQP.Basic.Methods.pas',
  AMQP.WireTests in 'AMQP.WireTests.pas',
  AMQP.FrameTests in 'AMQP.FrameTests.pas',
  AMQP.ConnectionMethodsTests in 'AMQP.ConnectionMethodsTests.pas',
  AMQP.Item2MethodsTests in 'AMQP.Item2MethodsTests.pas';

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
  nunitLogger: ITestLogger;
begin
  ReportMemoryLeaksOnShutdown := True;
  try
    TDUnitX.CheckCommandLine;

    if TDUnitX.Options.Include = '' then
      TDUnitX.Options.Include := '.';

    runner := TDUnitX.CreateRunner;
    runner.UseRTTI := True;
    runner.FailsOnNoAsserts := False;

    if TDUnitX.Options.ConsoleMode <> TDunitXConsoleMode.Off then
    begin
      logger := TDUnitXConsoleLogger.Create(
        TDUnitX.Options.ConsoleMode = TDunitXConsoleMode.Quiet);
      runner.AddLogger(logger);
    end;

    nunitLogger := TDUnitXXMLNUnitFileLogger.Create(TDUnitX.Options.XMLOutputFile);
    runner.AddLogger(nunitLogger);

    results := runner.Execute;

    if not results.AllPassed then
      System.ExitCode := EXIT_ERRORS;

    if (TDUnitX.Options.ExitBehavior = TDUnitXExitBehavior.Pause) and IsConsole then
    begin
      System.Write('Done.. press <Enter> key to quit.');
      System.Readln;
    end;
  except
    on E: Exception do
      System.Writeln(E.ClassName, ': ', E.Message);
  end;
end.
