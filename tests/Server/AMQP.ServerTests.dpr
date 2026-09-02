program AMQP.ServerTests;

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
  AMQP.Threading in '..\..\src\AMQP.Threading.pas',
  AMQP.Transport in '..\..\src\AMQP.Transport.pas',
  AMQP.Transport.Tls in '..\..\src\AMQP.Transport.Tls.pas',
  AMQP.Connection.Methods in '..\..\src\AMQP.Connection.Methods.pas',
  AMQP.Channel.Methods in '..\..\src\AMQP.Channel.Methods.pas',
  AMQP.Exchange.Methods in '..\..\src\AMQP.Exchange.Methods.pas',
  AMQP.Queue.Methods in '..\..\src\AMQP.Queue.Methods.pas',
  AMQP.Basic.Methods in '..\..\src\AMQP.Basic.Methods.pas',
  AMQP.Connection in '..\..\src\AMQP.Connection.pas',
  AMQP.Server.Auth in '..\..\src\server\AMQP.Server.Auth.pas',
  AMQP.Server.Types in '..\..\src\server\AMQP.Server.Types.pas',
  AMQP.Server.Channel in '..\..\src\server\AMQP.Server.Channel.pas',
  AMQP.Server.FrameIO in '..\..\src\server\AMQP.Server.FrameIO.pas',
  AMQP.Server.Connection in '..\..\src\server\AMQP.Server.Connection.pas',
  AMQP.Server.Broker in '..\..\src\server\AMQP.Server.Broker.pas',
  AMQP.ServerSkeletonTests in 'AMQP.ServerSkeletonTests.pas',
  AMQP.ServerHandshakeTests in 'AMQP.ServerHandshakeTests.pas';

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
