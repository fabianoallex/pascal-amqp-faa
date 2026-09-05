program AMQP.ServerTests;

{$APPTYPE CONSOLE}
{$STRONGLINKTYPES ON}

uses
  System.SysUtils,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  DUnitX.TestFramework,
  AMQP.TestTimeLogger in '..\AMQP.TestTimeLogger.pas',
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
  AMQP.Server.Message in '..\..\src\server\AMQP.Server.Message.pas',
  AMQP.Server.Header in '..\..\src\server\AMQP.Server.Header.pas',
  AMQP.Server.Resources in '..\..\src\server\AMQP.Server.Resources.pas',
  AMQP.Server.Routing in '..\..\src\server\AMQP.Server.Routing.pas',
  AMQP.Server.VHost in '..\..\src\server\AMQP.Server.VHost.pas',
  AMQP.Server.Queue in '..\..\src\server\AMQP.Server.Queue.pas',
  AMQP.Server.Delivery in '..\..\src\server\AMQP.Server.Delivery.pas',
  AMQP.Server.Engine in '..\..\src\server\AMQP.Server.Engine.pas',
  AMQP.Server.FrameIO in '..\..\src\server\AMQP.Server.FrameIO.pas',
  AMQP.Server.Connection in '..\..\src\server\AMQP.Server.Connection.pas',
  AMQP.Server.Broker in '..\..\src\server\AMQP.Server.Broker.pas',
  AMQP.Server.Wal in '..\..\src\server\AMQP.Server.Wal.pas',
  AMQP.Server.Journal in '..\..\src\server\AMQP.Server.Journal.pas',
  AMQP.Server.Records in '..\..\src\server\AMQP.Server.Records.pas',
  AMQP.Server.Confirm in '..\..\src\server\AMQP.Server.Confirm.pas',
  AMQP.ServerSkeletonTests in 'AMQP.ServerSkeletonTests.pas',
  AMQP.ServerHandshakeTests in 'AMQP.ServerHandshakeTests.pas',
  AMQP.ServerEngineTests in 'AMQP.ServerEngineTests.pas',
  AMQP.ServerRoutingTests in 'AMQP.ServerRoutingTests.pas',
  AMQP.ServerTestDoubles in 'AMQP.ServerTestDoubles.pas',
  AMQP.ServerQueueTests in 'AMQP.ServerQueueTests.pas',
  AMQP.ServerDeliveryTests in 'AMQP.ServerDeliveryTests.pas',
  AMQP.ServerEngineDispatchTests in 'AMQP.ServerEngineDispatchTests.pas',
  AMQP.ServerWalTests in 'AMQP.ServerWalTests.pas',
  AMQP.ServerJournalTests in 'AMQP.ServerJournalTests.pas',
  AMQP.ServerTopologyTests in 'AMQP.ServerTopologyTests.pas',
  AMQP.ServerPersistTests in 'AMQP.ServerPersistTests.pas',
  AMQP.ServerConfirmTests in 'AMQP.ServerConfirmTests.pas';

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

    // ANTES do logger de console, de proposito: o runner percorre os
    // loggers na ordem de registro, e o rodape "Done testing" sai do
    // OnTestingEnds do console. Assim a tabela de tempos aparece entre a
    // saida detalhada e o resumo que ela qualifica.
    runner.AddLogger(TAMQPTestTimeLogger.Create);

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
