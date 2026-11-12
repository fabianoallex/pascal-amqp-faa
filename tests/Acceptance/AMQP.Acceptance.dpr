program AMQP.Acceptance;

{ Acceptance -- WS9 of Phase 2 (decision D8 in CLAUDE.md).

  Delphi mirror of tests\Acceptance\fpc\AMQPAcceptanceFpc.lpr: runs
  EXACTLY the same client integration suite, but against this library's
  EMBEDDED BROKER instead of RabbitMQ. No test is rewritten -- the only
  difference is the AmqpUseEmbeddedBroker below.

  TWO PASSES: TRANSIENT AND DURABLE (WS10 of Phase 4)
  ---------------------------------------------------
  With the environment variable AMQP_ACCEPTANCE_DURABLE=1 the embedded broker
  starts with DataDir in a temporary directory, and the SAME 48 run again. The
  question of the second pass is different from the first: turning durability
  on MUST NOT CHANGE THE SEMANTICS the client sees. It is easy to break
  something subtle -- the order of the confirms, the moment of Basic.Return,
  the requeue of an unacknowledged one -- without any test of the SERVER suite
  noticing, because all of them were written already knowing about
  durability. These 48 were not: they are the CLIENT tests, written against
  RabbitMQ long before Phase 4 existed.

  VARIABLE, NOT FLAG: the DUnitX runner (like the FPCUnit one) parses the whole
  command line and rejects options it does not know. A --durable would die
  before reaching the tests.

    AMQP.Acceptance.exe
    set AMQP_ACCEPTANCE_DURABLE=1 && AMQP.Acceptance.exe

  Every build has a TLS broker since 0.6.0 (both backends serve the server
  side). If the encrypted port does not come up (no PKI found, for example),
  the TLS tests ignore themselves -- AMQP.IntegrationConfig points to a port
  where nothing listens, on purpose, so the suite does NOT fall back to
  RabbitMQ and report a false green. }

{$APPTYPE CONSOLE}
{$STRONGLINKTYPES ON}

uses
  System.SysUtils,
  System.IOUtils,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  DUnitX.TestFramework,
  PascalCommon.Transport,
  AMQP.TestTimeLogger in '..\AMQP.TestTimeLogger.pas',
  AMQP.Protocol in '..\..\src\AMQP.Protocol.pas',
  AMQP.Wire in '..\..\src\AMQP.Wire.pas',
  AMQP.Frame in '..\..\src\AMQP.Frame.pas',
  AMQP.Method in '..\..\src\AMQP.Method.pas',
  AMQP.Connection.Methods in '..\..\src\AMQP.Connection.Methods.pas',
  AMQP.Channel.Methods in '..\..\src\AMQP.Channel.Methods.pas',
  AMQP.Exchange.Methods in '..\..\src\AMQP.Exchange.Methods.pas',
  AMQP.Queue.Methods in '..\..\src\AMQP.Queue.Methods.pas',
  AMQP.Basic.Methods in '..\..\src\AMQP.Basic.Methods.pas',
  AMQP.Tracing in '..\..\src\AMQP.Tracing.pas',
  AMQP.Transport in '..\..\src\AMQP.Transport.pas',
  AMQP.Connection in '..\..\src\AMQP.Connection.pas',
  AMQP.Server.Auth in '..\..\src\server\AMQP.Server.Auth.pas',
  AMQP.Server.Types in '..\..\src\server\AMQP.Server.Types.pas',
  AMQP.Server.Events in '..\..\src\server\AMQP.Server.Events.pas',
  AMQP.Server.EventBus in '..\..\src\server\AMQP.Server.EventBus.pas',
  AMQP.Server.Message in '..\..\src\server\AMQP.Server.Message.pas',
  AMQP.Server.Header in '..\..\src\server\AMQP.Server.Header.pas',
  AMQP.Server.Resources in '..\..\src\server\AMQP.Server.Resources.pas',
  AMQP.Server.Routing in '..\..\src\server\AMQP.Server.Routing.pas',
  AMQP.Server.VHost in '..\..\src\server\AMQP.Server.VHost.pas',
  AMQP.Server.Queue in '..\..\src\server\AMQP.Server.Queue.pas',
  AMQP.Server.FrameIO in '..\..\src\server\AMQP.Server.FrameIO.pas',
  AMQP.Server.Delivery in '..\..\src\server\AMQP.Server.Delivery.pas',
  AMQP.Server.Engine in '..\..\src\server\AMQP.Server.Engine.pas',
  AMQP.Server.Channel in '..\..\src\server\AMQP.Server.Channel.pas',
  AMQP.Server.Connection in '..\..\src\server\AMQP.Server.Connection.pas',
  AMQP.Server.Broker in '..\..\src\server\AMQP.Server.Broker.pas',
  AMQP.Server.Wal in '..\..\src\server\AMQP.Server.Wal.pas',
  AMQP.Server.Journal in '..\..\src\server\AMQP.Server.Journal.pas',
  AMQP.Server.Records in '..\..\src\server\AMQP.Server.Records.pas',
  AMQP.Server.Confirm in '..\..\src\server\AMQP.Server.Confirm.pas',
  AMQP.Server.Recovery in '..\..\src\server\AMQP.Server.Recovery.pas',
  AMQP.HandshakeIntegrationTests in '..\Integration\AMQP.HandshakeIntegrationTests.pas',
  AMQP.DUnitXCompat in '..\Unit\AMQP.DUnitXCompat.pas',
  AMQP.TestSupport in '..\AMQP.TestSupport.pas',
  AMQP.IntegrationConfig in '..\AMQP.IntegrationConfig.pas',
  AMQP.ChannelIntegrationTests in '..\Integration\AMQP.ChannelIntegrationTests.pas',
  AMQP.ConsumeIntegrationTests in '..\Integration\AMQP.ConsumeIntegrationTests.pas',
  AMQP.ConsumerErrorIntegrationTests in '..\Integration\AMQP.ConsumerErrorIntegrationTests.pas',
  AMQP.ChannelCloseIntegrationTests in '..\Integration\AMQP.ChannelCloseIntegrationTests.pas',
  AMQP.ReconnectIntegrationTests in '..\Integration\AMQP.ReconnectIntegrationTests.pas',
  AMQP.TlsIntegrationTests in '..\Integration\AMQP.TlsIntegrationTests.pas',
  AMQP.ReviewRegressionTests in '..\Integration\AMQP.ReviewRegressionTests.pas';

// The embedded TLS broker's certificate: the test PKI of pascal-common-faa
// (AMQP.TestSupport.Pki), PEM pair on OpenSSL, PFX on SChannel. Since 0.6.0
// every build has a TLS broker (SChannel serves the server side too).
procedure ConfigureCertificate(ABroker: TAMQPServer);
begin
  if AmqpResolveTlsBackend(tbDefault) = tbOpenSsl then
  begin
    ABroker.TlsCertFile := Pki('srv_cert.pem');
    ABroker.TlsKeyFile := Pki('srv_key.pem');
  end
  else
  begin
    ABroker.TlsCertFile := Pki('srv.pfx');
    ABroker.TlsCertPassword := PKI_PFX_PASSWORD;
  end;
end;

// True when the environment variable asks for the durable pass.
function DurableRequested: Boolean;
var
  LVal: string;
begin
  LVal := LowerCase(Trim(GetEnvironmentVariable('AMQP_ACCEPTANCE_DURABLE')));
  Result := (LVal <> '') and (LVal <> '0') and (LVal <> 'false');
end;

// Clean directory for the durable pass. Not reused: a WAL from another run
// would make the suite start with queues and messages it did not create.
function DurableDir: string;
begin
  // Unique name without needing the PID -- getting the PID would require
  // Winapi.Windows, and that unit shadows FindClose and DeleteFile from
  // System.SysUtils (see the pitfalls table in CLAUDE.md). Not worth it just
  // for a folder name.
  Result := IncludeTrailingPathDelimiter(TPath.GetTempPath)
    + 'amqpaceita-' + TPath.GetGUIDFileName(False);
  ForceDirectories(Result);
end;

procedure ClearDir(const ADir: string);
var
  LRec: TSearchRec;
begin
  if FindFirst(IncludeTrailingPathDelimiter(ADir) + '*', faAnyFile, LRec) = 0 then
  begin
    try
      repeat
        if (LRec.Attr and faDirectory) = 0 then
          System.SysUtils.DeleteFile(
            IncludeTrailingPathDelimiter(ADir) + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      System.SysUtils.FindClose(LRec);
    end;
  end;
end;

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
  nunitLogger: ITestLogger;
  broker: TAMQPServer;
  tlsPort: Word;
  dataDir: string;
  brokerTls: TAMQPServer;
begin
  ReportMemoryLeaksOnShutdown := True;
  broker := TAMQPServer.Create;
  tlsPort := 0;
  brokerTls := nil;
  try
    broker.BindAddress := '127.0.0.1';
    broker.Port := 0; // ephemeral port
    dataDir := '';
    if DurableRequested then
    begin
      dataDir := DurableDir;
      ClearDir(dataDir);
    end;
    broker.DataDir := dataDir; // '' = the Phase 3 broker, to the letter (D19)
    broker.Start;
    if dataDir <> '' then
      WriteLn('PASSADA DURAVEL -- DataDir: ', dataDir)
    else
      WriteLn('passada transiente -- sem DataDir');

    // Without this the OpenSSL build ran the acceptance suite WITHOUT
    // exercising server-side TLS: the 5 TLS tests ignored themselves in 0.000s
    // and the summary said "28 Passed", hiding that 5 never touched the
    // broker. The XML (dunitx-results.xml) is what exposed it -- zero time.
    brokerTls := TAMQPServer.Create;
    brokerTls.BindAddress := '127.0.0.1';
    brokerTls.Port := 0;
    brokerTls.UseTls := True;
    try
      ConfigureCertificate(brokerTls);
      brokerTls.Start;
      tlsPort := brokerTls.Port;
    except
      on E: Exception do
      begin
        System.Writeln('AVISO: broker TLS nao subiu (', E.Message, ')');
        FreeAndNil(brokerTls);
      end;
    end;

    AmqpUseEmbeddedBroker(broker.Port, tlsPort);
    System.Writeln('broker embutido em 127.0.0.1:', broker.Port,
      ' (tls: ', tlsPort, ')');

    try
      TDUnitX.CheckCommandLine;

      if TDUnitX.Options.Include = '' then
        TDUnitX.Options.Include := '.';

      runner := TDUnitX.CreateRunner;
      runner.UseRTTI := True;
      runner.FailsOnNoAsserts := False;

      // BEFORE the console logger, on purpose: the runner walks the loggers
      // in registration order, and the "Done testing" footer comes from the
      // console's OnTestingEnds. That way the timing table shows up between
      // the detailed output and the summary it qualifies.
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
  finally
    brokerTls.Free;
    broker.Free;
  end;
end.
