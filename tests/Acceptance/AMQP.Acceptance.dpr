program AMQP.Acceptance;

{ Aceitacao -- WS9 da Fase 2 (decisao D8 do CLAUDE.md).

  Espelho Delphi de tests\Acceptance\fpc\AMQPAcceptanceFpc.lpr: roda
  EXATAMENTE a mesma suite de integracao do cliente, mas contra o BROKER
  EMBUTIDO desta lib em vez do RabbitMQ. Nenhum teste e' reescrito -- a unica
  diferenca e' o AmqpUseEmbeddedBroker abaixo.

  No build sem TLS de servidor (o backend SChannel desta lib so' implementa o
  lado cliente), o broker nao sobe porta cifrada e os testes de TLS se
  auto-ignoram -- o AMQP.IntegrationConfig aponta para uma porta onde nada
  escuta, de proposito, para a suite NAO cair de volta no RabbitMQ e reportar
  um verde falso. }

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
  AMQP.Connection.Methods in '..\..\src\AMQP.Connection.Methods.pas',
  AMQP.Channel.Methods in '..\..\src\AMQP.Channel.Methods.pas',
  AMQP.Exchange.Methods in '..\..\src\AMQP.Exchange.Methods.pas',
  AMQP.Queue.Methods in '..\..\src\AMQP.Queue.Methods.pas',
  AMQP.Basic.Methods in '..\..\src\AMQP.Basic.Methods.pas',
  AMQP.Transport.Tls in '..\..\src\AMQP.Transport.Tls.pas',
  AMQP.Connection in '..\..\src\AMQP.Connection.pas',
  AMQP.Server.Auth in '..\..\src\server\AMQP.Server.Auth.pas',
  AMQP.Server.Types in '..\..\src\server\AMQP.Server.Types.pas',
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
  AMQP.HandshakeIntegrationTests in '..\Integration\AMQP.HandshakeIntegrationTests.pas',
  AMQP.IntegrationConfig in '..\Integration\AMQP.IntegrationConfig.pas',
  AMQP.ChannelIntegrationTests in '..\Integration\AMQP.ChannelIntegrationTests.pas',
  AMQP.ConsumeIntegrationTests in '..\Integration\AMQP.ConsumeIntegrationTests.pas',
  AMQP.ReconnectIntegrationTests in '..\Integration\AMQP.ReconnectIntegrationTests.pas',
  AMQP.TlsIntegrationTests in '..\Integration\AMQP.TlsIntegrationTests.pas',
  AMQP.ReviewRegressionTests in '..\Integration\AMQP.ReviewRegressionTests.pas';

{$IFDEF AMQP_OPENSSL}
// Procura docker\certs subindo a partir da pasta do executavel (o exe roda de
// Win32\Debug ou Win32\OpenSSL, profundidades diferentes).
function AchaCerts(out ACert, AKey: string): Boolean;
var
  LDir: string;
  I: Integer;
begin
  LDir := ExtractFilePath(ParamStr(0));
  for I := 0 to 6 do
  begin
    ACert := LDir + 'docker\certs\server.crt';
    AKey := LDir + 'docker\certs\server.key';
    if FileExists(ACert) and FileExists(AKey) then
      Exit(True);
    LDir := LDir + '..\';
  end;
  Result := False;
end;
{$ENDIF}

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
  nunitLogger: ITestLogger;
  broker: TAMQPServer;
  portaTls: Word;
  {$IFDEF AMQP_OPENSSL}
  brokerTls: TAMQPServer;
  cert, key: string;
  {$ENDIF}
begin
  ReportMemoryLeaksOnShutdown := True;
  broker := TAMQPServer.Create;
  portaTls := 0;
  {$IFDEF AMQP_OPENSSL}
  brokerTls := nil;
  {$ENDIF}
  try
    broker.BindAddress := '127.0.0.1';
    broker.Port := 0; // porta efemera
    broker.Start;

    // Sem isto o build OpenSSL rodava a aceitacao SEM exercitar TLS de
    // servidor: os 5 testes de TLS se auto-ignoravam em 0.000s e o resumo
    // dizia "28 Passed", escondendo que 5 nem tocaram no broker. O XML
    // (dunitx-results.xml) foi o que expos isso -- tempo zero.
    {$IFDEF AMQP_OPENSSL}
    if AchaCerts(cert, key) then
    begin
      brokerTls := TAMQPServer.Create;
      brokerTls.BindAddress := '127.0.0.1';
      brokerTls.Port := 0;
      brokerTls.UseTls := True;
      brokerTls.TlsCertFile := cert;
      brokerTls.TlsKeyFile := key;
      try
        brokerTls.Start;
        portaTls := brokerTls.Port;
      except
        on E: Exception do
        begin
          System.Writeln('AVISO: broker TLS nao subiu (', E.Message, ')');
          FreeAndNil(brokerTls);
        end;
      end;
    end
    else
      System.Writeln('AVISO: docker\certs nao encontrado -- sem porta TLS');
    {$ENDIF}

    AmqpUseEmbeddedBroker(broker.Port, portaTls);
    System.Writeln('broker embutido em 127.0.0.1:', broker.Port,
      ' (tls: ', portaTls, ')');

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
  finally
    {$IFDEF AMQP_OPENSSL}
    brokerTls.Free;
    {$ENDIF}
    broker.Free;
  end;
end.
