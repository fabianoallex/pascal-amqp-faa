program AMQPIntegrationTestsFpc;

{ FPCUnit runner of the integration tests (same coverage as the DUnitX/Delphi
  tests/Integration/*.pas, ported to FPCUnit). Needs a real RabbitMQ on
  localhost:5672 (docker compose -f docker/docker-compose.yml up -d); for the
  TLS tests, add docker-compose.tls.yml — without the TLS broker they ignore
  themselves.

  Console (text output), when called with any parameter:
    .\AMQPIntegrationTestsFpc.exe --all --format=plain
  GUI (window with the test tree + green/red bar), without parameters:
    .\AMQPIntegrationTestsFpc.exe
  Outside Windows it always runs in console mode (no LCL/widgetset), with or
  without parameters. AMQP.TlsIntegrationTests is included on every platform: the
  default backend is SChannel on Windows and OpenSSL elsewhere (the openssl
  build mode makes it OpenSSL on Windows too). Without a TLS broker on 5671
  the TLS tests ignore themselves.

  See CLAUDE.md for the workarounds for FPC 3.2.2 internal errors and for
  TInterlocked, which does not exist in FPC (use PcAtomic* from
  PascalCommon.Threading). }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  {$IFDEF MSWINDOWS}
  Interfaces, Forms, GuiTestRunner,
  {$ENDIF}
  Classes, consoletestrunner, testregistry,
  AMQP.IntegrationConfig,
  AMQP.HandshakeIntegrationTests,
  AMQP.ChannelIntegrationTests,
  AMQP.ConsumeIntegrationTests,
  AMQP.ConsumerErrorIntegrationTests,
  AMQP.ChannelCloseIntegrationTests,
  AMQP.ReconnectIntegrationTests,
  AMQP.TlsIntegrationTests,
  AMQP.ReviewRegressionTests;

var
  ConsoleApp: TTestRunner;
begin
  // Plain FPC console (outside a Lazarus app): DefaultSystemCodePage is not
  // UTF-8 by default, so accented strings in the tests would be transcoded
  // wrongly. See CLAUDE.md.
  SetMultiByteConversionCodePage(CP_UTF8);

  {$IFDEF MSWINDOWS}
  if ParamCount = 0 then
  begin
    Application.Initialize;
    Application.CreateForm(TGUITestRunner, TestRunner);
    Application.Run;
  end
  else
  {$ENDIF}
  begin
    DefaultFormat := fPlain;
    DefaultRunAllTests := True;
    ConsoleApp := TTestRunner.Create(nil);
    try
      ConsoleApp.Initialize;
      ConsoleApp.Title := 'pascal-amqp-faa - testes de integracao (FPCUnit)';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  end;
end.
