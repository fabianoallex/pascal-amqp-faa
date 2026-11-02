program AMQPIntegrationTestsFpc;

{ Runner FPCUnit dos testes de integração (mesma cobertura do
  tests/Integration/*.pas DUnitX/Delphi, portada para FPCUnit). Precisa de um
  RabbitMQ real em localhost:5672 (docker compose -f docker/docker-compose.yml
  up -d); para os testes TLS, soma docker-compose.tls.yml — sem o broker TLS
  eles se auto-ignoram.

  Console (saida de texto), quando chamado com qualquer parametro:
    .\AMQPIntegrationTestsFpc.exe --all --format=plain
  GUI (janela com arvore de testes + barra verde/vermelha), sem parametros:
    .\AMQPIntegrationTestsFpc.exe
  Fora do Windows roda sempre em modo console (sem LCL/widgetset), com ou
  sem parametros. AMQP.TlsIntegrationTests entra em toda plataforma: the
  default backend is SChannel on Windows and OpenSSL elsewhere (the openssl
  build mode makes it OpenSSL on Windows too). Without a TLS broker on 5671
  the TLS tests ignore themselves.

  Ver CLAUDE.md para os workarounds de erros internos do FPC 3.2.2 e o
  TInterlocked que nao existe no FPC (usar PcAtomic* de PascalCommon.Threading). }

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
  // Console FPC puro (fora de app Lazarus): DefaultSystemCodePage nao e' UTF-8
  // por padrao, entao strings acentuadas nos testes seriam transcodificadas
  // errado. Ver CLAUDE.md.
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
