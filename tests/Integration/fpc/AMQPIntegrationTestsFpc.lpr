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

  Ver CLAUDE.md para os workarounds de erros internos do FPC 3.2.2 e o
  TInterlocked que nao existe no FPC (usar AmqpAtomic* de AMQP.Threading). }

{$mode delphi}{$H+}

uses
  Interfaces, Forms,
  Classes, consoletestrunner, testregistry, GuiTestRunner,
  AMQP.HandshakeIntegrationTests,
  AMQP.ChannelIntegrationTests,
  AMQP.ConsumeIntegrationTests,
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

  if ParamCount > 0 then
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
  end
  else
  begin
    Application.Initialize;
    Application.CreateForm(TGUITestRunner, TestRunner);
    Application.Run;
  end;
end.
