program AMQPAcceptanceFpc;

{ Aceitacao -- WS9 da Fase 2 (decisao D8 do CLAUDE.md).

  Roda EXATAMENTE a mesma suite de integracao do cliente, mas contra o BROKER
  EMBUTIDO desta lib em vez do RabbitMQ. Nenhum teste e' reescrito: a unica
  diferenca e' o AmqpUseEmbeddedBroker abaixo, que redireciona o
  AMQP.IntegrationConfig para a porta efemera do broker in-process.

  E' o teste que mede se o broker se comporta como o RabbitMQ na pratica --
  com um cliente que NAO foi escrito para ele.

    .\AMQPAcceptanceFpc.exe --all --format=plain

  TLS: no build Default o broker nao tem TLS (o backend SChannel desta lib so'
  implementa o lado cliente), entao os testes de TLS nao acham porta e se
  auto-ignoram -- mesmo caminho de quando o broker TLS esta fora do ar. No
  build `openssl` o broker sobe uma segunda porta cifrada com os certs de
  docker\certs e eles rodam de verdade. }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  SysUtils, Classes, consoletestrunner, testregistry,
  AMQP.Server.Types,
  AMQP.Server.Broker,
  AMQP.IntegrationConfig,
  AMQP.HandshakeIntegrationTests,
  AMQP.ChannelIntegrationTests,
  AMQP.ConsumeIntegrationTests,
  AMQP.ReconnectIntegrationTests,
  AMQP.TlsIntegrationTests,
  AMQP.ReviewRegressionTests;

// Sobe o broker em 127.0.0.1 numa porta efemera. Devolve a porta escolhida.
function IniciaBroker(out ABroker: TAMQPServer): Word;
begin
  ABroker := TAMQPServer.Create;
  ABroker.BindAddress := '127.0.0.1';
  ABroker.Port := 0; // o SO escolhe
  ABroker.Start;
  Result := ABroker.Port;
end;

{$IFDEF AMQP_OPENSSL}
// Procura docker\certs subindo a partir da pasta do executavel (o runner roda
// de profundidades diferentes conforme o build).
function AchaCerts(out ACert, AKey: string): Boolean;
var
  LDir: string;
  I: Integer;
begin
  LDir := ExtractFilePath(ParamStr(0));
  for I := 0 to 6 do
  begin
    ACert := LDir + 'docker' + PathDelim + 'certs' + PathDelim + 'server.crt';
    AKey := LDir + 'docker' + PathDelim + 'certs' + PathDelim + 'server.key';
    if FileExists(ACert) and FileExists(AKey) then
      Exit(True);
    LDir := LDir + '..' + PathDelim;
  end;
  Result := False;
end;

function IniciaBrokerTls(out ABroker: TAMQPServer): Word;
var
  LCert, LKey: string;
begin
  Result := 0;
  ABroker := nil;
  if not AchaCerts(LCert, LKey) then
  begin
    WriteLn('AVISO: docker\certs nao encontrado -- testes TLS vao se auto-ignorar');
    Exit;
  end;
  ABroker := TAMQPServer.Create;
  ABroker.BindAddress := '127.0.0.1';
  ABroker.Port := 0;
  ABroker.UseTls := True;
  ABroker.TlsCertFile := LCert;
  ABroker.TlsKeyFile := LKey;
  try
    ABroker.Start;
    Result := ABroker.Port;
  except
    on E: Exception do
    begin
      WriteLn('AVISO: broker TLS nao subiu (', E.Message,
        ') -- testes TLS vao se auto-ignorar');
      FreeAndNil(ABroker);
      Result := 0;
    end;
  end;
end;
{$ENDIF}

var
  ConsoleApp: TTestRunner;
  LBroker: TAMQPServer;
  LPorta, LPortaTls: Word;
  {$IFDEF AMQP_OPENSSL}
  LBrokerTls: TAMQPServer;
  {$ENDIF}
begin
  SetMultiByteConversionCodePage(CP_UTF8);

  LPorta := IniciaBroker(LBroker);
  LPortaTls := 0;
  {$IFDEF AMQP_OPENSSL}
  LBrokerTls := nil;
  LPortaTls := IniciaBrokerTls(LBrokerTls);
  {$ENDIF}
  try
    AmqpUseEmbeddedBroker(LPorta, LPortaTls);
    WriteLn('broker embutido em 127.0.0.1:', LPorta,
      ' (tls: ', LPortaTls, ')');

    DefaultFormat := fPlain;
    DefaultRunAllTests := True;
    ConsoleApp := TTestRunner.Create(nil);
    try
      ConsoleApp.Initialize;
      ConsoleApp.Title := 'pascal-amqp-faa - aceitacao contra o broker embutido';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  finally
    {$IFDEF AMQP_OPENSSL}
    LBrokerTls.Free;
    {$ENDIF}
    LBroker.Free;
  end;
end.
