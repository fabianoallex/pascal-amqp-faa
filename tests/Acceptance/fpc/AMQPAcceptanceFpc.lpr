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
  docker\certs e eles rodam de verdade.

  DUAS PASSADAS: TRANSIENTE E DURAVEL (WS10 da Fase 4)
  ----------------------------------------------------
  Com --durable o broker embutido sobe com DataDir apontando para um diretorio
  temporario, e as MESMAS 28 rodam de novo. A pergunta que a segunda passada
  responde e' diferente da primeira, e e' a razao de ela existir: ligar a
  durabilidade NAO PODE MUDAR A SEMANTICA que o cliente ve'.

  E' facil escrever um broker durave que quebre algo sutil -- a ordem dos
  confirms, o momento do Basic.Return, o requeue de uma nao-confirmada -- e
  nenhum teste da suite do SERVER perceberia, porque todos eles ja' foram
  escritos sabendo da durabilidade. Estes 28 nao: sao os testes do CLIENTE,
  escritos contra o RabbitMQ, muito antes de existir Fase 4.

  Duas passadas, dois processos:

    .\AMQPAcceptanceFpc.exe --all --format=plain
    set AMQP_ACEITACAO_DURAVEL=1 && .\AMQPAcceptanceFpc.exe --all --format=plain

  Processos separados de proposito: um broker duravel recem-criado num
  diretorio limpo e' o estado que interessa, e reaproveitar o processo
  carregaria o estado da primeira passada para a segunda. }

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

// True quando a variavel de ambiente pede a passada duravel.
//
// VARIAVEL, E NAO FLAG, e a razao e' pratica: tanto o runner do FPCUnit quanto
// o do DUnitX parseiam a linha de comando inteira e RECUSAM opcao que nao
// conhecem ("Invalid option at position 1"). Um --durable morreria antes de
// chegar aos testes. A variavel passa ao largo dos dois parsers e vale
// igualzinho nos dois espelhos.
function PediuDuravel: Boolean;
var
  LVal: string;
begin
  LVal := LowerCase(Trim(GetEnvironmentVariable('AMQP_ACEITACAO_DURAVEL')));
  Result := (LVal <> '') and (LVal <> '0') and (LVal <> 'false');
end;

// Diretorio limpo para a passada duravel. Nao reaproveita: um WAL de outra
// execucao faria a suite comecar com filas e mensagens que ela nao criou.
function DirDuravel: string;
begin
  Result := IncludeTrailingPathDelimiter(GetTempDir)
    + 'amqpaceita-' + IntToStr(GetProcessID);
  ForceDirectories(Result);
end;

procedure LimpaDir(const ADir: string);
var
  LRec: TSearchRec;
begin
  if FindFirst(IncludeTrailingPathDelimiter(ADir) + '*', faAnyFile, LRec) = 0 then
  begin
    try
      repeat
        if (LRec.Attr and faDirectory) = 0 then
          SysUtils.DeleteFile(IncludeTrailingPathDelimiter(ADir) + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      SysUtils.FindClose(LRec);
    end;
  end;
end;

// Sobe o broker em 127.0.0.1 numa porta efemera. Devolve a porta escolhida.
function IniciaBroker(out ABroker: TAMQPServer;
  const ADataDir: string): Word;
begin
  ABroker := TAMQPServer.Create;
  ABroker.BindAddress := '127.0.0.1';
  ABroker.Port := 0; // o SO escolhe
  ABroker.DataDir := ADataDir; // '' = broker da Fase 3, letra por letra (D19)
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
  LDataDir: string;
  {$IFDEF AMQP_OPENSSL}
  LBrokerTls: TAMQPServer;
  {$ENDIF}
begin
  SetMultiByteConversionCodePage(CP_UTF8);

  LDataDir := '';
  if PediuDuravel then
  begin
    LDataDir := DirDuravel;
    LimpaDir(LDataDir);
  end;
  LPorta := IniciaBroker(LBroker, LDataDir);
  LPortaTls := 0;
  {$IFDEF AMQP_OPENSSL}
  LBrokerTls := nil;
  LPortaTls := IniciaBrokerTls(LBrokerTls);
  {$ENDIF}
  try
    AmqpUseEmbeddedBroker(LPorta, LPortaTls);
    WriteLn('broker embutido em 127.0.0.1:', LPorta,
      ' (tls: ', LPortaTls, ')');
    if LDataDir <> '' then
      WriteLn('PASSADA DURAVEL -- DataDir: ', LDataDir)
    else
      WriteLn('passada transiente -- sem DataDir');

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
    if LDataDir <> '' then
      LimpaDir(LDataDir);
  end;
end.
