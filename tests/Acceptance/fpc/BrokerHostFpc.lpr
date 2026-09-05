program BrokerHostFpc;

{ Hospeda o broker embutido para o SmokeTest rodar contra ele -- WS9 da Fase 2.

  Por que um processo separado em vez de um "--embedded" dentro do SmokeTest:
  o SmokeTest e' um SAMPLE do cliente, e ele tem valor justamente por ser 100%
  cliente -- fazer o sample linkar o sub-modulo server mudaria o que ele
  demonstra e o que ele exige para compilar. Com dois processos, cada um fica
  puro: aqui o broker, la' o cliente.

    .\BrokerHostFpc.exe [porta] [--tls porta-tls] [--seconds N]
                        [--datadir <dir>]
    .\SmokeTest.exe --host 127.0.0.1 --port <porta>

  --datadir liga a durabilidade (Fase 4). E' o que permite o passo de RESTART
  do SmokeTest: sobe o host com um diretorio, o SmokeTest publica persistente,
  o host e' morto e sobe de novo no MESMO diretorio, e o SmokeTest confere que
  as mensagens estao la'. Ver tests\tools\smoke_restart.py.

  Sem argumentos: porta 5673 (fora do 5672 do RabbitMQ de dev, para os dois
  poderem coexistir), sem TLS, e fica no ar por 120 s ou ate' receber uma
  linha na entrada padrao. }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  SysUtils, Classes,
  AMQP.Threading,
  AMQP.Server.Types,
  AMQP.Server.Broker;

const
  PORTA_PADRAO = 5673;
  SEGUNDOS_PADRAO = 120;

{$IFDEF AMQP_OPENSSL}
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
{$ENDIF}

var
  LBroker: TAMQPServer;
  LPorta, LPortaTls: Word;
  LDataDir: string;
  LSegundos, I: Integer;
  LArg, LCert, LKey: string;
  LDeadline: UInt64;
  LLinha: string;
  {$IFDEF AMQP_OPENSSL}
  LBrokerTls: TAMQPServer;
  {$ENDIF}
begin
  SetMultiByteConversionCodePage(CP_UTF8);
  LPorta := PORTA_PADRAO;
  LPortaTls := 0;
  LSegundos := SEGUNDOS_PADRAO;
  LDataDir := '';

  I := 1;
  while I <= ParamCount do
  begin
    LArg := ParamStr(I);
    if SameText(LArg, '--tls') and (I < ParamCount) then
    begin
      LPortaTls := StrToIntDef(ParamStr(I + 1), 0);
      Inc(I);
    end
    else if SameText(LArg, '--seconds') and (I < ParamCount) then
    begin
      LSegundos := StrToIntDef(ParamStr(I + 1), SEGUNDOS_PADRAO);
      Inc(I);
    end
    else if SameText(LArg, '--datadir') and (I < ParamCount) then
    begin
      LDataDir := ParamStr(I + 1);
      Inc(I);
    end
    else
      LPorta := StrToIntDef(LArg, LPorta);
    Inc(I);
  end;

  LBroker := TAMQPServer.Create;
  {$IFDEF AMQP_OPENSSL}
  LBrokerTls := nil;
  {$ENDIF}
  try
    LBroker.BindAddress := '127.0.0.1';
    LBroker.Port := LPorta;
    // Vazio = broker da Fase 3, letra por letra (D19). Preenchido = o WAL
    // vive ali, e o host passa a sobreviver ao proprio restart.
    LBroker.DataDir := LDataDir;
    LBroker.Start;
    WriteLn('broker embutido ouvindo em 127.0.0.1:', LBroker.Port);

    if LPortaTls <> 0 then
    begin
      {$IFDEF AMQP_OPENSSL}
      if AchaCerts(LCert, LKey) then
      begin
        LBrokerTls := TAMQPServer.Create;
        LBrokerTls.BindAddress := '127.0.0.1';
        LBrokerTls.Port := LPortaTls;
        LBrokerTls.UseTls := True;
        LBrokerTls.TlsCertFile := LCert;
        LBrokerTls.TlsKeyFile := LKey;
        LBrokerTls.Start;
        WriteLn('broker TLS ouvindo em 127.0.0.1:', LBrokerTls.Port);
      end
      else
        WriteLn('AVISO: docker/certs nao encontrado -- sem porta TLS');
      {$ELSE}
      WriteLn('AVISO: build sem -dAMQP_OPENSSL -- o broker nao tem TLS');
      {$ENDIF}
    end;

    WriteLn('no ar por ate ', LSegundos, 's (ou ate uma linha na entrada padrao)');
    Flush(Output);

    // Sai por prazo OU por uma linha na entrada padrao: com prazo o script nao
    // trava se algo der errado; com a entrada padrao quem chamou encerra assim
    // que o SmokeTest termina.
    LDeadline := AmqpTickMs + UInt64(LSegundos) * 1000;
    while AmqpTickMs < LDeadline do
    begin
      if not Eof(Input) then
      begin
        ReadLn(LLinha);
        Break;
      end;
      Sleep(100);
    end;
  finally
    {$IFDEF AMQP_OPENSSL}
    LBrokerTls.Free;
    {$ENDIF}
    LBroker.Free;
  end;
  WriteLn('broker encerrado');
end.
