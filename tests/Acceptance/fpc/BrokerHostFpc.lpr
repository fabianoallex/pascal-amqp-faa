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
  PascalCommon.Threading,
  PascalCommon.Transport,
  AMQP.Transport,
  AMQP.Server.Types,
  AMQP.Server.Broker;

const
  PORTA_PADRAO = 5673;
  SEGUNDOS_PADRAO = 120;

// The test PKI of pascal-common-faa (external/pascal-common-faa/tests/pki),
// looked for upwards from the executable. '' when not found.
function AchaPki: string;
var
  LDir: string;
  I: Integer;
begin
  LDir := ExtractFileDir(ExpandFileName(ParamStr(0)));
  for I := 0 to 8 do
  begin
    Result := IncludeTrailingPathDelimiter(LDir) + 'external' + PathDelim +
      'pascal-common-faa' + PathDelim + 'tests' + PathDelim + 'pki' + PathDelim;
    if FileExists(Result + 'ca_cert.pem') then
      Exit;
    LDir := ExtractFileDir(LDir);
  end;
  Result := '';
end;

var
  LBroker: TAMQPServer;
  LPorta, LPortaTls: Word;
  LDataDir: string;
  LSegundos, I: Integer;
  LArg, LPki: string;
  LDeadline: UInt64;
  LLinha: string;
  LBrokerTls: TAMQPServer;
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
  LBrokerTls := nil;
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
      // Every build has TLS since 0.6.0: PEM pair on OpenSSL, PFX on
      // SChannel (the default on Windows outside the `openssl` mode).
      LPki := AchaPki;
      if LPki <> '' then
      begin
        LBrokerTls := TAMQPServer.Create;
        LBrokerTls.BindAddress := '127.0.0.1';
        LBrokerTls.Port := LPortaTls;
        LBrokerTls.UseTls := True;
        if AmqpResolveTlsBackend(tbDefault) = tbOpenSsl then
        begin
          LBrokerTls.TlsCertFile := LPki + 'srv_cert.pem';
          LBrokerTls.TlsKeyFile := LPki + 'srv_key.pem';
        end
        else
        begin
          LBrokerTls.TlsCertFile := LPki + 'srv.pfx';
          LBrokerTls.TlsCertPassword := 'pipestest';
        end;
        LBrokerTls.Start;
        WriteLn('broker TLS (', AmqpTlsBackendName, ') ouvindo em 127.0.0.1:',
          LBrokerTls.Port);
      end
      else
        WriteLn('AVISO: external/pascal-common-faa/tests/pki nao encontrado -- sem porta TLS');
    end;

    WriteLn('no ar por ate ', LSegundos, 's (ou ate uma linha na entrada padrao)');
    Flush(Output);

    // Sai por prazo OU por uma linha na entrada padrao: com prazo o script nao
    // trava se algo der errado; com a entrada padrao quem chamou encerra assim
    // que o SmokeTest termina.
    LDeadline := PcTickMs + UInt64(LSegundos) * 1000;
    while PcTickMs < LDeadline do
    begin
      if not Eof(Input) then
      begin
        ReadLn(LLinha);
        Break;
      end;
      Sleep(100);
    end;
  finally
    LBrokerTls.Free;
    LBroker.Free;
  end;
  WriteLn('broker encerrado');
end.
