program BrokerHostFpc;

{ Hosts the embedded broker for the SmokeTest to run against -- WS9 of Phase 2.

  Why a separate process instead of an "--embedded" inside the SmokeTest: the
  SmokeTest is a client SAMPLE, and its value is precisely in being 100%
  client -- making the sample link the server sub-module would change what it
  demonstrates and what it needs to compile. With two processes, each one
  stays pure: the broker here, the client there.

    .\BrokerHostFpc.exe [port] [--tls tls-port] [--seconds N]
                        [--datadir <dir>]
    .\SmokeTest.exe --host 127.0.0.1 --port <port>

  --datadir turns durability on (Phase 4). It is what enables the SmokeTest's
  RESTART step: the host starts with a directory, the SmokeTest publishes
  persistent messages, the host is killed and starts again on the SAME
  directory, and the SmokeTest checks that the messages are there. See
  tests\tools\smoke_restart.py.

  Without arguments: port 5673 (away from the dev RabbitMQ's 5672, so both can
  coexist), no TLS, and it stays up for 120 s or until it receives a line on
  standard input. }

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
  DEFAULT_PORT = 5673;
  DEFAULT_SECONDS = 120;

// The test PKI of pascal-common-faa (external/pascal-common-faa/tests/pki),
// looked for upwards from the executable. '' when not found.
function FindPki: string;
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
  LPorta, LTlsPort: Word;
  LDataDir: string;
  LSeconds, I: Integer;
  LArg, LPki: string;
  LDeadline: UInt64;
  LLine: string;
  LBrokerTls: TAMQPServer;
begin
  SetMultiByteConversionCodePage(CP_UTF8);
  LPorta := DEFAULT_PORT;
  LTlsPort := 0;
  LSeconds := DEFAULT_SECONDS;
  LDataDir := '';

  I := 1;
  while I <= ParamCount do
  begin
    LArg := ParamStr(I);
    if SameText(LArg, '--tls') and (I < ParamCount) then
    begin
      LTlsPort := StrToIntDef(ParamStr(I + 1), 0);
      Inc(I);
    end
    else if SameText(LArg, '--seconds') and (I < ParamCount) then
    begin
      LSeconds := StrToIntDef(ParamStr(I + 1), DEFAULT_SECONDS);
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
    // Empty = the Phase 3 broker, to the letter (D19). Filled in = the WAL
    // lives there, and the host survives its own restart.
    LBroker.DataDir := LDataDir;
    LBroker.Start;
    WriteLn('broker embutido ouvindo em 127.0.0.1:', LBroker.Port);

    if LTlsPort <> 0 then
    begin
      // Every build has TLS since 0.6.0: PEM pair on OpenSSL, PFX on
      // SChannel (the default on Windows outside the `openssl` mode).
      LPki := FindPki;
      if LPki <> '' then
      begin
        LBrokerTls := TAMQPServer.Create;
        LBrokerTls.BindAddress := '127.0.0.1';
        LBrokerTls.Port := LTlsPort;
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

    WriteLn('no ar por ate ', LSeconds, 's (ou ate uma linha na entrada padrao)');
    Flush(Output);

    // Exits on a deadline OR on a line on standard input: with the deadline
    // the script does not hang if something goes wrong; with standard input
    // the caller shuts it down as soon as the SmokeTest finishes.
    LDeadline := PcTickMs + UInt64(LSeconds) * 1000;
    while PcTickMs < LDeadline do
    begin
      if not Eof(Input) then
      begin
        ReadLn(LLine);
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
