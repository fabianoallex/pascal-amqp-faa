program AMQPAcceptanceFpc;

{ Acceptance -- WS9 of Phase 2 (decision D8 in CLAUDE.md).

  Runs EXACTLY the same client integration suite, but against this library's
  EMBEDDED BROKER instead of RabbitMQ. No test is rewritten: the only
  difference is the AmqpUseEmbeddedBroker below, which redirects
  AMQP.IntegrationConfig to the in-process broker's ephemeral port.

  It is the test that measures whether the broker behaves like RabbitMQ in
  practice -- with a client that was NOT written for it.

    .\AMQPAcceptanceFpc.exe --all --format=plain

  TLS: the broker brings up a second, encrypted port with the test PKI of
  pascal-common-faa, in every build since 0.6.0 (SChannel serves the server
  side on Windows; the `openssl` build mode makes OpenSSL the default). If
  it doesn't come up, the TLS tests find no port and ignore themselves.

  TWO PASSES: TRANSIENT AND DURABLE (WS10 of Phase 4)
  ---------------------------------------------------
  With AMQP_ACCEPTANCE_DURABLE=1 the embedded broker starts with DataDir pointing
  to a temporary directory, and the SAME 48 run again. The question the second pass
  answers is different from the first, and it is the reason it exists: turning
  durability on MUST NOT CHANGE THE SEMANTICS the client sees.

  It is easy to write a durable broker that breaks something subtle -- the
  order of the confirms, the moment of Basic.Return, the requeue of an
  unacknowledged one -- and no test of the SERVER suite would notice, because
  all of them were written already knowing about durability. These 48 were
  not: they are the CLIENT tests, written against RabbitMQ, long before Phase 4
  existed.

  Two passes, two processes:

    .\AMQPAcceptanceFpc.exe --all --format=plain
    set AMQP_ACCEPTANCE_DURABLE=1 && .\AMQPAcceptanceFpc.exe --all --format=plain

  Separate processes on purpose: a freshly created durable broker in a clean
  directory is the state that matters, and reusing the process would carry
  the first pass's state into the second. }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  SysUtils, Classes, consoletestrunner, testregistry,
  PascalCommon.Transport,
  AMQP.Transport,
  AMQP.TestSupport,
  AMQP.Server.Types,
  AMQP.Server.Broker,
  AMQP.IntegrationConfig,
  AMQP.HandshakeIntegrationTests,
  AMQP.ChannelIntegrationTests,
  AMQP.ConsumeIntegrationTests,
  AMQP.ConsumerErrorIntegrationTests,
  AMQP.ChannelCloseIntegrationTests,
  AMQP.ReconnectIntegrationTests,
  AMQP.TlsIntegrationTests,
  AMQP.ReviewRegressionTests;

// True when the environment variable asks for the durable pass.
//
// VARIABLE, NOT FLAG, and the reason is practical: both the FPCUnit runner and
// the DUnitX one parse the whole command line and REJECT options they do not
// know ("Invalid option at position 1"). A --durable would die before reaching
// the tests. The variable bypasses both parsers and works exactly the same in
// both mirrors.
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
  Result := IncludeTrailingPathDelimiter(GetTempDir)
    + 'amqpaceita-' + IntToStr(GetProcessID);
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
          SysUtils.DeleteFile(IncludeTrailingPathDelimiter(ADir) + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      SysUtils.FindClose(LRec);
    end;
  end;
end;

// Starts the broker on 127.0.0.1 on an ephemeral port. Returns the chosen port.
function StartBroker(out ABroker: TAMQPServer;
  const ADataDir: string): Word;
begin
  ABroker := TAMQPServer.Create;
  ABroker.BindAddress := '127.0.0.1';
  ABroker.Port := 0; // the OS picks
  ABroker.DataDir := ADataDir; // '' = the Phase 3 broker, to the letter (D19)
  ABroker.Start;
  Result := ABroker.Port;
end;

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

function StartBrokerTls(out ABroker: TAMQPServer): Word;
begin
  Result := 0;
  ABroker := TAMQPServer.Create;
  ABroker.BindAddress := '127.0.0.1';
  ABroker.Port := 0;
  ABroker.UseTls := True;
  try
    ConfigureCertificate(ABroker);
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

var
  ConsoleApp: TTestRunner;
  LBroker: TAMQPServer;
  LPorta, LTlsPort: Word;
  LDataDir: string;
  LBrokerTls: TAMQPServer;
begin
  SetMultiByteConversionCodePage(CP_UTF8);

  LDataDir := '';
  if DurableRequested then
  begin
    LDataDir := DurableDir;
    ClearDir(LDataDir);
  end;
  LPorta := StartBroker(LBroker, LDataDir);
  LTlsPort := StartBrokerTls(LBrokerTls);
  try
    AmqpUseEmbeddedBroker(LPorta, LTlsPort);
    WriteLn('broker embutido em 127.0.0.1:', LPorta,
      ' (tls: ', LTlsPort, ')');
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
    LBrokerTls.Free;
    LBroker.Free;
    if LDataDir <> '' then
      ClearDir(LDataDir);
  end;
end.
