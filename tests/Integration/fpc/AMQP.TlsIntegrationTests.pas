unit AMQP.TlsIntegrationTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Integration/AMQP.TlsIntegrationTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }

{ TLS integration (amqps://) — needs a RabbitMQ with a TLS listener on
  localhost:5671, self-signed cert. Start it with:
    docker compose -f docker/docker-compose.yml -f docker/docker-compose.tls.yml up -d
  (see the header of docker/docker-compose.tls.yml to generate the certs).

  If the TLS broker is NOT up, Setup cannot connect and the tests mark
  themselves as IGNORED (IgnoreTest, from AMQP.TestSupport: in FPCUnit they
  show up in the report's "Number of ignored tests"; in DUnitX, which has no
  runtime ignore, they come out as Passed with the message
  'IGNORED: ...'), so as not to break the suite when only the plain broker
  (5672) is up. The same path covers builds without a TLS backend
  (outside Windows without -dAMQP_OPENSSL): Open raises EAMQPConnection and the
  tests ignore themselves. To really run them outside Windows, compile the
  runner with -dAMQP_OPENSSL (needs libssl/libcrypto installed). }


interface

uses
  fpcunit, testregistry,
  AMQP.TestSupport,
  SysUtils, Classes,
  PascalCommon.Threading,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Transport, // EAMQPTls (applies to SChannel and OpenSSL)
  AMQP.Queue.Methods;

type
  TAMQPTlsIntegrationTests = class(TTestCase)
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FAvailable: Boolean; // TLS broker reachable? (otherwise, tests are ignored)
    FVerifyPeerParams: TAMQPConnectionParams;
    FPlainPortParams: TAMQPConnectionParams;
    FCount: Integer;      // messages processed (atomic)
    FCurrent: Integer;    // callbacks running right now (atomic)
    FPeak: Integer;       // observed concurrency peak (atomic)
    function TlsUnavailable: Boolean;
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
    function BuildBigBody(ASize: Integer): string;
    procedure WaitCount(AExpected, ATimeoutMs: Integer);
    procedure HandleDeliveryConcurrent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure DoOpenWithVerifyPeer;
    procedure DoOpenAgainstPlainPort;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure Tls_PublishAndGet;
    procedure Tls_LargePayload_Intact;
    procedure Tls_ConcurrentConsume_WithAck;
    procedure Tls_AgainstPlainPort_RaisesEAMQPTls;
    procedure Tls_VerifyPeerTrue_RejectsSelfSignedCert;
  end;

implementation

procedure TAMQPTlsIntegrationTests.SetUp;
begin
  FAvailable := False;
  FCount := 0;
  FCurrent := 0;
  FPeak := 0;
  // TlsVerifyPeer=False: accepts the dev broker's self-signed cert.
  FConn := TAMQPConnection.Create(IntegrationTlsParams);
  try
    FConn.Open;
    FChan := FConn.CreateChannel;
    FAvailable := True;
  except
    // TLS broker unavailable (or the handshake failed): ignore the tests.
    FreeAndNil(FConn);
  end;
end;

procedure TAMQPTlsIntegrationTests.TearDown;
begin
  FChan.Free;
  FConn.Free;
end;

// True = no TLS broker (or a build without a backend): marks the test as
// ignored (see the header). The Exit in the caller covers the case where
// IgnoreTest does not interrupt the test.
function TAMQPTlsIntegrationTests.TlsUnavailable: Boolean;
begin
  Result := not FAvailable;
  if Result then
    IgnoreTest('TLS broker (5671) unavailable or build without a TLS backend');
end;

function TAMQPTlsIntegrationTests.DeclareTempQueue: string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  LDecl.AutoDelete := True;
  Result := FChan.DeclareQueue(LDecl).QueueName;
end;

function TAMQPTlsIntegrationTests.GetWithRetry(const AQueue: string): TAMQPGetResult;
var
  I: Integer;
begin
  for I := 1 to 20 do
  begin
    Result := FChan.BasicGet(AQueue, True);
    if Result.Found then
      Exit;
    TThread.Sleep(25);
  end;
end;

// Deterministic ASCII-only body (byte for byte equal to the string): corruption
// or reordering of any part breaks the equality comparison.
function TAMQPTlsIntegrationTests.BuildBigBody(ASize: Integer): string;
var
  I: Integer;
begin
  SetLength(Result, ASize);
  for I := 1 to ASize do
    Result[I] := Chr(Ord('a') + ((I * 31 + I div 97) mod 26));
end;

procedure TAMQPTlsIntegrationTests.WaitCount(AExpected, ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (PcAtomicGet(FCount) < AExpected) and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
end;

procedure TAMQPTlsIntegrationTests.HandleDeliveryConcurrent(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LCur, LOldPeak, LWaited: Integer;
begin
  LCur := PcAtomicInc(FCurrent);
  // updates the concurrency peak (CAS)
  repeat
    LOldPeak := FPeak;
    if LCur <= LOldPeak then
      Break;
  until PcAtomicCompareExchange(FPeak, LCur, LOldPeak) = LOldPeak;

  // Holds the callback until it OBSERVES another one running alongside (or a
  // timeout): proves the overlap without depending on a timing window (a fixed
  // sleep flakes when the pool workers are slow to start under load). If a
  // peak >= 2 has already been recorded, the proof is done and nobody else
  // needs to wait.
  LWaited := 0;
  while (PcAtomicGet(FCurrent) < 2) and (PcAtomicGet(FPeak) < 2) and
        (LWaited < 2000) do
  begin
    TThread.Sleep(10);
    Inc(LWaited, 10);
  end;

  AChannel.Ack(ADelivery.DeliveryTag);
  PcAtomicDec(FCurrent);
  PcAtomicInc(FCount);
end;

procedure TAMQPTlsIntegrationTests.Tls_PublishAndGet;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  if TlsUnavailable then
    Exit;

  LQueue := DeclareTempQueue;
  FChan.PublishText('', LQueue, 'héllo over TLS');

  LResult := GetWithRetry(LQueue);
  TAssert.AssertTrue('message should have been delivered over TLS', LResult.Found);
  TAssert.AssertEquals('héllo over TLS', LResult.BodyAsText);
end;

procedure TAMQPTlsIntegrationTests.Tls_LargePayload_Intact;
const
  // Much larger than a TLS record (16KB) and than the negotiated frame_max
  // (128KB): exercises the chunking of the TLS write, the reassembly on read
  // across several records and the splitting of the body into AMQP frames.
  BODY_SIZE = 300000;
var
  LQueue, LBody: string;
  LResult: TAMQPGetResult;
begin
  if TlsUnavailable then
    Exit;

  LQueue := DeclareTempQueue;
  LBody := BuildBigBody(BODY_SIZE);
  FChan.PublishText('', LQueue, LBody);

  LResult := GetWithRetry(LQueue);
  TAssert.AssertTrue('large message should have been delivered over TLS', LResult.Found);
  TAssert.AssertEquals('body size', BODY_SIZE, Length(LResult.BodyAsText));
  // Direct equality (no AssertEquals) so as not to print 300KB in the report on failure.
  TAssert.AssertTrue('body should come back intact, byte for byte', LResult.BodyAsText = LBody);
end;

procedure TAMQPTlsIntegrationTests.Tls_ConcurrentConsume_WithAck;
const
  N = 8;
var
  LQueue: string;
  I: Integer;
begin
  if TlsUnavailable then
    Exit;

  // Publishers + consumer on the SAME TLS connection: stresses the reader
  // decrypting while the writers encrypt (the TLS stream's FLock/FSendLock).
  LQueue := DeclareTempQueue;
  for I := 1 to N do
    FChan.PublishText('', LQueue, Format('tls-msg-%d', [I]));

  // high prefetch: lets the server deliver all of them, allowing concurrency.
  FChan.Qos(N);
  FChan.Consume(LQueue, HandleDeliveryConcurrent);

  // Timeout > N × the callback's timeout: truly serialized execution reaches
  // the end and fails on the peak assertion (the right message), not on the count.
  WaitCount(N, 30000);

  TAssert.AssertEquals('every message should have been processed over TLS',
    N, PcAtomicGet(FCount));
  TAssert.AssertTrue('processing should be concurrent (peak > 1), not serialized',
    PcAtomicGet(FPeak) > 1);
end;

procedure TAMQPTlsIntegrationTests.DoOpenAgainstPlainPort;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(FPlainPortParams);
  try
    LConn.Open; // must raise EAMQPTls (handshake against a port without TLS)
  finally
    LConn.Free;
  end;
end;

procedure TAMQPTlsIntegrationTests.Tls_AgainstPlainPort_RaisesEAMQPTls;
begin
  if TlsUnavailable then
    Exit; // also ensures there IS a broker up (otherwise the failure would be a socket one, not TLS)

  // TLS handshake against the plain port (5672): the broker replies with the
  // AMQP header and closes — it must become EAMQPTls quickly, without hanging the suite.
  FPlainPortParams := IntegrationTlsParams;
  FPlainPortParams.Port := 5672;

  CheckRaises('TLS handshake against the plain port should fail with EAMQPTls',
    EAMQPTls, DoOpenAgainstPlainPort);
end;

procedure TAMQPTlsIntegrationTests.DoOpenWithVerifyPeer;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(FVerifyPeerParams);
  try
    LConn.Open; // must raise EAMQPTls (untrusted cert)
  finally
    LConn.Free;
  end;
end;

procedure TAMQPTlsIntegrationTests.Tls_VerifyPeerTrue_RejectsSelfSignedCert;
begin
  if TlsUnavailable then
    Exit; // without a TLS broker the validation cannot be proven

  // Same broker (self-signed cert), but NOW requiring chain + hostname
  // validation: the TLS handshake must FAIL (proves TlsVerifyPeer=True validates).
  FVerifyPeerParams := IntegrationTlsParams;
  FVerifyPeerParams.TlsVerifyPeer := True;

  CheckRaises('verifying the self-signed cert should refuse the connection',
    EAMQPTls, DoOpenWithVerifyPeer);
end;

initialization
  RegisterTest(TAMQPTlsIntegrationTests);

end.
