unit AMQP.ServerHandshakeTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Server/AMQP.ServerHandshakeTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }

{ Tests of the broker's handshake FSM and channel lifecycle (WS4).

  Two fronts:
  - TClientHandshakeTests: the lib's REAL CLIENT (TAMQPConnection) connects to the
    in-process broker. It is the test that matters — if the handshake is wrong, the
    client does not open.
  - TRawHandshakeTests: a raw socket building frames by hand, to exercise the
    error paths a correct client never produces (out-of-order method,
    Tune-Ok above what was proposed, a second Channel.Open, content without publish...). }


interface

uses
  fpcunit, testregistry,
  SysUtils, Classes, Rtti, SyncObjs,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  PascalCommon.Threading,
  PascalCommon.Transport,
  PascalCommon.Transport.Tcp,
  PascalCommon.Transport.Tls,
  PascalCommon.Transport.OpenSSL,
  AMQP.Transport,
  AMQP.TestSupport,
  AMQP.Connection.Methods,
  AMQP.Channel.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods,
  AMQP.Connection,
  AMQP.Server.Auth,
  AMQP.Server.Types,
  AMQP.Server.Connection,
  AMQP.Server.Broker;

type
  { Brings up a broker on 127.0.0.1 on an ephemeral port for each test. }

  TClientHandshakeTests = class(TTestCase)
  private
    FBroker: TAMQPServer;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure RealClientConnects;
    procedure WrongPassword_403;
    procedure MissingVhost_530;
    procedure ExtraVhostAccepted;
    procedure ChannelOpensAndCloses;
    procedure TwoSimultaneousConnections;
    procedure ClientClosesCleanly;
  end;

  TRawHandshakeTests = class(TTestCase)
  private
    FBroker: TAMQPServer;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure StartAdvertisesPlainAndServerProperties;
    procedure MethodOutOfOrder_503;
    procedure ConnectionOnNonZeroChannel_503;
    procedure TuneOkAboveProposed_530;
    procedure FrameMaxBelowMinimum_530;
    procedure SecondChannelOpen_504;
    procedure ChannelNotOpen_504;
    procedure UnknownFrameType_501;
    procedure ContentWithoutPublish_505;
    procedure ClientClosesConnection_ReceivesCloseOk;
    procedure UnimplementedClass_ClosesOnlyTheChannel;
    procedure NoLocalNoConsume_540;
    procedure PrefetchSizeNonZero_540;
    procedure MandatoryNoRoute_ReturnBeforeAck_312;
  end;

  { Content (WS5): reassembly of Basic.Publish + content-header + body frames,
    the interleave rule of spec 2.3.5 and the "null" dispatch of the resource
    classes -- every client->server method is decoded and answered with the
    right *-Ok, with no engine behind it. }
  TContentTests = class(TTestCase)
  private
    FBroker: TAMQPServer;
    /// Consumer never called (the null broker does not deliver); the lib requires an
    /// object method, it does not accept a closure.
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure ClientPublishesSimpleMessage;
    procedure LargeBodyInSeveralFrames;
    procedure PropertiesAndHeadersArriveIntact;
    procedure MessageWithoutBody_Accepted;
    procedure TwoInterleavedChannels;
    procedure MethodInTheMiddleOfContent_505;
    procedure BodyWithoutHeader_505;
    procedure BodyLargerThanBodySize_505;
    procedure ContentHeaderOfAnotherClass_505;
    procedure ClientDeclaresTopology;
    procedure RawHeaderPayload_DecodesTheSame;
  end;

  { Server-side TLS (WS3; pascal-common-faa's transport since 0.6.0). Every
    build has it: SChannel (PFX) on Windows, OpenSSL (PEM) everywhere. Uses
    the test PKI of pascal-common-faa (AMQP.TestSupport.Pki): the client
    VALIDATES the broker against the private CA, as an application should.

    This fixture runs with tbDefault on both sides; on Windows the classes
    below run the same tests with OpenSSL on both sides and crossed. They
    have no [TestFixture] on purpose: they inherit the tests, and the
    explicit registration is what DUnitX needs (tools/gen_fpc_mirror.py
    doesn't take a fixture that inherits from another fixture). }
  TServerTlsTests = class(TTestCase)
  private
    FBroker: TAMQPServer;
    FReady: Boolean;
    procedure OpenWithWrongCa;
    procedure OpenWithWrongName;
    procedure OpenWithoutClientCert;
    procedure OpenWithRogueClientCert;
    procedure StartWithMissingCert;
  protected
    function ClientBackend: TPcTlsBackend; virtual;
    function ServerBackend: TPcTlsBackend; virtual;
    /// False (and a notice) when a backend asked for is OpenSSL and it isn't
    /// loadable on Windows, where it is optional.
    function Ready: Boolean;
    /// A TLS broker with the PKI's server certificate; ACaFile <> '' turns
    /// mTLS on (client certificates from that CA).
    function NewBroker(const ACaFile: string): TAMQPServer;
    /// Client parameters validating the broker against the test CA, with
    /// an optional client certificate ('cli', 'rogue'...).
    function TlsParams(const AClientCert: string): TAMQPConnectionParams;
    procedure RestartWithMtls;
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure RealClientConnectsOverTls;
    procedure PublishesOverTls;
    procedure PlainClientOnTlsPort_Fails;
    procedure MissingCert_StartRaisesEAMQPTls;
    procedure WrongPrivateCa_ClientRejectsWithEAMQPTls;
    procedure WrongName_ClientRejectsWithEAMQPTls;
    procedure Mtls_ClientWithCaCertificate_Connects;
    procedure Mtls_ClientWithoutCertificate_Rejected;
    procedure Mtls_ClientFromAnotherCa_Rejected;
  end;

  {$IFDEF MSWINDOWS}
  TServerTlsOpenSslTests = class(TServerTlsTests)
  protected
    function ClientBackend: TPcTlsBackend; override;
    function ServerBackend: TPcTlsBackend; override;
  end;

  TServerTlsOpenSslClientSChannelServerTests = class(TServerTlsTests)
  protected
    function ClientBackend: TPcTlsBackend; override;
    function ServerBackend: TPcTlsBackend; override;
  end;

  TServerTlsSChannelClientOpenSslServerTests = class(TServerTlsTests)
  protected
    function ClientBackend: TPcTlsBackend; override;
    function ServerBackend: TPcTlsBackend; override;
  end;
  {$ENDIF}

  { Heartbeat (spec 4.2.7) and the Connection.Close-Ok deadline. The tests negotiate
    1 s intervals in the Tune-Ok to fit a reasonable suite time. }
  THeartbeatTests = class(TTestCase)
  private
    FBroker: TAMQPServer;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure BrokerSendsHeartbeat;
    procedure SilentPeer_IsDropped;
    procedure HeartbeatZero_DoesNotDrop;
    procedure NoCloseOk_DroppedAtTheDeadline;
    procedure RealClientSurvivesWithHeartbeat;
  end;

implementation

// --- frame helpers --------------------------------------------------------

const
  MAX_PAYLOAD = 131072;

// Reads the next method frame (skipping heartbeats). The caller frees the reader.
function ReadMethod(AStream: TStream; out ACh: Word;
  out AId: TAMQPMethodId): TAMQPReader;
var
  LF: TAMQPFrame;
begin
  repeat
    LF := TAMQPFrame.ReadFrom(AStream, MAX_PAYLOAD);
  until not LF.IsHeartbeat;
  if not LF.IsMethod then
    raise Exception.CreateFmt('expected a method frame, got type %d',
      [LF.FrameType]);
  ACh := LF.Channel;
  Result := TAMQPReader.Create(LF.Payload);
  AId := ReadMethodHeader(Result);
end;

procedure SendMethod(AStream: TStream; ACh: Word; const APayload: TBytes);
var
  LF: TAMQPFrame;
begin
  LF := TAMQPFrame.Create(AMQP_FRAME_METHOD, ACh, APayload);
  LF.WriteTo(AStream);
end;

// --- client-side handshake steps (raw) ------------------------------------

// Sends the protocol-header and consumes the Connection.Start. Frees the
// server-properties; returns only the scalar fields.
function RawStart(AStream: TStream): TAMQPConnectionStart;
var
  LR: TAMQPReader;
  LCh: Word;
  LId: TAMQPMethodId;
begin
  WriteProtocolHeader(AStream);
  LR := ReadMethod(AStream, LCh, LId);
  try
    if not LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START) then
      raise Exception.CreateFmt('expected Connection.Start, got %d/%d',
        [LId.ClassId, LId.MethodId]);
    Result := DecodeStart(LR);
  finally
    LR.Free;
  end;
end;

procedure RawStartOk(AStream: TStream; const AUser, APass: string;
  const AMechanism: string = AMQP_AUTH_PLAIN);
var
  LProps: TAMQPFieldTable;
begin
  LProps := DefaultClientProperties; // announces authentication_failure_close
  try
    SendMethod(AStream, AMQP_CHANNEL_CONNECTION,
      BuildStartOk(LProps, AMechanism, PlainAuthResponse(AUser, APass),
        AMQP_LOCALE_DEFAULT));
  finally
    LProps.Free;
  end;
end;

function RawTune(AStream: TStream): TAMQPConnectionTune;
var
  LR: TAMQPReader;
  LCh: Word;
  LId: TAMQPMethodId;
begin
  LR := ReadMethod(AStream, LCh, LId);
  try
    if not LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE) then
      raise Exception.CreateFmt('expected Connection.Tune, got %d/%d',
        [LId.ClassId, LId.MethodId]);
    Result := DecodeTune(LR);
  finally
    LR.Free;
  end;
end;

// Full handshake, from the header to Open-Ok. Raises if anything gets out of order.
procedure RawHandshake(AStream: TStream; const AVHost: string = '/';
  const AUser: string = 'guest'; const APass: string = 'guest');
var
  LStart: TAMQPConnectionStart;
  LTune: TAMQPConnectionTune;
  LR: TAMQPReader;
  LCh: Word;
  LId: TAMQPMethodId;
begin
  LStart := RawStart(AStream);
  LStart.ServerProperties.Free;
  RawStartOk(AStream, AUser, APass);
  LTune := RawTune(AStream);
  SendMethod(AStream, AMQP_CHANNEL_CONNECTION, BuildTuneOk(LTune));
  SendMethod(AStream, AMQP_CHANNEL_CONNECTION, BuildOpen(AVHost));
  LR := ReadMethod(AStream, LCh, LId);
  try
    if not LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK) then
      raise Exception.CreateFmt('expected Connection.Open-Ok, got %d/%d',
        [LId.ClassId, LId.MethodId]);
  finally
    LR.Free;
  end;
end;

// Full handshake forcing a heartbeat in the Tune-Ok (the broker adopts the value
// the CLIENT sends back, as the spec says).
procedure RawHandshakeHb(AStream: TStream; AHeartbeat: Word);
var
  LStart: TAMQPConnectionStart;
  LTune: TAMQPConnectionTune;
  LR: TAMQPReader;
  LCh: Word;
  LId: TAMQPMethodId;
begin
  LStart := RawStart(AStream);
  LStart.ServerProperties.Free;
  RawStartOk(AStream, 'guest', 'guest');
  LTune := RawTune(AStream);
  LTune.Heartbeat := AHeartbeat;
  SendMethod(AStream, AMQP_CHANNEL_CONNECTION, BuildTuneOk(LTune));
  SendMethod(AStream, AMQP_CHANNEL_CONNECTION, BuildOpen('/'));
  LR := ReadMethod(AStream, LCh, LId);
  try
    if not LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK) then
      raise Exception.CreateFmt('expected Connection.Open-Ok, got %d/%d',
        [LId.ClassId, LId.MethodId]);
  finally
    LR.Free;
  end;
end;

// Reads whatever comes, expecting a Connection.Close; returns the reply-code.
function ExpectConnectionClose(AStream: TStream): TAMQPConnectionClose;
var
  LR: TAMQPReader;
  LCh: Word;
  LId: TAMQPMethodId;
begin
  LR := ReadMethod(AStream, LCh, LId);
  try
    if not LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
      raise Exception.CreateFmt('expected Connection.Close, got %d/%d on channel %d',
        [LId.ClassId, LId.MethodId, LCh]);
    Result := DecodeClose(LR);
  finally
    LR.Free;
  end;
end;

function ExpectChannelClose(AStream: TStream; out ACh: Word): TAMQPCloseInfo;
var
  LR: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LR := ReadMethod(AStream, ACh, LId);
  try
    if not LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE) then
      raise Exception.CreateFmt('expected Channel.Close, got %d/%d',
        [LId.ClassId, LId.MethodId]);
    Result := DecodeChannelClose(LR);
  finally
    LR.Free;
  end;
end;

procedure ExpectMethodId(AStream: TStream; AClassId, AMethodId: Word;
  out ACh: Word);
var
  LR: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LR := ReadMethod(AStream, ACh, LId);
  try
    if not LId.Matches(AClassId, AMethodId) then
      raise Exception.CreateFmt('expected %d/%d, got %d/%d',
        [AClassId, AMethodId, LId.ClassId, LId.MethodId]);
  finally
    LR.Free;
  end;
end;

// Opens a raw socket against the broker; returns the stream (owner of the socket via
// AOutSock, which the caller frees AFTER the stream).
function RawConnect(APort: Word; out ASock: TPcEndpoint): TAMQPEndpointStream;
begin
  ASock := PcTcpConnect('127.0.0.1', APort, AmqpTcpOptions);
  Result := TAMQPEndpointStream.Create(ASock);
end;

{ Sink that records what went by, for the tests to check the reassembly. The
  calls come from the connection's thread, so everything is protected. The message
  and the headers table belong to the channel and go away as soon as RouteMessage
  returns: that is why we copy what matters RIGHT HERE. }
type
  TRecordingSink = class(TInterfacedObject, IAMQPMessageSink)
  private
    FLock: TCriticalSection;
    FCount: Integer;
    FExchange: string;
    FRoutingKey: string;
    FBody: TBytes;
    FContentType: string;
    FCorrelationId: string;
    FHeaderFoo: string;
    FHeaderPayload: TBytes;
  public
    constructor Create;
    destructor Destroy; override;
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage; out ARejected: Boolean;
      out ALsn: UInt64): Boolean;
    function Count: Integer;
    function Exchange: string;
    function RoutingKey: string;
    function Body: TBytes;
    function ContentType: string;
    function CorrelationId: string;
    function HeaderFoo: string;
    /// Raw payload of the content-header (decision D1 of Phase 2, see CLAUDE.md and
    /// AMQP.Server.Message) -- what the message hands to the engine.
    function HeaderPayload: TBytes;
    /// Waits up to ATimeoutMs for ACount routed messages.
    function WaitCount(ACount, ATimeoutMs: Integer): Boolean;
  end;

constructor TRecordingSink.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
end;

destructor TRecordingSink.Destroy;
begin
  FLock.Free;
  inherited;
end;

function TRecordingSink.RouteMessage(const AVHost: string;
  const AMessage: TAMQPServerMessage; out ARejected: Boolean;
  out ALsn: UInt64): Boolean;
var
  LValue: TValue;
begin
  ARejected := False; // this test double accepts everything
  ALsn := 0;           // and writes no journal at all
  FLock.Enter;
  try
    Inc(FCount);
    FExchange := AMessage.Exchange;
    FRoutingKey := AMessage.RoutingKey;
    FBody := Copy(AMessage.Body, 0, Length(AMessage.Body));
    FContentType := AMessage.Properties.ContentType;
    FCorrelationId := AMessage.Properties.CorrelationId;
    FHeaderPayload := Copy(AMessage.HeaderPayload, 0, Length(AMessage.HeaderPayload));
    FHeaderFoo := '';
    if AMessage.Properties.Has(bpHeaders) and
       Assigned(AMessage.Properties.Headers) and
       AMessage.Properties.Headers.TryGetValue('foo', LValue) then
      FHeaderFoo := LValue.AsString;
  finally
    FLock.Leave;
  end;
  Result := False; // null broker: nothing was routed
end;

function TRecordingSink.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FCount;
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.Exchange: string;
begin
  FLock.Enter;
  try
    Result := FExchange;
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.RoutingKey: string;
begin
  FLock.Enter;
  try
    Result := FRoutingKey;
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.Body: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FBody, 0, Length(FBody));
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.ContentType: string;
begin
  FLock.Enter;
  try
    Result := FContentType;
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.CorrelationId: string;
begin
  FLock.Enter;
  try
    Result := FCorrelationId;
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.HeaderFoo: string;
begin
  FLock.Enter;
  try
    Result := FHeaderFoo;
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.HeaderPayload: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FHeaderPayload, 0, Length(FHeaderPayload));
  finally
    FLock.Leave;
  end;
end;

function TRecordingSink.WaitCount(ACount, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  repeat
    if Count >= ACount then
      Exit(True);
    Sleep(10);
  until PcTickMs > LDeadline;
  Result := Count >= ACount;
end;

// Builds a Basic (class 60) content-header with the given body-size and no
// properties. AClassId allows forging a wrong class in the error tests.
function BuildRawContentHeader(ABodySize: UInt64; AClassId: Word = 60): TBytes;
var
  W: TAMQPWriter;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteShortUInt(AClassId);
    W.WriteShortUInt(0);            // weight (reserved, always 0)
    W.WriteLongLongUInt(ABodySize);
    W.WriteShortUInt(0);            // property flags: no properties
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure SendFrameRaw(AStream: TStream; AType: Byte; ACh: Word;
  const APayload: TBytes);
var
  LF: TAMQPFrame;
begin
  LF := TAMQPFrame.Create(AType, ACh, APayload);
  LF.WriteTo(AStream);
end;

function BytesOf(const AText: string): TBytes;
begin
  Result := AmqpUtf8Encode(AText);
end;

// Opens a channel and checks the Open-Ok.
procedure OpenChannel(AStream: TStream; ACh: Word);
var
  LGot: Word;
begin
  SendMethod(AStream, ACh, BuildChannelOpen);
  ExpectMethodId(AStream, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LGot);
  if LGot <> ACh then
    raise Exception.CreateFmt('Open-Ok came on channel %d, expected %d',
      [LGot, ACh]);
end;

{ What the fixtures with a broker share (the broker comes up on 127.0.0.1 on an
  ephemeral port for each test).
  The mirror generator (tools/gen_fpc_mirror.py) does not accept inheritance
  between fixtures: each one keeps its own fields and calls the routines below. }

procedure ServerSetUp(var FBroker: TAMQPServer);
begin
  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '127.0.0.1';
  FBroker.Port := 0;
  FBroker.Start;
end;

procedure ServerTearDown(var FBroker: TAMQPServer);
begin
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
end;

function Params(FBroker: TAMQPServer): TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := '127.0.0.1';
  Result.Port := FBroker.Port;
end;

function ServerConn(FBroker: TAMQPServer; ATimeoutMs: Integer = 3000): TAMQPServerConnection;
var
  LDeadline: UInt64;
  LAll: TArray<TAMQPServerConnection>;
begin
  Result := nil;
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  repeat
    LAll := FBroker.Connections;
    if Length(LAll) > 0 then
      Exit(LAll[0]);
    Sleep(10);
  until PcTickMs > LDeadline;
end;

function WaitState(AConn: TAMQPServerConnection; AState: TAMQPServerConnState; ATimeoutMs: Integer = 3000): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  repeat
    if AConn.State = AState then
      Exit(True);
    Sleep(10);
  until PcTickMs > LDeadline;
  Result := AConn.State = AState;
end;

function WaitChannels(AConn: TAMQPServerConnection; ACount: Integer; ATimeoutMs: Integer = 3000): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  repeat
    if AConn.ChannelCount = ACount then
      Exit(True);
    Sleep(10);
  until PcTickMs > LDeadline;
  Result := AConn.ChannelCount = ACount;
end;

{ SetUp/TearDown of the fixtures with a broker: each one keeps its own FBroker and
  calls ServerSetUp/ServerTearDown. The mirror generator does not accept
  inheritance between fixtures, which is why there is no base class. }

procedure TClientHandshakeTests.SetUp;
begin
  ServerSetUp(FBroker);
end;

procedure TClientHandshakeTests.TearDown;
begin
  ServerTearDown(FBroker);
end;

procedure TRawHandshakeTests.SetUp;
begin
  ServerSetUp(FBroker);
end;

procedure TRawHandshakeTests.TearDown;
begin
  ServerTearDown(FBroker);
end;

procedure TContentTests.SetUp;
begin
  ServerSetUp(FBroker);
end;

procedure TContentTests.TearDown;
begin
  ServerTearDown(FBroker);
end;

procedure THeartbeatTests.SetUp;
begin
  ServerSetUp(FBroker);
end;

procedure THeartbeatTests.TearDown;
begin
  ServerTearDown(FBroker);
end;

{ TClientHandshakeTests }

procedure TClientHandshakeTests.RealClientConnects;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
begin
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    TAssert.AssertTrue('client open', LCli.IsOpen);

    LConn := ServerConn(FBroker);
    TAssert.AssertTrue('connection registered in the broker', Assigned(LConn));
    TAssert.AssertTrue('FSM reached amqssOpen', WaitState(LConn, amqssOpen));
    TAssert.AssertEquals('authenticated user', 'guest', LConn.UserId);
    TAssert.AssertEquals('vhost', '/', LConn.VirtualHost);
    TAssert.AssertEquals('negotiated channel-max',
      AMQP_SERVER_DEFAULT_CHANNEL_MAX, Integer(LConn.Negotiated.ChannelMax));
    TAssert.AssertEquals('negotiated frame-max',
      Integer(AMQP_SERVER_DEFAULT_FRAME_MAX), Integer(LConn.Negotiated.FrameMax));
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.WrongPassword_403;
var
  LCli: TAMQPConnection;
  LP: TAMQPConnectionParams;
  LMsg: string;
begin
  LP := Params(FBroker);
  LP.Password := 'wrong-password';
  LCli := TAMQPConnection.Create(LP);
  try
    LMsg := '';
    try
      LCli.Open;
    except
      on E: Exception do
        LMsg := E.Message;
    end;
    TAssert.AssertFalse('must not open', LCli.IsOpen);
    TAssert.AssertTrue('message cites 403 (got: ' + LMsg + ')', Pos('403', LMsg) > 0);
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.MissingVhost_530;
var
  LCli: TAMQPConnection;
  LP: TAMQPConnectionParams;
  LMsg: string;
begin
  LP := Params(FBroker);
  LP.VirtualHost := '/does-not-exist';
  LCli := TAMQPConnection.Create(LP);
  try
    LMsg := '';
    try
      LCli.Open;
    except
      on E: Exception do
        LMsg := E.Message;
    end;
    TAssert.AssertFalse('must not open', LCli.IsOpen);
    TAssert.AssertTrue('message cites 530 (got: ' + LMsg + ')', Pos('530', LMsg) > 0);
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.ExtraVhostAccepted;
var
  LCli: TAMQPConnection;
  LP: TAMQPConnectionParams;
begin
  FBroker.VirtualHosts.Add('/app');
  LP := Params(FBroker);
  LP.VirtualHost := '/app';
  LCli := TAMQPConnection.Create(LP);
  try
    LCli.Open;
    TAssert.AssertTrue('client open on /app', LCli.IsOpen);
    TAssert.AssertEquals('/app', ServerConn(FBroker).VirtualHost);
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.ChannelOpensAndCloses;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
  LCh1, LCh2: TAMQPChannel;
begin
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    LConn := ServerConn(FBroker);
    TAssert.AssertTrue(Assigned(LConn));

    LCh1 := LCli.CreateChannel;
    try
      TAssert.AssertTrue('1 channel on the server', WaitChannels(LConn, 1));
      LCh2 := LCli.CreateChannel;
      try
        TAssert.AssertTrue('2 channels on the server', WaitChannels(LConn, 2));
        LCh2.Close;
      finally
        LCh2.Free;
      end;
      TAssert.AssertTrue('back to 1 channel after Channel.Close', WaitChannels(LConn, 1));
      LCh1.Close;
    finally
      LCh1.Free;
    end;
    TAssert.AssertTrue('no channel open', WaitChannels(LConn, 0));
    TAssert.AssertTrue('connection stays open', LCli.IsOpen);
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.TwoSimultaneousConnections;
var
  LA, LB: TAMQPConnection;
begin
  LA := TAMQPConnection.Create(Params(FBroker));
  try
    LB := TAMQPConnection.Create(Params(FBroker));
    try
      LA.Open;
      LB.Open;
      TAssert.AssertTrue(LA.IsOpen);
      TAssert.AssertTrue(LB.IsOpen);
      TAssert.AssertEquals('two live connections', 2, FBroker.ConnectionCount);
      TAssert.AssertEquals(2, FBroker.TotalAccepted);
    finally
      LB.Free;
    end;
  finally
    LA.Free;
  end;
end;

procedure TClientHandshakeTests.ClientClosesCleanly;
var
  LCli: TAMQPConnection;
  LDeadline: UInt64;
begin
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    TAssert.AssertTrue(Assigned(ServerConn(FBroker)));
    LCli.Close; // Connection.Close + waits for the broker's Close-Ok
    TAssert.AssertFalse('client closed', LCli.IsOpen);

    LDeadline := PcTickMs + 3000;
    while (FBroker.ConnectionCount > 0) and (PcTickMs < LDeadline) do
      Sleep(10);
    TAssert.AssertEquals('broker reaped the connection', 0, FBroker.ConnectionCount);
  finally
    LCli.Free;
  end;
end;

{ TRawHandshakeTests }

procedure TRawHandshakeTests.StartAdvertisesPlainAndServerProperties;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LStart: TAMQPConnectionStart;
  LProd: TValue;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    LStart := RawStart(LStrm);
    try
      TAssert.AssertEquals('version-major', 0, LStart.VersionMajor);
      TAssert.AssertEquals('version-minor', 9, LStart.VersionMinor);
      TAssert.AssertTrue('offers PLAIN', LStart.SupportsMechanism('PLAIN'));
      TAssert.AssertTrue('server-properties has product',
        LStart.ServerProperties.TryGetValue('product', LProd));
      TAssert.AssertEquals(AMQP_SERVER_PRODUCT, LProd.AsString);
    finally
      LStart.ServerProperties.Free;
    end;
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.MethodOutOfOrder_503;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LStart: TAMQPConnectionStart;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    LStart := RawStart(LStrm);
    LStart.ServerProperties.Free;
    // Channel.Open before authenticating.
    SendMethod(LStrm, 1, BuildChannelOpen);
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('COMMAND_INVALID', AMQP_COMMAND_INVALID, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.ConnectionOnNonZeroChannel_503;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
  LCloseArgs: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    LCloseArgs.ReplyCode := AMQP_REPLY_SUCCESS;
    LCloseArgs.ReplyText := 'bye';
    LCloseArgs.ClassId := 0;
    LCloseArgs.MethodId := 0;
    SendMethod(LStrm, 3, BuildClose(LCloseArgs)); // Connection.* outside channel 0
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('COMMAND_INVALID', AMQP_COMMAND_INVALID, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.TuneOkAboveProposed_530;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LStart: TAMQPConnectionStart;
  LTune: TAMQPConnectionTune;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    LStart := RawStart(LStrm);
    LStart.ServerProperties.Free;
    RawStartOk(LStrm, 'guest', 'guest');
    LTune := RawTune(LStrm);
    LTune.FrameMax := LTune.FrameMax * 2; // asks for more than the broker proposed
    SendMethod(LStrm, AMQP_CHANNEL_CONNECTION, BuildTuneOk(LTune));
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('NOT_ALLOWED', AMQP_NOT_ALLOWED, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.FrameMaxBelowMinimum_530;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LStart: TAMQPConnectionStart;
  LTune: TAMQPConnectionTune;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    LStart := RawStart(LStrm);
    LStart.ServerProperties.Free;
    RawStartOk(LStrm, 'guest', 'guest');
    LTune := RawTune(LStrm);
    LTune.FrameMax := 1024; // below the spec's 4096
    SendMethod(LStrm, AMQP_CHANNEL_CONNECTION, BuildTuneOk(LTune));
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('NOT_ALLOWED', AMQP_NOT_ALLOWED, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.SecondChannelOpen_504;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    TAssert.AssertEquals('Open-Ok on channel 1', 1, Integer(LCh));

    SendMethod(LStrm, 1, BuildChannelOpen); // again on the same channel
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('CHANNEL_ERROR', AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.ChannelNotOpen_504;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
  LDeclare: TAMQPQueueDeclare;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    LDeclare := TAMQPQueueDeclare.Create('q.no.channel');
    SendMethod(LStrm, 7, BuildQueueDeclare(LDeclare)); // channel 7 never opened
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('CHANNEL_ERROR', AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.UnknownFrameType_501;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LF: TAMQPFrame;
  LPayload: TBytes;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SetLength(LPayload, 1);
    LPayload[0] := 0;
    LF := TAMQPFrame.Create(42, 0, LPayload); // nonexistent type
    LF.WriteTo(LStrm);
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('FRAME_ERROR', AMQP_FRAME_ERROR, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.ContentWithoutPublish_505;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LF: TAMQPFrame;
  LPayload: TBytes;
  LClose: TAMQPConnectionClose;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);

    SetLength(LPayload, 4);
    FillChar(LPayload[0], 4, 0);
    LF := TAMQPFrame.Create(AMQP_FRAME_HEADER, 1, LPayload); // without Basic.Publish
    LF.WriteTo(LStrm);
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.ClientClosesConnection_ReceivesCloseOk;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LArgs: TAMQPConnectionClose;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    LArgs.ReplyCode := AMQP_REPLY_SUCCESS;
    LArgs.ReplyText := 'Goodbye';
    LArgs.ClassId := 0;
    LArgs.MethodId := 0;
    SendMethod(LStrm, AMQP_CHANNEL_CONNECTION, BuildClose(LArgs));
    ExpectMethodId(LStrm, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK, LCh);
    TAssert.AssertEquals('Close-Ok on channel 0', 0, Integer(LCh));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

{ THeartbeatTests }

procedure THeartbeatTests.BrokerSendsHeartbeat;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LF: TAMQPFrame;
  LDeadline: UInt64;
  LGot: Boolean;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshakeHb(LStrm, 1); // 1 s => broker emits every ~500 ms
    LGot := False;
    // Only reads: with 1 s negotiated, the first heartbeat has to arrive well
    // before the 2 s after which the broker would consider us dead.
    LDeadline := PcTickMs + 1500;
    while (not LGot) and (PcTickMs < LDeadline) do
    begin
      LF := TAMQPFrame.ReadFrom(LStrm, MAX_PAYLOAD);
      if LF.IsHeartbeat then
        LGot := True;
    end;
    TAssert.AssertTrue('broker sent a heartbeat on its own', LGot);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.SilentPeer_IsDropped;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LDeadline: UInt64;
  LDropped: Boolean;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshakeHb(LStrm, 1);
    // We send nothing else. The broker has to drop us after 2x1 s.
    // We keep reading (only its heartbeats arrive) until the stream ends.
    LDropped := False;
    LDeadline := PcTickMs + 6000;
    while (not LDropped) and (PcTickMs < LDeadline) do
      try
        TAMQPFrame.ReadFrom(LStrm, MAX_PAYLOAD);
      except
        on EAMQPFrame do
          LDropped := True;
      end;
    TAssert.AssertTrue('silent peer dropped by the heartbeat', LDropped);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.HeartbeatZero_DoesNotDrop;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshakeHb(LStrm, 0); // heartbeat disabled by the client
    Sleep(2500);              // more than 2x the default interval, if there were one
    // Still alive: a Channel.Open is still answered.
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    TAssert.AssertEquals('channel open after silence', 1, Integer(LCh));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.NoCloseOk_DroppedAtTheDeadline;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
  LCh: Word;
  LDeadline: UInt64;
  LDropped: Boolean;
begin
  FBroker.CloseTimeoutMs := 700; // applies to the connections accepted from here on
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    // Heartbeat 0 isolates the test: only the Close-Ok deadline can drop us.
    RawHandshakeHb(LStrm, 0);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    SendMethod(LStrm, 1, BuildChannelOpen); // second Open => 504
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('CHANNEL_ERROR', AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode));

    // We never answer the Close-Ok: the broker has to drop us within the deadline.
    LDropped := False;
    LDeadline := PcTickMs + 5000;
    while (not LDropped) and (PcTickMs < LDeadline) do
      try
        TAMQPFrame.ReadFrom(LStrm, MAX_PAYLOAD);
      except
        on EAMQPFrame do
          LDropped := True;
      end;
    TAssert.AssertTrue('dropped for not answering the Close-Ok', LDropped);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.RealClientSurvivesWithHeartbeat;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
  LCh: TAMQPChannel;
begin
  // The broker proposes 1 s; the client negotiates min(1,60) = 1. Both sides start
  // emitting a heartbeat every ~500 ms — if either one is wrong, one
  // drops the other within 2 s.
  FBroker.Heartbeat := 1;
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    LConn := ServerConn(FBroker);
    TAssert.AssertTrue(Assigned(LConn));
    TAssert.AssertEquals('negotiated heartbeat', 1, Integer(LConn.Negotiated.Heartbeat));

    Sleep(3000); // 3x the interval, idle on both sides

    TAssert.AssertTrue('client stays open', LCli.IsOpen);
    TAssert.AssertEquals('broker did not drop it', 1, FBroker.ConnectionCount);
    // And it still really works.
    LCh := LCli.CreateChannel;
    try
      TAssert.AssertTrue('channel opens after the silence', WaitChannels(LConn, 1));
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

{ TRawHandshakeTests -- class outside Phase 1 }

procedure TRawHandshakeTests.UnimplementedClass_ClosesOnlyTheChannel;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPCloseInfo;
  LCh: Word;
  W: TAMQPWriter;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);

    // Tx (class 90) is deliberately outside the scope of Phase 1.
    W := BeginMethod(AMQP_CLASS_TX, 10); // Tx.Select
    try
      SendMethod(LStrm, 1, W.ToBytes);
    finally
      W.Free;
    end;

    LClose := ExpectChannelClose(LStrm, LCh);
    TAssert.AssertEquals('error on channel 1', 1, Integer(LCh));
    TAssert.AssertEquals('NOT_IMPLEMENTED', AMQP_NOT_IMPLEMENTED, Integer(LClose.ReplyCode));

    // The connection survives: we answer the Close-Ok and reopen the channel.
    SendMethod(LStrm, 1, BuildChannelCloseOk);
    OpenChannel(LStrm, 1);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

{ TContentTests }

procedure TContentTests.OnDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  // nothing: Phase 1 delivers no message at all
end;

procedure TContentTests.ClientPublishesSimpleMessage;
var
  LSink: TRecordingSink;
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink; // the broker holds the reference
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.PublishText('', 'test.queue', 'hi from WS5');
      TAssert.AssertTrue('message reached the sink', LSink.WaitCount(1, 3000));
      TAssert.AssertEquals('routing key', 'test.queue', LSink.RoutingKey);
      TAssert.AssertEquals('default exchange', '', LSink.Exchange);
      TAssert.AssertEquals('reassembled body', 'hi from WS5', AmqpUtf8Decode(LSink.Body));
      TAssert.AssertTrue('connection stays open', LCli.IsOpen);
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TContentTests.LargeBodyInSeveralFrames;
var
  LSink: TRecordingSink;
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
  LBody: TBytes;
  LGot: TBytes;
  I: Integer;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  // 300 KB with a 128 KB frame-max => the client splits it into several body frames.
  SetLength(LBody, 300 * 1024);
  for I := 0 to High(LBody) do
    LBody[I] := Byte(I and $FF);

  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.Publish('', 'big', LBody, TAMQPBasicProperties.Empty);
      TAssert.AssertTrue('message arrived', LSink.WaitCount(1, 5000));
      LGot := LSink.Body;
      TAssert.AssertEquals('reassembled size', Length(LBody), Length(LGot));
      for I := 0 to High(LBody) do
        if LGot[I] <> LBody[I] then
          TAssert.Fail(Format('body differs at octet %d', [I]));
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TContentTests.PropertiesAndHeadersArriveIntact;
var
  LSink: TRecordingSink;
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
  LProps: TAMQPBasicProperties;
  LHeaders: TAMQPFieldTable;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      // The caller owns the table: SetHeaders does not copy and Publish does not
      // free it (same convention as the HeadersEventsVcl sample).
      LHeaders := TAMQPFieldTable.Create;
      try
        LHeaders.Put('foo', 'bar');
        LProps := TAMQPBasicProperties.Empty;
        LProps.SetContentType('application/json');
        LProps.SetCorrelationId('req-42');
        LProps.SetHeaders(LHeaders);
        LCh.Publish('', 'props', BytesOf('{}'), LProps);
      finally
        LHeaders.Free;
      end;

      TAssert.AssertTrue('message arrived', LSink.WaitCount(1, 3000));
      TAssert.AssertEquals('content-type', 'application/json', LSink.ContentType);
      TAssert.AssertEquals('correlation-id', 'req-42', LSink.CorrelationId);
      TAssert.AssertEquals('foo header', 'bar', LSink.HeaderFoo);
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TContentTests.MessageWithoutBody_Accepted;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LSink: TRecordingSink;
  LCh: Word;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'empty'));
    // body-size 0: the message completes in the content-header itself.
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(0));
    TAssert.AssertTrue('bodiless message accepted', LSink.WaitCount(1, 3000));
    TAssert.AssertEquals('empty body', 0, Length(LSink.Body));

    // The channel went back to idle: a normal method is answered.
    SendMethod(LStrm, 1, BuildQueueDeclare(TAMQPQueueDeclare.Create('q1')));
    ExpectMethodId(LStrm, AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK, LCh);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.TwoInterleavedChannels;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LSink: TRecordingSink;
  LCh: Word;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    OpenChannel(LStrm, 2);

    // Channel 1 starts publishing and STOPS in the middle...
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r1'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(5));

    // ...and channel 2 publishes a whole one in the middle of that. Interleaving between
    // CHANNELS is legitimate -- it is only forbidden within the same channel.
    SendMethod(LStrm, 2, BuildBasicPublish('', 'r2'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 2, BuildRawContentHeader(4));
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 2, BytesOf('beta'));

    TAssert.AssertTrue('channel 2 completed', LSink.WaitCount(1, 3000));

    // Now channel 1 finishes its own.
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, BytesOf('alpha'));
    TAssert.AssertTrue('channel 1 completed', LSink.WaitCount(2, 3000));

    // Connection intact.
    SendMethod(LStrm, 1, BuildQueueDeclare(TAMQPQueueDeclare.Create('q1')));
    ExpectMethodId(LStrm, AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK, LCh);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.MethodInTheMiddleOfContent_505;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(10));
    // Instead of the body, a method ON THE SAME channel: violates the interleave rule.
    SendMethod(LStrm, 1, BuildQueueDeclare(TAMQPQueueDeclare.Create('q1')));
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.BodyWithoutHeader_505;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    // A body straight away, without the content-header in between.
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, BytesOf('xxx'));
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.BodyLargerThanBodySize_505;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(5));
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, BytesOf('ten octets!'));
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.ContentHeaderOfAnotherClass_505;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    // The content-header's class-id has to match the method's class (60).
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1,
      BuildRawContentHeader(3, AMQP_CLASS_QUEUE));
    LClose := ExpectConnectionClose(LStrm);
    TAssert.AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.ClientDeclaresTopology;
var
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
  LDeclareOk: TAMQPQueueDeclareOk;
  LBind: TAMQPQueueBind;
  LGet: TAMQPGetResult;
  LDelete: TAMQPQueueDelete;
  LTag: string;
begin
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      // Anonymous queue: the name comes from the server.
      LDeclareOk := LCh.DeclareQueue(TAMQPQueueDeclare.Create(''));
      TAssert.AssertTrue('server-generated name (got: ' + LDeclareOk.QueueName + ')',
        Pos('amq.gen-', LDeclareOk.QueueName) = 1);

      LDeclareOk := LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.ws5'));
      TAssert.AssertEquals('name echoed', 'q.ws5', LDeclareOk.QueueName);

      LCh.DeclareExchange(TAMQPExchangeDeclare.Create('x.ws5'));

      LBind.QueueName := 'q.ws5';
      LBind.ExchangeName := 'x.ws5';
      LBind.RoutingKey := 'k';
      LBind.NoWait := False;
      LBind.Arguments := nil;
      LCh.BindQueue(LBind);

      LCh.Qos(10);
      LCh.ConfirmSelect;

      // Null broker: Basic.Get always returns Get-Empty.
      LGet := LCh.BasicGet('q.ws5');
      TAssert.AssertFalse('empty queue on the null broker', LGet.Found);

      LTag := LCh.Consume('q.ws5', OnDelivery, True);
      TAssert.AssertTrue('consumer-tag returned', LTag <> '');
      LCh.Cancel(LTag);

      LDelete.QueueName := 'q.ws5';
      LDelete.IfUnused := False;
      LDelete.IfEmpty := False;
      LDelete.NoWait := False;
      TAssert.AssertEquals('no message deleted', 0, Integer(LCh.DeleteQueue(LDelete)));
      TAssert.AssertTrue('connection intact', LCli.IsOpen);
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

// WS1 of Phase 2 (CLAUDE.md, decision D1): the raw payload of the content-header
// reaches the sink inside the TAMQPServerMessage, and decoding THAT payload
// (instead of the original TAMQPFieldTable) returns the same properties the
// client published -- proves that faithful re-emission works without re-decoding
// anything along the way.
procedure TContentTests.RawHeaderPayload_DecodesTheSame;
var
  LSink: TRecordingSink;
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
  LProps: TAMQPBasicProperties;
  LHeaders: TAMQPFieldTable;
  LReader: TAMQPReader;
  LDecoded: TAMQPContentHeader;
  LFooVal: TValue;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  LCli := TAMQPConnection.Create(Params(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LHeaders := TAMQPFieldTable.Create;
      try
        LHeaders.Put('foo', 'bar');
        LProps := TAMQPBasicProperties.Empty;
        LProps.SetContentType('application/json');
        LProps.SetHeaders(LHeaders);
        LCh.Publish('', 'rawrun', BytesOf('{}'), LProps);
      finally
        LHeaders.Free;
      end;

      TAssert.AssertTrue('message arrived', LSink.WaitCount(1, 3000));
      TAssert.AssertTrue('raw header payload not empty', Length(LSink.HeaderPayload) > 0);

      LReader := TAMQPReader.Create(LSink.HeaderPayload);
      try
        LDecoded := DecodeContentHeader(LReader);
        try
          TAssert.AssertEquals('content-type decoded from the raw payload',
            'application/json', LDecoded.Properties.ContentType);
          TAssert.AssertTrue('foo header present in the raw payload',
            LDecoded.Properties.Headers.TryGetValue('foo', LFooVal));
          TAssert.AssertEquals('value of the foo header', 'bar', LFooVal.AsString);
        finally
          if LDecoded.Properties.Has(bpHeaders) and
             Assigned(LDecoded.Properties.Headers) then
            LDecoded.Properties.Headers.Free;
        end;
      finally
        LReader.Free;
      end;
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

{ TServerTlsTests }

var
  GOpenSslNoticePrinted: Boolean;

function TServerTlsTests.ClientBackend: TPcTlsBackend;
begin
  Result := tbDefault;
end;

function TServerTlsTests.ServerBackend: TPcTlsBackend;
begin
  Result := tbDefault;
end;

function TServerTlsTests.Ready: Boolean;
var
  LError: string;
begin
  Result := True;
  if (AmqpResolveTlsBackend(ClientBackend) <> tbOpenSsl)
    and (AmqpResolveTlsBackend(ServerBackend) <> tbOpenSsl) then
    Exit;
  if PcOpenSslAvailable(LError) then
    Exit;
  {$IFDEF MSWINDOWS}
  // SChannel is the default here and OpenSSL optional: its fixtures pass
  // with a notice instead of failing a machine without it.
  if not GOpenSslNoticePrinted then
  begin
    GOpenSslNoticePrinted := True;
    // One argument: on Delphi, inside a fixture, WriteLn resolves to a
    // one-parameter routine, not to the intrinsic.
    WriteLn('NOTICE: OpenSSL broker TLS tests skipped: ' + LError);
  end;
  Result := False;
  {$ELSE}
  TAssert.Fail('OpenSSL is the only TLS backend here and it is missing: ' +
    LError);
  Result := False;
  {$ENDIF}
end;

function TServerTlsTests.NewBroker(const ACaFile: string): TAMQPServer;
begin
  Result := TAMQPServer.Create;
  try
    Result.BindAddress := '127.0.0.1';
    Result.Port := 0;
    Result.UseTls := True;
    Result.TlsBackend := ServerBackend;
    if AmqpResolveTlsBackend(ServerBackend) = tbOpenSsl then
    begin
      Result.TlsCertFile := Pki('srv_cert.pem');
      Result.TlsKeyFile := Pki('srv_key.pem');
    end
    else
    begin
      Result.TlsCertFile := Pki('srv.pfx');
      Result.TlsCertPassword := PKI_PFX_PASSWORD;
    end;
    if ACaFile <> '' then
      Result.TlsCaFile := Pki(ACaFile);
    Result.Start;
  except
    Result.Free;
    raise;
  end;
end;

function TServerTlsTests.TlsParams(const AClientCert: string): TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := '127.0.0.1';
  Result.Port := FBroker.Port;
  Result.UseTls := True;
  Result.TlsVerifyPeer := True;
  Result.TlsCaFile := Pki('ca_cert.pem'); // the broker's CA, not the system's
  Result.TlsBackend := ClientBackend;
  if AClientCert <> '' then
  begin
    if AmqpResolveTlsBackend(ClientBackend) = tbOpenSsl then
    begin
      Result.TlsCertFile := Pki(AClientCert + '_cert.pem');
      Result.TlsKeyFile := Pki(AClientCert + '_key.pem');
    end
    else
    begin
      Result.TlsCertFile := Pki(AClientCert + '.pfx');
      Result.TlsCertPassword := PKI_PFX_PASSWORD;
    end;
  end;
end;

procedure TServerTlsTests.SetUp;
begin
  // On purpose this does NOT call ServerSetUp: it brings up a plain broker, and here
  // we want one with TLS. ServerTearDown stops and frees this one.
  FBroker := nil;
  FReady := Ready;
  if FReady then
    FBroker := NewBroker('');
end;

procedure TServerTlsTests.TearDown;
begin
  ServerTearDown(FBroker);
end;

procedure TServerTlsTests.RestartWithMtls;
begin
  ServerTearDown(FBroker);
  FBroker := NewBroker('ca_cert.pem');
end;

procedure TServerTlsTests.RealClientConnectsOverTls;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
  LCh: TAMQPChannel;
begin
  if not FReady then
    Exit;
  LCli := TAMQPConnection.Create(TlsParams(''));
  try
    LCli.Open;
    TAssert.AssertTrue('client open over TLS', LCli.IsOpen);
    LConn := ServerConn(FBroker);
    TAssert.AssertTrue('connection registered', Assigned(LConn));
    TAssert.AssertTrue('FSM reached amqssOpen', WaitState(LConn, amqssOpen));
    TAssert.AssertEquals('authenticated user', 'guest', LConn.UserId);
    // It is a real AMQP connection, not just an encrypted socket.
    LCh := LCli.CreateChannel;
    try
      TAssert.AssertTrue('channel opens over TLS', WaitChannels(LConn, 1));
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.PublishesOverTls;
var
  LSink: TRecordingSink;
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
  LBody: TBytes;
  LGot: TBytes;
  I: Integer;
begin
  if not FReady then
    Exit;
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  // 300 KB: forces several body frames AND several TLS records.
  SetLength(LBody, 300 * 1024);
  for I := 0 to High(LBody) do
    LBody[I] := Byte((I * 7) and $FF);

  LCli := TAMQPConnection.Create(TlsParams(''));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.Publish('', 'over.tls', LBody, TAMQPBasicProperties.Empty);
      TAssert.AssertTrue('message arrived', LSink.WaitCount(1, 8000));
      LGot := LSink.Body;
      TAssert.AssertEquals('reassembled size', Length(LBody), Length(LGot));
      for I := 0 to High(LBody) do
        if LGot[I] <> LBody[I] then
          TAssert.Fail(Format('body differs at octet %d', [I]));
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.PlainClientOnTlsPort_Fails;
var
  LP: TAMQPConnectionParams;
  LCli: TAMQPConnection;
  LFailed: Boolean;
begin
  if not FReady then
    Exit;
  LP := TlsParams('');
  LP.UseTls := False; // plaintext client against a port that expects TLS
  LCli := TAMQPConnection.Create(LP);
  try
    LFailed := False;
    try
      LCli.Open;
    except
      on Exception do
        LFailed := True;
    end;
    TAssert.AssertTrue('a plain client cannot open on a TLS port',
      LFailed or (not LCli.IsOpen));
  finally
    LCli.Free;
  end;
  // The broker survives: the next TLS client gets in.
  LCli := TAMQPConnection.Create(TlsParams(''));
  try
    LCli.Open;
    TAssert.AssertTrue('broker keeps accepting TLS', LCli.IsOpen);
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.StartWithMissingCert;
var
  B: TAMQPServer;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.UseTls := True;
    B.TlsBackend := ServerBackend;
    B.TlsCertFile := Pki('does-not-exist.pem');
    B.TlsKeyFile := Pki('does-not-exist.pem');
    B.TlsCertPassword := PKI_PFX_PASSWORD;
    try
      B.Start;
    finally
      TAssert.AssertFalse('a broker without a certificate does not keep running', B.Running);
    end;
  finally
    B.Free;
  end;
end;

procedure TServerTlsTests.MissingCert_StartRaisesEAMQPTls;
begin
  if not FReady then
    Exit;
  // Since 0.6.0 the certificate is loaded once, at Start (the common's
  // PcTlsListen): a wrong file fails there, before any client, instead of
  // failing every handshake.
  CheckRaises('missing certificate', EAMQPTls, StartWithMissingCert);
end;

procedure TServerTlsTests.OpenWithWrongCa;
var
  LP: TAMQPConnectionParams;
  LCli: TAMQPConnection;
begin
  LP := TlsParams('');
  LP.TlsCaFile := Pki('rogue_ca_cert.pem');
  LCli := TAMQPConnection.Create(LP);
  try
    LCli.Open;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.WrongPrivateCa_ClientRejectsWithEAMQPTls;
begin
  if not FReady then
    Exit;
  CheckRaises('server from another CA', EAMQPTls, OpenWithWrongCa);
end;

procedure TServerTlsTests.OpenWithWrongName;
var
  LP: TAMQPConnectionParams;
  LCli: TAMQPConnection;
begin
  LP := TlsParams('');
  // The certificate is for localhost and 127.0.0.1.
  LP.TlsServerName := 'other.example';
  LCli := TAMQPConnection.Create(LP);
  try
    LCli.Open;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.WrongName_ClientRejectsWithEAMQPTls;
begin
  if not FReady then
    Exit;
  CheckRaises('a name the certificate does not have', EAMQPTls, OpenWithWrongName);
end;

procedure TServerTlsTests.Mtls_ClientWithCaCertificate_Connects;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
begin
  if not FReady then
    Exit;
  RestartWithMtls;
  LCli := TAMQPConnection.Create(TlsParams('cli'));
  try
    LCli.Open;
    TAssert.AssertTrue('client with a CA certificate opens', LCli.IsOpen);
    LConn := ServerConn(FBroker);
    TAssert.AssertTrue('FSM reached amqssOpen', WaitState(LConn, amqssOpen));
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.OpenWithoutClientCert;
var
  LCli: TAMQPConnection;
begin
  LCli := TAMQPConnection.Create(TlsParams(''));
  try
    LCli.Open;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.Mtls_ClientWithoutCertificate_Rejected;
var
  LFailed: Boolean;
begin
  if not FReady then
    Exit;
  RestartWithMtls;
  // TLS 1.3 may let the client finish its side of the handshake before the
  // server refuses, so the failure can surface as EAMQPTls or as the
  // connection dropping during the AMQP handshake: either way it must not
  // open.
  LFailed := False;
  try
    OpenWithoutClientCert;
  except
    on Exception do
      LFailed := True;
  end;
  TAssert.AssertTrue('without a client certificate, mTLS refuses', LFailed);
end;

procedure TServerTlsTests.OpenWithRogueClientCert;
var
  LCli: TAMQPConnection;
begin
  LCli := TAMQPConnection.Create(TlsParams('rogue'));
  try
    LCli.Open;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.Mtls_ClientFromAnotherCa_Rejected;
var
  LFailed: Boolean;
begin
  if not FReady then
    Exit;
  RestartWithMtls;
  LFailed := False;
  try
    OpenWithRogueClientCert;
  except
    on Exception do
      LFailed := True;
  end;
  TAssert.AssertTrue('certificate from another CA, mTLS refuses', LFailed);
end;

{$IFDEF MSWINDOWS}
function TServerTlsOpenSslTests.ClientBackend: TPcTlsBackend;
begin
  Result := tbOpenSsl;
end;

function TServerTlsOpenSslTests.ServerBackend: TPcTlsBackend;
begin
  Result := tbOpenSsl;
end;

function TServerTlsOpenSslClientSChannelServerTests.ClientBackend: TPcTlsBackend;
begin
  Result := tbOpenSsl;
end;

function TServerTlsOpenSslClientSChannelServerTests.ServerBackend: TPcTlsBackend;
begin
  Result := tbSChannel;
end;

function TServerTlsSChannelClientOpenSslServerTests.ClientBackend: TPcTlsBackend;
begin
  Result := tbSChannel;
end;

function TServerTlsSChannelClientOpenSslServerTests.ServerBackend: TPcTlsBackend;
begin
  Result := tbOpenSsl;
end;
{$ENDIF}

procedure TRawHandshakeTests.NoLocalNoConsume_540;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPCloseInfo;
  LCh: Word;
  LCons: TAMQPBasicConsume;
begin
  // The client lib exposes neither no-local nor prefetch-size, so these two
  // rows of the error table are only reachable through a raw socket.
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    SendMethod(LStrm, 1, BuildQueueDeclare(TAMQPQueueDeclare.Create('q.nl')));
    ExpectMethodId(LStrm, AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK, LCh);

    LCons := TAMQPBasicConsume.Create('q.nl', 'ct', True);
    LCons.NoLocal := True;
    SendMethod(LStrm, 1, BuildBasicConsume(LCons));

    LClose := ExpectChannelClose(LStrm, LCh);
    TAssert.AssertEquals('no-local closes the channel with NOT_IMPLEMENTED',
      AMQP_NOT_IMPLEMENTED, Integer(LClose.ReplyCode));
    TAssert.AssertEquals('on the offending channel', 1, Integer(LCh));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.PrefetchSizeNonZero_540;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LClose: TAMQPCloseInfo;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);

    // prefetch-size (a limit in OCTETS) is not implemented -- and the broker
    // refuses instead of silently accepting.
    SendMethod(LStrm, 1, BuildBasicQos(10, False, 4096));

    LClose := ExpectChannelClose(LStrm, LCh);
    TAssert.AssertEquals('prefetch-size closes the channel with NOT_IMPLEMENTED',
      AMQP_NOT_IMPLEMENTED, Integer(LClose.ReplyCode));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.MandatoryNoRoute_ReturnBeforeAck_312;
var
  LSock: TPcEndpoint;
  LStrm: TAMQPEndpointStream;
  LCh: Word;
  LDecl: TAMQPExchangeDeclare;
  LMsgBody: TBytes;
  LFrame: TAMQPFrame;
  LR: TAMQPReader;
  LId: TAMQPMethodId;
  LReturn: TAMQPBasicReturn;
  LAck: TAMQPBasicAck;
begin
  // The ORDER between Basic.Return and Basic.Ack is the subtle point of WS6, and no
  // test through the client observes it deterministically (the two arrive in
  // callbacks on different threads). On the raw socket, the order on the wire is the
  // assertion itself.
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);

    LDecl := TAMQPExchangeDeclare.Create('ex.ord', AMQP_EXCHANGE_TYPE_DIRECT);
    SendMethod(LStrm, 1, BuildExchangeDeclare(LDecl));
    ExpectMethodId(LStrm, AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE_OK, LCh);

    SendMethod(LStrm, 1, BuildConfirmSelect(False));
    ExpectMethodId(LStrm, AMQP_CLASS_CONFIRM, AMQP_CONFIRM_SELECT_OK, LCh);

    // Mandatory publish to an exchange with NO binding at all.
    LMsgBody := AmqpUtf8Encode('no route');
    SendMethod(LStrm, 1, BuildBasicPublish('ex.ord', 'nothing', True, False));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1,
      BuildContentHeader(Length(LMsgBody), TAMQPBasicProperties.Empty));
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, LMsgBody);

    // 1st: the Basic.Return, with the content right behind it.
    LFrame := TAMQPFrame.ReadFrom(LStrm);
    TAssert.AssertEquals('1st frame is a method', Integer(AMQP_FRAME_METHOD),
      Integer(LFrame.FrameType));
    LR := TAMQPReader.Create(LFrame.Payload);
    try
      LId.ClassId := LR.ReadShortUInt;
      LId.MethodId := LR.ReadShortUInt;
      TAssert.AssertTrue('the 1st method is Basic.Return, not the Ack',
        LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN));
      LReturn := DecodeBasicReturn(LR);
    finally
      LR.Free;
    end;
    TAssert.AssertEquals('NO_ROUTE', Integer(AMQP_NO_ROUTE), Integer(LReturn.ReplyCode));
    TAssert.AssertEquals('exchange returned', 'ex.ord', LReturn.Exchange);
    TAssert.AssertEquals('routing key returned', 'nothing', LReturn.RoutingKey);

    LFrame := TAMQPFrame.ReadFrom(LStrm);
    TAssert.AssertEquals('2nd frame is the content-header', Integer(AMQP_FRAME_HEADER),
      Integer(LFrame.FrameType));
    LFrame := TAMQPFrame.ReadFrom(LStrm);
    TAssert.AssertEquals('3rd frame is the body', Integer(AMQP_FRAME_BODY),
      Integer(LFrame.FrameType));
    TAssert.AssertEquals('with the original body', 'no route',
      AmqpUtf8Decode(LFrame.Payload));

    // ONLY THEN the confirm's Basic.Ack: a message without a route is also confirmed.
    LFrame := TAMQPFrame.ReadFrom(LStrm);
    TAssert.AssertEquals('4th frame is a method', Integer(AMQP_FRAME_METHOD),
      Integer(LFrame.FrameType));
    LR := TAMQPReader.Create(LFrame.Payload);
    try
      LId.ClassId := LR.ReadShortUInt;
      LId.MethodId := LR.ReadShortUInt;
      TAssert.AssertTrue('it is the Basic.Ack, after the Return',
        LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK));
      LAck := DecodeBasicAck(LR);
    finally
      LR.Free;
    end;
    TAssert.AssertEquals('confirming seq-no 1', 1, Integer(LAck.DeliveryTag));
    TAssert.AssertFalse('without multiple', LAck.Multiple);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

initialization
  RegisterTest(TClientHandshakeTests);
  RegisterTest(TRawHandshakeTests);
  RegisterTest(THeartbeatTests);
  RegisterTest(TContentTests);
  RegisterTest(TServerTlsTests);
  {$IFDEF MSWINDOWS}
  RegisterTest(TServerTlsOpenSslTests);
  RegisterTest(TServerTlsOpenSslClientSChannelServerTests);
  RegisterTest(TServerTlsSChannelClientOpenSslServerTests);
  {$ENDIF}

end.
