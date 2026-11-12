unit AMQP.TransportTests;

{ The library's side of the transport, without sockets (0.6.0): the
  exception boundary of TAMQPEndpointStream (decision T6 of pascal-common-faa's
  transport-design), the mapping of TAMQPConnectionParams to the common's TLS
  options, and which backend tbDefault means in this build. The sockets and
  TLS themselves are tested in pascal-common-faa; the broker and the client
  over them, in tests/Server. }

interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Classes,
  PascalCommon.Transport,
  AMQP.Transport,
  AMQP.Frame,
  AMQP.Connection,
  AMQP.TestSupport;

type
  [TestFixture]
  TTransportTests = class
  private
    FStream: TAMQPEndpointStream;
    procedure ReadOnce;
    procedure WriteOnce;
    procedure ReadFrame;
  public
    [TearDown] procedure TearDown;
    [Test] procedure Read_PeerClosed_ReturnsZero;
    [Test] procedure Read_PeerClosed_FrameReaderSeesEOF;
    [Test] procedure Read_TransportError_BecomesEAMQPTransport;
    [Test] procedure Read_Timeout_BecomesEAMQPTransport;
    [Test] procedure Read_TlsError_BecomesEAMQPTls;
    [Test] procedure Write_PeerClosed_BecomesEAMQPTransport;
    [Test] procedure Write_TlsError_BecomesEAMQPTls;
    [Test] procedure Message_PreservedInTranslation;
    [Test] procedure EAMQPTls_DoesNotInheritFromEAMQPTransport;
    [Test] procedure Params_Localhost_TlsDefaults;
    [Test] procedure ClientTlsOptions_MapsAllFields;
    [Test] procedure ClientTlsOptions_DefaultVerifyPeer_Validates;
    [Test] procedure ClientTlsOptions_DefaultBackend_Resolved;
    [Test] procedure DefaultBackend_OfThisBuild;
  end;

implementation

type
  { An endpoint that fails every read and write with the exception it was
    given (class and message). }
  TFailingEndpoint = class(TPcEndpoint)
  private
    FClass: ExceptClass;
    FMessage: string;
  public
    constructor Create(AClass: ExceptClass; const AMessage: string);
    function Read(var ABuffer; ACount: Integer): Integer; override;
    procedure WriteExactly(const ABuffer; ACount: Integer); override;
    procedure CloseAbort; override;
  end;

  { The stream owns nothing; this one frees its endpoint, for the tests. }
  TOwningStream = class(TAMQPEndpointStream)
  public
    destructor Destroy; override;
  end;

constructor TFailingEndpoint.Create(AClass: ExceptClass; const AMessage: string);
begin
  inherited Create;
  FClass := AClass;
  FMessage := AMessage;
end;

function TFailingEndpoint.Read(var ABuffer; ACount: Integer): Integer;
begin
  raise FClass.Create(FMessage);
end;

procedure TFailingEndpoint.WriteExactly(const ABuffer; ACount: Integer);
begin
  raise FClass.Create(FMessage);
end;

procedure TFailingEndpoint.CloseAbort;
begin
end;

destructor TOwningStream.Destroy;
begin
  Endpoint.Free;
  inherited;
end;

function NewStream(AClass: ExceptClass; const AMessage: string): TAMQPEndpointStream;
begin
  Result := TOwningStream.Create(TFailingEndpoint.Create(AClass, AMessage));
end;

{ TTransportTests }

procedure TTransportTests.TearDown;
begin
  FreeAndNil(FStream);
end;

procedure TTransportTests.ReadOnce;
var
  B: array[0..7] of Byte;
begin
  FStream.Read(B[0], SizeOf(B));
end;

procedure TTransportTests.WriteOnce;
var
  B: array[0..7] of Byte;
begin
  FillChar(B, SizeOf(B), 0);
  FStream.Write(B[0], SizeOf(B));
end;

procedure TTransportTests.ReadFrame;
begin
  TAMQPFrame.ReadFrom(FStream);
end;

procedure TTransportTests.Read_PeerClosed_ReturnsZero;
var
  B: array[0..7] of Byte;
begin
  // The peer closing is the end of the stream for a frame reader, as with
  // the old socket stream (Receive <= 0), not an error.
  FStream := NewStream(EPcTransportClosed, 'closed by the peer');
  TAssert.AssertEquals('fim de stream', 0, FStream.Read(B[0], SizeOf(B)));
end;

procedure TTransportTests.Read_PeerClosed_FrameReaderSeesEOF;
begin
  // What the client's and the broker's read loops catch to end normally.
  FStream := NewStream(EPcTransportClosed, 'closed by the peer');
  CheckRaises('peer fechou durante um frame', EAMQPFrameEOF, ReadFrame);
end;

procedure TTransportTests.Read_TransportError_BecomesEAMQPTransport;
begin
  FStream := NewStream(EPcTransport, 'recv failed');
  CheckRaises('erro de transporte na leitura', EAMQPTransport, ReadOnce);
end;

procedure TTransportTests.Read_Timeout_BecomesEAMQPTransport;
begin
  // The library sets no deadline, but the class must not leak if one fires.
  FStream := NewStream(EPcTransportTimeout, 'read timed out');
  CheckRaises('prazo na leitura', EAMQPTransport, ReadOnce);
end;

procedure TTransportTests.Read_TlsError_BecomesEAMQPTls;
begin
  FStream := NewStream(EPcTls, 'bad record mac');
  CheckRaises('erro de TLS na leitura', EAMQPTls, ReadOnce);
end;

procedure TTransportTests.Write_PeerClosed_BecomesEAMQPTransport;
begin
  // Writing has no "end of stream": a closed peer is an error the writer
  // must see (the client turns it into EAMQPConnection).
  FStream := NewStream(EPcTransportClosed, 'closed by the peer');
  CheckRaises('peer fechou na escrita', EAMQPTransport, WriteOnce);
end;

procedure TTransportTests.Write_TlsError_BecomesEAMQPTls;
begin
  FStream := NewStream(EPcTls, 'encrypt failed');
  CheckRaises('erro de TLS na escrita', EAMQPTls, WriteOnce);
end;

procedure TTransportTests.Message_PreservedInTranslation;
var
  LMsg: string;
begin
  FStream := NewStream(EPcTls, 'certificate verify failed');
  LMsg := '';
  try
    ReadOnce;
  except
    on E: EAMQPTls do
      LMsg := E.Message;
  end;
  TAssert.AssertEquals('a mensagem da common, sem mudanca',
    'certificate verify failed', LMsg);
end;

procedure TTransportTests.EAMQPTls_DoesNotInheritFromEAMQPTransport;
begin
  // An "except on EAMQPTransport" written before 0.6.0 must not start
  // catching TLS failures.
  TAssert.AssertFalse('EAMQPTls fora da hierarquia de EAMQPTransport',
    EAMQPTls.InheritsFrom(EAMQPTransport));
end;

procedure TTransportTests.Params_Localhost_TlsDefaults;
var
  P: TAMQPConnectionParams;
begin
  P := TAMQPConnectionParams.Localhost;
  TAssert.AssertFalse('TLS desligado', P.UseTls);
  TAssert.AssertTrue('valida o servidor', P.TlsVerifyPeer);
  TAssert.AssertEquals('sem CA privada', '', P.TlsCaFile);
  TAssert.AssertEquals('sem certificado de cliente', '', P.TlsCertFile);
  TAssert.AssertEquals('sem chave', '', P.TlsKeyFile);
  TAssert.AssertEquals('sem senha', '', P.TlsCertPassword);
  TAssert.AssertTrue('backend padrao', P.TlsBackend = tbDefault);
  TAssert.AssertEquals('connect: o padrao da common', 0,
    Integer(P.ConnectTimeoutMs));
end;

procedure TTransportTests.ClientTlsOptions_MapsAllFields;
var
  P: TAMQPConnectionParams;
  O: TPcTlsOptions;
begin
  P := TAMQPConnectionParams.LocalhostTls;
  P.TlsCaFile := 'ca.pem';
  P.TlsCertFile := 'cli.pem';
  P.TlsKeyFile := 'cli.key';
  P.TlsCertPassword := 'segredo';
  P.TlsVerifyPeer := False;
  P.TlsBackend := tbOpenSsl;
  O := AmqpClientTlsOptions(P);
  TAssert.AssertEquals('CaFile', 'ca.pem', O.CaFile);
  TAssert.AssertEquals('CertFile', 'cli.pem', O.CertFile);
  TAssert.AssertEquals('KeyFile', 'cli.key', O.KeyFile);
  TAssert.AssertEquals('CertPassword', 'segredo', O.CertPassword);
  TAssert.AssertTrue('TlsVerifyPeer=False pula a validacao',
    O.SkipServerVerification);
  TAssert.AssertTrue('backend pedido', O.Backend = tbOpenSsl);
  // No socket deadline: only the handshake's, the common's default.
  TAssert.AssertEquals('prazo do handshake: o padrao da common', 0,
    Integer(O.HandshakeTimeoutMs));
end;

procedure TTransportTests.ClientTlsOptions_DefaultVerifyPeer_Validates;
var
  P: TAMQPConnectionParams;
begin
  P := TAMQPConnectionParams.Localhost;
  P.UseTls := True;
  TAssert.AssertFalse('o padrao valida o servidor',
    AmqpClientTlsOptions(P).SkipServerVerification);
end;

procedure TTransportTests.ClientTlsOptions_DefaultBackend_Resolved;
var
  P: TAMQPConnectionParams;
begin
  P := TAMQPConnectionParams.Localhost;
  TAssert.AssertTrue('tbDefault resolvido pelo build',
    AmqpClientTlsOptions(P).Backend = AmqpDefaultTlsBackend);
end;

procedure TTransportTests.DefaultBackend_OfThisBuild;
begin
  // AMQP_OPENSSL only changes the default (decision T3).
  {$IFDEF AMQP_OPENSSL}
  TAssert.AssertTrue('-dAMQP_OPENSSL: OpenSSL', AmqpDefaultTlsBackend = tbOpenSsl);
  {$ELSE}
    {$IFDEF MSWINDOWS}
  TAssert.AssertTrue('Windows: SChannel', AmqpDefaultTlsBackend = tbSChannel);
    {$ELSE}
  TAssert.AssertTrue('fora do Windows: OpenSSL', AmqpDefaultTlsBackend = tbOpenSsl);
    {$ENDIF}
  {$ENDIF}
  TAssert.AssertEquals('nome do backend', 'OpenSSL',
    AmqpTlsBackendName(tbOpenSsl));
end;

initialization
  TDUnitX.RegisterTestFixture(TTransportTests);

end.
