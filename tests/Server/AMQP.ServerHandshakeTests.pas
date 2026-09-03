unit AMQP.ServerHandshakeTests;

{ Testes da FSM de handshake e do ciclo de vida de canais do broker (WS4).

  Duas frentes:
  - TClientHandshakeTests: o CLIENTE REAL da lib (TAMQPConnection) conecta no
    broker in-process. É o teste que importa — se o handshake estiver errado, o
    cliente não abre.
  - TRawHandshakeTests: socket cru montando frames à mão, para exercitar os
    caminhos de erro que um cliente correto nunca produz (método fora de ordem,
    Tune-Ok acima do proposto, segundo Channel.Open, conteúdo sem publish...).

  Espelho DUnitX de tests\Server\fpc\AMQP.ServerHandshakeTests.pas — mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas asserções). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.Rtti,
  System.SyncObjs,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  AMQP.Threading,
  AMQP.Transport,
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
  [TestFixture]
  TClientHandshakeTests = class
  private
    FBroker: TAMQPServer;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure ClienteRealConecta;
    [Test] procedure SenhaErrada_403;
    [Test] procedure VhostInexistente_530;
    [Test] procedure VhostExtraAceito;
    [Test] procedure CanalAbreEFecha;
    [Test] procedure DuasConexoesSimultaneas;
    [Test] procedure ClienteFechaLimpo;
  end;

  [TestFixture]
  TRawHandshakeTests = class
  private
    FBroker: TAMQPServer;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure StartAnunciaPlainEServerProperties;
    [Test] procedure MetodoForaDeOrdem_503;
    [Test] procedure ConnectionEmCanalNaoZero_503;
    [Test] procedure TuneOkAcimaDoProposto_530;
    [Test] procedure FrameMaxAbaixoDoMinimo_530;
    [Test] procedure SegundoChannelOpen_504;
    [Test] procedure CanalNaoAberto_504;
    [Test] procedure TipoDeFrameDesconhecido_501;
    [Test] procedure ConteudoSemPublish_505;
    [Test] procedure ClienteFechaConexao_RecebeCloseOk;
    [Test] procedure ClasseNaoImplementada_FechaSoOCanal;
    [Test] procedure NoLocalNoConsume_540;
    [Test] procedure PrefetchSizeNaoZero_540;
    [Test] procedure MandatorySemRota_ReturnAntesDoAck_312;
  end;

  { Conteudo (WS5): remontagem de Basic.Publish + content-header + body frames,
    a regra de interleave da spec 2.3.5 e o despacho "nulo" das classes de
    recurso -- todo metodo cliente->servidor e' decodificado e respondido com o
    *-Ok correto, sem engine por tras. }
  [TestFixture]
  TContentTests = class
  private
    FBroker: TAMQPServer;
    /// Consumer nunca chamado (o broker nulo nao entrega); a lib exige um
    /// metodo de objeto, nao aceita closure.
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure ClientePublicaMensagemSimples;
    [Test] procedure CorpoGrandeEmVariosFrames;
    [Test] procedure PropriedadesEHeadersChegamIntactos;
    [Test] procedure MensagemSemCorpo_Aceita;
    [Test] procedure DoisCanaisIntercalados;
    [Test] procedure MetodoNoMeioDoConteudo_505;
    [Test] procedure BodySemHeader_505;
    [Test] procedure CorpoMaiorQueOBodySize_505;
    [Test] procedure ContentHeaderDeOutraClasse_505;
    [Test] procedure ClienteDeclaraTopologia;
    [Test] procedure HeaderPayloadCru_DecodificaIgual;
  end;

  {$IFDEF AMQP_OPENSSL}
  { TLS do lado servidor (WS3). So existe em builds com -dAMQP_OPENSSL: o
    backend SChannel desta lib implementa apenas o lado cliente, entao no build
    Default o broker nao aceita TLS e nao ha o que testar. Usa os certs de dev
    de docker\certs (self-signed), por isso o cliente conecta com
    TlsVerifyPeer=False. }
  [TestFixture]
  TServerTlsTests = class
  private
    FBroker: TAMQPServer;
    function TlsParams: TAMQPConnectionParams;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure ClienteRealConectaSobreTls;
    [Test] procedure PublicaSobreTls;
    [Test] procedure ClientePlainNaPortaTls_Falha;
    [Test] procedure CertInexistente_ConexaoFalha;
  end;
  {$ENDIF}

  { Heartbeat (spec 4.2.7) e o prazo do Connection.Close-Ok. Os testes negociam
    intervalos de 1 s no Tune-Ok para caber num tempo de suíte razoável. }
  [TestFixture]
  THeartbeatTests = class
  private
    FBroker: TAMQPServer;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure BrokerEmiteHeartbeat;
    [Test] procedure PeerMudo_EhDerrubado;
    [Test] procedure HeartbeatZero_NaoDerruba;
    [Test] procedure SemCloseOk_DerrubadoNoPrazo;
    [Test] procedure ClienteRealSobreviveComHeartbeat;
  end;

implementation

const
  MAX_PAYLOAD = 131072;

// --- fixture compartilhada (funções livres: as duas fixtures usam) ---------

function NewBroker: TAMQPServer;
begin
  Result := TAMQPServer.Create;
  Result.BindAddress := '127.0.0.1';
  Result.Port := 0;
  Result.Start;
end;

// Parâmetros do cliente apontando para ABroker.
function BrokerParams(ABroker: TAMQPServer): TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := '127.0.0.1';
  Result.Port := ABroker.Port;
end;

// Primeira conexão viva do lado servidor (espera até ATimeoutMs).
function ServerConn(ABroker: TAMQPServer;
  ATimeoutMs: Integer = 3000): TAMQPServerConnection;
var
  LDeadline: UInt64;
  LAll: TArray<TAMQPServerConnection>;
begin
  Result := nil;
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    LAll := ABroker.Connections;
    if Length(LAll) > 0 then
      Exit(LAll[0]);
    TThread.Sleep(10);
  until AmqpTickMs > LDeadline;
end;

function WaitState(AConn: TAMQPServerConnection; AState: TAMQPServerConnState;
  ATimeoutMs: Integer = 3000): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    if AConn.State = AState then
      Exit(True);
    TThread.Sleep(10);
  until AmqpTickMs > LDeadline;
  Result := AConn.State = AState;
end;

function WaitChannels(AConn: TAMQPServerConnection; ACount: Integer;
  ATimeoutMs: Integer = 3000): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    if AConn.ChannelCount = ACount then
      Exit(True);
    TThread.Sleep(10);
  until AmqpTickMs > LDeadline;
  Result := AConn.ChannelCount = ACount;
end;

// --- helpers de frame -----------------------------------------------------

// Lê o próximo frame de método (pulando heartbeats). O chamador libera o reader.
function ReadMethod(AStream: TStream; out ACh: Word;
  out AId: TAMQPMethodId): TAMQPReader;
var
  LF: TAMQPFrame;
begin
  repeat
    LF := TAMQPFrame.ReadFrom(AStream, MAX_PAYLOAD);
  until not LF.IsHeartbeat;
  if not LF.IsMethod then
    raise Exception.CreateFmt('esperava frame de metodo, veio tipo %d',
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

// --- passos do handshake do lado cliente (cru) ----------------------------

// Manda o protocol-header e consome o Connection.Start. O chamador libera as
// server-properties do resultado.
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
      raise Exception.CreateFmt('esperava Connection.Start, veio %d/%d',
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
  LProps := DefaultClientProperties; // anuncia authentication_failure_close
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
      raise Exception.CreateFmt('esperava Connection.Tune, veio %d/%d',
        [LId.ClassId, LId.MethodId]);
    Result := DecodeTune(LR);
  finally
    LR.Free;
  end;
end;

// Handshake completo, do header ao Open-Ok. Levanta se algo sair da ordem.
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
      raise Exception.CreateFmt('esperava Connection.Open-Ok, veio %d/%d',
        [LId.ClassId, LId.MethodId]);
  finally
    LR.Free;
  end;
end;

// Handshake completo forçando um heartbeat no Tune-Ok (o broker adota o valor
// que o CLIENTE devolve, conforme a spec).
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
      raise Exception.CreateFmt('esperava Connection.Open-Ok, veio %d/%d',
        [LId.ClassId, LId.MethodId]);
  finally
    LR.Free;
  end;
end;

// Lê o que vier esperando um Connection.Close.
function ExpectConnectionClose(AStream: TStream): TAMQPConnectionClose;
var
  LR: TAMQPReader;
  LCh: Word;
  LId: TAMQPMethodId;
begin
  LR := ReadMethod(AStream, LCh, LId);
  try
    if not LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
      raise Exception.CreateFmt('esperava Connection.Close, veio %d/%d no canal %d',
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
      raise Exception.CreateFmt('esperava Channel.Close, veio %d/%d',
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
      raise Exception.CreateFmt('esperava %d/%d, veio %d/%d',
        [AClassId, AMethodId, LId.ClassId, LId.MethodId]);
  finally
    LR.Free;
  end;
end;

// Abre um socket cru contra o broker; devolve o stream. O chamador libera o
// stream e DEPOIS o socket devolvido em ASock.
function RawConnect(APort: Word; out ASock: TAMQPTcpSocket): TAMQPSocketStream;
begin
  ASock := TAMQPTcpSocket.Create;
  ASock.Connect('127.0.0.1', APort);
  Result := TAMQPSocketStream.Create(ASock);
end;

// Sobe da pasta do executavel procurando docker\certs\server.crt: as duas
// suites rodam de profundidades diferentes (tests\Server\fpc no FPC,
// tests\Server\Win32\<config> no Delphi).
function CertsDir: string;
var
  LDir: string;
  I: Integer;
begin
  LDir := ExtractFilePath(ParamStr(0));
  for I := 0 to 6 do
  begin
    if FileExists(LDir + 'docker' + PathDelim + 'certs' + PathDelim + 'server.crt') then
      Exit(LDir + 'docker' + PathDelim + 'certs' + PathDelim);
    LDir := ExtractFilePath(ExcludeTrailingPathDelimiter(LDir));
    if LDir = '' then
      Break;
  end;
  Result := '';
end;

// Broker com TLS ligado apontando para os certs de dev do repo.
function NewTlsBroker: TAMQPServer;
var
  LCerts: string;
begin
  LCerts := CertsDir;
  if LCerts = '' then
    raise Exception.Create('docker/certs/server.crt nao encontrado a partir de ' +
      ExtractFilePath(ParamStr(0)));
  Result := TAMQPServer.Create;
  Result.BindAddress := '127.0.0.1';
  Result.Port := 0;
  Result.UseTls := True;
  Result.TlsCertFile := LCerts + 'server.crt';
  Result.TlsKeyFile := LCerts + 'server.key';
  Result.Start;
end;

{ Sink que grava o que passou, para os testes conferirem a remontagem. As
  chamadas vem da thread da conexao, entao tudo e' protegido. A mensagem e a
  tabela de headers pertencem ao canal e somem assim que RouteMessage retorna:
  por isso copiamos o que interessa AQUI DENTRO. }
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
      const AMessage: TAMQPServerMessage): Boolean;
    function Count: Integer;
    function Exchange: string;
    function RoutingKey: string;
    function Body: TBytes;
    function ContentType: string;
    function CorrelationId: string;
    function HeaderFoo: string;
    /// Payload cru do content-header (decisao D1 da Fase 2, ver CLAUDE.md e
    /// AMQP.Server.Message) -- o que a mensagem entrega a' engine.
    function HeaderPayload: TBytes;
    /// Espera ACount mensagens roteadas por ate ATimeoutMs.
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
  const AMessage: TAMQPServerMessage): Boolean;
var
  LValue: TValue;
begin
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
  Result := False; // broker nulo: nada foi roteado
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
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    if Count >= ACount then
      Exit(True);
    TThread.Sleep(10);
  until AmqpTickMs > LDeadline;
  Result := Count >= ACount;
end;

// Monta um content-header de Basic (classe 60) com o body-size dado e nenhuma
// propriedade. AClassId permite forjar uma classe errada nos testes de erro.
function BuildRawContentHeader(ABodySize: UInt64; AClassId: Word = 60): TBytes;
var
  W: TAMQPWriter;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteShortUInt(AClassId);
    W.WriteShortUInt(0);            // weight (reservado, sempre 0)
    W.WriteLongLongUInt(ABodySize);
    W.WriteShortUInt(0);            // property flags: nenhuma propriedade
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

// Abre um canal e confirma o Open-Ok.
procedure OpenChannel(AStream: TStream; ACh: Word);
var
  LGot: Word;
begin
  SendMethod(AStream, ACh, BuildChannelOpen);
  ExpectMethodId(AStream, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LGot);
  if LGot <> ACh then
    raise Exception.CreateFmt('Open-Ok veio no canal %d, esperava %d',
      [LGot, ACh]);
end;

{ TClientHandshakeTests }

procedure TClientHandshakeTests.Setup;
begin
  FBroker := NewBroker;
end;

procedure TClientHandshakeTests.TearDown;
begin
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
end;

procedure TClientHandshakeTests.ClienteRealConecta;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
begin
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    Assert.IsTrue(LCli.IsOpen, 'cliente aberto');

    LConn := ServerConn(FBroker);
    Assert.IsNotNull(LConn, 'conexao registrada no broker');
    Assert.IsTrue(WaitState(LConn, amqssOpen), 'FSM chegou a amqssOpen');
    Assert.AreEqual('guest', LConn.UserId, 'usuario autenticado');
    Assert.AreEqual('/', LConn.VirtualHost, 'vhost');
    Assert.AreEqual(AMQP_SERVER_DEFAULT_CHANNEL_MAX,
      Integer(LConn.Negotiated.ChannelMax), 'channel-max negociado');
    Assert.AreEqual(Integer(AMQP_SERVER_DEFAULT_FRAME_MAX),
      Integer(LConn.Negotiated.FrameMax), 'frame-max negociado');
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.SenhaErrada_403;
var
  LCli: TAMQPConnection;
  LP: TAMQPConnectionParams;
  LMsg: string;
begin
  LP := BrokerParams(FBroker);
  LP.Password := 'senha-errada';
  LCli := TAMQPConnection.Create(LP);
  try
    LMsg := '';
    try
      LCli.Open;
    except
      on E: Exception do
        LMsg := E.Message;
    end;
    Assert.IsFalse(LCli.IsOpen, 'nao deve abrir');
    Assert.IsTrue(Pos('403', LMsg) > 0, 'mensagem cita 403 (veio: ' + LMsg + ')');
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.VhostInexistente_530;
var
  LCli: TAMQPConnection;
  LP: TAMQPConnectionParams;
  LMsg: string;
begin
  LP := BrokerParams(FBroker);
  LP.VirtualHost := '/nao-existe';
  LCli := TAMQPConnection.Create(LP);
  try
    LMsg := '';
    try
      LCli.Open;
    except
      on E: Exception do
        LMsg := E.Message;
    end;
    Assert.IsFalse(LCli.IsOpen, 'nao deve abrir');
    Assert.IsTrue(Pos('530', LMsg) > 0, 'mensagem cita 530 (veio: ' + LMsg + ')');
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.VhostExtraAceito;
var
  LCli: TAMQPConnection;
  LP: TAMQPConnectionParams;
begin
  FBroker.VirtualHosts.Add('/app');
  LP := BrokerParams(FBroker);
  LP.VirtualHost := '/app';
  LCli := TAMQPConnection.Create(LP);
  try
    LCli.Open;
    Assert.IsTrue(LCli.IsOpen, 'cliente aberto no /app');
    Assert.AreEqual('/app', ServerConn(FBroker).VirtualHost);
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.CanalAbreEFecha;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
  LCh1, LCh2: TAMQPChannel;
begin
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    LConn := ServerConn(FBroker);
    Assert.IsNotNull(LConn);

    LCh1 := LCli.CreateChannel;
    try
      Assert.IsTrue(WaitChannels(LConn, 1), '1 canal no servidor');
      LCh2 := LCli.CreateChannel;
      try
        Assert.IsTrue(WaitChannels(LConn, 2), '2 canais no servidor');
        LCh2.Close;
      finally
        LCh2.Free;
      end;
      Assert.IsTrue(WaitChannels(LConn, 1), 'volta a 1 canal apos Channel.Close');
      LCh1.Close;
    finally
      LCh1.Free;
    end;
    Assert.IsTrue(WaitChannels(LConn, 0), 'nenhum canal aberto');
    Assert.IsTrue(LCli.IsOpen, 'conexao segue aberta');
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.DuasConexoesSimultaneas;
var
  LA, LB: TAMQPConnection;
begin
  LA := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LB := TAMQPConnection.Create(BrokerParams(FBroker));
    try
      LA.Open;
      LB.Open;
      Assert.IsTrue(LA.IsOpen);
      Assert.IsTrue(LB.IsOpen);
      Assert.AreEqual(2, FBroker.ConnectionCount, 'duas conexoes vivas');
      Assert.AreEqual(2, FBroker.TotalAccepted);
    finally
      LB.Free;
    end;
  finally
    LA.Free;
  end;
end;

procedure TClientHandshakeTests.ClienteFechaLimpo;
var
  LCli: TAMQPConnection;
  LDeadline: UInt64;
begin
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    Assert.IsNotNull(ServerConn(FBroker));
    LCli.Close; // Connection.Close + espera o Close-Ok do broker
    Assert.IsFalse(LCli.IsOpen, 'cliente fechado');

    LDeadline := AmqpTickMs + 3000;
    while (FBroker.ConnectionCount > 0) and (AmqpTickMs < LDeadline) do
      TThread.Sleep(10);
    Assert.AreEqual(0, FBroker.ConnectionCount, 'broker reapou a conexao');
  finally
    LCli.Free;
  end;
end;

{ TRawHandshakeTests }

procedure TRawHandshakeTests.Setup;
begin
  FBroker := NewBroker;
end;

procedure TRawHandshakeTests.TearDown;
begin
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
end;

procedure TRawHandshakeTests.StartAnunciaPlainEServerProperties;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LStart: TAMQPConnectionStart;
  LProd: TValue;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    LStart := RawStart(LStrm);
    try
      Assert.AreEqual(0, Integer(LStart.VersionMajor), 'version-major');
      Assert.AreEqual(9, Integer(LStart.VersionMinor), 'version-minor');
      Assert.IsTrue(LStart.SupportsMechanism('PLAIN'), 'oferece PLAIN');
      Assert.IsTrue(LStart.ServerProperties.TryGetValue('product', LProd),
        'server-properties tem product');
      Assert.AreEqual(AMQP_SERVER_PRODUCT, LProd.AsString);
    finally
      LStart.ServerProperties.Free;
    end;
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.MetodoForaDeOrdem_503;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LStart: TAMQPConnectionStart;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    LStart := RawStart(LStrm);
    LStart.ServerProperties.Free;
    // Channel.Open antes de autenticar.
    SendMethod(LStrm, 1, BuildChannelOpen);
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_COMMAND_INVALID, Integer(LClose.ReplyCode),
      'COMMAND_INVALID');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.ConnectionEmCanalNaoZero_503;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
  LCloseArgs: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    LCloseArgs.ReplyCode := AMQP_REPLY_SUCCESS;
    LCloseArgs.ReplyText := 'tchau';
    LCloseArgs.ClassId := 0;
    LCloseArgs.MethodId := 0;
    SendMethod(LStrm, 3, BuildClose(LCloseArgs)); // Connection.* fora do canal 0
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_COMMAND_INVALID, Integer(LClose.ReplyCode),
      'COMMAND_INVALID');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.TuneOkAcimaDoProposto_530;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
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
    LTune.FrameMax := LTune.FrameMax * 2; // pede mais do que o broker propôs
    SendMethod(LStrm, AMQP_CHANNEL_CONNECTION, BuildTuneOk(LTune));
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_NOT_ALLOWED, Integer(LClose.ReplyCode), 'NOT_ALLOWED');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.FrameMaxAbaixoDoMinimo_530;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
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
    LTune.FrameMax := 1024; // abaixo dos 4096 da spec
    SendMethod(LStrm, AMQP_CHANNEL_CONNECTION, BuildTuneOk(LTune));
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_NOT_ALLOWED, Integer(LClose.ReplyCode), 'NOT_ALLOWED');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.SegundoChannelOpen_504;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    Assert.AreEqual(1, Integer(LCh), 'Open-Ok no canal 1');

    SendMethod(LStrm, 1, BuildChannelOpen); // de novo no mesmo canal
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode), 'CHANNEL_ERROR');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.CanalNaoAberto_504;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
  LDeclare: TAMQPQueueDeclare;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    LDeclare := TAMQPQueueDeclare.Create('q.sem.canal');
    SendMethod(LStrm, 7, BuildQueueDeclare(LDeclare)); // canal 7 nunca aberto
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode), 'CHANNEL_ERROR');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.TipoDeFrameDesconhecido_501;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LF: TAMQPFrame;
  LPayload: TBytes;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SetLength(LPayload, 1);
    LPayload[0] := 0;
    LF := TAMQPFrame.Create(42, 0, LPayload); // tipo inexistente
    LF.WriteTo(LStrm);
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_FRAME_ERROR, Integer(LClose.ReplyCode), 'FRAME_ERROR');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.ConteudoSemPublish_505;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
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
    LF := TAMQPFrame.Create(AMQP_FRAME_HEADER, 1, LPayload); // sem Basic.Publish
    LF.WriteTo(LStrm);
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_UNEXPECTED_FRAME, Integer(LClose.ReplyCode),
      'UNEXPECTED_FRAME');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.ClienteFechaConexao_RecebeCloseOk;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
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
    Assert.AreEqual(0, Integer(LCh), 'Close-Ok no canal 0');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

{ THeartbeatTests }

procedure THeartbeatTests.Setup;
begin
  FBroker := NewBroker;
end;

procedure THeartbeatTests.TearDown;
begin
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
end;

procedure THeartbeatTests.BrokerEmiteHeartbeat;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LF: TAMQPFrame;
  LDeadline: UInt64;
  LGot: Boolean;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshakeHb(LStrm, 1); // 1 s => broker emite a cada ~500 ms
    LGot := False;
    // Fica só lendo: com 1 s negociado, o primeiro heartbeat tem de chegar bem
    // antes dos 2 s em que o broker nos consideraria mortos.
    LDeadline := AmqpTickMs + 1500;
    while (not LGot) and (AmqpTickMs < LDeadline) do
    begin
      LF := TAMQPFrame.ReadFrom(LStrm, MAX_PAYLOAD);
      if LF.IsHeartbeat then
        LGot := True;
    end;
    Assert.IsTrue(LGot, 'broker emitiu heartbeat sozinho');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.PeerMudo_EhDerrubado;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LDeadline: UInt64;
  LDropped: Boolean;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshakeHb(LStrm, 1);
    // Não mandamos mais nada. O broker tem de nos derrubar depois de 2x1 s.
    // Seguimos lendo (só chegam heartbeats dele) até o stream acabar.
    LDropped := False;
    LDeadline := AmqpTickMs + 6000;
    while (not LDropped) and (AmqpTickMs < LDeadline) do
      try
        TAMQPFrame.ReadFrom(LStrm, MAX_PAYLOAD);
      except
        on EAMQPFrame do
          LDropped := True;
      end;
    Assert.IsTrue(LDropped, 'peer mudo derrubado pelo heartbeat');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.HeartbeatZero_NaoDerruba;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshakeHb(LStrm, 0);  // heartbeat desabilitado pelo cliente
    TThread.Sleep(2500);       // mais que 2x o intervalo default, se houvesse
    // Continua viva: um Channel.Open ainda é respondido.
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    Assert.AreEqual(1, Integer(LCh), 'canal aberto apos silencio');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.SemCloseOk_DerrubadoNoPrazo;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
  LCh: Word;
  LDeadline: UInt64;
  LDropped: Boolean;
begin
  FBroker.CloseTimeoutMs := 700; // vale para as conexões aceitas daqui em diante
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    // Heartbeat 0 isola o teste: só o prazo do Close-Ok pode nos derrubar.
    RawHandshakeHb(LStrm, 0);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    SendMethod(LStrm, 1, BuildChannelOpen); // segundo Open => 504
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode), 'CHANNEL_ERROR');

    // Nunca respondemos o Close-Ok: o broker tem de derrubar no prazo.
    LDropped := False;
    LDeadline := AmqpTickMs + 5000;
    while (not LDropped) and (AmqpTickMs < LDeadline) do
      try
        TAMQPFrame.ReadFrom(LStrm, MAX_PAYLOAD);
      except
        on EAMQPFrame do
          LDropped := True;
      end;
    Assert.IsTrue(LDropped, 'derrubado por nao responder o Close-Ok');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure THeartbeatTests.ClienteRealSobreviveComHeartbeat;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
  LCh: TAMQPChannel;
begin
  // Broker propõe 1 s; o cliente negocia min(1,60) = 1. Os dois lados passam a
  // emitir heartbeat a cada ~500 ms — se algum dos dois estiver errado, um
  // derruba o outro dentro de 2 s.
  FBroker.Heartbeat := 1;
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    LConn := ServerConn(FBroker);
    Assert.IsNotNull(LConn);
    Assert.AreEqual(1, Integer(LConn.Negotiated.Heartbeat), 'heartbeat negociado');

    TThread.Sleep(3000); // 3x o intervalo, ocioso dos dois lados

    Assert.IsTrue(LCli.IsOpen, 'cliente segue aberto');
    Assert.AreEqual(1, FBroker.ConnectionCount, 'broker nao derrubou');
    // E ainda funciona de verdade.
    LCh := LCli.CreateChannel;
    try
      Assert.IsTrue(WaitChannels(LConn, 1), 'canal abre apos o silencio');
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

{ TRawHandshakeTests -- classe fora da Fase 1 }

procedure TRawHandshakeTests.ClasseNaoImplementada_FechaSoOCanal;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPCloseInfo;
  LCh: Word;
  W: TAMQPWriter;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);

    // Tx (classe 90) e' deliberadamente fora do escopo da Fase 1.
    W := BeginMethod(AMQP_CLASS_TX, 10); // Tx.Select
    try
      SendMethod(LStrm, 1, W.ToBytes);
    finally
      W.Free;
    end;

    LClose := ExpectChannelClose(LStrm, LCh);
    Assert.AreEqual(1, Integer(LCh), 'erro no canal 1');
    Assert.AreEqual(AMQP_NOT_IMPLEMENTED, Integer(LClose.ReplyCode),
      'NOT_IMPLEMENTED');

    // A conexao sobrevive: respondemos o Close-Ok e reabrimos o canal.
    SendMethod(LStrm, 1, BuildChannelCloseOk);
    OpenChannel(LStrm, 1);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

{ TContentTests }

procedure TContentTests.Setup;
begin
  FBroker := NewBroker;
end;

procedure TContentTests.TearDown;
begin
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
end;

procedure TContentTests.OnDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  // nada: a Fase 1 nao entrega mensagem nenhuma
end;

procedure TContentTests.ClientePublicaMensagemSimples;
var
  LSink: TRecordingSink;
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink; // o broker segura a referencia
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.PublishText('', 'fila.teste', 'ola do WS5');
      Assert.IsTrue(LSink.WaitCount(1, 3000), 'mensagem chegou ao sink');
      Assert.AreEqual('fila.teste', LSink.RoutingKey, 'routing key');
      Assert.AreEqual('', LSink.Exchange, 'exchange default');
      Assert.AreEqual('ola do WS5', AmqpUtf8Decode(LSink.Body), 'corpo remontado');
      Assert.IsTrue(LCli.IsOpen, 'conexao segue aberta');
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TContentTests.CorpoGrandeEmVariosFrames;
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
  // 300 KB com frame-max de 128 KB => o cliente parte em varios body frames.
  SetLength(LBody, 300 * 1024);
  for I := 0 to High(LBody) do
    LBody[I] := Byte(I and $FF);

  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.Publish('', 'grande', LBody, TAMQPBasicProperties.Empty);
      Assert.IsTrue(LSink.WaitCount(1, 5000), 'mensagem chegou');
      LGot := LSink.Body;
      Assert.AreEqual(Length(LBody), Length(LGot), 'tamanho remontado');
      for I := 0 to High(LBody) do
        if LGot[I] <> LBody[I] then
          Assert.Fail(Format('corpo diverge no octeto %d', [I]));
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TContentTests.PropriedadesEHeadersChegamIntactos;
var
  LSink: TRecordingSink;
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
  LProps: TAMQPBasicProperties;
  LHeaders: TAMQPFieldTable;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      // O chamador e' dono da tabela: SetHeaders nao copia e o Publish nao
      // libera (mesma convencao do sample EventosHeadersVcl).
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

      Assert.IsTrue(LSink.WaitCount(1, 3000), 'mensagem chegou');
      Assert.AreEqual('application/json', LSink.ContentType, 'content-type');
      Assert.AreEqual('req-42', LSink.CorrelationId, 'correlation-id');
      Assert.AreEqual('bar', LSink.HeaderFoo, 'header foo');
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TContentTests.MensagemSemCorpo_Aceita;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LSink: TRecordingSink;
  LCh: Word;
begin
  LSink := TRecordingSink.Create;
  FBroker.MessageSink := LSink;
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'vazia'));
    // body-size 0: a mensagem fecha no proprio content-header.
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(0));
    Assert.IsTrue(LSink.WaitCount(1, 3000), 'mensagem sem corpo aceita');
    Assert.AreEqual(0, Length(LSink.Body), 'corpo vazio');

    // Canal voltou ao ocioso: um metodo normal e' respondido.
    SendMethod(LStrm, 1, BuildQueueDeclare(TAMQPQueueDeclare.Create('q1')));
    ExpectMethodId(LStrm, AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK, LCh);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.DoisCanaisIntercalados;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
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

    // Canal 1 comeca a publicar e PARA no meio...
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r1'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(5));

    // ...e o canal 2 publica inteiro no meio disso. Intercalar entre CANAIS e'
    // legitimo -- so dentro do mesmo canal e' que e' proibido.
    SendMethod(LStrm, 2, BuildBasicPublish('', 'r2'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 2, BuildRawContentHeader(4));
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 2, BytesOf('dois'));

    Assert.IsTrue(LSink.WaitCount(1, 3000), 'canal 2 completou');

    // Agora o canal 1 termina o dele.
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, BytesOf('umumu'));
    Assert.IsTrue(LSink.WaitCount(2, 3000), 'canal 1 completou');

    // Conexao intacta.
    SendMethod(LStrm, 1, BuildQueueDeclare(TAMQPQueueDeclare.Create('q1')));
    ExpectMethodId(LStrm, AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK, LCh);
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.MetodoNoMeioDoConteudo_505;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(10));
    // Em vez do body, um metodo NO MESMO canal: viola a regra de interleave.
    SendMethod(LStrm, 1, BuildQueueDeclare(TAMQPQueueDeclare.Create('q1')));
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_UNEXPECTED_FRAME, Integer(LClose.ReplyCode),
      'UNEXPECTED_FRAME');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.BodySemHeader_505;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    // Body direto, sem o content-header no meio.
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, BytesOf('xxx'));
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_UNEXPECTED_FRAME, Integer(LClose.ReplyCode),
      'UNEXPECTED_FRAME');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.CorpoMaiorQueOBodySize_505;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1, BuildRawContentHeader(5));
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, BytesOf('dez octetos'));
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_UNEXPECTED_FRAME, Integer(LClose.ReplyCode),
      'UNEXPECTED_FRAME');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.ContentHeaderDeOutraClasse_505;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPConnectionClose;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    OpenChannel(LStrm, 1);
    SendMethod(LStrm, 1, BuildBasicPublish('', 'r'));
    // class-id do content-header tem de casar com a classe do metodo (60).
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1,
      BuildRawContentHeader(3, AMQP_CLASS_QUEUE));
    LClose := ExpectConnectionClose(LStrm);
    Assert.AreEqual(AMQP_UNEXPECTED_FRAME, Integer(LClose.ReplyCode),
      'UNEXPECTED_FRAME');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TContentTests.ClienteDeclaraTopologia;
var
  LCli: TAMQPConnection;
  LCh: TAMQPChannel;
  LDeclareOk: TAMQPQueueDeclareOk;
  LBind: TAMQPQueueBind;
  LGet: TAMQPGetResult;
  LDelete: TAMQPQueueDelete;
  LTag: string;
begin
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      // Fila anonima: o nome vem do servidor.
      LDeclareOk := LCh.DeclareQueue(TAMQPQueueDeclare.Create(''));
      Assert.IsTrue(Pos('amq.gen-', LDeclareOk.QueueName) = 1,
        'nome gerado pelo servidor (veio: ' + LDeclareOk.QueueName + ')');

      LDeclareOk := LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.ws5'));
      Assert.AreEqual('q.ws5', LDeclareOk.QueueName, 'nome ecoado');

      LCh.DeclareExchange(TAMQPExchangeDeclare.Create('x.ws5'));

      LBind.QueueName := 'q.ws5';
      LBind.ExchangeName := 'x.ws5';
      LBind.RoutingKey := 'k';
      LBind.NoWait := False;
      LBind.Arguments := nil;
      LCh.BindQueue(LBind);

      LCh.Qos(10);
      LCh.ConfirmSelect;

      // Broker nulo: Basic.Get sempre devolve Get-Empty.
      LGet := LCh.BasicGet('q.ws5');
      Assert.IsFalse(LGet.Found, 'fila vazia no broker nulo');

      LTag := LCh.Consume('q.ws5', OnDelivery, True);
      Assert.IsTrue(LTag <> '', 'consumer-tag devolvido');
      LCh.Cancel(LTag);

      LDelete.QueueName := 'q.ws5';
      LDelete.IfUnused := False;
      LDelete.IfEmpty := False;
      LDelete.NoWait := False;
      Assert.AreEqual(0, Integer(LCh.DeleteQueue(LDelete)),
        'nenhuma mensagem apagada');
      Assert.IsTrue(LCli.IsOpen, 'conexao intacta');
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

// WS1 da Fase 2 (CLAUDE.md, decisao D1): o payload cru do content-header
// chega ate' o sink dentro da TAMQPServerMessage, e decodificar ESSE payload
// (em vez da TAMQPFieldTable original) devolve as mesmas propriedades que o
// cliente publicou -- prova que a reemissao fiel funciona sem redecodificar
// nada no meio do caminho.
procedure TContentTests.HeaderPayloadCru_DecodificaIgual;
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
  LCli := TAMQPConnection.Create(BrokerParams(FBroker));
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
        LCh.Publish('', 'crurun', BytesOf('{}'), LProps);
      finally
        LHeaders.Free;
      end;

      Assert.IsTrue(LSink.WaitCount(1, 3000), 'mensagem chegou');
      Assert.IsTrue(Length(LSink.HeaderPayload) > 0,
        'header payload cru nao vazio');

      LReader := TAMQPReader.Create(LSink.HeaderPayload);
      try
        LDecoded := DecodeContentHeader(LReader);
        try
          Assert.AreEqual('application/json', LDecoded.Properties.ContentType,
            'content-type decodificado do payload cru');
          Assert.IsTrue(
            LDecoded.Properties.Headers.TryGetValue('foo', LFooVal),
            'header foo presente no payload cru');
          Assert.AreEqual('bar', LFooVal.AsString, 'valor do header foo');
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

{$IFDEF AMQP_OPENSSL}
{ TServerTlsTests }

procedure TServerTlsTests.Setup;
begin
  FBroker := NewTlsBroker;
end;

procedure TServerTlsTests.TearDown;
begin
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
end;

function TServerTlsTests.TlsParams: TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := '127.0.0.1';
  Result.Port := FBroker.Port;
  Result.UseTls := True;
  Result.TlsVerifyPeer := False; // cert self-signed de dev
end;

procedure TServerTlsTests.ClienteRealConectaSobreTls;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
  LCh: TAMQPChannel;
begin
  LCli := TAMQPConnection.Create(TlsParams);
  try
    LCli.Open;
    Assert.IsTrue(LCli.IsOpen, 'cliente aberto sobre TLS');
    LConn := ServerConn(FBroker);
    Assert.IsNotNull(LConn, 'conexao registrada');
    Assert.IsTrue(WaitState(LConn, amqssOpen), 'FSM chegou a amqssOpen');
    Assert.AreEqual('guest', LConn.UserId, 'usuario autenticado');
    // E' uma conexao AMQP de verdade, nao so um socket cifrado.
    LCh := LCli.CreateChannel;
    try
      Assert.IsTrue(WaitChannels(LConn, 1), 'canal abre sobre TLS');
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.PublicaSobreTls;
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
  // 300 KB: forca varios body frames E varios registros TLS.
  SetLength(LBody, 300 * 1024);
  for I := 0 to High(LBody) do
    LBody[I] := Byte((I * 7) and $FF);

  LCli := TAMQPConnection.Create(TlsParams);
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.Publish('', 'sobre.tls', LBody, TAMQPBasicProperties.Empty);
      Assert.IsTrue(LSink.WaitCount(1, 8000), 'mensagem chegou');
      LGot := LSink.Body;
      Assert.AreEqual(Length(LBody), Length(LGot), 'tamanho remontado');
      for I := 0 to High(LBody) do
        if LGot[I] <> LBody[I] then
          Assert.Fail(Format('corpo diverge no octeto %d', [I]));
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.ClientePlainNaPortaTls_Falha;
var
  LP: TAMQPConnectionParams;
  LCli: TAMQPConnection;
  LFalhou: Boolean;
begin
  LP := TlsParams;
  LP.UseTls := False; // cliente em claro contra uma porta que espera TLS
  LCli := TAMQPConnection.Create(LP);
  try
    LFalhou := False;
    try
      LCli.Open;
    except
      on Exception do
        LFalhou := True;
    end;
    Assert.IsTrue(LFalhou or (not LCli.IsOpen),
      'cliente plain nao pode abrir numa porta TLS');
  finally
    LCli.Free;
  end;
end;

procedure TServerTlsTests.CertInexistente_ConexaoFalha;
var
  LCli: TAMQPConnection;
  LFalhou: Boolean;
begin
  // Reconfigura o broker: a config e' lida a cada conexao aceita.
  FBroker.TlsCertFile := CertsDir + 'nao-existe.crt';
  LCli := TAMQPConnection.Create(TlsParams);
  try
    LFalhou := False;
    try
      LCli.Open;
    except
      on Exception do
        LFalhou := True;
    end;
    Assert.IsTrue(LFalhou or (not LCli.IsOpen),
      'sem certificado valido a conexao nao abre');
  finally
    LCli.Free;
  end;
end;
{$ENDIF}

procedure TRawHandshakeTests.NoLocalNoConsume_540;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPCloseInfo;
  LCh: Word;
  LCons: TAMQPBasicConsume;
begin
  // A lib cliente nao expoe no-local nem prefetch-size, entao estas duas
  // linhas da tabela de erros so' sao alcancaveis por socket cru.
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
    Assert.AreEqual(Integer(AMQP_NOT_IMPLEMENTED),
      Integer(LClose.ReplyCode),
      'no-local fecha o canal com NOT_IMPLEMENTED');
    Assert.AreEqual(1, Integer(LCh), 'no canal ofensor');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.PrefetchSizeNaoZero_540;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LClose: TAMQPCloseInfo;
  LCh: Word;
begin
  LStrm := RawConnect(FBroker.Port, LSock);
  try
    RawHandshake(LStrm);
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);

    // prefetch-size (limite em OCTETOS) nao e' implementado -- e o broker
    // recusa em vez de aceitar em silencio.
    SendMethod(LStrm, 1, BuildBasicQos(10, False, 4096));

    LClose := ExpectChannelClose(LStrm, LCh);
    Assert.AreEqual(Integer(AMQP_NOT_IMPLEMENTED),
      Integer(LClose.ReplyCode),
      'prefetch-size fecha o canal com NOT_IMPLEMENTED');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

procedure TRawHandshakeTests.MandatorySemRota_ReturnAntesDoAck_312;
var
  LSock: TAMQPTcpSocket;
  LStrm: TAMQPSocketStream;
  LCh: Word;
  LDecl: TAMQPExchangeDeclare;
  LCorpo: TBytes;
  LFrame: TAMQPFrame;
  LR: TAMQPReader;
  LId: TAMQPMethodId;
  LReturn: TAMQPBasicReturn;
  LAck: TAMQPBasicAck;
begin
  // A ORDEM entre Basic.Return e Basic.Ack e o ponto sutil da WS6, e nenhum
  // teste pelo cliente a observa de forma deterministica (os dois chegam em
  // callbacks de threads diferentes). No socket cru, a ordem no wire e a
  // propria assercao.
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

    // Publish mandatory num exchange SEM binding nenhum.
    LCorpo := AmqpUtf8Encode('sem rota');
    SendMethod(LStrm, 1, BuildBasicPublish('ex.ord', 'nada', True, False));
    SendFrameRaw(LStrm, AMQP_FRAME_HEADER, 1,
      BuildContentHeader(Length(LCorpo), TAMQPBasicProperties.Empty));
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, LCorpo);

    // 1o: o Basic.Return, com o conteudo logo atras.
    LFrame := TAMQPFrame.ReadFrom(LStrm);
    Assert.AreEqual(Integer(AMQP_FRAME_METHOD),
      Integer(LFrame.FrameType), '1o frame e metodo');
    LR := TAMQPReader.Create(LFrame.Payload);
    try
      LId.ClassId := LR.ReadShortUInt;
      LId.MethodId := LR.ReadShortUInt;
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN),
        'o 1o metodo e Basic.Return, nao o Ack');
      LReturn := DecodeBasicReturn(LR);
    finally
      LR.Free;
    end;
    Assert.AreEqual(Integer(AMQP_NO_ROUTE), Integer(LReturn.ReplyCode),
      'NO_ROUTE');
    Assert.AreEqual('ex.ord', LReturn.Exchange, 'exchange devolvido');
    Assert.AreEqual('nada', LReturn.RoutingKey, 'routing key devolvida');

    LFrame := TAMQPFrame.ReadFrom(LStrm);
    Assert.AreEqual(Integer(AMQP_FRAME_HEADER), Integer(LFrame.FrameType),
      '2o frame e o content-header');
    LFrame := TAMQPFrame.ReadFrom(LStrm);
    Assert.AreEqual(Integer(AMQP_FRAME_BODY), Integer(LFrame.FrameType),
      '3o frame e o body');
    Assert.AreEqual('sem rota', AmqpUtf8Decode(LFrame.Payload),
      'com o corpo original');

    // SO' ENTAO o Basic.Ack do confirm: mensagem sem rota tambem e confirmada.
    LFrame := TAMQPFrame.ReadFrom(LStrm);
    Assert.AreEqual(Integer(AMQP_FRAME_METHOD),
      Integer(LFrame.FrameType), '4o frame e metodo');
    LR := TAMQPReader.Create(LFrame.Payload);
    try
      LId.ClassId := LR.ReadShortUInt;
      LId.MethodId := LR.ReadShortUInt;
      Assert.IsTrue(LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK),
        'e o Basic.Ack, depois do Return');
      LAck := DecodeBasicAck(LR);
    finally
      LR.Free;
    end;
    Assert.AreEqual(1, Integer(LAck.DeliveryTag),
      'confirmando o seq-no 1');
    Assert.IsFalse(LAck.Multiple, 'sem multiple');
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TClientHandshakeTests);
  TDUnitX.RegisterTestFixture(TRawHandshakeTests);
  TDUnitX.RegisterTestFixture(THeartbeatTests);
  TDUnitX.RegisterTestFixture(TContentTests);
  {$IFDEF AMQP_OPENSSL}
  TDUnitX.RegisterTestFixture(TServerTlsTests);
  {$ENDIF}

end.
