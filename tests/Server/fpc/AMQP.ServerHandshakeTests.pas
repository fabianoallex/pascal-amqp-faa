unit AMQP.ServerHandshakeTests;

{ Testes da FSM de handshake e do ciclo de vida de canais do broker (WS4).

  Duas frentes:
  - TClientHandshakeTests: o CLIENTE REAL da lib (TAMQPConnection) conecta no
    broker in-process. É o teste que importa — se o handshake estiver errado, o
    cliente não abre.
  - TRawHandshakeTests: socket cru montando frames à mão, para exercitar os
    caminhos de erro que um cliente correto nunca produz (método fora de ordem,
    Tune-Ok acima do proposto, segundo Channel.Open, conteúdo sem publish...). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes, Rtti, SyncObjs,
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
  { Sobe um broker em 127.0.0.1 numa porta efêmera para cada teste. }
  TServerFixture = class(TTestCase)
  protected
    FBroker: TAMQPServer;
    procedure SetUp; override;
    procedure TearDown; override;
    /// Parâmetros do cliente apontando para este broker.
    function Params: TAMQPConnectionParams;
    /// Primeira conexão viva do lado servidor (espera até ATimeoutMs).
    function ServerConn(ATimeoutMs: Integer = 3000): TAMQPServerConnection;
    /// Espera a conexão do lado servidor chegar a AState.
    function WaitState(AConn: TAMQPServerConnection;
      AState: TAMQPServerConnState; ATimeoutMs: Integer = 3000): Boolean;
    function WaitChannels(AConn: TAMQPServerConnection; ACount: Integer;
      ATimeoutMs: Integer = 3000): Boolean;
  end;

  TClientHandshakeTests = class(TServerFixture)
  published
    procedure ClienteRealConecta;
    procedure SenhaErrada_403;
    procedure VhostInexistente_530;
    procedure VhostExtraAceito;
    procedure CanalAbreEFecha;
    procedure DuasConexoesSimultaneas;
    procedure ClienteFechaLimpo;
  end;

  TRawHandshakeTests = class(TServerFixture)
  published
    procedure StartAnunciaPlainEServerProperties;
    procedure MetodoForaDeOrdem_503;
    procedure ConnectionEmCanalNaoZero_503;
    procedure TuneOkAcimaDoProposto_530;
    procedure FrameMaxAbaixoDoMinimo_530;
    procedure SegundoChannelOpen_504;
    procedure CanalNaoAberto_504;
    procedure TipoDeFrameDesconhecido_501;
    procedure ConteudoSemPublish_505;
    procedure ClienteFechaConexao_RecebeCloseOk;
    procedure ClasseNaoImplementada_FechaSoOCanal;
  end;

  { Conteudo (WS5): remontagem de Basic.Publish + content-header + body frames,
    a regra de interleave da spec 2.3.5 e o despacho "nulo" das classes de
    recurso -- todo metodo cliente->servidor e' decodificado e respondido com o
    *-Ok correto, sem engine por tras. }
  TContentTests = class(TServerFixture)
  private
    /// Consumer nunca chamado (o broker nulo nao entrega); a lib exige um
    /// metodo de objeto, nao aceita closure.
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  published
    procedure ClientePublicaMensagemSimples;
    procedure CorpoGrandeEmVariosFrames;
    procedure PropriedadesEHeadersChegamIntactos;
    procedure MensagemSemCorpo_Aceita;
    procedure DoisCanaisIntercalados;
    procedure MetodoNoMeioDoConteudo_505;
    procedure BodySemHeader_505;
    procedure CorpoMaiorQueOBodySize_505;
    procedure ContentHeaderDeOutraClasse_505;
    procedure ClienteDeclaraTopologia;
  end;

  {$IFDEF AMQP_OPENSSL}
  { TLS do lado servidor (WS3). So existe em builds com -dAMQP_OPENSSL: o
    backend SChannel desta lib implementa apenas o lado cliente, entao no build
    Default o broker nao aceita TLS e nao ha o que testar. Usa os certs de dev
    de docker\certs (self-signed), por isso o cliente conecta com
    TlsVerifyPeer=False. }
  TServerTlsTests = class(TServerFixture)
  protected
    /// Troca o broker plain da fixture por um com TLS ligado.
    procedure SetUp; override;
    function TlsParams: TAMQPConnectionParams;
  published
    procedure ClienteRealConectaSobreTls;
    procedure PublicaSobreTls;
    procedure ClientePlainNaPortaTls_Falha;
    procedure CertInexistente_ConexaoFalha;
  end;
  {$ENDIF}

  { Heartbeat (spec 4.2.7) e o prazo do Connection.Close-Ok. Os testes negociam
    intervalos de 1 s no Tune-Ok para caber num tempo de suíte razoável. }
  THeartbeatTests = class(TServerFixture)
  published
    procedure BrokerEmiteHeartbeat;
    procedure PeerMudo_EhDerrubado;
    procedure HeartbeatZero_NaoDerruba;
    procedure SemCloseOk_DerrubadoNoPrazo;
    procedure ClienteRealSobreviveComHeartbeat;
  end;

implementation

// --- helpers de frame -----------------------------------------------------

const
  MAX_PAYLOAD = 131072;

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

// Manda o protocol-header e consome o Connection.Start. Libera as
// server-properties; devolve só os campos escalares.
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

// Lê o que vier esperando um Connection.Close; devolve o reply-code.
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

// Abre um socket cru contra o broker; devolve o stream (dono do socket via
// AOutSock, que o chamador libera DEPOIS do stream).
function RawConnect(APort: Word; out ASock: TAMQPTcpSocket): TAMQPSocketStream;
begin
  ASock := TAMQPTcpSocket.Create;
  ASock.Connect('127.0.0.1', APort);
  Result := TAMQPSocketStream.Create(ASock);
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

function TRecordingSink.WaitCount(ACount, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    if Count >= ACount then
      Exit(True);
    Sleep(10);
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

{ TServerFixture }

procedure TServerFixture.SetUp;
begin
  inherited SetUp;
  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '127.0.0.1';
  FBroker.Port := 0;
  FBroker.Start;
end;

procedure TServerFixture.TearDown;
begin
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
  inherited TearDown;
end;

function TServerFixture.Params: TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := '127.0.0.1';
  Result.Port := FBroker.Port;
end;

function TServerFixture.ServerConn(ATimeoutMs: Integer): TAMQPServerConnection;
var
  LDeadline: UInt64;
  LAll: TArray<TAMQPServerConnection>;
begin
  Result := nil;
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    LAll := FBroker.Connections;
    if Length(LAll) > 0 then
      Exit(LAll[0]);
    Sleep(10);
  until AmqpTickMs > LDeadline;
end;

function TServerFixture.WaitState(AConn: TAMQPServerConnection;
  AState: TAMQPServerConnState; ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    if AConn.State = AState then
      Exit(True);
    Sleep(10);
  until AmqpTickMs > LDeadline;
  Result := AConn.State = AState;
end;

function TServerFixture.WaitChannels(AConn: TAMQPServerConnection;
  ACount: Integer; ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  repeat
    if AConn.ChannelCount = ACount then
      Exit(True);
    Sleep(10);
  until AmqpTickMs > LDeadline;
  Result := AConn.ChannelCount = ACount;
end;

{ TClientHandshakeTests }

procedure TClientHandshakeTests.ClienteRealConecta;
var
  LCli: TAMQPConnection;
  LConn: TAMQPServerConnection;
begin
  LCli := TAMQPConnection.Create(Params);
  try
    LCli.Open;
    AssertTrue('cliente aberto', LCli.IsOpen);

    LConn := ServerConn;
    AssertNotNull('conexao registrada no broker', LConn);
    AssertTrue('FSM chegou a amqssOpen', WaitState(LConn, amqssOpen));
    AssertEquals('usuario autenticado', 'guest', LConn.UserId);
    AssertEquals('vhost', '/', LConn.VirtualHost);
    AssertEquals('channel-max negociado',
      AMQP_SERVER_DEFAULT_CHANNEL_MAX, Integer(LConn.Negotiated.ChannelMax));
    AssertEquals('frame-max negociado',
      Integer(AMQP_SERVER_DEFAULT_FRAME_MAX), Integer(LConn.Negotiated.FrameMax));
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
  LP := Params;
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
    AssertFalse('nao deve abrir', LCli.IsOpen);
    AssertTrue('mensagem cita 403 (veio: ' + LMsg + ')', Pos('403', LMsg) > 0);
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
  LP := Params;
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
    AssertFalse('nao deve abrir', LCli.IsOpen);
    AssertTrue('mensagem cita 530 (veio: ' + LMsg + ')', Pos('530', LMsg) > 0);
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
  LP := Params;
  LP.VirtualHost := '/app';
  LCli := TAMQPConnection.Create(LP);
  try
    LCli.Open;
    AssertTrue('cliente aberto no /app', LCli.IsOpen);
    AssertEquals('/app', ServerConn.VirtualHost);
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
  LCli := TAMQPConnection.Create(Params);
  try
    LCli.Open;
    LConn := ServerConn;
    AssertNotNull(LConn);

    LCh1 := LCli.CreateChannel;
    try
      AssertTrue('1 canal no servidor', WaitChannels(LConn, 1));
      LCh2 := LCli.CreateChannel;
      try
        AssertTrue('2 canais no servidor', WaitChannels(LConn, 2));
        LCh2.Close;
      finally
        LCh2.Free;
      end;
      AssertTrue('volta a 1 canal apos Channel.Close', WaitChannels(LConn, 1));
      LCh1.Close;
    finally
      LCh1.Free;
    end;
    AssertTrue('nenhum canal aberto', WaitChannels(LConn, 0));
    AssertTrue('conexao segue aberta', LCli.IsOpen);
  finally
    LCli.Free;
  end;
end;

procedure TClientHandshakeTests.DuasConexoesSimultaneas;
var
  LA, LB: TAMQPConnection;
begin
  LA := TAMQPConnection.Create(Params);
  try
    LB := TAMQPConnection.Create(Params);
    try
      LA.Open;
      LB.Open;
      AssertTrue(LA.IsOpen);
      AssertTrue(LB.IsOpen);
      AssertEquals('duas conexoes vivas', 2, FBroker.ConnectionCount);
      AssertEquals(2, FBroker.TotalAccepted);
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
  LCli := TAMQPConnection.Create(Params);
  try
    LCli.Open;
    AssertNotNull(ServerConn);
    LCli.Close; // Connection.Close + espera o Close-Ok do broker
    AssertFalse('cliente fechado', LCli.IsOpen);

    LDeadline := AmqpTickMs + 3000;
    while (FBroker.ConnectionCount > 0) and (AmqpTickMs < LDeadline) do
      Sleep(10);
    AssertEquals('broker reapou a conexao', 0, FBroker.ConnectionCount);
  finally
    LCli.Free;
  end;
end;

{ TRawHandshakeTests }

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
      AssertEquals('version-major', 0, LStart.VersionMajor);
      AssertEquals('version-minor', 9, LStart.VersionMinor);
      AssertTrue('oferece PLAIN', LStart.SupportsMechanism('PLAIN'));
      AssertTrue('server-properties tem product',
        LStart.ServerProperties.TryGetValue('product', LProd));
      AssertEquals(AMQP_SERVER_PRODUCT, LProd.AsString);
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
    AssertEquals('COMMAND_INVALID', AMQP_COMMAND_INVALID, Integer(LClose.ReplyCode));
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
    AssertEquals('COMMAND_INVALID', AMQP_COMMAND_INVALID, Integer(LClose.ReplyCode));
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
    AssertEquals('NOT_ALLOWED', AMQP_NOT_ALLOWED, Integer(LClose.ReplyCode));
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
    AssertEquals('NOT_ALLOWED', AMQP_NOT_ALLOWED, Integer(LClose.ReplyCode));
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
    AssertEquals('Open-Ok no canal 1', 1, Integer(LCh));

    SendMethod(LStrm, 1, BuildChannelOpen); // de novo no mesmo canal
    LClose := ExpectConnectionClose(LStrm);
    AssertEquals('CHANNEL_ERROR', AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode));
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
    AssertEquals('CHANNEL_ERROR', AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode));
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
    AssertEquals('FRAME_ERROR', AMQP_FRAME_ERROR, Integer(LClose.ReplyCode));
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
    AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
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
    AssertEquals('Close-Ok no canal 0', 0, Integer(LCh));
  finally
    LStrm.Free;
    LSock.Free;
  end;
end;

{ THeartbeatTests }

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
    AssertTrue('broker emitiu heartbeat sozinho', LGot);
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
    AssertTrue('peer mudo derrubado pelo heartbeat', LDropped);
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
    RawHandshakeHb(LStrm, 0); // heartbeat desabilitado pelo cliente
    Sleep(2500);              // mais que 2x o intervalo default, se houvesse
    // Continua viva: um Channel.Open ainda é respondido.
    SendMethod(LStrm, 1, BuildChannelOpen);
    ExpectMethodId(LStrm, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK, LCh);
    AssertEquals('canal aberto apos silencio', 1, Integer(LCh));
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
    AssertEquals('CHANNEL_ERROR', AMQP_CHANNEL_ERROR, Integer(LClose.ReplyCode));

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
    AssertTrue('derrubado por nao responder o Close-Ok', LDropped);
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
  LCli := TAMQPConnection.Create(Params);
  try
    LCli.Open;
    LConn := ServerConn;
    AssertNotNull(LConn);
    AssertEquals('heartbeat negociado', 1, Integer(LConn.Negotiated.Heartbeat));

    Sleep(3000); // 3x o intervalo, ocioso dos dois lados

    AssertTrue('cliente segue aberto', LCli.IsOpen);
    AssertEquals('broker nao derrubou', 1, FBroker.ConnectionCount);
    // E ainda funciona de verdade.
    LCh := LCli.CreateChannel;
    try
      AssertTrue('canal abre apos o silencio', WaitChannels(LConn, 1));
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
    AssertEquals('erro no canal 1', 1, Integer(LCh));
    AssertEquals('NOT_IMPLEMENTED', AMQP_NOT_IMPLEMENTED, Integer(LClose.ReplyCode));

    // A conexao sobrevive: respondemos o Close-Ok e reabrimos o canal.
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
  LCli := TAMQPConnection.Create(Params);
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.PublishText('', 'fila.teste', 'ola do WS5');
      AssertTrue('mensagem chegou ao sink', LSink.WaitCount(1, 3000));
      AssertEquals('routing key', 'fila.teste', LSink.RoutingKey);
      AssertEquals('exchange default', '', LSink.Exchange);
      AssertEquals('corpo remontado', 'ola do WS5', AmqpUtf8Decode(LSink.Body));
      AssertTrue('conexao segue aberta', LCli.IsOpen);
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

  LCli := TAMQPConnection.Create(Params);
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      LCh.Publish('', 'grande', LBody, TAMQPBasicProperties.Empty);
      AssertTrue('mensagem chegou', LSink.WaitCount(1, 5000));
      LGot := LSink.Body;
      AssertEquals('tamanho remontado', Length(LBody), Length(LGot));
      for I := 0 to High(LBody) do
        if LGot[I] <> LBody[I] then
          Fail(Format('corpo diverge no octeto %d', [I]));
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
  LCli := TAMQPConnection.Create(Params);
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

      AssertTrue('mensagem chegou', LSink.WaitCount(1, 3000));
      AssertEquals('content-type', 'application/json', LSink.ContentType);
      AssertEquals('correlation-id', 'req-42', LSink.CorrelationId);
      AssertEquals('header foo', 'bar', LSink.HeaderFoo);
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
    AssertTrue('mensagem sem corpo aceita', LSink.WaitCount(1, 3000));
    AssertEquals('corpo vazio', 0, Length(LSink.Body));

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

    AssertTrue('canal 2 completou', LSink.WaitCount(1, 3000));

    // Agora o canal 1 termina o dele.
    SendFrameRaw(LStrm, AMQP_FRAME_BODY, 1, BytesOf('umumu'));
    AssertTrue('canal 1 completou', LSink.WaitCount(2, 3000));

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
    AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
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
    AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
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
    AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
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
    AssertEquals('UNEXPECTED_FRAME', AMQP_UNEXPECTED_FRAME,
      Integer(LClose.ReplyCode));
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
  LCli := TAMQPConnection.Create(Params);
  try
    LCli.Open;
    LCh := LCli.CreateChannel;
    try
      // Fila anonima: o nome vem do servidor.
      LDeclareOk := LCh.DeclareQueue(TAMQPQueueDeclare.Create(''));
      AssertTrue('nome gerado pelo servidor (veio: ' + LDeclareOk.QueueName + ')',
        Pos('amq.gen-', LDeclareOk.QueueName) = 1);

      LDeclareOk := LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.ws5'));
      AssertEquals('nome ecoado', 'q.ws5', LDeclareOk.QueueName);

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
      AssertFalse('fila vazia no broker nulo', LGet.Found);

      LTag := LCh.Consume('q.ws5', OnDelivery, True);
      AssertTrue('consumer-tag devolvido', LTag <> '');
      LCh.Cancel(LTag);

      LDelete.QueueName := 'q.ws5';
      LDelete.IfUnused := False;
      LDelete.IfEmpty := False;
      LDelete.NoWait := False;
      AssertEquals('nenhuma mensagem apagada', 0, Integer(LCh.DeleteQueue(LDelete)));
      AssertTrue('conexao intacta', LCli.IsOpen);
    finally
      LCh.Free;
    end;
  finally
    LCli.Free;
  end;
end;

{$IFDEF AMQP_OPENSSL}
{ TServerTlsTests }

procedure TServerTlsTests.SetUp;
begin
  // De proposito NAO chama inherited: o SetUp da TServerFixture sobe um broker
  // plain, e aqui queremos um com TLS. O TearDown herdado para e libera este.
  FBroker := NewTlsBroker;
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
    AssertTrue('cliente aberto sobre TLS', LCli.IsOpen);
    LConn := ServerConn;
    AssertNotNull('conexao registrada', LConn);
    AssertTrue('FSM chegou a amqssOpen', WaitState(LConn, amqssOpen));
    AssertEquals('usuario autenticado', 'guest', LConn.UserId);
    // E' uma conexao AMQP de verdade, nao so um socket cifrado.
    LCh := LCli.CreateChannel;
    try
      AssertTrue('canal abre sobre TLS', WaitChannels(LConn, 1));
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
      AssertTrue('mensagem chegou', LSink.WaitCount(1, 8000));
      LGot := LSink.Body;
      AssertEquals('tamanho remontado', Length(LBody), Length(LGot));
      for I := 0 to High(LBody) do
        if LGot[I] <> LBody[I] then
          Fail(Format('corpo diverge no octeto %d', [I]));
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
    AssertTrue('cliente plain nao pode abrir numa porta TLS',
      LFalhou or (not LCli.IsOpen));
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
    AssertTrue('sem certificado valido a conexao nao abre',
      LFalhou or (not LCli.IsOpen));
  finally
    LCli.Free;
  end;
end;
{$ENDIF}

initialization
  RegisterTest(TClientHandshakeTests);
  RegisterTest(TRawHandshakeTests);
  RegisterTest(THeartbeatTests);
  RegisterTest(TContentTests);
  {$IFDEF AMQP_OPENSSL}
  RegisterTest(TServerTlsTests);
  {$ENDIF}

end.
