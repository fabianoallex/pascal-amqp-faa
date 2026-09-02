unit AMQP.ConnectionMethodsTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Rtti,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Connection.Methods;

type
  [TestFixture]
  TConnectionMethodsTests = class
  public
    [Test] procedure StartOk_RoundTrip;
    [Test] procedure DecodeStart_LeArgumentos;
    [Test] procedure Start_SupportsMechanism;
    [Test] procedure Tune_DecodeEBuildTuneOk;
    [Test] procedure Open_RoundTrip;
    [Test] procedure OpenOk_Decode;
    [Test] procedure Close_RoundTrip;
    [Test] procedure Blocked_Decode;
    [Test] procedure PlainAuth_Formato;
    [Test] procedure DefaultClientProperties_TemProduct;
    [Test] procedure DefaultClientProperties_AnunciaConnectionBlocked;
  end;

  { Lado servidor (sub-módulo broker): build dos métodos servidor->cliente e
    decode dos métodos cliente->servidor. Vários testes alimentam o Decode do
    servidor com o output do Build que o CLIENTE já produz — garante interop
    com o próprio cliente da lib. }
  [TestFixture]
  TConnectionServerCodecTests = class
  public
    [Test] procedure Start_RoundTrip;
    [Test] procedure StartOk_DecodeDoBuildDoCliente;
    [Test] procedure StartOk_ResponseComNulPreservada;
    [Test] procedure TuneOk_DecodeDoBuildDoCliente;
    [Test] procedure Open_DecodeDoBuildDoCliente;
    [Test] procedure OpenOk_DecodeDoClienteConsome;
    [Test] procedure Blocked_ClienteDecodificaOBuildDoServidor;
    [Test] procedure Unblocked_SoCabecalho;
    [Test] procedure Secure_RoundTrip;
    [Test] procedure CloseOk_RoundTrip;
    [Test] procedure DefaultServerProperties_TemProductECapabilities;
  end;

  [TestFixture]
  TTuneNegotiationTests = class
  public
    // Gotcha do channel-max (ver CLAUDE.md)
    [Test] procedure ChannelMax_ClienteZero_AdotaServidor;
    [Test] procedure ChannelMax_NuncaExcedeServidor;
    [Test] procedure ChannelMax_ServidorZero_UsaCliente;
    [Test] procedure ChannelMax_AmbosZero_FicaZero;
    [Test] procedure ChannelMax_AmbosFinitos_MenorVence;
    [Test] procedure FrameMax_Negocia;
    [Test] procedure NegotiateTune_Combina;
  end;

implementation

{ TConnectionMethodsTests }

procedure TConnectionMethodsTests.StartOk_RoundTrip;
var
  LProps, LDecoded: TAMQPFieldTable;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LProps := DefaultClientProperties;
  try
    LPayload := BuildStartOk(LProps, AMQP_AUTH_PLAIN,
      PlainAuthResponse('guest', 'guest'), AMQP_LOCALE_DEFAULT);

    R := TAMQPReader.Create(LPayload);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK),
        'class/method de Start-Ok');

      LDecoded := R.ReadFieldTable;
      try
        Assert.AreEqual('pascal-amqp-faa', LDecoded['product'].AsString);
      finally
        LDecoded.Free;
      end;

      Assert.AreEqual(AMQP_AUTH_PLAIN, R.ReadShortStr, 'mechanism');
      Assert.AreEqual(PlainAuthResponse('guest', 'guest'), R.ReadLongStr, 'response');
      Assert.AreEqual(AMQP_LOCALE_DEFAULT, R.ReadShortStr, 'locale');
      Assert.IsTrue(R.EndOfData);
    finally
      R.Free;
    end;
  finally
    LProps.Free;
  end;
end;

procedure TConnectionMethodsTests.DecodeStart_LeArgumentos;
var
  W: TAMQPWriter;
  LServerProps: TAMQPFieldTable;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LStart: TAMQPConnectionStart;
begin
  // Monta um Start como o servidor mandaria.
  LServerProps := TAMQPFieldTable.Create;
  LServerProps.Put('product', 'RabbitMQ');
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START);
    W.WriteOctet(0);   // version-major
    W.WriteOctet(9);   // version-minor
    W.WriteFieldTable(LServerProps);
    W.WriteLongStr('PLAIN AMQPLAIN');
    W.WriteLongStr('en_US');

    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START));
      LStart := DecodeStart(R);
      try
        Assert.AreEqual(0, Integer(LStart.VersionMajor));
        Assert.AreEqual(9, Integer(LStart.VersionMinor));
        Assert.AreEqual('PLAIN AMQPLAIN', LStart.Mechanisms);
        Assert.AreEqual('en_US', LStart.Locales);
        Assert.AreEqual('RabbitMQ', LStart.ServerProperties['product'].AsString);
      finally
        LStart.ServerProperties.Free;
      end;
    finally
      R.Free;
    end;
  finally
    W.Free;
    LServerProps.Free;
  end;
end;

procedure TConnectionMethodsTests.Start_SupportsMechanism;
var
  LStart: TAMQPConnectionStart;
begin
  LStart.Mechanisms := 'PLAIN AMQPLAIN';
  LStart.ServerProperties := nil;
  Assert.IsTrue(LStart.SupportsMechanism('PLAIN'));
  Assert.IsTrue(LStart.SupportsMechanism('plain'), 'case-insensitive');
  Assert.IsTrue(LStart.SupportsMechanism('AMQPLAIN'));
  Assert.IsFalse(LStart.SupportsMechanism('EXTERNAL'));
end;

procedure TConnectionMethodsTests.Tune_DecodeEBuildTuneOk;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LTune: TAMQPConnectionTune;
  LPayload: TBytes;
begin
  // Tune do servidor: channel-max=2047, frame-max=131072, heartbeat=60
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE);
    W.WriteShortUInt(2047);
    W.WriteLongUInt(131072);
    W.WriteShortUInt(60);

    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE));
      LTune := DecodeTune(R);
      Assert.AreEqual(Word(2047), LTune.ChannelMax);
      Assert.AreEqual(Cardinal(131072), LTune.FrameMax);
      Assert.AreEqual(Word(60), LTune.Heartbeat);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;

  // Tune-Ok de volta com os mesmos valores.
  LTune.ChannelMax := 2047;
  LTune.FrameMax := 131072;
  LTune.Heartbeat := 60;
  LPayload := BuildTuneOk(LTune);
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK));
    Assert.AreEqual(Word(2047), R.ReadShortUInt);
    Assert.AreEqual(Cardinal(131072), R.ReadLongUInt);
    Assert.AreEqual(Word(60), R.ReadShortUInt);
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionMethodsTests.Open_RoundTrip;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LPayload := BuildOpen('/');
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN));
    Assert.AreEqual('/', R.ReadShortStr, 'virtual-host');
    Assert.AreEqual('', R.ReadShortStr, 'reserved-1');
    Assert.IsFalse(R.ReadBit, 'reserved-2 (insist)');
  finally
    R.Free;
  end;
end;

procedure TConnectionMethodsTests.OpenOk_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK);
    W.WriteShortStr(''); // known-hosts reservado
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK));
      DecodeOpenOk(R);
      Assert.IsTrue(R.EndOfData);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TConnectionMethodsTests.Close_RoundTrip;
var
  LClose, LDecoded: TAMQPConnectionClose;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LClose.ReplyCode := 320;
  LClose.ReplyText := 'CONNECTION_FORCED';
  LClose.ClassId := 0;
  LClose.MethodId := 0;

  LPayload := BuildClose(LClose);
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE));
    LDecoded := DecodeClose(R);
    Assert.AreEqual(Word(320), LDecoded.ReplyCode);
    Assert.AreEqual('CONNECTION_FORCED', LDecoded.ReplyText);
    Assert.AreEqual(Word(0), LDecoded.ClassId);
    Assert.AreEqual(Word(0), LDecoded.MethodId);
  finally
    R.Free;
  end;
end;

procedure TConnectionMethodsTests.Blocked_Decode;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  // Connection.Blocked como o broker mandaria: só um shortstr com o motivo.
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED);
    W.WriteShortStr('low on memory');
    R := TAMQPReader.Create(W.ToBytes);
    try
      LId := ReadMethodHeader(R);
      Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED));
      Assert.AreEqual('low on memory', DecodeBlocked(R), 'reason');
      Assert.IsTrue(R.EndOfData);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TConnectionMethodsTests.PlainAuth_Formato;
var
  S: string;
begin
  // #0 guest #0 guest -> 1 + 5 + 1 + 5 = 12 caracteres
  S := PlainAuthResponse('guest', 'guest');
  Assert.AreEqual(12, Length(S));
  Assert.IsTrue(S[1] = #0, 'separador inicial');
  Assert.IsTrue(S[7] = #0, 'separador entre usuario e senha');
end;

procedure TConnectionMethodsTests.DefaultClientProperties_TemProduct;
var
  P: TAMQPFieldTable;
begin
  P := DefaultClientProperties;
  try
    Assert.AreEqual('pascal-amqp-faa', P['product'].AsString);
    Assert.IsTrue(P.ContainsKey('capabilities'));
  finally
    P.Free;
  end;
end;

procedure TConnectionMethodsTests.DefaultClientProperties_AnunciaConnectionBlocked;
var
  P, LCaps: TAMQPFieldTable;
begin
  // O broker só envia Connection.Blocked/Unblocked se o cliente anunciar esta
  // capability em client-properties -> tem de estar presente e True.
  P := DefaultClientProperties;
  try
    LCaps := P['capabilities'].AsType<TAMQPFieldTable>;
    Assert.IsTrue(LCaps.ContainsKey('connection.blocked'), 'capability presente');
    Assert.IsTrue(LCaps['connection.blocked'].AsBoolean, 'capability = True');
  finally
    P.Free;
  end;
end;

{ TConnectionServerCodecTests }

procedure TConnectionServerCodecTests.Start_RoundTrip;
var
  LProps: TAMQPFieldTable;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LStart: TAMQPConnectionStart;
begin
  LProps := DefaultServerProperties;
  try
    LPayload := BuildStart(0, 9, LProps, 'PLAIN', 'en_US');
  finally
    LProps.Free;
  end;

  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START),
      'class/method de Start');
    LStart := DecodeStart(R);
    try
      Assert.AreEqual(0, Integer(LStart.VersionMajor));
      Assert.AreEqual(9, Integer(LStart.VersionMinor));
      Assert.AreEqual('PLAIN', LStart.Mechanisms);
      Assert.AreEqual('en_US', LStart.Locales);
      Assert.AreEqual('pascal-amqp-faa (broker)',
        LStart.ServerProperties['product'].AsString);
      Assert.IsTrue(R.EndOfData);
    finally
      LStart.ServerProperties.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.StartOk_DecodeDoBuildDoCliente;
var
  LClientProps: TAMQPFieldTable;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LStartOk: TAMQPConnectionStartOk;
begin
  LClientProps := DefaultClientProperties;
  try
    LPayload := BuildStartOk(LClientProps, AMQP_AUTH_PLAIN,
      PlainAuthResponse('guest', 'guest'), AMQP_LOCALE_DEFAULT);
  finally
    LClientProps.Free;
  end;

  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK));
    LStartOk := DecodeStartOk(R);
    try
      Assert.AreEqual(AMQP_AUTH_PLAIN, LStartOk.Mechanism, 'mechanism');
      Assert.AreEqual(AMQP_LOCALE_DEFAULT, LStartOk.Locale, 'locale');
      Assert.AreEqual('pascal-amqp-faa', LStartOk.ClientProperties['product'].AsString);
      Assert.IsTrue(R.EndOfData);
    finally
      LStartOk.ClientProperties.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.StartOk_ResponseComNulPreservada;
var
  LClientProps: TAMQPFieldTable;
  LPayload, LEsperado: TBytes;
  R: TAMQPReader;
  LStartOk: TAMQPConnectionStartOk;
  I: Integer;
begin
  LEsperado := AmqpUtf8Encode(PlainAuthResponse('guest', 'secret'));
  LClientProps := DefaultClientProperties;
  try
    LPayload := BuildStartOk(LClientProps, AMQP_AUTH_PLAIN,
      PlainAuthResponse('guest', 'secret'), AMQP_LOCALE_DEFAULT);
  finally
    LClientProps.Free;
  end;

  R := TAMQPReader.Create(LPayload);
  try
    ReadMethodHeader(R);
    LStartOk := DecodeStartOk(R);
    try
      // A resposta SASL (#0 guest #0 secret) tem de sobreviver byte-a-byte.
      Assert.AreEqual(Length(LEsperado), Length(LStartOk.Response), 'tamanho');
      for I := 0 to High(LEsperado) do
        Assert.AreEqual(Integer(LEsperado[I]), Integer(LStartOk.Response[I]),
          'byte ' + IntToStr(I));
      Assert.AreEqual(0, Integer(LStartOk.Response[0]), 'separador inicial');
      Assert.AreEqual(0, Integer(LStartOk.Response[6]), 'separador do meio');
    finally
      LStartOk.ClientProperties.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.TuneOk_DecodeDoBuildDoCliente;
var
  LTune, LDecoded: TAMQPConnectionTune;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LTune.ChannelMax := 100;
  LTune.FrameMax := 65536;
  LTune.Heartbeat := 30;
  LPayload := BuildTuneOk(LTune); // build do CLIENTE

  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK));
    LDecoded := DecodeTuneOk(R); // decode do SERVIDOR
    Assert.AreEqual(Word(100), LDecoded.ChannelMax);
    Assert.AreEqual(Cardinal(65536), LDecoded.FrameMax);
    Assert.AreEqual(Word(30), LDecoded.Heartbeat);
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;

  // E o Tune que o servidor manda decodifica com o DecodeTune do cliente.
  LPayload := BuildTune(LTune);
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE));
    LDecoded := DecodeTune(R);
    Assert.AreEqual(Word(100), LDecoded.ChannelMax);
    Assert.AreEqual(Word(30), LDecoded.Heartbeat);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.Open_DecodeDoBuildDoCliente;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LOpen: TAMQPConnectionOpen;
begin
  LPayload := BuildOpen('/app'); // build do CLIENTE
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN));
    LOpen := DecodeOpen(R);
    Assert.AreEqual('/app', LOpen.VirtualHost, 'virtual-host');
    Assert.IsTrue(R.EndOfData, 'reservados consumidos');
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.OpenOk_DecodeDoClienteConsome;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LPayload := BuildOpenOk; // build do SERVIDOR
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK));
    DecodeOpenOk(R); // decode do CLIENTE
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.Blocked_ClienteDecodificaOBuildDoServidor;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LPayload := BuildBlocked('low on memory'); // build do SERVIDOR
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED));
    Assert.AreEqual('low on memory', DecodeBlocked(R), 'reason'); // decode do CLIENTE
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.Unblocked_SoCabecalho;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LPayload := BuildUnblocked;
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_UNBLOCKED));
    Assert.IsTrue(R.EndOfData, 'sem argumentos');
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.Secure_RoundTrip;
var
  LPayload, LResp, LEsperado: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  W: TAMQPWriter;
  I: Integer;
begin
  LPayload := BuildSecure('rabbit-challenge');
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_SECURE));
    Assert.AreEqual('rabbit-challenge', R.ReadLongStr, 'challenge');
  finally
    R.Free;
  end;

  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_SECURE_OK);
    W.WriteLongStr(#0'u'#0'p');
    LPayload := W.ToBytes;
  finally
    W.Free;
  end;
  LEsperado := AmqpUtf8Encode(#0'u'#0'p');
  R := TAMQPReader.Create(LPayload);
  try
    ReadMethodHeader(R);
    LResp := DecodeSecureOk(R);
    Assert.AreEqual(Length(LEsperado), Length(LResp));
    for I := 0 to High(LEsperado) do
      Assert.AreEqual(Integer(LEsperado[I]), Integer(LResp[I]));
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.CloseOk_RoundTrip;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LPayload := BuildCloseOk;
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    Assert.IsTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK));
    DecodeCloseOk(R);
    Assert.IsTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.DefaultServerProperties_TemProductECapabilities;
var
  P, LCaps: TAMQPFieldTable;
begin
  P := DefaultServerProperties;
  try
    Assert.AreEqual('pascal-amqp-faa (broker)', P['product'].AsString);
    Assert.IsTrue(P.ContainsKey('capabilities'));
    LCaps := P['capabilities'].AsType<TAMQPFieldTable>;
    Assert.IsTrue(LCaps.ContainsKey('publisher_confirms'), 'publisher_confirms');
    Assert.IsTrue(LCaps['publisher_confirms'].AsBoolean);
    Assert.IsTrue(LCaps.ContainsKey('basic.nack'), 'basic.nack');
  finally
    P.Free;
  end;
end;

{ TTuneNegotiationTests }

procedure TTuneNegotiationTests.ChannelMax_ClienteZero_AdotaServidor;
begin
  // Cliente quer "sem limite" (0), servidor propõe 2047 -> usa 2047, nunca 0.
  Assert.AreEqual(Word(2047), NegotiateChannelMax(2047, 0));
end;

procedure TTuneNegotiationTests.ChannelMax_NuncaExcedeServidor;
begin
  // Cliente pede mais do que o servidor permite -> limita ao servidor.
  Assert.AreEqual(Word(2047), NegotiateChannelMax(2047, 5000));
end;

procedure TTuneNegotiationTests.ChannelMax_ServidorZero_UsaCliente;
begin
  Assert.AreEqual(Word(100), NegotiateChannelMax(0, 100));
end;

procedure TTuneNegotiationTests.ChannelMax_AmbosZero_FicaZero;
begin
  Assert.AreEqual(Word(0), NegotiateChannelMax(0, 0));
end;

procedure TTuneNegotiationTests.ChannelMax_AmbosFinitos_MenorVence;
begin
  Assert.AreEqual(Word(100), NegotiateChannelMax(2047, 100));
end;

procedure TTuneNegotiationTests.FrameMax_Negocia;
begin
  Assert.AreEqual(Cardinal(131072), NegotiateFrameMax(131072, 0));
  Assert.AreEqual(Cardinal(65536), NegotiateFrameMax(131072, 65536));
  Assert.AreEqual(Cardinal(131072), NegotiateFrameMax(0, 131072));
end;

procedure TTuneNegotiationTests.NegotiateTune_Combina;
var
  LServer, LResult: TAMQPConnectionTune;
begin
  LServer.ChannelMax := 2047;
  LServer.FrameMax := 131072;
  LServer.Heartbeat := 60;
  // Cliente: channel-max 0 (sem limite), frame-max 0, heartbeat 30.
  LResult := NegotiateTune(LServer, 0, 0, 30);
  Assert.AreEqual(Word(2047), LResult.ChannelMax, 'channel-max adota servidor');
  Assert.AreEqual(Cardinal(131072), LResult.FrameMax, 'frame-max adota servidor');
  Assert.AreEqual(Word(30), LResult.Heartbeat, 'heartbeat menor vence');
end;

initialization
  TDUnitX.RegisterTestFixture(TConnectionMethodsTests);
  TDUnitX.RegisterTestFixture(TConnectionServerCodecTests);
  TDUnitX.RegisterTestFixture(TTuneNegotiationTests);

end.
