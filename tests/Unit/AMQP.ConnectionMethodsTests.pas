unit AMQP.ConnectionMethodsTests;


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Rtti,
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
  LProdutoVal: TValue;
begin
  LProps := DefaultClientProperties;
  try
    LPayload := BuildStartOk(LProps, AMQP_AUTH_PLAIN,
      PlainAuthResponse('guest', 'guest'), AMQP_LOCALE_DEFAULT);

    R := TAMQPReader.Create(LPayload);
    try
      LId := ReadMethodHeader(R);
      TAssert.AssertTrue('class/method de Start-Ok',
        LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK));

      LDecoded := R.ReadFieldTable;
      try
        LProdutoVal := LDecoded['product'];
        TAssert.AssertEquals('pascal-amqp-faa', LProdutoVal.AsString);
      finally
        LDecoded.Free;
      end;

      TAssert.AssertEquals('mechanism', AMQP_AUTH_PLAIN, R.ReadShortStr);
      TAssert.AssertEquals('response', PlainAuthResponse('guest', 'guest'), R.ReadLongStr);
      TAssert.AssertEquals('locale', AMQP_LOCALE_DEFAULT, R.ReadShortStr);
      TAssert.AssertTrue(R.EndOfData);
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
  LProdutoVal: TValue;
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START));
      LStart := DecodeStart(R);
      try
        TAssert.AssertEquals(0, Integer(LStart.VersionMajor));
        TAssert.AssertEquals(9, Integer(LStart.VersionMinor));
        TAssert.AssertEquals('PLAIN AMQPLAIN', LStart.Mechanisms);
        TAssert.AssertEquals('en_US', LStart.Locales);
        LProdutoVal := LStart.ServerProperties['product'];
        TAssert.AssertEquals('RabbitMQ', LProdutoVal.AsString);
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
  TAssert.AssertTrue(LStart.SupportsMechanism('PLAIN'));
  TAssert.AssertTrue('case-insensitive', LStart.SupportsMechanism('plain'));
  TAssert.AssertTrue(LStart.SupportsMechanism('AMQPLAIN'));
  TAssert.AssertFalse(LStart.SupportsMechanism('EXTERNAL'));
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE));
      LTune := DecodeTune(R);
      TAssert.AssertEquals(2047, Integer(LTune.ChannelMax));
      TAssert.AssertEquals(Int64(131072), Int64(LTune.FrameMax));
      TAssert.AssertEquals(60, Integer(LTune.Heartbeat));
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK));
    TAssert.AssertEquals(2047, Integer(R.ReadShortUInt));
    TAssert.AssertEquals(Int64(131072), Int64(R.ReadLongUInt));
    TAssert.AssertEquals(60, Integer(R.ReadShortUInt));
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN));
    TAssert.AssertEquals('virtual-host', '/', R.ReadShortStr);
    TAssert.AssertEquals('reserved-1', '', R.ReadShortStr);
    TAssert.AssertFalse('reserved-2 (insist)', R.ReadBit);
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK));
      DecodeOpenOk(R);
      TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE));
    LDecoded := DecodeClose(R);
    TAssert.AssertEquals(320, Integer(LDecoded.ReplyCode));
    TAssert.AssertEquals('CONNECTION_FORCED', LDecoded.ReplyText);
    TAssert.AssertEquals(0, Integer(LDecoded.ClassId));
    TAssert.AssertEquals(0, Integer(LDecoded.MethodId));
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
      TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED));
      TAssert.AssertEquals('reason', 'low on memory', DecodeBlocked(R));
      TAssert.AssertTrue(R.EndOfData);
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
  TAssert.AssertEquals(12, Length(S));
  TAssert.AssertTrue('separador inicial', S[1] = #0);
  TAssert.AssertTrue('separador entre usuario e senha', S[7] = #0);
end;

procedure TConnectionMethodsTests.DefaultClientProperties_TemProduct;
var
  P: TAMQPFieldTable;
  LProdutoVal: TValue;
begin
  P := DefaultClientProperties;
  try
    LProdutoVal := P['product'];
    TAssert.AssertEquals('pascal-amqp-faa', LProdutoVal.AsString);
    TAssert.AssertTrue(P.ContainsKey('capabilities'));
  finally
    P.Free;
  end;
end;

procedure TConnectionMethodsTests.DefaultClientProperties_AnunciaConnectionBlocked;
var
  P, LCaps: TAMQPFieldTable;
  LCapsVal, LBlockedVal: TValue;
begin
  // O broker só envia Connection.Blocked/Unblocked se o cliente anunciar esta
  // capability em client-properties -> tem de estar presente e True.
  P := DefaultClientProperties;
  try
    LCapsVal := P['capabilities'];
    // TValue.AsType<T> nao existe no FPC (ver CLAUDE.md) -> AsObject + cast.
    LCaps := TAMQPFieldTable(LCapsVal.AsObject);
    TAssert.AssertTrue('capability presente', LCaps.ContainsKey('connection.blocked'));
    LBlockedVal := LCaps['connection.blocked'];
    TAssert.AssertTrue('capability = True', LBlockedVal.AsBoolean);
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
  LProdutoVal: TValue;
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
    TAssert.AssertTrue('class/method de Start',
      LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START));
    // Decodifica com o mesmo DecodeStart que o cliente usa.
    LStart := DecodeStart(R);
    try
      TAssert.AssertEquals(0, Integer(LStart.VersionMajor));
      TAssert.AssertEquals(9, Integer(LStart.VersionMinor));
      TAssert.AssertEquals('PLAIN', LStart.Mechanisms);
      TAssert.AssertEquals('en_US', LStart.Locales);
      LProdutoVal := LStart.ServerProperties['product'];
      TAssert.AssertEquals('pascal-amqp-faa (broker)', LProdutoVal.AsString);
      TAssert.AssertTrue(R.EndOfData);
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
  LProdutoVal: TValue;
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK));
    LStartOk := DecodeStartOk(R);
    try
      TAssert.AssertEquals('mechanism', AMQP_AUTH_PLAIN, LStartOk.Mechanism);
      TAssert.AssertEquals('locale', AMQP_LOCALE_DEFAULT, LStartOk.Locale);
      LProdutoVal := LStartOk.ClientProperties['product'];
      TAssert.AssertEquals('pascal-amqp-faa', LProdutoVal.AsString);
      TAssert.AssertTrue(R.EndOfData);
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
      // A resposta SASL (#0 guest #0 secret) tem de sobreviver byte-a-byte:
      // nada de passar por AmqpUtf8Decode e perder os #0.
      TAssert.AssertEquals('tamanho da resposta', Length(LEsperado), Length(LStartOk.Response));
      for I := 0 to High(LEsperado) do
        TAssert.AssertEquals('byte ' + IntToStr(I),
          Integer(LEsperado[I]), Integer(LStartOk.Response[I]));
      TAssert.AssertEquals('separador inicial', 0, Integer(LStartOk.Response[0]));
      TAssert.AssertEquals('separador do meio', 0, Integer(LStartOk.Response[6]));
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK));
    LDecoded := DecodeTuneOk(R); // decode do SERVIDOR
    TAssert.AssertEquals(100, Integer(LDecoded.ChannelMax));
    TAssert.AssertEquals(Int64(65536), Int64(LDecoded.FrameMax));
    TAssert.AssertEquals(30, Integer(LDecoded.Heartbeat));
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  // E o Tune que o servidor manda decodifica com o DecodeTune do cliente.
  LPayload := BuildTune(LTune);
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE));
    LDecoded := DecodeTune(R);
    TAssert.AssertEquals(100, Integer(LDecoded.ChannelMax));
    TAssert.AssertEquals(30, Integer(LDecoded.Heartbeat));
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN));
    LOpen := DecodeOpen(R);
    TAssert.AssertEquals('virtual-host', '/app', LOpen.VirtualHost);
    TAssert.AssertTrue('reservados consumidos', R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK));
    DecodeOpenOk(R); // decode do CLIENTE
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED));
    TAssert.AssertEquals('reason', 'low on memory', DecodeBlocked(R)); // decode do CLIENTE
    TAssert.AssertTrue(R.EndOfData);
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_UNBLOCKED));
    TAssert.AssertTrue('sem argumentos', R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.Secure_RoundTrip;
var
  LPayload, LResp: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  W: TAMQPWriter;
  I: Integer;
  LEsperado: TBytes;
begin
  LPayload := BuildSecure('rabbit-challenge');
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_SECURE));
    TAssert.AssertEquals('challenge', 'rabbit-challenge', R.ReadLongStr);
  finally
    R.Free;
  end;

  // Secure-Ok: o servidor lê a resposta crua.
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
    TAssert.AssertEquals(Length(LEsperado), Length(LResp));
    for I := 0 to High(LEsperado) do
      TAssert.AssertEquals(Integer(LEsperado[I]), Integer(LResp[I]));
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
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK));
    DecodeCloseOk(R);
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.DefaultServerProperties_TemProductECapabilities;
var
  P, LCaps: TAMQPFieldTable;
  LProdutoVal, LCapsVal, LConfirmsVal: TValue;
begin
  P := DefaultServerProperties;
  try
    LProdutoVal := P['product'];
    TAssert.AssertEquals('pascal-amqp-faa (broker)', LProdutoVal.AsString);
    TAssert.AssertTrue(P.ContainsKey('capabilities'));
    LCapsVal := P['capabilities'];
    LCaps := TAMQPFieldTable(LCapsVal.AsObject);
    TAssert.AssertTrue('publisher_confirms', LCaps.ContainsKey('publisher_confirms'));
    LConfirmsVal := LCaps['publisher_confirms'];
    TAssert.AssertTrue(LConfirmsVal.AsBoolean);
    TAssert.AssertTrue('basic.nack', LCaps.ContainsKey('basic.nack'));
  finally
    P.Free;
  end;
end;

{ TTuneNegotiationTests }

procedure TTuneNegotiationTests.ChannelMax_ClienteZero_AdotaServidor;
begin
  // Cliente quer "sem limite" (0), servidor propõe 2047 -> usa 2047, nunca 0.
  TAssert.AssertEquals(2047, Integer(NegotiateChannelMax(2047, 0)));
end;

procedure TTuneNegotiationTests.ChannelMax_NuncaExcedeServidor;
begin
  // Cliente pede mais do que o servidor permite -> limita ao servidor.
  TAssert.AssertEquals(2047, Integer(NegotiateChannelMax(2047, 5000)));
end;

procedure TTuneNegotiationTests.ChannelMax_ServidorZero_UsaCliente;
begin
  TAssert.AssertEquals(100, Integer(NegotiateChannelMax(0, 100)));
end;

procedure TTuneNegotiationTests.ChannelMax_AmbosZero_FicaZero;
begin
  TAssert.AssertEquals(0, Integer(NegotiateChannelMax(0, 0)));
end;

procedure TTuneNegotiationTests.ChannelMax_AmbosFinitos_MenorVence;
begin
  TAssert.AssertEquals(100, Integer(NegotiateChannelMax(2047, 100)));
end;

procedure TTuneNegotiationTests.FrameMax_Negocia;
begin
  TAssert.AssertEquals(Int64(131072), Int64(NegotiateFrameMax(131072, 0)));
  TAssert.AssertEquals(Int64(65536), Int64(NegotiateFrameMax(131072, 65536)));
  TAssert.AssertEquals(Int64(131072), Int64(NegotiateFrameMax(0, 131072)));
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
  TAssert.AssertEquals('channel-max adota servidor', 2047, Integer(LResult.ChannelMax));
  TAssert.AssertEquals('frame-max adota servidor', Int64(131072), Int64(LResult.FrameMax));
  TAssert.AssertEquals('heartbeat menor vence', 30, Integer(LResult.Heartbeat));
end;

initialization
  TDUnitX.RegisterTestFixture(TConnectionMethodsTests);
  TDUnitX.RegisterTestFixture(TConnectionServerCodecTests);
  TDUnitX.RegisterTestFixture(TTuneNegotiationTests);

end.
