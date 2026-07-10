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
  TDUnitX.RegisterTestFixture(TTuneNegotiationTests);

end.
