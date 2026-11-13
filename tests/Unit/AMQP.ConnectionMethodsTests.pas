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
    [Test] procedure DecodeStart_ReadsArguments;
    [Test] procedure Start_SupportsMechanism;
    [Test] procedure Tune_DecodeAndBuildTuneOk;
    [Test] procedure Open_RoundTrip;
    [Test] procedure OpenOk_Decode;
    [Test] procedure Close_RoundTrip;
    [Test] procedure Blocked_Decode;
    [Test] procedure PlainAuth_Format;
    [Test] procedure DefaultClientProperties_HasProduct;
    [Test] procedure DefaultClientProperties_AnnouncesConnectionBlocked;
  end;

  { Server side (broker sub-module): build of the server->client methods and
    decode of the client->server methods. Several tests feed the server's
    Decode with the output of the Build the CLIENT already produces — it ensures
    interop with the lib's own client. }
  [TestFixture]
  TConnectionServerCodecTests = class
  public
    [Test] procedure Start_RoundTrip;
    [Test] procedure StartOk_DecodeOfClientBuild;
    [Test] procedure StartOk_ResponseWithNulPreserved;
    [Test] procedure TuneOk_DecodeOfClientBuild;
    [Test] procedure Open_DecodeOfClientBuild;
    [Test] procedure OpenOk_DecodeConsumedByClient;
    [Test] procedure Blocked_ClientDecodesTheServerBuild;
    [Test] procedure Unblocked_HeaderOnly;
    [Test] procedure Secure_RoundTrip;
    [Test] procedure CloseOk_RoundTrip;
    [Test] procedure DefaultServerProperties_HasProductAndCapabilities;
  end;

  [TestFixture]
  TTuneNegotiationTests = class
  public
    // channel-max gotcha (see CLAUDE.md)
    [Test] procedure ChannelMax_ClientZero_AdoptsServer;
    [Test] procedure ChannelMax_NeverExceedsServer;
    [Test] procedure ChannelMax_ServerZero_UsesClient;
    [Test] procedure ChannelMax_BothZero_StaysZero;
    [Test] procedure ChannelMax_BothFinite_SmallerWins;
    [Test] procedure FrameMax_Negotiates;
    [Test] procedure NegotiateTune_Combines;
  end;

implementation

{ TConnectionMethodsTests }

procedure TConnectionMethodsTests.StartOk_RoundTrip;
var
  LProps, LDecoded: TAMQPFieldTable;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LProductVal: TValue;
begin
  LProps := DefaultClientProperties;
  try
    LPayload := BuildStartOk(LProps, AMQP_AUTH_PLAIN,
      PlainAuthResponse('guest', 'guest'), AMQP_LOCALE_DEFAULT);

    R := TAMQPReader.Create(LPayload);
    try
      LId := ReadMethodHeader(R);
      TAssert.AssertTrue('Start-Ok class/method',
        LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK));

      LDecoded := R.ReadFieldTable;
      try
        LProductVal := LDecoded['product'];
        TAssert.AssertEquals('pascal-amqp-faa', LProductVal.AsString);
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

procedure TConnectionMethodsTests.DecodeStart_ReadsArguments;
var
  W: TAMQPWriter;
  LServerProps: TAMQPFieldTable;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LStart: TAMQPConnectionStart;
  LProductVal: TValue;
begin
  // Builds a Start as the server would send it.
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
        LProductVal := LStart.ServerProperties['product'];
        TAssert.AssertEquals('RabbitMQ', LProductVal.AsString);
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

procedure TConnectionMethodsTests.Tune_DecodeAndBuildTuneOk;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LTune: TAMQPConnectionTune;
  LPayload: TBytes;
begin
  // Server Tune: channel-max=2047, frame-max=131072, heartbeat=60
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

  // Tune-Ok back with the same values.
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
    W.WriteShortStr(''); // known-hosts reserved
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
  // Connection.Blocked as the broker would send it: just a shortstr with the reason.
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

procedure TConnectionMethodsTests.PlainAuth_Format;
var
  S: string;
begin
  // #0 guest #0 guest -> 1 + 5 + 1 + 5 = 12 characters
  S := PlainAuthResponse('guest', 'guest');
  TAssert.AssertEquals(12, Length(S));
  TAssert.AssertTrue('leading separator', S[1] = #0);
  TAssert.AssertTrue('separator between user and password', S[7] = #0);
end;

procedure TConnectionMethodsTests.DefaultClientProperties_HasProduct;
var
  P: TAMQPFieldTable;
  LProductVal: TValue;
begin
  P := DefaultClientProperties;
  try
    LProductVal := P['product'];
    TAssert.AssertEquals('pascal-amqp-faa', LProductVal.AsString);
    TAssert.AssertTrue(P.ContainsKey('capabilities'));
  finally
    P.Free;
  end;
end;

procedure TConnectionMethodsTests.DefaultClientProperties_AnnouncesConnectionBlocked;
var
  P, LCaps: TAMQPFieldTable;
  LCapsVal, LBlockedVal: TValue;
begin
  // The broker only sends Connection.Blocked/Unblocked if the client announces this
  // capability in client-properties -> it must be present and True.
  P := DefaultClientProperties;
  try
    LCapsVal := P['capabilities'];
    // TValue.AsType<T> does not exist in FPC (see CLAUDE.md) -> AsObject + cast.
    LCaps := TAMQPFieldTable(LCapsVal.AsObject);
    TAssert.AssertTrue('capability present', LCaps.ContainsKey('connection.blocked'));
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
  LProductVal: TValue;
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
    TAssert.AssertTrue('Start class/method',
      LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START));
    // Decodes with the same DecodeStart the client uses.
    LStart := DecodeStart(R);
    try
      TAssert.AssertEquals(0, Integer(LStart.VersionMajor));
      TAssert.AssertEquals(9, Integer(LStart.VersionMinor));
      TAssert.AssertEquals('PLAIN', LStart.Mechanisms);
      TAssert.AssertEquals('en_US', LStart.Locales);
      LProductVal := LStart.ServerProperties['product'];
      TAssert.AssertEquals('pascal-amqp-faa (broker)', LProductVal.AsString);
      TAssert.AssertTrue(R.EndOfData);
    finally
      LStart.ServerProperties.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.StartOk_DecodeOfClientBuild;
var
  LClientProps: TAMQPFieldTable;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LStartOk: TAMQPConnectionStartOk;
  LProductVal: TValue;
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
      LProductVal := LStartOk.ClientProperties['product'];
      TAssert.AssertEquals('pascal-amqp-faa', LProductVal.AsString);
      TAssert.AssertTrue(R.EndOfData);
    finally
      LStartOk.ClientProperties.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.StartOk_ResponseWithNulPreserved;
var
  LClientProps: TAMQPFieldTable;
  LPayload, LExpected: TBytes;
  R: TAMQPReader;
  LStartOk: TAMQPConnectionStartOk;
  I: Integer;
begin
  LExpected := AmqpUtf8Encode(PlainAuthResponse('guest', 'secret'));
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
      // The SASL response (#0 guest #0 secret) must survive byte for byte:
      // no going through AmqpUtf8Decode and losing the #0s.
      TAssert.AssertEquals('response length', Length(LExpected), Length(LStartOk.Response));
      for I := 0 to High(LExpected) do
        TAssert.AssertEquals('byte ' + IntToStr(I),
          Integer(LExpected[I]), Integer(LStartOk.Response[I]));
      TAssert.AssertEquals('leading separator', 0, Integer(LStartOk.Response[0]));
      TAssert.AssertEquals('middle separator', 0, Integer(LStartOk.Response[6]));
    finally
      LStartOk.ClientProperties.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.TuneOk_DecodeOfClientBuild;
var
  LTune, LDecoded: TAMQPConnectionTune;
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LTune.ChannelMax := 100;
  LTune.FrameMax := 65536;
  LTune.Heartbeat := 30;
  LPayload := BuildTuneOk(LTune); // CLIENT build

  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK));
    LDecoded := DecodeTuneOk(R); // SERVER decode
    TAssert.AssertEquals(100, Integer(LDecoded.ChannelMax));
    TAssert.AssertEquals(Int64(65536), Int64(LDecoded.FrameMax));
    TAssert.AssertEquals(30, Integer(LDecoded.Heartbeat));
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;

  // And the Tune the server sends decodes with the client's DecodeTune.
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

procedure TConnectionServerCodecTests.Open_DecodeOfClientBuild;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
  LOpen: TAMQPConnectionOpen;
begin
  LPayload := BuildOpen('/app'); // CLIENT build
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN));
    LOpen := DecodeOpen(R);
    TAssert.AssertEquals('virtual-host', '/app', LOpen.VirtualHost);
    TAssert.AssertTrue('reserved fields consumed', R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.OpenOk_DecodeConsumedByClient;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LPayload := BuildOpenOk; // SERVER build
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK));
    DecodeOpenOk(R); // CLIENT decode
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.Blocked_ClientDecodesTheServerBuild;
var
  LPayload: TBytes;
  R: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LPayload := BuildBlocked('low on memory'); // SERVER build
  R := TAMQPReader.Create(LPayload);
  try
    LId := ReadMethodHeader(R);
    TAssert.AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED));
    TAssert.AssertEquals('reason', 'low on memory', DecodeBlocked(R)); // CLIENT decode
    TAssert.AssertTrue(R.EndOfData);
  finally
    R.Free;
  end;
end;

procedure TConnectionServerCodecTests.Unblocked_HeaderOnly;
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
    TAssert.AssertTrue('no arguments', R.EndOfData);
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
  LExpected: TBytes;
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

  // Secure-Ok: the server reads the raw response.
  W := TAMQPWriter.Create;
  try
    WriteMethodHeader(W, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_SECURE_OK);
    W.WriteLongStr(#0'u'#0'p');
    LPayload := W.ToBytes;
  finally
    W.Free;
  end;
  LExpected := AmqpUtf8Encode(#0'u'#0'p');
  R := TAMQPReader.Create(LPayload);
  try
    ReadMethodHeader(R);
    LResp := DecodeSecureOk(R);
    TAssert.AssertEquals(Length(LExpected), Length(LResp));
    for I := 0 to High(LExpected) do
      TAssert.AssertEquals(Integer(LExpected[I]), Integer(LResp[I]));
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

procedure TConnectionServerCodecTests.DefaultServerProperties_HasProductAndCapabilities;
var
  P, LCaps: TAMQPFieldTable;
  LProductVal, LCapsVal, LConfirmsVal: TValue;
begin
  P := DefaultServerProperties;
  try
    LProductVal := P['product'];
    TAssert.AssertEquals('pascal-amqp-faa (broker)', LProductVal.AsString);
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

procedure TTuneNegotiationTests.ChannelMax_ClientZero_AdoptsServer;
begin
  // Client wants "no limit" (0), server proposes 2047 -> uses 2047, never 0.
  TAssert.AssertEquals(2047, Integer(NegotiateChannelMax(2047, 0)));
end;

procedure TTuneNegotiationTests.ChannelMax_NeverExceedsServer;
begin
  // Client asks for more than the server allows -> capped at the server's value.
  TAssert.AssertEquals(2047, Integer(NegotiateChannelMax(2047, 5000)));
end;

procedure TTuneNegotiationTests.ChannelMax_ServerZero_UsesClient;
begin
  TAssert.AssertEquals(100, Integer(NegotiateChannelMax(0, 100)));
end;

procedure TTuneNegotiationTests.ChannelMax_BothZero_StaysZero;
begin
  TAssert.AssertEquals(0, Integer(NegotiateChannelMax(0, 0)));
end;

procedure TTuneNegotiationTests.ChannelMax_BothFinite_SmallerWins;
begin
  TAssert.AssertEquals(100, Integer(NegotiateChannelMax(2047, 100)));
end;

procedure TTuneNegotiationTests.FrameMax_Negotiates;
begin
  TAssert.AssertEquals(Int64(131072), Int64(NegotiateFrameMax(131072, 0)));
  TAssert.AssertEquals(Int64(65536), Int64(NegotiateFrameMax(131072, 65536)));
  TAssert.AssertEquals(Int64(131072), Int64(NegotiateFrameMax(0, 131072)));
end;

procedure TTuneNegotiationTests.NegotiateTune_Combines;
var
  LServer, LResult: TAMQPConnectionTune;
begin
  LServer.ChannelMax := 2047;
  LServer.FrameMax := 131072;
  LServer.Heartbeat := 60;
  // Client: channel-max 0 (no limit), frame-max 0, heartbeat 30.
  LResult := NegotiateTune(LServer, 0, 0, 30);
  TAssert.AssertEquals('channel-max takes the server''s', 2047, Integer(LResult.ChannelMax));
  TAssert.AssertEquals('frame-max takes the server''s', Int64(131072), Int64(LResult.FrameMax));
  TAssert.AssertEquals('smaller heartbeat wins', 30, Integer(LResult.Heartbeat));
end;

initialization
  TDUnitX.RegisterTestFixture(TConnectionMethodsTests);
  TDUnitX.RegisterTestFixture(TConnectionServerCodecTests);
  TDUnitX.RegisterTestFixture(TTuneNegotiationTests);

end.
