unit AMQP.ConnectionMethodsTests;

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Rtti,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Connection.Methods;

type
  TConnectionMethodsTests = class(TTestCase)
  published
    procedure StartOk_RoundTrip;
    procedure DecodeStart_LeArgumentos;
    procedure Start_SupportsMechanism;
    procedure Tune_DecodeEBuildTuneOk;
    procedure Open_RoundTrip;
    procedure OpenOk_Decode;
    procedure Close_RoundTrip;
    procedure Blocked_Decode;
    procedure PlainAuth_Formato;
    procedure DefaultClientProperties_TemProduct;
    procedure DefaultClientProperties_AnunciaConnectionBlocked;
  end;

  TTuneNegotiationTests = class(TTestCase)
  published
    // Gotcha do channel-max (ver CLAUDE.md)
    procedure ChannelMax_ClienteZero_AdotaServidor;
    procedure ChannelMax_NuncaExcedeServidor;
    procedure ChannelMax_ServidorZero_UsaCliente;
    procedure ChannelMax_AmbosZero_FicaZero;
    procedure ChannelMax_AmbosFinitos_MenorVence;
    procedure FrameMax_Negocia;
    procedure NegotiateTune_Combina;
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
      AssertTrue('class/method de Start-Ok',
        LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK));

      LDecoded := R.ReadFieldTable;
      try
        LProdutoVal := LDecoded['product'];
        AssertEquals('pascal-amqp-faa', LProdutoVal.AsString);
      finally
        LDecoded.Free;
      end;

      AssertEquals('mechanism', AMQP_AUTH_PLAIN, R.ReadShortStr);
      AssertEquals('response', PlainAuthResponse('guest', 'guest'), R.ReadLongStr);
      AssertEquals('locale', AMQP_LOCALE_DEFAULT, R.ReadShortStr);
      AssertTrue(R.EndOfData);
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
      AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START));
      LStart := DecodeStart(R);
      try
        AssertEquals(0, Integer(LStart.VersionMajor));
        AssertEquals(9, Integer(LStart.VersionMinor));
        AssertEquals('PLAIN AMQPLAIN', LStart.Mechanisms);
        AssertEquals('en_US', LStart.Locales);
        LProdutoVal := LStart.ServerProperties['product'];
        AssertEquals('RabbitMQ', LProdutoVal.AsString);
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
  AssertTrue(LStart.SupportsMechanism('PLAIN'));
  AssertTrue('case-insensitive', LStart.SupportsMechanism('plain'));
  AssertTrue(LStart.SupportsMechanism('AMQPLAIN'));
  AssertFalse(LStart.SupportsMechanism('EXTERNAL'));
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
      AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE));
      LTune := DecodeTune(R);
      AssertEquals(2047, Integer(LTune.ChannelMax));
      AssertEquals(QWord(131072), QWord(LTune.FrameMax));
      AssertEquals(60, Integer(LTune.Heartbeat));
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
    AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK));
    AssertEquals(2047, Integer(R.ReadShortUInt));
    AssertEquals(QWord(131072), QWord(R.ReadLongUInt));
    AssertEquals(60, Integer(R.ReadShortUInt));
    AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN));
    AssertEquals('virtual-host', '/', R.ReadShortStr);
    AssertEquals('reserved-1', '', R.ReadShortStr);
    AssertFalse('reserved-2 (insist)', R.ReadBit);
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
      AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK));
      DecodeOpenOk(R);
      AssertTrue(R.EndOfData);
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
    AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE));
    LDecoded := DecodeClose(R);
    AssertEquals(320, Integer(LDecoded.ReplyCode));
    AssertEquals('CONNECTION_FORCED', LDecoded.ReplyText);
    AssertEquals(0, Integer(LDecoded.ClassId));
    AssertEquals(0, Integer(LDecoded.MethodId));
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
      AssertTrue(LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED));
      AssertEquals('reason', 'low on memory', DecodeBlocked(R));
      AssertTrue(R.EndOfData);
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
  AssertEquals(12, Length(S));
  AssertTrue('separador inicial', S[1] = #0);
  AssertTrue('separador entre usuario e senha', S[7] = #0);
end;

procedure TConnectionMethodsTests.DefaultClientProperties_TemProduct;
var
  P: TAMQPFieldTable;
  LProdutoVal: TValue;
begin
  P := DefaultClientProperties;
  try
    LProdutoVal := P['product'];
    AssertEquals('pascal-amqp-faa', LProdutoVal.AsString);
    AssertTrue(P.ContainsKey('capabilities'));
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
    AssertTrue('capability presente', LCaps.ContainsKey('connection.blocked'));
    LBlockedVal := LCaps['connection.blocked'];
    AssertTrue('capability = True', LBlockedVal.AsBoolean);
  finally
    P.Free;
  end;
end;

{ TTuneNegotiationTests }

procedure TTuneNegotiationTests.ChannelMax_ClienteZero_AdotaServidor;
begin
  // Cliente quer "sem limite" (0), servidor propõe 2047 -> usa 2047, nunca 0.
  AssertEquals(2047, Integer(NegotiateChannelMax(2047, 0)));
end;

procedure TTuneNegotiationTests.ChannelMax_NuncaExcedeServidor;
begin
  // Cliente pede mais do que o servidor permite -> limita ao servidor.
  AssertEquals(2047, Integer(NegotiateChannelMax(2047, 5000)));
end;

procedure TTuneNegotiationTests.ChannelMax_ServidorZero_UsaCliente;
begin
  AssertEquals(100, Integer(NegotiateChannelMax(0, 100)));
end;

procedure TTuneNegotiationTests.ChannelMax_AmbosZero_FicaZero;
begin
  AssertEquals(0, Integer(NegotiateChannelMax(0, 0)));
end;

procedure TTuneNegotiationTests.ChannelMax_AmbosFinitos_MenorVence;
begin
  AssertEquals(100, Integer(NegotiateChannelMax(2047, 100)));
end;

procedure TTuneNegotiationTests.FrameMax_Negocia;
begin
  AssertEquals(QWord(131072), QWord(NegotiateFrameMax(131072, 0)));
  AssertEquals(QWord(65536), QWord(NegotiateFrameMax(131072, 65536)));
  AssertEquals(QWord(131072), QWord(NegotiateFrameMax(0, 131072)));
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
  AssertEquals('channel-max adota servidor', 2047, Integer(LResult.ChannelMax));
  AssertEquals('frame-max adota servidor', QWord(131072), QWord(LResult.FrameMax));
  AssertEquals('heartbeat menor vence', 30, Integer(LResult.Heartbeat));
end;

initialization
  RegisterTest(TConnectionMethodsTests);
  RegisterTest(TTuneNegotiationTests);

end.
