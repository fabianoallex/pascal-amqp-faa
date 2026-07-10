unit AMQP.FrameTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  AMQP.Protocol,
  AMQP.Frame;

type
  [TestFixture]
  TAMQPFrameTests = class
  public
    [Test] procedure WriteTo_LayoutBinario;
    [Test] procedure WriteTo_TerminaComFrameEnd;
    [Test] procedure RoundTrip_Method;
    [Test] procedure RoundTrip_PayloadVazio;
    [Test] procedure RoundTrip_Heartbeat;
    [Test] procedure ReadFrom_FrameEndInvalido_Levanta;
    [Test] procedure ReadFrom_StreamTruncado_Levanta;
    [Test] procedure ReadFrom_PayloadAcimaDoMaximo_Levanta;
    [Test] procedure ProtocolHeader_OitoOctetosCorretos;
    [Test] procedure RoundTrip_MultiplosFramesEmSequencia;
  end;

implementation

// Forca a sobrecarga nao-generica Assert.AreEqual(Integer, Integer).
procedure EqualByte(const AExpected: Integer; const AActual: Byte;
  const AMessage: string = '');
begin
  Assert.AreEqual(AExpected, Integer(AActual), AMessage);
end;

function BuildStream(const ABytes: array of Byte): TBytesStream;
var
  LTmp: TBytes;
  I: Integer;
begin
  SetLength(LTmp, Length(ABytes));
  for I := 0 to High(ABytes) do
    LTmp[I] := ABytes[I];
  Result := TBytesStream.Create(LTmp);
end;

{ TAMQPFrameTests }

procedure TAMQPFrameTests.WriteTo_LayoutBinario;
var
  LFrame: TAMQPFrame;
  LStream: TBytesStream;
  B: TBytes;
begin
  LFrame := TAMQPFrame.Create(AMQP_FRAME_METHOD, 1, TBytes.Create($AA, $BB));
  LStream := TBytesStream.Create;
  try
    LFrame.WriteTo(LStream);
    B := Copy(LStream.Bytes, 0, Integer(LStream.Size));
    // type=1, channel=00 01, size=00 00 00 02, payload=AA BB, end=CE => 10 octetos
    Assert.AreEqual(10, Length(B));
    EqualByte(AMQP_FRAME_METHOD, B[0]);
    EqualByte($00, B[1]);
    EqualByte($01, B[2]);
    EqualByte($00, B[3]);
    EqualByte($00, B[4]);
    EqualByte($00, B[5]);
    EqualByte($02, B[6]);
    EqualByte($AA, B[7]);
    EqualByte($BB, B[8]);
    EqualByte(AMQP_FRAME_END, B[9]);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.WriteTo_TerminaComFrameEnd;
var
  LFrame: TAMQPFrame;
  LStream: TBytesStream;
  B: TBytes;
begin
  LFrame := TAMQPFrame.Create(AMQP_FRAME_METHOD, 0, TBytes.Create($01));
  LStream := TBytesStream.Create;
  try
    LFrame.WriteTo(LStream);
    B := Copy(LStream.Bytes, 0, Integer(LStream.Size));
    EqualByte(AMQP_FRAME_END, B[High(B)]);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.RoundTrip_Method;
var
  LOut, LIn: TAMQPFrame;
  LStream: TBytesStream;
begin
  LOut := TAMQPFrame.Create(AMQP_FRAME_METHOD, 7, TBytes.Create(10, 20, 30, 40));
  LStream := TBytesStream.Create;
  try
    LOut.WriteTo(LStream);
    LStream.Position := 0;
    LIn := TAMQPFrame.ReadFrom(LStream);
    EqualByte(AMQP_FRAME_METHOD, LIn.FrameType);
    Assert.AreEqual(Word(7), LIn.Channel);
    Assert.AreEqual(4, Length(LIn.Payload));
    EqualByte(30, LIn.Payload[2]);
    Assert.IsTrue(LIn.IsMethod);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.RoundTrip_PayloadVazio;
var
  LOut, LIn: TAMQPFrame;
  LStream: TBytesStream;
begin
  LOut := TAMQPFrame.Create(AMQP_FRAME_HEARTBEAT, 0, nil);
  LStream := TBytesStream.Create;
  try
    LOut.WriteTo(LStream);
    LStream.Position := 0;
    LIn := TAMQPFrame.ReadFrom(LStream);
    Assert.AreEqual(0, Length(LIn.Payload));
    Assert.AreEqual(Word(0), LIn.Channel);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.RoundTrip_Heartbeat;
var
  LIn: TAMQPFrame;
  LStream: TBytesStream;
begin
  LStream := TBytesStream.Create;
  try
    TAMQPFrame.Heartbeat.WriteTo(LStream);
    LStream.Position := 0;
    LIn := TAMQPFrame.ReadFrom(LStream);
    Assert.IsTrue(LIn.IsHeartbeat);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.ReadFrom_FrameEndInvalido_Levanta;
var
  LStream: TBytesStream;
begin
  // type=1, channel=0, size=1, payload=00, frame-end=FF (invalido)
  LStream := BuildStream([1, 0, 0, 0, 0, 0, 1, 0, $FF]);
  try
    Assert.WillRaise(
      procedure
      begin
        TAMQPFrame.ReadFrom(LStream);
      end,
      EAMQPFrame);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.ReadFrom_StreamTruncado_Levanta;
var
  LStream: TBytesStream;
begin
  // cabecalho diz size=10 mas o stream acaba antes
  LStream := BuildStream([1, 0, 0, 0, 0, 0, 10, 1, 2, 3]);
  try
    Assert.WillRaise(
      procedure
      begin
        TAMQPFrame.ReadFrom(LStream);
      end,
      EAMQPFrame);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.ReadFrom_PayloadAcimaDoMaximo_Levanta;
var
  LStream: TBytesStream;
begin
  // size=100, mas passamos AMaxPayload=16
  LStream := BuildStream([1, 0, 0, 0, 0, 0, 100]);
  try
    Assert.WillRaise(
      procedure
      begin
        TAMQPFrame.ReadFrom(LStream, 16);
      end,
      EAMQPFrame);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.ProtocolHeader_OitoOctetosCorretos;
var
  LStream: TBytesStream;
  B: TBytes;
begin
  LStream := TBytesStream.Create;
  try
    WriteProtocolHeader(LStream);
    B := Copy(LStream.Bytes, 0, Integer(LStream.Size));
    Assert.AreEqual(8, Length(B));
    EqualByte(Ord('A'), B[0]);
    EqualByte(Ord('M'), B[1]);
    EqualByte(Ord('Q'), B[2]);
    EqualByte(Ord('P'), B[3]);
    EqualByte(0, B[4]);
    EqualByte(0, B[5]);
    EqualByte(9, B[6]);
    EqualByte(1, B[7]);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.RoundTrip_MultiplosFramesEmSequencia;
var
  LStream: TBytesStream;
  F1, F2, R1, R2: TAMQPFrame;
begin
  F1 := TAMQPFrame.Create(AMQP_FRAME_METHOD, 1, TBytes.Create($11));
  F2 := TAMQPFrame.Create(AMQP_FRAME_BODY, 2, TBytes.Create($22, $33));
  LStream := TBytesStream.Create;
  try
    F1.WriteTo(LStream);
    F2.WriteTo(LStream);
    LStream.Position := 0;
    R1 := TAMQPFrame.ReadFrom(LStream);
    R2 := TAMQPFrame.ReadFrom(LStream);
    Assert.AreEqual(Word(1), R1.Channel);
    EqualByte($11, R1.Payload[0]);
    Assert.AreEqual(Word(2), R2.Channel);
    EqualByte($33, R2.Payload[1]);
  finally
    LStream.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPFrameTests);

end.
