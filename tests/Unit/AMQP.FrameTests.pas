unit AMQP.FrameTests;


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  AMQP.TestSupport,
  SysUtils, Classes, AMQP.Protocol, AMQP.Frame;

type
  [TestFixture]
  TAMQPFrameTests = class
  private
    FTruncStream: TBytesStream;
    procedure DoReadInvalidFrameEnd;
    procedure DoReadTruncatedStream;
    procedure DoReadPayloadAboveMaximum;
  public
    [Test] procedure WriteTo_BinaryLayout;
    [Test] procedure WriteTo_EndsWithFrameEnd;
    [Test] procedure RoundTrip_Method;
    [Test] procedure RoundTrip_EmptyPayload;
    [Test] procedure RoundTrip_Heartbeat;
    [Test] procedure ReadFrom_InvalidFrameEnd_Raises;
    [Test] procedure ReadFrom_TruncatedStream_Raises;
    [Test] procedure ReadFrom_PayloadAboveMaximum_Raises;
    [Test] procedure ProtocolHeader_EightCorrectOctets;
    [Test] procedure RoundTrip_MultipleFramesInSequence;
  end;

implementation

// Forces the non-generic AssertEquals(Integer, Integer) overload.
procedure EqualByte(AExpected: Integer; AActual: Byte);
begin
  TAssert.AssertEquals(AExpected, Integer(AActual));
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

procedure TAMQPFrameTests.WriteTo_BinaryLayout;
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
    // type=1, channel=00 01, size=00 00 00 02, payload=AA BB, end=CE => 10 octets
    TAssert.AssertEquals(10, Length(B));
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

procedure TAMQPFrameTests.WriteTo_EndsWithFrameEnd;
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
    TAssert.AssertEquals(7, Integer(LIn.Channel));
    TAssert.AssertEquals(4, Length(LIn.Payload));
    EqualByte(30, LIn.Payload[2]);
    TAssert.AssertTrue(LIn.IsMethod);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.RoundTrip_EmptyPayload;
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
    TAssert.AssertEquals(0, Length(LIn.Payload));
    TAssert.AssertEquals(0, Integer(LIn.Channel));
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
    TAssert.AssertTrue(LIn.IsHeartbeat);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.DoReadInvalidFrameEnd;
begin
  TAMQPFrame.ReadFrom(FTruncStream);
end;

procedure TAMQPFrameTests.ReadFrom_InvalidFrameEnd_Raises;
begin
  // type=1, channel=0, size=1, payload=00, frame-end=FF (invalid)
  FTruncStream := BuildStream([1, 0, 0, 0, 0, 0, 1, 0, $FF]);
  try
    CheckRaises(EAMQPFrame, DoReadInvalidFrameEnd);
  finally
    FTruncStream.Free;
  end;
end;

procedure TAMQPFrameTests.DoReadTruncatedStream;
begin
  TAMQPFrame.ReadFrom(FTruncStream);
end;

procedure TAMQPFrameTests.ReadFrom_TruncatedStream_Raises;
begin
  // header says size=10 but the stream ends before that
  FTruncStream := BuildStream([1, 0, 0, 0, 0, 0, 10, 1, 2, 3]);
  try
    // EAMQPFrameEOF (subclass of EAMQPFrame): end of stream, not a malformed
    // frame. CheckRaises requires the exact class.
    CheckRaises(EAMQPFrameEOF, DoReadTruncatedStream);
  finally
    FTruncStream.Free;
  end;
end;

procedure TAMQPFrameTests.DoReadPayloadAboveMaximum;
begin
  TAMQPFrame.ReadFrom(FTruncStream, 16);
end;

procedure TAMQPFrameTests.ReadFrom_PayloadAboveMaximum_Raises;
begin
  // size=100, but we pass AMaxPayload=16
  FTruncStream := BuildStream([1, 0, 0, 0, 0, 0, 100]);
  try
    CheckRaises(EAMQPFrame, DoReadPayloadAboveMaximum);
  finally
    FTruncStream.Free;
  end;
end;

procedure TAMQPFrameTests.ProtocolHeader_EightCorrectOctets;
var
  LStream: TBytesStream;
  B: TBytes;
begin
  LStream := TBytesStream.Create;
  try
    WriteProtocolHeader(LStream);
    B := Copy(LStream.Bytes, 0, Integer(LStream.Size));
    TAssert.AssertEquals(8, Length(B));
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

procedure TAMQPFrameTests.RoundTrip_MultipleFramesInSequence;
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
    TAssert.AssertEquals(1, Integer(R1.Channel));
    EqualByte($11, R1.Payload[0]);
    TAssert.AssertEquals(2, Integer(R2.Channel));
    EqualByte($33, R2.Payload[1]);
  finally
    LStream.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPFrameTests);

end.
