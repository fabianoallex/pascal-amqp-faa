unit AMQP.FrameTests;

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes, AMQP.Protocol, AMQP.Frame;

type
  TAMQPFrameTests = class(TTestCase)
  private
    FTruncStream: TBytesStream;
    procedure DoReadFrameEndInvalido;
    procedure DoReadStreamTruncado;
    procedure DoReadPayloadAcimaDoMaximo;
  published
    procedure WriteTo_LayoutBinario;
    procedure WriteTo_TerminaComFrameEnd;
    procedure RoundTrip_Method;
    procedure RoundTrip_PayloadVazio;
    procedure RoundTrip_Heartbeat;
    procedure ReadFrom_FrameEndInvalido_Levanta;
    procedure ReadFrom_StreamTruncado_Levanta;
    procedure ReadFrom_PayloadAcimaDoMaximo_Levanta;
    procedure ProtocolHeader_OitoOctetosCorretos;
    procedure RoundTrip_MultiplosFramesEmSequencia;
  end;

implementation

// Forca a sobrecarga nao-generica AssertEquals(Integer, Integer).
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
    AssertEquals(10, Length(B));
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
    AssertEquals(7, Integer(LIn.Channel));
    AssertEquals(4, Length(LIn.Payload));
    EqualByte(30, LIn.Payload[2]);
    AssertTrue(LIn.IsMethod);
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
    AssertEquals(0, Length(LIn.Payload));
    AssertEquals(0, Integer(LIn.Channel));
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
    AssertTrue(LIn.IsHeartbeat);
  finally
    LStream.Free;
  end;
end;

procedure TAMQPFrameTests.DoReadFrameEndInvalido;
begin
  TAMQPFrame.ReadFrom(FTruncStream);
end;

procedure TAMQPFrameTests.ReadFrom_FrameEndInvalido_Levanta;
begin
  // type=1, channel=0, size=1, payload=00, frame-end=FF (invalido)
  FTruncStream := BuildStream([1, 0, 0, 0, 0, 0, 1, 0, $FF]);
  try
    AssertException(EAMQPFrame, DoReadFrameEndInvalido);
  finally
    FTruncStream.Free;
  end;
end;

procedure TAMQPFrameTests.DoReadStreamTruncado;
begin
  TAMQPFrame.ReadFrom(FTruncStream);
end;

procedure TAMQPFrameTests.ReadFrom_StreamTruncado_Levanta;
begin
  // cabecalho diz size=10 mas o stream acaba antes
  FTruncStream := BuildStream([1, 0, 0, 0, 0, 0, 10, 1, 2, 3]);
  try
    AssertException(EAMQPFrame, DoReadStreamTruncado);
  finally
    FTruncStream.Free;
  end;
end;

procedure TAMQPFrameTests.DoReadPayloadAcimaDoMaximo;
begin
  TAMQPFrame.ReadFrom(FTruncStream, 16);
end;

procedure TAMQPFrameTests.ReadFrom_PayloadAcimaDoMaximo_Levanta;
begin
  // size=100, mas passamos AMaxPayload=16
  FTruncStream := BuildStream([1, 0, 0, 0, 0, 0, 100]);
  try
    AssertException(EAMQPFrame, DoReadPayloadAcimaDoMaximo);
  finally
    FTruncStream.Free;
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
    AssertEquals(8, Length(B));
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
    AssertEquals(1, Integer(R1.Channel));
    EqualByte($11, R1.Payload[0]);
    AssertEquals(2, Integer(R2.Channel));
    EqualByte($33, R2.Payload[1]);
  finally
    LStream.Free;
  end;
end;

initialization
  RegisterTest(TAMQPFrameTests);

end.
