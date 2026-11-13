unit AMQP.WireTests;


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  AMQP.TestSupport,
  SysUtils, Rtti, AMQP.Wire;

type
  [TestFixture]
  TAMQPWriterTests = class
  private
    FWriter: TAMQPWriter;
    procedure DoWriteShortStrTooLong;
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure Octet_WritesOneByte;
    [Test] procedure ShortUInt_BigEndian;
    [Test] procedure LongUInt_BigEndian;
    [Test] procedure LongLongUInt_BigEndian;
    [Test] procedure ShortStr_PrefixesLengthInOneOctet;
    [Test] procedure ShortStr_UsesUTF8;
    [Test] procedure ShortStr_Above255_Raises;
    [Test] procedure LongStr_PrefixesLengthAsU32;
    [Test] procedure Bits_PacksLSBFirst;
    [Test] procedure Bits_NinthBitGoesToSecondOctet;
    [Test] procedure Bit_FollowedByOctet_FlushesPartialByte;
  end;

  [TestFixture]
  TAMQPRoundTripTests = class
  public
    [Test] procedure Octet;
    [Test] procedure ShortUInt_Limits;
    [Test] procedure LongUInt_Limits;
    [Test] procedure LongLongUInt_Limits;
    [Test] procedure ShortStr_WithAccents;
    [Test] procedure LongStr_Empty;
    [Test] procedure Timestamp;
    [Test] procedure Bits_Sequence;
  end;

  [TestFixture]
  TAMQPFieldTableTests = class
  public
    [Test] procedure RoundTrip_BasicTypes;
    [Test] procedure RoundTrip_NestedTable;
    [Test] procedure EmptyTable_HasZeroLength;
    [Test] procedure Decode_Boolean;
    [Test] procedure Decode_LongStr;
    [Test] procedure Decode_ArrayOfTables_XDeathStyle;
    [Test] procedure Encode_ArrayOfTables_XDeathStyle;
    [Test] procedure RoundTrip_ArrayOfScalars;
    [Test] procedure Encode_EmptyArray;
    [Test] procedure Encode_TBytes_GoesOutAsTagX_NotAsArray;
    [Test] procedure ClientProperties_Typical;
  end;

implementation

// Compares an expected octet (as Integer) with the actual octet, forcing the
// AssertEquals(Integer, Integer) overload.
procedure EqualByte(AExpected: Integer; AActual: Byte);
begin
  TAssert.AssertEquals(AExpected, Integer(AActual));
end;

// Hex of a TBytes, to compare whole buffers in a single assertion (the
// failure message shows both sides, which a byte-by-byte AssertEquals
// would not).
function BytesHex(const ABytes: TBytes): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(ABytes) do
    Result := Result + IntToHex(ABytes[I], 2);
end;

{ TAMQPWriterTests }

procedure TAMQPWriterTests.SetUp;
begin
  FWriter := TAMQPWriter.Create;
end;

procedure TAMQPWriterTests.TearDown;
begin
  FWriter.Free;
end;

procedure TAMQPWriterTests.DoWriteShortStrTooLong;
begin
  FWriter.WriteShortStr(StringOfChar('x', 256));
end;

procedure TAMQPWriterTests.Octet_WritesOneByte;
var
  B: TBytes;
begin
  FWriter.WriteOctet($7F);
  B := FWriter.ToBytes;
  TAssert.AssertEquals(1, Length(B));
  EqualByte($7F, B[0]);
end;

procedure TAMQPWriterTests.ShortUInt_BigEndian;
var
  B: TBytes;
begin
  FWriter.WriteShortUInt($1234);
  B := FWriter.ToBytes;
  TAssert.AssertEquals(2, Length(B));
  EqualByte($12, B[0]);
  EqualByte($34, B[1]);
end;

procedure TAMQPWriterTests.LongUInt_BigEndian;
var
  B: TBytes;
begin
  FWriter.WriteLongUInt($DEADBEEF);
  B := FWriter.ToBytes;
  TAssert.AssertEquals(4, Length(B));
  EqualByte($DE, B[0]);
  EqualByte($AD, B[1]);
  EqualByte($BE, B[2]);
  EqualByte($EF, B[3]);
end;

procedure TAMQPWriterTests.LongLongUInt_BigEndian;
var
  B: TBytes;
begin
  FWriter.WriteLongLongUInt(UInt64($0102030405060708));
  B := FWriter.ToBytes;
  TAssert.AssertEquals(8, Length(B));
  EqualByte($01, B[0]);
  EqualByte($08, B[7]);
end;

procedure TAMQPWriterTests.ShortStr_PrefixesLengthInOneOctet;
var
  B: TBytes;
begin
  FWriter.WriteShortStr('hi');
  B := FWriter.ToBytes;
  TAssert.AssertEquals(3, Length(B));
  EqualByte(2, B[0]);
  EqualByte(Ord('h'), B[1]);
  EqualByte(Ord('i'), B[2]);
end;

procedure TAMQPWriterTests.ShortStr_UsesUTF8;
var
  B: TBytes;
begin
  // 'á' = 2 octets in UTF-8 (0xC3 0xA1).
  FWriter.WriteShortStr('á');
  B := FWriter.ToBytes;
  TAssert.AssertEquals(3, Length(B));
  EqualByte(2, B[0]);
  EqualByte($C3, B[1]);
  EqualByte($A1, B[2]);
end;

procedure TAMQPWriterTests.ShortStr_Above255_Raises;
begin
  CheckRaises(EAMQPWire, DoWriteShortStrTooLong);
end;

procedure TAMQPWriterTests.LongStr_PrefixesLengthAsU32;
var
  B: TBytes;
begin
  FWriter.WriteLongStr('AB');
  B := FWriter.ToBytes;
  TAssert.AssertEquals(6, Length(B));
  EqualByte(0, B[0]);
  EqualByte(0, B[1]);
  EqualByte(0, B[2]);
  EqualByte(2, B[3]);
  EqualByte(Ord('A'), B[4]);
end;

procedure TAMQPWriterTests.Bits_PacksLSBFirst;
var
  B: TBytes;
begin
  // true, false, true -> bits 0 and 2 set = 0b00000101 = 0x05
  FWriter.WriteBit(True);
  FWriter.WriteBit(False);
  FWriter.WriteBit(True);
  B := FWriter.ToBytes;
  TAssert.AssertEquals(1, Length(B));
  EqualByte($05, B[0]);
end;

procedure TAMQPWriterTests.Bits_NinthBitGoesToSecondOctet;
var
  B: TBytes;
  I: Integer;
begin
  // 8 bits true = 0xFF, then 1 bit true = 0x01 in the second octet.
  for I := 1 to 8 do
    FWriter.WriteBit(True);
  FWriter.WriteBit(True);
  B := FWriter.ToBytes;
  TAssert.AssertEquals(2, Length(B));
  EqualByte($FF, B[0]);
  EqualByte($01, B[1]);
end;

procedure TAMQPWriterTests.Bit_FollowedByOctet_FlushesPartialByte;
var
  B: TBytes;
begin
  FWriter.WriteBit(True);       // partial octet = 0x01
  FWriter.WriteOctet($AA);      // must flush the bit first
  B := FWriter.ToBytes;
  TAssert.AssertEquals(2, Length(B));
  EqualByte($01, B[0]);
  EqualByte($AA, B[1]);
end;

{ TAMQPRoundTripTests }

procedure TAMQPRoundTripTests.Octet;
var
  W: TAMQPWriter;
  R: TAMQPReader;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteOctet(200);
    R := TAMQPReader.Create(W.ToBytes);
    try
      EqualByte(200, R.ReadOctet);
      TAssert.AssertTrue(R.EndOfData);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.ShortUInt_Limits;
var
  W: TAMQPWriter;
  R: TAMQPReader;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteShortUInt(0);
    W.WriteShortUInt(65535);
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertEquals(0, Integer(R.ReadShortUInt));
      TAssert.AssertEquals(65535, Integer(R.ReadShortUInt));
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.LongUInt_Limits;
var
  W: TAMQPWriter;
  R: TAMQPReader;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteLongUInt(0);
    W.WriteLongUInt(Cardinal($FFFFFFFF));
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertEquals(Int64(0), Int64(R.ReadLongUInt));
      TAssert.AssertEquals(Int64($FFFFFFFF), Int64(R.ReadLongUInt));
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.LongLongUInt_Limits;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  V: UInt64;
begin
  V := UInt64($FEDCBA9876543210);
  W := TAMQPWriter.Create;
  try
    W.WriteLongLongUInt(V);
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertTrue(V = R.ReadLongLongUInt);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.ShortStr_WithAccents;
var
  W: TAMQPWriter;
  R: TAMQPReader;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteShortStr('naïve-café');
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertEquals('naïve-café', R.ReadShortStr);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.LongStr_Empty;
var
  W: TAMQPWriter;
  R: TAMQPReader;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteLongStr('');
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertEquals('', R.ReadLongStr);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.Timestamp;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  V: UInt64;
begin
  V := UInt64(1751731200); // ~2025-07-05 in epoch seconds
  W := TAMQPWriter.Create;
  try
    W.WriteTimestamp(V);
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertTrue(V = R.ReadTimestamp);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.Bits_Sequence;
var
  W: TAMQPWriter;
  R: TAMQPReader;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteBit(True);
    W.WriteBit(False);
    W.WriteBit(True);
    W.WriteBit(True);
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertTrue(R.ReadBit);
      TAssert.AssertFalse(R.ReadBit);
      TAssert.AssertTrue(R.ReadBit);
      TAssert.AssertTrue(R.ReadBit);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

{ TAMQPFieldTableTests }

procedure TAMQPFieldTableTests.RoundTrip_BasicTypes;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LIn, LOut: TAMQPFieldTable;
  LTextVal, LPriceVal: TValue;
  LText: string;
  LPrice: Double;
begin
  LIn := TAMQPFieldTable.Create;
  W := TAMQPWriter.Create;
  try
    // Separate (not chained) calls and intermediate results in
    // local variables (not inline): some combinations of chained generics/TValue
    // trigger FPC 3.2.2 internal errors in this specific unit
    // (Internal error 2015071704 / 200510032) — see notes below.
    LIn.Put('flag', True);
    LIn.Put('number', Integer(42));
    LIn.Put('big', Int64(9876543210));
    LIn.Put('text', 'café');
    LIn.Put('price', TValue.From<Double>(19.9));
    W.WriteFieldTable(LIn);

    R := TAMQPReader.Create(W.ToBytes);
    try
      LOut := R.ReadFieldTable;
      try
        TAssert.AssertTrue('flag', LOut['flag'].AsBoolean);
        TAssert.AssertEquals('number', 42, LOut['number'].AsInteger);
        TAssert.AssertEquals('big', Int64(9876543210), LOut['big'].AsInt64);
        // .AsString/.AsExtended chained directly on the indexer (Tabela['x'].AsY)
        // trigger an FPC 3.2.2 internal error (see note above) — that is why the
        // indexer's value goes into a local TValue before the accessor.
        LTextVal := LOut['text'];
        LText := LTextVal.AsString;
        TAssert.AssertEquals('text', 'café', LText);
        LPriceVal := LOut['price'];
        LPrice := LPriceVal.AsExtended;
        TAssert.AssertEquals('price', Double(19.9), LPrice, 0.0001);
      finally
        LOut.Free;
      end;
    finally
      R.Free;
    end;
  finally
    W.Free;
    LIn.Free;
  end;
end;

procedure TAMQPFieldTableTests.RoundTrip_NestedTable;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LIn, LNested, LOut, LOutNested: TAMQPFieldTable;
  LCapsVal, LFlagVal: TValue;
begin
  LNested := TAMQPFieldTable.Create;
  LNested.Put('authentication_failure_close', True);

  LIn := TAMQPFieldTable.Create;
  LIn.Put('capabilities', TValue.From<TAMQPFieldTable>(LNested));

  W := TAMQPWriter.Create;
  try
    W.WriteFieldTable(LIn);
    R := TAMQPReader.Create(W.ToBytes);
    try
      LOut := R.ReadFieldTable;
      try
        // TValue.AsType<T> does not exist in FPC (see CLAUDE.md) -> AsObject + cast.
        LCapsVal := LOut['capabilities'];
        LOutNested := TAMQPFieldTable(LCapsVal.AsObject);
        LFlagVal := LOutNested['authentication_failure_close'];
        TAssert.AssertTrue(LFlagVal.AsBoolean);
      finally
        LOut.Free; // also frees the decoded nested table
      end;
    finally
      R.Free;
    end;
  finally
    W.Free;
    LIn.Free; // also frees LNested (owner of the input nested table)
  end;
end;

procedure TAMQPFieldTableTests.EmptyTable_HasZeroLength;
var
  W: TAMQPWriter;
  LIn: TAMQPFieldTable;
  B: TBytes;
begin
  LIn := TAMQPFieldTable.Create;
  W := TAMQPWriter.Create;
  try
    W.WriteFieldTable(LIn);
    B := W.ToBytes;
    // Just the u32 length = 0.
    TAssert.AssertEquals(4, Length(B));
    EqualByte(0, B[0]);
    EqualByte(0, B[3]);
  finally
    W.Free;
    LIn.Free;
  end;
end;

procedure TAMQPFieldTableTests.Decode_Boolean;
var
  R: TAMQPReader;
  T: TAMQPFieldTable;
begin
  // len=4; name 'k'(len 1); 't'; 0x01
  R := TAMQPReader.Create(
    TBytes.Create(0, 0, 0, 4,
                  1, Ord('k'),
                  Ord('t'), 1));
  try
    T := R.ReadFieldTable;
    try
      TAssert.AssertTrue(T.ContainsKey('k'));
      TAssert.AssertTrue(T['k'].AsBoolean);
    finally
      T.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TAMQPFieldTableTests.Decode_LongStr;
var
  R: TAMQPReader;
  T: TAMQPFieldTable;
  LVal: TValue;
begin
  // name 'v'; 'S' longstr len=2 "hi"
  R := TAMQPReader.Create(
    TBytes.Create(0, 0, 0, 9,
                  1, Ord('v'),
                  Ord('S'), 0, 0, 0, 2, Ord('h'), Ord('i')));
  try
    T := R.ReadFieldTable;
    try
      LVal := T['v'];
      TAssert.AssertEquals('hi', LVal.AsString);
    finally
      T.Free;
    end;
  finally
    R.Free;
  end;
end;

// Builds by hand the wire of {'x-death': [table {queue,reason,count}]} — the
// format the broker sends in dead-lettered messages. The writer does not emit
// 'A' arrays (they are decode-only in this lib), so the array wrapper is written
// raw: 'A' + u32(size) + elements ('F' + table).
procedure TAMQPFieldTableTests.Decode_ArrayOfTables_XDeathStyle;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LInner, LOut, LTab: TAMQPFieldTable;
  LInnerBytes, LArrPayload, LBody, LOuterBytes: TBytes;
  LVal, LEntry, LField: TValue;
  LText: string;
begin
  LInner := TAMQPFieldTable.Create;
  W := TAMQPWriter.Create;
  try
    LInner.Put('queue', 'work-queue');
    LInner.Put('reason', 'rejected');
    LInner.Put('count', Int64(2));
    W.WriteFieldTable(LInner);
    LInnerBytes := W.ToBytes;
  finally
    W.Free;
    LInner.Free;
  end;

  W := TAMQPWriter.Create;
  try
    W.WriteOctet(Ord('F')); // one element: field-table
    W.WriteRaw(LInnerBytes);
    LArrPayload := W.ToBytes;
  finally
    W.Free;
  end;

  W := TAMQPWriter.Create;
  try
    W.WriteShortStr('x-death');
    W.WriteOctet(Ord('A'));
    W.WriteLongUInt(Cardinal(Length(LArrPayload)));
    W.WriteRaw(LArrPayload);
    LBody := W.ToBytes;
  finally
    W.Free;
  end;

  W := TAMQPWriter.Create;
  try
    W.WriteLongUInt(Cardinal(Length(LBody)));
    W.WriteRaw(LBody);
    LOuterBytes := W.ToBytes;
  finally
    W.Free;
  end;

  R := TAMQPReader.Create(LOuterBytes);
  try
    LOut := R.ReadFieldTable;
    try
      TAssert.AssertTrue('must have x-death', LOut.TryGetValue('x-death', LVal));
      TAssert.AssertTrue('value must be an array', LVal.IsArray);
      TAssert.AssertEquals('one element', 1, LVal.GetArrayLength);
      // AmqpUnwrapValue is mandatory: GetArrayElement returns the element
      // re-wrapped in a tkRecord TValue — without the unwrap, IsObject would give
      // False. It holds on Delphi too: neither of the two collapses in Make.
      LEntry := AmqpUnwrapValue(LVal.GetArrayElement(0));
      TAssert.AssertTrue('element must be an object', LEntry.IsObject);
      TAssert.AssertTrue('element must be a table', LEntry.AsObject is TAMQPFieldTable);
      LTab := TAMQPFieldTable(LEntry.AsObject);
      TAssert.AssertTrue('table must have queue', LTab.TryGetValue('queue', LField));
      LText := LField.AsString;
      TAssert.AssertEquals('queue', 'work-queue', LText);
      TAssert.AssertTrue('table must have reason', LTab.TryGetValue('reason', LField));
      LText := LField.AsString;
      TAssert.AssertEquals('reason', 'rejected', LText);
      TAssert.AssertTrue('table must have count', LTab.TryGetValue('count', LField));
      TAssert.AssertEquals('count', 2, Integer(LField.AsInt64));
    finally
      // Also exercises the destructor: the nested table INSIDE the array must
      // be freed (it leaked on FPC before AmqpUnwrapValue in Destroy).
      LOut.Free;
    end;
  finally
    R.Free;
  end;
end;

// The WRITE path of the same x-death the test above reads. Until Phase 3
// the codec was asymmetric: it read 'A' arrays and did not write them
// (WriteFieldValue did not handle tkDynArray), so this shape -- an array of
// tables, which is exactly dead-lettering's x-death header -- raised
// EAMQPWire. It holds for both ends: without this, neither does the broker emit
// x-death nor can a client application publish an array header.
procedure TAMQPFieldTableTests.Encode_ArrayOfTables_XDeathStyle;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LEntry, LOut, LTab: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal, LElem, LField: TValue;
  LBytes: TBytes;
  LText: string;
begin
  LEntry := TAMQPFieldTable.Create;
  LEntry.Put('queue', 'work-queue');
  LEntry.Put('reason', 'expired');
  LEntry.Put('count', Int64(3));

  SetLength(LArr, 1);
  LArr[0] := TValue.From<TObject>(LEntry);

  LOut := TAMQPFieldTable.Create;
  try
    // TValue.From<T> into a variable before Put: chaining the specialization
    // inline in a Put crashes FPC 3.2 with an internal error (CLAUDE.md gotcha).
    LVal := TValue.From<TAMQPValueArray>(LArr);
    LOut.Put('x-death', LVal);

    W := TAMQPWriter.Create;
    try
      W.WriteFieldTable(LOut);
      LBytes := W.ToBytes;
    finally
      W.Free;
    end;
  finally
    LOut.Free; // owner of the nested input inside the array
  end;

  TAssert.AssertTrue('must have written bytes', Length(LBytes) > 0);

  R := TAMQPReader.Create(LBytes);
  try
    LOut := R.ReadFieldTable;
    try
      TAssert.AssertTrue('must have x-death', LOut.TryGetValue('x-death', LVal));
      LVal := AmqpUnwrapValue(LVal);
      TAssert.AssertTrue('value must be an array', LVal.IsArray);
      TAssert.AssertEquals('one element', 1, LVal.GetArrayLength);
      LElem := AmqpUnwrapValue(LVal.GetArrayElement(0));
      TAssert.AssertTrue('element must be an object', LElem.IsObject);
      TAssert.AssertTrue('element must be a table', LElem.AsObject is TAMQPFieldTable);
      LTab := TAMQPFieldTable(LElem.AsObject);
      TAssert.AssertTrue('must have queue', LTab.TryGetValue('queue', LField));
      LText := LField.AsString;
      TAssert.AssertEquals('queue', 'work-queue', LText);
      TAssert.AssertTrue('must have reason', LTab.TryGetValue('reason', LField));
      LText := LField.AsString;
      TAssert.AssertEquals('reason', 'expired', LText);
      TAssert.AssertTrue('must have count', LTab.TryGetValue('count', LField));
      TAssert.AssertEquals('count', 3, Integer(LField.AsInt64));
    finally
      LOut.Free;
    end;
  finally
    R.Free;
  end;
end;

// Write -> read -> write must give the SAME bytes. It is the property
// dead-lettering depends on: rewriting a header may only change what actually
// changed. Covers three element types in the same array.
procedure TAMQPFieldTableTests.RoundTrip_ArrayOfScalars;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LT1, LT2: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal, LElem: TValue;
  B1, B2: TBytes;
  LText: string;
begin
  SetLength(LArr, 3);
  LArr[0] := TValue.From<string>('um');
  LArr[1] := TValue.From<Int64>(42);
  LArr[2] := TValue.From<Boolean>(True);

  LT1 := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<TAMQPValueArray>(LArr);
    LT1.Put('list', LVal);
    W := TAMQPWriter.Create;
    try
      W.WriteFieldTable(LT1);
      B1 := W.ToBytes;
    finally
      W.Free;
    end;
  finally
    LT1.Free;
  end;

  R := TAMQPReader.Create(B1);
  try
    LT2 := R.ReadFieldTable;
  finally
    R.Free;
  end;
  try
    TAssert.AssertTrue('must have list', LT2.TryGetValue('list', LVal));
    LVal := AmqpUnwrapValue(LVal);
    TAssert.AssertEquals('three elements', 3, LVal.GetArrayLength);
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(0));
    LText := LElem.AsString;
    TAssert.AssertEquals('element 0', 'um', LText);
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(1));
    TAssert.AssertEquals('element 1', 42, Integer(LElem.AsInt64));
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(2));
    TAssert.AssertTrue('element 2', LElem.AsBoolean);

    W := TAMQPWriter.Create;
    try
      W.WriteFieldTable(LT2);
      B2 := W.ToBytes;
    finally
      W.Free;
    end;
  finally
    LT2.Free;
  end;

  TAssert.AssertEquals('the bytes must be stable on the second pass',
    BytesHex(B1), BytesHex(B2));
end;

procedure TAMQPFieldTableTests.Encode_EmptyArray;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LT1, LT2: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal: TValue;
  LBytes: TBytes;
begin
  SetLength(LArr, 0);
  LT1 := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<TAMQPValueArray>(LArr);
    LT1.Put('nothing', LVal);
    W := TAMQPWriter.Create;
    try
      W.WriteFieldTable(LT1);
      LBytes := W.ToBytes;
    finally
      W.Free;
    end;
  finally
    LT1.Free;
  end;

  R := TAMQPReader.Create(LBytes);
  try
    LT2 := R.ReadFieldTable;
    try
      TAssert.AssertTrue('must have the key', LT2.TryGetValue('nothing', LVal));
      LVal := AmqpUnwrapValue(LVal);
      TAssert.AssertTrue('must be an array', LVal.IsArray);
      TAssert.AssertEquals('no elements', 0, LVal.GetArrayLength);
    finally
      LT2.Free;
    end;
  finally
    R.Free;
  end;
end;

// TBytes is ALSO a dynamic array, but its wire form is 'x' (FV_BYTES), not 'A'.
// Without that distinction in WriteFieldValue, a bytes value read back from the wire
// would be rewritten as an array of integers -- the type would silently change in the
// round-trip.
procedure TAMQPFieldTableTests.Encode_TBytes_GoesOutAsTagX_NotAsArray;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LT1, LT2: TAMQPFieldTable;
  LVal: TValue;
  LOrig, LRoundTripped, LBytes: TBytes;
  I: Integer;
begin
  SetLength(LOrig, 4);
  LOrig[0] := 1; LOrig[1] := 255; LOrig[2] := 0; LOrig[3] := 128;

  LT1 := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<TBytes>(LOrig);
    LT1.Put('blob', LVal);
    W := TAMQPWriter.Create;
    try
      W.WriteFieldTable(LT1);
      LBytes := W.ToBytes;
    finally
      W.Free;
    end;
  finally
    LT1.Free;
  end;

  // 4 (table size) + 1 (name size) + 4 ('blob') = offset of the tag.
  TAssert.AssertEquals('the value tag must be x (FV_BYTES), not A',
    Ord('x'), Integer(LBytes[9]));

  R := TAMQPReader.Create(LBytes);
  try
    LT2 := R.ReadFieldTable;
    try
      TAssert.AssertTrue('must have blob', LT2.TryGetValue('blob', LVal));
      // AsType<T> does not exist in FPC 3.2 -- extracts element by element, which
      // works on both compilers.
      SetLength(LRoundTripped, LVal.GetArrayLength);
      for I := 0 to High(LRoundTripped) do
        LRoundTripped[I] := Byte(LVal.GetArrayElement(I).AsInteger);
      TAssert.AssertEquals('four bytes back', 4, Length(LRoundTripped));
      TAssert.AssertEquals('content intact', BytesHex(LOrig), BytesHex(LRoundTripped));
    finally
      LT2.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TAMQPFieldTableTests.ClientProperties_Typical;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LProps, LCaps, LOut, LCapsOut: TAMQPFieldTable;
  LProductVal, LCapsVal, LConfirmsVal: TValue;
begin
  // Builds client-properties as in Start-Ok and checks the round-trip.
  LCaps := TAMQPFieldTable.Create;
  LCaps.Put('publisher_confirms', True)
       .Put('consumer_cancel_notify', True);

  LProps := TAMQPFieldTable.Create;
  // Separate calls (see the note in RoundTrip_BasicTypes about the FPC ICE
  // when chaining .Put(...) ending in a generic TValue.From<T>).
  LProps.Put('product', 'pascal-amqp-faa');
  LProps.Put('version', '0.1.0');
  LProps.Put('platform', 'FreePascal');
  LProps.Put('capabilities', TValue.From<TAMQPFieldTable>(LCaps));

  W := TAMQPWriter.Create;
  try
    W.WriteFieldTable(LProps);
    R := TAMQPReader.Create(W.ToBytes);
    try
      LOut := R.ReadFieldTable;
      try
        LProductVal := LOut['product'];
        TAssert.AssertEquals('pascal-amqp-faa', LProductVal.AsString);
        LCapsVal := LOut['capabilities'];
        LCapsOut := TAMQPFieldTable(LCapsVal.AsObject);
        LConfirmsVal := LCapsOut['publisher_confirms'];
        TAssert.AssertTrue(LConfirmsVal.AsBoolean);
      finally
        LOut.Free;
      end;
    finally
      R.Free;
    end;
  finally
    W.Free;
    LProps.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPWriterTests);
  TDUnitX.RegisterTestFixture(TAMQPRoundTripTests);
  TDUnitX.RegisterTestFixture(TAMQPFieldTableTests);

end.
