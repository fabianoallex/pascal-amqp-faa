unit AMQP.WireTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Unit/AMQP.WireTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }


interface

uses
  fpcunit, testregistry,
  AMQP.TestSupport,
  SysUtils, Rtti, AMQP.Wire;

type
  TAMQPWriterTests = class(TTestCase)
  private
    FWriter: TAMQPWriter;
    procedure DoWriteShortStrTooLong;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure Octet_EscreveUmByte;
    procedure ShortUInt_BigEndian;
    procedure LongUInt_BigEndian;
    procedure LongLongUInt_BigEndian;
    procedure ShortStr_PrefixaComprimentoEmUmOcteto;
    procedure ShortStr_UsaUTF8;
    procedure ShortStr_AcimaDe255_Levanta;
    procedure LongStr_PrefixaComprimentoEmU32;
    procedure Bits_EmpacotaLSBPrimeiro;
    procedure Bits_NonoBitVaiParaSegundoOcteto;
    procedure Bit_SeguidoDeOcteto_DescarregaByteParcial;
  end;

  TAMQPRoundTripTests = class(TTestCase)
  published
    procedure Octet;
    procedure ShortUInt_Limites;
    procedure LongUInt_Limites;
    procedure LongLongUInt_Limites;
    procedure ShortStr_ComAcentos;
    procedure LongStr_Vazia;
    procedure Timestamp;
    procedure Bits_Sequencia;
  end;

  TAMQPFieldTableTests = class(TTestCase)
  published
    procedure RoundTrip_TiposBasicos;
    procedure RoundTrip_TabelaAninhada;
    procedure TabelaVazia_TemComprimentoZero;
    procedure Decode_Boolean;
    procedure Decode_LongStr;
    procedure Decode_ArrayDeTabelas_EstiloXDeath;
    procedure Encode_ArrayDeTabelas_EstiloXDeath;
    procedure RoundTrip_ArrayDeEscalares;
    procedure Encode_ArrayVazio;
    procedure Encode_TBytes_SaiComoTagX_NaoComoArray;
    procedure PropriedadesDeCliente_Tipicas;
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

procedure TAMQPWriterTests.Octet_EscreveUmByte;
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

procedure TAMQPWriterTests.ShortStr_PrefixaComprimentoEmUmOcteto;
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

procedure TAMQPWriterTests.ShortStr_UsaUTF8;
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

procedure TAMQPWriterTests.ShortStr_AcimaDe255_Levanta;
begin
  ChecaLevanta(EAMQPWire, DoWriteShortStrTooLong);
end;

procedure TAMQPWriterTests.LongStr_PrefixaComprimentoEmU32;
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

procedure TAMQPWriterTests.Bits_EmpacotaLSBPrimeiro;
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

procedure TAMQPWriterTests.Bits_NonoBitVaiParaSegundoOcteto;
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

procedure TAMQPWriterTests.Bit_SeguidoDeOcteto_DescarregaByteParcial;
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

procedure TAMQPRoundTripTests.ShortUInt_Limites;
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

procedure TAMQPRoundTripTests.LongUInt_Limites;
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

procedure TAMQPRoundTripTests.LongLongUInt_Limites;
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

procedure TAMQPRoundTripTests.ShortStr_ComAcentos;
var
  W: TAMQPWriter;
  R: TAMQPReader;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteShortStr('emissão-NFe');
    R := TAMQPReader.Create(W.ToBytes);
    try
      TAssert.AssertEquals('emissão-NFe', R.ReadShortStr);
    finally
      R.Free;
    end;
  finally
    W.Free;
  end;
end;

procedure TAMQPRoundTripTests.LongStr_Vazia;
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

procedure TAMQPRoundTripTests.Bits_Sequencia;
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

procedure TAMQPFieldTableTests.RoundTrip_TiposBasicos;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LIn, LOut: TAMQPFieldTable;
  LTextoVal, LPrecoVal: TValue;
  LTexto: string;
  LPreco: Double;
begin
  LIn := TAMQPFieldTable.Create;
  W := TAMQPWriter.Create;
  try
    // Separate (not chained) calls and intermediate results in
    // local variables (not inline): some combinations of chained generics/TValue
    // trigger FPC 3.2.2 internal errors in this specific unit
    // (Internal error 2015071704 / 200510032) — see notes below.
    LIn.Put('flag', True);
    LIn.Put('numero', Integer(42));
    LIn.Put('grande', Int64(9876543210));
    LIn.Put('texto', 'olá');
    LIn.Put('preco', TValue.From<Double>(19.9));
    W.WriteFieldTable(LIn);

    R := TAMQPReader.Create(W.ToBytes);
    try
      LOut := R.ReadFieldTable;
      try
        TAssert.AssertTrue('flag', LOut['flag'].AsBoolean);
        TAssert.AssertEquals('numero', 42, LOut['numero'].AsInteger);
        TAssert.AssertEquals('grande', Int64(9876543210), LOut['grande'].AsInt64);
        // .AsString/.AsExtended chained directly on the indexer (Tabela['x'].AsY)
        // trigger an FPC 3.2.2 internal error (see note above) — that is why the
        // indexer's value goes into a local TValue before the accessor.
        LTextoVal := LOut['texto'];
        LTexto := LTextoVal.AsString;
        TAssert.AssertEquals('texto', 'olá', LTexto);
        LPrecoVal := LOut['preco'];
        LPreco := LPrecoVal.AsExtended;
        TAssert.AssertEquals('preco', Double(19.9), LPreco, 0.0001);
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

procedure TAMQPFieldTableTests.RoundTrip_TabelaAninhada;
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

procedure TAMQPFieldTableTests.TabelaVazia_TemComprimentoZero;
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
procedure TAMQPFieldTableTests.Decode_ArrayDeTabelas_EstiloXDeath;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LInner, LOut, LTab: TAMQPFieldTable;
  LInnerBytes, LArrPayload, LBody, LOuterBytes: TBytes;
  LVal, LEntrada, LCampo: TValue;
  LTexto: string;
begin
  LInner := TAMQPFieldTable.Create;
  W := TAMQPWriter.Create;
  try
    LInner.Put('queue', 'fila-trab');
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
      TAssert.AssertTrue('deve ter x-death', LOut.TryGetValue('x-death', LVal));
      TAssert.AssertTrue('valor deve ser array', LVal.IsArray);
      TAssert.AssertEquals('um elemento', 1, LVal.GetArrayLength);
      // AmqpUnwrapValue is mandatory: GetArrayElement returns the element
      // re-wrapped in a tkRecord TValue — without the unwrap, IsObject would give
      // False. It holds on Delphi too: neither of the two collapses in Make.
      LEntrada := AmqpUnwrapValue(LVal.GetArrayElement(0));
      TAssert.AssertTrue('elemento deve ser objeto', LEntrada.IsObject);
      TAssert.AssertTrue('elemento deve ser tabela', LEntrada.AsObject is TAMQPFieldTable);
      LTab := TAMQPFieldTable(LEntrada.AsObject);
      TAssert.AssertTrue('tabela deve ter queue', LTab.TryGetValue('queue', LCampo));
      LTexto := LCampo.AsString;
      TAssert.AssertEquals('queue', 'fila-trab', LTexto);
      TAssert.AssertTrue('tabela deve ter reason', LTab.TryGetValue('reason', LCampo));
      LTexto := LCampo.AsString;
      TAssert.AssertEquals('reason', 'rejected', LTexto);
      TAssert.AssertTrue('tabela deve ter count', LTab.TryGetValue('count', LCampo));
      TAssert.AssertEquals('count', 2, Integer(LCampo.AsInt64));
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
procedure TAMQPFieldTableTests.Encode_ArrayDeTabelas_EstiloXDeath;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LEntrada, LOut, LTab: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal, LElem, LCampo: TValue;
  LBytes: TBytes;
  LTexto: string;
begin
  LEntrada := TAMQPFieldTable.Create;
  LEntrada.Put('queue', 'fila-trab');
  LEntrada.Put('reason', 'expired');
  LEntrada.Put('count', Int64(3));

  SetLength(LArr, 1);
  LArr[0] := TValue.From<TObject>(LEntrada);

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

  TAssert.AssertTrue('deve ter escrito bytes', Length(LBytes) > 0);

  R := TAMQPReader.Create(LBytes);
  try
    LOut := R.ReadFieldTable;
    try
      TAssert.AssertTrue('deve ter x-death', LOut.TryGetValue('x-death', LVal));
      LVal := AmqpUnwrapValue(LVal);
      TAssert.AssertTrue('valor deve ser array', LVal.IsArray);
      TAssert.AssertEquals('um elemento', 1, LVal.GetArrayLength);
      LElem := AmqpUnwrapValue(LVal.GetArrayElement(0));
      TAssert.AssertTrue('elemento deve ser objeto', LElem.IsObject);
      TAssert.AssertTrue('elemento deve ser tabela', LElem.AsObject is TAMQPFieldTable);
      LTab := TAMQPFieldTable(LElem.AsObject);
      TAssert.AssertTrue('deve ter queue', LTab.TryGetValue('queue', LCampo));
      LTexto := LCampo.AsString;
      TAssert.AssertEquals('queue', 'fila-trab', LTexto);
      TAssert.AssertTrue('deve ter reason', LTab.TryGetValue('reason', LCampo));
      LTexto := LCampo.AsString;
      TAssert.AssertEquals('reason', 'expired', LTexto);
      TAssert.AssertTrue('deve ter count', LTab.TryGetValue('count', LCampo));
      TAssert.AssertEquals('count', 3, Integer(LCampo.AsInt64));
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
procedure TAMQPFieldTableTests.RoundTrip_ArrayDeEscalares;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LT1, LT2: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal, LElem: TValue;
  B1, B2: TBytes;
  LTexto: string;
begin
  SetLength(LArr, 3);
  LArr[0] := TValue.From<string>('um');
  LArr[1] := TValue.From<Int64>(42);
  LArr[2] := TValue.From<Boolean>(True);

  LT1 := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<TAMQPValueArray>(LArr);
    LT1.Put('lista', LVal);
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
    TAssert.AssertTrue('deve ter lista', LT2.TryGetValue('lista', LVal));
    LVal := AmqpUnwrapValue(LVal);
    TAssert.AssertEquals('tres elementos', 3, LVal.GetArrayLength);
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(0));
    LTexto := LElem.AsString;
    TAssert.AssertEquals('elemento 0', 'um', LTexto);
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(1));
    TAssert.AssertEquals('elemento 1', 42, Integer(LElem.AsInt64));
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(2));
    TAssert.AssertTrue('elemento 2', LElem.AsBoolean);

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

  TAssert.AssertEquals('os bytes tem de ser estaveis na segunda passada',
    BytesHex(B1), BytesHex(B2));
end;

procedure TAMQPFieldTableTests.Encode_ArrayVazio;
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
    LT1.Put('nada', LVal);
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
      TAssert.AssertTrue('deve ter a chave', LT2.TryGetValue('nada', LVal));
      LVal := AmqpUnwrapValue(LVal);
      TAssert.AssertTrue('deve ser array', LVal.IsArray);
      TAssert.AssertEquals('sem elementos', 0, LVal.GetArrayLength);
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
procedure TAMQPFieldTableTests.Encode_TBytes_SaiComoTagX_NaoComoArray;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LT1, LT2: TAMQPFieldTable;
  LVal: TValue;
  LOrig, LVolta, LBytes: TBytes;
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
  TAssert.AssertEquals('tag do valor tem de ser x (FV_BYTES), nao A',
    Ord('x'), Integer(LBytes[9]));

  R := TAMQPReader.Create(LBytes);
  try
    LT2 := R.ReadFieldTable;
    try
      TAssert.AssertTrue('deve ter blob', LT2.TryGetValue('blob', LVal));
      // AsType<T> does not exist in FPC 3.2 -- extracts element by element, which
      // works on both compilers.
      SetLength(LVolta, LVal.GetArrayLength);
      for I := 0 to High(LVolta) do
        LVolta[I] := Byte(LVal.GetArrayElement(I).AsInteger);
      TAssert.AssertEquals('quatro bytes de volta', 4, Length(LVolta));
      TAssert.AssertEquals('conteudo intacto', BytesHex(LOrig), BytesHex(LVolta));
    finally
      LT2.Free;
    end;
  finally
    R.Free;
  end;
end;

procedure TAMQPFieldTableTests.PropriedadesDeCliente_Tipicas;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LProps, LCaps, LOut, LCapsOut: TAMQPFieldTable;
  LProdutoVal, LCapsVal, LConfirmsVal: TValue;
begin
  // Builds client-properties as in Start-Ok and checks the round-trip.
  LCaps := TAMQPFieldTable.Create;
  LCaps.Put('publisher_confirms', True)
       .Put('consumer_cancel_notify', True);

  LProps := TAMQPFieldTable.Create;
  // Separate calls (see the note in RoundTrip_TiposBasicos about the FPC ICE
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
        LProdutoVal := LOut['product'];
        TAssert.AssertEquals('pascal-amqp-faa', LProdutoVal.AsString);
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
  RegisterTest(TAMQPWriterTests);
  RegisterTest(TAMQPRoundTripTests);
  RegisterTest(TAMQPFieldTableTests);

end.
