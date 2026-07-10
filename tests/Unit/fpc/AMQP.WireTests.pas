unit AMQP.WireTests;

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Rtti, AMQP.Wire;

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
    procedure PropriedadesDeCliente_Tipicas;
  end;

implementation

// Compara um octeto esperado (como Integer) com o octeto real — mesma ideia
// do EqualByte do DUnitX, so' que chamando o AssertEquals de classe do
// FPCUnit (TAssert), ja que aqui e' uma funcao solta, nao um metodo.
procedure EqualByte(AExpected: Integer; AActual: Byte);
begin
  TAssert.AssertEquals(AExpected, Integer(AActual));
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
  AssertEquals(1, Length(B));
  EqualByte($7F, B[0]);
end;

procedure TAMQPWriterTests.ShortUInt_BigEndian;
var
  B: TBytes;
begin
  FWriter.WriteShortUInt($1234);
  B := FWriter.ToBytes;
  AssertEquals(2, Length(B));
  EqualByte($12, B[0]);
  EqualByte($34, B[1]);
end;

procedure TAMQPWriterTests.LongUInt_BigEndian;
var
  B: TBytes;
begin
  FWriter.WriteLongUInt($DEADBEEF);
  B := FWriter.ToBytes;
  AssertEquals(4, Length(B));
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
  AssertEquals(8, Length(B));
  EqualByte($01, B[0]);
  EqualByte($08, B[7]);
end;

procedure TAMQPWriterTests.ShortStr_PrefixaComprimentoEmUmOcteto;
var
  B: TBytes;
begin
  FWriter.WriteShortStr('hi');
  B := FWriter.ToBytes;
  AssertEquals(3, Length(B));
  EqualByte(2, B[0]);
  EqualByte(Ord('h'), B[1]);
  EqualByte(Ord('i'), B[2]);
end;

procedure TAMQPWriterTests.ShortStr_UsaUTF8;
var
  B: TBytes;
begin
  // 'á' = 2 octetos em UTF-8 (0xC3 0xA1).
  FWriter.WriteShortStr('á');
  B := FWriter.ToBytes;
  AssertEquals(3, Length(B));
  EqualByte(2, B[0]);
  EqualByte($C3, B[1]);
  EqualByte($A1, B[2]);
end;

procedure TAMQPWriterTests.ShortStr_AcimaDe255_Levanta;
begin
  AssertException(EAMQPWire, DoWriteShortStrTooLong);
end;

procedure TAMQPWriterTests.LongStr_PrefixaComprimentoEmU32;
var
  B: TBytes;
begin
  FWriter.WriteLongStr('AB');
  B := FWriter.ToBytes;
  AssertEquals(6, Length(B));
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
  // true, false, true -> bits 0 e 2 setados = 0b00000101 = 0x05
  FWriter.WriteBit(True);
  FWriter.WriteBit(False);
  FWriter.WriteBit(True);
  B := FWriter.ToBytes;
  AssertEquals(1, Length(B));
  EqualByte($05, B[0]);
end;

procedure TAMQPWriterTests.Bits_NonoBitVaiParaSegundoOcteto;
var
  B: TBytes;
  I: Integer;
begin
  // 8 bits true = 0xFF, depois 1 bit true = 0x01 no segundo octeto.
  for I := 1 to 8 do
    FWriter.WriteBit(True);
  FWriter.WriteBit(True);
  B := FWriter.ToBytes;
  AssertEquals(2, Length(B));
  EqualByte($FF, B[0]);
  EqualByte($01, B[1]);
end;

procedure TAMQPWriterTests.Bit_SeguidoDeOcteto_DescarregaByteParcial;
var
  B: TBytes;
begin
  FWriter.WriteBit(True);       // octeto parcial = 0x01
  FWriter.WriteOctet($AA);      // deve descarregar o bit antes
  B := FWriter.ToBytes;
  AssertEquals(2, Length(B));
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
      AssertTrue(R.EndOfData);
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
      AssertEquals(0, Integer(R.ReadShortUInt));
      AssertEquals(65535, Integer(R.ReadShortUInt));
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
      AssertEquals(QWord(0), QWord(R.ReadLongUInt));
      AssertEquals(QWord($FFFFFFFF), QWord(R.ReadLongUInt));
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
      AssertTrue(V = R.ReadLongLongUInt);
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
      AssertEquals('emissão-NFe', R.ReadShortStr);
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
      AssertEquals('', R.ReadLongStr);
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
  V := UInt64(1751731200); // ~2025-07-05 em segundos epoch
  W := TAMQPWriter.Create;
  try
    W.WriteTimestamp(V);
    R := TAMQPReader.Create(W.ToBytes);
    try
      AssertTrue(V = R.ReadTimestamp);
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
      AssertTrue(R.ReadBit);
      AssertFalse(R.ReadBit);
      AssertTrue(R.ReadBit);
      AssertTrue(R.ReadBit);
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
    // Chamadas separadas (nao encadeadas) e resultados intermediarios em
    // variaveis locais (nao inline): algumas combinacoes de generics/TValue
    // encadeados disparam erros internos do FPC 3.2.2 nesta unit especifica
    // (Internal error 2015071704 / 200510032) — ver notas abaixo.
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
        AssertTrue('flag', LOut['flag'].AsBoolean);
        AssertEquals('numero', 42, LOut['numero'].AsInteger);
        AssertEquals('grande', Int64(9876543210), LOut['grande'].AsInt64);
        // .AsString/.AsExtended encadeados direto no indexador (Tabela['x'].AsY)
        // disparam um erro interno do FPC 3.2.2 (ver nota acima) — por isso o
        // valor do indexador vai para uma TValue local antes do accessor.
        LTextoVal := LOut['texto'];
        LTexto := LTextoVal.AsString;
        AssertEquals('texto', 'olá', LTexto);
        LPrecoVal := LOut['preco'];
        LPreco := LPrecoVal.AsExtended;
        AssertEquals('preco', Double(19.9), LPreco, 0.0001);
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
        // TValue.AsType<T> nao existe no FPC (ver CLAUDE.md) -> AsObject + cast.
        LCapsVal := LOut['capabilities'];
        LOutNested := TAMQPFieldTable(LCapsVal.AsObject);
        LFlagVal := LOutNested['authentication_failure_close'];
        AssertTrue(LFlagVal.AsBoolean);
      finally
        LOut.Free; // libera tambem a tabela aninhada decodificada
      end;
    finally
      R.Free;
    end;
  finally
    W.Free;
    LIn.Free; // libera tambem LNested (dona da tabela aninhada de entrada)
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
    // Apenas o u32 de comprimento = 0.
    AssertEquals(4, Length(B));
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
  // len=4; nome 'k'(len 1); 't'; 0x01
  R := TAMQPReader.Create(
    TBytes.Create(0, 0, 0, 4,
                  1, Ord('k'),
                  Ord('t'), 1));
  try
    T := R.ReadFieldTable;
    try
      AssertTrue(T.ContainsKey('k'));
      AssertTrue(T['k'].AsBoolean);
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
  // nome 'v'; 'S' longstr len=2 "hi"
  R := TAMQPReader.Create(
    TBytes.Create(0, 0, 0, 9,
                  1, Ord('v'),
                  Ord('S'), 0, 0, 0, 2, Ord('h'), Ord('i')));
  try
    T := R.ReadFieldTable;
    try
      LVal := T['v'];
      AssertEquals('hi', LVal.AsString);
    finally
      T.Free;
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
  // Monta client-properties como no Start-Ok e confere o round-trip.
  LCaps := TAMQPFieldTable.Create;
  LCaps.Put('publisher_confirms', True)
       .Put('consumer_cancel_notify', True);

  LProps := TAMQPFieldTable.Create;
  // Chamadas separadas (ver nota em RoundTrip_TiposBasicos sobre o ICE do FPC
  // ao encadear .Put(...) terminando num TValue.From<T> generico).
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
        AssertEquals('pascal-amqp-faa', LProdutoVal.AsString);
        LCapsVal := LOut['capabilities'];
        LCapsOut := TAMQPFieldTable(LCapsVal.AsObject);
        LConfirmsVal := LCapsOut['publisher_confirms'];
        AssertTrue(LConfirmsVal.AsBoolean);
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
