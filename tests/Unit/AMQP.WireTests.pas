unit AMQP.WireTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Rtti,
  AMQP.Wire;

type
  [TestFixture]
  TAMQPWriterTests = class
  private
    FWriter: TAMQPWriter;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure Octet_EscreveUmByte;
    [Test] procedure ShortUInt_BigEndian;
    [Test] procedure LongUInt_BigEndian;
    [Test] procedure LongLongUInt_BigEndian;
    [Test] procedure ShortStr_PrefixaComprimentoEmUmOcteto;
    [Test] procedure ShortStr_UsaUTF8;
    [Test] procedure ShortStr_AcimaDe255_Levanta;
    [Test] procedure LongStr_PrefixaComprimentoEmU32;
    [Test] procedure Bits_EmpacotaLSBPrimeiro;
    [Test] procedure Bits_NonoBitVaiParaSegundoOcteto;
    [Test] procedure Bit_SeguidoDeOcteto_DescarregaByteParcial;
  end;

  [TestFixture]
  TAMQPRoundTripTests = class
  public
    [Test] procedure Octet;
    [Test] procedure ShortUInt_Limites;
    [Test] procedure LongUInt_Limites;
    [Test] procedure LongLongUInt_Limites;
    [Test] procedure ShortStr_ComAcentos;
    [Test] procedure LongStr_Vazia;
    [Test] procedure Timestamp;
    [Test] procedure Bits_Sequencia;
  end;

  [TestFixture]
  TAMQPFieldTableTests = class
  public
    [Test] procedure RoundTrip_TiposBasicos;
    [Test] procedure RoundTrip_TabelaAninhada;
    [Test] procedure TabelaVazia_TemComprimentoZero;
    [Test] procedure Decode_Boolean;
    [Test] procedure Decode_LongStr;
    [Test] procedure Decode_ArrayDeTabelas_EstiloXDeath;
    [Test] procedure Encode_ArrayDeTabelas_EstiloXDeath;
    [Test] procedure RoundTrip_ArrayDeEscalares;
    [Test] procedure Encode_ArrayVazio;
    [Test] procedure Encode_TBytes_SaiComoTagX_NaoComoArray;
    [Test] procedure PropriedadesDeCliente_Tipicas;
  end;

implementation

// Compara um octeto esperado (como Integer) com o octeto real, forcando a
// sobrecarga nao-generica Assert.AreEqual(Integer, Integer) — evita
// ambiguidade de overload ao passar Byte diretamente.
procedure EqualByte(const AExpected: Integer; const AActual: Byte;
  const AMessage: string = '');
begin
  Assert.AreEqual(AExpected, Integer(AActual), AMessage);
end;

// Hex de um TBytes, para comparar buffers inteiros numa assercao so' (a
// mensagem de falha mostra os dois lados, o que um AreEqual byte a byte nao
// daria).
function BytesHex(const ABytes: TBytes): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(ABytes) do
    Result := Result + IntToHex(ABytes[I], 2);
end;

{ TAMQPWriterTests }

procedure TAMQPWriterTests.Setup;
begin
  FWriter := TAMQPWriter.Create;
end;

procedure TAMQPWriterTests.TearDown;
begin
  FWriter.Free;
end;

procedure TAMQPWriterTests.Octet_EscreveUmByte;
var
  B: TBytes;
begin
  FWriter.WriteOctet($7F);
  B := FWriter.ToBytes;
  Assert.AreEqual(1, Length(B));
  EqualByte($7F, B[0]);
end;

procedure TAMQPWriterTests.ShortUInt_BigEndian;
var
  B: TBytes;
begin
  FWriter.WriteShortUInt($1234);
  B := FWriter.ToBytes;
  Assert.AreEqual(2, Length(B));
  EqualByte($12, B[0]);
  EqualByte($34, B[1]);
end;

procedure TAMQPWriterTests.LongUInt_BigEndian;
var
  B: TBytes;
begin
  FWriter.WriteLongUInt($DEADBEEF);
  B := FWriter.ToBytes;
  Assert.AreEqual(4, Length(B));
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
  Assert.AreEqual(8, Length(B));
  EqualByte($01, B[0]);
  EqualByte($08, B[7]);
end;

procedure TAMQPWriterTests.ShortStr_PrefixaComprimentoEmUmOcteto;
var
  B: TBytes;
begin
  FWriter.WriteShortStr('hi');
  B := FWriter.ToBytes;
  Assert.AreEqual(3, Length(B));
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
  Assert.AreEqual(3, Length(B));
  EqualByte(2, B[0]);
  EqualByte($C3, B[1]);
  EqualByte($A1, B[2]);
end;

procedure TAMQPWriterTests.ShortStr_AcimaDe255_Levanta;
begin
  Assert.WillRaise(
    procedure
    begin
      FWriter.WriteShortStr(StringOfChar('x', 256));
    end,
    EAMQPWire);
end;

procedure TAMQPWriterTests.LongStr_PrefixaComprimentoEmU32;
var
  B: TBytes;
begin
  FWriter.WriteLongStr('AB');
  B := FWriter.ToBytes;
  Assert.AreEqual(6, Length(B));
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
  Assert.AreEqual(1, Length(B));
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
  Assert.AreEqual(2, Length(B));
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
  Assert.AreEqual(2, Length(B));
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
      Assert.IsTrue(R.EndOfData);
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
      Assert.AreEqual(Word(0), R.ReadShortUInt);
      Assert.AreEqual(Word(65535), R.ReadShortUInt);
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
      Assert.AreEqual(Cardinal(0), R.ReadLongUInt);
      Assert.AreEqual(Cardinal($FFFFFFFF), R.ReadLongUInt);
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
      Assert.IsTrue(V = R.ReadLongLongUInt);
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
      Assert.AreEqual('emissão-NFe', R.ReadShortStr);
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
      Assert.AreEqual('', R.ReadLongStr);
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
      Assert.IsTrue(V = R.ReadTimestamp);
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
      Assert.IsTrue(R.ReadBit);
      Assert.IsFalse(R.ReadBit);
      Assert.IsTrue(R.ReadBit);
      Assert.IsTrue(R.ReadBit);
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
begin
  LIn := TAMQPFieldTable.Create;
  W := TAMQPWriter.Create;
  try
    LIn.Put('flag', True)
       .Put('numero', Integer(42))
       .Put('grande', Int64(9876543210))
       .Put('texto', 'olá')
       .Put('preco', TValue.From<Double>(19.9));
    W.WriteFieldTable(LIn);

    R := TAMQPReader.Create(W.ToBytes);
    try
      LOut := R.ReadFieldTable;
      try
        Assert.IsTrue(LOut['flag'].AsBoolean, 'flag');
        Assert.AreEqual(42, LOut['numero'].AsInteger, 'numero');
        Assert.AreEqual(Int64(9876543210), LOut['grande'].AsInt64, 'grande');
        Assert.AreEqual('olá', LOut['texto'].AsString, 'texto');
        Assert.AreEqual(Double(19.9), Double(LOut['preco'].AsExtended),
          Double(0.0001), 'preco');
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
        LOutNested := LOut['capabilities'].AsType<TAMQPFieldTable>;
        Assert.IsTrue(LOutNested['authentication_failure_close'].AsBoolean);
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
    Assert.AreEqual(4, Length(B));
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
      Assert.IsTrue(T.ContainsKey('k'));
      Assert.IsTrue(T['k'].AsBoolean);
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
begin
  // nome 'v'; 'S' longstr len=2 "hi"
  R := TAMQPReader.Create(
    TBytes.Create(0, 0, 0, 9,
                  1, Ord('v'),
                  Ord('S'), 0, 0, 0, 2, Ord('h'), Ord('i')));
  try
    T := R.ReadFieldTable;
    try
      Assert.AreEqual('hi', T['v'].AsString);
    finally
      T.Free;
    end;
  finally
    R.Free;
  end;
end;

// Monta na mao o wire de {'x-death': [tabela {queue,reason,count}]} — o
// formato que o broker envia em mensagens dead-lettered. O writer nao emite
// arrays 'A' (sao decode-only nesta lib), entao o wrapper do array e' escrito
// cru: 'A' + u32(tamanho) + elementos ('F' + tabela).
procedure TAMQPFieldTableTests.Decode_ArrayDeTabelas_EstiloXDeath;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LInner, LOut, LTab: TAMQPFieldTable;
  LInnerBytes, LArrPayload, LBody, LOuterBytes: TBytes;
  LVal, LEntrada, LCampo: TValue;
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
    W.WriteOctet(Ord('F')); // um elemento: field-table
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
      Assert.IsTrue(LOut.TryGetValue('x-death', LVal), 'deve ter x-death');
      Assert.IsTrue(LVal.IsArray, 'valor deve ser array');
      Assert.AreEqual(1, LVal.GetArrayLength, 'um elemento');
      // AmqpUnwrapValue: no Delphi o Make ja colapsa TValue-em-TValue (e'
      // no-op aqui), mas no FPC o GetArrayElement devolve o elemento
      // re-embrulhado num TValue tkRecord — o unwrap uniformiza os dois.
      LEntrada := AmqpUnwrapValue(LVal.GetArrayElement(0));
      Assert.IsTrue(LEntrada.IsObject, 'elemento deve ser objeto');
      Assert.IsTrue(LEntrada.AsObject is TAMQPFieldTable, 'elemento deve ser tabela');
      LTab := TAMQPFieldTable(LEntrada.AsObject);
      Assert.IsTrue(LTab.TryGetValue('queue', LCampo), 'tabela deve ter queue');
      Assert.AreEqual('fila-trab', LCampo.AsString, 'queue');
      Assert.IsTrue(LTab.TryGetValue('reason', LCampo), 'tabela deve ter reason');
      Assert.AreEqual('rejected', LCampo.AsString, 'reason');
      Assert.IsTrue(LTab.TryGetValue('count', LCampo), 'tabela deve ter count');
      Assert.AreEqual(Int64(2), LCampo.AsInt64, 'count');
    finally
      // Tambem exercita o destrutor: a tabela aninhada DENTRO do array deve
      // ser liberada (fazia leak no FPC antes do AmqpUnwrapValue no Destroy;
      // a suite DUnitX roda com deteccao de leaks e valida o lado Delphi).
      LOut.Free;
    end;
  finally
    R.Free;
  end;
end;

// O caminho de ESCRITA do mesmo x-death que o teste acima le'. Ate' a Fase 3
// o codec era assimetrico: lia arrays 'A' e nao os escrevia (o
// WriteFieldValue nao tratava tkDynArray), entao esta forma -- array de
// tabelas, que e' exatamente o header x-death do dead-lettering -- levantava
// EAMQPWire. Vale para as duas pontas: sem isto, nem o broker emite x-death
// nem uma aplicacao cliente publica header de array.
procedure TAMQPFieldTableTests.Encode_ArrayDeTabelas_EstiloXDeath;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LEntrada, LOut, LTab: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal, LElem, LCampo: TValue;
  LBytes: TBytes;
begin
  LEntrada := TAMQPFieldTable.Create;
  LEntrada.Put('queue', 'fila-trab');
  LEntrada.Put('reason', 'expired');
  LEntrada.Put('count', Int64(3));

  SetLength(LArr, 1);
  LArr[0] := TValue.From<TObject>(LEntrada);

  LOut := TAMQPFieldTable.Create;
  try
    // TValue.From<T> numa variavel antes do Put: encadear a especializacao
    // inline num Put trava o FPC 3.2 com erro interno (gotcha do CLAUDE.md).
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
    LOut.Free; // dona da entrada aninhada dentro do array
  end;

  Assert.IsTrue(Length(LBytes) > 0, 'deve ter escrito bytes');

  R := TAMQPReader.Create(LBytes);
  try
    LOut := R.ReadFieldTable;
    try
      Assert.IsTrue(LOut.TryGetValue('x-death', LVal), 'deve ter x-death');
      LVal := AmqpUnwrapValue(LVal);
      Assert.IsTrue(LVal.IsArray, 'valor deve ser array');
      Assert.AreEqual(1, LVal.GetArrayLength, 'um elemento');
      LElem := AmqpUnwrapValue(LVal.GetArrayElement(0));
      Assert.IsTrue(LElem.IsObject and (LElem.AsObject is TAMQPFieldTable),
        'elemento deve ser tabela');
      LTab := TAMQPFieldTable(LElem.AsObject);
      Assert.IsTrue(LTab.TryGetValue('queue', LCampo), 'deve ter queue');
      Assert.AreEqual('fila-trab', LCampo.AsString, 'queue');
      Assert.IsTrue(LTab.TryGetValue('reason', LCampo), 'deve ter reason');
      Assert.AreEqual('expired', LCampo.AsString, 'reason');
      Assert.IsTrue(LTab.TryGetValue('count', LCampo), 'deve ter count');
      Assert.AreEqual(Int64(3), LCampo.AsInt64, 'count');
    finally
      LOut.Free;
    end;
  finally
    R.Free;
  end;
end;

// Escrever -> ler -> escrever tem de dar os MESMOS bytes. E' a propriedade que
// o dead-lettering depende: reescrever um header so' pode mudar o que mudou de
// fato. Cobre tres tipos de elemento no mesmo array.
procedure TAMQPFieldTableTests.RoundTrip_ArrayDeEscalares;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LT1, LT2: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal, LElem: TValue;
  B1, B2: TBytes;
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
    Assert.IsTrue(LT2.TryGetValue('lista', LVal), 'deve ter lista');
    LVal := AmqpUnwrapValue(LVal);
    Assert.AreEqual(3, LVal.GetArrayLength, 'tres elementos');
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(0));
    Assert.AreEqual('um', LElem.AsString, 'elemento 0');
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(1));
    Assert.AreEqual(Int64(42), LElem.AsInt64, 'elemento 1');
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(2));
    Assert.IsTrue(LElem.AsBoolean, 'elemento 2');

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

  Assert.AreEqual(BytesHex(B1), BytesHex(B2),
    'os bytes tem de ser estaveis na segunda passada');
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
      Assert.IsTrue(LT2.TryGetValue('nada', LVal), 'deve ter a chave');
      LVal := AmqpUnwrapValue(LVal);
      Assert.IsTrue(LVal.IsArray, 'deve ser array');
      Assert.AreEqual(0, LVal.GetArrayLength, 'sem elementos');
    finally
      LT2.Free;
    end;
  finally
    R.Free;
  end;
end;

// TBytes TAMBEM e' dynamic array, mas o wire dele e' 'x' (FV_BYTES), nao 'A'.
// Sem essa distincao no WriteFieldValue, um valor de bytes relido do wire
// seria reescrito como array de inteiros -- o tipo mudaria em silencio no
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

  // 4 (tamanho da tabela) + 1 (tamanho do nome) + 4 ('blob') = offset do tag.
  Assert.AreEqual(Ord('x'), Integer(LBytes[9]),
    'tag do valor tem de ser x (FV_BYTES), nao A');

  R := TAMQPReader.Create(LBytes);
  try
    LT2 := R.ReadFieldTable;
    try
      Assert.IsTrue(LT2.TryGetValue('blob', LVal), 'deve ter blob');
      // AsType<T> nao existe no FPC 3.2 -- extrai elemento a elemento, que
      // funciona nos dois compiladores.
      SetLength(LVolta, LVal.GetArrayLength);
      for I := 0 to High(LVolta) do
        LVolta[I] := Byte(LVal.GetArrayElement(I).AsInteger);
      Assert.AreEqual(4, Length(LVolta), 'quatro bytes de volta');
      Assert.AreEqual(BytesHex(LOrig), BytesHex(LVolta), 'conteudo intacto');
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
  LProps, LCaps, LOut: TAMQPFieldTable;
begin
  // Monta client-properties como no Start-Ok e confere o round-trip.
  LCaps := TAMQPFieldTable.Create;
  LCaps.Put('publisher_confirms', True)
       .Put('consumer_cancel_notify', True);

  LProps := TAMQPFieldTable.Create;
  LProps.Put('product', 'delphi-amqp-faa')
        .Put('version', '0.1.0')
        .Put('platform', 'Delphi')
        .Put('capabilities', TValue.From<TAMQPFieldTable>(LCaps));

  W := TAMQPWriter.Create;
  try
    W.WriteFieldTable(LProps);
    R := TAMQPReader.Create(W.ToBytes);
    try
      LOut := R.ReadFieldTable;
      try
        Assert.AreEqual('delphi-amqp-faa', LOut['product'].AsString);
        Assert.IsTrue(
          LOut['capabilities'].AsType<TAMQPFieldTable>['publisher_confirms'].AsBoolean);
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
