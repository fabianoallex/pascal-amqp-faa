unit AMQP.Wire;

{$I amqp.inc}

{ Codec dos tipos primitivos do AMQP 0-9-1 (spec 4.2.5).

  TAMQPWriter acumula dados em um buffer de bytes; TAMQPReader consome de um
  TBytes. Todos os inteiros multi-byte sao big-endian (network byte order).

  Cuidados de protocolo (ver CLAUDE.md):
  - shortstr/longstr trafegam em UTF-8 (o string do Delphi e' UTF-16); shortstr
    tem no maximo 255 octetos.
  - bits consecutivos (usados em argumentos de metodo) sao empacotados LSB
    primeiro dentro de um octeto; um campo nao-bit "descarrega" o octeto parcial.
  - field-table usa TValue para carregar o valor de cada campo. Tabelas
    aninhadas ('F') sao TAMQPFieldTable e a tabela dona as libera. }

interface

uses
  SysUtils,
  Classes,
  TypInfo,
  Rtti,
  Generics.Collections;

// --- Conversao string <-> UTF-8 ---------------------------------------------
// No Delphi, string e' UTF-16 e a conversao e' TEncoding.UTF8. No FPC, string
// (AnsiString) carrega um codepage dinamico; convertemos via RawByteString +
// SetCodePage, que respeita o codepage real da string dos dois lados. Em apps
// Lazarus (DefaultSystemCodePage = UTF-8) o caminho todo e' lossless; em
// console FPC puro, configure SetMultiByteConversionCodePage(CP_UTF8) se for
// usar strings nao-ASCII.

function AmqpUtf8Encode(const AValue: string): TBytes;
function AmqpUtf8Decode(const ABytes: TBytes): string;

{ Desembrulha TValue-dentro-de-TValue. GetArrayElement sobre um array 'A'
  (TArray<TValue>) devolve, no FPC 3.2, o elemento RE-EMBRULHADO num TValue de
  Kind=tkRecord contendo o TValue interno — IsObject/IsArray/As* falham no
  embrulho. (O TValue.Make do Delphi colapsa TValue-em-TValue; o do FPC nao.)
  Use apos todo GetArrayElement de um array de field-values — ex.: as entradas
  do header x-death de mensagens dead-lettered. Idempotente: num TValue ja
  "plano" (ou no Delphi) devolve o valor como veio. }
function AmqpUnwrapValue(const AValue: TValue): TValue;

type
  /// Erro de encode/decode no nível do wire AMQP (shortstr/field-table fora
  /// dos limites do protocolo, tipo de TValue não suportado num field-table,
  /// leitura além do fim do buffer). Quase sempre aponta payload malformado
  /// vindo do broker, ou um TValue de tipo não suportado passado pela
  /// aplicação em TAMQPFieldTable.Put.
  EAMQPWire = class(Exception);

  { Alias para arrays de field-value ('A'). Alem de documentar, contorna o
    parser do FPC 3.2, que nao aceita o generic aninhado TValue.From<TArray<
    TValue>> em expressao. E' um alias puro: TypeInfo identico ao de
    TArray<TValue>. }
  TAMQPValueArray = TArray<TValue>;

  { Tabela de campos AMQP (field-table).
    Mapeia nome do campo -> valor (TValue). Valores aninhados do tipo tabela
    ('F') sao instancias de TAMQPFieldTable e sao liberados no destrutor desta
    tabela. Arrays ('A') decodificados que contenham tabelas tambem tem suas
    tabelas liberadas. }
  TAMQPFieldTable = class(TDictionary<string, TValue>)
  public
    destructor Destroy; override;
    /// Acesso fluente para montar tabelas (ex.: client-properties).
    function Put(const AName: string; const AValue: TValue): TAMQPFieldTable;
  end;

  TAMQPWriter = class
  private
    FStream: TBytesStream;
    FBitBuffer: Byte;
    FBitCount: Integer;
    procedure FlushBits;
    procedure WriteFieldValue(const AValue: TValue);
  public
    constructor Create;
    destructor Destroy; override;

    procedure WriteOctet(AValue: Byte);
    procedure WriteShortUInt(AValue: Word);
    procedure WriteLongUInt(AValue: Cardinal);
    procedure WriteLongLongUInt(AValue: UInt64);
    procedure WriteShortStr(const AValue: string);
    procedure WriteLongStr(const AValue: string);
    procedure WriteBit(AValue: Boolean);
    procedure WriteTimestamp(AValue: UInt64);
    procedure WriteFieldTable(ATable: TAMQPFieldTable);
    /// Escreve bytes crus, sem qualquer prefixo (descarrega bits pendentes antes).
    procedure WriteRaw(const ABytes: TBytes);

    /// Snapshot do conteudo acumulado (descarrega bits pendentes).
    function ToBytes: TBytes;
    function Size: Integer;
  end;

  TAMQPReader = class
  private
    FData: TBytes;
    FPos: Integer;
    FEnd: Integer;
    FBitByte: Byte;
    FBitsLeft: Integer;
    procedure ResetBits;
    function ReadRawByte: Byte;
    function ReadFieldValue: TValue;
  public
    /// Le a partir de AData[AOffset .. AOffset+ACount-1]. ACount<0 = ate o fim.
    constructor Create(const AData: TBytes; AOffset: Integer = 0; ACount: Integer = -1);

    function ReadOctet: Byte;
    function ReadShortUInt: Word;
    function ReadLongUInt: Cardinal;
    function ReadLongLongUInt: UInt64;
    function ReadShortStr: string;
    function ReadLongStr: string;
    function ReadBit: Boolean;
    function ReadTimestamp: UInt64;
    function ReadFieldTable: TAMQPFieldTable;
    function ReadRaw(ACount: Integer): TBytes;

    function BytesRemaining: Integer;
    function EndOfData: Boolean;
  end;

implementation

{$IFDEF FPC}
function AmqpUtf8Encode(const AValue: string): TBytes;
var
  R: RawByteString;
begin
  R := AValue;
  if (R <> '') and (StringCodePage(R) <> CP_UTF8) then
    SetCodePage(R, CP_UTF8, True); // converte do codepage real para UTF-8
  SetLength(Result, Length(R));
  if R <> '' then
    Move(R[1], Result[0], Length(R));
end;

function AmqpUtf8Decode(const ABytes: TBytes): string;
var
  R: RawByteString;
begin
  SetLength(R, Length(ABytes));
  if Length(ABytes) > 0 then
    Move(ABytes[0], R[1], Length(ABytes));
  SetCodePage(R, CP_UTF8, False); // marca os bytes como UTF-8 (sem converter)
  Result := R; // conversao (se houver) respeita o codepage de destino
end;
{$ELSE}
function AmqpUtf8Encode(const AValue: string): TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(AValue);
end;

function AmqpUtf8Decode(const ABytes: TBytes): string;
begin
  Result := TEncoding.UTF8.GetString(ABytes);
end;
{$ENDIF}

// Tags de tipo de field-value conforme implementadas pelo RabbitMQ.
const
  FV_BOOLEAN     = Ord('t');
  FV_INT8        = Ord('b');
  FV_UINT8       = Ord('B');
  FV_INT16       = Ord('s');
  FV_UINT16      = Ord('u');
  FV_INT32       = Ord('I');
  FV_UINT32      = Ord('i');
  FV_INT64       = Ord('l');
  FV_FLOAT       = Ord('f');
  FV_DOUBLE      = Ord('d');
  FV_DECIMAL     = Ord('D');
  FV_LONGSTR     = Ord('S');
  FV_ARRAY       = Ord('A');
  FV_TIMESTAMP   = Ord('T');
  FV_TABLE       = Ord('F');
  FV_VOID        = Ord('V');
  FV_BYTES       = Ord('x');

// Potencia inteira de 10 (auxiliar para decodificar 'D' decimal),
// evitando dependencia de System.Math so por isto.
function Power10(AExp: Byte): Double;
var
  I: Integer;
begin
  Result := 1;
  for I := 1 to AExp do
    Result := Result * 10;
end;

function AmqpUnwrapValue(const AValue: TValue): TValue;
type
  PLocalValue = ^TValue;
begin
  Result := AValue;
  // Loop por segurança (aninhamento múltiplo é teórico); no Delphi nunca
  // entra — o TValue.Make de lá já colapsa TValue-em-TValue.
  while (Result.Kind = tkRecord) and (Result.TypeInfo = TypeInfo(TValue)) do
    Result := PLocalValue(Result.GetReferenceToRawData)^;
end;

{ TAMQPFieldTable }

destructor TAMQPFieldTable.Destroy;

  procedure FreeValue(const AValue: TValue);
  var
    LObj: TObject;
    I: Integer;
  begin
    if AValue.IsObject then
    begin
      LObj := AValue.AsObject;
      if LObj is TAMQPFieldTable then
        LObj.Free;
    end
    // So arrays 'A' (TArray<TValue>) podem conter tabelas a liberar; um 'x'
    // (TBytes) tambem satisfaz IsArray, por isso checamos o tipo exato.
    // (GetArrayLength/GetArrayElement em vez de AsType<TArray<TValue>>, que
    // nao existe no TValue do FPC 3.2.)
    else if AValue.IsArray and (AValue.TypeInfo = TypeInfo(TArray<TValue>)) then
    begin
      // AmqpUnwrapValue: no FPC, GetArrayElement devolve o elemento
      // re-embrulhado (tkRecord) e o IsObject do embrulho daria False —
      // as tabelas aninhadas vazariam.
      for I := 0 to AValue.GetArrayLength - 1 do
        FreeValue(AmqpUnwrapValue(AValue.GetArrayElement(I)));
    end;
  end;

var
  LValue: TValue;
begin
  for LValue in Values do
    FreeValue(LValue);
  inherited;
end;

function TAMQPFieldTable.Put(const AName: string; const AValue: TValue): TAMQPFieldTable;
begin
  AddOrSetValue(AName, AValue);
  Result := Self;
end;

{ TAMQPWriter }

constructor TAMQPWriter.Create;
begin
  inherited Create;
  FStream := TBytesStream.Create;
end;

destructor TAMQPWriter.Destroy;
begin
  FStream.Free;
  inherited;
end;

procedure TAMQPWriter.FlushBits;
begin
  if FBitCount > 0 then
  begin
    // Buffer passado por referencia (untyped) — sem @ (ver CLAUDE.md).
    FStream.Write(FBitBuffer, 1);
    FBitBuffer := 0;
    FBitCount := 0;
  end;
end;

procedure TAMQPWriter.WriteOctet(AValue: Byte);
begin
  FlushBits;
  FStream.Write(AValue, 1);
end;

procedure TAMQPWriter.WriteShortUInt(AValue: Word);
var
  LBytes: array[0..1] of Byte;
begin
  FlushBits;
  LBytes[0] := Byte(AValue shr 8);
  LBytes[1] := Byte(AValue);
  FStream.Write(LBytes[0], 2);
end;

procedure TAMQPWriter.WriteLongUInt(AValue: Cardinal);
var
  LBytes: array[0..3] of Byte;
begin
  FlushBits;
  LBytes[0] := Byte(AValue shr 24);
  LBytes[1] := Byte(AValue shr 16);
  LBytes[2] := Byte(AValue shr 8);
  LBytes[3] := Byte(AValue);
  FStream.Write(LBytes[0], 4);
end;

procedure TAMQPWriter.WriteLongLongUInt(AValue: UInt64);
var
  LBytes: array[0..7] of Byte;
  I: Integer;
begin
  FlushBits;
  for I := 0 to 7 do
    LBytes[I] := Byte(AValue shr (8 * (7 - I)));
  FStream.Write(LBytes[0], 8);
end;

procedure TAMQPWriter.WriteShortStr(const AValue: string);
var
  LBytes: TBytes;
begin
  LBytes := AmqpUtf8Encode(AValue);
  if Length(LBytes) > 255 then
    raise EAMQPWire.CreateFmt('shortstr excede 255 octetos (%d)', [Length(LBytes)]);
  WriteOctet(Byte(Length(LBytes)));
  if Length(LBytes) > 0 then
    FStream.Write(LBytes[0], Length(LBytes));
end;

procedure TAMQPWriter.WriteLongStr(const AValue: string);
var
  LBytes: TBytes;
begin
  LBytes := AmqpUtf8Encode(AValue);
  WriteLongUInt(Cardinal(Length(LBytes)));
  if Length(LBytes) > 0 then
    FStream.Write(LBytes[0], Length(LBytes));
end;

procedure TAMQPWriter.WriteBit(AValue: Boolean);
begin
  if FBitCount >= 8 then
    FlushBits;
  if AValue then
    FBitBuffer := FBitBuffer or (1 shl FBitCount);
  Inc(FBitCount);
end;

procedure TAMQPWriter.WriteTimestamp(AValue: UInt64);
begin
  WriteLongLongUInt(AValue);
end;

procedure TAMQPWriter.WriteRaw(const ABytes: TBytes);
begin
  FlushBits;
  if Length(ABytes) > 0 then
    FStream.Write(ABytes[0], Length(ABytes));
end;

procedure TAMQPWriter.WriteFieldValue(const AValue: TValue);
var
  LObj: TObject;
  LSingle: Single;
  LDouble: Double;
begin
  case AValue.Kind of
    // No FPC, Boolean tem Kind proprio (tkBool); no Delphi cai em tkEnumeration.
    {$IFDEF FPC}
    tkBool:
      begin
        WriteOctet(FV_BOOLEAN);
        if AValue.AsBoolean then
          WriteOctet(1)
        else
          WriteOctet(0);
      end;
    {$ENDIF}
    tkEnumeration:
      if AValue.TypeInfo = TypeInfo(Boolean) then
      begin
        WriteOctet(FV_BOOLEAN);
        if AValue.AsBoolean then
          WriteOctet(1)
        else
          WriteOctet(0);
      end
      else
        raise EAMQPWire.Create('field-table: enum nao suportado (apenas Boolean)');

    tkInteger:
      begin
        WriteOctet(FV_INT32);
        WriteLongUInt(Cardinal(AValue.AsInteger));
      end;

    tkInt64:
      begin
        WriteOctet(FV_INT64);
        WriteLongLongUInt(UInt64(AValue.AsInt64));
      end;

    tkFloat:
      if AValue.TypeInfo = TypeInfo(Single) then
      begin
        WriteOctet(FV_FLOAT);
        LSingle := AValue.AsExtended;
        WriteLongUInt(PCardinal(@LSingle)^);
      end
      else
      begin
        WriteOctet(FV_DOUBLE);
        LDouble := AValue.AsExtended;
        WriteLongLongUInt(PUInt64(@LDouble)^);
      end;

    tkString, tkUString, tkLString, tkWString, tkChar, tkWChar
    {$IFDEF FPC}, tkAString{$ENDIF}: // AnsiString do FPC tem kind proprio
      begin
        WriteOctet(FV_LONGSTR);
        WriteLongStr(AValue.AsString);
      end;

    tkClass:
      begin
        LObj := AValue.AsObject;
        if LObj is TAMQPFieldTable then
        begin
          WriteOctet(FV_TABLE);
          WriteFieldTable(TAMQPFieldTable(LObj));
        end
        else
          raise EAMQPWire.Create('field-table: objeto nao suportado (apenas TAMQPFieldTable)');
      end;
  else
    raise EAMQPWire.CreateFmt('field-table: TValue.Kind nao suportado (%d)', [Ord(AValue.Kind)]);
  end;
end;

procedure TAMQPWriter.WriteFieldTable(ATable: TAMQPFieldTable);
var
  LInner: TAMQPWriter;
  LPair: TPair<string, TValue>;
  LBytes: TBytes;
begin
  LInner := TAMQPWriter.Create;
  try
    if ATable <> nil then
      for LPair in ATable do
      begin
        LInner.WriteShortStr(LPair.Key);
        LInner.WriteFieldValue(LPair.Value);
      end;
    LBytes := LInner.ToBytes;
  finally
    LInner.Free;
  end;
  WriteLongUInt(Cardinal(Length(LBytes)));
  WriteRaw(LBytes);
end;

function TAMQPWriter.ToBytes: TBytes;
begin
  FlushBits;
  Result := Copy(FStream.Bytes, 0, Integer(FStream.Size));
end;

function TAMQPWriter.Size: Integer;
begin
  Result := Integer(FStream.Size);
  if FBitCount > 0 then
    Inc(Result);
end;

{ TAMQPReader }

constructor TAMQPReader.Create(const AData: TBytes; AOffset, ACount: Integer);
begin
  inherited Create;
  FData := AData;
  FPos := AOffset;
  if ACount < 0 then
    FEnd := Length(AData)
  else
    FEnd := AOffset + ACount;
  if FEnd > Length(AData) then
    raise EAMQPWire.Create('TAMQPReader: intervalo excede o buffer');
end;

procedure TAMQPReader.ResetBits;
begin
  FBitsLeft := 0;
end;

function TAMQPReader.ReadRawByte: Byte;
begin
  if FPos >= FEnd then
    raise EAMQPWire.Create('leitura alem do fim do buffer');
  Result := FData[FPos];
  Inc(FPos);
end;

function TAMQPReader.ReadOctet: Byte;
begin
  ResetBits;
  Result := ReadRawByte;
end;

function TAMQPReader.ReadShortUInt: Word;
var
  LHi, LLo: Byte;
begin
  ResetBits;
  // Ler em locais separados: a ordem de avaliacao de operandos com efeito
  // colateral nao e' garantida no Delphi (leria os octetos trocados).
  LHi := ReadRawByte;
  LLo := ReadRawByte;
  Result := (Word(LHi) shl 8) or Word(LLo);
end;

function TAMQPReader.ReadLongUInt: Cardinal;
var
  I: Integer;
begin
  ResetBits;
  Result := 0;
  for I := 0 to 3 do
    Result := (Result shl 8) or Cardinal(ReadRawByte);
end;

function TAMQPReader.ReadLongLongUInt: UInt64;
var
  I: Integer;
begin
  ResetBits;
  Result := 0;
  for I := 0 to 7 do
    Result := (Result shl 8) or UInt64(ReadRawByte);
end;

function TAMQPReader.ReadRaw(ACount: Integer): TBytes;
begin
  ResetBits;
  if ACount < 0 then
    raise EAMQPWire.Create('ReadRaw: contagem negativa');
  if FPos + ACount > FEnd then
    raise EAMQPWire.Create('ReadRaw: leitura alem do fim do buffer');
  Result := Copy(FData, FPos, ACount);
  Inc(FPos, ACount);
end;

function TAMQPReader.ReadShortStr: string;
var
  LLen: Byte;
begin
  LLen := ReadOctet;
  Result := AmqpUtf8Decode(ReadRaw(LLen));
end;

function TAMQPReader.ReadLongStr: string;
var
  LLen: Cardinal;
begin
  LLen := ReadLongUInt;
  Result := AmqpUtf8Decode(ReadRaw(Integer(LLen)));
end;

function TAMQPReader.ReadBit: Boolean;
begin
  if FBitsLeft = 0 then
  begin
    FBitByte := ReadRawByte;
    FBitsLeft := 8;
  end;
  Result := (FBitByte and 1) <> 0;
  FBitByte := FBitByte shr 1;
  Dec(FBitsLeft);
end;

function TAMQPReader.ReadTimestamp: UInt64;
begin
  Result := ReadLongLongUInt;
end;

function TAMQPReader.ReadFieldValue: TValue;
var
  LTag: Byte;
  LScale: Byte;
  LUnscaled: Integer;
  LU32: Cardinal;
  LU64: UInt64;
  LSingle: Single;
  LDouble: Double;
  LArr: TArray<TValue>;
  LArrLen: Integer;
  LArrEnd: Integer;
begin
  ResetBits;
  LTag := ReadRawByte;
  case LTag of
    FV_BOOLEAN:   Result := TValue.From<Boolean>(ReadOctet <> 0);
    FV_INT8:      Result := Integer(ShortInt(ReadOctet));
    FV_UINT8:     Result := Integer(ReadOctet);
    FV_INT16:     Result := Integer(SmallInt(ReadShortUInt));
    FV_UINT16:    Result := Integer(ReadShortUInt);
    FV_INT32:     Result := Integer(ReadLongUInt);
    FV_UINT32:    Result := Int64(ReadLongUInt);
    FV_INT64:     Result := Int64(ReadLongLongUInt);
    FV_TIMESTAMP: Result := Int64(ReadTimestamp);
    FV_FLOAT:
      begin
        LU32 := ReadLongUInt;
        Move(LU32, LSingle, SizeOf(LSingle));
        Result := TValue.From<Single>(LSingle);
      end;
    FV_DOUBLE:
      begin
        LU64 := ReadLongLongUInt;
        Move(LU64, LDouble, SizeOf(LDouble));
        Result := TValue.From<Double>(LDouble);
      end;
    FV_DECIMAL:
      begin
        LScale := ReadOctet;
        LUnscaled := Integer(ReadLongUInt);
        Result := TValue.From<Double>(LUnscaled / Power10(LScale));
      end;
    FV_LONGSTR:   Result := ReadLongStr;
    FV_BYTES:     Result := TValue.From<TBytes>(ReadRaw(Integer(ReadLongUInt)));
    FV_VOID:      Result := TValue.Empty;
    FV_TABLE:     Result := TValue.From<TAMQPFieldTable>(ReadFieldTable);
    FV_ARRAY:
      begin
        LArrEnd := FPos + Integer(ReadLongUInt);
        LArrLen := 0;
        SetLength(LArr, 0);
        while FPos < LArrEnd do
        begin
          Inc(LArrLen);
          SetLength(LArr, LArrLen);
          LArr[LArrLen - 1] := ReadFieldValue;
        end;
        Result := TValue.From<TAMQPValueArray>(LArr);
      end;
  else
    raise EAMQPWire.CreateFmt('field-value: tag desconhecida (%d / %s)',
      [LTag, string(Chr(LTag))]);
  end;
end;

function TAMQPReader.ReadFieldTable: TAMQPFieldTable;
var
  LTableEnd: Integer;
  LName: string;
begin
  LTableEnd := FPos + Integer(ReadLongUInt);
  if LTableEnd > FEnd then
    raise EAMQPWire.Create('field-table: tamanho excede o buffer');
  Result := TAMQPFieldTable.Create;
  try
    while FPos < LTableEnd do
    begin
      LName := ReadShortStr;
      Result.AddOrSetValue(LName, ReadFieldValue);
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TAMQPReader.BytesRemaining: Integer;
begin
  Result := FEnd - FPos;
end;

function TAMQPReader.EndOfData: Boolean;
begin
  Result := FPos >= FEnd;
end;

end.
