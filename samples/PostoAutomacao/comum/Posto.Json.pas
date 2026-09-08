unit Posto.Json;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Codec JSON minimo (DOM), um fonte para os dois compiladores.

  Existe porque fpjson (FPC) e System.JSON (Delphi) divergem em API e em
  detalhe de tipo, e o sample precisa de UM contrato de serializacao. Cobre
  o que os payloads do posto usam: objeto, array, string, numero, booleano,
  null. Nao pretende ser RFC 8259 completo -- sem numeros em notacao
  estranha, sem chaves duplicadas.

  Numeros sao guardados como Double (todos os valores do dominio -- versao,
  epoch ms, litros, precos -- cabem exatos abaixo de 2^53). A saida usa '.'
  como separador decimal SEMPRE (JsonFormatSettings), independente do locale.

  Posse: um TJsonValue e' dono dos filhos. Liberar a raiz libera a arvore. }

interface

uses
  SysUtils, Math, Generics.Collections;

type
  EPostoJson = class(Exception);

  TJsonKind = (jkNull, jkBool, jkNumber, jkString, jkArray, jkObject);

  TJsonValue = class
  private
    FChildren: TObjectList<TJsonValue>;
    function Children: TObjectList<TJsonValue>;
    procedure AppendTo(var ABuf: string);
  public
    Name: string;      // chave, quando este valor e' membro de um objeto
    Kind: TJsonKind;
    BoolVal: Boolean;
    NumVal: Double;
    StrVal: string;

    constructor Create(AKind: TJsonKind);
    destructor Destroy; override;

    class function NewObject: TJsonValue; static;
    class function NewArray: TJsonValue; static;
    class function Parse(const AText: string): TJsonValue; static;

    { --- construcao (objeto) --- }
    procedure Put(const AKey, AValue: string); overload;
    procedure Put(const AKey: string; const AValue: Double); overload;
    procedure Put(const AKey: string; const AValue: Boolean); overload;
    procedure PutInt(const AKey: string; const AValue: Int64);
    procedure PutRaw(const AKey: string; AValue: TJsonValue); // assume posse
    function PutObject(const AKey: string): TJsonValue;
    function PutArray(const AKey: string): TJsonValue;

    { --- construcao (array) --- }
    procedure AddItem(AValue: TJsonValue);   // assume posse
    function AddItemObject: TJsonValue;

    { --- leitura --- }
    function Has(const AKey: string): Boolean;
    function Get(const AKey: string): TJsonValue;   // nil se ausente
    function AsStr(const AKey: string; const ADef: string = ''): string;
    function AsNum(const AKey: string; const ADef: Double = 0): Double;
    function AsInt(const AKey: string; const ADef: Int64 = 0): Int64;
    function AsBool(const AKey: string; const ADef: Boolean = False): Boolean;
    function Count: Integer;                 // nº de itens (array ou objeto)
    function Item(AIndex: Integer): TJsonValue;

    function ToJson: string;
  end;

{ TFormatSettings com ponto decimal, para FloatToStr/StrToFloat locale-safe. }
function JsonFormatSettings: TFormatSettings;

implementation

var
  GFmt: TFormatSettings;

function JsonFormatSettings: TFormatSettings;
begin
  Result := GFmt;
end;

{ ----------------------------------------------------------------- saida --- }

procedure AppendCp(var S: string; ACp: Cardinal);
{$IFDEF FPC}
begin
  // string do FPC (mode delphi) e' AnsiString -- emitir bytes UTF-8.
  if ACp < $80 then
    S := S + Chr(ACp)
  else if ACp < $800 then
    S := S + Chr($C0 or (ACp shr 6)) + Chr($80 or (ACp and $3F))
  else if ACp < $10000 then
    S := S + Chr($E0 or (ACp shr 12)) + Chr($80 or ((ACp shr 6) and $3F)) +
      Chr($80 or (ACp and $3F))
  else
    S := S + Chr($F0 or (ACp shr 18)) + Chr($80 or ((ACp shr 12) and $3F)) +
      Chr($80 or ((ACp shr 6) and $3F)) + Chr($80 or (ACp and $3F));
end;
{$ELSE}
begin
  // string do Delphi e' UTF-16.
  if ACp < $10000 then
    S := S + Char(ACp)
  else
  begin
    ACp := ACp - $10000;
    S := S + Char($D800 or (ACp shr 10)) + Char($DC00 or (ACp and $3FF));
  end;
end;
{$ENDIF}

function EscapeStr(const S: string): string;
var
  I: Integer;
  C: Char;
begin
  Result := '"';
  for I := 1 to Length(S) do
  begin
    C := S[I];
    case C of
      '"':  Result := Result + '\"';
      '\':  Result := Result + '\\';
      #8:   Result := Result + '\b';
      #9:   Result := Result + '\t';
      #10:  Result := Result + '\n';
      #12:  Result := Result + '\f';
      #13:  Result := Result + '\r';
    else
      if C < ' ' then
        Result := Result + '\u' + LowerCase(IntToHex(Ord(C), 4))
      else
        Result := Result + C;   // UTF-8 (FPC) / UTF-16 (Delphi) passa direto
    end;
  end;
  Result := Result + '"';
end;

function NumToJson(const AValue: Double): string;
begin
  if IsNan(AValue) or IsInfinite(AValue) then
    Exit('0');
  if (Frac(AValue) = 0) and (Abs(AValue) < 1e15) then
    Result := IntToStr(Round(AValue))
  else
    Result := FloatToStr(AValue, JsonFormatSettings);
end;

procedure TJsonValue.AppendTo(var ABuf: string);
var
  I: Integer;
begin
  case Kind of
    jkNull:   ABuf := ABuf + 'null';
    jkBool:   if BoolVal then ABuf := ABuf + 'true' else ABuf := ABuf + 'false';
    jkNumber: ABuf := ABuf + NumToJson(NumVal);
    jkString: ABuf := ABuf + EscapeStr(StrVal);
    jkArray:
      begin
        ABuf := ABuf + '[';
        if FChildren <> nil then
          for I := 0 to FChildren.Count - 1 do
          begin
            if I > 0 then ABuf := ABuf + ',';
            FChildren[I].AppendTo(ABuf);
          end;
        ABuf := ABuf + ']';
      end;
    jkObject:
      begin
        ABuf := ABuf + '{';
        if FChildren <> nil then
          for I := 0 to FChildren.Count - 1 do
          begin
            if I > 0 then ABuf := ABuf + ',';
            ABuf := ABuf + EscapeStr(FChildren[I].Name) + ':';
            FChildren[I].AppendTo(ABuf);
          end;
        ABuf := ABuf + '}';
      end;
  end;
end;

function TJsonValue.ToJson: string;
begin
  Result := '';
  AppendTo(Result);
end;

{ --------------------------------------------------------------- parsing --- }

type
  TJsonParser = record
    Txt: string;
    Pos: Integer;
    procedure Fail(const AMsg: string);
    procedure SkipWs;
    function Peek: Char;
    function Next: Char;
    function ParseValue: TJsonValue;
    function ParseString: string;
    function ParseNumber: TJsonValue;
    function ParseKeyword(const AWord: string; AKind: TJsonKind;
      ABool: Boolean): TJsonValue;
    function ParseArray: TJsonValue;
    function ParseObject: TJsonValue;
  end;

procedure TJsonParser.Fail(const AMsg: string);
begin
  raise EPostoJson.CreateFmt('JSON invalido na posicao %d: %s', [Pos, AMsg]);
end;

procedure TJsonParser.SkipWs;
begin
  while (Pos <= Length(Txt)) and (Txt[Pos] <= ' ') do
    Inc(Pos);
end;

function TJsonParser.Peek: Char;
begin
  if Pos <= Length(Txt) then
    Result := Txt[Pos]
  else
    Result := #0;
end;

function TJsonParser.Next: Char;
begin
  Result := Peek;
  Inc(Pos);
end;

function TJsonParser.ParseString: string;
var
  C: Char;
  LHex: string;
  LCp, LLow: Cardinal;
begin
  Result := '';
  if Next <> '"' then Fail('esperava "');
  while True do
  begin
    if Pos > Length(Txt) then Fail('string sem fechamento');
    C := Next;
    if C = '"' then
      Break
    else if C = '\' then
    begin
      C := Next;
      case C of
        '"': Result := Result + '"';
        '\': Result := Result + '\';
        '/': Result := Result + '/';
        'b': Result := Result + #8;
        'f': Result := Result + #12;
        'n': Result := Result + #10;
        'r': Result := Result + #13;
        't': Result := Result + #9;
        'u':
          begin
            LHex := Copy(Txt, Pos, 4);
            if Length(LHex) < 4 then Fail('\u incompleto');
            Inc(Pos, 4);
            LCp := StrToInt('$' + LHex);
            if (LCp >= $D800) and (LCp <= $DBFF) and
               (Copy(Txt, Pos, 2) = '\u') then
            begin
              LLow := StrToInt('$' + Copy(Txt, Pos + 2, 4));
              if (LLow >= $DC00) and (LLow <= $DFFF) then
              begin
                Inc(Pos, 6);
                LCp := $10000 + ((LCp - $D800) shl 10) + (LLow - $DC00);
              end;
            end;
            AppendCp(Result, LCp);
          end;
      else
        Fail('escape desconhecido \' + C);
      end;
    end
    else
      Result := Result + C;
  end;
end;

function TJsonParser.ParseNumber: TJsonValue;
var
  LStart: Integer;
  LTok: string;
begin
  LStart := Pos;
  if Peek = '-' then Inc(Pos);
  while (Pos <= Length(Txt)) and CharInSet(Txt[Pos], ['0'..'9']) do Inc(Pos);
  if Peek = '.' then
  begin
    Inc(Pos);
    while (Pos <= Length(Txt)) and CharInSet(Txt[Pos], ['0'..'9']) do Inc(Pos);
  end;
  if CharInSet(Peek, ['e', 'E']) then
  begin
    Inc(Pos);
    if CharInSet(Peek, ['+', '-']) then Inc(Pos);
    while (Pos <= Length(Txt)) and CharInSet(Txt[Pos], ['0'..'9']) do Inc(Pos);
  end;
  LTok := Copy(Txt, LStart, Pos - LStart);
  Result := TJsonValue.Create(jkNumber);
  try
    Result.NumVal := StrToFloat(LTok, JsonFormatSettings);
  except
    Result.Free;
    Fail('numero invalido "' + LTok + '"');
  end;
end;

function TJsonParser.ParseKeyword(const AWord: string; AKind: TJsonKind;
  ABool: Boolean): TJsonValue;
begin
  if Copy(Txt, Pos, Length(AWord)) <> AWord then
    Fail('esperava ' + AWord);
  Inc(Pos, Length(AWord));
  Result := TJsonValue.Create(AKind);
  Result.BoolVal := ABool;
end;

function TJsonParser.ParseArray: TJsonValue;
begin
  Result := TJsonValue.Create(jkArray);
  Inc(Pos); // '['
  SkipWs;
  if Peek = ']' then begin Inc(Pos); Exit; end;
  while True do
  begin
    SkipWs;
    Result.Children.Add(ParseValue);
    SkipWs;
    case Next of
      ',': Continue;
      ']': Break;
    else
      Result.Free;
      Fail('esperava , ou ]');
    end;
  end;
end;

function TJsonParser.ParseObject: TJsonValue;
var
  LKey: string;
  LChild: TJsonValue;
begin
  Result := TJsonValue.Create(jkObject);
  Inc(Pos); // '{'
  SkipWs;
  if Peek = '}' then begin Inc(Pos); Exit; end;
  while True do
  begin
    SkipWs;
    if Peek <> '"' then begin Result.Free; Fail('esperava chave'); end;
    LKey := ParseString;
    SkipWs;
    if Next <> ':' then begin Result.Free; Fail('esperava :'); end;
    SkipWs;
    LChild := ParseValue;
    LChild.Name := LKey;
    Result.Children.Add(LChild);
    SkipWs;
    case Next of
      ',': Continue;
      '}': Break;
    else
      Result.Free;
      Fail('esperava , ou }');
    end;
  end;
end;

function TJsonParser.ParseValue: TJsonValue;
begin
  SkipWs;
  case Peek of
    '{': Result := ParseObject;
    '[': Result := ParseArray;
    '"':
      begin
        Result := TJsonValue.Create(jkString);
        Result.StrVal := ParseString;
      end;
    't': Result := ParseKeyword('true', jkBool, True);
    'f': Result := ParseKeyword('false', jkBool, False);
    'n': Result := ParseKeyword('null', jkNull, False);
    '-', '0'..'9': Result := ParseNumber;
  else
    Fail('valor inesperado');
    Result := nil;
  end;
end;

{ --------------------------------------------------------------- TJsonValue - }

constructor TJsonValue.Create(AKind: TJsonKind);
begin
  inherited Create;
  Kind := AKind;
end;

destructor TJsonValue.Destroy;
begin
  FChildren.Free;
  inherited Destroy;
end;

function TJsonValue.Children: TObjectList<TJsonValue>;
begin
  if FChildren = nil then
    FChildren := TObjectList<TJsonValue>.Create(True);
  Result := FChildren;
end;

class function TJsonValue.NewObject: TJsonValue;
begin
  Result := TJsonValue.Create(jkObject);
end;

class function TJsonValue.NewArray: TJsonValue;
begin
  Result := TJsonValue.Create(jkArray);
end;

class function TJsonValue.Parse(const AText: string): TJsonValue;
var
  LP: TJsonParser;
begin
  LP.Txt := AText;
  LP.Pos := 1;
  Result := LP.ParseValue;
  LP.SkipWs;
  if LP.Pos <= Length(AText) then
  begin
    Result.Free;
    raise EPostoJson.CreateFmt('JSON com lixo apos a posicao %d', [LP.Pos]);
  end;
end;

procedure TJsonValue.Put(const AKey, AValue: string);
var
  LV: TJsonValue;
begin
  LV := TJsonValue.Create(jkString);
  LV.Name := AKey;
  LV.StrVal := AValue;
  Children.Add(LV);
end;

procedure TJsonValue.Put(const AKey: string; const AValue: Double);
var
  LV: TJsonValue;
begin
  LV := TJsonValue.Create(jkNumber);
  LV.Name := AKey;
  LV.NumVal := AValue;
  Children.Add(LV);
end;

procedure TJsonValue.Put(const AKey: string; const AValue: Boolean);
var
  LV: TJsonValue;
begin
  LV := TJsonValue.Create(jkBool);
  LV.Name := AKey;
  LV.BoolVal := AValue;
  Children.Add(LV);
end;

procedure TJsonValue.PutInt(const AKey: string; const AValue: Int64);
var
  LV: TJsonValue;
begin
  LV := TJsonValue.Create(jkNumber);
  LV.Name := AKey;
  LV.NumVal := AValue;
  Children.Add(LV);
end;

procedure TJsonValue.PutRaw(const AKey: string; AValue: TJsonValue);
begin
  AValue.Name := AKey;
  Children.Add(AValue);
end;

function TJsonValue.PutObject(const AKey: string): TJsonValue;
begin
  Result := TJsonValue.Create(jkObject);
  Result.Name := AKey;
  Children.Add(Result);
end;

function TJsonValue.PutArray(const AKey: string): TJsonValue;
begin
  Result := TJsonValue.Create(jkArray);
  Result.Name := AKey;
  Children.Add(Result);
end;

procedure TJsonValue.AddItem(AValue: TJsonValue);
begin
  Children.Add(AValue);
end;

function TJsonValue.AddItemObject: TJsonValue;
begin
  Result := TJsonValue.Create(jkObject);
  Children.Add(Result);
end;

function TJsonValue.Get(const AKey: string): TJsonValue;
var
  I: Integer;
begin
  if FChildren <> nil then
    for I := 0 to FChildren.Count - 1 do
      if FChildren[I].Name = AKey then
        Exit(FChildren[I]);
  Result := nil;
end;

function TJsonValue.Has(const AKey: string): Boolean;
begin
  Result := Get(AKey) <> nil;
end;

function TJsonValue.AsStr(const AKey: string; const ADef: string): string;
var
  LV: TJsonValue;
begin
  LV := Get(AKey);
  if (LV <> nil) and (LV.Kind = jkString) then
    Result := LV.StrVal
  else
    Result := ADef;
end;

function TJsonValue.AsNum(const AKey: string; const ADef: Double): Double;
var
  LV: TJsonValue;
begin
  LV := Get(AKey);
  if (LV <> nil) and (LV.Kind = jkNumber) then
    Result := LV.NumVal
  else
    Result := ADef;
end;

function TJsonValue.AsInt(const AKey: string; const ADef: Int64): Int64;
var
  LV: TJsonValue;
begin
  LV := Get(AKey);
  if (LV <> nil) and (LV.Kind = jkNumber) then
    Result := Round(LV.NumVal)
  else
    Result := ADef;
end;

function TJsonValue.AsBool(const AKey: string; const ADef: Boolean): Boolean;
var
  LV: TJsonValue;
begin
  LV := Get(AKey);
  if (LV <> nil) and (LV.Kind = jkBool) then
    Result := LV.BoolVal
  else
    Result := ADef;
end;

function TJsonValue.Count: Integer;
begin
  if FChildren <> nil then
    Result := FChildren.Count
  else
    Result := 0;
end;

function TJsonValue.Item(AIndex: Integer): TJsonValue;
begin
  Result := FChildren[AIndex];
end;

initialization
  {$IFDEF FPC}
  GFmt := DefaultFormatSettings;
  {$ELSE}
  GFmt := TFormatSettings.Create;
  {$ENDIF}
  GFmt.DecimalSeparator := '.';
  GFmt.ThousandSeparator := #0;

end.
