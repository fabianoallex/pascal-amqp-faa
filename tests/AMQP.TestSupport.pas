unit AMQP.TestSupport;

{ O que os testes precisam e o TAssert do kit (AMQP.DUnitXCompat, do lado
  Delphi; fpcunit.TAssert, do lado FPC) não tem. Os mestres DUnitX são
  escritos no dialeto do FPCUnit e o espelho FPC é gerado por
  tools/gen_fpc_mirror.py, que recusa qualquer API do DUnitX no corpo. O que
  difere de verdade entre os compiladores mora aqui, num arquivo só, que os
  dois runners compilam: esta unit não é gerada (o gerador só lê *Tests.pas).

  - ChecaLevanta: o AssertException do FPCUnit. Exige a classe EXATA (como
    ele, e diferente do Assert.WillRaise do DUnitX, que aceita subclasse).
  - IgnoraTeste: o Ignore do FPCUnit (marca IGNORED). O DUnitX desta versão
    não tem ignorado em tempo de execução: o teste sai pelo Assert.Pass com
    a mensagem 'IGNORADO: ...' e conta como Passed. O chamador sai com Exit
    logo depois, para o caso de nenhum dos dois interromper.
  - DirTemporario: o Delphi não tem GetTempDir; o FPC não tem System.IOUtils.
  - Pki: the TLS test PKI, pascal-common-faa's tests/pki through the
    submodule (CA, server, legitimate and rogue clients; README there). Not
    a compiler difference, but every runner needs it, server and acceptance
    alike. }

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

interface

uses
  SysUtils;

type
  TMetodoDeTeste = procedure of object;

procedure ChecaLevanta(AClasse: ExceptClass; AMetodo: TMetodoDeTeste); overload;
procedure ChecaLevanta(const AMensagem: string; AClasse: ExceptClass;
  AMetodo: TMetodoDeTeste); overload;
procedure IgnoraTeste(const AMotivo: string);
function DirTemporario: string;
/// Full path of a file in the test PKI (external/pascal-common-faa/tests/pki,
/// looked for upwards from the executable). Raises if it isn't there.
function Pki(const AName: string): string;
/// Password of every PFX in the test PKI.
const
  PKI_PFX_PASSWORD = 'pipestest';

implementation

uses
{$IFDEF FPC}
  fpcunit;
{$ELSE}
  IOUtils,
  DUnitX.TestFramework,
  AMQP.DUnitXCompat;
{$ENDIF}

procedure ChecaLevanta(const AMensagem: string; AClasse: ExceptClass;
  AMetodo: TMetodoDeTeste);
var
  LLevantou: Boolean;
  LClasse: string;
begin
  LLevantou := False;
  LClasse := '';
  try
    AMetodo;
  except
    on E: Exception do
    begin
      LLevantou := True;
      LClasse := E.ClassName;
    end;
  end;
  TAssert.AssertTrue(Trim(AMensagem + ' esperava ' + AClasse.ClassName +
    ', e nada foi levantado'), LLevantou);
  TAssert.AssertEquals(Trim(AMensagem + ' classe da exceção'), AClasse.ClassName,
    LClasse);
end;

procedure ChecaLevanta(AClasse: ExceptClass; AMetodo: TMetodoDeTeste);
begin
  ChecaLevanta('', AClasse, AMetodo);
end;

procedure IgnoraTeste(const AMotivo: string);
begin
{$IFDEF FPC}
  raise EIgnoredTest.Create(AMotivo);
{$ELSE}
  Assert.Pass('IGNORADO: ' + AMotivo);
{$ENDIF}
end;

function DirTemporario: string;
begin
{$IFDEF FPC}
  Result := GetTempDir;
{$ELSE}
  Result := TPath.GetTempPath;
{$ENDIF}
end;

var
  GPkiDir: string;

function Pki(const AName: string): string;
var
  LDir, LTry: string;
  I: Integer;
begin
  if GPkiDir = '' then
  begin
    LDir := ExtractFileDir(ExpandFileName(ParamStr(0)));
    for I := 0 to 8 do
    begin
      LTry := IncludeTrailingPathDelimiter(LDir) + 'external' + PathDelim +
        'pascal-common-faa' + PathDelim + 'tests' + PathDelim + 'pki';
      if FileExists(LTry + PathDelim + 'ca_cert.pem') then
      begin
        GPkiDir := LTry;
        Break;
      end;
      LDir := ExtractFileDir(LDir);
    end;
    if GPkiDir = '' then
      raise Exception.Create(
        'external/pascal-common-faa/tests/pki not found above ' + ParamStr(0));
  end;
  Result := IncludeTrailingPathDelimiter(GPkiDir) + AName;
end;

end.
