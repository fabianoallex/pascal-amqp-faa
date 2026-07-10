unit AMQP.Method;

{$I amqp.inc}

{ Cabeçalho de método AMQP 0-9-1.

  O payload de um frame de método (AMQP.Frame, tipo 1) começa com dois u16 em
  big-endian — class-id e method-id — seguidos dos argumentos do método,
  codificados com os tipos primitivos de AMQP.Wire.

    +-----------+------------+ +----------------+
    | class-id  | method-id  | | argumentos...  |
    | u16 (BE)  | u16 (BE)   | |                |
    +-----------+------------+ +----------------+

  Esta unit trata só do cabeçalho (os dois IDs). Os argumentos de cada método
  ficam nas units específicas (ex.: AMQP.Connection.Methods). }

interface

uses
  AMQP.Wire;

type
  { Identifica um método pelo par (classe, método). }
  TAMQPMethodId = record
    ClassId: Word;
    MethodId: Word;
    function Matches(AClassId, AMethodId: Word): Boolean;
  end;

/// Escreve class-id + method-id no writer (início do payload de um método).
procedure WriteMethodHeader(const AWriter: TAMQPWriter;
  AClassId, AMethodId: Word);

/// Lê class-id + method-id do reader (consome 4 octetos).
function ReadMethodHeader(const AReader: TAMQPReader): TAMQPMethodId;

/// Cria um writer já com o cabeçalho do método escrito, pronto para os
/// argumentos. O chamador é dono do writer (deve liberá-lo).
function BeginMethod(AClassId, AMethodId: Word): TAMQPWriter;

implementation

{ TAMQPMethodId }

function TAMQPMethodId.Matches(AClassId, AMethodId: Word): Boolean;
begin
  Result := (ClassId = AClassId) and (MethodId = AMethodId);
end;

procedure WriteMethodHeader(const AWriter: TAMQPWriter;
  AClassId, AMethodId: Word);
begin
  AWriter.WriteShortUInt(AClassId);
  AWriter.WriteShortUInt(AMethodId);
end;

function ReadMethodHeader(const AReader: TAMQPReader): TAMQPMethodId;
begin
  Result.ClassId := AReader.ReadShortUInt;
  Result.MethodId := AReader.ReadShortUInt;
end;

function BeginMethod(AClassId, AMethodId: Word): TAMQPWriter;
begin
  Result := TAMQPWriter.Create;
  WriteMethodHeader(Result, AClassId, AMethodId);
end;

end.
