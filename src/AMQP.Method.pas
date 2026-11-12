unit AMQP.Method;

{$I amqp.inc}

{ AMQP 0-9-1 method header.

  The payload of a method frame (AMQP.Frame, type 1) starts with two big-endian
  u16 — class-id and method-id — followed by the method arguments,
  encoded with the primitive types of AMQP.Wire.

    +-----------+------------+ +----------------+
    | class-id  | method-id  | | arguments...   |
    | u16 (BE)  | u16 (BE)   | |                |
    +-----------+------------+ +----------------+

  This unit handles only the header (the two IDs). The arguments of each method
  live in the specific units (e.g. AMQP.Connection.Methods). }

interface

uses
  AMQP.Wire;

type
  { Identifies a method by the (class, method) pair. }
  TAMQPMethodId = record
    ClassId: Word;
    MethodId: Word;
    function Matches(AClassId, AMethodId: Word): Boolean;
  end;

/// Writes class-id + method-id to the writer (start of a method payload).
procedure WriteMethodHeader(const AWriter: TAMQPWriter;
  AClassId, AMethodId: Word);

/// Reads class-id + method-id from the reader (consumes 4 octets).
function ReadMethodHeader(const AReader: TAMQPReader): TAMQPMethodId;

/// Creates a writer with the method header already written, ready for the
/// arguments. The caller owns the writer (must free it).
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
