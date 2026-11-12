unit AMQP.Server.Records;

{$I amqp.inc}

{ What each WAL record carries -- broker sub-module, WS3 of Phase 4
  (durability, see CLAUDE.md, decisions D19-D28).

  WS1 (AMQP.Server.Wal) treats a record's payload as opaque bytes and the
  Kind field as a number with no meaning. This is where both gain meaning:
  this unit is the dictionary between "bytes in a file" and "a durable exchange was
  declared". It writes nothing and knows no journal -- it only encodes
  and decodes.

  WHY THE CODEC IS AMQP.WIRE'S, AND NOT A NEW ONE
  -----------------------------------------------
  The topology fields are the SAME ones that come over the wire: names are shortstr and
  the x-arguments are a field-table. AMQP.Wire already knows how to read and write both, and
  its field-table is the part of the lib with the most tests on top (121 unit
  assertions, including the round trip of nested tables and arrays that WS1 of
  Phase 3 fixed). Inventing a second format for the same data would create
  a second chance to get it wrong, with half the coverage.

  KIND NUMBERING IS PERMANENT
  ---------------------------
  A Kind written today will be read by a future version of the broker. A Kind
  number is NEVER reused: if a record goes out of use, its number stays
  retired. The 10-19 range is topology (WS3); 20-29 is for content and
  placement (WS4, D22).

  WHAT STAYS OUT OF THE RECORD, ON PURPOSE
  ----------------------------------------
  An EXCLUSIVE queue has no record at all (D20): it dies with its owning connection, and
  after a restart there is no owning connection. Note that the queue record
  does not even CARRY the `exclusive` flag -- so the format cannot express
  something that should not exist, and there is no way for a future path to write it
  by accident.

  OWNERSHIP
  ---------
  Decoding CREATES the Arguments TAMQPFieldTable (or returns nil if the table
  was empty); the caller owns it and must free it. Encoding takes ownership
  of nothing. }

interface

uses
  SysUtils,
  AMQP.Wire;

const
  // --- topology (WS3) -- range 10-19 --------------------------------------
  AMQP_REC_EXCHANGE_DECLARE = 10;
  AMQP_REC_EXCHANGE_DELETE  = 11;
  AMQP_REC_QUEUE_DECLARE    = 12;
  AMQP_REC_QUEUE_DELETE     = 13;
  AMQP_REC_QUEUE_BIND       = 14;
  AMQP_REC_QUEUE_UNBIND     = 15;
  AMQP_REC_EXCHANGE_BIND    = 16;
  AMQP_REC_EXCHANGE_UNBIND  = 17;

  // --- content and placement (WS4, decision D22) -- range 20-29 ----------
  /// The message BODY, written ONCE. N queues share it.
  AMQP_REC_CONTENT = 20;
  /// A PLACEMENT: this message, in this queue, with this header.
  AMQP_REC_ENQUEUE = 21;
  /// Retires a placement (ack, cap, expiry, purge, death).
  AMQP_REC_DEQUEUE = 22;

type
  EAMQPRecords = class(Exception);

  { Durable exchange declare. }
  TAMQPRecExchange = record
    VHost: string;
    Name: string;
    ExchangeType: string;
    /// Always True today (only the durable one is written, D20). Kept anyway
    /// so the file is readable without knowing the writer's rule.
    Durable: Boolean;
    AutoDelete: Boolean;
    Internal: Boolean;
    /// nil when there were no arguments. The DECODE creates it; the caller frees it.
    Arguments: TAMQPFieldTable;
  end;

  { Durable queue declare. No `exclusive`, on purpose -- see the header. }
  TAMQPRecQueue = record
    VHost: string;
    Name: string;
    Durable: Boolean;
    AutoDelete: Boolean;
    Arguments: TAMQPFieldTable;
  end;

  { Bind/unbind, of a queue or of an exchange. Destination is the destination queue
    or exchange; Source is always the source exchange. }
  TAMQPRecBinding = record
    VHost: string;
    Source: string;
    Destination: string;
    RoutingKey: string;
    Arguments: TAMQPFieldTable;
  end;

  { Delete: only the name. }
  TAMQPRecName = record
    VHost: string;
    Name: string;
  end;

  { The BODY, written once and shared by N placements (D22). Nothing here
    changes when the message is republished to a dead-letter exchange -- that is
    why the expensive part is never rewritten. }
  TAMQPRecContent = record
    ContentId: UInt64;
    /// Authenticated user of the publisher. Survives dead-lettering.
    UserId: string;
    Body: TBytes;
  end;

  { ONE PLACEMENT: this message, in this queue, with THIS header.

    Exchange, RoutingKey and HeaderPayload live here, not in the content, because
    dead-lettering changes all three -- it derives a new placement with the SAME
    ContentId (D22/D10).

    Time follows D21: no monotonic clock. The WALL-clock instant of the
    enqueue is written plus the effective TTL in ms, and recovery
    rebuilds the deadline. }
  TAMQPRecEnqueue = record
    VHost: string;
    Queue: string;
    EntryId: UInt64;
    ContentId: UInt64;
    Exchange: string;
    RoutingKey: string;
    Priority: Byte;
    /// ms since the epoch, UTC (AmqpWallMs).
    EnqueuedAtWall: Int64;
    /// Effective TTL in ms; -1 = no deadline.
    TtlMs: Int64;
    /// RAW content-header payload (decision D1).
    HeaderPayload: TBytes;
  end;

  { Retires a placement. Only the (queue, entry) pair -- the rest is already in the
    matching ENQ. }
  TAMQPRecDequeue = record
    VHost: string;
    Queue: string;
    EntryId: UInt64;
  end;

function AmqpEncodeRecExchange(const ARec: TAMQPRecExchange): TBytes;
function AmqpDecodeRecExchange(const APayload: TBytes): TAMQPRecExchange;

function AmqpEncodeRecQueue(const ARec: TAMQPRecQueue): TBytes;
function AmqpDecodeRecQueue(const APayload: TBytes): TAMQPRecQueue;

function AmqpEncodeRecBinding(const ARec: TAMQPRecBinding): TBytes;
function AmqpDecodeRecBinding(const APayload: TBytes): TAMQPRecBinding;

function AmqpEncodeRecName(const ARec: TAMQPRecName): TBytes;
function AmqpDecodeRecName(const APayload: TBytes): TAMQPRecName;

function AmqpEncodeRecContent(const ARec: TAMQPRecContent): TBytes;
function AmqpDecodeRecContent(const APayload: TBytes): TAMQPRecContent;

function AmqpEncodeRecEnqueue(const ARec: TAMQPRecEnqueue): TBytes;
function AmqpDecodeRecEnqueue(const APayload: TBytes): TAMQPRecEnqueue;

function AmqpEncodeRecDequeue(const ARec: TAMQPRecDequeue): TBytes;
function AmqpDecodeRecDequeue(const APayload: TBytes): TAMQPRecDequeue;

/// True if AKind is a content/placement record (range 20-29).
/// Recovery (WS6) uses it together with AmqpIsTopoRecord to separate the passes.
function AmqpIsDataRecord(AKind: Byte): Boolean;

/// True if AKind is one of this unit's topology records. Recovery
/// (WS6) uses it to separate the topology pass from the entries pass.
function AmqpIsTopoRecord(AKind: Byte): Boolean;

/// Readable name of the Kind, for error messages and diagnostics.
function AmqpRecordKindName(AKind: Byte): string;

implementation

function AmqpIsTopoRecord(AKind: Byte): Boolean;
begin
  Result := (AKind >= AMQP_REC_EXCHANGE_DECLARE)
    and (AKind <= AMQP_REC_EXCHANGE_UNBIND);
end;

function AmqpIsDataRecord(AKind: Byte): Boolean;
begin
  Result := (AKind >= AMQP_REC_CONTENT) and (AKind <= AMQP_REC_DEQUEUE);
end;

function AmqpRecordKindName(AKind: Byte): string;
begin
  case AKind of
    AMQP_REC_EXCHANGE_DECLARE: Result := 'exchange.declare';
    AMQP_REC_EXCHANGE_DELETE:  Result := 'exchange.delete';
    AMQP_REC_QUEUE_DECLARE:    Result := 'queue.declare';
    AMQP_REC_QUEUE_DELETE:     Result := 'queue.delete';
    AMQP_REC_QUEUE_BIND:       Result := 'queue.bind';
    AMQP_REC_QUEUE_UNBIND:     Result := 'queue.unbind';
    AMQP_REC_EXCHANGE_BIND:    Result := 'exchange.bind';
    AMQP_REC_EXCHANGE_UNBIND:  Result := 'exchange.unbind';
    AMQP_REC_CONTENT:          Result := 'content';
    AMQP_REC_ENQUEUE:          Result := 'enqueue';
    AMQP_REC_DEQUEUE:          Result := 'dequeue';
  else
    Result := Format('unknown(%d)', [AKind]);
  end;
end;

// The empty table is written as an empty table (four bytes of zero size) and
// comes back as nil -- so encoding nil and encoding a table with no keys give
// the SAME bytes, and there are not two ways of saying "no arguments" in the file.
procedure WriteArgs(AWriter: TAMQPWriter; ATable: TAMQPFieldTable);
var
  LEmpty: TAMQPFieldTable;
begin
  if ATable <> nil then
    AWriter.WriteFieldTable(ATable)
  else
  begin
    LEmpty := TAMQPFieldTable.Create;
    try
      AWriter.WriteFieldTable(LEmpty);
    finally
      LEmpty.Free;
    end;
  end;
end;

function ReadArgs(AReader: TAMQPReader): TAMQPFieldTable;
begin
  Result := AReader.ReadFieldTable;
  if (Result <> nil) and (Result.Count = 0) then
    FreeAndNil(Result);
end;

{ exchange declare }

function AmqpEncodeRecExchange(const ARec: TAMQPRecExchange): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Name);
    LW.WriteShortStr(ARec.ExchangeType);
    LW.WriteBit(ARec.Durable);
    LW.WriteBit(ARec.AutoDelete);
    LW.WriteBit(ARec.Internal);
    WriteArgs(LW, ARec.Arguments);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecExchange(const APayload: TBytes): TAMQPRecExchange;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Name := LR.ReadShortStr;
    Result.ExchangeType := LR.ReadShortStr;
    Result.Durable := LR.ReadBit;
    Result.AutoDelete := LR.ReadBit;
    Result.Internal := LR.ReadBit;
    Result.Arguments := ReadArgs(LR);
  finally
    LR.Free;
  end;
end;

{ queue declare }

function AmqpEncodeRecQueue(const ARec: TAMQPRecQueue): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Name);
    LW.WriteBit(ARec.Durable);
    LW.WriteBit(ARec.AutoDelete);
    WriteArgs(LW, ARec.Arguments);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecQueue(const APayload: TBytes): TAMQPRecQueue;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Name := LR.ReadShortStr;
    Result.Durable := LR.ReadBit;
    Result.AutoDelete := LR.ReadBit;
    Result.Arguments := ReadArgs(LR);
  finally
    LR.Free;
  end;
end;

{ bind / unbind }

function AmqpEncodeRecBinding(const ARec: TAMQPRecBinding): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Source);
    LW.WriteShortStr(ARec.Destination);
    LW.WriteShortStr(ARec.RoutingKey);
    WriteArgs(LW, ARec.Arguments);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecBinding(const APayload: TBytes): TAMQPRecBinding;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Source := LR.ReadShortStr;
    Result.Destination := LR.ReadShortStr;
    Result.RoutingKey := LR.ReadShortStr;
    Result.Arguments := ReadArgs(LR);
  finally
    LR.Free;
  end;
end;

{ delete }

function AmqpEncodeRecName(const ARec: TAMQPRecName): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Name);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecName(const APayload: TBytes): TAMQPRecName;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Name := LR.ReadShortStr;
  finally
    LR.Free;
  end;
end;


{ binary blob: 32-bit length + raw bytes.

  WriteLongStr can NOT be used here: it treats the value as TEXT and goes through
  AmqpUtf8Encode. A message body and a content header are BINARY -- any
  transcoding would silently destroy them. }
procedure WriteBlob(AWriter: TAMQPWriter; const ABytes: TBytes);
begin
  AWriter.WriteLongUInt(Cardinal(Length(ABytes)));
  if Length(ABytes) > 0 then
    AWriter.WriteRaw(ABytes);
end;

function ReadBlob(AReader: TAMQPReader): TBytes;
var
  LLen: Cardinal;
begin
  Result := nil;
  LLen := AReader.ReadLongUInt;
  if LLen = 0 then
    Exit;
  Result := AReader.ReadRaw(Integer(LLen));
end;

{ content }

function AmqpEncodeRecContent(const ARec: TAMQPRecContent): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteLongLongUInt(ARec.ContentId);
    LW.WriteShortStr(ARec.UserId);
    WriteBlob(LW, ARec.Body);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecContent(const APayload: TBytes): TAMQPRecContent;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.ContentId := LR.ReadLongLongUInt;
    Result.UserId := LR.ReadShortStr;
    Result.Body := ReadBlob(LR);
  finally
    LR.Free;
  end;
end;

{ placement }

function AmqpEncodeRecEnqueue(const ARec: TAMQPRecEnqueue): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Queue);
    LW.WriteLongLongUInt(ARec.EntryId);
    LW.WriteLongLongUInt(ARec.ContentId);
    LW.WriteShortStr(ARec.Exchange);
    LW.WriteShortStr(ARec.RoutingKey);
    LW.WriteOctet(ARec.Priority);
    LW.WriteLongLongUInt(UInt64(ARec.EnqueuedAtWall));
    LW.WriteLongLongUInt(UInt64(ARec.TtlMs));
    WriteBlob(LW, ARec.HeaderPayload);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecEnqueue(const APayload: TBytes): TAMQPRecEnqueue;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Queue := LR.ReadShortStr;
    Result.EntryId := LR.ReadLongLongUInt;
    Result.ContentId := LR.ReadLongLongUInt;
    Result.Exchange := LR.ReadShortStr;
    Result.RoutingKey := LR.ReadShortStr;
    Result.Priority := LR.ReadOctet;
    Result.EnqueuedAtWall := Int64(LR.ReadLongLongUInt);
    Result.TtlMs := Int64(LR.ReadLongLongUInt);
    Result.HeaderPayload := ReadBlob(LR);
  finally
    LR.Free;
  end;
end;

{ retirement }

function AmqpEncodeRecDequeue(const ARec: TAMQPRecDequeue): TBytes;
var
  LW: TAMQPWriter;
begin
  LW := TAMQPWriter.Create;
  try
    LW.WriteShortStr(ARec.VHost);
    LW.WriteShortStr(ARec.Queue);
    LW.WriteLongLongUInt(ARec.EntryId);
    Result := LW.ToBytes;
  finally
    LW.Free;
  end;
end;

function AmqpDecodeRecDequeue(const APayload: TBytes): TAMQPRecDequeue;
var
  LR: TAMQPReader;
begin
  LR := TAMQPReader.Create(APayload);
  try
    Result.VHost := LR.ReadShortStr;
    Result.Queue := LR.ReadShortStr;
    Result.EntryId := LR.ReadLongLongUInt;
  finally
    LR.Free;
  end;
end;

end.
