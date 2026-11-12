unit AMQP.Server.Header;

{$I amqp.inc}

{ Content-header rewriting -- broker sub-module, WS3 of Phase 3 (message
  lifecycle, see CLAUDE.md).

  WHY THIS EXISTS, AND WHY IT DOES NOT VIOLATE D1
  -----------------------------------------------
  Decision D1 (Phase 2) made TAMQPMessage carry the RAW content-header
  payload instead of a clone of the TAMQPFieldTable: faithful byte-by-byte
  re-emission to the consumer, and none of FPC 3.2's TValue-in-TValue problem when
  cloning nested tables/arrays. Dead-lettering (WS5) needs the opposite --
  touching the header, to add or extend x-death.

  The point that undoes the knot: D1 forbids cloning on the HOT path (every publish,
  every delivery), not on the cold one. A message DIES ONCE. And TAMQPMessage is
  already immutable by a Phase 2 invariant ("nobody mutates the message after it
  is created"), so the rewrite mutates nothing: it DERIVES a new message, with a
  rebuilt HeaderPayload and THE SAME Body by reference. The expensive part (the body)
  is never copied; the cheap part (the header) is rebuilt only when it actually changes.

  FIDELITY, MEASURED AND NOT ASSUMED
  ----------------------------------
  The DecodeContentHeader -> BuildContentHeader round trip returns the SAME
  bytes: checked before this code existed and covered by this workstream's tests
  over all FOURTEEN properties of the Basic class, not over a sample.

  KNOWN LIMITATION, deliberate: DecodeContentHeader consumes and DROPS the
  property-flags continuation words (bit 0 of each word chains the
  next). No property of the Basic class lives beyond the first word,
  so in practice this is theoretical -- but if a peer sends a continuation, the
  rewrite silently erases it. A message that NEVER goes through here stays
  intact, because normal delivery re-emits the raw payload without touching it.

  NO CALLBACK, ON PURPOSE
  -----------------------
  The obvious way would be "rewrite this payload applying this function to the table",
  but FPC 3.2 has no anonymous methods and a `procedure of object` would force
  the caller to create a class just to carry the mutation state (see the
  forbidden table in CLAUDE.md). Instead, an explicit EDITOR:
  construct (decodes) -> touch the live table -> BuildPayload (re-encodes). }

interface

uses
  SysUtils,
  DateUtils,
  Rtti,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Server.Message;

const
  /// Dead-letter reasons this broker produces (RabbitMQ's de facto spec).
  /// 'delivery_limit' is left out: it belongs to quorum queues, which do not exist here.
  AMQP_DEATH_REJECTED = 'rejected'; // Basic.Nack/Reject with requeue=false
  AMQP_DEATH_EXPIRED  = 'expired';  // TTL (queue's or message's)
  AMQP_DEATH_MAXLEN   = 'maxlen';   // exceeded the message cap

type
  { Editor of an already serialized content header.

    Usage cycle:
      LEd := TAMQPHeaderEditor.Create(AMsg.HeaderPayload);
      try
        LEd.Headers.Put('x-death', ...);      // LIVE table, change it freely
        LNovo := LEd.BuildPayload(AMsg.BodySize);
      finally
        LEd.Free;
      end;

    Ownership: the editor owns the decoded headers table and frees it in the
    destructor. The TBytes returned by BuildPayload belongs to the caller. The input
    payload is NOT modified -- it is only read. }
  TAMQPHeaderEditor = class
  private
    FProps: TAMQPBasicProperties;
    FBodySize: UInt64;
    FHadHeaders: Boolean;
  public
    /// Decodes APayload (the raw content header, as it came in the frame).
    /// Raises EAMQPWire if the payload is malformed.
    constructor Create(const APayload: TBytes);
    destructor Destroy; override;

    /// LIVE headers table, to read and change. Created empty on demand if the
    /// message did not have the property -- asking for the table already declares
    /// the intent to change it, and without that there would be no way to add x-death
    /// to a message published without headers. It still belongs to the EDITOR: do not
    /// free it, do not keep it after the destructor.
    function Headers: TAMQPFieldTable;
    /// True if the ORIGINAL message carried the headers property -- useful for
    /// callers that want to avoid adding an empty table needlessly.
    function HadHeaders: Boolean;

    /// Re-encodes the header with the changes. ABodySize is EXPLICIT on purpose:
    /// deriving a message with a different body and forgetting to adjust the
    /// body-size would make the consumer wait for body frames that never arrive.
    /// Callers that only share the body should use AmqpDeriveMessage below, which
    /// fills this in by itself and cannot get it wrong.
    function BuildPayload(ABodySize: UInt64): TBytes;

    /// Removes the 'expiration' property and returns the value it had
    /// ('' if there was none). It exists for dead-lettering: a message that
    /// died BY TTL cannot carry the same deadline to the DLQ, or it dies
    /// again on arrival and the retry-with-delay pattern does not work. The old value
    /// becomes 'original-expiration' in the x-death entry.
    function TakeExpiration: string;

    /// Body-size read from the original payload.
    property BodySizeOriginal: UInt64 read FBodySize;
    /// Decoded properties (read-only). The headers table inside them
    /// is the same one Headers returns.
    property Properties: TAMQPBasicProperties read FProps;
  end;

/// Sum of the 'count' fields of ALL x-death entries -- that is,
/// how many dead-letter hops the message has already made. It is the counter of
/// the cycle guard (decision D14): the DLX graph may have a cycle, and the cap by
/// DEPTH is cheaper than RabbitMQ's set of visited queues,
/// with the same practical effect in an in-memory engine.
/// A nil table, absent x-death or malformed x-death return 0.
function AmqpDeathHops(AHeaders: TAMQPFieldTable): Int64;

/// Adds a death to the x-death of the editor's LIVE table.
///
/// COLLAPSES by (queue, reason): if an entry with the same pair already exists, it only
/// increments 'count' and updates 'time'. Without this a retry loop inflates the
/// header on every round until it exceeds the frame-max -- that is why the spec
/// has the count field. A new entry goes to the FRONT of the array (most recent
/// first), as RabbitMQ does.
///
/// AOriginalExpiration: when the message carried 'expiration', the value goes
/// into the entry and the PROPERTY IS REMOVED from the header (the caller already took
/// it out with TakeExpiration). Without this the message would die again on arrival
/// at the DLQ, and the retry-with-delay pattern would not work. Pass '' if there was none.
procedure AmqpAddDeath(AEditor: TAMQPHeaderEditor;
  const AQueue, AReason, AExchange, ARoutingKey, AOriginalExpiration: string);

/// Derives a NEW message from ASource, swapping only the header.
///
/// The Body is SHARED by reference (normal dynamic array semantics):
/// nothing is copied, and since TAMQPMessage is immutable, sharing is safe.
/// The new header's body-size comes from the real body, so they cannot diverge.
///
/// The returned message is born with RefCount=1 and belongs to the caller (Release,
/// never Free). ASource is NOT touched nor freed.
///
/// AExchange/ARoutingKey change the destination -- it is what dead-lettering needs
/// (the dead message is republished to the DLX with another routing key).
function AmqpDeriveMessage(ASource: TAMQPMessage;
  const ANewHeaderPayload: TBytes;
  const AExchange, ARoutingKey: string): TAMQPMessage;

implementation

{ TAMQPHeaderEditor }

constructor TAMQPHeaderEditor.Create(const APayload: TBytes);
var
  LReader: TAMQPReader;
  LHeader: TAMQPContentHeader;
begin
  inherited Create;
  LReader := TAMQPReader.Create(APayload);
  try
    LHeader := DecodeContentHeader(LReader);
  finally
    LReader.Free;
  end;
  FProps := LHeader.Properties;
  FBodySize := LHeader.BodySize;
  FHadHeaders := FProps.Has(bpHeaders) and (FProps.Headers <> nil);
end;

destructor TAMQPHeaderEditor.Destroy;
begin
  // The decode hands the table to the caller; here the owner is the editor.
  if FProps.Has(bpHeaders) then
    FProps.Headers.Free;
  inherited;
end;

function TAMQPHeaderEditor.Headers: TAMQPFieldTable;
begin
  if not (FProps.Has(bpHeaders) and (FProps.Headers <> nil)) then
    // SetHeaders fills the value and sets the flag together.
    FProps.SetHeaders(TAMQPFieldTable.Create);
  Result := FProps.Headers;
end;

function TAMQPHeaderEditor.HadHeaders: Boolean;
begin
  Result := FHadHeaders;
end;

function TAMQPHeaderEditor.BuildPayload(ABodySize: UInt64): TBytes;
begin
  Result := BuildContentHeader(ABodySize, FProps);
end;

function TAMQPHeaderEditor.TakeExpiration: string;
begin
  Result := '';
  if not FProps.Has(bpExpiration) then
    Exit;
  Result := FProps.Expiration;
  Exclude(FProps.Flags, bpExpiration);
  FProps.Expiration := '';
end;

{ --- x-death --------------------------------------------------------------

  Shape of the header, as RabbitMQ defines it and clients expect it:

    x-death = array of TABLES, each one with the fields
       count               long
       reason              longstr   (rejected | expired | maxlen)
       queue               longstr
       time                timestamp
       exchange            longstr
       routing-keys        array of longstr
       original-expiration longstr, only when there was 'expiration'

  CAREFUL when writing this prose: a closing brace inside a block comment
  CLOSES THE COMMENT -- that is why the fields go in a list, and not in brace
  notation. It is the same gotcha CLAUDE.md already records for directives quoted
  in comments, and it bit here on the first writing.

  Array of TABLES -- exactly the shape the codec only learned to
  WRITE in WS1 of this phase. Before that, this would raise EAMQPWire. }

// Reads x-death as an array, already unwrapped. nil if absent/malformed.
// AmqpUnwrapValue is mandatory on EVERY GetArrayElement (the TValue gotcha,
// on both compilers: an element of TArray<TValue> comes back re-wrapped).
function ReadDeaths(AHeaders: TAMQPFieldTable; out AArr: TValue): Boolean;
begin
  AArr := TValue.Empty;
  Result := (AHeaders <> nil) and AHeaders.TryGetValue('x-death', AArr);
  if not Result then
    Exit;
  AArr := AmqpUnwrapValue(AArr);
  Result := AArr.IsArray;
end;

function EntryOf(const AArr: TValue; AIdx: Integer): TAMQPFieldTable;
var
  LElem: TValue;
begin
  Result := nil;
  LElem := AmqpUnwrapValue(AArr.GetArrayElement(AIdx));
  if LElem.IsObject and (LElem.AsObject is TAMQPFieldTable) then
    Result := TAMQPFieldTable(LElem.AsObject);
end;

function StrField(ATab: TAMQPFieldTable; const AKey: string): string;
var
  LVal: TValue;
begin
  Result := '';
  if (ATab <> nil) and ATab.TryGetValue(AKey, LVal) then
  begin
    LVal := AmqpUnwrapValue(LVal);
    if not LVal.IsObject then
      Result := LVal.AsString;
  end;
end;

function IntField(ATab: TAMQPFieldTable; const AKey: string): Int64;
var
  LVal: TValue;
begin
  Result := 0;
  if (ATab <> nil) and ATab.TryGetValue(AKey, LVal) then
  begin
    LVal := AmqpUnwrapValue(LVal);
    case LVal.Kind of
      tkInteger: Result := LVal.AsInteger;
      tkInt64:   Result := LVal.AsInt64;
    end;
  end;
end;

function AmqpDeathHops(AHeaders: TAMQPFieldTable): Int64;
var
  LArr: TValue;
  I: Integer;
begin
  Result := 0;
  if not ReadDeaths(AHeaders, LArr) then
    Exit;
  for I := 0 to LArr.GetArrayLength - 1 do
    Inc(Result, IntField(EntryOf(LArr, I), 'count'));
end;

procedure AmqpAddDeath(AEditor: TAMQPHeaderEditor;
  const AQueue, AReason, AExchange, ARoutingKey, AOriginalExpiration: string);
var
  LHeaders, LEntry, LNew: TAMQPFieldTable;
  LArr, LVal: TValue;
  LNewArr, LKeys: TAMQPValueArray;
  I, N: Integer;
  LNow: Int64;
begin
  LHeaders := AEditor.Headers; // created on demand if the message had none
  LNow := DateTimeToUnix(Now, False); // False = the input is LOCAL time
  LEntry := nil;

  // Collapse by (queue, reason).
  if ReadDeaths(LHeaders, LArr) then
    for I := 0 to LArr.GetArrayLength - 1 do
    begin
      LNew := EntryOf(LArr, I);
      if (LNew <> nil) and (StrField(LNew, 'queue') = AQueue)
        and (StrField(LNew, 'reason') = AReason) then
      begin
        LEntry := LNew;
        Break;
      end;
    end;

  if LEntry <> nil then
  begin
    LVal := TValue.From<Int64>(IntField(LEntry, 'count') + 1);
    LEntry.Put('count', LVal);
    LVal := TValue.From<Int64>(LNow);
    LEntry.Put('time', LVal);
    Exit; // the array does not change: the entry is already in it
  end;

  // New entry.
  LEntry := TAMQPFieldTable.Create;
  LVal := TValue.From<Int64>(1);
  LEntry.Put('count', LVal);
  LVal := TValue.From<string>(AReason);
  LEntry.Put('reason', LVal);
  LVal := TValue.From<string>(AQueue);
  LEntry.Put('queue', LVal);
  LVal := TValue.From<Int64>(LNow);
  LEntry.Put('time', LVal);
  LVal := TValue.From<string>(AExchange);
  LEntry.Put('exchange', LVal);
  SetLength(LKeys, 1);
  LKeys[0] := TValue.From<string>(ARoutingKey);
  LVal := TValue.From<TAMQPValueArray>(LKeys);
  LEntry.Put('routing-keys', LVal);
  if AOriginalExpiration <> '' then
  begin
    LVal := TValue.From<string>(AOriginalExpiration);
    LEntry.Put('original-expiration', LVal);
  end;

  // Most recent at the FRONT (RabbitMQ's order).
  N := 0;
  if LArr.IsArray then
    N := LArr.GetArrayLength;
  SetLength(LNewArr, N + 1);
  LNewArr[0] := TValue.From<TObject>(LEntry);
  for I := 0 to N - 1 do
    LNewArr[I + 1] := TValue.From<TObject>(EntryOf(LArr, I));
  LVal := TValue.From<TAMQPValueArray>(LNewArr);
  LHeaders.Put('x-death', LVal);

  // x-first-death-*: only on the FIRST death, never overwritten afterwards.
  if not LHeaders.ContainsKey('x-first-death-queue') then
  begin
    LVal := TValue.From<string>(AQueue);
    LHeaders.Put('x-first-death-queue', LVal);
    LVal := TValue.From<string>(AReason);
    LHeaders.Put('x-first-death-reason', LVal);
    LVal := TValue.From<string>(AExchange);
    LHeaders.Put('x-first-death-exchange', LVal);
  end;
end;

{ AmqpDeriveMessage }

function AmqpDeriveMessage(ASource: TAMQPMessage;
  const ANewHeaderPayload: TBytes;
  const AExchange, ARoutingKey: string): TAMQPMessage;
begin
  if ASource = nil then
    raise EAMQPWire.Create('AmqpDeriveMessage: source message is nil');
  // UserId comes from the source: whoever published is still the publisher, even
  // after the message is republished to a dead-letter exchange.
  Result := TAMQPMessage.Create(AExchange, ARoutingKey, ASource.UserId,
    ANewHeaderPayload, ASource.Body);
end;

end.
