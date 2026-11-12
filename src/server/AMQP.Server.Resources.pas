unit AMQP.Server.Resources;

{$I amqp.inc}

{ Descriptors of the declared resources (exchange/queue) -- broker
  sub-module, Phase 2 WS1.

  Only the data structure and the comparison/validation functions go here;
  the per-vhost registry of exchanges/queues built on top of it is WS2's
  (topology). The most direct use of AmqpArgsEqual/EquivalentTo is the basis
  of the divergent redeclare (406 PRECONDITION_FAILED, spec): declaring an
  existing resource again with different flags/arguments is an error; with
  the SAME flags/arguments it is idempotent.

  Ownership of Arguments: the descriptors TAKE OWNERSHIP of the
  TAMQPFieldTable coming from the Exchange.Declare/Queue.Declare decode and
  free it in the destructor -- same spirit as decision D1 (the message carries
  the raw payload instead of cloning TValue-in-TValue): cloning the table
  would have the same nested tables/arrays problem that AmqpUnwrapValue
  (AMQP.Wire) exists to work around, so the descriptor becomes the direct
  owner instead of copying. }

interface

uses
  SysUtils,
  Rtti,
  TypInfo,
  AMQP.Wire,
  AMQP.Exchange.Methods;

const
  /// This implementation's x-max-priority cap (Phase 3 decision D12).
  /// RabbitMQ accepts up to 255 and recommends <= 5; above 9 the per-queue
  /// memory cost (one ready bucket per level) is no longer worth what it gives.
  /// Declaring above this is a 406, not a silent clamp -- a deliberate
  /// deviation, documented in CLAUDE.md.
  AMQP_MAX_PRIORITY_LEVEL = 9;

type
  { x-overflow policy: what to do when the queue hits its cap.
    - amqovDropHead: drops from the head (RabbitMQ's de facto spec default);
    - amqovRejectPublish: rejects the publish, which in confirm mode becomes Basic.Nack.
    RabbitMQ's 'reject-publish-dlx' is not included: dead-lettering on overflow
    is WS5's 'maxlen' reason, which already covers the case the normal way. }
  TAMQPOverflow = (amqovDropHead, amqovRejectPublish);

  { A queue's x-arguments, already decoded and validated.

    WS2 scope (Phase 3): this unit only READS and VALIDATES. No field here has
    any effect yet -- TTL and priority are WS4, dead-letter WS5, the caps WS6,
    x-expires WS8. Keeping the policy in the descriptor from now on spares each
    workstream from re-parsing the table its own way.

    Absence is -1 for the numerics (and not 0, which is a VALID and different
    value: x-max-length=0 means "a queue that keeps nothing"). For strings, the
    Has* pair separates "absent" from "present and empty" --
    x-dead-letter-exchange='' is the DEFAULT exchange, not the absence of a DLX. }
  TAMQPQueuePolicy = record
    MessageTtlMs: Int64;             // x-message-ttl        (-1 = absent)
    ExpiresMs: Int64;                // x-expires            (-1 = absent)
    MaxLength: Int64;                // x-max-length         (-1 = absent)
    MaxLengthBytes: Int64;           // x-max-length-bytes   (-1 = absent)
    MaxPriority: Integer;            // x-max-priority       (-1 = no priority)
    Overflow: TAMQPOverflow;         // x-overflow           (default drop-head)
    DeadLetterExchange: string;      // x-dead-letter-exchange
    HasDeadLetterExchange: Boolean;
    DeadLetterRoutingKey: string;    // x-dead-letter-routing-key
    HasDeadLetterRoutingKey: Boolean;

    class function Empty: TAMQPQueuePolicy; static;
    /// True if the queue has at least one argument with an effect -- a
    /// shortcut for the later workstreams to skip the expensive path on a plain queue.
    function HasAny: Boolean;
  end;

  { Describes a declared exchange. Owns Arguments. }
  TAMQPExchangeDef = class
  private
    FName: string;
    FExchangeType: string;
    FDurable: Boolean;
    FAutoDelete: Boolean;
    FInternal: Boolean;
    FArguments: TAMQPFieldTable;
    FAlternateExchange: string;
    FHasAlternateExchange: Boolean;
  public
    /// AArguments: the descriptor becomes its owner (frees it in the
    /// destructor); may be nil.
    constructor Create(const AName, AExchangeType: string;
      ADurable, AAutoDelete, AInternal: Boolean; AArguments: TAMQPFieldTable);
    destructor Destroy; override;

    /// True if AOther describes an EQUIVALENT exchange (same type, same
    /// flags, same arguments by AmqpArgsEqual) -- does not compare Name, only
    /// the rest: the caller already knows the name matches (it is the
    /// redeclare of the same name that is being validated).
    function EquivalentTo(AOther: TAMQPExchangeDef): Boolean;

    property Name: string read FName;
    property ExchangeType: string read FExchangeType;
    property Durable: Boolean read FDurable;
    property AutoDelete: Boolean read FAutoDelete;
    property Internal: Boolean read FInternal;
    property Arguments: TAMQPFieldTable read FArguments;
    /// 'alternate-exchange', derived from Arguments at construction. Phase 3
    /// WS7 routes here whatever matched no binding; until then the field
    /// merely exists. Has* separates absent from present-and-empty.
    property AlternateExchange: string read FAlternateExchange;
    property HasAlternateExchange: Boolean read FHasAlternateExchange;
  end;

  { Describes a declared queue. Owns Arguments. }
  TAMQPQueueDef = class
  private
    FName: string;
    FDurable: Boolean;
    FExclusive: Boolean;
    FAutoDelete: Boolean;
    FArguments: TAMQPFieldTable;
    FOwnerId: NativeUInt;
    FPolicy: TAMQPQueuePolicy;
  public
    /// AArguments: the descriptor becomes its owner (frees it in the
    /// destructor); may be nil. AOwnerId: identity of the owning connection
    /// (0 = non-exclusive queue, or owner not assigned yet) -- see the comment
    /// on the OwnerId property.
    constructor Create(const AName: string;
      ADurable, AExclusive, AAutoDelete: Boolean; AArguments: TAMQPFieldTable;
      AOwnerId: NativeUInt = 0);
    destructor Destroy; override;

    /// True if AOther describes an EQUIVALENT queue (same flags, same
    /// arguments by AmqpArgsEqual). OwnerId is NOT part of the comparison: the
    /// owner is not part of what a redeclare validates (same semantics as RabbitMQ).
    function EquivalentTo(AOther: TAMQPQueueDef): Boolean;

    property Name: string read FName;
    property Durable: Boolean read FDurable;
    property Exclusive: Boolean read FExclusive;
    property AutoDelete: Boolean read FAutoDelete;
    property Arguments: TAMQPFieldTable read FArguments;
    /// Owner of an exclusive queue. Type choice: a NativeUInt holding the
    /// identity of the owning TAMQPServerConnection (e.g. NativeUInt(Connection)
    /// or a sequential id it exposes) -- not a direct reference to the object,
    /// so this unit does not need to see AMQP.Server.Connection (and avoids a
    /// unit cycle). 0 = no owner. WS3 (queue actor) and WS7 (cross lifecycle /
    /// teardown) use it to deny access from another connection and to release
    /// the queue when its owner goes down.
    property OwnerId: NativeUInt read FOwnerId write FOwnerId;
    /// The x-arguments, already decoded (Phase 3 WS2). Derived from Arguments
    /// at construction -- the table is still the source of truth (it is what
    /// EquivalentTo compares), the policy is just its typed reading.
    /// An invalid argument never gets here: the FSM validates BEFORE declaring.
    property Policy: TAMQPQueuePolicy read FPolicy;
  end;

/// Compares two field-tables SEMANTICALLY (not by reference):
/// - nil and an empty table are considered equal;
/// - they must have the same number of keys;
/// - each key of A must exist in B with the SAME value;
/// - a nested table ('F') compares recursively by AmqpArgsEqual;
/// - an array ('A' and the like) compares element by element, with
///   AmqpUnwrapValue (AMQP.Wire) on EVERY GetArrayElement -- a known TValue
///   gotcha, on Delphi and on FPC (GetArrayElement over an array of
///   field-values returns the element re-wrapped; see CLAUDE.md).
/// It is the basis of the 406 PRECONDITION_FAILED of a divergent redeclare.
function AmqpArgsEqual(A, B: TAMQPFieldTable): Boolean;

/// Reserved prefix 'amq.' (spec 1.4: exchange/queue names starting with it
/// are reserved for the server). WS5 uses it to deny a declare/bind of a
/// reserved name coming from the client (403 ACCESS_REFUSED), except for the
/// predefined exchanges the broker itself seeds.
function AmqpIsReservedName(const AName: string): Boolean;

/// True if AType is one of this implementation's four exchange types
/// (direct/fanout/topic/headers). EXACT, case-sensitive comparison: AMQP is
/// case-sensitive here (unlike, for example, the SASL mechanism), and
/// 'Direct'/'DIRECT' do not count.
function AmqpIsValidExchangeType(const AType: string): Boolean;

/// Compares two field-VALUES (a key/value pair already unwrapped from a
/// TAMQPFieldTable) semantically: the same Kind is a prerequisite, a nested
/// table ('F') recurses through AmqpArgsEqual, an array ('A') compares
/// element by element with AmqpUnwrapValue (FPC 3.2 gotcha, see AmqpArgsEqual
/// above), scalars compare by AsBoolean/AsInteger/.../AsString according to
/// the Kind (TValue.AsVariant does not exist in FPC 3.2 -- there is no generic
/// shortcut). Exported (not only for AmqpArgsEqual's internal use) so that
/// WS2 (AMQP.Server.Routing) reuses it for headers-exchange matching instead
/// of duplicating the TValue comparison logic.
function AmqpValuesEqual(const A, B: TValue): Boolean;

/// Reads and VALIDATES the queue x-arguments in AArgs (may be nil = empty table).
///
/// False = some argument is invalid, and AError carries the message already in
/// the format that goes into the reply-text of the 406 PRECONDITION_FAILED,
/// NAMING the offending argument (e.g. "invalid arg 'x-max-priority' for
/// queue: must be an integer between 0 and 9"). Naming it is what separates an
/// actionable error from a "precondition failed" the user has to guess at --
/// RabbitMQ does the same.
///
/// The caller is the FSM (AMQP.Server.Connection), BEFORE handing the table to
/// the engine: validation produces a reply-code, and the engine knows no
/// reply-code at all (a Phase 2 invariant). Unknown 'x-' keys are IGNORED, not
/// rejected -- that is what RabbitMQ does, and rejecting would break a client
/// that sends an argument from an extension this broker does not have.
function AmqpParseQueuePolicy(AArgs: TAMQPFieldTable;
  out APolicy: TAMQPQueuePolicy; out AError: string): Boolean;

/// Reads and validates the 'alternate-exchange' argument of an Exchange.Declare.
/// AName comes out empty and Result=True when the argument is absent (use
/// AHas to tell it from "present and empty"). Same AError contract.
function AmqpParseAlternateExchange(AArgs: TAMQPFieldTable;
  out AName: string; out AHas: Boolean; out AError: string): Boolean;

/// Reads Basic's 'expiration' property: the MESSAGE TTL in milliseconds,
/// which the spec defines as a decimal STRING and not as a number (that is
/// why TAMQPBasicProperties.Expiration is a string).
///
/// Empty AValue = absent: returns True with AMs = -1 (no TTL of its own).
/// Malformed (non-digit, negative, overflow) returns False -- and the FSM
/// turns that into a 406, the same way it does with an invalid x-argument.
/// Silently ignoring it would be worse than rejecting: the message the client
/// asked to expire would live forever, and the client would have no way to know.
function AmqpParseExpirationMs(const AValue: string; out AMs: Int64): Boolean;

implementation

{ TAMQPQueuePolicy }

class function TAMQPQueuePolicy.Empty: TAMQPQueuePolicy;
begin
  Result.MessageTtlMs := -1;
  Result.ExpiresMs := -1;
  Result.MaxLength := -1;
  Result.MaxLengthBytes := -1;
  Result.MaxPriority := -1;
  Result.Overflow := amqovDropHead;
  Result.DeadLetterExchange := '';
  Result.HasDeadLetterExchange := False;
  Result.DeadLetterRoutingKey := '';
  Result.HasDeadLetterRoutingKey := False;
end;

function TAMQPQueuePolicy.HasAny: Boolean;
begin
  Result := (MessageTtlMs >= 0) or (ExpiresMs >= 0) or (MaxLength >= 0)
    or (MaxLengthBytes >= 0) or (MaxPriority >= 0)
    or (Overflow <> amqovDropHead)
    or HasDeadLetterExchange or HasDeadLetterRoutingKey;
end;

{ --- typed reading of an x-argument ---------------------------------------
  The wire decoder does not produce a single type for numbers: FV_INT8/16/32
  become tkInteger and FV_UINT32/INT64/TIMESTAMP become tkInt64 (see
  TAMQPReader.ReadFieldValue). A client that sends x-message-ttl as 'b' and
  another that sends it as 'l' are both right, so accepting only one Kind
  would reject a valid client. Boolean is left out on purpose: in FPC it has
  its own Kind (tkBool) and in Delphi it falls under tkEnumeration -- in
  neither is it a number, and accepting True as 1 would mask a client type
  error. }

// True if the key exists. AOk comes out False when it exists but is not a number.
function TryArgInt64(AArgs: TAMQPFieldTable; const AKey: string;
  out AValue: Int64; out AOk: Boolean): Boolean;
var
  LVal: TValue;
begin
  AValue := 0;
  AOk := False;
  // The chained indexer (Table['k'].AsInt64) crashes FPC 3.2 with an internal
  // error -- which is why the TValue goes into a local variable first.
  Result := (AArgs <> nil) and AArgs.TryGetValue(AKey, LVal);
  if not Result then
    Exit;
  LVal := AmqpUnwrapValue(LVal);
  case LVal.Kind of
    tkInteger: begin AValue := LVal.AsInteger; AOk := True; end;
    tkInt64:   begin AValue := LVal.AsInt64;   AOk := True; end;
  end;
end;

// True if the key exists. AOk comes out False when it exists but is not a string.
function TryArgString(AArgs: TAMQPFieldTable; const AKey: string;
  out AValue: string; out AOk: Boolean): Boolean;
var
  LVal: TValue;
begin
  AValue := '';
  AOk := False;
  Result := (AArgs <> nil) and AArgs.TryGetValue(AKey, LVal);
  if not Result then
    Exit;
  LVal := AmqpUnwrapValue(LVal);
  case LVal.Kind of
    tkString, tkUString, tkLString, tkWString, tkChar, tkWChar
    {$IFDEF FPC}, tkAString{$ENDIF}:
      begin
        AValue := LVal.AsString;
        AOk := True;
      end;
  end;
end;

function ArgError(const AKey, ADetail: string): string;
begin
  Result := Format('invalid arg ''%s'' for queue: %s', [AKey, ADetail]);
end;

// Reads a numeric with a floor. AMin is the smallest ACCEPTED value (0 for the
// caps and the TTL, 1 for x-expires -- a queue that expires in 0 ms would
// vanish the instant it is declared, which RabbitMQ also rejects).
function ReadNumeric(AArgs: TAMQPFieldTable; const AKey: string; AMin: Int64;
  var ADestination: Int64; out AError: string): Boolean;
var
  LVal: Int64;
  LOk: Boolean;
begin
  AError := '';
  Result := True;
  if not TryArgInt64(AArgs, AKey, LVal, LOk) then
    Exit; // absent
  if not LOk then
  begin
    AError := ArgError(AKey, 'must be an integer');
    Exit(False);
  end;
  if LVal < AMin then
  begin
    AError := ArgError(AKey, Format('must be >= %d', [AMin]));
    Exit(False);
  end;
  ADestination := LVal;
end;

function AmqpParseQueuePolicy(AArgs: TAMQPFieldTable;
  out APolicy: TAMQPQueuePolicy; out AError: string): Boolean;
var
  LVal: Int64;
  LText: string;
  LOk: Boolean;
begin
  APolicy := TAMQPQueuePolicy.Empty;
  AError := '';

  if not ReadNumeric(AArgs, 'x-message-ttl', 0, APolicy.MessageTtlMs, AError) then
    Exit(False);
  if not ReadNumeric(AArgs, 'x-expires', 1, APolicy.ExpiresMs, AError) then
    Exit(False);
  if not ReadNumeric(AArgs, 'x-max-length', 0, APolicy.MaxLength, AError) then
    Exit(False);
  if not ReadNumeric(AArgs, 'x-max-length-bytes', 0, APolicy.MaxLengthBytes,
    AError) then
    Exit(False);

  // x-max-priority: D12's closed range, not just "non-negative".
  if TryArgInt64(AArgs, 'x-max-priority', LVal, LOk) then
  begin
    if (not LOk) or (LVal < 0) or (LVal > AMQP_MAX_PRIORITY_LEVEL) then
    begin
      AError := ArgError('x-max-priority', Format(
        'must be an integer between 0 and %d', [AMQP_MAX_PRIORITY_LEVEL]));
      Exit(False);
    end;
    APolicy.MaxPriority := Integer(LVal);
  end;

  // x-overflow: only this implementation's two policies.
  if TryArgString(AArgs, 'x-overflow', LText, LOk) then
  begin
    if not LOk then
    begin
      AError := ArgError('x-overflow', 'must be a string');
      Exit(False);
    end;
    if LText = 'drop-head' then
      APolicy.Overflow := amqovDropHead
    else if LText = 'reject-publish' then
      APolicy.Overflow := amqovRejectPublish
    else
    begin
      AError := ArgError('x-overflow',
        'must be ''drop-head'' or ''reject-publish''');
      Exit(False);
    end;
  end;

  if TryArgString(AArgs, 'x-dead-letter-exchange', LText, LOk) then
  begin
    if not LOk then
    begin
      AError := ArgError('x-dead-letter-exchange', 'must be a string');
      Exit(False);
    end;
    APolicy.DeadLetterExchange := LText;
    APolicy.HasDeadLetterExchange := True;
  end;

  if TryArgString(AArgs, 'x-dead-letter-routing-key', LText, LOk) then
  begin
    if not LOk then
    begin
      AError := ArgError('x-dead-letter-routing-key', 'must be a string');
      Exit(False);
    end;
    APolicy.DeadLetterRoutingKey := LText;
    APolicy.HasDeadLetterRoutingKey := True;
  end;

  Result := True;
end;

function AmqpParseAlternateExchange(AArgs: TAMQPFieldTable;
  out AName: string; out AHas: Boolean; out AError: string): Boolean;
var
  LOk: Boolean;
begin
  AName := '';
  AHas := False;
  AError := '';
  Result := True;
  if not TryArgString(AArgs, 'alternate-exchange', AName, LOk) then
    Exit; // absent
  if not LOk then
  begin
    AName := '';
    AError := 'invalid arg ''alternate-exchange'' for exchange: '
      + 'must be a string';
    Exit(False);
  end;
  AHas := True;
end;

function AmqpParseExpirationMs(const AValue: string; out AMs: Int64): Boolean;
var
  I: Integer;
  LAcc: Int64;
begin
  AMs := -1;
  if AValue = '' then
    Exit(True); // absent
  LAcc := 0;
  for I := 1 to Length(AValue) do
  begin
    if (AValue[I] < '0') or (AValue[I] > '9') then
      Exit(False);
    // Generous but finite cap: an 'expiration' with 19 digits is a client
    // error, and letting Int64 overflow would give a silent negative deadline.
    if LAcc > (High(Int64) - 9) div 10 then
      Exit(False);
    LAcc := LAcc * 10 + (Ord(AValue[I]) - Ord('0'));
  end;
  AMs := LAcc;
  Result := True;
end;

function AmqpArgsEqual(A, B: TAMQPFieldTable): Boolean;
var
  LKey: string;
  LValA, LValB: TValue;
begin
  // nil and an empty table are equivalent for semantic comparison.
  if (A = nil) or (A.Count = 0) then
    Exit((B = nil) or (B.Count = 0));
  if (B = nil) or (B.Count = 0) then
    Exit(False); // A non-empty (early exit above), B empty/nil: they differ

  if A.Count <> B.Count then
    Exit(False);

  Result := True;
  for LKey in A.Keys do
  begin
    if not B.TryGetValue(LKey, LValB) then
      Exit(False);
    LValA := A[LKey]; // indexer -> local variable before use (FPC gotcha)
    if not AmqpValuesEqual(LValA, LValB) then
      Exit(False);
  end;
end;

function AmqpValuesEqual(const A, B: TValue): Boolean;
var
  LObjA, LObjB: TObject;
  LLen, I: Integer;
  LElemA, LElemB: TValue;
begin
  if A.Kind <> B.Kind then
    Exit(False);

  // 'V' (void): TValue.Empty on both sides -- the kind already matched,
  // nothing else to compare.
  if A.IsEmpty then
    Exit(True);

  if A.IsObject then
  begin
    LObjA := A.AsObject;
    LObjB := B.AsObject;
    // 'F' (nested table): the only object a field-value carries today.
    if (LObjA is TAMQPFieldTable) and (LObjB is TAMQPFieldTable) then
      Exit(AmqpArgsEqual(TAMQPFieldTable(LObjA), TAMQPFieldTable(LObjB)));
    Exit(LObjA = LObjB); // fallback: same instance
  end;

  if A.IsArray then
  begin
    if not B.IsArray then
      Exit(False);
    LLen := A.GetArrayLength;
    if LLen <> B.GetArrayLength then
      Exit(False);
    // 'A' (array of field-values): GetArrayElement returns the element
    // re-wrapped in a TValue, on Delphi and on FPC -- AmqpUnwrapValue undoes it.
    // We unwrap EVERY element, of any array: it is idempotent, and in an
    // array that is not of field-values (e.g. 'x' = TBytes) the element is
    // not a wrapped TValue, so it is a no-op.
    //
    // Do NOT test "TypeInfo = TypeInfo(TArray<TValue>)" here to decide whether
    // to unwrap: in FPC 3.2 that specialization WRITTEN INLINE in this unit is
    // a DIFFERENT type from the TAMQPValueArray that AMQP.Wire produces when
    // decoding (measured: the comparison gives False), and the effect would be
    // comparing still-wrapped elements -- equal tables coming out "different".
    for I := 0 to LLen - 1 do
    begin
      LElemA := AmqpUnwrapValue(A.GetArrayElement(I));
      LElemB := AmqpUnwrapValue(B.GetArrayElement(I));
      if not AmqpValuesEqual(LElemA, LElemB) then
        Exit(False);
    end;
    Exit(True);
  end;

  // Scalars: the only Kinds left are the ones WriteFieldValue knows how to
  // write (see AMQP.Wire) -- TValue.AsVariant does NOT exist in FPC 3.2, so
  // the dispatch is by Kind, mirroring WriteFieldValue's case exactly.
  case A.Kind of
    {$IFDEF FPC}
    tkBool:
      Result := A.AsBoolean = B.AsBoolean;
    {$ENDIF}
    tkEnumeration:
      if A.TypeInfo = TypeInfo(Boolean) then
        Result := A.AsBoolean = B.AsBoolean
      else
        Result := False; // non-Boolean enum: not expected in a field-value
    tkInteger:
      Result := A.AsInteger = B.AsInteger;
    tkInt64:
      Result := A.AsInt64 = B.AsInt64;
    tkFloat:
      Result := A.AsExtended = B.AsExtended;
    tkString, tkUString, tkLString, tkWString, tkChar, tkWChar
    {$IFDEF FPC}, tkAString{$ENDIF}:
      Result := A.AsString = B.AsString;
  else
    Result := False; // Kind not supported in a field-value
  end;
end;

function AmqpIsReservedName(const AName: string): Boolean;
begin
  Result := (Length(AName) >= 4) and (Copy(AName, 1, 4) = 'amq.');
end;

function AmqpIsValidExchangeType(const AType: string): Boolean;
begin
  Result := (AType = AMQP_EXCHANGE_TYPE_DIRECT) or
            (AType = AMQP_EXCHANGE_TYPE_FANOUT) or
            (AType = AMQP_EXCHANGE_TYPE_TOPIC) or
            (AType = AMQP_EXCHANGE_TYPE_HEADERS);
end;

{ TAMQPExchangeDef }

constructor TAMQPExchangeDef.Create(const AName, AExchangeType: string;
  ADurable, AAutoDelete, AInternal: Boolean; AArguments: TAMQPFieldTable);
var
  LError: string;
begin
  inherited Create;
  FName := AName;
  FExchangeType := AExchangeType;
  FDurable := ADurable;
  FAutoDelete := AAutoDelete;
  FInternal := AInternal;
  FArguments := AArguments;
  // Same policy as TAMQPQueueDef: does not raise -- the FSM already validated.
  if not AmqpParseAlternateExchange(FArguments, FAlternateExchange,
    FHasAlternateExchange, LError) then
  begin
    FAlternateExchange := '';
    FHasAlternateExchange := False;
  end;
end;

destructor TAMQPExchangeDef.Destroy;
begin
  FArguments.Free;
  inherited;
end;

function TAMQPExchangeDef.EquivalentTo(AOther: TAMQPExchangeDef): Boolean;
begin
  Result := Assigned(AOther) and
    (FExchangeType = AOther.FExchangeType) and
    (FDurable = AOther.FDurable) and
    (FAutoDelete = AOther.FAutoDelete) and
    (FInternal = AOther.FInternal) and
    AmqpArgsEqual(FArguments, AOther.FArguments);
end;

{ TAMQPQueueDef }

constructor TAMQPQueueDef.Create(const AName: string;
  ADurable, AExclusive, AAutoDelete: Boolean; AArguments: TAMQPFieldTable;
  AOwnerId: NativeUInt);
var
  LError: string;
begin
  inherited Create;
  FName := AName;
  FDurable := ADurable;
  FExclusive := AExclusive;
  FAutoDelete := AAutoDelete;
  FArguments := AArguments;
  FOwnerId := AOwnerId;
  // Does not raise: the FSM already validated before getting here, and a
  // descriptor that fails to construct would leave the topology in a worse
  // state than an empty policy. If it ever fails, it is our bug and shows up
  // as an argument with no effect.
  if not AmqpParseQueuePolicy(FArguments, FPolicy, LError) then
    FPolicy := TAMQPQueuePolicy.Empty;
end;

destructor TAMQPQueueDef.Destroy;
begin
  FArguments.Free;
  inherited;
end;

function TAMQPQueueDef.EquivalentTo(AOther: TAMQPQueueDef): Boolean;
begin
  Result := Assigned(AOther) and
    (FDurable = AOther.FDurable) and
    (FExclusive = AOther.FExclusive) and
    (FAutoDelete = AOther.FAutoDelete) and
    AmqpArgsEqual(FArguments, AOther.FArguments);
end;

end.
