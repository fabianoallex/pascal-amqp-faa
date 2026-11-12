unit AMQP.Server.Message;

{$I amqp.inc}

{ The published message, the way the engine (Phase 2) keeps it in the queues
  (broker sub-module, WS1 of Phase 2).

  Decision D1 (CLAUDE.md, "Broker sub-module" / Phase 2): the message carries the
  RAW content-header payload (HeaderPayload), not a clone of the Properties
  TAMQPFieldTable -- faithful byte-by-byte re-emission to the consumer
  (Basic.Deliver) and none of FPC 3.2's TValue-in-TValue problem when cloning
  nested tables/arrays. Whoever needs the decoded properties (e.g.
  the binding match of a headers exchange) decodes from the channel's LIVE
  table, before the enqueue -- that is what the "new invariant" in CLAUDE.md
  describes. TAMQPMessage therefore keeps NO TAMQPBasicProperties at all.

  Refcount (architecture decision locked in CLAUDE.md, "Message" section):
  TAMQPMessage is born with RefCount=1 and is shared by N queues without copying
  the body (COW) -- each queue that holds the message calls AddRef; Release when
  it lets go. The last Release frees the object. NOBODY mutates the message after it
  is created: Body and HeaderPayload are exposed read-only.

  Atomics via PcAtomicInc/Dec (PascalCommon.Threading) -- TInterlocked does not exist in
  FPC (see the forbidden table in CLAUDE.md). }

interface

uses
  SysUtils,
  PascalCommon.Threading,
  AMQP.Server.Types;

type
  { A piece of the body, for splitting into N body frames by the frame-max of the
    delivery's RECIPIENT. Named type because of FPC: a parameter
    typed as "array of T" is an open array (value semantics, copies the
    elements) -- here it is only the return type of AmqpSplitBody, but it stays
    named for documentation and symmetry with the rest of the lib (see the
    named array vs. open array gotcha in CLAUDE.md). }
  TAMQPBodyChunks = array of TBytes;

  { Published message, kept by the engine under an atomic refcount. Immutable
    once constructed. }
  TAMQPMessage = class
  private
    FRefCount: Integer; // atomic -- see AddRef/Release
    FExchange: string;
    FRoutingKey: string;
    FUserId: string;
    FHeaderPayload: TBytes;
    FBody: TBytes;
    function GetBodySize: Integer;
    function GetRefCount: Integer;
  public
    /// Body and HeaderPayload are kept by REFERENCE (normal Pascal dynamic
    /// array semantics): a caller that wants an independent buffer
    /// copies BEFORE constructing. RefCount starts at 1 -- the creator owns
    /// that first reference and must call Release (never Free directly).
    constructor Create(const AExchange, ARoutingKey, AUserId: string;
      const AHeaderPayload, ABody: TBytes);

    /// Increments the refcount; returns the new value.
    function AddRef: Integer;
    /// Decrements the refcount; when it reaches 0, frees the object (Self.Free)
    /// and returns 0 -- in that case, Self can no longer be used.
    function Release: Integer;

    /// Builds from a TAMQPServerMessage already reassembled by the channel
    /// (BeginContent -> SetContentHeader -> AppendBody -> CurrentMessage).
    /// RefCount starts at 1, as in Create.
    class function FromServerMessage(
      const AMsg: TAMQPServerMessage): TAMQPMessage;

    property Exchange: string read FExchange;
    property RoutingKey: string read FRoutingKey;
    /// Authenticated user of the publishing connection (auditing / future
    /// validation of the user-id property).
    property UserId: string read FUserId;
    /// Raw content-header payload (class-id/weight/body-size/flags and the
    /// present properties), exactly as it came in the frame -- decision D1.
    /// Re-emitting to a consumer is writing this payload straight into the delivery's
    /// header frame, without re-decoding/re-encoding.
    property HeaderPayload: TBytes read FHeaderPayload;
    /// Message body. Shared by N queues without copying -- nobody mutates it.
    property Body: TBytes read FBody;
    property BodySize: Integer read GetBodySize;
    property RefCount: Integer read GetRefCount;
  end;

/// Slices ABody into pieces of at most AMaxPayload bytes each. AMaxPayload is
/// the maximum payload size of a body frame for the delivery's
/// RECIPIENT -- the caller already subtracts the frame overhead from the negotiated
/// frame-max (frame header + terminator). Empty body -> 0 pieces.
/// AMaxPayload = 0 -> a single piece with the whole body (no limit).
function AmqpSplitBody(const ABody: TBytes;
  AMaxPayload: Cardinal): TAMQPBodyChunks;

implementation

{ TAMQPMessage }

constructor TAMQPMessage.Create(const AExchange, ARoutingKey, AUserId: string;
  const AHeaderPayload, ABody: TBytes);
begin
  inherited Create;
  FRefCount := 1;
  FExchange := AExchange;
  FRoutingKey := ARoutingKey;
  FUserId := AUserId;
  FHeaderPayload := AHeaderPayload;
  FBody := ABody;
end;

function TAMQPMessage.GetBodySize: Integer;
begin
  Result := Length(FBody);
end;

function TAMQPMessage.GetRefCount: Integer;
begin
  Result := PcAtomicGet(FRefCount);
end;

function TAMQPMessage.AddRef: Integer;
begin
  Result := PcAtomicInc(FRefCount);
end;

function TAMQPMessage.Release: Integer;
begin
  Result := PcAtomicDec(FRefCount);
  if Result = 0 then
    Free;
end;

class function TAMQPMessage.FromServerMessage(
  const AMsg: TAMQPServerMessage): TAMQPMessage;
begin
  Result := TAMQPMessage.Create(AMsg.Exchange, AMsg.RoutingKey, AMsg.UserId,
    AMsg.HeaderPayload, AMsg.Body);
end;

{ AmqpSplitBody }

function AmqpSplitBody(const ABody: TBytes;
  AMaxPayload: Cardinal): TAMQPBodyChunks;
var
  LTotal, LMax, LOffset, LChunkLen, LCount, I: Integer;
begin
  Result := nil;
  LTotal := Length(ABody);
  if LTotal = 0 then
    Exit; // empty body -> 0 pieces

  if AMaxPayload = 0 then
  begin
    SetLength(Result, 1);
    Result[0] := Copy(ABody, 0, LTotal);
    Exit;
  end;

  LMax := Integer(AMaxPayload);
  LCount := (LTotal + LMax - 1) div LMax; // ceiling of the division
  SetLength(Result, LCount);
  LOffset := 0;
  for I := 0 to LCount - 1 do
  begin
    LChunkLen := LMax;
    if LOffset + LChunkLen > LTotal then
      LChunkLen := LTotal - LOffset;
    Result[I] := Copy(ABody, LOffset, LChunkLen);
    Inc(LOffset, LChunkLen);
  end;
end;

end.
