unit AMQP.Server.Delivery;

{$I amqp.inc}

{ A channel's delivery target -- broker sub-module, WS4 of Phase 2.

  It is the piece between the queue actor and the connection's writing: "the actor
  delivers; the connection writes". The queue (AMQP.Server.Queue) only knows the
  IAMQPDeliveryTarget interface; this unit is what knows how to build
  Basic.Deliver + content header + body frames and push them into the write queue.

  Why there is a SEPARATE CLASS instead of TAMQPServerChannel itself
  implementing the interface
  ------------------------------------------------------------------------
  Lifecycle. The channel and the connection die when the peer sends Channel.Close,
  when the socket drops or when the broker's reaper collects the connection -- and the
  queue actor, which runs on a pool thread, may be right in the middle of
  a delivery at that instant. If the queue kept a pointer to the channel,
  that would be use-after-free: it is the risk the Phase 2 plan marks as "the
  spot where the phase's use-after-free lives".

  The way out is the refcount + detach pair:
   - the target is a TInterfacedObject, so every queue that keeps a consumer
     holds a reference and the OBJECT does not die while anyone holds it,
     even if the channel and the connection have already been freed;
   - Detach (called by the channel/connection when dying) clears the reference to
     the writer UNDER the target's LOCK and marks it as dead. After that every
     TryDeliver returns False without touching anything freed, and the queues drop
     the consumer on the next delivery round (IsAlive = False).
  Detach never waits for I/O: TryDeliver only does non-blocking work, so
  teardown does not get stuck behind a slow consumer.

  Accounting of unacked deliveries (and of the prefetch)
  ------------------------------------------------------
  The channel's list of open deliveries lives HERE, not in the channel, for a
  who-knows-what reason: delivery happens on the QUEUE ACTOR's thread, which
  is the only one that knows the (delivery-tag, source queue) pair; the connection's
  read thread, which sees the client's Basic.Ack, knows the tag but not the queue.
  Without this record, a consumer ack reaches no queue at all and the message
  hangs forever -- that was exactly the bug the test
  AckRemovesForGood_DoesNotReturnAfterCrash caught.

  So: the delivery (TryDeliver) and the FSM's Basic.Get (NoteGet) REGISTER;
  the read thread RESOLVES when processing
  Basic.Ack/Nack/Reject (NoteResolved), which returns the resolved entries
  so the caller can forward the settlement to each queue.

  Prefetch (Basic.Qos): only entries marked Prefetch=True count toward the
  limit. A no-ack delivery creates no entry at all (it will never be acked);
  Basic.Get creates an entry (it must be acked) but does NOT count toward the
  prefetch, which by the spec limits deliveries to a CONSUMER. }

interface

uses
  SysUtils,
  SyncObjs,
  Generics.Collections,
  AMQP.Protocol,
  AMQP.Frame,
  AMQP.Basic.Methods,
  PascalCommon.Threading,
  AMQP.Server.FrameIO,
  AMQP.Server.Message,
  AMQP.Server.Queue;

type
  { An open delivery on this channel: the tag, the queue that holds the message and
    whether it counts toward the Basic.Qos limit. }
  TAMQPOutstanding = record
    Tag: UInt64;
    Queue: string;
    Prefetch: Boolean;
  end;

  { Delivery target of ONE channel. Created by the channel, referenced by the queues
    where it has consumers. }
  TAMQPChannelDeliveryTarget = class(TInterfacedObject, IAMQPDeliveryTarget)
  private
    FLock: TCriticalSection;
    FWriter: TAMQPFrameWriter; // nil after Detach -- always under FLock
    FChannelNo: Word;
    FMaxPayload: Cardinal;
    FIdentity: NativeUInt;
    FNextTag: UInt64;   // under FLock
    FAttached: Integer; // atomic (1/0) -- mirrors "FWriter <> nil"
    FActive: Integer;   // atomic (1/0) -- Channel.Flow
    FPrefetch: Integer; // atomic
    // Open deliveries ON THIS channel, in ascending tag order (the sequence
    // is monotonic, so appending at the end keeps it sorted). A list, not a
    // counter, because Basic.Ack/Nack with multiple resolves ALL tags <=
    // the given one -- and because each one must remember WHICH QUEUE it came from.
    // Under FLock.
    FOutstanding: TList<TAMQPOutstanding>;
    function BuildFrames(const AConsumerTag: string; ADeliveryTag: UInt64;
      ARedelivered: Boolean; AMessage: TAMQPMessage): TArray<TAMQPFrame>;
    function PrefetchInFlight: Integer;
  public
    /// AMaxPayload: how much fits in the payload of ONE frame for this peer (the
    /// negotiated frame-max minus the frame overhead) -- 0 = no limit.
    /// The target does NOT own the writer.
    constructor Create(AWriter: TAMQPFrameWriter; AChannelNo: Word;
      AMaxPayload: Cardinal);
    destructor Destroy; override;

    // --- IAMQPDeliveryTarget ---
    function ChannelId: NativeUInt;
    function IsAlive: Boolean;
    function TryDeliver(const AConsumerTag, AQueueName: string;
      ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
      out ADeliveryTag: UInt64): Boolean;
    function TryCancel(const AConsumerTag: string): Boolean;

    /// The channel/connection died: releases the writer and marks the target as dead.
    /// Idempotent and callable from any thread.
    procedure Detach;

    /// Next delivery-tag from the SAME per-channel sequence the delivery uses.
    /// Basic.Get needs it (the tag belongs to the channel, not the consumer).
    function NextDeliveryTag: UInt64;
    /// The client settled (Basic.Ack/Nack/Reject) a delivery of this channel.
    /// AMultiple resolves all tags <= ADeliveryTag (and, with
    /// ADeliveryTag=0, all pending ones -- spec). Returns the resolved
    /// entries, WITH THE SOURCE QUEUE of each: that is what lets the
    /// caller forward the settlement to the right queue. Called by the connection's
    /// read thread, which is the one that sees the client's method.
    function NoteResolved(ADeliveryTag: UInt64;
      AMultiple: Boolean): TArray<TAMQPOutstanding>;
    /// Registers a delivery made outside consumer delivery (Basic.Get in
    /// ack mode). Does not count toward the prefetch.
    procedure NoteGet(ADeliveryTag: UInt64; const AQueue: string);
    /// Queues with an open delivery on this channel (no repeats). The teardown uses
    /// it to notify each one.
    function PendingQueues: TArray<string>;

    /// Channel.Flow. False = the peer asked us to stop delivering content.
    procedure SetActive(AValue: Boolean);
    /// Basic.Qos prefetch-count (0 = unlimited).
    procedure SetPrefetch(AValue: Word);
    /// Deliveries not yet acked on this channel.
    function UnackedCount: Integer;
    property ChannelNo: Word read FChannelNo;
  end;

/// CONTENT frames (content header + N body) of a message, for the given channel
/// and frame-max. Used by delivery (Basic.Deliver) and by the FSM's Basic.Get
/// -- both emit method + header + body, changing only the method.
/// AMaxPayload = 0: body in a single frame.
///
/// The header comes from the RAW payload stored in the message (decision D1): byte
/// by byte re-emission, without re-decoding properties. Only when there is no raw
/// payload (message created by the engine itself) is a minimal header built.
function AmqpBuildContentFrames(AChannelNo: Word; AMessage: TAMQPMessage;
  AMaxPayload: Cardinal): TArray<TAMQPFrame>;

implementation

function AmqpBuildContentFrames(AChannelNo: Word; AMessage: TAMQPMessage;
  AMaxPayload: Cardinal): TArray<TAMQPFrame>;
var
  LHeader: TBytes;
  LChunks: TAMQPBodyChunks;
  I: Integer;
begin
  Result := nil;
  LHeader := AMessage.HeaderPayload;
  if Length(LHeader) = 0 then
    LHeader := BuildContentHeader(AMessage.BodySize,
      TAMQPBasicProperties.Empty);
  LChunks := AmqpSplitBody(AMessage.Body, AMaxPayload);
  SetLength(Result, 1 + Length(LChunks));
  Result[0] := TAMQPFrame.Create(AMQP_FRAME_HEADER, AChannelNo, LHeader);
  for I := 0 to High(LChunks) do
    Result[1 + I] := TAMQPFrame.Create(AMQP_FRAME_BODY, AChannelNo,
      LChunks[I]);
end;

var
  // Channel identity the queue keys unacked deliveries by. A global, monotonic
  // counter instead of the object address: an address can be
  // reused by a new target after the old one dies, and an orphaned unacked
  // delivery would match the wrong channel.
  GIdentitySeq: Integer = 0;

{ TAMQPChannelDeliveryTarget }

constructor TAMQPChannelDeliveryTarget.Create(AWriter: TAMQPFrameWriter;
  AChannelNo: Word; AMaxPayload: Cardinal);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FOutstanding := TList<TAMQPOutstanding>.Create;
  FWriter := AWriter;
  FChannelNo := AChannelNo;
  FMaxPayload := AMaxPayload;
  FIdentity := NativeUInt(Cardinal(PcAtomicInc(GIdentitySeq)));
  FNextTag := 1; // spec: a channel's delivery-tag sequence starts at 1
  FAttached := 1;
  FActive := 1;
end;

destructor TAMQPChannelDeliveryTarget.Destroy;
begin
  FOutstanding.Free;
  FLock.Free;
  inherited;
end;

function TAMQPChannelDeliveryTarget.ChannelId: NativeUInt;
begin
  Result := FIdentity;
end;

function TAMQPChannelDeliveryTarget.IsAlive: Boolean;
begin
  // No lock on purpose: it is queried for every consumer in every delivery
  // round. The atomic flag is enough -- a late caller falls into the
  // FWriter=nil test inside TryDeliver, which is under the lock.
  Result := PcAtomicGet(FAttached) = 1;
end;

procedure TAMQPChannelDeliveryTarget.Detach;
begin
  FLock.Enter;
  try
    FWriter := nil;
    PcAtomicSet(FAttached, 0);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPChannelDeliveryTarget.SetActive(AValue: Boolean);
begin
  if AValue then
    PcAtomicSet(FActive, 1)
  else
    PcAtomicSet(FActive, 0);
end;

procedure TAMQPChannelDeliveryTarget.SetPrefetch(AValue: Word);
begin
  PcAtomicSet(FPrefetch, AValue);
end;

function TAMQPChannelDeliveryTarget.UnackedCount: Integer;
begin
  FLock.Enter;
  try
    Result := FOutstanding.Count;
  finally
    FLock.Leave;
  end;
end;

// How many of the open deliveries count toward Basic.Qos. Call UNDER FLock.
function TAMQPChannelDeliveryTarget.PrefetchInFlight: Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := 0 to FOutstanding.Count - 1 do
    if FOutstanding[I].Prefetch then
      Inc(Result);
end;

procedure TAMQPChannelDeliveryTarget.NoteGet(ADeliveryTag: UInt64;
  const AQueue: string);
var
  LEntry: TAMQPOutstanding;
begin
  LEntry.Tag := ADeliveryTag;
  LEntry.Queue := AQueue;
  LEntry.Prefetch := False; // Basic.Get does not count toward the Qos limit
  FLock.Enter;
  try
    FOutstanding.Add(LEntry);
  finally
    FLock.Leave;
  end;
end;

function TAMQPChannelDeliveryTarget.PendingQueues: TArray<string>;
var
  I, J: Integer;
  LNames: TList<string>;
begin
  LNames := TList<string>.Create;
  try
    FLock.Enter;
    try
      for I := 0 to FOutstanding.Count - 1 do
      begin
        J := LNames.IndexOf(FOutstanding[I].Queue);
        if J < 0 then
          LNames.Add(FOutstanding[I].Queue);
      end;
    finally
      FLock.Leave;
    end;
    Result := LNames.ToArray;
  finally
    LNames.Free;
  end;
end;

function TAMQPChannelDeliveryTarget.NoteResolved(ADeliveryTag: UInt64;
  AMultiple: Boolean): TArray<TAMQPOutstanding>;
var
  I: Integer;
  LRes: TList<TAMQPOutstanding>;
begin
  LRes := TList<TAMQPOutstanding>.Create;
  try
    FLock.Enter;
    try
      if AMultiple then
      begin
        // The list is in ascending order: the resolved ones are a PREFIX of it.
        // Tag 0 with multiple = all pending ones (spec 1.8.3.13).
        while (FOutstanding.Count > 0)
          and ((ADeliveryTag = 0) or (FOutstanding[0].Tag <= ADeliveryTag)) do
        begin
          LRes.Add(FOutstanding[0]);
          FOutstanding.Delete(0);
        end;
      end
      else
      begin
        for I := 0 to FOutstanding.Count - 1 do
          if FOutstanding[I].Tag = ADeliveryTag then
          begin
            LRes.Add(FOutstanding[I]);
            FOutstanding.Delete(I);
            Break;
          end;
        // An unknown tag is not an error HERE: it may be a duplicate ack. The FSM
        // is what treats an invalid ack as a channel error.
      end;
    finally
      FLock.Leave;
    end;
    Result := LRes.ToArray;
  finally
    LRes.Free;
  end;
end;

function TAMQPChannelDeliveryTarget.NextDeliveryTag: UInt64;
begin
  FLock.Enter;
  try
    Result := FNextTag;
    Inc(FNextTag);
  finally
    FLock.Leave;
  end;
end;

// Builds the INDIVISIBLE batch of a delivery: method + content header + N body.
function TAMQPChannelDeliveryTarget.BuildFrames(const AConsumerTag: string;
  ADeliveryTag: UInt64; ARedelivered: Boolean;
  AMessage: TAMQPMessage): TArray<TAMQPFrame>;
var
  LDeliver: TAMQPBasicDeliver;
  LContent: TArray<TAMQPFrame>;
  I: Integer;
begin
  Result := nil;
  LDeliver.ConsumerTag := AConsumerTag;
  LDeliver.DeliveryTag := ADeliveryTag;
  LDeliver.Redelivered := ARedelivered;
  LDeliver.Exchange := AMessage.Exchange;
  LDeliver.RoutingKey := AMessage.RoutingKey;

  LContent := AmqpBuildContentFrames(FChannelNo, AMessage, FMaxPayload);
  SetLength(Result, 1 + Length(LContent));
  Result[0] := TAMQPFrame.Create(AMQP_FRAME_METHOD, FChannelNo,
    BuildBasicDeliver(LDeliver));
  for I := 0 to High(LContent) do
    Result[1 + I] := LContent[I];
end;

// Server -> client Basic.Cancel: the consumer's queue is gone. A single
// method frame, through the same non-blocking path as delivery.
function TAMQPChannelDeliveryTarget.TryCancel(
  const AConsumerTag: string): Boolean;
begin
  Result := False;
  FLock.Enter;
  try
    if FWriter = nil then
      Exit; // channel already dead: nobody to notify
    Result := FWriter.TryPostFrames([TAMQPFrame.Create(AMQP_FRAME_METHOD,
      FChannelNo, BuildBasicCancel(AConsumerTag, True))]);
  finally
    FLock.Leave;
  end;
end;

function TAMQPChannelDeliveryTarget.TryDeliver(
  const AConsumerTag, AQueueName: string; ANoAck, ARedelivered: Boolean;
  AMessage: TAMQPMessage; out ADeliveryTag: UInt64): Boolean;
var
  LTag: UInt64;
  LPrefetch: Integer;
  LEntry: TAMQPOutstanding;
begin
  ADeliveryTag := 0;
  Result := False;
  FLock.Enter;
  try
    if FWriter = nil then
      Exit; // channel/connection already dead (Detach)
    if PcAtomicGet(FActive) = 0 then
      Exit; // Channel.Flow turned off by the peer
    if not ANoAck then
    begin
      LPrefetch := PcAtomicGet(FPrefetch);
      if (LPrefetch > 0) and (PrefetchInFlight >= LPrefetch) then
        Exit; // prefetch exceeded: the channel already has all it can take
    end;

    // The tag is only CONSUMED if the batch is accepted -- a refusal must not
    // open a gap in the channel's sequence.
    LTag := FNextTag;
    if not FWriter.TryPostFrames(BuildFrames(AConsumerTag, LTag, ARedelivered,
      AMessage)) then
      Exit; // write queue full: D3, the consumer leaves this round's round-robin

    FNextTag := LTag + 1;
    if not ANoAck then
    begin
      // The source queue is registered here because only THIS call
      // knows it -- see the unit header.
      LEntry.Tag := LTag;
      LEntry.Queue := AQueueName;
      LEntry.Prefetch := True;
      FOutstanding.Add(LEntry);
    end;
    ADeliveryTag := LTag;
    Result := True;
  finally
    FLock.Leave;
  end;
end;

end.
