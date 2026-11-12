unit AMQP.Server.Recovery;

{$I amqp.inc}

{ WAL replay -- broker sub-module, WS6 of Phase 4 (durability).

  This unit does NOT talk to the engine. It reads the segments in LSN order and
  answers a single question: GIVEN THIS LOG, WHAT IS THE STATE? Applying that
  state to a live broker is TAMQPServer.Start's job, and the separation is deliberate
  -- that way the hard part (the replay semantics) is testable without opening a socket,
  without threads and without a broker, with the log built by hand byte by byte.

  WHAT THE REPLAY HAS TO GET RIGHT, AND THAT IS NOT OBVIOUS
  ---------------------------------------------------------

  1. THE LOG IS OF OPERATIONS, THE STATE IS THE FINAL ONE. A QUEUE_DECLARE followed by
     QUEUE_DELETE leaves no queue at all -- and no BINDING at all, nor any
     PLACEMENT of that queue. Deleting the queue and forgetting what was pending in it
     would resurrect messages of a queue that no longer exists.

  2. THE LIVE PLACEMENT IS THE ONE WITH AN ENQ AND NO DEQ. That is all of D22: the
     delivery is not journaled, so a message delivered and NOT acked
     is still a live ENQ and comes back ready (D27). Nothing to do for that to
     happen -- it follows from writing nothing on delivery.

  3. THE BODY COMES ONLY ONCE. N placements reference the same ContentId
     (D22), and dead-lettering derives a new placement with the SAME content. That is
     why the content lives in a separate map, and the placement only keeps the id.

  4. THE ORDER OF THE PLACEMENTS IS THE ORDER OF THE LOG. D27 pays off here: since the
     journal is append-only and the priority bucket is inside the ENQ,
     re-enqueueing in read order puts each message exactly where
     D12's RequeueFront would put it. There is no sorting to do.

  5. THE HIGHEST ID SEEN MATTERS. EntryId and ContentId come from the same engine
     counter; if it restarted from zero after a restart, a new placement
     would collide with a recovered one and the DEQ of one would retire the other.
     That is why MaxId comes out of here.

  WHAT THIS UNIT DOES NOT DO: it does not decide deadlines. The ENQ carries the
  WALL-clock instant and the TTL (D21), and converting that to the monotonic clock is the job
  of whoever holds the live queue -- here both numbers pass through intact. }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  AMQP.Wire,
  AMQP.Server.Wal,
  AMQP.Server.Records;

type
  { A surviving bind, with the destination type that distinguishes it. }
  TAMQPRecoveredBinding = record
    Binding: TAMQPRecBinding;
    /// True = exchange->exchange (D5); False = exchange->queue.
    DestinationIsExchange: Boolean;
  end;

  { Replay counts, for the broker log and for the tests. }
  TAMQPRecoveryStats = record
    /// Segments opened.
    Segments: Integer;
    /// Intact records read.
    Records: Integer;
    /// Why reading the LAST segment stopped. awsFim = whole log.
    StopReason: TAMQPWalStop;
    /// Placements that stayed alive.
    Live: Integer;
    /// Placements retired by a DEQ.
    Retired: Integer;
    /// Placements dropped because their queue was deleted later.
    OrphanedFromQueue: Integer;
    /// Placements that appeared TWICE in the log and were unified. It is not
    /// an anomaly: it is the price -- and the proof -- of compaction being
    /// crash-safe. It rewrites the live records with the SAME EntryId before
    /// deleting the old segments, so a crash in the middle leaves both
    /// copies, and the replay has to treat them as ONE.
    Duplicates: Integer;
    /// Placements dropped because the content is not in the log. It should not
    /// happen (CONTENT and ENQ go in the SAME indivisible batch), and that is why it is
    /// counted instead of ignored: if it is ever non-zero, the defect
    /// is in the writer, not here.
    OrphanedContent: Integer;
  end;

  { The state the log describes. Owns everything it created -- including the
    argument TAMQPFieldTables, which the decoders allocate. }
  TAMQPRecoveredState = class
  private
    FExchanges: TList<TAMQPRecExchange>;
    FQueues: TList<TAMQPRecQueue>;
    FBindings: TList<TAMQPRecoveredBinding>;
    // Placements in LOG ORDER. A retired one becomes a TOMBSTONE (EntryId = 0, which
    // no real placement uses -- the engine counter starts at 1) instead
    // of leaving the list: removing from the middle would cost O(n) per DEQ, and the log
    // of a long-lived broker has one DEQ for every ENQ. The tombstones vanish in a
    // single sweep, at the end of the replay.
    FEntries: TList<TAMQPRecEnqueue>;
    /// EntryId -> position in FEntries, LIVE ones only. It is what makes unifying
    /// a duplicate and finding a DEQ's target cost O(1) instead of O(live).
    FLive: TDictionary<UInt64, Integer>;
    FTombstones: Integer;
    FContents: TDictionary<UInt64, TAMQPRecContent>;
    FStats: TAMQPRecoveryStats;
    FMaxId: UInt64;
    function FindExchange(const AVHost, AName: string): Integer;
    function FindQueue(const AVHost, AName: string): Integer;
    function FindBinding(const AVHost, ASource, ADestination,
      ARoutingKey: string; ADestinationIsExchange: Boolean): Integer;
    procedure RemoveBindingsFrom(const AVHost, AName: string; AIsQueue: Boolean);
    procedure Apply(const ARec: TAMQPWalRecord);
    procedure TrackId(AId: UInt64);
    /// Marks the placement at position AIndex as dead, without removing it from the list.
    procedure Tombstone(AIndex: Integer);
    /// Removes the tombstones, preserving order. One pass, at the end of the replay.
    procedure SweepTombstones;
  public
    constructor Create;
    destructor Destroy; override;

    /// Surviving durable exchanges, in the order they were declared.
    property Exchanges: TList<TAMQPRecExchange> read FExchanges;
    /// Surviving durable queues, in the order they were declared.
    property Queues: TList<TAMQPRecQueue> read FQueues;
    /// Surviving bindings (queue and exchange), in creation order.
    property Bindings: TList<TAMQPRecoveredBinding> read FBindings;
    /// Live placements, IN LOG ORDER -- which is the enqueue order.
    property Entries: TList<TAMQPRecEnqueue> read FEntries;
    /// ContentId -> body. Only those referenced by Entries survive.
    property Contents: TDictionary<UInt64, TAMQPRecContent> read FContents;
    /// Highest EntryId/ContentId seen. The engine counter has to continue
    /// AFTER this number.
    property MaxId: UInt64 read FMaxId;
    property Stats: TAMQPRecoveryStats read FStats;
  end;

/// Reads the whole WAL in ADir and returns the state it describes. Never raises
/// because of a torn tail -- that is the NORMAL case after a crash, and the valid prefix
/// is exactly what D23 says to replay. An empty directory returns an empty
/// state.
///
/// The caller owns the result.
function AmqpReplayWal(const ADir: string): TAMQPRecoveredState;

implementation

{ TAMQPRecoveredState }

constructor TAMQPRecoveredState.Create;
begin
  inherited Create;
  FExchanges := TList<TAMQPRecExchange>.Create;
  FQueues := TList<TAMQPRecQueue>.Create;
  FBindings := TList<TAMQPRecoveredBinding>.Create;
  FEntries := TList<TAMQPRecEnqueue>.Create;
  FLive := TDictionary<UInt64, Integer>.Create;
  FContents := TDictionary<UInt64, TAMQPRecContent>.Create;
end;

destructor TAMQPRecoveredState.Destroy;
var
  I: Integer;
  LPair: TPair<UInt64, TAMQPRecContent>;
begin
  // The argument tables are created by the decoders; ownership belongs to
  // the caller, and here that means us.
  for I := 0 to FExchanges.Count - 1 do
    FExchanges[I].Arguments.Free;
  for I := 0 to FQueues.Count - 1 do
    FQueues[I].Arguments.Free;
  for I := 0 to FBindings.Count - 1 do
    FBindings[I].Binding.Arguments.Free;
  FExchanges.Free;
  FQueues.Free;
  FBindings.Free;
  FEntries.Free;
  FLive.Free;
  FContents.Free;
  inherited;
end;

// EntryId = 0 is the tombstone: no real placement uses that number, because the
// engine counter does Inc BEFORE returning and therefore starts at 1.
procedure TAMQPRecoveredState.Tombstone(AIndex: Integer);
var
  LEnq: TAMQPRecEnqueue;
begin
  LEnq := FEntries[AIndex];
  FLive.Remove(LEnq.EntryId);
  LEnq.EntryId := 0;
  FEntries[AIndex] := LEnq;
  Inc(FTombstones);
end;

procedure TAMQPRecoveredState.SweepTombstones;
var
  I, LDestination: Integer;
begin
  if FTombstones = 0 then
    Exit;
  LDestination := 0;
  for I := 0 to FEntries.Count - 1 do
    if FEntries[I].EntryId <> 0 then
    begin
      if LDestination <> I then
        FEntries[LDestination] := FEntries[I];
      Inc(LDestination);
    end;
  FEntries.Count := LDestination;
  FLive.Clear; // the positions changed; nobody queries the index anymore
  FTombstones := 0;
end;

procedure TAMQPRecoveredState.TrackId(AId: UInt64);
begin
  if AId > FMaxId then
    FMaxId := AId;
end;

function TAMQPRecoveredState.FindExchange(const AVHost, AName: string): Integer;
var
  I: Integer;
begin
  for I := 0 to FExchanges.Count - 1 do
    if (FExchanges[I].VHost = AVHost) and (FExchanges[I].Name = AName) then
      Exit(I);
  Result := -1;
end;

function TAMQPRecoveredState.FindQueue(const AVHost, AName: string): Integer;
var
  I: Integer;
begin
  for I := 0 to FQueues.Count - 1 do
    if (FQueues[I].VHost = AVHost) and (FQueues[I].Name = AName) then
      Exit(I);
  Result := -1;
end;

function TAMQPRecoveredState.FindBinding(const AVHost, ASource, ADestination,
  ARoutingKey: string; ADestinationIsExchange: Boolean): Integer;
var
  I: Integer;
  LB: TAMQPRecoveredBinding;
begin
  for I := 0 to FBindings.Count - 1 do
  begin
    LB := FBindings[I];
    if (LB.DestinationIsExchange = ADestinationIsExchange)
      and (LB.Binding.VHost = AVHost)
      and (LB.Binding.Source = ASource)
      and (LB.Binding.Destination = ADestination)
      and (LB.Binding.RoutingKey = ARoutingKey) then
      Exit(I);
  end;
  Result := -1;
end;

// Deleting a resource deletes the bindings it appears in -- on BOTH sides,
// because an exchange may be the source of some and the destination of others (D5).
procedure TAMQPRecoveredState.RemoveBindingsFrom(const AVHost, AName: string;
  AIsQueue: Boolean);
var
  I: Integer;
  LB: TAMQPRecoveredBinding;
begin
  for I := FBindings.Count - 1 downto 0 do
  begin
    LB := FBindings[I];
    if LB.Binding.VHost <> AVHost then
      Continue;
    if AIsQueue then
    begin
      if (not LB.DestinationIsExchange) and (LB.Binding.Destination = AName) then
      begin
        LB.Binding.Arguments.Free;
        FBindings.Delete(I);
      end;
    end
    else if (LB.Binding.Source = AName)
      or (LB.DestinationIsExchange and (LB.Binding.Destination = AName)) then
    begin
      LB.Binding.Arguments.Free;
      FBindings.Delete(I);
    end;
  end;
end;

procedure TAMQPRecoveredState.Apply(const ARec: TAMQPWalRecord);
var
  LEx: TAMQPRecExchange;
  LQ: TAMQPRecQueue;
  LB: TAMQPRecoveredBinding;
  LName: TAMQPRecName;
  LCount: TAMQPRecContent;
  LEnq: TAMQPRecEnqueue;
  LDeq: TAMQPRecDequeue;
  I: Integer;
begin
  case ARec.Kind of
    AMQP_REC_EXCHANGE_DECLARE:
      begin
        LEx := AmqpDecodeRecExchange(ARec.Payload);
        I := FindExchange(LEx.VHost, LEx.Name);
        if I >= 0 then
        begin
          // Redeclare: the last one wins. (The engine already refused incompatibility
          // back then -- the log only keeps what was accepted.)
          FExchanges[I].Arguments.Free;
          FExchanges[I] := LEx;
        end
        else
          FExchanges.Add(LEx);
      end;
    AMQP_REC_EXCHANGE_DELETE:
      begin
        LName := AmqpDecodeRecName(ARec.Payload);
        I := FindExchange(LName.VHost, LName.Name);
        if I >= 0 then
        begin
          FExchanges[I].Arguments.Free;
          FExchanges.Delete(I);
        end;
        RemoveBindingsFrom(LName.VHost, LName.Name, False);
      end;
    AMQP_REC_QUEUE_DECLARE:
      begin
        LQ := AmqpDecodeRecQueue(ARec.Payload);
        I := FindQueue(LQ.VHost, LQ.Name);
        if I >= 0 then
        begin
          FQueues[I].Arguments.Free;
          FQueues[I] := LQ;
        end
        else
          FQueues.Add(LQ);
      end;
    AMQP_REC_QUEUE_DELETE:
      begin
        LName := AmqpDecodeRecName(ARec.Payload);
        I := FindQueue(LName.VHost, LName.Name);
        if I >= 0 then
        begin
          FQueues[I].Arguments.Free;
          FQueues.Delete(I);
        end;
        RemoveBindingsFrom(LName.VHost, LName.Name, True);
        // ...and what was pending in it goes too: resurrecting a message of a
        // deleted queue would be worse than losing it.
        for I := FEntries.Count - 1 downto 0 do
          if (FEntries[I].EntryId <> 0)
            and (FEntries[I].VHost = LName.VHost)
            and (FEntries[I].Queue = LName.Name) then
          begin
            Tombstone(I);
            Inc(FStats.OrphanedFromQueue);
          end;
      end;
    AMQP_REC_QUEUE_BIND, AMQP_REC_EXCHANGE_BIND:
      begin
        LB.Binding := AmqpDecodeRecBinding(ARec.Payload);
        LB.DestinationIsExchange := ARec.Kind = AMQP_REC_EXCHANGE_BIND;
        I := FindBinding(LB.Binding.VHost, LB.Binding.Source,
          LB.Binding.Destination, LB.Binding.RoutingKey, LB.DestinationIsExchange);
        if I >= 0 then
        begin
          // Rebind of the same triple: idempotent, and the last one wins.
          FBindings[I].Binding.Arguments.Free;
          FBindings[I] := LB;
        end
        else
          FBindings.Add(LB);
      end;
    AMQP_REC_QUEUE_UNBIND, AMQP_REC_EXCHANGE_UNBIND:
      begin
        LB.Binding := AmqpDecodeRecBinding(ARec.Payload);
        try
          I := FindBinding(LB.Binding.VHost, LB.Binding.Source,
            LB.Binding.Destination, LB.Binding.RoutingKey,
            ARec.Kind = AMQP_REC_EXCHANGE_UNBIND);
          if I >= 0 then
          begin
            FBindings[I].Binding.Arguments.Free;
            FBindings.Delete(I);
          end;
        finally
          LB.Binding.Arguments.Free; // the unbind only needed the key
        end;
      end;
    AMQP_REC_CONTENT:
      begin
        LCount := AmqpDecodeRecContent(ARec.Payload);
        TrackId(LCount.ContentId);
        FContents.AddOrSetValue(LCount.ContentId, LCount);
      end;
    AMQP_REC_ENQUEUE:
      begin
        LEnq := AmqpDecodeRecEnqueue(ARec.Payload);
        TrackId(LEnq.EntryId);
        // SAME PLACEMENT TWICE = ONE. It is what makes the compaction rewrite
        // crash-safe: it rewrites the live data with the SAME EntryId
        // and only AFTERWARDS deletes the old segments, so a crash in the middle
        // leaves both copies in the log. Without this, every interrupted
        // compaction would duplicate the whole backlog.
        //
        // The FIRST occurrence stays: the log order is the enqueue
        // order (D27), and the old copy is the one at the right position
        // in the queue. Both are equal field by field, so the choice only
        // matters for the POSITION.
        if FLive.ContainsKey(LEnq.EntryId) then
          Inc(FStats.Duplicates)
        else
        begin
          FLive.Add(LEnq.EntryId, FEntries.Count);
          FEntries.Add(LEnq);
        end;
      end;
    AMQP_REC_DEQUEUE:
      begin
        LDeq := AmqpDecodeRecDequeue(ARec.Payload);
        // The EntryId is unique in the broker (it comes from ONE engine counter), so
        // it alone identifies the placement -- vhost and queue come as a bonus.
        if FLive.TryGetValue(LDeq.EntryId, I) then
        begin
          Tombstone(I);
          Inc(FStats.Retired);
        end;
      end;
  end;
end;

function AmqpReplayWal(const ADir: string): TAMQPRecoveredState;
var
  LSegs: TArray<Cardinal>;
  LWalFile: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LWalRecords: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LN, I, J: Integer;
  LDir: string;
  LUsed: TDictionary<UInt64, Byte>;
  LRelease: TArray<UInt64>;
  LKey: UInt64;
begin
  Result := TAMQPRecoveredState.Create;
  Result.FStats.StopReason := awsEnd;
  if (ADir = '') or (not DirectoryExists(ADir)) then
    Exit;
  LDir := IncludeTrailingPathDelimiter(ADir);
  LSegs := AmqpWalListSegments(ADir);
  // AmqpWalListSegments returns in ascending number order, and the number grows
  // over time -- so walking in the given order IS walking in LSN order.
  for I := 0 to High(LSegs) do
  begin
    LWalFile := TAMQPWalOsFile.Create(LDir + AmqpWalSegmentName(LSegs[I]), False);
    LSeg := TAMQPWalSegment.OpenExisting(LWalFile, False);
    try
      LN := LSeg.ReadPrefix(LWalRecords, LStop);
      Inc(Result.FStats.Segments);
      Inc(Result.FStats.Records, LN);
      Result.FStats.StopReason := LStop;
      for J := 0 to LN - 1 do
        Result.Apply(LWalRecords[J]);
    finally
      LSeg.Free;
      LWalFile := nil;
    end;
  end;

  // The tombstones leave HERE, in a single pass: during the replay they stay so
  // the index positions remain valid.
  Result.SweepTombstones;

  // Content that no live placement references matters to nobody --
  // it is the body of an already consumed message, still in the file only because the
  // log is append-only. Releasing it here is what keeps recovery from loading
  // the whole lifetime backlog of the broker into memory.
  LUsed := TDictionary<UInt64, Byte>.Create;
  try
    for I := Result.FEntries.Count - 1 downto 0 do
      if Result.FContents.ContainsKey(Result.FEntries[I].ContentId) then
        LUsed.AddOrSetValue(Result.FEntries[I].ContentId, 0)
      else
      begin
        // CONTENT and ENQ go in the SAME indivisible batch, so this should not
        // happen. Counted instead of ignored -- see TAMQPRecoveryStats.
        Result.FEntries.Delete(I);
        Inc(Result.FStats.OrphanedContent);
      end;
    // Keys to release collected BEFORE deleting: touching a dictionary during
    // its own enumeration is undefined behavior on both compilers.
    LRelease := nil;
    for LKey in Result.FContents.Keys do
      if not LUsed.ContainsKey(LKey) then
      begin
        SetLength(LRelease, Length(LRelease) + 1);
        LRelease[High(LRelease)] := LKey;
      end;
    for I := 0 to High(LRelease) do
      Result.FContents.Remove(LRelease[I]);
  finally
    LUsed.Free;
  end;
  Result.FStats.Live := Result.FEntries.Count;
end;

end.
