unit AMQP.Server.Journal;

{$I amqp.inc}

{ The journal thread and group commit -- broker sub-module, WS2 of Phase 4
  (durability, see CLAUDE.md, decisions D19-D28).

  This is the unit that turns "writing to the file" (WS1, AMQP.Server.Wal)
  into "promising durability": it numbers the records, gathers what arrived
  into a batch, writes, does ONE fsync and only then advances the WATERMARK --
  the number up to which a confirm may be released (D24).

  THE LOOP, WHICH IS ALL OF D25
  -----------------------------
      wait until there is something in the queue
      take EVERYTHING that is there
      write the records
      ONE fsync
      advance the watermark and notify
      repeat

  There is no wait window and no configurable batch size, and that is
  deliberate: under low load the batch is 1 and the latency is that of a lone
  fsync; under high load the batch grows by itself, because what arrives
  DURING an fsync is exactly the next batch. The gain measured in WS0
  (35x-50x on Windows, ~250x on the VM's Linux) comes from that alone -- and
  it grows the worse the disk is, which is the property you want from a
  self-tuning batch.

  WHO MAY BLOCK, AND WHO MAY NOT (D24, corollary c)
  -------------------------------------------------
  The submission queue has a cap, and backpressure lives in the PUBLISHER:

    Submit        BLOCKS while the queue is above the cap. The caller is the
                  connection's read thread, and making it stop reading that
                  connection is exactly the desired backpressure -- the same
                  shape as the writer's outbound queue.
    SubmitNoWait  NEVER blocks and NEVER refuses: it is how the queue ACTOR
                  writes the retirements only it decides (ack, drop-head,
                  expiration, dead-letter). D2 forbids the actor from waiting
                  on I/O, and the volume of these records is bounded by the
                  number of live entries -- there is no way for the actor
                  alone to fill the queue forever.

  THE SINK MAY NOT BLOCK, for the same reason the actor may not: it runs ON
  the journal thread, and holding it stalls the whole group commit.

  FAILURE CLOSES THE DOOR, IT DOES NOT LOWER THE GUARD
  ----------------------------------------------------
  An fsync that fails does NOT advance the watermark -- the records stay
  pending and the next batch tries again (the bytes are already in the file;
  a later fsync covers the earlier ones). A failed WRITE is another matter:
  the record is nowhere, and there is no way to restore the promise. In that
  case the journal enters a FAILED state, permanently: Submit starts raising,
  WaitDurable returns False, and the sink is notified once. A broker that can
  no longer write has to say so, not keep accepting persistent publishes as if
  nothing had happened.

  WHAT IS NOT HERE
  ----------------
  COMPACTION (WS7, D26) -- why it is crash-safe without a transaction
  -------------------------------------------------------------------
  Compacting is rewriting what is still live and deleting the rest. D26 asks
  for ONE mechanism only and no snapshot file, and the way to get both is to
  reuse the REPLAY: the live set is exactly what recovery already knows how to
  compute (AMQP.Server.Recovery).

  Order is what replaces the transaction, as in D23:
    1. close the active segment (from then on nothing is written to the old ones);
    2. read the whole log and find out what is live;
    3. write all of it to a NEW segment, with the SAME EntryId/ContentId;
    4. fsync;
    5. only then delete the old segments.

  A crash at ANY point is safe, and the reason is in step 3: since the
  identifiers are preserved, a log containing both copies describes the SAME
  state -- replay unifies a repeated placement by EntryId. Crashing between
  3 and 5 leaves twice the bytes, never twice the messages. Nothing is lost
  because the new one only becomes truth after the fsync, and the old one only
  goes away after that.

  WHEN IT MAY RUN -- and why there is NO automatic trigger
  --------------------------------------------------------
  Only with the journal QUIESCENT: nothing submitted and not yet written.
  Today that means INSIDE Start, after recovery and before the listening
  socket opens, where there is no publisher and no actor.

  The first version fired compaction on rotation, in mid-operation, and THAT
  WAS WRONG -- the test caught it. The LSN is assigned in Submit, on the
  publisher's thread; compaction runs later, on the journal thread, and
  assigns NEW LSNs to the records it rewrites. A publish that got its LSN
  BEFORE compaction and is still in the queue is written AFTER it -- with an
  LSN lower than the file's last one. Append refuses (WS1's increasing-LSN
  invariant), the write fails and the whole journal goes to the failed state.

  There is no cheap fix: LSN order has to be file order, and the compacted
  content is LOGICALLY OLDER than any concurrent submit. Compacting in flight
  requires quiescing LSN allocation -- which runs into D2, which forbids the
  actor from waiting. It is left as work of its own; what exists today bounds
  growth PER RESTART, not in flight.

  Before rotation there was ONE active
  segment and it was not rolled when it passed the target size. The registry
  of pending confirms per channel is WS5 -- here there is only the seam
  (IAMQPDurabilitySink) and the watermark it consults. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Server.Events,
  AMQP.Server.Wal,
  AMQP.Server.Records,
  AMQP.Server.Recovery;

const
  /// Cap of the submission queue, in payload bytes. Above it Submit BLOCKS
  /// (the actor's SubmitNoWait ignores it). Four megabytes hold several
  /// thousand typical records -- deep enough for group commit to grow,
  /// shallow enough for backpressure to reach the publisher before memory
  /// becomes a problem.
  AMQP_JOURNAL_MAX_PENDING_BYTES = 4 * 1024 * 1024;

  /// How long a blocked Submit waits before giving up. Generous: the journal
  /// waits on no peer, only the disk, so exceeding this means a stalled disk
  /// or a bug of ours.
  AMQP_JOURNAL_SUBMIT_TIMEOUT_MS = 30000;

  /// How many times LARGER than the live content the log may get before the
  /// journal compacts itself. Four is D26's 25% live fraction, written from
  /// this side: after each compaction the next threshold becomes four times
  /// what was left, so a genuinely LIVE log is never recompacted for nothing
  /// -- it only fires again when it quadruples again. No time parameter, in
  /// the shape of D25.
  AMQP_JOURNAL_COMPACT_FACTOR = 4;

  /// How long Stop waits for the thread to drain what was already accepted.
  AMQP_JOURNAL_STOP_TIMEOUT_MS = 30000;

type
  EAMQPJournal = class(Exception);

  /// A record to write. Kind is the same opaque field from WS1 -- its meaning
  /// (D22: CONTENT, ENQ, DEQ, TOPO) is given by WS3/WS4.
  TAMQPJournalRecord = record
    Kind: Byte;
    Payload: TBytes;
  end;

  TAMQPJournalRecords = array of TAMQPJournalRecord;

  { Whoever wants to know that an LSN became durable. Implemented in WS5 by the
    registry of pending confirms.

    MAY NOT BLOCK OR DO I/O: it runs on the journal thread, between the fsync
    and the next batch. It is the same rule as the queue actor's (D2), for the
    same reason. }
  IAMQPDurabilitySink = interface
    ['{4A1D7E90-2C63-4B58-8F07-D9E3A5C61B24}']
    /// Everything with LSN <= ALsn is on disk.
    procedure Durable(ALsn: UInt64);
    /// The journal will not make anything durable anymore. Called ONCE.
    procedure JournalFailed(const AMessage: string);
  end;

  { Journal counters. They go together in a record read UNDER THE LOCK, not as
    loose properties, for a measured and not aesthetic reason: this project's
    Delphi build is Win32, where Inc() on an Int64 is a NON-atomic
    read-modify-write -- a test thread reading a counter while the journal
    thread increments it could see half a value. It is the same reason the
    PcAtomic*64 exist in PascalCommon.Threading. }
  TAMQPJournalStats = record
    /// Loop rounds that wrote something.
    Batches: Int64;
    /// fsyncs attempted. Records/Syncs is the average batch size -- it is this
    /// number that proves group commit is grouping.
    Syncs: Int64;
    /// fsyncs that returned False (did not advance the watermark).
    FailedSyncs: Int64;
    Records: Int64;
    /// Largest batch ever written in one round.
    MaxBatchSize: Integer;
    /// How many times the full segment was closed and another opened (D26).
    Rotations: Int64;
    /// How many compactions ran, and how many segments they deleted.
    Compactions: Int64;
    DeletedSegments: Int64;
    /// Persistent publishes refused by the disk cap (D26).
    Refused: Int64;
    /// Approximate size of the log now.
    Bytes: Int64;
    /// Payload bytes waiting in the submission queue NOW.
    PendingBytes: Int64;
    DurableLsn: UInt64;
    /// LSN the next submitted record will get.
    NextLsn: UInt64;
  end;

  TAMQPJournal = class;

  { The thread that writes. One per journal, persistent, no FreeOnTerminate --
    the same shape as the broker's TAMQPMonitorThread. }
  TAMQPJournalThread = class(TThread)
  private
    FJournal: TAMQPJournal;
  protected
    procedure Execute; override;
  public
    constructor Create(AJournal: TAMQPJournal);
  end;

  { An item in the submission queue. The LSN comes already assigned: Submit
    gives it, under the lock, because the publisher needs it BEFORE returning
    (D24 -- it is the number it keeps in the pending confirm). }
  TAMQPJournalItem = record
    Lsn: UInt64;
    Kind: Byte;
    Payload: TBytes;
  end;

  TAMQPJournal = class
  private
    FDir: string;
    FLock: TAMQPWalDirLock;
    FSegment: TAMQPWalSegment;
    FFile: IAMQPWalFile;
    FThread: TAMQPJournalThread;
    FMon: TPcMonitor;
    FPendings: TQueue<TAMQPJournalItem>;
    FPendingBytes: Int64;
    FMaxPendingBytes: Int64;
    FEvents: IAMQPEventSink;
    FMaxSegmentBytes: Int64;
    FCompactAbove: Int64;
    FMaxJournalBytes: Int64;
    /// Approximate size of the log NOW, kept by the journal thread and read
    /// without a lock by the publishers. Approximate on purpose: a disk cap
    /// does not need byte precision, and scanning the directory on the
    /// publish hot path is what would be wrong.
    FTotalBytes: UInt64;
    FClosedBytes: UInt64;
    FRefused: Int64;
    FSegNo: Cardinal;
    FNextLsn: UInt64;
    FDurableLsn: UInt64;   // atomic -- read outside the lock
    FRunning: Boolean;
    FStopping: Boolean;
    FFailed: Boolean;
    FError: string;
    FSink: IAMQPDurabilitySink;
    FBatches: Int64;
    FRotations: Int64;
    FCompactions: Int64;
    FDeletedSegments: Int64;
    FSyncs: Int64;
    FRecords: Int64;
    FMaxBatchSize: Integer;
    FFailedSyncs: Int64;
    procedure OpenOrRecover;
    procedure MarkFailed(const AMsg: string);
    function InternalSubmit(const ARecs: array of TAMQPJournalRecord;
      AWaitVacancy: Boolean): UInt64;
  protected
    /// One loop round: drains, writes, syncs, advances the watermark.
    /// Returns False when there is nothing left to do and a stop was requested.
    function ProcessSingleBatch: Boolean;
    /// Exact point between "took the batch from the queue" and "wrote" -- the
    /// window in which a concurrent Submit decides whether it goes into this
    /// batch or the next. No-op in production; the test overrides it to post
    /// from another thread inside it. MAY NOT RAISE. (Same idea as the queue's
    /// ActorRoundEnding.)
    procedure BatchTaken; virtual;
    /// The file seam of the active segment. Virtual so the test can inject a
    /// test double that counts fsyncs and fails at the chosen moment (layer 2
    /// of D28).
    function CreateFile(const APath: string; ACreate: Boolean): IAMQPWalFile; virtual;
    /// Closes the full segment and opens the next. Called ONLY at a batch
    /// boundary -- see the comment at the call site.
    procedure Rotate;
    /// Opens a new segment (the next number) and makes it the active one.
    procedure OpenNextSegment;
    /// Recomputes FBytesTotal. Only on the journal thread.
    procedure UpdateSize;
    /// Writes the live records to the active segment, with the identifiers
    /// PRESERVED. Returns the LSN of the LAST one written (0 if it wrote
    /// nothing) -- it is the number the watermark has to reach after the
    /// compaction's fsync.
    function WriteLiveRecords(AState: TAMQPRecoveredState): UInt64;
  public
    /// ADir is the DataDir. The directory's exclusive lock is taken in Start,
    /// not here -- constructing a journal must not steal the directory from a
    /// broker that is still up.
    constructor Create(const ADir: string);
    destructor Destroy; override;

    /// Takes the directory lock, opens (or creates) the active segment,
    /// recovers the next LSN from what was already written and starts the
    /// thread. Idempotent.
    procedure Start;
    /// Drains what was already accepted, syncs one last time and stops the
    /// thread. Idempotent; called by the destructor.
    procedure Stop;

    /// Enqueues ARecs as an INDIVISIBLE BATCH (contiguous LSNs, in the given
    /// order) and returns the LSN of the LAST one -- the number the caller
    /// keeps to know when the batch became durable. BLOCKS while the queue is
    /// above the cap. Raises if the journal is stopped or failed.
    function Submit(const ARecs: array of TAMQPJournalRecord): UInt64;
    /// Same, but NEVER blocks and NEVER refuses -- it is how the actor writes
    /// (see the unit header).
    function SubmitNoWait(const ARecs: array of TAMQPJournalRecord): UInt64;

    /// Everything with LSN <= this is on disk. Atomic read, no lock.
    function DurableLsn: UInt64;
    /// Waits for ALsn to become durable. False on timeout or if the journal fails.
    function WaitDurable(ALsn: UInt64; ATimeoutMs: Cardinal): Boolean;

    property Dir: string read FDir;
    property Running: Boolean read FRunning;
    /// True after a WRITE failure -- permanent (see the header).
    property Failed: Boolean read FFailed;
    property LastError: string read FError;
    /// Who is notified on each successful fsync. Injected before Start.
    property DurabilitySink: IAMQPDurabilitySink read FSink write FSink;
    property MaxPendingBytes: Int64 read FMaxPendingBytes
      write FMaxPendingBytes;
    /// Observability sink (Phase 4.1, Inc. 2). Emits ONE event per BATCH that
    /// became durable, never one per record: D25's group commit exists
    /// precisely because fsync is expensive, and an event per record would
    /// cost more than the fsync it observes (non-goal of D36).
    property Events: IAMQPEventSink read FEvents write FEvents;
    /// How much a segment grows before the journal closes it and opens the
    /// next (D26). Changing this only makes sense in tests: the default is the WAL's.
    property MaxSegmentBytes: Int64 read FMaxSegmentBytes
      write FMaxSegmentBytes;
    /// Total size from which Start compacts the log. 0 turns it off.
    /// There is NO in-flight compaction -- see the unit header.
    property CompactAbove: Int64 read FCompactAbove
      write FCompactAbove;
    /// HARD CAP of the log in bytes. 0 (default) = unlimited, the shape of D7.
    /// When exceeded, the PERSISTENT publish is refused -- see Cheio.
    property MaxJournalBytes: Int64 read FMaxJournalBytes
      write FMaxJournalBytes;

    /// Counts one more publish refused by the disk cap.
    procedure IncrementRefused;
    /// True when the log has reached MaxJournalBytes.
    ///
    /// THE DIFFERENCE THAT MATTERS COMPARED TO D7: the MEMORY cap drops from
    /// the head; the DISK cap **refuses**. Dropping on disk would mean deleting
    /// data the broker already confirmed as durable -- exactly the promise
    /// Phase 4 exists to keep. Refusing hands the decision back to the
    /// publisher, who still has the message in hand.
    ///
    /// Applies ONLY to the publisher path (Submit). A RETIREMENT record
    /// (SubmitNoWait, from the actor) is never refused: besides D24 forbidding
    /// the actor from being held back, refusing a DEQ would resurrect the
    /// message on the next boot -- and it is precisely the DEQs that let
    /// compaction shrink the log and get out of this situation.
    function IsFull: Boolean;
    /// Number of the ACTIVE segment. Grows with each rotation.
    function ActiveSegment: Cardinal;
    /// Sum of the sizes of all segments, in bytes.
    function TotalSize: Int64;

    /// Rewrites the log keeping only what is live and deletes the rest (D26).
    /// MAY ONLY BE CALLED BY THE JOURNAL THREAD (or with it stopped): it is
    /// the only one that writes, and compaction relies on that so it needs no
    /// lock at all over the file.
    ///
    /// False when there was nothing to do or the journal has failed; an I/O
    /// failure halfway marks the journal as failed, like any other.
    function Compact: Boolean;

    /// Counters, read under the lock (see TAMQPJournalStats).
    function Stats: TAMQPJournalStats;
  end;

implementation

{ TAMQPJournalThread }

constructor TAMQPJournalThread.Create(AJournal: TAMQPJournal);
begin
  FJournal := AJournal;
  inherited Create(False);
  {$IFDEF FPC}
  NameThreadForDebugging('amqp-journal');
  {$ELSE}
  NameThreadForDebugging('amqp-journal', ThreadID);
  {$ENDIF}
end;

procedure TAMQPJournalThread.Execute;
begin
  while FJournal.ProcessSingleBatch do
    ;
end;

{ TAMQPJournal }

constructor TAMQPJournal.Create(const ADir: string);
begin
  inherited Create;
  FDir := ADir;
  FMon := TPcMonitor.Create;
  FPendings := TQueue<TAMQPJournalItem>.Create;
  FMaxPendingBytes := AMQP_JOURNAL_MAX_PENDING_BYTES;
  FMaxSegmentBytes := AMQP_WAL_SEGMENT_BYTES;
  FCompactAbove := AMQP_WAL_SEGMENT_BYTES * AMQP_JOURNAL_COMPACT_FACTOR;
  FNextLsn := 1;
end;

destructor TAMQPJournal.Destroy;
begin
  Stop;
  FPendings.Free;
  FSegment.Free;
  FFile := nil;
  FLock.Free;
  FMon.Free;
  inherited Destroy;
end;

function TAMQPJournal.CreateFile(const APath: string;
  ACreate: Boolean): IAMQPWalFile;
begin
  Result := TAMQPWalOsFile.Create(APath, ACreate);
end;

procedure TAMQPJournal.OpenOrRecover;
var
  LSegs: TArray<Cardinal>;
  LNo: Cardinal;
  LPath: string;
begin
  LSegs := AmqpWalListSegments(FDir);
  if Length(LSegs) = 0 then
  begin
    LNo := 1;
    LPath := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LNo);
    FFile := CreateFile(LPath, True);
    FSegment := TAMQPWalSegment.CreateNew(FFile, LNo);
    FSegNo := LNo;
  end
  else
  begin
    // The LAST segment is the active one. Opening with trim drops the torn tail
    // a crash left -- that is what makes the next append reachable (WS1).
    LNo := LSegs[High(LSegs)];
    LPath := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LNo);
    FFile := CreateFile(LPath, False);
    FSegment := TAMQPWalSegment.OpenExisting(FFile, True);
    FSegNo := LNo;
  end;
  // THE LSN CONTINUES FROM WHERE IT STOPPED. Restarting from 1 after a restart
  // would make WS1's own Append raise (non-increasing LSN) -- and, worse, if it
  // did not raise, it would produce a file that recovery would truncate at the
  // point where it wrapped.
  FNextLsn := FSegment.LastLsn + 1;
  // The starting size comes from the DISK: the segments already there count
  // toward the cap from the first publish, not only after the first batch.
  FClosedBytes := 0;
  FTotalBytes := 0;
  if TotalSize > FSegment.EndOffset then
    FClosedBytes := UInt64(TotalSize - FSegment.EndOffset);
  UpdateSize;
  // What was already in the file is on disk by definition: nobody promises
  // anything about it now, but the watermark must not start BEHIND it.
  PcAtomicWrite64(FDurableLsn, FSegment.LastLsn);
end;

procedure TAMQPJournal.Start;
begin
  FMon.Enter;
  try
    if FRunning then
      Exit;
    FStopping := False;
    FFailed := False;
    FError := '';
  finally
    FMon.Leave;
  end;

  // Outside the lock: taking the directory lock and opening a file are I/O,
  // and can raise. If they do, the journal stays stopped and nothing was promised.
  FLock := TAMQPWalDirLock.Create(FDir);
  try
    OpenOrRecover;
  except
    on E: Exception do
    begin
      FreeAndNil(FLock);
      raise;
    end;
  end;

  FMon.Enter;
  try
    FRunning := True;
  finally
    FMon.Leave;
  end;
  FThread := TAMQPJournalThread.Create(Self);
end;

procedure TAMQPJournal.Stop;
begin
  FMon.Enter;
  try
    if not FRunning then
    begin
      FStopping := True;
      FMon.PulseAll;
      Exit;
    end;
    FStopping := True;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;

  if FThread <> nil then
  begin
    // The thread leaves the loop by itself after draining: RodaUmLote only
    // returns False when the queue is empty AND a stop was requested. That is
    // why there is NO "stop command" in the queue -- what was already accepted
    // still goes to disk. WaitFor cannot hang: the loop's wait has a 100 ms
    // timeout and rechecks FParando on every turn.
    FThread.WaitFor;
    FreeAndNil(FThread);
  end;

  FMon.Enter;
  try
    FRunning := False;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;

  FreeAndNil(FSegment);
  FFile := nil;
  FreeAndNil(FLock);
end;

procedure TAMQPJournal.MarkFailed(const AMsg: string);
var
  LSink: IAMQPDurabilitySink;
  LNotify: Boolean;
begin
  FMon.Enter;
  try
    LNotify := not FFailed;
    FFailed := True;
    if FError = '' then
      FError := AMsg;
    FMon.PulseAll; // releases whoever waits for room and whoever waits for durability
  finally
    FMon.Leave;
  end;
  LSink := FSink;
  if LNotify and (LSink <> nil) then
    try
      LSink.JournalFailed(AMsg);
    except
    end;
end;

function TAMQPJournal.InternalSubmit(const ARecs: array of TAMQPJournalRecord;
  AWaitVacancy: Boolean): UInt64;
var
  I: Integer;
  LItem: TAMQPJournalItem;
  LBytes: Int64;
  LDeadline: UInt64;
begin
  if Length(ARecs) = 0 then
    raise EAMQPJournal.Create('submit without any records');

  LBytes := 0;
  for I := 0 to High(ARecs) do
    Inc(LBytes, Length(ARecs[I].Payload));

  FMon.Enter;
  try
    if AWaitVacancy then
    begin
      // Backpressure: wait for the queue to drop below the cap. Rechecks in a
      // loop with a deadline, as TPcMonitor's contract requires (spurious wakeup).
      LDeadline := PcTickMs + AMQP_JOURNAL_SUBMIT_TIMEOUT_MS;
      while FRunning and (not FStopping) and (not FFailed)
        and (FPendingBytes >= FMaxPendingBytes) and (PcTickMs < LDeadline) do
        FMon.Wait(50);
    end;

    if FFailed then
      raise EAMQPJournal.CreateFmt('journal has failed: %s', [FError]);
    if (not FRunning) or FStopping then
      raise EAMQPJournal.Create('journal is stopped');
    if AWaitVacancy and (FPendingBytes >= FMaxPendingBytes) then
      raise EAMQPJournal.CreateFmt('journal queue has been full for more than %d ms '
        + '(%d pending bytes)',
        [AMQP_JOURNAL_SUBMIT_TIMEOUT_MS, FPendingBytes]);


    // Contiguous LSNs, assigned HERE: the publisher needs the number before
    // returning (D24). And the queue is ordered by LSN for free, because
    // assignment and insertion happen under the same lock -- that is what
    // makes the batch indivisible without any marker.
    Result := 0;
    for I := 0 to High(ARecs) do
    begin
      LItem.Lsn := FNextLsn;
      LItem.Kind := ARecs[I].Kind;
      LItem.Payload := ARecs[I].Payload;
      FPendings.Enqueue(LItem);
      Result := FNextLsn;
      Inc(FNextLsn);
    end;
    Inc(FPendingBytes, LBytes);
    FMon.PulseAll; // wakes the journal thread
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.Submit(const ARecs: array of TAMQPJournalRecord): UInt64;
begin
  Result := InternalSubmit(ARecs, True);
end;

function TAMQPJournal.SubmitNoWait(
  const ARecs: array of TAMQPJournalRecord): UInt64;
begin
  Result := InternalSubmit(ARecs, False);
end;

function TAMQPJournal.DurableLsn: UInt64;
begin
  Result := PcAtomicRead64(FDurableLsn);
end;

function TAMQPJournal.WaitDurable(ALsn: UInt64; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMon.Enter;
  try
    while (PcAtomicRead64(FDurableLsn) < ALsn) and (not FFailed)
      and (PcTickMs < LDeadline) do
      FMon.Wait(20);
    Result := (not FFailed) and (PcAtomicRead64(FDurableLsn) >= ALsn);
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.Stats: TAMQPJournalStats;
begin
  FMon.Enter;
  try
    Result.Batches := FBatches;
    Result.Rotations := FRotations;
    Result.Compactions := FCompactions;
    Result.DeletedSegments := FDeletedSegments;
    Result.Refused := FRefused;
    Result.Bytes := Int64(FTotalBytes);
    Result.Syncs := FSyncs;
    Result.FailedSyncs := FFailedSyncs;
    Result.Records := FRecords;
    Result.MaxBatchSize := FMaxBatchSize;
    Result.PendingBytes := FPendingBytes;
    Result.DurableLsn := PcAtomicRead64(FDurableLsn);
    Result.NextLsn := FNextLsn;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPJournal.BatchTaken;
begin
  // no-op in production -- see the declaration
end;

// Closes the full segment and opens the next. The LSN CONTINUES: numbering
// belongs to the whole journal, not to the file, and that is what lets
// recovery read the segments in sequence as if they were a single log.
procedure TAMQPJournal.OpenNextSegment;
var
  LPath: string;
begin
  if FSegment <> nil then
    Inc(FClosedBytes, UInt64(FSegment.EndOffset));
  FreeAndNil(FSegment);
  FFile := nil; // closes the previous file
  Inc(FSegNo);
  LPath := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(FSegNo);
  FFile := CreateFile(LPath, True);
  FSegment := TAMQPWalSegment.CreateNew(FFile, FSegNo);
  UpdateSize;
end;

procedure TAMQPJournal.Rotate;
begin
  OpenNextSegment;
  FMon.Enter;
  try
    Inc(FRotations);
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.TotalSize: Int64;
var
  LSegs: TArray<Cardinal>;
  I: Integer;
  LFile: TSearchRec;
  LName: string;
begin
  Result := 0;
  LSegs := AmqpWalListSegments(FDir);
  for I := 0 to High(LSegs) do
  begin
    LName := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LSegs[I]);
    if FindFirst(LName, faAnyFile, LFile) = 0 then
    begin
      Result := Result + LFile.Size;
      SysUtils.FindClose(LFile);
    end;
  end;
end;

// Rewrites the live set into the ACTIVE segment, with the identifiers
// PRESERVED -- that is what makes the rewrite idempotent and therefore
// crash-safe (see the unit header). The order repeats the original log's:
// topology first, then each content before the placement that uses it.
function TAMQPJournal.WriteLiveRecords(AState: TAMQPRecoveredState): UInt64;
var
  I: Integer;
  LEx: TAMQPRecExchange;
  LQ: TAMQPRecQueue;
  LB: TAMQPRecoveredBinding;
  LEnq: TAMQPRecEnqueue;
  LCount: TAMQPRecContent;
  LAlreadyWritten: TDictionary<UInt64, Byte>;

  procedure AppendRecord(AKind: Byte; const APayload: TBytes);
  var
    LLsn: UInt64;
  begin
    FMon.Enter;
    try
      LLsn := FNextLsn;
      Inc(FNextLsn);
    finally
      FMon.Leave;
    end;
    FSegment.Append(LLsn, AKind, APayload);
    Result := LLsn;
  end;

begin
  Result := 0;
  for I := 0 to AState.Exchanges.Count - 1 do
  begin
    LEx := AState.Exchanges[I];
    AppendRecord(AMQP_REC_EXCHANGE_DECLARE, AmqpEncodeRecExchange(LEx));
  end;
  for I := 0 to AState.Queues.Count - 1 do
  begin
    LQ := AState.Queues[I];
    AppendRecord(AMQP_REC_QUEUE_DECLARE, AmqpEncodeRecQueue(LQ));
  end;
  for I := 0 to AState.Bindings.Count - 1 do
  begin
    LB := AState.Bindings[I];
    if LB.DestinationIsExchange then
      AppendRecord(AMQP_REC_EXCHANGE_BIND, AmqpEncodeRecBinding(LB.Binding))
    else
      AppendRecord(AMQP_REC_QUEUE_BIND, AmqpEncodeRecBinding(LB.Binding));
  end;

  LAlreadyWritten := TDictionary<UInt64, Byte>.Create;
  try
    for I := 0 to AState.Entries.Count - 1 do
    begin
      LEnq := AState.Entries[I];
      // THE BODY ONLY ONCE, as in the original log (D22): N placements of the
      // same fan-out, and the dead-letter derivative, share the ContentId.
      if not LAlreadyWritten.ContainsKey(LEnq.ContentId) then
        if AState.Contents.TryGetValue(LEnq.ContentId, LCount) then
        begin
          AppendRecord(AMQP_REC_CONTENT, AmqpEncodeRecContent(LCount));
          LAlreadyWritten.Add(LEnq.ContentId, 0);
        end;
      AppendRecord(AMQP_REC_ENQUEUE, AmqpEncodeRecEnqueue(LEnq));
    end;
  finally
    LAlreadyWritten.Free;
  end;
end;

function TAMQPJournal.Compact: Boolean;
var
  LOlds: TArray<Cardinal>;
  LState: TAMQPRecoveredState;
  I: Integer;
  LPath: string;
  LLast: UInt64;
begin
  Result := False;
  if FFailed or (FSegment = nil) then
    Exit;

  LOlds := AmqpWalListSegments(FDir);
  if Length(LOlds) = 0 then
    Exit;

  try
    // (1) CLOSE THE ACTIVE ONE. From then on no old segment receives anything
    // else, and step 2's replay reads a log that has stopped growing.
    FreeAndNil(FSegment);
    FFile := nil;

    // (2) THE LIVE SET -- the SAME read as recovery, without a second
    // implementation of the semantics (D26: one mechanism only).
    LState := AmqpReplayWal(FDir);
    try
      // (3) THE NEW ONE, with the identifiers preserved.
      OpenNextSegment;
      LLast := WriteLiveRecords(LState);
    finally
      LState.Free;
    end;

    // (4) Only after the fsync is the new one true. If it fails, NOTHING is
    // deleted: the log stays larger than it needed to be, which is the right
    // mistake to make.
    if not FSegment.Sync then
      Exit;

    // THE WATERMARK HAS TO REACH WHAT COMPACTION WROTE. It consumed LSNs; if
    // the watermark stayed behind, whoever waited for one of them would wait
    // until the next publish batch -- which may never come. These records ARE
    // on disk: saying so is the honest thing.
    if LLast > 0 then
      PcAtomicWrite64(FDurableLsn, LLast);

    // (5) Now the old ones can go. A crash here leaves both copies, and replay
    // unifies them by EntryId -- twice the bytes, never twice the messages.
    for I := 0 to High(LOlds) do
    begin
      LPath := IncludeTrailingPathDelimiter(FDir)
        + AmqpWalSegmentName(LOlds[I]);
      if SysUtils.DeleteFile(LPath) then
        Inc(FDeletedSegments);
    end;
    // The old ones are gone: the only segment that counts now is the active one.
    FClosedBytes := 0;
    UpdateSize;
  except
    on E: Exception do
    begin
      MarkFailed(Format('journal compaction failed: %s', [E.Message]));
      Exit(False);
    end;
  end;

  FMon.Enter;
  try
    Inc(FCompactions);
  finally
    FMon.Leave;
  end;
  Result := True;
end;

procedure TAMQPJournal.IncrementRefused;
begin
  FMon.Enter;
  try
    Inc(FRefused);
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.IsFull: Boolean;
begin
  // Read without a lock, on purpose: it is a cap, not a money counter. The
  // worst case is accepting (or refusing) one publish at the exact boundary,
  // and the next one already sees the right number.
  Result := (FMaxJournalBytes > 0)
    and (Int64(PcAtomicRead64(FTotalBytes)) >= FMaxJournalBytes);
end;

// Recomputes the approximate size of the log. Runs ONLY on the journal thread,
// at the three moments it really changes: end of batch, rotation and
// compaction.
procedure TAMQPJournal.UpdateSize;
var
  LEnd: Int64;
begin
  LEnd := 0;
  if FSegment <> nil then
    LEnd := FSegment.EndOffset;
  PcAtomicWrite64(FTotalBytes, UInt64(Int64(FClosedBytes) + LEnd));
end;

function TAMQPJournal.ActiveSegment: Cardinal;
begin
  FMon.Enter;
  try
    Result := FSegNo;
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.ProcessSingleBatch: Boolean;
var
  LBatch: array of TAMQPJournalItem;
  LN, I: Integer;
  LMaxLsn: UInt64;
  LSink: IAMQPDurabilitySink;
  LSyncOk: Boolean;
  LEv: TAMQPServerEvent;
begin
  Result := True;
  LBatch := nil;
  LN := 0;

  FMon.Enter;
  try
    while (FPendings.Count = 0) and (not FStopping) and (not FFailed) do
      FMon.Wait(100);

    if FFailed then
      Exit(False);

    LN := FPendings.Count;
    if LN = 0 then
    begin
      // Empty queue AND stop requested: the thread exits. This is the ONLY
      // normal exit path, and it only happens after everything accepted has
      // been written -- that is what keeps Stop from losing records.
      if FStopping then
        Exit(False);
      Exit(True);
    end;

    // TAKE EVERYTHING -- that is group commit. What arrives during the write
    // is already the next batch, and that is how the batch size adjusts itself
    // to the load without any parameter (D25).
    SetLength(LBatch, LN);
    for I := 0 to LN - 1 do
      LBatch[I] := FPendings.Dequeue;
    FPendingBytes := 0;
    FMon.PulseAll; // releases whoever was blocked by backpressure
  finally
    FMon.Leave;
  end;

  BatchTaken;

  // --- outside the lock: I/O ---
  LMaxLsn := LBatch[LN - 1].Lsn;
  try
    for I := 0 to LN - 1 do
      FSegment.Append(LBatch[I].Lsn, LBatch[I].Kind, LBatch[I].Payload);
  except
    on E: Exception do
    begin
      // A failed write cannot be restored: the records are nowhere. Failed
      // state, permanent.
      MarkFailed(Format('journal write failed: %s', [E.Message]));
      Exit(False);
    end;
  end;

  // A failed fsync is NOT fatal: the bytes are in the file, and a later fsync
  // covers this batch. What must NOT happen is the watermark moving -- that
  // would promise durability nobody confirmed.
  LSyncOk := FSegment.Sync;
  if LSyncOk then
    PcAtomicWrite64(FDurableLsn, LMaxLsn);
  UpdateSize;

  // ROTATION, and ONLY HERE: at the batch boundary, after the fsync. Never in
  // the middle of a batch -- the batch is indivisible (D24) and splitting it
  // across two files would give an artificial torn tail in the first, which
  // recovery would drop together with half of an already accepted publish.
  //
  // After an fsync that FAILED there is no rotation: this batch's bytes are
  // not yet guaranteed on disk, and closing the file now would take away the
  // chance of a later fsync covering them.
  if LSyncOk and (FSegment.EndOffset >= FMaxSegmentBytes) then
  begin
    try
      Rotate;
    except
      on E: Exception do
      begin
        // Failing to open the next segment is a write failure: the next
        // records would have nowhere to go.
        MarkFailed(Format('segment rotation failed: %s', [E.Message]));
        Exit(False);
      end;
    end;

  end;

  FMon.Enter;
  try
    Inc(FBatches);
    Inc(FRecords, LN);
    Inc(FSyncs);
    if not LSyncOk then
      Inc(FFailedSyncs);
    if LN > FMaxBatchSize then
      FMaxBatchSize := LN;
    FMon.PulseAll; // wakes WaitDurable
  finally
    FMon.Leave;
  end;

  if not LSyncOk then
    Exit(True);

  if (FEvents <> nil) and FEvents.Wants(seJournalFlushed) then
  begin
    // After the fsync AND after the watermark moved: before that nothing is
    // durable, and the event would be promising what the disk did not confirm.
    LEv := AmqpNewEvent(seJournalFlushed);
    LEv.Lsn := LMaxLsn;
    LEv.Count := LN; // how many records this fsync covered
    FEvents.Emit(LEv);
  end;

  LSink := FSink;
  if LSink <> nil then
    try
      LSink.Durable(LMaxLsn);
    except
      // A sink that raises must not bring down the journal thread: the record
      // IS ALREADY durable and the watermark has already moved. Swallowing and
      // moving on is less bad than ceasing durability because of someone who
      // only wanted to be notified.
    end;
end;

end.
