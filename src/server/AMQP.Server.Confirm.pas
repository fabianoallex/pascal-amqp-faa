unit AMQP.Server.Confirm;

{$I amqp.inc}

{ Confirms deferred by the durability watermark -- broker sub-module,
  WS5 of Phase 4. This is D24 turned into code.

  The problem
  -----------
  Up to Phase 3 a publish's Basic.Ack went out right after RouteMessage, and
  that was RIGHT while everything was memory: enqueueing is synchronous,
  so when RouteMessage returns the message is already where it must be.
  With durability on, "where it must be" becomes the DISK, and the disk
  only answers after the fsync -- which the journal thread does in batches (D25).
  Confirming before that would be lying: the publisher would release the
  publish state believing the message survives a crash, and it would not.

  The design
  ----------
  The publishing side writes the batch and keeps the LSN of the last record (that
  is the engine, on the publisher thread -- see AMQP.Server.Engine.RouteMessage). The
  channel registers the (seq, lsn) pair HERE and emits NOTHING. After each fsync,
  the journal thread advances the watermark and calls Release on every registered
  tracker; each one releases what is already durable and COLLAPSES it into a single
  Basic.Ack(highestSeq, multiple=true) -- the letter of D9.

  Three things the design forces, and that live in this file:

  1. SEQ ORDER PER CHANNEL (corollary (a) of D24). A transient publish that
     arrives AFTER a persistent one still pending cannot jump ahead:
     it is deferred too (with Lsn=0) and goes out collapsed in the same multiple=true.
     That is why TryDefer defers when there is a pending one, not only when there is an LSN.

  2. NEVER BLOCK THE JOURNAL THREAD. Release uses TryPostFrames, and whatever the
     write queue refuses STAYS pending for the next round -- the shape of
     D3 applied to the confirm. Since a channel with a full output could never again
     see the watermark move (if nobody else published), the monitor thread calls
     RetryPending on every tick.

  3. LIFECYCLE. The journal thread does the releasing; the channel may have
     died in between. Same remedy as TAMQPChannelDeliveryTarget: the refcount
     keeps the object alive while the registry holds it, and Detach clears the
     writer UNDER LOCK, without waiting for I/O.

  Nack in the middle of the queue
  -------------------------------
  A Basic.Nack cannot be collapsed with multiple=true together with acks: it
  marks ONE seq. So the release emits the accumulated ack UP TO the previous seq,
  then the nack alone, and starts accumulating again. A nack does not wait for the
  disk (Lsn=0): it promises no durability at all -- it only has to respect the order. }

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
  AMQP.Server.Types,
  AMQP.Server.Journal;

type
  { A confirm that has not gone out yet. }
  TAMQPPendingConfirm = record
    /// The seq-no the client assigned to this publish.
    Seq: UInt64;
    /// The LSN that must be durable before it can be confirmed. Zero =
    /// nothing to wait for from the disk (only the order).
    Lsn: UInt64;
    /// True => goes out as Basic.Nack (full queue with reject-publish, or failed
    /// journal), and therefore alone, without multiple.
    Nack: Boolean;
  end;

  { Tracker for ONE channel. Created by the registry, referenced by the channel. }
  TAMQPConfirmTracker = class(TInterfacedObject, IAMQPConfirmTracker)
  private
    FLock: TCriticalSection;
    FWriter: TAMQPFrameWriter; // nil after Detach -- always under FLock
    FChannelNo: Word;
    // Pending entries in ASCENDING seq order. Since the seq-no is monotonic per
    // channel and only the connection's read thread registers, appending at the
    // end already keeps it sorted -- there is no sorting to do.
    FPend: TList<TAMQPPendingConfirm>;
    function TryPost(const APayload: TBytes): Boolean;
  public
    constructor Create(AWriter: TAMQPFrameWriter; AChannelNo: Word);
    destructor Destroy; override;

    // --- IAMQPConfirmTracker ---
    function TryDefer(ASeq, ALsn: UInt64; ANack: Boolean): Boolean;
    procedure Release(AMark: UInt64);
    procedure FailAll;
    procedure Detach;
    function PendingCount: Integer;
  end;

  { The registry for the whole broker, and the sink the journal thread calls. }
  TAMQPConfirmRegistry = class(TInterfacedObject, IAMQPConfirmRegistry,
    IAMQPDurabilitySink)
  private
    FLock: TCriticalSection;
    FTrackers: TList<IAMQPConfirmTracker>;
    FMark: UInt64; // atomic (Read64/Write64) -- read without lock
    /// A copy of the list, taken under lock, to walk OUTSIDE it: the
    /// release calls TryPostFrames, and holding the registry lock while
    /// writing to N channels would put the journal thread behind all of them.
    function Snapshot: TArray<IAMQPConfirmTracker>;
    /// Tells every tracker to release what can already go out with the mark at ALsn.
    /// Separate from Durable on purpose: Durable ADVANCES the mark (and only the
    /// journal thread calls it), while this only RELEASES -- it is what the
    /// monitor thread needs, and writing the mark from there would make it move
    /// BACKWARDS when the two threads crossed.
    procedure ReleaseAll(ALsn: UInt64);
  public
    constructor Create;
    destructor Destroy; override;

    // --- IAMQPConfirmRegistry ---
    function CreateTracker(AWriter: TAMQPFrameWriter;
      AChannelNo: Word): IAMQPConfirmTracker;
    procedure Unregister(const ATracker: IAMQPConfirmTracker);
    function Watermark: UInt64;
    function TrackerCount: Integer;
    procedure RetryPending;

    // --- IAMQPDurabilitySink (called by the journal thread) ---
    procedure Durable(ALsn: UInt64);
    procedure JournalFailed(const AMessage: string);
  end;

implementation

{ TAMQPConfirmTracker }

constructor TAMQPConfirmTracker.Create(AWriter: TAMQPFrameWriter;
  AChannelNo: Word);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FWriter := AWriter;
  FChannelNo := AChannelNo;
  FPend := TList<TAMQPPendingConfirm>.Create;
end;

destructor TAMQPConfirmTracker.Destroy;
begin
  FPend.Free;
  FLock.Free;
  inherited;
end;

// Called UNDER FLock. Does not block: whatever the write queue refuses comes back
// as False and the pending entry stays where it is.
function TAMQPConfirmTracker.TryPost(const APayload: TBytes): Boolean;
begin
  Result := (FWriter <> nil)
    and FWriter.TryPostFrames([TAMQPFrame.Create(AMQP_FRAME_METHOD,
      FChannelNo, APayload)]);
end;

function TAMQPConfirmTracker.TryDefer(ASeq, ALsn: UInt64;
  ANack: Boolean): Boolean;
var
  LItem: TAMQPPendingConfirm;
begin
  FLock.Enter;
  try
    if FWriter = nil then
    begin
      // Dead channel: defers nothing. The caller will not manage to emit
      // either, and that is exactly what has to happen.
      Result := False;
      Exit;
    end;
    // Corollary (a) of D24: with a pending one ahead, EVEN the transient waits.
    Result := (ALsn > 0) or (FPend.Count > 0);
    if not Result then
      Exit;
    LItem.Seq := ASeq;
    LItem.Lsn := ALsn;
    LItem.Nack := ANack;
    FPend.Add(LItem);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmTracker.Release(AMark: UInt64);
var
  I, LDone, LIdxAck: Integer;
  LHasAck: Boolean;
  LLastAck: UInt64;
begin
  FLock.Enter;
  try
    if FWriter = nil then
    begin
      FPend.Clear; // dead channel: nobody waits for these acks anymore
      Exit;
    end;
    LDone := 0;
    LHasAck := False;
    LLastAck := 0;
    LIdxAck := -1;
    I := 0;
    while (I < FPend.Count) and (FPend[I].Lsn <= AMark) do
    begin
      if FPend[I].Nack then
      begin
        // The nack cuts the collapse: what was accumulated goes out first, with
        // the PREVIOUS seq, and the nack goes out alone.
        if LHasAck then
        begin
          if not TryPost(BuildBasicAck(LLastAck, True)) then
            Break;
          LDone := LIdxAck + 1;
          LHasAck := False;
        end;
        if not TryPost(BuildBasicNack(FPend[I].Seq, False, False)) then
          Break;
        LDone := I + 1;
      end
      else
      begin
        // Accumulate: a single Basic.Ack with multiple=true settles them all.
        LHasAck := True;
        LLastAck := FPend[I].Seq;
        LIdxAck := I;
      end;
      Inc(I);
    end;
    // Nested, not 'and': the post has a SIDE EFFECT, and relying on short-circuit
    // evaluation to skip it would mean relying on a compiler directive.
    if LHasAck then
      if TryPost(BuildBasicAck(LLastAck, True)) then
        LDone := LIdxAck + 1;

    for I := 1 to LDone do
      FPend.Delete(0);
  finally
    FLock.Leave;
  end;
end;


procedure TAMQPConfirmTracker.FailAll;
var
  I, LDone: Integer;
begin
  FLock.Enter;
  try
    if FWriter = nil then
    begin
      FPend.Clear;
      Exit;
    end;
    // Failed journal: the promise cannot be kept, so EVERYTHING that was
    // waiting for the disk becomes a nack -- including what would already be durable,
    // because the mark will not move anymore and the publisher would hang. Nack is the
    // honest frame here: "the broker could not handle the message".
    LDone := 0;
    for I := 0 to FPend.Count - 1 do
    begin
      if not TryPost(BuildBasicNack(FPend[I].Seq, False, False)) then
        Break;
      LDone := I + 1;
    end;
    for I := 1 to LDone do
      FPend.Delete(0);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmTracker.Detach;
begin
  FLock.Enter;
  try
    FWriter := nil;
    FPend.Clear;
  finally
    FLock.Leave;
  end;
end;

function TAMQPConfirmTracker.PendingCount: Integer;
begin
  FLock.Enter;
  try
    Result := FPend.Count;
  finally
    FLock.Leave;
  end;
end;

{ TAMQPConfirmRegistry }

constructor TAMQPConfirmRegistry.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FTrackers := TList<IAMQPConfirmTracker>.Create;
  FMark := 0;
end;

destructor TAMQPConfirmRegistry.Destroy;
begin
  FTrackers.Free;
  FLock.Free;
  inherited;
end;

function TAMQPConfirmRegistry.Snapshot: TArray<IAMQPConfirmTracker>;
var
  I: Integer;
begin
  FLock.Enter;
  try
    SetLength(Result, FTrackers.Count);
    for I := 0 to FTrackers.Count - 1 do
      Result[I] := FTrackers[I];
  finally
    FLock.Leave;
  end;
end;

function TAMQPConfirmRegistry.CreateTracker(AWriter: TAMQPFrameWriter;
  AChannelNo: Word): IAMQPConfirmTracker;
begin
  Result := TAMQPConfirmTracker.Create(AWriter, AChannelNo);
  FLock.Enter;
  try
    FTrackers.Add(Result);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmRegistry.Unregister(const ATracker: IAMQPConfirmTracker);
var
  I: Integer;
begin
  if ATracker = nil then
    Exit;
  FLock.Enter;
  try
    for I := FTrackers.Count - 1 downto 0 do
      if FTrackers[I] = ATracker then
        FTrackers.Delete(I);
  finally
    FLock.Leave;
  end;
end;

function TAMQPConfirmRegistry.Watermark: UInt64;
begin
  Result := PcAtomicRead64(FMark);
end;

function TAMQPConfirmRegistry.TrackerCount: Integer;
begin
  FLock.Enter;
  try
    Result := FTrackers.Count;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmRegistry.ReleaseAll(ALsn: UInt64);
var
  LTrackers: TArray<IAMQPConfirmTracker>;
  I: Integer;
begin
  LTrackers := Snapshot;
  for I := 0 to High(LTrackers) do
    LTrackers[I].Release(ALsn);
end;

procedure TAMQPConfirmRegistry.RetryPending;
begin
  // Does NOT go through Durable: retrying is not advancing the mark.
  ReleaseAll(Watermark);
end;

// --- IAMQPDurabilitySink: runs on the JOURNAL THREAD, after each fsync ---

procedure TAMQPConfirmRegistry.Durable(ALsn: UInt64);
begin
  PcAtomicWrite64(FMark, ALsn);
  ReleaseAll(ALsn);
end;

procedure TAMQPConfirmRegistry.JournalFailed(const AMessage: string);
var
  LTrackers: TArray<IAMQPConfirmTracker>;
  I: Integer;
begin
  LTrackers := Snapshot;
  for I := 0 to High(LTrackers) do
    LTrackers[I].FailAll;
end;

end.
