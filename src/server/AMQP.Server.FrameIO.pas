unit AMQP.Server.FrameIO;

{$I amqp.inc}

{ Serial frame writing for ONE broker connection (server sub-module, WS4).

  Several producers enqueue frames: the read thread (method replies,
  Channel/Connection.Close), the heartbeat timer and, in Phase 2, the queue
  workers (Basic.Deliver) and the channel handler (confirms). A single
  internal thread drains the queue into the TStream, in arrival order. So the
  SSL object / the socket never see concurrent writes, and the frame order on
  the channel is deterministic.

  - PostFrames enqueues a batch under a single lock: method + content-header +
    body of a delivery are never interleaved with another sequence on the same
    channel (otherwise the client sees UNEXPECTED_FRAME / 505).
  - The queue is bounded (FMaxDepth): when full, Post* blocks until the writer
    thread drains it, which propagates the socket's backpressure as natural
    flow control.
  - If a write to the stream fails, the writer thread stores the error, wakes
    the blocked producers and exits; every later Post* raises EAMQPTransport.

  Does not own the TStream (whoever creates the writer owns the stream/socket). }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Frame,
  AMQP.Transport; // EAMQPTransport

type
  TAMQPFrameWriter = class;

  { Internal thread: drains the queue and writes to the stream. }
  TAMQPFrameWriterThread = class(TThread)
  private
    FOwner: TAMQPFrameWriter;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TAMQPFrameWriter);
  end;

  TAMQPFrameWriter = class
  private
    FStream: TStream;
    FQueue: TQueue<TAMQPFrame>;
    FMon: TPcMonitor;
    FThread: TAMQPFrameWriterThread;
    FJoinLock: TCriticalSection;
    FJoined: Boolean;
    FMaxDepth: Integer;
    FStopping: Boolean;   // Stop requested (under FMon)
    FFailed: Boolean;     // a write failed (under FMon)
    FError: string;       // reason for FFailed (under FMon)
    FStarted: Boolean;
    FLastWriteTick: UInt64; // atomic; last real flush to the stream
    procedure DrainLoop;
    procedure EnqueueOne(const AFrame: TAMQPFrame);
  public
    /// AMaxDepth = queue depth before Post* blocks.
    constructor Create(AStream: TStream; AMaxDepth: Integer = 256);
    destructor Destroy; override;

    /// Enqueues one frame. Blocks if the queue is full. Raises
    /// EAMQPTransport if the writer thread has already stopped or failed.
    procedure PostFrame(const AFrame: TAMQPFrame);
    /// Enqueues several frames atomically (not interleaved with another batch).
    procedure PostFrames(const AFrames: array of TAMQPFrame);
    /// Like PostFrames, but **DOES NOT BLOCK and DOES NOT RAISE**: returns False
    /// if the queue is full right now, or if the writer thread has already
    /// stopped/failed, and in that case nothing is enqueued (the batch is
    /// indivisible: method + header + body of a delivery never go in halfway).
    ///
    /// This is the DELIVERY path (WS4 of Phase 2). The queue actor runs on a
    /// pool worker and must not block waiting for I/O (decision D2); a consumer
    /// whose channel accepts no more frames leaves the round-robin for that
    /// round instead of stalling the whole queue (decision D3), which is what
    /// would happen if delivery used PostFrames.
    function TryPostFrames(const AFrames: array of TAMQPFrame): Boolean;

    /// Stops the writer thread: drains what is already queued (best-effort) and
    /// ends the thread. Idempotent. After Stop, Post* raises an exception.
    procedure Stop;

    /// '' while healthy; the reason after a write failure.
    function LastError: string;
    function Failed: Boolean;
    /// Tick (PcTickMs) of the last write that actually reached the stream, not
    /// of the last Post*. That is what the heartbeat needs: the spec talks about
    /// idleness of sending on the wire, and a frame sitting in the queue has not
    /// left yet. 0 while nothing has been written. Atomic read; callable from
    /// another thread.
    function LastWriteTick: UInt64;
  end;

implementation

{ TAMQPFrameWriterThread }

constructor TAMQPFrameWriterThread.Create(AOwner: TAMQPFrameWriter);
begin
  FOwner := AOwner;
  inherited Create(False); // starts right away
end;

procedure TAMQPFrameWriterThread.Execute;
begin
  FOwner.DrainLoop;
end;

{ TAMQPFrameWriter }

constructor TAMQPFrameWriter.Create(AStream: TStream; AMaxDepth: Integer);
begin
  inherited Create;
  FStream := AStream;
  FMaxDepth := AMaxDepth;
  if FMaxDepth < 1 then
    FMaxDepth := 1;
  FQueue := TQueue<TAMQPFrame>.Create;
  FMon := TPcMonitor.Create;
  FJoinLock := TCriticalSection.Create;
  FThread := TAMQPFrameWriterThread.Create(Self);
  FStarted := True;
end;

destructor TAMQPFrameWriter.Destroy;
begin
  Stop; // already does the join (only once)
  FThread.Free;
  FQueue.Free;
  FMon.Free;
  FJoinLock.Free;
  inherited;
end;

function TAMQPFrameWriter.TryPostFrames(
  const AFrames: array of TAMQPFrame): Boolean;
var
  I: Integer;
begin
  if Length(AFrames) = 0 then
    Exit(True);
  FMon.Enter;
  try
    if FStopping or FFailed or (FQueue.Count >= FMaxDepth) then
      Exit(False);
    // Once it is decided that it fits, the whole batch goes in -- as in
    // PostFrames, the last batch may exceed the cap, and that is what keeps it atomic.
    for I := 0 to High(AFrames) do
      FQueue.Enqueue(AFrames[I]);
    FMon.PulseAll;
    Result := True;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPFrameWriter.Stop;
begin
  if not FStarted then
    Exit;
  FMon.Enter;
  try
    FStopping := True;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
  // JOIN ONLY ONCE. `TThread.WaitFor` on FPC/Unix is an UNCONDITIONAL
  // `pthread_join` (rtl/unix/tthread.inc: WaitForThreadTerminate -> pthread_join,
  // without checking whether a join already happened); on Windows it is
  // WaitForSingleObject, which is idempotent on an already signaled handle.
  // Calling it twice is therefore harmless on one and UNDEFINED behavior on the
  // other -- and if the pthread_t has been recycled by a new thread, the second
  // join waits for the WRONG thread, forever. The lock is what makes the second
  // caller wait for the first join to FINISH before going on, instead of just skipping.
  FJoinLock.Enter;
  try
    if not FJoined then
    begin
      FThread.WaitFor;
      FJoined := True;
    end;
  finally
    FJoinLock.Leave;
  end;
end;

procedure TAMQPFrameWriter.EnqueueOne(const AFrame: TAMQPFrame);
begin
  // call WHILE HOLDING FMon
  while (FQueue.Count >= FMaxDepth) and (not FStopping) and (not FFailed) do
    FMon.Wait(PC_WAIT_INFINITE);
  if FStopping then
    raise EAMQPTransport.Create('frame writer stopped');
  if FFailed then
    raise EAMQPTransport.Create('frame writer failed: ' + FError);
  FQueue.Enqueue(AFrame);
end;

procedure TAMQPFrameWriter.PostFrame(const AFrame: TAMQPFrame);
begin
  FMon.Enter;
  try
    EnqueueOne(AFrame);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPFrameWriter.PostFrames(const AFrames: array of TAMQPFrame);
var
  I: Integer;
begin
  if Length(AFrames) = 0 then
    Exit;
  FMon.Enter;
  try
    // Waits only once (the whole batch goes in together, even if it exceeds the
    // cap: an indivisible batch cannot be left stuck halfway).
    while (FQueue.Count >= FMaxDepth) and (not FStopping) and (not FFailed) do
      FMon.Wait(PC_WAIT_INFINITE);
    if FStopping then
      raise EAMQPTransport.Create('frame writer stopped');
    if FFailed then
      raise EAMQPTransport.Create('frame writer failed: ' + FError);
    for I := 0 to High(AFrames) do
      FQueue.Enqueue(AFrames[I]);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPFrameWriter.DrainLoop;
var
  LBatch: array of TAMQPFrame;
  LCount, I: Integer;
  LDone: Boolean;
begin
  LBatch := nil;
  LDone := False;
  repeat
    // Takes everything in the queue (or waits).
    FMon.Enter;
    try
      while (FQueue.Count = 0) and (not FStopping) and (not FFailed) do
        FMon.Wait(PC_WAIT_INFINITE);

      LCount := FQueue.Count;
      if LCount > 0 then
      begin
        SetLength(LBatch, LCount);
        for I := 0 to LCount - 1 do
          LBatch[I] := FQueue.Dequeue;
      end
      else
        SetLength(LBatch, 0);

      // After Stop, we drain what is left and then exit.
      if (FStopping or FFailed) and (FQueue.Count = 0) then
        LDone := True;

      FMon.PulseAll; // releases producers waiting for room
    finally
      FMon.Leave;
    end;

    // Write OUTSIDE the lock.
    if Length(LBatch) > 0 then
    begin
      try
        for I := 0 to High(LBatch) do
          LBatch[I].WriteTo(FStream);
        PcAtomicWrite64(FLastWriteTick, PcTickMs);
      except
        on E: Exception do
        begin
          FMon.Enter;
          try
            if not FFailed then
            begin
              FFailed := True;
              FError := E.Message;
            end;
            FMon.PulseAll;
          finally
            FMon.Leave;
          end;
          LDone := True;
        end;
      end;
    end;
  until LDone;
end;

function TAMQPFrameWriter.LastError: string;
begin
  FMon.Enter;
  try
    Result := FError;
  finally
    FMon.Leave;
  end;
end;

function TAMQPFrameWriter.LastWriteTick: UInt64;
begin
  Result := PcAtomicRead64(FLastWriteTick);
end;

function TAMQPFrameWriter.Failed: Boolean;
begin
  FMon.Enter;
  try
    Result := FFailed;
  finally
    FMon.Leave;
  end;
end;

end.
