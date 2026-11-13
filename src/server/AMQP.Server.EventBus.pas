unit AMQP.Server.EventBus;

{$I amqp.inc}

{ Transport for the observability events (Phase 4.1, D30-D33, D35).

  ONE bounded ring and ONE notifier thread, and EVERY event goes through here --
  including the ones born on the connection's read thread. D30 rejected the
  three alternatives, each for a reason of its own:

  - pool worker: the queue actors' pool (the broker's own pool since F8;
    before that, the global one) would let a slow handler starve the actor.
    That would be D2 violated indirectly, which is the worst form -- the one
    that does not show in the code that violates it. And PcPool belongs to
    the whole process, so the handler would compete for workers with other
    libs' callbacks;
  - monitor thread: would put heartbeat, the Close-Ok deadline, the TTL sweep
    and the reaping of dead connections behind a user handler;
  - inline on the read thread: stalls frame processing for that connection,
    HEARTBEAT INCLUDED, until the client drops it on timeout.

  The temptation was to call the lifecycle events inline (they are born on
  the read thread, they look cheap) and marshal only the actor's. There is no
  safe half: what changes among the three is the blast radius, not whether
  there is damage. Uniformity gives one contract, one ordering and one
  exception policy -- and D34's flat record makes going async cost one copy
  by value.

  DELIVERY CONTRACT (D31), in three lines:

  - the emitter NEVER blocks and NEVER raises. Ring full => drop and count;
  - drops the NEWEST, not the head. What entered the ring is delivered, so
    the handler sees a CONTIGUOUS PREFIX of the emission order with the gaps
    counted in DroppedCount. Dropping the head would rewrite history
    silently -- and that is exactly what D7 does in memory, where the newest
    message is the one that matters. Here it is the opposite;
  - the promised ordering is PER ORIGIN (per connection, per queue). The
    single consumer does give a total order of acceptance into the ring, but
    between two threads that order means nothing and does not become an API
    promise.

  Completes the trio of the three pressures in this codebase: memory drops
  the head (D7), disk REFUSES (D26), events drop the newest. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Server.Events,
  AMQP.Server.Types;

type
  TAMQPEventBus = class;

  { The notifier. Same shape as TAMQPJournalThread and
    TAMQPMonitorThread: the loop lives in the owning object, the thread only repeats it. }
  TAMQPEventNotifierThread = class(TThread)
  private
    FBus: TAMQPEventBus;
  protected
    procedure Execute; override;
  public
    constructor Create(ABus: TAMQPEventBus);
  end;

  { One subscriber: the method and the type mask it asked for. }
  TAMQPEventSubscription = record
    Handler: TAMQPServerEventHandler;
    Mask: Cardinal;
  end;

  { The bus. Owner: TAMQPServer, which creates it in the constructor (so that
    Subscribe works before Start) and destroys it in the destructor.

    The interface refcount is a no-op, as in TAMQPEngine -- and for the same
    reason: the server rules the lifetime, not the count. }
  TAMQPEventBus = class(TInterfacedObject, IAMQPEventSink)
  private
    // --- ring + counters (under FMon) ---
    FMon: TPcMonitor;
    FRing: array of TAMQPServerEvent;
    FCapacity: Integer;
    FHead: Integer;       // next out
    FTail: Integer;       // next in
    FCount: Integer;      // occupancy
    FStopping: Boolean;
    FDispatching: Boolean; // notifier inside a handler
    FEmitted: Int64;
    FDropped: Int64;
    FFailed: Int64;
    FLastFailure: string;
    FThread: TAMQPEventNotifierThread;

    // --- subscribers (under FSubsLock) ---
    FSubsLock: TCriticalSection;
    FSubs: TList<TAMQPEventSubscription>;
    FMask: Integer; // atomic: union of the masks, read lock-free by Wants

    procedure RebuildMaskLocked;
    procedure SetCapacity(AValue: Integer);
    function TakeBatch(out ABatch: TArray<TAMQPServerEvent>): Boolean;
    procedure DispatchBatch(const ABatch: TArray<TAMQPServerEvent>);
  protected
    // IAMQPEventSink -- refcount disabled (see the class header). The
    // IInterface calling convention depends on the PLATFORM in FPC:
    // stdcall on Windows, cdecl on Unix (CLAUDE.md).
    function _AddRef: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
    function _Release: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
  public
    constructor Create;
    destructor Destroy; override;

    // --- IAMQPEventSink ---
    /// True if ANY subscriber wants this type. Atomic read of a mask,
    /// lock-free: it is what makes a broker with no subscriber pay nothing.
    function Wants(AType: TAMQPServerEventType): Boolean;
    /// Enqueues. Never blocks, never raises, may drop (D31).
    procedure Emit(const AEvent: TAMQPServerEvent);

    // --- subscription ---
    /// Subscribes to all types.
    procedure Subscribe(AHandler: TAMQPServerEventHandler); overload;
    /// Subscribes only to the requested types. Subscribing the same method
    /// twice REPLACES the previous mask instead of duplicating delivery.
    procedure Subscribe(AHandler: TAMQPServerEventHandler;
      const ATypes: TAMQPServerEventTypes); overload;
    /// Removes. It is MANDATORY to call it before destroying the object that
    /// owns the method (D33): an orphan handler becomes an AV inside the
    /// notifier, which counts it in FailedCount and moves on -- but the event
    /// is lost and the symptom shows up far from the cause.
    procedure Unsubscribe(AHandler: TAMQPServerEventHandler);
    function SubscriberCount: Integer;

    // --- lifecycle (called by TAMQPServer) ---
    procedure Start;
    /// Stops the notifier AFTER delivering what is already in the ring. That
    /// is why the server takes it down last: the connection and channel close
    /// events are born during teardown and are still delivered.
    procedure Stop;

    /// Waits for the ring to empty AND the current handler to finish. It is
    /// D35's barrier -- tests wait on it, not on Sleep. False = timed out.
    /// Do NOT call it from inside a handler (it would wait for itself).
    function Drain(ATimeoutMs: Cardinal): Boolean;

    /// How many events entered the ring.
    function EmittedCount: Int64;
    /// How many were dropped because the ring was full (D31). It is the only
    /// way for the observer to know it lost something.
    function DroppedCount: Int64;
    /// How many handler exceptions were caught (D33).
    function FailedCount: Int64;
    /// Class and message of the last handler exception ('' if none).
    function LastFailure: string;

    /// Ring size, in events. Can only change before Start.
    property Capacity: Integer read FCapacity write SetCapacity;
  end;

implementation

{ Compares two object methods. TList<T>.IndexOf would need a comparer for a
  record; the manual loop is shorter than the comparer. }
function SameHandler(const A, B: TAMQPServerEventHandler): Boolean;
begin
  Result := (TMethod(A).Code = TMethod(B).Code)
        and (TMethod(A).Data = TMethod(B).Data);
end;

{ TAMQPEventNotifierThread }

constructor TAMQPEventNotifierThread.Create(ABus: TAMQPEventBus);
begin
  // The field BEFORE inherited: with Create(False) the thread may already be
  // running when the constructor returns.
  FBus := ABus;
  inherited Create(False);
  {$IFDEF FPC}
  NameThreadForDebugging('amqp-events');
  {$ELSE}
  NameThreadForDebugging('amqp-events', ThreadID);
  {$ENDIF}
end;

procedure TAMQPEventNotifierThread.Execute;
var
  LBatch: TArray<TAMQPServerEvent>;
begin
  while FBus.TakeBatch(LBatch) do
  begin
    if Length(LBatch) > 0 then
      FBus.DispatchBatch(LBatch);
    LBatch := nil;
  end;
end;

{ TAMQPEventBus }

constructor TAMQPEventBus.Create;
begin
  inherited Create;
  FMon := TPcMonitor.Create;
  FSubsLock := TCriticalSection.Create;
  FSubs := TList<TAMQPEventSubscription>.Create;
  FCapacity := AMQP_EVENT_QUEUE_CAPACITY;
  SetLength(FRing, FCapacity);
end;

destructor TAMQPEventBus.Destroy;
begin
  Stop;
  FSubs.Free;
  FSubsLock.Free;
  FMon.Free;
  FRing := nil;
  inherited Destroy;
end;

function TAMQPEventBus._AddRef: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
begin
  Result := -1; // no refcount: the owner is TAMQPServer
end;

function TAMQPEventBus._Release: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
begin
  Result := -1;
end;

procedure TAMQPEventBus.SetCapacity(AValue: Integer);
begin
  if AValue < 1 then
    AValue := 1;
  if FThread <> nil then
    raise EAMQPServerAbort.Create(
      'EventQueueCapacity can only change before Start');
  FCapacity := AValue;
  SetLength(FRing, FCapacity);
  FHead := 0;
  FTail := 0;
  FCount := 0;
end;

{ --- subscription -------------------------------------------------------- }

procedure TAMQPEventBus.RebuildMaskLocked;
var
  I: Integer;
  LMask: Cardinal;
begin
  LMask := 0;
  for I := 0 to FSubs.Count - 1 do
    LMask := LMask or FSubs[I].Mask;
  // Atomic publication: Wants reads with no lock at all, on the hot path.
  PcAtomicSet(FMask, Integer(LMask));
end;

procedure TAMQPEventBus.Subscribe(AHandler: TAMQPServerEventHandler);
begin
  Subscribe(AHandler, AMQP_EVENT_ALL_TYPES);
end;

procedure TAMQPEventBus.Subscribe(AHandler: TAMQPServerEventHandler;
  const ATypes: TAMQPServerEventTypes);
var
  I: Integer;
  LSub: TAMQPEventSubscription;
  LFound: Boolean;
begin
  if not Assigned(AHandler) then
    Exit;
  LSub.Handler := AHandler;
  LSub.Mask := AmqpEventMask(ATypes);
  FSubsLock.Enter;
  try
    LFound := False;
    for I := 0 to FSubs.Count - 1 do
      if SameHandler(FSubs[I].Handler, AHandler) then
      begin
        // Re-subscribing REPLACES the mask. Duplicating delivery would be the
        // surprise: the same event would reach the same method twice.
        FSubs[I] := LSub;
        LFound := True;
        Break;
      end;
    if not LFound then
      FSubs.Add(LSub);
    RebuildMaskLocked;
  finally
    FSubsLock.Leave;
  end;
end;

procedure TAMQPEventBus.Unsubscribe(AHandler: TAMQPServerEventHandler);
var
  I: Integer;
begin
  FSubsLock.Enter;
  try
    for I := FSubs.Count - 1 downto 0 do
      if SameHandler(FSubs[I].Handler, AHandler) then
        FSubs.Delete(I);
    RebuildMaskLocked;
  finally
    FSubsLock.Leave;
  end;
end;

function TAMQPEventBus.SubscriberCount: Integer;
begin
  FSubsLock.Enter;
  try
    Result := FSubs.Count;
  finally
    FSubsLock.Leave;
  end;
end;

{ --- emission ------------------------------------------------------------ }

function TAMQPEventBus.Wants(AType: TAMQPServerEventType): Boolean;
begin
  Result := (PcAtomicGet(FMask)
    and Integer(Cardinal(1) shl Ord(AType))) <> 0;
end;

procedure TAMQPEventBus.Emit(const AEvent: TAMQPServerEvent);
begin
  FMon.Enter;
  try
    // After Stop there is nobody to drain: accepting would leak the event (and
    // its strings) until the destructor.
    if FStopping then
    begin
      Inc(FDropped);
      Exit;
    end;
    if FCount >= FCapacity then
    begin
      // D31: drops the NEWEST. This Exit is the whole contract -- the
      // emitter returns at once, without blocking and without raising, and
      // the gap is counted. Turning it into a wait here would put the actor
      // (or the read thread) behind a user handler, which is D2 inside out.
      Inc(FDropped);
      Exit;
    end;
    FRing[FTail] := AEvent; // copy by value; strings bump the refcount
    FTail := (FTail + 1) mod FCapacity;
    Inc(FCount);
    Inc(FEmitted);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

{ --- consumption --------------------------------------------------------- }

function TAMQPEventBus.TakeBatch(out ABatch: TArray<TAMQPServerEvent>): Boolean;
var
  LN, I: Integer;
begin
  ABatch := nil;
  Result := True;
  FMon.Enter;
  try
    while (FCount = 0) and (not FStopping) do
      FMon.Wait(100);

    LN := FCount;
    if LN = 0 then
    begin
      // Ring empty AND stop requested: the thread's only exit path, and it
      // only happens after everything that came in has gone out. It is what
      // makes Stop deliver the teardown events instead of dropping them.
      if FStopping then
        Exit(False);
      Exit(True);
    end;

    // Takes EVERYTHING at once. Whatever arrives during dispatch is the next
    // batch -- same shape as the journal's group commit (D25), for the same
    // reason: the batch adapts to the load by itself, with no parameter.
    SetLength(ABatch, LN);
    for I := 0 to LN - 1 do
    begin
      ABatch[I] := FRing[FHead];
      // Releases the slot's strings: without this the ring keeps the last
      // FCapacity messages alive forever.
      FRing[FHead] := Default(TAMQPServerEvent);
      FHead := (FHead + 1) mod FCapacity;
    end;
    FCount := 0;
    FDispatching := True;
    FMon.PulseAll; // wakes whoever waited for room or for a drain
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPEventBus.DispatchBatch(const ABatch: TArray<TAMQPServerEvent>);
var
  LSubs: TArray<TAMQPEventSubscription>;
  I, J: Integer;
  LBit: Cardinal;
  LFailed: Int64;
  LLastFailure: string;
begin
  LFailed := 0;
  LLastFailure := '';
  try
    // Snapshot under the lock, call OUTSIDE it: the handler may subscribe,
    // unsubscribe, or take as long as it likes.
    FSubsLock.Enter;
    try
      SetLength(LSubs, FSubs.Count);
      for I := 0 to FSubs.Count - 1 do
        LSubs[I] := FSubs[I];
    finally
      FSubsLock.Leave;
    end;

    for I := 0 to High(ABatch) do
    begin
      LBit := Cardinal(1) shl Ord(ABatch[I].EventType);
      for J := 0 to High(LSubs) do
      begin
        if (LSubs[J].Mask and LBit) = 0 then
          Continue;
        try
          LSubs[J].Handler(ABatch[I]);
        except
          // D33: NEVER "except end". The notifier must not die because of a
          // handler, and the exception must not vanish. Count it, keep the
          // last one, move on to the next handler and the next event.
          //
          // Catching Exception catches EAccessViolation on both compilers --
          // which is the REAL failure mode here: a handler whose owner was
          // destroyed without Unsubscribe. FailedCount is how it is found.
          on E: Exception do
          begin
            Inc(LFailed);
            LLastFailure := E.ClassName + ': ' + E.Message;
          end;
        end;
      end;
    end;
  finally
    // The finally is what guarantees FDispatching drops even if something
    // above escapes -- otherwise Drain would wait forever.
    FMon.Enter;
    try
      FDispatching := False;
      if LFailed > 0 then
      begin
        Inc(FFailed, LFailed);
        FLastFailure := LLastFailure;
      end;
      FMon.PulseAll;
    finally
      FMon.Leave;
    end;
  end;
end;

{ --- lifecycle ----------------------------------------------------------- }

procedure TAMQPEventBus.Start;
begin
  if FThread <> nil then
    Exit;
  FMon.Enter;
  try
    FStopping := False;
  finally
    FMon.Leave;
  end;
  FThread := TAMQPEventNotifierThread.Create(Self);
end;

procedure TAMQPEventBus.Stop;
begin
  if FThread = nil then
    Exit;
  FMon.Enter;
  try
    FStopping := True;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
  FThread.WaitFor;
  // NIL it right after the join: TThread.WaitFor is NOT idempotent on FPC/Unix
  // (unconditional pthread_join -- CLAUDE.md). FreeAndNil closes that door.
  FreeAndNil(FThread);
end;

function TAMQPEventBus.Drain(ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := PcTickMs + ATimeoutMs;
  FMon.Enter;
  try
    // Re-checks the condition in a loop with a deadline: TPcMonitor may wake
    // spuriously (its contract).
    while ((FCount > 0) or FDispatching) and (PcTickMs < LDeadline) do
      FMon.Wait(20);
    Result := (FCount = 0) and (not FDispatching);
  finally
    FMon.Leave;
  end;
end;

{ --- counters ------------------------------------------------------------ }

function TAMQPEventBus.EmittedCount: Int64;
begin
  FMon.Enter;
  try
    Result := FEmitted;
  finally
    FMon.Leave;
  end;
end;

function TAMQPEventBus.DroppedCount: Int64;
begin
  FMon.Enter;
  try
    Result := FDropped;
  finally
    FMon.Leave;
  end;
end;

function TAMQPEventBus.FailedCount: Int64;
begin
  FMon.Enter;
  try
    Result := FFailed;
  finally
    FMon.Leave;
  end;
end;

function TAMQPEventBus.LastFailure: string;
begin
  FMon.Enter;
  try
    Result := FLastFailure;
  finally
    FMon.Leave;
  end;
end;

end.
