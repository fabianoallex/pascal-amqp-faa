unit AMQP.Server.Broker;

{$I amqp.inc}

{ The broker: listener lifecycle, accept loop and connection registry
  (server sub-module).

  Start brings up the listener and an accept thread that creates one
  TAMQPServerConnection per connection, handing each the current config
  (authenticator, authorizer, vhosts and the Tune limits); Stop tears it all
  down without leaks. The handshake FSM and the channel map live in the
  connection (WS4); the routing engine (WS5/Phase 2) and heartbeats (WS7) come
  later.

  Usage:
    B := TAMQPServer.Create;
    B.BindAddress := '0.0.0.0';
    B.Port := 5672;
    // B.Authenticator := TAMQPStaticAuthenticator.Create(['admin','s3cr3t']);
    B.VirtualHosts.Add('/app');
    B.Start;
    ...
    B.Stop;  // idempotent; Destroy also stops
}

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  PascalCommon.Transport,
  AMQP.Transport,
  AMQP.Server.Auth,
  AMQP.Server.Types,
  AMQP.Server.Events,
  AMQP.Server.EventBus,
  AMQP.Server.Engine,
  AMQP.Server.Journal,
  AMQP.Server.Confirm,
  AMQP.Server.Recovery,
  AMQP.Server.Connection;

// TAMQPVirtualHostRegistry moved to AMQP.Server.Types in WS4 (the handshake
// FSM needs to consult it, and it cannot use this unit — which uses it).
// Code that touches vhosts must list AMQP.Server.Types in its own uses.

type
  TAMQPServer = class;

  TAMQPAcceptThread = class(TThread)
  private
    FServer: TAMQPServer;
  protected
    procedure Execute; override;
  public
    constructor Create(AServer: TAMQPServer);
  end;

  { Monitor thread: a single one for the whole broker (not one per connection).
    Every AMQP_SERVER_MONITOR_TICK_MS it walks the live connections applying the
    heartbeat step and the Connection.Close-Ok deadline, and collects the dead
    ones. It is the ONLY thread that frees connections while the broker runs —
    which is why the snapshot it takes cannot be freed under it. }
  TAMQPMonitorThread = class(TThread)
  private
    FServer: TAMQPServer;
  protected
    procedure Execute; override;
  public
    constructor Create(AServer: TAMQPServer);
  end;

  TAMQPServer = class
  private
    FListener: TPcListener; // TCP or TLS (pascal-common-faa)
    FAccept: TAMQPAcceptThread;
    FMonitor: TAMQPMonitorThread;
    FMonitorStop: TEvent;
    FLock: TCriticalSection;
    FActive: TList<TAMQPServerConnection>;
    FDead: TList<TAMQPServerConnection>;
    FAuth: IAMQPAuthenticator;
    FAuthorizer: IAMQPAuthorizer;
    FSink: IAMQPMessageSink;
    FEngine: TAMQPEngine; // WS5: the real engine; also the Sink
    // The queue actors' OWN pool (F8). Not pascal-common-faa's PcPool: that
    // one belongs to the whole process -- consumer callbacks of this client
    // and of the other *-faa libs, which may block on I/O for seconds -- and
    // has a cap. Measured (TPoolIsolationTests): with PcPool saturated and the
    // actor on it, Queue.Declare-Ok waited the 15 s of the actor's synchronous
    // command and the connection dropped. The actor never blocks (D2), so this
    // pool only competes for CPU among queues, which is what it should compete
    // for. Creating it is cheap (no thread before the first Queue).
    FActorPool: TPcThreadPool;
    FJournal: TAMQPJournal; // Phase 4: nil while DataDir is empty
    // Where channels park deferred confirms. Exists together with the journal
    // and for the same reason: without a DataDir there is nothing to wait for,
    // and the confirm goes out inline as in Phase 3.
    FConfirms: IAMQPConfirmRegistry;
    // What the last recovery found. Zeroed while DataDir is empty. Exists for
    // the same reason as the journal counters: a broker that came up with a
    // truncated log has to be able to SAY so.
    FRecoveryStats: TAMQPRecoveryStats;
    FMaxJournalBytes: Int64;
    FDataDir: string;
    FMaxQueueLength: Integer;
    FVHosts: TAMQPVirtualHostRegistry;
    FBindAddress: string;
    FPort: Word;
    FBacklog: Integer;
    FChannelMax: Word;
    FFrameMax: Cardinal;
    FHeartbeat: Word;
    FCloseTimeoutMs: Cardinal;
    FUseTls: Boolean;
    FTlsCertFile: string;
    FTlsKeyFile: string;
    FTlsCertPassword: string;
    FTlsCaFile: string;
    FTlsBackend: TPcTlsBackend;
    FRunning: Boolean;
    FStopping: Boolean;
    FTotalAccepted: Integer; // atomic
    FEventBus: TAMQPEventBus; // owner; lives from constructor to destructor
    function ConnConfig: TAMQPServerConnConfig;
    function OpenListener: TPcListener;
    procedure AcceptLoop;
    procedure MonitorLoop;
    procedure StopMonitor;
    procedure HandleConnClosed(AConn: TAMQPServerConnection);
    procedure ReapDead;
    procedure SetMaxQueueLength(AValue: Integer);
    procedure SetDataDir(const AValue: string);
    function GetEventQueueCapacity: Integer;
    procedure SetEventQueueCapacity(AValue: Integer);
  public
    constructor Create;
    destructor Destroy; override;

    procedure Start;
    procedure Stop;

    // --- observability (Phase 4.1, D29-D36) ---
    /// Registers a handler for ALL event types. Thread-safe, and may be
    /// called before or after Start.
    ///
    /// The handler runs on the broker's notifier thread -- never on a
    /// connection's read thread, never on a pool worker (D30). It may take
    /// its time without stalling anything, but while it does the ring fills
    /// up and starts DROPPING events, counted in EventsDropped (D31).
    ///
    /// Call Unsubscribe BEFORE destroying the object that owns the method: an
    /// orphan handler becomes an AV, which the notifier counts and survives
    /// (D33), but the symptom shows up far from the cause.
    procedure Subscribe(AHandler: TAMQPServerEventHandler); overload;
    /// Same, filtering the types. Subscribing the same method again REPLACES
    /// the previous mask, instead of duplicating delivery.
    procedure Subscribe(AHandler: TAMQPServerEventHandler;
      const ATypes: TAMQPServerEventTypes); overload;
    /// Removes a previously registered handler. Thread-safe.
    procedure Unsubscribe(AHandler: TAMQPServerEventHandler);

    /// Waits for the notifier to deliver everything already emitted. It is
    /// D35's barrier -- tests wait on it, not on Sleep. False = the deadline
    /// expired. Do NOT call from inside a handler.
    function DrainEvents(ATimeoutMs: Cardinal = 2000): Boolean;
    /// How many events entered the ring since Create.
    function EventsEmitted: Int64;
    /// How many were DROPPED because the ring was full. A value > 0 means the
    /// observer did not see everything -- it is the price of D31's best-effort,
    /// and the only way to know it was paid.
    function EventsDropped: Int64;
    /// How many handler exceptions the notifier caught (D33).
    function EventsFailed: Int64;
    /// Class and message of the last handler exception ('' if none).
    function LastEventFailure: string;

    /// Size of the event ring, in events (default
    /// AMQP_EVENT_QUEUE_CAPACITY). Can only change before Start.
    property EventQueueCapacity: Integer read GetEventQueueCapacity
      write SetEventQueueCapacity;

    /// Number of connections alive right now.
    function ConnectionCount: Integer;
    /// Snapshot of the live connections. The objects may be reaped right
    /// after — use only for immediate inspection (logs, tests, metrics).
    function Connections: TArray<TAMQPServerConnection>;
    /// Total connections accepted since Start (includes the ones already closed).
    property TotalAccepted: Integer read FTotalAccepted;

    /// The routing engine (Phase 2). Created by the broker and used as the
    /// MessageSink; exposed for tests and diagnostics.
    property Engine: TAMQPEngine read FEngine;
    /// Durability journal, or nil when DataDir is empty. Exists only between
    /// Start and Stop.
    property Journal: TAMQPJournal read FJournal;
    /// Registry of deferred confirms, or nil when DataDir is empty.
    /// Exists for the same reason as Journal above: diagnostics and tests.
    property Confirms: IAMQPConfirmRegistry read FConfirms;
    /// What the WAL replay found on the last Start. All zero when DataDir is
    /// empty or the log was empty.
    property RecoveryStats: TAMQPRecoveryStats read FRecoveryStats;

    /// Data directory. EMPTY (default) = broker exactly like Phase 3's:
    /// `durable` and `delivery-mode 2` are still accepted and ignored, and
    /// nothing is written to disk. Set = durability on (decision D19).
    ///
    /// It is opt-in for the same reason as UseTls: the broker runs INSIDE the
    /// user's app, and nobody should get files on disk for having called
    /// Start. Can only change with the broker stopped.
    property DataDir: string read FDataDir write SetDataDir;
    /// Hard cap of the WAL in bytes. 0 (default) = unlimited, the shape of
    /// MaxQueueLength. When exceeded, the PERSISTENT publish gets Basic.Nack --
    /// the memory cap drops, the disk cap refuses. Applied from the next Start.
    property MaxJournalBytes: Int64 read FMaxJournalBytes
      write FMaxJournalBytes;
    /// Cap of ready messages PER QUEUE (decision D7 in CLAUDE.md).
    /// 0 (default) = unlimited. The broker runs inside the user's app: a
    /// queue with no consumer must not bring down the host process. Applies
    /// to queues created after it is set. Per-queue `x-max-length` is Phase 3.
    property MaxQueueLength: Integer read FMaxQueueLength
      write SetMaxQueueLength;

    /// '' or '0.0.0.0' (default) = every IPv4 interface; '::' = every IPv6
    /// one; or an address of this machine.
    property BindAddress: string read FBindAddress write FBindAddress;
    /// Port to bind; 0 = the OS chooses. After Start, reflects the actual one.
    property Port: Word read FPort write FPort;
    /// IGNORED since 0.6.0: the listener comes from pascal-common-faa, which
    /// uses its own backlog. Kept so that existing code compiles.
    property Backlog: Integer read FBacklog write FBacklog;
    property Running: Boolean read FRunning;

    // --- limits proposed in Connection.Tune (0 = no limit). Read on each
    //     accepted connection, which gets a copy of the config.
    property ChannelMax: Word read FChannelMax write FChannelMax;
    property FrameMax: Cardinal read FFrameMax write FFrameMax;
    /// Heartbeat proposed in Tune. The client decides in Tune-Ok (spec): the
    /// broker adopts the value it returns. 0 = no heartbeat.
    property Heartbeat: Word read FHeartbeat write FHeartbeat;
    /// How long to wait for Connection.Close-Ok before dropping the socket.
    property CloseTimeoutMs: Cardinal read FCloseTimeoutMs write FCloseTimeoutMs;

    // --- TLS (WS3; pascal-common-faa's transport since 0.6.0). Each accepted
    //     connection runs the TLS handshake on ITS OWN thread, before the
    //     AMQP protocol-header -- it's port 5671 "AMQPS", not STARTTLS. The
    //     handshake has the common's default deadline (15 s): a peer that
    //     opens TCP and never talks holds only its own thread, for that long.
    //     All of these are read at Start: the credentials are loaded there,
    //     so a wrong file or password makes Start raise EAMQPTls.
    /// Turns TLS on.
    property UseTls: Boolean read FUseTls write FUseTls;
    /// The server's certificate. OpenSSL: a PEM chain (the server's first),
    /// with TlsKeyFile. SChannel: a PFX, with TlsCertPassword.
    property TlsCertFile: string read FTlsCertFile write FTlsCertFile;
    /// OpenSSL: the private key in PEM, without a password.
    property TlsKeyFile: string read FTlsKeyFile write FTlsKeyFile;
    /// SChannel: the PFX's password.
    property TlsCertPassword: string read FTlsCertPassword
      write FTlsCertPassword;
    /// mTLS: require a client certificate issued by this CA (PEM, both
    /// backends). '' (default) = no client certificate asked for.
    property TlsCaFile: string read FTlsCaFile write FTlsCaFile;
    /// Which TLS implementation, chosen at run time. tbDefault = SChannel on
    /// Windows (OpenSSL with -dAMQP_OPENSSL), OpenSSL elsewhere; see
    /// AmqpDefaultTlsBackend in AMQP.Transport.
    property TlsBackend: TPcTlsBackend read FTlsBackend write FTlsBackend;

    /// Default: TAMQPStaticAuthenticator with guest/guest. Only before Start.
    property Authenticator: IAMQPAuthenticator read FAuth write FAuth;
    property Authorizer: IAMQPAuthorizer read FAuthorizer write FAuthorizer;
    /// Where published messages go. Default: TAMQPNullMessageSink, which
    /// drops them — that is what makes Phase 1 a "null broker". Phase 2's
    /// routing engine plugs in here.
    property MessageSink: IAMQPMessageSink read FSink write FSink;
    property VirtualHosts: TAMQPVirtualHostRegistry read FVHosts;
  end;

implementation

uses
  PascalCommon.Transport.Tcp,
  PascalCommon.Transport.Tls;

{ TAMQPAcceptThread }

constructor TAMQPAcceptThread.Create(AServer: TAMQPServer);
begin
  FServer := AServer;
  inherited Create(True);
end;

procedure TAMQPAcceptThread.Execute;
begin
  FServer.AcceptLoop;
end;

{ TAMQPMonitorThread }

constructor TAMQPMonitorThread.Create(AServer: TAMQPServer);
begin
  FServer := AServer;
  inherited Create(True);
end;

procedure TAMQPMonitorThread.Execute;
begin
  FServer.MonitorLoop;
end;

{ TAMQPServer }

constructor TAMQPServer.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FActive := TList<TAMQPServerConnection>.Create;
  FDead := TList<TAMQPServerConnection>.Create;
  FVHosts := TAMQPVirtualHostRegistry.Create;
  FAuth := TAMQPStaticAuthenticator.Create; // guest/guest
  FAuthorizer := TAMQPAllowAllAuthorizer.Create;
  // WS5: the engine is the sink. Phase 1's content path (channel reassembles
  // -> Sink.RouteMessage) did not change -- only what is plugged into it.
  FEngine := TAMQPEngine.Create;
  // Before any queue exists (Start's recovery already creates queues).
  FActorPool := TPcThreadPool.Create;
  FEngine.Pool := FActorPool;
  FSink := FEngine;
  FMonitorStop := TEvent.Create(nil, True, False, '');
  // The bus is born with the server (not in Start) so that Subscribe works
  // before the broker comes up -- which is the normal case: an observer wants
  // the first event, not the second.
  FEventBus := TAMQPEventBus.Create;
  // The engine passes the sink on to each queue it creates (Inc. 2). Seeded
  // here, not in Start, because a queue may be born before it (recovery).
  FEngine.Events := FEventBus;
  FBindAddress := '0.0.0.0';
  FPort := 5672;
  FBacklog := 64;
  FChannelMax := AMQP_SERVER_DEFAULT_CHANNEL_MAX;
  FFrameMax := AMQP_SERVER_DEFAULT_FRAME_MAX;
  FHeartbeat := AMQP_SERVER_DEFAULT_HEARTBEAT;
  FCloseTimeoutMs := AMQP_SERVER_CLOSE_TIMEOUT_MS;
end;

function TAMQPServer.OpenListener: TPcListener;
var
  LTls: TPcTlsOptions;
begin
  if not FUseTls then
    Exit(PcTcpListen(FBindAddress, FPort, AmqpTcpOptions));
  LTls := Default(TPcTlsOptions);
  LTls.CertFile := FTlsCertFile;
  LTls.KeyFile := FTlsKeyFile;
  LTls.CertPassword := FTlsCertPassword;
  LTls.CaFile := FTlsCaFile;
  LTls.Backend := AmqpResolveTlsBackend(FTlsBackend);
  Result := PcTlsListen(FBindAddress, FPort, AmqpTcpOptions, LTls);
end;

function TAMQPServer.ConnConfig: TAMQPServerConnConfig;
begin
  Result := TAMQPServerConnConfig.Defaults;
  Result.Auth := FAuth;
  Result.Authorizer := FAuthorizer;
  Result.VHosts := FVHosts;
  Result.ChannelMax := FChannelMax;
  Result.FrameMax := FFrameMax;
  Result.Heartbeat := FHeartbeat;
  Result.CloseTimeoutMs := FCloseTimeoutMs;
  Result.Sink := FSink;
  Result.Confirms := FConfirms;
  Result.Tls := FUseTls;
  Result.Events := FEventBus;
end;

destructor TAMQPServer.Destroy;
begin
  Stop;
  FActive.Free;
  FDead.Free;
  FVHosts.Free;
  FMonitorStop.Free;
  FLock.Free;
  FAuth := nil;
  FAuthorizer := nil;
  FSink := nil;
  // After Stop: no live connection, so no queue actor is being fed. The
  // engine's destructor stops the actors before freeing.
  FreeAndNil(FJournal);
  FEngine.Free;
  // After the engine: its destructor stops each actor (Stop waits for the
  // in-flight work item to leave the loop), so here no worker holds a queue
  // item. The pool's Destroy would run whatever was still queued -- it never
  // dropped --, but nothing is left to run.
  FActorPool.Free;
  // THE BUS LAST, after the engine and the journal. The engine, each QUEUE it
  // owns and the journal hold an IAMQPEventSink pointing to it; freeing the
  // bus earlier makes the destructor of each of those fields call _Release on
  // already freed memory -- access violation on exit, with the whole broker
  // working until then. It is the same lifetime rule the documentation tells
  // the user to follow (Unsubscribe before destroying the handler's owner),
  // only applied from the inside out.
  FEventBus.Free; // its destructor stops the notifier
  inherited;
end;

procedure TAMQPServer.SetDataDir(const AValue: string);
begin
  if FRunning then
    raise EAMQPServerAbort.Create(
      'DataDir cannot be changed while the broker is running');
  FDataDir := AValue;
end;

procedure TAMQPServer.Start;
var
  LState: TAMQPRecoveredState;
begin
  if FRunning then
    Exit;
  FStopping := False;
  FTotalAccepted := 0;

  // The journal comes up BEFORE the listening socket: no client may reach a
  // half-recovered broker (recovery proper is WS6, and it goes exactly here).
  // If it does not come up -- directory already owned, disk without
  // permission --, Start raises and nothing was promised to anyone.
  if FDataDir <> '' then
  begin
    FJournal := TAMQPJournal.Create(FDataDir);
    FJournal.MaxJournalBytes := FMaxJournalBytes;
    FJournal.Events := FEventBus;
    // The confirm registry IS the journal's IAMQPDurabilitySink: that is how
    // "the watermark moved" becomes "these channels may confirm" (D24).
    // Injected BEFORE Start, as the property requires.
    FConfirms := TAMQPConfirmRegistry.Create;
    FJournal.DurabilitySink := FConfirms as IAMQPDurabilitySink;
    try
      FJournal.Start;
    except
      FConfirms := nil;
      FreeAndNil(FJournal);
      raise;
    end;
    FEngine.Journal := FJournal;

    // RECOVERY RUNS HERE: after the journal is up -- and with the directory
    // lock already in hand, which guarantees no other broker is touching the
    // same WAL -- and BEFORE the listening socket opens. Nobody may talk to a
    // half-recovered broker.
    LState := AmqpReplayWal(FDataDir);
    try
      FEngine.Recover(LState);
      FRecoveryStats := LState.Stats;
    finally
      LState.Free;
    end;

    // COMPACTION, and ONLY HERE: the journal is quiescent (recovery has
    // finished and the listening socket has not opened yet, so no publisher
    // or actor is submitting). Compacting in flight would require quiescing
    // LSN allocation, which runs into D2 -- see AMQP.Server.Journal's header.
    // The practical effect: the log is pruned ON EVERY RESTART.
    if (FJournal.CompactAbove > 0)
      and (FJournal.TotalSize >= FJournal.CompactAbove) then
      FJournal.Compact;
  end;
  // The notifier comes up BEFORE the listener: that way the first
  // seConnectionEstablished already finds someone to deliver it.
  FEventBus.Start;

  try
    FListener := OpenListener;
  except
    // Nothing listens: undo what this Start brought up (the same tail as
    // Stop's), so the broker is stopped and a later Start begins clean. A
    // wrong TLS certificate lands here since 0.6.0 (loaded at listen).
    on E: Exception do
    begin
      FEventBus.Stop;
      if FJournal <> nil then
      begin
        FEngine.Journal := nil;
        try
          FJournal.Stop;
        except
        end;
        FreeAndNil(FJournal);
      end;
      FConfirms := nil;
      if E is EPcTransport then
        AmqpRaiseTransportError(EPcTransport(E));
      raise;
    end;
  end;
  FPort := FListener.Port; // actual (relevant if it was 0)
  FRunning := True;
  FAccept := TAMQPAcceptThread.Create(Self);
  FAccept.Start;
  FMonitorStop.ResetEvent;
  FMonitor := TAMQPMonitorThread.Create(Self);
  FMonitor.Start;
end;

procedure TAMQPServer.Stop;
var
  LSnapshot: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  LSnapshot := nil;
  if not FRunning then
  begin
    ReapDead;
    Exit;
  end;
  FStopping := True;

  // 0) stop the monitor BEFORE everything: while it lives, it is the one that
  //    frees dead connections — two concurrent reapers would double-free.
  StopMonitor;

  // 1) stop accepting: close the listener (unblocks the pending Accept).
  if FListener <> nil then
    FListener.Close;
  if FAccept <> nil then
  begin
    FAccept.WaitFor;
    FreeAndNil(FAccept);
  end;
  FreeAndNil(FListener);

  // 2) drop the live connections. Snapshot under the lock, then act without
  //    it (HandleConnClosed also takes the lock).
  FLock.Enter;
  try
    SetLength(LSnapshot, FActive.Count);
    for I := 0 to FActive.Count - 1 do
      LSnapshot[I] := FActive[I];
  finally
    FLock.Leave;
  end;
  for I := 0 to High(LSnapshot) do
    LSnapshot[I].Shutdown;
  for I := 0 to High(LSnapshot) do
    LSnapshot[I].WaitFor;

  // 3) each connection's OnClosed already moved it to FDead; free them all.
  ReapDead;

  // whatever happens to still be in FActive (rare race): move and free.
  FLock.Enter;
  try
    for I := 0 to FActive.Count - 1 do
      FDead.Add(FActive[I]);
    FActive.Clear;
  finally
    FLock.Leave;
  end;
  ReapDead;

  // The journal goes down LAST: the connections are already dead, so nobody
  // submits anymore, and its Stop drains what was accepted before closing.
  if FJournal <> nil then
  begin
    FEngine.Journal := nil;
    try
      FJournal.Stop;
    except
    end;
    FreeAndNil(FJournal);
  end;
  // The registry only goes after the journal: while the journal thread lives,
  // it may be inside a Release.
  FConfirms := nil;

  // The notifier goes down LAST, on purpose: the channel and connection
  // close events are born during the teardown above, and its Stop delivers
  // what is already in the ring before the thread exits.
  FEventBus.Stop;

  FRunning := False;
end;

procedure TAMQPServer.AcceptLoop;
var
  LSock: TPcEndpoint;
  LConn: TAMQPServerConnection;
begin
  while not FStopping do
  begin
    LSock := FListener.Accept;
    if LSock = nil then
      Break; // listener closed
    if FStopping then
    begin
      LSock.Free;
      Break;
    end;

    // (reaping is left to the monitor thread — see MonitorLoop)

    // The connection becomes the endpoint's owner and gets a copy of the broker's config.
    // Under TLS it isn't negotiated yet: the handshake runs on the
    // connection's thread (TAMQPServerConnection.SetupTransport).
    LConn := TAMQPServerConnection.Create(LSock, ConnConfig);
    LConn.OnClosed := HandleConnClosed;
    // Before Start: the FSM consults the engine from Connection.Open on.
    LConn.Engine := FEngine;
    FLock.Enter;
    try
      FActive.Add(LConn);
    finally
      FLock.Leave;
    end;
    PcAtomicInc(FTotalAccepted);
    LConn.Start;
  end;
end;

procedure TAMQPServer.MonitorLoop;
var
  LSnapshot: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  while not FStopping do
  begin
    if FMonitorStop.WaitFor(AMQP_SERVER_MONITOR_TICK_MS) = wrSignaled then
      Break;
    if FStopping then
      Break;

    // Snapshot under the lock; the Ticks run OUTSIDE the lock (HeartbeatTick
    // may post to the writer and the Shutdown of a dead connection calls
    // HandleConnClosed, which also takes the lock). Safe because only THIS
    // thread frees connections while the broker runs, and ReapDead below only
    // happens after the Ticks.
    // WS5: recovers a confirm that got stuck because the channel's write queue
    // was full at the moment of the fsync. Without this nudge, such a channel
    // would only confirm on the NEXT watermark advance -- which may never come,
    // if nobody publishes again. Same idea as PostDeliverTick for delivery (D3).
    if FConfirms <> nil then
      try
        FConfirms.RetryPending;
      except
      end;

    LSnapshot := Connections;
    for I := 0 to High(LSnapshot) do
    begin
      try
        LSnapshot[I].HeartbeatTick;
      except
      end;
      try
        LSnapshot[I].EnforceCloseDeadline;
      except
      end;
    end;

    // D3's degraded case: a consumer that refused and no ack arriving --
    // without this push the queue would only resume delivering on the next publish.
    if FEngine <> nil then
      try
        FEngine.DeliverTick;
      except
      end;

    // WS8: a queue with x-expires that went past its deadline unused is
    // collected here. Same place as DeliverTick because it is the same role:
    // the connection's thread is blocked in read, so whoever has to wake up to
    // look at the clock is this monitor.
    if FEngine <> nil then
      try
        FEngine.ExpireIdleQueues;
      except
      end;

    ReapDead;
  end;
end;

procedure TAMQPServer.SetMaxQueueLength(AValue: Integer);
begin
  if AValue < 0 then
    AValue := 0;
  FMaxQueueLength := AValue;
  if FEngine <> nil then
    FEngine.MaxQueueLength := AValue;
end;

procedure TAMQPServer.StopMonitor;
begin
  if FMonitor = nil then
    Exit;
  FMonitorStop.SetEvent;
  FMonitor.WaitFor;
  FreeAndNil(FMonitor);
end;

procedure TAMQPServer.HandleConnClosed(AConn: TAMQPServerConnection);
begin
  // Fired ON the connection's thread: only moves it to the reap list.
  FLock.Enter;
  try
    FActive.Remove(AConn);
    if FDead.IndexOf(AConn) < 0 then
      FDead.Add(AConn);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPServer.ReapDead;
var
  LToFree: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  LToFree := nil;
  FLock.Enter;
  try
    SetLength(LToFree, FDead.Count);
    for I := 0 to FDead.Count - 1 do
      LToFree[I] := FDead[I];
    FDead.Clear;
  finally
    FLock.Leave;
  end;
  // Free outside the lock: the connection's destructor joins the read thread.
  for I := 0 to High(LToFree) do
    LToFree[I].Free;
end;

function TAMQPServer.ConnectionCount: Integer;
begin
  FLock.Enter;
  try
    Result := FActive.Count;
  finally
    FLock.Leave;
  end;
end;

function TAMQPServer.Connections: TArray<TAMQPServerConnection>;
var
  I: Integer;
begin
  Result := nil;
  FLock.Enter;
  try
    SetLength(Result, FActive.Count);
    for I := 0 to FActive.Count - 1 do
      Result[I] := FActive[I];
  finally
    FLock.Leave;
  end;
end;

function TAMQPServer.GetEventQueueCapacity: Integer;
begin
  Result := FEventBus.Capacity;
end;

procedure TAMQPServer.SetEventQueueCapacity(AValue: Integer);
begin
  FEventBus.Capacity := AValue;
end;

procedure TAMQPServer.Subscribe(AHandler: TAMQPServerEventHandler);
begin
  FEventBus.Subscribe(AHandler);
end;

procedure TAMQPServer.Subscribe(AHandler: TAMQPServerEventHandler;
  const ATypes: TAMQPServerEventTypes);
begin
  FEventBus.Subscribe(AHandler, ATypes);
end;

procedure TAMQPServer.Unsubscribe(AHandler: TAMQPServerEventHandler);
begin
  FEventBus.Unsubscribe(AHandler);
end;

function TAMQPServer.DrainEvents(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FEventBus.Drain(ATimeoutMs);
end;

function TAMQPServer.EventsEmitted: Int64;
begin
  Result := FEventBus.EmittedCount;
end;

function TAMQPServer.EventsDropped: Int64;
begin
  Result := FEventBus.DroppedCount;
end;

function TAMQPServer.EventsFailed: Int64;
begin
  Result := FEventBus.FailedCount;
end;

function TAMQPServer.LastEventFailure: string;
begin
  Result := FEventBus.LastFailure;
end;

end.
