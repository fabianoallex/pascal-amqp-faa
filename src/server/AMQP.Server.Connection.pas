unit AMQP.Server.Connection;

{$I amqp.inc}

{ One broker connection: the read thread, the handshake FSM, the channel map
  and the serial writer (server sub-module, WS4).

  Execution model (see the broker sub-module section of CLAUDE.md): thread-per-connection,
  blocking I/O. ONLY this thread reads the socket and ONLY it touches the FSM
  and the channel map; everything outbound goes through the TAMQPFrameWriter
  (one writer thread per connection). Shutdown (called by the broker from
  another thread) closes the socket to unblock a pending ReadFrom. The
  connection NEVER frees itself (no FreeOnTerminate): the broker reaps it.

  FSM (spec 0-9-1, 1.4.2.1 / 4.2.5 / 4.2.7):

    client: "AMQP" 0 0 9 1
    S->C: Connection.Start   (server-properties, mechanisms, locales)
    C->S: Connection.Start-Ok(client-properties, mechanism, response, locale)
          -> IAMQPAuthenticator.Authenticate
    S->C: Connection.Tune    (proposed channel-max, frame-max, heartbeat)
    C->S: Connection.Tune-Ok (what the client accepts; never above the proposal)
    C->S: Connection.Open    (virtual-host)
    S->C: Connection.Open-Ok -> connection usable (amqssOpen)

  Out of this order, any method is COMMAND_INVALID (503) and brings the
  connection down: that is what stops a client from publishing before it
  authenticates.

  Error model:
  - EAMQPChannelError    -> Channel.Close on the offending channel; the
                            connection goes on.
  - EAMQPConnectionError -> Connection.Close on channel 0; we wait for the
                            Close-Ok (dropping everything else) and end.
  - EAMQPServerAbort     -> drops the socket without a Close.

  WS5 hangs the Exchange/Queue/Basic/Confirm dispatch and content assembly on
  the two virtual hooks (DispatchChannelMethod / DispatchContent); in WS4 they
  refuse and the channel dies with NOT_IMPLEMENTED (540). }

interface

uses
  SysUtils,
  Classes,
  Rtti,
  SyncObjs,
  Generics.Collections,
  PascalCommon.Threading,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  PascalCommon.Transport,
  AMQP.Transport,
  AMQP.Connection.Methods,
  AMQP.Channel.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods,
  AMQP.Server.Auth,
  AMQP.Server.Events,
  AMQP.Server.Types,
  AMQP.Server.Channel,
  AMQP.Server.Delivery,
  AMQP.Server.Message,
  AMQP.Server.Queue,
  AMQP.Server.Resources,
  AMQP.Server.Engine,
  AMQP.Server.FrameIO;

const
  /// frame-max accepted while the Tune-Ok has not arrived (a safety floor
  /// against a client that announces a giant frame before negotiating). After
  /// the Tune-Ok the negotiated value applies.
  AMQP_SERVER_FRAME_MAX_BOOT = 131072;

  /// Octets of a frame that are NOT payload: 7 of header + 1 of frame-end.
  /// The spec's frame-max counts the whole frame; TAMQPFrame.ReadFrom limits
  /// only the payload.
  AMQP_FRAME_OVERHEAD = 8;

type
  TAMQPServerConnection = class;

  /// Notification that the connection ended (the read thread exited). Fired
  /// ON the connection's thread; the handler (the broker) must only move the
  /// connection to the reap list, without blocking.
  TAMQPConnectionClosedEvent = procedure(AConn: TAMQPServerConnection) of object;

  /// States of the FSM above. Written only by the read thread.
  TAMQPServerConnState = (
    amqssHeader,       // waiting for the client's protocol-header
    amqssAwaitStartOk, // Connection.Start sent
    amqssAwaitTuneOk,  // Connection.Tune sent
    amqssAwaitOpen,    // Tune-Ok received, waiting for Connection.Open
    amqssOpen,         // Open-Ok sent: connection usable
    amqssClosing,      // we sent Connection.Close, waiting for the Close-Ok
    amqssClosed        // done (the read thread exits)
  );

  TAMQPServerConnThread = class(TThread)
  private
    FOwner: TAMQPServerConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TAMQPServerConnection);
  end;

  TAMQPServerConnection = class
  private
    // The accepted connection (pascal-common-faa); owned. Under TLS it is
    // negotiated only in SetupTransport, on this connection's thread.
    FEndpoint: TPcEndpoint;
    // TAMQPEndpointStream over FEndpoint: the AMQP byte stream, plain or TLS
    // alike. Doesn't own the endpoint.
    FStream: TStream;
    FWriter: TAMQPFrameWriter;
    FThread: TAMQPServerConnThread;
    FConfig: TAMQPServerConnConfig;
    FPeer: string;
    FFramesRead: Integer;      // atomic
    FProtocolOk: Boolean;
    FError: string;
    FClosing: Integer;         // atomic (0/1)
    FOnClosed: TAMQPConnectionClosedEvent;

    // --- FSM state (only the read thread writes) ---
    FState: TAMQPServerConnState;
    FNegotiated: TAMQPConnectionTune;
    FUserId: string;
    FVirtualHost: string;
    FAuthFailureClose: Boolean; // the client announced the capability
    FCloseDeadline: UInt64;     // deadline tick for the Close-Ok (0 = none)
    FLastReadTick: UInt64;      // atomic; last frame ARRIVED from the peer
    FEngine: TAMQPEngine;       // WS5: who actually declares/routes/delivers
    FConnId: NativeUInt;        // identity of this connection (exclusive queue)

    // --- channels ---
    FChLock: TCriticalSection;
    FJoinLock: TCriticalSection;
    FJoined: Boolean;
    FChannels: TDictionary<Word, TAMQPServerChannel>;

    procedure SetupTransport;
    function ReadProtocolHeader: Boolean;

    // --- observability (Phase 4.1) ---
    // NewEvent already fills in the connection context, so the emit site only
    // takes care of what is specific to the event. Emit NEVER blocks and NEVER
    // raises (D31), so none of these three needs a try/except.
    function WantsEvent(AType: TAMQPServerEventType): Boolean;
    function NewEvent(AType: TAMQPServerEventType): TAMQPServerEvent;
    procedure EmitEvent(const AEvent: TAMQPServerEvent);
    /// Shortcut for the event that only has connection and channel context.
    procedure EmitSimpleEvent(AType: TAMQPServerEventType; AChannel: Word);
    procedure RunReadLoop;
    function CurrentMaxPayload: Cardinal;

    procedure HandleFrame(const AFrame: TAMQPFrame);
    procedure HandleMethodFrame(const AFrame: TAMQPFrame);
    procedure HandleConnectionMethod(const AId: TAMQPMethodId;
      const AReader: TAMQPReader);
    procedure HandleChannelFrame(const AFrame: TAMQPFrame;
      const AId: TAMQPMethodId; const AReader: TAMQPReader);
    procedure HandleContentFrame(const AFrame: TAMQPFrame);

    procedure DoStartOk(const AReader: TAMQPReader);
    procedure DoTuneOk(const AReader: TAMQPReader);
    procedure DoOpen(const AReader: TAMQPReader);

    procedure PostMethod(AChannel: Word; const APayload: TBytes);
    procedure SendConnectionStart;
    procedure SendTune;
    procedure SendConnectionClose(ACode: Word; const AText: string;
      AClassId, AMethodId: Word);
    procedure SendChannelClose(AChannel, ACode: Word; const AText: string;
      AClassId, AMethodId: Word);

    function FindChannel(AId: Word): TAMQPServerChannel;
    function AddChannel(AId: Word): TAMQPServerChannel;
    procedure DropChannel(AId: Word);
    procedure ClearChannels;
    procedure DetachAllDelivery;

    function ClientWantsAuthFailureClose(AProps: TAMQPFieldTable): Boolean;
    function MechanismOffered(const AMechanism: string): Boolean;

    function GeneratedName(const APrefix: string): string;
    /// Translates the engine's result into the AMQP error model. amqerOk
    /// does nothing; the rest raises EAMQPChannelError with the table's
    /// reply-code (403/404/405/406) -- all are CHANNEL errors: the connection
    /// survives and the client can open another channel.
    procedure CheckEngine(AChannel: TAMQPServerChannel;
      ARes: TAMQPEngineResult; const AWhat: string;
      AClassId, AMethodId: Word);
    /// Raises 406 PRECONDITION_FAILED when a declare's x-argument is invalid,
    /// with the AError the parser produced -- which NAMES the offending
    /// argument. CHANNEL error: a wrong config in a declare does not take the
    /// connection with it (same line as amqerTipoInvalido; see CheckEngine).
    procedure CheckArgs(AChannel: TAMQPServerChannel; AOk: Boolean;
      const AError: string; AClassId, AMethodId: Word);
    /// Raises 403 if the name is reserved ('amq.') or empty when the client
    /// may not choose -- spec 1.4: the prefix belongs to the server.
    procedure CheckReservedName(AChannel: TAMQPServerChannel;
      const AName, AWhat: string; AClassId, AMethodId: Word);
    /// Tells the engine to let go of what this channel held (consumers and
    /// unacked messages go back to the queues) and detaches the delivery target.
    procedure ReleaseChannelResources(AChannel: TAMQPServerChannel);
    /// Basic.Return + content-header + body of a `mandatory` publish that found
    /// no route (spec 1.8.3.6, reply-code 312 NO_ROUTE).
    /// Emits (or defers until the watermark) the confirm of a publish. ALsn=0
    /// means nothing was written to the journal by this publish.
    procedure ConfirmPublish(AChannel: TAMQPServerChannel;
      ASeq, ALsn: UInt64; ANack: Boolean);
    procedure SendBasicReturn(AChannel: TAMQPServerChannel;
      const AMessage: TAMQPServerMessage);
    procedure SendGetOk(AChannel: TAMQPServerChannel;
      ADeliveryTag: UInt64; ARedelivered: Boolean; AMessageCount: Integer;
      AMessage: TAMQPMessage);
    function DispatchExchange(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    function DispatchQueue(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    function DispatchBasic(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    function DispatchConfirm(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    procedure CompleteContent(AChannel: TAMQPServerChannel);
  protected
    /// WS5/Phase 2: dispatches a method of the Exchange/Queue/Basic/
    /// Confirm/Tx classes on an open channel. Returning False = method not
    /// handled, and the FSM closes the channel with NOT_IMPLEMENTED (540). Raise
    /// EAMQPChannelError / EAMQPConnectionError for the other protocol errors.
    function DispatchChannelMethod(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean; virtual;
    /// WS5: content-header / content-body of a Basic.Publish in progress.
    /// False = no publish was in progress -> UNEXPECTED_FRAME (505).
    function DispatchContent(AChannel: TAMQPServerChannel;
      const AFrame: TAMQPFrame): Boolean; virtual;
  public
    constructor Create(AEndpoint: TPcEndpoint;
      const AConfig: TAMQPServerConnConfig);
    destructor Destroy; override;

    /// Starts the read thread.
    procedure Start;
    /// Closes the socket (unblocks the ReadFrom) and stops the writer. Callable
    /// from another thread and more than once. Does not join: use WaitFor.
    procedure Shutdown;
    /// Blocks until the read thread exits.
    procedure WaitFor;

    /// Drops the connection if it is waiting for a Connection.Close-Ok that did
    /// not come within FConfig.CloseTimeoutMs. Callable from another thread.
    procedure EnforceCloseDeadline;

    /// One heartbeat step (spec 4.2.7), called periodically by the broker's
    /// monitor thread, NOT by this connection's thread, which is blocked in the
    /// read. Does two things, both over the interval negotiated in the Tune-Ok:
    ///  - sending idle for >= half the interval -> posts a heartbeat;
    ///  - nothing received for > 2x the interval -> considers the peer dead and
    ///    drops the connection (closes the socket, which unblocks the read).
    /// No-op while the Tune-Ok has not arrived or if the negotiated interval is 0.
    procedure HeartbeatTick;

    /// Tick of the last frame received from the peer (0 if none). Diagnostics/tests.
    function LastReadTick: UInt64;

    /// Number of channels open right now (thread-safe).
    function ChannelCount: Integer;

    property Peer: string read FPeer;
    property Writer: TAMQPFrameWriter read FWriter;
    property FramesRead: Integer read FFramesRead;
    property ProtocolOk: Boolean read FProtocolOk;
    property LastError: string read FError;
    /// FSM snapshot (written by the connection's thread; reading it from
    /// outside is informational, for logs/tests).
    property State: TAMQPServerConnState read FState;
    /// Filled in during the handshake; valid from amqssOpen on.
    property UserId: string read FUserId;
    property VirtualHost: string read FVirtualHost;
    property Negotiated: TAMQPConnectionTune read FNegotiated;
    property OnClosed: TAMQPConnectionClosedEvent read FOnClosed write FOnClosed;
    /// The broker's engine (WS5). Assigned by TAMQPServer right after
    /// accepting the connection and BEFORE Start. nil = broker without an engine
    /// (Phase 1 answered the *-Ok without doing anything) -- the handshake tests
    /// use it that way.
    property Engine: TAMQPEngine read FEngine write FEngine;
    /// Identity of this connection, used as the owner of an exclusive queue.
    property ConnId: NativeUInt read FConnId;
  end;

implementation

var
  // Connection identity (owner of an exclusive queue). A monotonic counter and
  // not an object address: a reused address would match an orphaned exclusive
  // queue with the wrong connection -- same reason as the channel identity in
  // AMQP.Server.Delivery.
  GConnIdSeq: Integer = 0;

{ TAMQPServerConnThread }

constructor TAMQPServerConnThread.Create(AOwner: TAMQPServerConnection);
begin
  FOwner := AOwner;
  inherited Create(True); // suspended; explicit Start
end;

procedure TAMQPServerConnThread.Execute;
begin
  FOwner.RunReadLoop;
end;

{ TAMQPServerConnection }

constructor TAMQPServerConnection.Create(AEndpoint: TPcEndpoint;
  const AConfig: TAMQPServerConnConfig);
begin
  inherited Create;
  FEndpoint := AEndpoint;
  FConfig := AConfig;
  if not FEndpoint.TryPeerAddress(FPeer) then
    FPeer := '';
  FState := amqssHeader;
  FStream := TAMQPEndpointStream.Create(FEndpoint);
  // FWriter is only born in SetupTransport, on this connection's thread, after
  // the TLS handshake: that is blocking and cannot run on the accept thread,
  // otherwise a slow (or hostile) client stalls the whole broker.
  FChLock := TCriticalSection.Create;
  FJoinLock := TCriticalSection.Create;
  FChannels := TDictionary<Word, TAMQPServerChannel>.Create;
  FConnId := NativeUInt(Cardinal(PcAtomicInc(GConnIdSeq)));
  FThread := TAMQPServerConnThread.Create(Self);
end;

destructor TAMQPServerConnection.Destroy;
begin
  Shutdown;
  if FThread <> nil then
  begin
    WaitFor; // guarded: join only once
    FThread.Free;
  end;
  FWriter.Free;   // Stop was already called in Shutdown (nil-safe)
  FStream.Free;
  FEndpoint.Free; // the connection OWNS the accepted endpoint
  ClearChannels;
  FChannels.Free;
  FChLock.Free;
  FJoinLock.Free;
  inherited;
end;

procedure TAMQPServerConnection.Start;
begin
  FThread.Start;
end;

procedure TAMQPServerConnection.Shutdown;
begin
  if PcAtomicCompareExchange(FClosing, 1, 0) <> 0 then
    Exit; // already in progress
  // Closes the socket: unblocks a pending ReadFrom on the read thread.
  // Guarded: Shutdown is called by the broker's Stop and by the destructor, and
  // a failure here (peer already gone) must not bring down the whole broker's teardown.
  if FEndpoint <> nil then
    try
      FEndpoint.CloseAbort;
    except
    end;
  DetachAllDelivery; // no delivery can reach the writer after this point
  if FWriter <> nil then
    FWriter.Stop;
end;

procedure TAMQPServerConnection.WaitFor;
begin
  if FThread = nil then
    Exit;
  // JOIN ONLY ONCE -- same reason as TAMQPFrameWriter.Stop: on FPC/Unix
  // TThread.WaitFor is an UNCONDITIONAL pthread_join, and this WaitFor is called
  // by TAMQPServer.Stop AND by the connection's destructor.
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

function TAMQPServerConnection.LastReadTick: UInt64;
begin
  Result := PcAtomicRead64(FLastReadTick);
end;

procedure TAMQPServerConnection.HeartbeatTick;
var
  LIntervalMs, LNow, LLastRead, LLastWrite: UInt64;
begin
  // Before the Tune-Ok there is no negotiated interval; after Closing it is not
  // worth sending a heartbeat (we are already ending).
  if not (FState in [amqssAwaitOpen, amqssOpen]) then
    Exit;
  LIntervalMs := UInt64(FNegotiated.Heartbeat) * 1000;
  if LIntervalMs = 0 then
    Exit; // heartbeat disabled by the client in the Tune-Ok

  LNow := PcTickMs;

  // Dead peer: the spec says to drop after two intervals with NOTHING arriving
  // (any frame counts, not just heartbeats). Closing the socket unblocks this
  // connection's thread's ReadFrom, which then ends along the normal path.
  LLastRead := PcAtomicRead64(FLastReadTick);
  if (LLastRead <> 0) and ((LNow - LLastRead) > (2 * LIntervalMs)) then
  begin
    if FError = '' then
      FError := Format('peer has not sent frames for more than %d ms (heartbeat)',
        [2 * LIntervalMs]);
    Shutdown;
    Exit;
  end;

  // Idle sending: sends a heartbeat so the peer does not judge us dead. Half
  // an interval leaves slack for the frame to arrive before its deadline.
  LLastWrite := FWriter.LastWriteTick;
  if (LLastWrite = 0) or ((LNow - LLastWrite) >= (LIntervalMs div 2)) then
    try
      FWriter.PostFrame(TAMQPFrame.Heartbeat);
    except
      // writer stopped/failed: the read thread will notice soon enough
    end;
end;

procedure TAMQPServerConnection.EnforceCloseDeadline;
begin
  if (FState = amqssClosing) and (FCloseDeadline <> 0) and
     (PcTickMs > FCloseDeadline) then
  begin
    if FError = '' then
      FError := 'client did not respond to Connection.Close within the deadline';
    Shutdown;
  end;
end;

{ --- channels -------------------------------------------------------------- }

function TAMQPServerConnection.FindChannel(AId: Word): TAMQPServerChannel;
begin
  FChLock.Enter;
  try
    if not FChannels.TryGetValue(AId, Result) then
      Result := nil;
  finally
    FChLock.Leave;
  end;
end;

function TAMQPServerConnection.AddChannel(AId: Word): TAMQPServerChannel;
begin
  Result := TAMQPServerChannel.Create(AId);
  // WS4: the channel is born with a delivery target already. DropChannel/
  // ClearChannels (and, as a last resort, the channel's destructor) do the
  // Detach -- without it a queue delivering on a pool thread would write to an
  // already freed writer.
  Result.AttachDelivery(FWriter, CurrentMaxPayload);
  FChLock.Enter;
  try
    FChannels.Add(AId, Result);
  finally
    FChLock.Leave;
  end;
end;

procedure TAMQPServerConnection.DropChannel(AId: Word);
var
  LCh: TAMQPServerChannel;
begin
  LCh := nil;
  FChLock.Enter;
  try
    if FChannels.TryGetValue(AId, LCh) then
      FChannels.Remove(AId);
  finally
    FChLock.Leave;
  end;
  if LCh <> nil then
  begin
    // Before freeing: the queues need to know that this channel's consumers
    // are gone and that its unacked messages go back to the queue.
    ReleaseChannelResources(LCh);
    LCh.Free;
    // Here, and not at the three sites that call DropChannel: it is the funnel
    // ALL channel closing goes through -- the client's Close, the Close-Ok of
    // a Close of ours, and the connection teardown.
    EmitSimpleEvent(seChannelClosed, AId);
  end;
end;

// Releases the delivery target of every channel. Called in Shutdown BEFORE
// stopping the writer: from here on every in-flight delivery (on a pool
// thread) fails cleanly instead of writing to a writer that is being torn down.
procedure TAMQPServerConnection.DetachAllDelivery;
var
  LCh: TAMQPServerChannel;
begin
  FChLock.Enter;
  try
    for LCh in FChannels.Values do
      LCh.DetachDelivery;
  finally
    FChLock.Leave;
  end;
end;

procedure TAMQPServerConnection.ClearChannels;
var
  LAll: TArray<TAMQPServerChannel>;
  LCh: TAMQPServerChannel;
  I: Integer;
  LId: Word;
  LEv: TAMQPServerEvent;
begin
  LAll := nil;
  FChLock.Enter;
  try
    SetLength(LAll, FChannels.Count);
    I := 0;
    for LCh in FChannels.Values do
    begin
      LAll[I] := LCh;
      Inc(I);
    end;
    FChannels.Clear;
  finally
    FChLock.Leave;
  end;
  for I := 0 to High(LAll) do
  begin
    LId := LAll[I].Id;
    ReleaseChannelResources(LAll[I]);
    LAll[I].Free;
    // A channel that dies TOGETHER with the connection also closes, and also
    // emits. Without this an observer counting opens against closes would
    // report a channel leak for every dropped connection -- which is what the
    // sample's run against the SmokeTest showed (3 opens, 2 closes).
    // Reason tells this path apart from the Channel.Close the client asked for.
    if WantsEvent(seChannelClosed) then
    begin
      LEv := NewEvent(seChannelClosed);
      LEv.ChannelNumber := LId;
      LEv.Reason := 'connection teardown';
      EmitEvent(LEv);
    end;
  end;
end;

function TAMQPServerConnection.ChannelCount: Integer;
begin
  FChLock.Enter;
  try
    Result := FChannels.Count;
  finally
    FChLock.Leave;
  end;
end;

{ --- output --------------------------------------------------------------- }

procedure TAMQPServerConnection.PostMethod(AChannel: Word;
  const APayload: TBytes);
begin
  FWriter.PostFrame(TAMQPFrame.Create(AMQP_FRAME_METHOD, AChannel, APayload));
end;

procedure TAMQPServerConnection.SendConnectionStart;
var
  LProps: TAMQPFieldTable;
  LMechs: string;
  LList: TArray<string>;
  I: Integer;
begin
  LMechs := '';
  if FConfig.Auth <> nil then
  begin
    LList := FConfig.Auth.Mechanisms;
    for I := 0 to High(LList) do
      if LMechs = '' then
        LMechs := LList[I]
      else
        LMechs := LMechs + ' ' + LList[I];
  end;
  if LMechs = '' then
    LMechs := AMQP_MECH_PLAIN;

  LProps := DefaultServerProperties;
  try
    PostMethod(AMQP_CHANNEL_CONNECTION,
      BuildStart(AMQP_VERSION_MAJOR, AMQP_VERSION_MINOR, LProps,
        LMechs, AMQP_LOCALE_DEFAULT));
  finally
    LProps.Free;
  end;
end;

procedure TAMQPServerConnection.SendTune;
var
  LTune: TAMQPConnectionTune;
begin
  LTune.ChannelMax := FConfig.ChannelMax;
  LTune.FrameMax := FConfig.FrameMax;
  LTune.Heartbeat := FConfig.Heartbeat;
  PostMethod(AMQP_CHANNEL_CONNECTION, BuildTune(LTune));
end;

procedure TAMQPServerConnection.SendConnectionClose(ACode: Word;
  const AText: string; AClassId, AMethodId: Word);
var
  LClose: TAMQPConnectionClose;
begin
  LClose.ReplyCode := ACode;
  LClose.ReplyText := AText;
  LClose.ClassId := AClassId;
  LClose.MethodId := AMethodId;
  if FError = '' then
    FError := Format('%d %s', [ACode, AText]);
  PostMethod(AMQP_CHANNEL_CONNECTION, BuildClose(LClose));
  FCloseDeadline := PcTickMs + FConfig.CloseTimeoutMs;
end;

procedure TAMQPServerConnection.SendChannelClose(AChannel, ACode: Word;
  const AText: string; AClassId, AMethodId: Word);
var
  LClose: TAMQPCloseInfo;
  LCh: TAMQPServerChannel;
begin
  LClose.ReplyCode := ACode;
  LClose.ReplyText := AText;
  LClose.ClassId := AClassId;
  LClose.MethodId := AMethodId;
  PostMethod(AChannel, BuildChannelClose(LClose));
  // The channel stays alive until the Channel.Close-Ok; in the meantime
  // everything arriving on it is dropped (spec 1.4.2.2).
  LCh := FindChannel(AChannel);
  if LCh <> nil then
    LCh.State := amqchClosing;
end;

{ --- handshake ------------------------------------------------------------ }

function TAMQPServerConnection.MechanismOffered(
  const AMechanism: string): Boolean;
var
  LList: TArray<string>;
  I: Integer;
begin
  Result := False;
  if FConfig.Auth = nil then
    Exit;
  LList := FConfig.Auth.Mechanisms;
  for I := 0 to High(LList) do
    if SameText(LList[I], AMechanism) then
      Exit(True);
end;

function TAMQPServerConnection.ClientWantsAuthFailureClose(
  AProps: TAMQPFieldTable): Boolean;
var
  LValue, LFlag: TValue;
  LObj: TObject;
begin
  // Defensive read: anything outside the expected format becomes False
  // (and the spec says, in that case, to close the socket without Connection.Close).
  Result := False;
  if AProps = nil then
    Exit;
  if not AProps.TryGetValue('capabilities', LValue) then
    Exit;
  LValue := AmqpUnwrapValue(LValue);
  if not LValue.IsObject then
    Exit;
  LObj := LValue.AsObject;
  if not (LObj is TAMQPFieldTable) then
    Exit;
  if not TAMQPFieldTable(LObj).TryGetValue('authentication_failure_close',
    LFlag) then
    Exit;
  try
    Result := LFlag.AsBoolean;
  except
    Result := False;
  end;
end;

procedure TAMQPServerConnection.DoStartOk(const AReader: TAMQPReader);
var
  LStartOk: TAMQPConnectionStartOk;
  LRes: TAMQPAuthResult;
  LBadMech: Boolean;
begin
  LStartOk := DecodeStartOk(AReader);
  try
    FAuthFailureClose := ClientWantsAuthFailureClose(LStartOk.ClientProperties);
    LBadMech := not MechanismOffered(LStartOk.Mechanism);
    if LBadMech then
      // Format (not '+'): concatenating a UTF-8 literal with an AnsiString
      // triggers an implicit conversion on FPC.
      LRes := TAMQPAuthResult.Deny(
        Format('unsupported mechanism: %s', [LStartOk.Mechanism]))
    else
      LRes := FConfig.Auth.Authenticate(LStartOk.Mechanism, LStartOk.Response,
        FPeer, FConfig.Tls);
  finally
    LStartOk.ClientProperties.Free;
  end;

  if not LRes.Granted then
  begin
    // A client that announced 'authentication_failure_close' gets a
    // Connection.Close with the reason; the others only see the socket drop
    // (spec / historical broker behavior).
    if not FAuthFailureClose then
      raise EAMQPServerAbort.Create(
        Format('authentication refused: %s', [LRes.FailReason]));
    if LBadMech then
      raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED, LRes.FailReason,
        AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK);
    raise EAMQPConnectionError.Create(AMQP_ACCESS_REFUSED, LRes.FailReason,
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK);
  end;

  FUserId := LRes.UserId;
  SendTune;
  FState := amqssAwaitTuneOk;
end;

procedure TAMQPServerConnection.DoTuneOk(const AReader: TAMQPReader);
var
  LTune: TAMQPConnectionTune;
begin
  LTune := DecodeTuneOk(AReader);

  // The client may not ask for more than we proposed (0 = "no limit", which
  // is MORE than any finite limit of ours).
  if (FConfig.ChannelMax > 0) and
     ((LTune.ChannelMax = 0) or (LTune.ChannelMax > FConfig.ChannelMax)) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('invalid negotiated channel-max: %d (maximum %d)',
        [LTune.ChannelMax, FConfig.ChannelMax]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK);

  if (FConfig.FrameMax > 0) and
     ((LTune.FrameMax = 0) or (LTune.FrameMax > FConfig.FrameMax)) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('invalid negotiated frame-max: %u (maximum %u)',
        [LTune.FrameMax, FConfig.FrameMax]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK);

  // Spec floor (4.2.5): every peer must accept 4096 octets.
  if (LTune.FrameMax <> 0) and (LTune.FrameMax < AMQP_FRAME_MIN_SIZE) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('frame-max below spec minimum: %u (minimum %d)',
        [LTune.FrameMax, AMQP_FRAME_MIN_SIZE]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK);

  // The heartbeat is the client's call (spec: "the delay ... that the client
  // wants"); we accept whatever it sends.
  FNegotiated := LTune;
  FState := amqssAwaitOpen;
end;

procedure TAMQPServerConnection.DoOpen(const AReader: TAMQPReader);
var
  LOpen: TAMQPConnectionOpen;
begin
  LOpen := DecodeOpen(AReader);
  if (FConfig.VHosts = nil) or (not FConfig.VHosts.Contains(LOpen.VirtualHost)) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('virtual host "%s" does not exist on this broker', [LOpen.VirtualHost]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN);

  FVirtualHost := LOpen.VirtualHost;
  if FEngine <> nil then
    // This vhost's topology comes into existence (with the predefined
    // exchanges) the moment someone opens on it.
    FEngine.EnsureVHost(FVirtualHost);
  PostMethod(AMQP_CHANNEL_CONNECTION, BuildOpenOk);
  FState := amqssOpen;
  // Here, and not at the end of Start-Ok: only after the Open-Ok are the user
  // AND the vhost both defined, and that pair is what an auditor cares about.
  EmitSimpleEvent(seConnectionAuthenticated, 0);
end;

{ --- dispatch ------------------------------------------------------------- }

procedure TAMQPServerConnection.HandleConnectionMethod(const AId: TAMQPMethodId;
  const AReader: TAMQPReader);
begin
  // Connection.Close and Close-Ok are valid in ANY state: the client may give
  // up in the middle of the handshake.
  if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
  begin
    DecodeClose(AReader);
    PostMethod(AMQP_CHANNEL_CONNECTION, BuildCloseOk);
    FState := amqssClosed;
    Exit;
  end;
  if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK) then
  begin
    DecodeCloseOk(AReader);
    FState := amqssClosed;
    Exit;
  end;

  case FState of
    amqssAwaitStartOk:
      if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK) then
      begin
        DoStartOk(AReader);
        Exit;
      end;
    amqssAwaitTuneOk:
      if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK) then
      begin
        DoTuneOk(AReader);
        Exit;
      end;
    amqssAwaitOpen:
      if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN) then
      begin
        DoOpen(AReader);
        Exit;
      end;
  end;

  // Out of order (or a server-only method, like Blocked/Unblocked, coming from
  // the client): the spec says COMMAND_INVALID.
  raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
    Format('unexpected method %d/%d on channel 0 at this point of the handshake',
      [AId.ClassId, AId.MethodId]), AId.ClassId, AId.MethodId);
end;

procedure TAMQPServerConnection.HandleChannelFrame(const AFrame: TAMQPFrame;
  const AId: TAMQPMethodId; const AReader: TAMQPReader);
var
  LCh: TAMQPServerChannel;
begin
  LCh := FindChannel(AFrame.Channel);

  // Channel closing: only the Close-Ok matters; the rest is dropped.
  if (LCh <> nil) and (LCh.State = amqchClosing) then
  begin
    if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK) then
      DropChannel(AFrame.Channel);
    Exit;
  end;

  // Interleave rule (spec 2.3.5): between the method that carries content
  // (Basic.Publish) and the last body frame, NO other frame may appear on
  // THIS channel -- not another method, not a Channel.Close. It holds per
  // channel only: other channels remain free to interleave, which is exactly
  // why the assembly state lives in the channel.
  if (LCh <> nil) and (LCh.AsmState <> amqasIdle) then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('method %d/%d on channel %d in the middle of a content body',
        [AId.ClassId, AId.MethodId, AFrame.Channel]), AId.ClassId, AId.MethodId);

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN) then
  begin
    DecodeChannelOpen(AReader);
    if LCh <> nil then
      raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
        Format('second Channel.Open on channel %d', [AFrame.Channel]),
        AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN);
    if (FNegotiated.ChannelMax > 0) and (AFrame.Channel > FNegotiated.ChannelMax) then
      raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
        Format('channel %d exceeds negotiated channel-max (%d)',
          [AFrame.Channel, FNegotiated.ChannelMax]),
        AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN);
    AddChannel(AFrame.Channel);
    PostMethod(AFrame.Channel, BuildChannelOpenOk);
    EmitSimpleEvent(seChannelOpened, AFrame.Channel);
    Exit;
  end;

  // From here on the channel must exist: using a channel that is not open is a
  // CONNECTION error (504), not a channel one -- there is no channel to send the Channel.Close on.
  if LCh = nil then
    raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
      Format('method %d/%d on channel %d, which is not open',
        [AId.ClassId, AId.MethodId, AFrame.Channel]), AId.ClassId, AId.MethodId);

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE) then
  begin
    DecodeChannelClose(AReader);
    PostMethod(AFrame.Channel, BuildChannelCloseOk);
    DropChannel(AFrame.Channel);
    Exit;
  end;

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK) then
    // Close-Ok without a Close of ours: the channel was not closing.
    raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
      Format('unexpected Channel.Close-Ok on channel %d', [AFrame.Channel]),
      AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK);

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW) then
  begin
    LCh.Active := DecodeChannelFlow(AReader);
    PostMethod(AFrame.Channel, BuildChannelFlowOk(LCh.Active));
    Exit;
  end;

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW_OK) then
  begin
    DecodeChannelFlowOk(AReader); // reply to a Flow of ours; nothing to do
    Exit;
  end;

  // Exchange/Queue/Basic/Confirm/Tx: WS5.
  if not DispatchChannelMethod(LCh, AId, AReader) then
    raise EAMQPChannelError.Create(AFrame.Channel, AMQP_NOT_IMPLEMENTED,
      Format('method %d/%d is not implemented by this broker',
        [AId.ClassId, AId.MethodId]), AId.ClassId, AId.MethodId);
end;

procedure TAMQPServerConnection.HandleMethodFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);

    if AFrame.Channel = AMQP_CHANNEL_CONNECTION then
    begin
      if LId.ClassId <> AMQP_CLASS_CONNECTION then
        raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
          Format('class %d is not allowed on channel 0', [LId.ClassId]),
          LId.ClassId, LId.MethodId);
      HandleConnectionMethod(LId, LReader);
      Exit;
    end;

    // Channel != 0 only after the Open-Ok.
    if FState <> amqssOpen then
      raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
        Format('method %d/%d on channel %d before Connection.Open-Ok',
          [LId.ClassId, LId.MethodId, AFrame.Channel]),
        LId.ClassId, LId.MethodId);

    if LId.ClassId = AMQP_CLASS_CONNECTION then
      raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
        'Connection methods are only valid on channel 0',
        LId.ClassId, LId.MethodId);

    HandleChannelFrame(AFrame, LId, LReader);
  finally
    LReader.Free;
  end;
end;

procedure TAMQPServerConnection.HandleContentFrame(const AFrame: TAMQPFrame);
var
  LCh: TAMQPServerChannel;
begin
  if (AFrame.Channel = AMQP_CHANNEL_CONNECTION) or (FState <> amqssOpen) then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('content frame (type %d) on channel %d out of context',
        [AFrame.FrameType, AFrame.Channel]), 0, 0);

  LCh := FindChannel(AFrame.Channel);
  if LCh = nil then
    raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
      Format('content on channel %d, which is not open', [AFrame.Channel]),
      0, 0);
  if LCh.State = amqchClosing then
    Exit; // dropped until the Close-Ok

  if not DispatchContent(LCh, AFrame) then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('content frame (type %d) out of sequence on channel %d ' +
        '(assembly state: %d)',
        [AFrame.FrameType, AFrame.Channel, Ord(LCh.AsmState)]), 0, 0);
end;

procedure TAMQPServerConnection.HandleFrame(const AFrame: TAMQPFrame);
begin
  case AFrame.FrameType of
    AMQP_FRAME_HEARTBEAT:
      begin
        if AFrame.Channel <> AMQP_CHANNEL_CONNECTION then
          raise EAMQPConnectionError.Create(AMQP_FRAME_ERROR,
            'Heartbeat outside of channel 0', 0, 0);
        // Nothing to reply: heartbeats are not echoed. Each side emits on its
        // own timer (see HeartbeatTick); receiving one already counted, because
        // FLastReadTick was updated in the read loop.
      end;
    AMQP_FRAME_METHOD:
      HandleMethodFrame(AFrame);
    AMQP_FRAME_HEADER, AMQP_FRAME_BODY:
      HandleContentFrame(AFrame);
  else
    raise EAMQPConnectionError.Create(AMQP_FRAME_ERROR,
      Format('unknown frame type: %d', [AFrame.FrameType]), 0, 0);
  end;
end;

{ --- dispatch of the resource classes (Phase 1's "null" broker) ------------ }

var
  // Sequence of the PROCESS, not of the connection: the queue name lives in
  // the vhost, which is shared. It used to be a per-connection counter (every
  // connection started at 1), and two connections in the same millisecond
  // generated the same amq.gen-1-<tick> -- with the first exclusive queue
  // alive, the second got 405 (test FilaAnonima_NomeNaoRepeteEntreConexoes).
  GNameSeq: Integer = 0;

// Names the server generates when the client sends an empty one (anonymous
// queue, automatic consumer-tag). Unique within the process; the tick tells one
// process from the next (durable queues with a generated name come back in recovery).
function TAMQPServerConnection.GeneratedName(const APrefix: string): string;
begin
  Result := Format('%s%d-%d', [APrefix, PcAtomicInc(GNameSeq),
    Integer(PcTickMs and $FFFFFF)]);
end;

procedure TAMQPServerConnection.CheckArgs(AChannel: TAMQPServerChannel;
  AOk: Boolean; const AError: string; AClassId, AMethodId: Word);
begin
  if AOk then
    Exit;
  raise EAMQPChannelError.Create(AChannel.Id, AMQP_PRECONDITION_FAILED,
    AError, AClassId, AMethodId);
end;

procedure TAMQPServerConnection.CheckEngine(AChannel: TAMQPServerChannel;
  ARes: TAMQPEngineResult; const AWhat: string; AClassId, AMethodId: Word);
var
  LCode: Word;
  LText: string;
begin
  if ARes = amqerOk then
    Exit;
  // A CONNECTION error, not a channel one, and therefore outside the case: a
  // broker that can no longer record durable topology is broken as a whole
  // (Phase 4, WS3). Going on answering Declare-Ok would be lying about
  // durability; 541 tells the truth and drops the connection.
  if ARes = amqerNoDurability then
    raise EAMQPConnectionError.Create(AMQP_INTERNAL_ERROR,
      'could not journal durable ' + AWhat + ': broker cannot guarantee '
      + 'durability', AClassId, AMethodId);
  case ARes of
    amqerNotFound:
      begin
        LCode := AMQP_NOT_FOUND;
        LText := 'no ' + AWhat;
      end;
    amqerPreconditionFailed:
      begin
        LCode := AMQP_PRECONDITION_FAILED;
        LText := 'inequivalent arg for ' + AWhat;
      end;
    amqerCommandInvalid:
      begin
        // Deliberate deviation from RabbitMQ, decided in WS2 and recorded in
        // CLAUDE.md: it treats an unknown exchange type as 503
        // COMMAND_INVALID, which drops the CONNECTION. Here it is a channel
        // 406 -- a wrong config in a declare need not take the connection with it.
        LCode := AMQP_PRECONDITION_FAILED;
        LText := 'invalid type or match for ' + AWhat;
      end;
    amqerInUse:
      begin
        LCode := AMQP_PRECONDITION_FAILED;
        LText := AWhat + ' in use';
      end;
    amqerNotEmpty:
      begin
        LCode := AMQP_PRECONDITION_FAILED;
        LText := AWhat + ' not empty';
      end;
    amqerAccessRefused:
      begin
        LCode := AMQP_ACCESS_REFUSED;
        LText := 'access to ' + AWhat + ' refused: reserved name';
      end;
    amqerResourceLocked:
      begin
        LCode := AMQP_RESOURCE_LOCKED;
        LText := 'cannot obtain exclusive access to ' + AWhat;
      end;
  else
    LCode := AMQP_PRECONDITION_FAILED;
    LText := AWhat;
  end;
  raise EAMQPChannelError.Create(AChannel.Id, LCode, LText, AClassId,
    AMethodId);
end;

procedure TAMQPServerConnection.CheckReservedName(
  AChannel: TAMQPServerChannel; const AName, AWhat: string;
  AClassId, AMethodId: Word);
begin
  if AmqpIsReservedName(AName) then
    CheckEngine(AChannel, amqerAccessRefused, AWhat + ' ' + AName,
      AClassId, AMethodId);
end;

procedure TAMQPServerConnection.ReleaseChannelResources(
  AChannel: TAMQPServerChannel);
var
  LNames: TArray<string>;
  LQueue: TAMQPServerQueue;
  I: Integer;
  LId: NativeUInt;
begin
  // DeliveryId and not Delivery.ChannelId: when this method runs in the
  // connection teardown, DetachAllDelivery has already released the target and
  // Delivery is nil -- that is exactly how the unacked messages of a dropped
  // connection stopped going back to the queue.
  if (FEngine <> nil) and (AChannel.DeliveryId <> 0) then
  begin
    LId := AChannel.DeliveryId;
    LNames := AChannel.TouchedQueues;
    for I := 0 to High(LNames) do
    begin
      LQueue := FEngine.FindQueue(FVirtualHost, LNames[I]);
      if LQueue <> nil then
        // Removes this channel's consumers and returns its unacked messages
        // to the queue -- the client never acked them.
        LQueue.PostRemoveChannel(LId);
    end;
    // WS7, AFTER posting all the removals: an auto-delete queue that lost its
    // last consumer with this channel goes away now. The Stats inside
    // MaybeAutoDeleteQueue is the barrier over the PostRemoveChannel above.
    for I := 0 to High(LNames) do
      FEngine.MaybeAutoDeleteQueue(FVirtualHost, LNames[I]);
  end;
  AChannel.DetachDelivery; // also detaches the confirm tracker
  // ... and it leaves the broker's registry, otherwise the journal thread would
  // keep walking the trackers of dead channels on every fsync.
  if (FConfig.Confirms <> nil) and (AChannel.Confirms <> nil) then
  begin
    FConfig.Confirms.Unregister(AChannel.Confirms);
    AChannel.Confirms := nil;
  end;
end;

// Basic.Return + content-header + body, in an indivisible batch: the client
// reassembles the returned message exactly as it published it.
procedure TAMQPServerConnection.SendBasicReturn(AChannel: TAMQPServerChannel;
  const AMessage: TAMQPServerMessage);
var
  LReturn: TAMQPBasicReturn;
  LMsg: TAMQPMessage;
  LContent, LAll: TArray<TAMQPFrame>;
  I: Integer;
begin
  LReturn.ReplyCode := AMQP_NO_ROUTE;
  LReturn.ReplyText := 'NO_ROUTE';
  LReturn.Exchange := AMessage.Exchange;
  LReturn.RoutingKey := AMessage.RoutingKey;

  // Only to reuse the content assembly (which reads the header's raw payload --
  // decision D1); this message goes to no queue at all.
  LMsg := TAMQPMessage.FromServerMessage(AMessage);
  try
    LContent := AmqpBuildContentFrames(AChannel.Id, LMsg, CurrentMaxPayload);
    SetLength(LAll, 1 + Length(LContent));
    LAll[0] := TAMQPFrame.Create(AMQP_FRAME_METHOD, AChannel.Id,
      BuildBasicReturn(LReturn));
    for I := 0 to High(LContent) do
      LAll[1 + I] := LContent[I];
    FWriter.PostFrames(LAll);
  finally
    LMsg.Release;
  end;
end;

// Basic.Get-Ok + content-header + body, in an indivisible batch.
procedure TAMQPServerConnection.SendGetOk(AChannel: TAMQPServerChannel;
  ADeliveryTag: UInt64; ARedelivered: Boolean; AMessageCount: Integer;
  AMessage: TAMQPMessage);
var
  LGetOk: TAMQPBasicGetOk;
  LContent, LAll: TArray<TAMQPFrame>;
  I: Integer;
begin
  LGetOk.DeliveryTag := ADeliveryTag;
  LGetOk.Redelivered := ARedelivered;
  LGetOk.Exchange := AMessage.Exchange;
  LGetOk.RoutingKey := AMessage.RoutingKey;
  LGetOk.MessageCount := Cardinal(AMessageCount);

  LContent := AmqpBuildContentFrames(AChannel.Id, AMessage,
    CurrentMaxPayload);
  SetLength(LAll, 1 + Length(LContent));
  LAll[0] := TAMQPFrame.Create(AMQP_FRAME_METHOD, AChannel.Id,
    BuildBasicGetOk(LGetOk));
  for I := 0 to High(LContent) do
    LAll[1 + I] := LContent[I];
  FWriter.PostFrames(LAll);
end;

function TAMQPServerConnection.DispatchExchange(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LDeclare: TAMQPExchangeDeclare;
  LDelete: TAMQPExchangeDelete;
  LBind: TAMQPExchangeBinding;
  LRes: TAMQPEngineResult;
  LAltName, LErrorArg: string;
  LAltHas: Boolean;
begin
  Result := True;
  case AId.MethodId of
    AMQP_EXCHANGE_DECLARE:
      begin
        LDeclare := DecodeExchangeDeclare(AReader);
        // The decoded argument table is OURS until the engine takes ownership
        // of it. The reserved-name checks raise BEFORE that, so without this
        // try/finally the table would leak -- which is what the DUnitX leak
        // report caught (FPCUnit does not report leaks).
        try
          if FEngine <> nil then
          begin
            if not LDeclare.Passive then
            begin
              // Reserved name or the default exchange: neither may be declared
              // by the client (spec 1.4). Passive escapes: querying
              // 'amq.direct' is legitimate and common.
              if LDeclare.ExchangeName = '' then
                CheckEngine(AChannel, amqerAccessRefused, 'default exchange',
                  AId.ClassId, AId.MethodId);
              CheckReservedName(AChannel, LDeclare.ExchangeName, 'exchange',
                AId.ClassId, AId.MethodId);
            end;
            // WS2 of Phase 3: the x-arguments are validated HERE, before the
            // engine takes ownership of the table -- validation produces a
            // reply-code, and the engine knows no reply-codes (Phase 2 invariant).
            // Passive does not validate: querying an existing exchange declares
            // nothing, and the client usually sends an empty table in that case.
            if not LDeclare.Passive then
              CheckArgs(AChannel, AmqpParseAlternateExchange(
                LDeclare.Arguments, LAltName, LAltHas, LErrorArg), LErrorArg,
                AId.ClassId, AId.MethodId);
            LRes := FEngine.DeclareExchange(FVirtualHost,
              LDeclare.ExchangeName, LDeclare.ExchangeType, LDeclare.Passive,
              LDeclare.Durable, LDeclare.AutoDelete, LDeclare.Internal,
              LDeclare.Arguments);
            LDeclare.Arguments := nil; // the engine took ownership (on ANY result)
            CheckEngine(AChannel, LRes, 'exchange ' + LDeclare.ExchangeName,
              AId.ClassId, AId.MethodId);
          end;
          if not LDeclare.NoWait then
            PostMethod(AChannel.Id, BuildExchangeDeclareOk);
        finally
          LDeclare.Arguments.Free; // only left over if nobody took ownership
        end;
      end;
    AMQP_EXCHANGE_DELETE:
      begin
        LDelete := DecodeExchangeDelete(AReader);
        if FEngine <> nil then
        begin
          CheckReservedName(AChannel, LDelete.ExchangeName, 'exchange',
            AId.ClassId, AId.MethodId);
          CheckEngine(AChannel,
            FEngine.DeleteExchange(FVirtualHost, LDelete.ExchangeName,
              LDelete.IfUnused),
            'exchange ' + LDelete.ExchangeName,
            AId.ClassId, AId.MethodId);
        end;
        if not LDelete.NoWait then
          PostMethod(AChannel.Id, BuildExchangeDeleteOk);
      end;
    AMQP_EXCHANGE_BIND:
      begin
        LBind := DecodeExchangeBind(AReader);
        try
          if FEngine <> nil then
          begin
            LRes := FEngine.BindExchange(FVirtualHost, LBind.Destination,
              LBind.Source, LBind.RoutingKey, LBind.Arguments);
            LBind.Arguments := nil;
            CheckEngine(AChannel, LRes, 'exchange ' + LBind.Source,
              AId.ClassId, AId.MethodId);
          end;
          if not LBind.NoWait then
            PostMethod(AChannel.Id, BuildExchangeBindOk);
        finally
          LBind.Arguments.Free;
        end;
      end;
    AMQP_EXCHANGE_UNBIND:
      begin
        LBind := DecodeExchangeUnbind(AReader);
        try
          if FEngine <> nil then
          begin
            LRes := FEngine.UnbindExchange(FVirtualHost, LBind.Destination,
              LBind.Source, LBind.RoutingKey, LBind.Arguments);
            LBind.Arguments := nil;
            CheckEngine(AChannel, LRes, 'exchange ' + LBind.Source,
              AId.ClassId, AId.MethodId);
            FEngine.MaybeAutoDeleteExchange(FVirtualHost, LBind.Source);
          end;
          if not LBind.NoWait then
            PostMethod(AChannel.Id, BuildExchangeUnbindOk);
        finally
          LBind.Arguments.Free;
        end;
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchQueue(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LDeclare: TAMQPQueueDeclare;
  LBind: TAMQPQueueBind;
  LUnbind: TAMQPQueueUnbind;
  LPurge: TAMQPQueuePurge;
  LDelete: TAMQPQueueDelete;
  LName: string;
  LMsgs, LCons: Integer;
  LRes: TAMQPEngineResult;
  LPolicy: TAMQPQueuePolicy;
  LErrorArg: string;
  LArgs: TAMQPFieldTable;
begin
  Result := True;
  case AId.MethodId of
    AMQP_QUEUE_DECLARE:
      begin
        LDeclare := DecodeQueueDeclare(AReader);
        LName := LDeclare.QueueName;
        LMsgs := 0;
        LCons := 0;
        // See the ownership comment in AMQP_EXCHANGE_DECLARE: the checks
        // below raise BEFORE the engine takes over the table.
        try
          if FEngine = nil then
          begin
            if LName = '' then
              LName := GeneratedName('amq.gen-');
          end
          else
          begin
            // The reserved name is checked BEFORE generating ours, which also
            // starts with 'amq.' and is legitimate precisely because it is ours.
            if (LName <> '') and (not LDeclare.Passive) then
              CheckReservedName(AChannel, LName, 'queue',
                AId.ClassId, AId.MethodId);
            if LName = '' then
            begin
              if LDeclare.Passive then
                // Passive without a name has nothing to check.
                CheckEngine(AChannel, amqerNotFound, 'queue',
                  AId.ClassId, AId.MethodId);
              LName := GeneratedName('amq.gen-');
            end;
            // WS2 of Phase 3 -- see the equivalent comment in Exchange.Declare.
            if not LDeclare.Passive then
              CheckArgs(AChannel, AmqpParseQueuePolicy(LDeclare.Arguments,
                LPolicy, LErrorArg), LErrorArg, AId.ClassId, AId.MethodId);
            // Ownership passes BEFORE the call: the engine hands the table to
            // the queue's descriptor and then still waits for the actor (Stats,
            // synchronous), which may raise on timeout. Clearing it only on return
            // made the finally below free the table the descriptor already owned --
            // a double free in the broker's Destroy, found in F8 with the actor pool saturated.
            LArgs := LDeclare.Arguments;
            LDeclare.Arguments := nil;
            LRes := FEngine.DeclareQueue(FVirtualHost, LName, LDeclare.Passive,
              LDeclare.Durable, LDeclare.Exclusive, LDeclare.AutoDelete,
              LArgs, FConnId, LMsgs, LCons);
            CheckEngine(AChannel, LRes, 'queue ' + LName,
              AId.ClassId, AId.MethodId);
            // The channel's "current" queue: the client may omit the name in
            // Basic.Consume/Get and in Queue.Bind (spec 1.7.2.1).
            AChannel.LastQueue := LName;
          end;
          if not LDeclare.NoWait then
            PostMethod(AChannel.Id, BuildQueueDeclareOk(LName, LMsgs, LCons));
        finally
          LDeclare.Arguments.Free;
        end;
      end;
    AMQP_QUEUE_BIND:
      begin
        LBind := DecodeQueueBind(AReader);
        try
          if FEngine <> nil then
          begin
            LName := LBind.QueueName;
            if LName = '' then
              LName := AChannel.LastQueue;
            LRes := FEngine.BindQueue(FVirtualHost, LName, LBind.ExchangeName,
              LBind.RoutingKey, LBind.Arguments, FConnId);
            LBind.Arguments := nil;
            CheckEngine(AChannel, LRes,
              'queue ' + LName + ' or exchange ' + LBind.ExchangeName,
              AId.ClassId, AId.MethodId);
          end;
          if not LBind.NoWait then
            PostMethod(AChannel.Id, BuildQueueBindOk);
        finally
          LBind.Arguments.Free;
        end;
      end;
    AMQP_QUEUE_UNBIND:
      begin
        // queue.unbind has no no-wait in 0-9-1: it always replies.
        LUnbind := DecodeQueueUnbind(AReader);
        try
          if FEngine <> nil then
          begin
            LName := LUnbind.QueueName;
            if LName = '' then
              LName := AChannel.LastQueue;
            LRes := FEngine.UnbindQueue(FVirtualHost, LName,
              LUnbind.ExchangeName, LUnbind.RoutingKey, LUnbind.Arguments,
              FConnId);
            LUnbind.Arguments := nil;
            CheckEngine(AChannel, LRes,
              'queue ' + LName + ' or exchange ' + LUnbind.ExchangeName,
              AId.ClassId, AId.MethodId);
            // WS7: it may have been the last binding of an auto-delete exchange.
            FEngine.MaybeAutoDeleteExchange(FVirtualHost,
              LUnbind.ExchangeName);
          end;
          PostMethod(AChannel.Id, BuildQueueUnbindOk);
        finally
          LUnbind.Arguments.Free;
        end;
      end;
    AMQP_QUEUE_PURGE:
      begin
        LPurge := DecodeQueuePurge(AReader);
        LMsgs := 0;
        if FEngine <> nil then
        begin
          LName := LPurge.QueueName;
          if LName = '' then
            LName := AChannel.LastQueue;
          CheckEngine(AChannel,
            FEngine.PurgeQueue(FVirtualHost, LName, FConnId, LMsgs),
            'queue ' + LName, AId.ClassId, AId.MethodId);
        end;
        if not LPurge.NoWait then
          PostMethod(AChannel.Id, BuildQueuePurgeOk(Cardinal(LMsgs)));
      end;
    AMQP_QUEUE_DELETE:
      begin
        LDelete := DecodeQueueDelete(AReader);
        LMsgs := 0;
        if FEngine <> nil then
        begin
          LName := LDelete.QueueName;
          if LName = '' then
            LName := AChannel.LastQueue;
          CheckEngine(AChannel,
            FEngine.DeleteQueue(FVirtualHost, LName, LDelete.IfUnused,
              LDelete.IfEmpty, FConnId, LMsgs),
            'queue ' + LName, AId.ClassId, AId.MethodId);
        end;
        if not LDelete.NoWait then
          PostMethod(AChannel.Id, BuildQueueDeleteOk(Cardinal(LMsgs)));
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchBasic(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LQos: TAMQPBasicQosArgs;
  LConsume: TAMQPBasicConsume;
  LCancel: TAMQPBasicCancelArgs;
  LPublish: TAMQPBasicPublish;
  LGet: TAMQPBasicGet;
  LAck: TAMQPBasicAck;
  LNack: TAMQPBasicNack;
  LReject: TAMQPBasicReject;
  LTag, LQueueName: string;
  LDeliveryTag: UInt64;
  LFound, LRedelivered: Boolean;
  LMsg: TAMQPMessage;
  LMsgs: Integer;
  LRefs: TArray<TAMQPOutstanding>;
  LQueue: TAMQPServerQueue;
  I: Integer;
  LEv: TAMQPServerEvent;
begin
  Result := True;
  case AId.MethodId of
    AMQP_BASIC_QOS:
      begin
        LQos := DecodeBasicQos(AReader);
        // prefetch-size (a limit in OCTETS) is not implemented -- RabbitMQ
        // does not implement it either. Silently ignoring it would be worse
        // than refusing: the client would think a byte limit is in force.
        if LQos.PrefetchSize <> 0 then
          raise EAMQPChannelError.Create(AChannel.Id, AMQP_NOT_IMPLEMENTED,
            'prefetch-size not implemented', AId.ClassId, AId.MethodId);
        AChannel.PrefetchCount := LQos.PrefetchCount;
        PostMethod(AChannel.Id, BuildBasicQosOk); // qos has no no-wait
      end;
    AMQP_BASIC_CONSUME:
      begin
        LConsume := DecodeBasicConsume(AReader);
        LConsume.Arguments.Free;
        LTag := LConsume.ConsumerTag;
        if LTag = '' then
          LTag := GeneratedName('amq.ctag-');
        if FEngine <> nil then
        begin
          // no-local (do not deliver to me what I published) would require
          // tagging the origin on every message; RabbitMQ does not implement it either.
          if LConsume.NoLocal then
            raise EAMQPChannelError.Create(AChannel.Id, AMQP_NOT_IMPLEMENTED,
              'no-local not implemented', AId.ClassId, AId.MethodId);
          // A repeated tag on the same channel is a CONNECTION error per the
          // spec (1.8.3.3, not-allowed), not a channel one.
          if AChannel.ConsumerQueue(LTag, LQueueName) then
            raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
              'consumer tag already used: ' + LTag, AId.ClassId, AId.MethodId);
          LQueueName := LConsume.Queue;
          if LQueueName = '' then
            LQueueName := AChannel.LastQueue;
          CheckEngine(AChannel,
            FEngine.AddConsumer(FVirtualHost, LQueueName, LTag, LConsume.NoAck,
              LConsume.Exclusive, AChannel.DeliveryTarget, FConnId),
            'queue ' + LQueueName, AId.ClassId, AId.MethodId);
          AChannel.AddConsumerTag(LTag, LQueueName);
          if WantsEvent(seConsumerRegistered) then
          begin
            LEv := NewEvent(seConsumerRegistered);
            LEv.ChannelNumber := AChannel.Id;
            LEv.QueueName := LQueueName;
            LEv.ConsumerTag := LTag;
            EmitEvent(LEv);
          end;
        end;
        if not LConsume.NoWait then
          PostMethod(AChannel.Id, BuildBasicConsumeOk(LTag));
      end;
    AMQP_BASIC_CANCEL:
      begin
        LCancel := DecodeBasicCancelArgs(AReader);
        if FEngine <> nil then
        begin
          // An unknown tag is not an error: the spec says to reply Cancel-Ok
          // anyway (the consumer may have gone away along with the queue).
          if AChannel.ConsumerQueue(LCancel.ConsumerTag, LQueueName) then
          begin
            FEngine.RemoveConsumer(FVirtualHost, LQueueName, LCancel.ConsumerTag,
              AChannel.Delivery.ChannelId);
            AChannel.RemoveConsumerTag(LCancel.ConsumerTag);
            // WS7: if the queue is auto-delete and this was the last consumer,
            // it goes away now.
            FEngine.MaybeAutoDeleteQueue(FVirtualHost, LQueueName);
            if WantsEvent(seConsumerCancelled) then
            begin
              LEv := NewEvent(seConsumerCancelled);
              LEv.ChannelNumber := AChannel.Id;
              LEv.QueueName := LQueueName;
              LEv.ConsumerTag := LCancel.ConsumerTag;
              EmitEvent(LEv);
            end;
          end;
        end;
        if not LCancel.NoWait then
          PostMethod(AChannel.Id, BuildBasicCancelOk(LCancel.ConsumerTag));
      end;
    AMQP_BASIC_PUBLISH:
      begin
        // No reply: what comes next is the content-header and the body. From
        // here on the channel is assembling and no other frame may appear on
        // it (see the interleave rule in HandleChannelFrame).
        LPublish := DecodeBasicPublish(AReader);
        if (FEngine <> nil) and (LPublish.Exchange <> '')
          and (not FEngine.ExchangeExists(FVirtualHost, LPublish.Exchange)) then
          raise EAMQPChannelError.Create(AChannel.Id, AMQP_NOT_FOUND,
            'no exchange ' + LPublish.Exchange, AId.ClassId, AId.MethodId);
        AChannel.BeginContent(LPublish);
      end;
    AMQP_BASIC_GET:
      begin
        LGet := DecodeBasicGet(AReader);
        if FEngine = nil then
        begin
          PostMethod(AChannel.Id, BuildBasicGetEmpty);
          Exit;
        end;
        LQueueName := LGet.Queue;
        if LQueueName = '' then
          LQueueName := AChannel.LastQueue;
        // The tag belongs to the CHANNEL, even on Get: it comes from the same sequence as delivery.
        LDeliveryTag := AChannel.Delivery.NextDeliveryTag;
        CheckEngine(AChannel,
          FEngine.GetMessage(FVirtualHost, LQueueName, LGet.NoAck, LDeliveryTag,
            AChannel.Delivery.ChannelId, FConnId, LFound, LMsg, LRedelivered,
            LMsgs),
          'queue ' + LQueueName, AId.ClassId, AId.MethodId);
        if not LFound then
          PostMethod(AChannel.Id, BuildBasicGetEmpty)
        else
          try
            if not LGet.NoAck then
              // Recorded on the TARGET, like consumer deliveries -- but without
              // counting toward the prefetch (the spec limits consumer delivery).
              AChannel.Delivery.NoteGet(LDeliveryTag, LQueueName);
            SendGetOk(AChannel, LDeliveryTag, LRedelivered, LMsgs, LMsg);
          finally
            LMsg.Release; // the reference the Get handed to us
          end;
      end;
    // Ack/Nack/Reject reply nothing. Two things happen here: the prefetch is
    // released on the target (delivery increments it; only this thread sees the
    // client's method) and it is forwarded to the QUEUE that holds the unacked
    // message -- the tag belongs to the channel, but the message to the queue.
    AMQP_BASIC_ACK:
      begin
        LAck := DecodeBasicAck(AReader);
        if AChannel.Delivery = nil then
          Exit;
        // NoteResolved does both things at once: releases the prefetch and
        // returns the resolved deliveries WITH each one's source queue.
        LRefs := AChannel.Delivery.NoteResolved(LAck.DeliveryTag,
          LAck.Multiple);
        if WantsEvent(seMessageAcked) then
        begin
          LEv := NewEvent(seMessageAcked);
          LEv.ChannelNumber := AChannel.Id;
          LEv.DeliveryTag := LAck.DeliveryTag;
          LEv.Multiple := LAck.Multiple;
          // How many deliveries the ack actually resolved -- with multiple=true
          // a single frame may close dozens, and the number is what matters.
          LEv.Count := Length(LRefs);
          EmitEvent(LEv);
        end;
        if FEngine <> nil then
          for I := 0 to High(LRefs) do
          begin
            LQueue := FEngine.FindQueue(FVirtualHost, LRefs[I].Queue);
            if LQueue <> nil then
              LQueue.PostAck(AChannel.DeliveryId, LRefs[I].Tag, False);
          end;
      end;
    AMQP_BASIC_NACK:
      begin
        LNack := DecodeBasicNack(AReader);
        if AChannel.Delivery = nil then
          Exit;
        LRefs := AChannel.Delivery.NoteResolved(LNack.DeliveryTag,
          LNack.Multiple);
        if WantsEvent(seMessageNacked) then
        begin
          LEv := NewEvent(seMessageNacked);
          LEv.ChannelNumber := AChannel.Id;
          LEv.DeliveryTag := LNack.DeliveryTag;
          LEv.Multiple := LNack.Multiple;
          LEv.Requeue := LNack.Requeue;
          LEv.Count := Length(LRefs);
          EmitEvent(LEv);
        end;
        if FEngine <> nil then
          for I := 0 to High(LRefs) do
          begin
            LQueue := FEngine.FindQueue(FVirtualHost, LRefs[I].Queue);
            if LQueue <> nil then
              LQueue.PostNack(AChannel.DeliveryId, LRefs[I].Tag, False,
                LNack.Requeue);
          end;
      end;
    AMQP_BASIC_REJECT:
      begin
        LReject := DecodeBasicReject(AReader); // Reject has no multiple
        if AChannel.Delivery = nil then
          Exit;
        LRefs := AChannel.Delivery.NoteResolved(LReject.DeliveryTag, False);
        if WantsEvent(seMessageRejected) then
        begin
          LEv := NewEvent(seMessageRejected);
          LEv.ChannelNumber := AChannel.Id;
          LEv.DeliveryTag := LReject.DeliveryTag;
          LEv.Requeue := LReject.Requeue;
          LEv.Count := Length(LRefs);
          EmitEvent(LEv);
        end;
        if FEngine <> nil then
          for I := 0 to High(LRefs) do
          begin
            LQueue := FEngine.FindQueue(FVirtualHost, LRefs[I].Queue);
            if LQueue <> nil then
              LQueue.PostReject(AChannel.DeliveryId, LRefs[I].Tag,
                LReject.Requeue);
          end;
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchConfirm(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LNoWait: Boolean;
begin
  Result := True;
  case AId.MethodId of
    AMQP_CONFIRM_SELECT:
      begin
        LNoWait := DecodeConfirmSelect(AReader);
        AChannel.ConfirmMode := True;
        // WS5 of Phase 4: with durability on, this channel's confirm is now
        // deferred until the watermark. Without DataDir the registry is nil and
        // nothing changes -- the confirm goes out inline, exactly as in Phase 3 (D19).
        if (FConfig.Confirms <> nil) and (AChannel.Confirms = nil) then
          AChannel.Confirms := FConfig.Confirms.CreateTracker(FWriter,
            AChannel.Id);
        if not LNoWait then
          PostMethod(AChannel.Id, BuildConfirmSelectOk);
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchChannelMethod(
  AChannel: TAMQPServerChannel; const AId: TAMQPMethodId;
  const AReader: TAMQPReader): Boolean;
begin
  // Phase 1 ("null broker"): decodes everything the client may send and
  // replies the correct *-Ok, with no state behind it. The Tx class (90) is
  // left out on purpose -- it is not in the codec and does not enter Phase 1
  // (see the decision not to implement transactions in CLAUDE.md) --, so it
  // falls into False and the channel closes with NOT_IMPLEMENTED.
  case AId.ClassId of
    AMQP_CLASS_EXCHANGE: Result := DispatchExchange(AChannel, AId, AReader);
    AMQP_CLASS_QUEUE:    Result := DispatchQueue(AChannel, AId, AReader);
    AMQP_CLASS_BASIC:    Result := DispatchBasic(AChannel, AId, AReader);
    AMQP_CLASS_CONFIRM:  Result := DispatchConfirm(AChannel, AId, AReader);
  else
    Result := False;
  end;
end;

{ --- content assembly (spec 2.3.5) ---------------------------------------- }

// Complete content: hands it to the sink and clears the channel. The sink does
// NOT own the message -- the ResetContent in the finally frees the headers table.
procedure TAMQPServerConnection.CompleteContent(AChannel: TAMQPServerChannel);
var
  LMsg: TAMQPServerMessage;
  LSeq: UInt64;
  LRouted: Boolean;
  LRejected: Boolean;
  LTtlMs: Int64;
  LLsn: UInt64;
  LEv: TAMQPServerEvent;
begin
  try
    LMsg := AChannel.CurrentMessage(FUserId);
    // The user-id property, when present, must match the authenticated user --
    // otherwise a client could sign a message on behalf of another.
    if LMsg.Properties.Has(bpUserId) and (LMsg.Properties.UserId <> FUserId) then
      raise EAMQPChannelError.Create(AChannel.Id, AMQP_ACCESS_REFUSED,
        'user-id property does not match authenticated user',
        AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH);

    // WS4 of Phase 3: 'expiration' is a TTL in ms as a decimal STRING (that is
    // how the spec defines it). A malformed one is refused with 406, not
    // ignored: a message the client asked to expire that lived forever would be
    // a silent error, and the client would have no way to find out. Same line
    // as the 406 for an invalid x-argument in WS2.
    if LMsg.Properties.Has(bpExpiration) then
      if not AmqpParseExpirationMs(LMsg.Properties.Expiration, LTtlMs) then
        raise EAMQPChannelError.Create(AChannel.Id, AMQP_PRECONDITION_FAILED,
          Format('invalid expiration ''%s'': must be a non-negative integer '
            + 'number of milliseconds, as a decimal string',
            [LMsg.Properties.Expiration]),
          AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH);

    // WS6: the seq-no is consumed by EVERY publish in confirm mode -- routed
    // or not. That is how the client correlates the ack with the publish it
    // numbered itself; skipping numbers would break its WaitForConfirms.
    LSeq := 0;
    if AChannel.ConfirmMode then
      LSeq := AChannel.NextPublishSeq;

    LRouted := False;
    LRejected := False;
    LLsn := 0;
    try
      if FConfig.Sink <> nil then
        LRouted := FConfig.Sink.RouteMessage(FVirtualHost, LMsg, LRejected,
          LLsn);
    except
      // Basic.Nack is reserved for an INTERNAL FAILURE of the broker (extension
      // spec: "the broker could not handle the message"). That is not the case
      // of an unroutable message, which is a success and gets an ack.
      on E: Exception do
      begin
        if AChannel.ConfirmMode then
          ConfirmPublish(AChannel, LSeq, 0, True);
        raise;
      end;
    end;

    // The event goes out AFTER routing (only then is it known whether it routed
    // and which LSN came out) and BEFORE the Return/Ack, keeping the observed
    // order the same as the frame order. A publish rejected by x-overflow and
    // an unroutable publish are different things: the first is
    // seMessagePublishRejected, the second is a successful publish that found no queue.
    if LRejected then
    begin
      if WantsEvent(seMessagePublishRejected) then
      begin
        LEv := NewEvent(seMessagePublishRejected);
        LEv.ChannelNumber := AChannel.Id;
        LEv.ExchangeName := LMsg.Exchange;
        LEv.RoutingKey := LMsg.RoutingKey;
        LEv.MessageSize := UInt64(Length(LMsg.Body));
        LEv.Mandatory := LMsg.Mandatory;
        LEv.Reason := 'reject-publish';
        EmitEvent(LEv);
      end;
    end
    else if WantsEvent(seMessagePublished) then
    begin
      LEv := NewEvent(seMessagePublished);
      LEv.ChannelNumber := AChannel.Id;
      LEv.ExchangeName := LMsg.Exchange;
      LEv.RoutingKey := LMsg.RoutingKey;
      LEv.MessageSize := UInt64(Length(LMsg.Body));
      LEv.Mandatory := LMsg.Mandatory;
      LEv.Lsn := LLsn;
      if not LRouted then
        // Unroutable is NOT an error (it gets an ack, and a Return if
        // mandatory). The reason goes in Reason so an auditor need not deduce it.
        LEv.Reason := 'unroutable';
      EmitEvent(LEv);
    end;

    // ORDER: the Return goes out BEFORE the Ack. The client may release the
    // publish's state as soon as the ack arrives (this lib's WaitForConfirms
    // does that); if the ack came first, the return would arrive for a publish
    // the client already considers resolved.
    if (not LRouted) and LMsg.Mandatory then
      SendBasicReturn(AChannel, LMsg);

    if AChannel.ConfirmMode then
      // WS6: a full queue declared with x-overflow reject-publish is the ONLY
      // case, besides an internal failure, in which a publish gets a Nack
      // instead of an Ack. An unroutable message is still a success.
      //
      // If the message was going to several queues and only one rejected it,
      // the publish is nacked anyway -- and the others HAVE ALREADY RECEIVED it.
      // That is RabbitMQ's behavior, and the alternative (undoing what already
      // went in) does not exist without transactions. The publisher has to treat
      // the nack as "may have been partially delivered", which is the confirms
      // contract anyway.
      //
      // WS5 of Phase 4: whoever decides whether the frame goes out NOW or is
      // held to the watermark is ConfirmPublish. A nack does not wait for the
      // disk (it promises no durability at all), but still respects seq order.
      if LRejected then
        ConfirmPublish(AChannel, LSeq, 0, True)
      else
        ConfirmPublish(AChannel, LSeq, LLsn, False);
  finally
    AChannel.ResetContent;
  end;
end;

// The confirm of ONE publish. Without durability (or with nothing to wait for
// and no pending one ahead) the frame goes out right here, as in Phase 3;
// otherwise it stays with the channel's tracker until the LSN is on disk --
// and goes out from there collapsed with its neighbors into a single multiple=true (D24/D9).
procedure TAMQPServerConnection.ConfirmPublish(AChannel: TAMQPServerChannel;
  ASeq, ALsn: UInt64; ANack: Boolean);
begin
  if (AChannel.Confirms <> nil)
    and AChannel.Confirms.TryDefer(ASeq, ALsn, ANack) then
    Exit;
  if ANack then
    PostMethod(AChannel.Id, BuildBasicNack(ASeq, False, False))
  else
    PostMethod(AChannel.Id, BuildBasicAck(ASeq, False));
end;

function TAMQPServerConnection.DispatchContent(AChannel: TAMQPServerChannel;
  const AFrame: TAMQPFrame): Boolean;
var
  LReader: TAMQPReader;
  LHeader: TAMQPContentHeader;
  LClassId: Word;
begin
  if AFrame.FrameType = AMQP_FRAME_HEADER then
  begin
    if AChannel.AsmState <> amqasHeader then
      Exit(False); // content-header without a Basic.Publish before -> 505

    // The content-header's class-id must match the class of the method that
    // requested it (spec 2.3.5.2). Read directly from the payload because
    // DecodeContentHeader consumes and discards that field.
    if Length(AFrame.Payload) < 2 then
      raise EAMQPConnectionError.Create(AMQP_FRAME_ERROR,
        'Truncated content-header', 0, 0);
    LClassId := (Word(AFrame.Payload[0]) shl 8) or Word(AFrame.Payload[1]);
    if LClassId <> AMQP_CLASS_BASIC then
      raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
        Format('content-header for class %d after Basic.Publish', [LClassId]),
        0, 0);

    LReader := TAMQPReader.Create(AFrame.Payload);
    try
      LHeader := DecodeContentHeader(LReader);
    finally
      LReader.Free;
    end;

    if AChannel.SetContentHeader(LHeader, AFrame.Payload) then
      CompleteContent(AChannel); // body-size 0: message without a body
    Exit(True);
  end;

  if AFrame.FrameType = AMQP_FRAME_BODY then
  begin
    if AChannel.AsmState <> amqasBody then
      Exit(False); // body without a header before -> 505
    if AChannel.AppendBody(AFrame.Payload) then
      CompleteContent(AChannel);
    Exit(True);
  end;

  Result := False;
end;

{ --- read loop ------------------------------------------------------------ }

// Sets up the transport on THIS connection's thread: under TLS, the handshake
// is synchronous and may take a while (or never finish, if the peer vanishes).
// Only after it is the writer born, because everything it writes must go out encrypted.
{ --- observability (Phase 4.1) -------------------------------------------- }

function TAMQPServerConnection.WantsEvent(
  AType: TAMQPServerEventType): Boolean;
begin
  // Without a sink (or without a subscriber for the type) the emit site does not
  // even build the record -- that is what makes D29 hold: who does not observe does not pay.
  Result := (FConfig.Events <> nil) and FConfig.Events.Wants(AType);
end;

function TAMQPServerConnection.NewEvent(
  AType: TAMQPServerEventType): TAMQPServerEvent;
begin
  Result := AmqpNewEvent(AType);
  Result.ConnectionId := UInt64(FConnId);
  Result.RemoteAddr := FPeer;
  Result.Username := FUserId;
  Result.VHost := FVirtualHost;
end;

procedure TAMQPServerConnection.EmitEvent(const AEvent: TAMQPServerEvent);
begin
  if FConfig.Events <> nil then
    FConfig.Events.Emit(AEvent);
end;

procedure TAMQPServerConnection.EmitSimpleEvent(
  AType: TAMQPServerEventType; AChannel: Word);
var
  LEv: TAMQPServerEvent;
begin
  if not WantsEvent(AType) then
    Exit;
  LEv := NewEvent(AType);
  LEv.ChannelNumber := AChannel;
  EmitEvent(LEv);
end;

procedure TAMQPServerConnection.SetupTransport;
begin
  // Server-side TLS negotiates here, on this connection's thread, with the
  // common's handshake deadline (15 s); plain TCP has nothing to do. A
  // client speaking plain AMQP to a TLS port, a refused client certificate
  // (mTLS) or a peer that never talks end here as EAMQPTls.
  try
    FEndpoint.Handshake;
  except
    on E: EPcTransport do
      AmqpRaiseTransportError(E);
  end;
  FWriter := TAMQPFrameWriter.Create(FStream);
end;

function TAMQPServerConnection.ReadProtocolHeader: Boolean;
var
  LBuf: array[0..7] of Byte;
  LTotal, LRead, I: Integer;
begin
  FillChar(LBuf, SizeOf(LBuf), 0);
  LTotal := 0;
  while LTotal < 8 do
  begin
    LRead := FStream.Read(LBuf[LTotal], 8 - LTotal);
    if LRead <= 0 then
      Exit(False);
    Inc(LTotal, LRead);
  end;
  for I := 0 to 7 do
    if LBuf[I] <> AMQP_PROTOCOL_HEADER[I] then
    begin
      // spec 4.2.2: sends back the supported header and ends. It goes through
      // FStream (and not straight to the socket) so it goes out ENCRYPTED when
      // the connection is TLS; nothing was posted to the writer yet, so there is no race with it.
      try
        FStream.Write(AMQP_PROTOCOL_HEADER[0], Length(AMQP_PROTOCOL_HEADER));
      except
      end;
      FError := 'invalid protocol-header';
      Exit(False);
    end;
  Result := True;
end;

function TAMQPServerConnection.CurrentMaxPayload: Cardinal;
var
  LMax: Cardinal;
begin
  if FState in [amqssHeader, amqssAwaitStartOk, amqssAwaitTuneOk] then
    LMax := AMQP_SERVER_FRAME_MAX_BOOT
  else
    LMax := FNegotiated.FrameMax;
  if LMax = 0 then
    Exit(0); // no negotiated limit
  if LMax <= AMQP_FRAME_OVERHEAD then
    Result := 1
  else
    Result := LMax - AMQP_FRAME_OVERHEAD;
end;

procedure TAMQPServerConnection.RunReadLoop;
var
  LFrame: TAMQPFrame;
  LClosedEv: TAMQPServerEvent;
begin
  try
    try
      // BEFORE SetupTransport: a TLS handshake that fails also produces the
      // established/closed pair, and that is exactly the case one wants to observe.
      EmitSimpleEvent(seConnectionEstablished, 0);

      SetupTransport; // TLS handshake here, if applicable

      FProtocolOk := ReadProtocolHeader;
      if not FProtocolOk then
        Exit;

      PcAtomicWrite64(FLastReadTick, PcTickMs);
      SendConnectionStart;
      FState := amqssAwaitStartOk;

      while FState <> amqssClosed do
      begin
        try
          LFrame := TAMQPFrame.ReadFrom(FStream, CurrentMaxPayload);
        except
          on E: EAMQPFrameEOF do
            Break; // peer disconnected: normal shutdown
          on E: EAMQPFrame do
          begin
            // Malformed frame / above frame-max: the stream got out of sync,
            // so we notify and exit without waiting for a Close-Ok.
            if FState <> amqssClosing then
              try
                SendConnectionClose(AMQP_FRAME_ERROR, E.Message, 0, 0);
              except
              end;
            Break;
          end;
        end;

        PcAtomicInc(FFramesRead);
        PcAtomicWrite64(FLastReadTick, PcTickMs);

        try
          if FState = amqssClosing then
          begin
            // Waiting for the Close-Ok: only it matters (spec 4.2.7).
            if LFrame.IsMethod and (Length(LFrame.Payload) >= 4) then
              HandleMethodFrame(LFrame);
          end
          else
            HandleFrame(LFrame);
        except
          on E: EAMQPChannelError do
            SendChannelClose(E.Channel, E.Code, E.Message, E.ClassId, E.MethodId);
          on E: EAMQPConnectionError do
            if FState <> amqssClosing then
            begin
              SendConnectionClose(E.Code, E.Message, E.ClassId, E.MethodId);
              FState := amqssClosing;
            end;
          on E: EAMQPServerAbort do
          begin
            if FError = '' then
              FError := E.Message;
            FState := amqssClosed;
          end;
          on E: EAMQPTransport do
          begin
            // The writer died: there is no way to talk to the client.
            if FError = '' then
              FError := E.Message;
            FState := amqssClosed;
          end;
        end;
      end;
    except
      on E: EAMQPFrame do
        ; // end of stream outside the loop: normal
      on E: EAMQPTls do
        if FError = '' then
          FError := 'TLS: ' + E.Message;
      on E: EAMQPTransport do
        if FError = '' then
          FError := E.Message;
      on E: Exception do
        if FError = '' then
          FError := E.ClassName + ': ' + E.Message;
    end;
  finally
    FState := amqssClosed;
    // Order matters: Stop drains the queue (the Close-Ok we just posted, for
    // example) and only then do we drop the TCP connection. Closing here -- and
    // not only in the destructor -- is what the spec says after the Close-Ok,
    // and it is what makes the peer leave its recv: a client that closes cleanly
    // hangs if the server's socket stays open until the reaper comes by.
    // Before that, the Detach: this is the NORMAL closing path (the peer sent
    // Connection.Close), and a queue may be delivering at this very instant on
    // a pool thread.
    DetachAllDelivery;
    if FWriter <> nil then
      FWriter.Stop;
    if FEndpoint <> nil then
      try
        FEndpoint.CloseAbort;
      except
      end;
    ClearChannels;
    // WS7: an exclusive queue belongs to ONE connection and dies with it (spec
    // 1.7.2.1). After ClearChannels, which has already released consumers and
    // returned the unacked messages.
    if FEngine <> nil then
      try
        FEngine.DeleteExclusiveQueuesOf(FVirtualHost, FConnId);
      except
        on E: Exception do
          ; // teardown must not fail because of this
      end;
    // After the teardown and BEFORE OnClosed: the broker moves the connection
    // to the reap list in there, and the event must go out while this
    // connection's fields are still valid.
    if WantsEvent(seConnectionClosed) then
    begin
      LClosedEv := NewEvent(seConnectionClosed);
      LClosedEv.Reason := FError;
      EmitEvent(LClosedEv);
    end;
    if Assigned(FOnClosed) then
      FOnClosed(Self);
  end;
end;

end.
