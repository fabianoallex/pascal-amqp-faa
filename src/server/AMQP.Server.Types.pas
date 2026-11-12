unit AMQP.Server.Types;

{$I amqp.inc}

{ Shared types of the broker sub-module (WS4).

  Lives below AMQP.Server.Connection and AMQP.Server.Broker so both can see
  the same pieces without a unit cycle:

  - TAMQPVirtualHostRegistry — the vhosts the broker accepts on Connection.Open
    (it was in AMQP.Server.Broker in WS6; the handshake FSM needs to consult it,
    and the broker cannot be used by the connection, which it uses itself).
  - TAMQPServerConnConfig — what the broker hands each accepted connection
    (authenticator, authorizer, vhosts and the limits proposed in Tune).
  - The exceptions the FSM uses as flow control for the AMQP error model:
    EAMQPChannelError (closes only the channel), EAMQPConnectionError (closes
    the connection with Connection.Close) and EAMQPServerAbort (drops the
    socket without Close). }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  PascalCommon.Threading,
  AMQP.Basic.Methods, // TAMQPBasicProperties
  AMQP.Server.Events, // IAMQPEventSink (TAMQPServerConnConfig.Events)
  AMQP.Server.Auth,
  AMQP.Server.FrameIO; // TAMQPFrameWriter (the confirm tracker writes to
                       // the publisher's channel)

const
  /// Limits the broker proposes in Connection.Tune. Same as RabbitMQ's, so
  /// the client defaults (TAMQPConnectionParams.Localhost) match without
  /// negotiating down.
  AMQP_SERVER_DEFAULT_CHANNEL_MAX = 2047;
  AMQP_SERVER_DEFAULT_FRAME_MAX   = 131072;
  AMQP_SERVER_DEFAULT_HEARTBEAT   = 60;

  /// How long we wait for Connection.Close-Ok after sending a
  /// Connection.Close before simply dropping the socket. Enforced by the
  /// broker's monitor thread via TAMQPServerConnection.EnforceCloseDeadline.
  AMQP_SERVER_CLOSE_TIMEOUT_MS = 10000;

  /// Period of the broker's monitor thread (heartbeats, dead-peer detection,
  /// Close-Ok deadline and reaping of dead connections). It must be well below
  /// half of the smallest useful heartbeat — at 200 ms, a negotiated heartbeat
  /// of 1 s (used in the tests) is still served with room to spare.
  AMQP_SERVER_MONITOR_TICK_MS = 200;

type
  { Set of virtual hosts the broker accepts on Connection.Open.
    Phase 1: only existence matters (no resource isolation, no
    permissions — see the deviations in CLAUDE.md). Default: only the root "/".
    Thread-safe (several connections handshake in parallel). }
  TAMQPVirtualHostRegistry = class
  private
    FLock: TCriticalSection;
    FNames: TStringList; // CaseSensitive, Sorted, Duplicates ignored
  public
    constructor Create;
    destructor Destroy; override;
    procedure Add(const AName: string);
    procedure Remove(const AName: string);
    function Contains(const AName: string): Boolean;
    function ToArray: TArray<string>;
  end;

  { A published message, with its content already reassembled (Basic.Publish
    method + content-header + N body frames). }
  TAMQPServerMessage = record
    Exchange: string;
    RoutingKey: string;
    Mandatory: Boolean;   // no route => return it with Basic.Return (Phase 2)
    Immediate: Boolean;   // obsolete in RabbitMQ; decoded and ignored
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    /// Authenticated user of the publishing connection (to validate the
    /// user-id property and for auditing in Phase 2).
    UserId: string;
    /// RAW payload of the content-header (class-id/weight/body-size/flags and
    /// the properties present), exactly as it came in the frame — decision D1
    /// of Phase 2 (CLAUDE.md): faithful re-emission to the consumer without
    /// re-decoding and without cloning TAMQPFieldTable (none of FPC's
    /// TValue-in-TValue). Filled in by TAMQPServerChannel from the bytes the
    /// connection read in the header frame (see AMQP.Server.Channel.SetContentHeader).
    HeaderPayload: TBytes;
  end;

  { Destination of a published message. Phase 1 uses TAMQPNullMessageSink
    (which drops); Phase 2's routing engine plugs in here without changing
    any caller's signature.

    IMPORTANT: the message and the Headers table inside Properties belong to
    the CHANNEL, which frees them as soon as RouteMessage returns. An
    implementation that needs to keep the message must copy what it needs. }
  IAMQPMessageSink = interface
    ['{2A0F6E31-1C5D-4E8B-9F27-6D3B0A5C4E19}']
    /// True if the message was routed to at least one queue. False with
    /// Mandatory=True is what triggers Basic.Return in Phase 2.
    ///
    /// ARejected (Phase 3 WS6) comes out True when at least one destination
    /// queue was full and declared with `x-overflow: reject-publish` — in that
    /// case the publish gets `Basic.Nack` instead of `Basic.Ack`. It is a
    /// SEPARATE result from "not routed": a message may have been accepted by
    /// two queues and rejected by the third, and the publisher needs to know.
    ///
    /// ALsn (Phase 4 WS5) comes out with the HIGHEST LSN this publish wrote to
    /// the journal, or 0 when it wrote nothing (transient message, transient
    /// queue, or broker without DataDir). It is the number this seq-no's
    /// confirm must wait to become durable -- D24 in one line: whoever routed
    /// is who knows which LSN ties to which seq.
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage;
      out ARejected: Boolean; out ALsn: UInt64): Boolean;
  end;

  { Tracker of ONE channel's deferred confirms -- Phase 4 WS5, D24.

    While the broker has no DataDir, nobody creates one of these and the
    confirm path is, letter for letter, Phase 3's. With durability on, the
    persistent publish does NOT get an ack right away: it waits here, tied to
    the LSN that must be on disk, until the journal thread advances the
    watermark.

    Same lifecycle pattern as TAMQPChannelDeliveryTarget, and for the same
    reason: the one releasing the pending confirm is the JOURNAL thread, and
    the channel may have died meanwhile. The refcount keeps the object alive,
    and Detach releases the writer under the lock without waiting for I/O. }
  IAMQPConfirmTracker = interface
    ['{7C4E1B85-93A0-4D62-B1F7-0E58C2A46D31}']
    /// Records ASeq's confirm as DEFERRED and returns True; returns False when
    /// there is nothing to wait for and the caller can emit right away.
    ///
    /// Defers when ALsn > 0 (this publish wrote to disk) OR when there is
    /// already something pending on this channel -- corollary (a) of D24:
    /// confirms go out in seq order PER CHANNEL, so a transient publish behind
    /// a pending persistent one waits and goes out collapsed in the same
    /// multiple=true.
    function TryDefer(ASeq, ALsn: UInt64; ANack: Boolean): Boolean;
    /// Releases every pending confirm with Lsn <= AMarca, collapsed per channel
    /// into a single Basic.Ack(highestSeq, multiple=true). Never blocks; what
    /// the write queue refuses waits for the next round (the shape of D3).
    procedure Release(AMarca: UInt64);
    /// The journal failed for good: every pending confirm becomes Basic.Nack,
    /// because the durability promise can no longer be kept.
    procedure FailAll;
    /// The channel died: releases the writer and drops what is pending.
    procedure Detach;
    /// How many confirms are held here (diagnostics and tests).
    function PendingCount: Integer;
  end;

  { Where all the broker's trackers live. It is the IAMQPDurabilitySink the
    journal thread calls after each fsync -- the only piece that knows that
    "the watermark moved" must become "these channels may confirm". }
  IAMQPConfirmRegistry = interface
    ['{1F60A2D9-5E37-4C84-A0B6-83D915E7C42A}']
    /// Creates and registers the channel's tracker. The caller keeps the
    /// interface and calls Unregister when the channel dies.
    function CreateTracker(AWriter: TAMQPFrameWriter;
      AChannelNo: Word): IAMQPConfirmTracker;
    procedure Unregister(const ATracker: IAMQPConfirmTracker);
    /// Everything with LSN <= this is already on disk.
    function Watermark: UInt64;
    /// How many channels have a tracker registered right now. Diagnostics --
    /// and the only way for an end-to-end test to see that the channel REALLY
    /// started deferring the confirm, instead of emitting it inline as in Phase 3.
    function TrackerCount: Integer;
    /// Recovers what got stuck because the write queue refused it. Called by
    /// the monitor thread: without it, a channel whose output was full at the
    /// moment of the fsync would only confirm on the next watermark advance --
    /// which may never come, if nobody publishes again.
    procedure RetryPending;
  end;

  { Drops everything (Phase 1's "null" broker). Counts what went through, for
    the tests and for diagnostics. }
  TAMQPNullMessageSink = class(TInterfacedObject, IAMQPMessageSink)
  private
    FCount: Integer; // atomic
  public
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage;
      out ARejected: Boolean; out ALsn: UInt64): Boolean;
    /// How many messages were dropped since creation.
    function Count: Integer;
  end;

  { Channel error (spec 1.4.2.2): the broker sends Channel.Close on the
    offending channel, the channel dies and the connection stays alive. }
  EAMQPChannelError = class(Exception)
  private
    FChannel: Word;
    FCode: Word;
    FClassId: Word;
    FMethodId: Word;
  public
    constructor Create(AChannel, ACode: Word; const AText: string;
      AClassId: Word = 0; AMethodId: Word = 0); reintroduce;
    /// Channel where the error happened (never 0).
    property Channel: Word read FChannel;
    /// reply-code (see AMQP.Protocol: 403/404/405/406/311...).
    property Code: Word read FCode;
    /// Method that caused the error (0/0 when not applicable).
    property ClassId: Word read FClassId;
    property MethodId: Word read FMethodId;
  end;

  { Connection error (spec 1.4.2.3): the broker sends Connection.Close on
    channel 0, waits for Close-Ok and shuts down. }
  EAMQPConnectionError = class(Exception)
  private
    FCode: Word;
    FClassId: Word;
    FMethodId: Word;
  public
    constructor Create(ACode: Word; const AText: string;
      AClassId: Word = 0; AMethodId: Word = 0); reintroduce;
    property Code: Word read FCode;
    property ClassId: Word read FClassId;
    property MethodId: Word read FMethodId;
  end;

  { Ends the connection WITHOUT Connection.Close: either the transport is
    already dead, or the spec says to close the socket directly (auth denied
    to a client that did not announce the 'authentication_failure_close'
    capability). }
  EAMQPServerAbort = class(Exception);

  { What the broker hands each accepted connection. The connection owns
    NOTHING here (the broker outlives it). }
  TAMQPServerConnConfig = record
    Auth: IAMQPAuthenticator;
    Authorizer: IAMQPAuthorizer;
    VHosts: TAMQPVirtualHostRegistry;
    /// Limits proposed in Connection.Tune (0 = no limit).
    ChannelMax: Word;
    FrameMax: Cardinal;
    Heartbeat: Word;
    /// True => the connection does a TLS handshake before any AMQP byte.
    /// Also passed on to the authenticator (which may require an encrypted channel).
    /// The certificate lives in the broker's listener since 0.6.0 (loaded
    /// once at Start); the connection only runs the handshake.
    Tls: Boolean;
    /// How long to wait for Connection.Close-Ok before dropping the socket.
    CloseTimeoutMs: Cardinal;
    /// Where published messages go. Never nil on the connections the broker
    /// creates (it seeds them with TAMQPNullMessageSink).
    Sink: IAMQPMessageSink;
    /// Where the channels register deferred confirms. nil = no durability, and
    /// the confirm goes out inline as in Phase 3 (D19: the broker without
    /// DataDir is the Phase 3 broker, here too).
    Confirms: IAMQPConfirmRegistry;
    /// Where the connection emits the observability events. nil = off, and
    /// then the hot path pays nothing (D29). Never nil on the connections the
    /// broker creates: it seeds them with its own TAMQPEventBus.
    Events: IAMQPEventSink;
    /// Config without authenticator/vhosts (the broker fills in those two).
    class function Defaults: TAMQPServerConnConfig; static;
  end;

implementation

{ TAMQPVirtualHostRegistry }

constructor TAMQPVirtualHostRegistry.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FNames := TStringList.Create;
  FNames.CaseSensitive := True;
  FNames.Sorted := True;
  FNames.Duplicates := dupIgnore;
  FNames.Add('/');
end;

destructor TAMQPVirtualHostRegistry.Destroy;
begin
  FNames.Free;
  FLock.Free;
  inherited;
end;

procedure TAMQPVirtualHostRegistry.Add(const AName: string);
begin
  FLock.Enter;
  try
    FNames.Add(AName);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPVirtualHostRegistry.Remove(const AName: string);
var
  I: Integer;
begin
  FLock.Enter;
  try
    I := FNames.IndexOf(AName);
    if I >= 0 then
      FNames.Delete(I);
  finally
    FLock.Leave;
  end;
end;

function TAMQPVirtualHostRegistry.Contains(const AName: string): Boolean;
begin
  FLock.Enter;
  try
    Result := FNames.IndexOf(AName) >= 0;
  finally
    FLock.Leave;
  end;
end;

function TAMQPVirtualHostRegistry.ToArray: TArray<string>;
var
  I: Integer;
begin
  Result := nil;
  FLock.Enter;
  try
    SetLength(Result, FNames.Count);
    for I := 0 to FNames.Count - 1 do
      Result[I] := FNames[I];
  finally
    FLock.Leave;
  end;
end;

{ EAMQPChannelError }

constructor EAMQPChannelError.Create(AChannel, ACode: Word; const AText: string;
  AClassId: Word; AMethodId: Word);
begin
  inherited Create(AText);
  FChannel := AChannel;
  FCode := ACode;
  FClassId := AClassId;
  FMethodId := AMethodId;
end;

{ EAMQPConnectionError }

constructor EAMQPConnectionError.Create(ACode: Word; const AText: string;
  AClassId: Word; AMethodId: Word);
begin
  inherited Create(AText);
  FCode := ACode;
  FClassId := AClassId;
  FMethodId := AMethodId;
end;

{ TAMQPServerConnConfig }

class function TAMQPServerConnConfig.Defaults: TAMQPServerConnConfig;
begin
  Result.Auth := nil;
  Result.Authorizer := nil;
  Result.VHosts := nil;
  Result.ChannelMax := AMQP_SERVER_DEFAULT_CHANNEL_MAX;
  Result.FrameMax := AMQP_SERVER_DEFAULT_FRAME_MAX;
  Result.Heartbeat := AMQP_SERVER_DEFAULT_HEARTBEAT;
  Result.Tls := False;
  Result.CloseTimeoutMs := AMQP_SERVER_CLOSE_TIMEOUT_MS;
  Result.Sink := nil;
  Result.Confirms := nil;
end;

{ TAMQPNullMessageSink }

function TAMQPNullMessageSink.RouteMessage(const AVHost: string;
  const AMessage: TAMQPServerMessage; out ARejected: Boolean;
  out ALsn: UInt64): Boolean;
begin
  ARejected := False; // a null sink never rejects: there is no queue to fill
  ALsn := 0;           // and nothing became durable: there is no journal
  PcAtomicInc(FCount);
  // Phase 1: no queue exists, so nothing was routed. Returning False is what
  // Phase 2 will use to trigger the Basic.Return of a mandatory publish.
  Result := False;
end;

function TAMQPNullMessageSink.Count: Integer;
begin
  Result := PcAtomicGet(FCount);
end;

end.
