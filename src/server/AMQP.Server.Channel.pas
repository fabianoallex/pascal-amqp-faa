unit AMQP.Server.Channel;

{$I amqp.inc}

{ One channel of a broker connection (server sub-module).

  Holds the channel lifecycle (WS4) and content assembly (WS5): in
  AMQP 0-9-1 a published message arrives in three parts ON THIS channel, in this
  exact order (spec 2.3.5):

    1) the Basic.Publish method        -> BeginContent
    2) a content header with the body-size and the properties  -> SetContentHeader
    3) N body frames until they add up to the body-size        -> AppendBody

  Between part 1 and the last part 3 no other frame may appear ON THIS
  channel — that is the "interleave rule", and the connection enforces it (505
  UNEXPECTED_FRAME). Different channels may interleave freely: that is why the
  assembly state lives here, per channel, and not in the connection.

  Only the read thread of the owning connection touches these objects. }

interface

uses
  SysUtils,
  Generics.Collections,
  AMQP.Protocol,
  AMQP.Basic.Methods,
  AMQP.Server.Types,
  AMQP.Server.FrameIO,
  AMQP.Server.Queue,
  AMQP.Server.Delivery;

type
  { amqchOpen  — usable channel.
    amqchClosing — the broker sent Channel.Close and waits for Channel.Close-Ok;
    every frame arriving on the channel in the meantime is dropped (spec 1.4.2.2). }
  TAMQPServerChannelState = (amqchOpen, amqchClosing);

  { amqasIdle    — no content being assembled.
    amqasHeader  — Basic.Publish arrived, the content header is missing.
    amqasBody    — the header arrived, body frames are missing to reach the body-size. }
  TAMQPChannelAsmState = (amqasIdle, amqasHeader, amqasBody);

  TAMQPServerChannel = class
  private
    FId: Word;
    FState: TAMQPServerChannelState;
    FActive: Boolean;
    FConfirmMode: Boolean;
    FPublishSeq: UInt64;
    FPrefetchCount: Word;
    // Delivery target (WS4). The channel creates and DETACHES it; the queues where
    // this channel has consumers hold the interface, which keeps the object alive
    // even after the channel dies -- see AMQP.Server.Delivery.
    FTargetObj: TAMQPChannelDeliveryTarget;
    FTarget: IAMQPDeliveryTarget;
    // Target identity, kept SEPARATELY because it must SURVIVE
    // DetachDelivery: connection teardown detaches the targets before sweeping
    // the channels, and it is precisely in that sweep that the queues need to
    // know which channel to release consumers and unacked deliveries from.
    FDeliveryId: NativeUInt;
    // Deferred-confirm tracker (WS5 of Phase 4). nil while the broker
    // has no DataDir -- without durability the confirm goes out inline, as in
    // Phase 3. The connection creates and registers it (only it knows the
    // broker's registry); the channel only holds and detaches it, for the same
    // reason as the delivery target: the JOURNAL thread releases the pending one.
    FConfirms: IAMQPConfirmTracker;
    // WS5: the channel is what knows how to tie what the client says to what the
    // engine needs. Two ties:
    //  - consumer-tag -> queue name, for Basic.Cancel and for teardown;
    //  - delivery-tag -> queue name, because Basic.Ack/Nack/Reject arrives
    //    with a CHANNEL tag and the QUEUE holds the unacked delivery.
    // Only this connection's read thread touches these maps.
    FLastQueue: string;
    FConsumers: TDictionary<string, string>;
    FUnacked: TDictionary<UInt64, string>;
    // --- content assembly ---
    FAsmState: TAMQPChannelAsmState;
    FPublish: TAMQPBasicPublish;
    FProps: TAMQPBasicProperties;
    FHeaderPayload: TBytes;
    FBodySize: UInt64;
    FBody: TBytes;
    FBodyLen: Integer;
    procedure FreeProps;
    procedure SetActive(AValue: Boolean);
    procedure SetPrefetchCount(AValue: Word);
    procedure SetConfirmMode(AValue: Boolean);
  public
    constructor Create(AId: Word);
    destructor Destroy; override;

    function IsOpen: Boolean;

    /// Creates this channel's delivery target over the connection writer.
    /// AMaxPayload = how much fits in a frame payload (negotiated frame-max
    /// minus the overhead). Call once, when opening the channel.
    procedure AttachDelivery(AWriter: TAMQPFrameWriter; AMaxPayload: Cardinal);
    /// Releases the target: from here on every pending delivery fails instead of
    /// touching the writer. MANDATORY before freeing the channel or the writer --
    /// a queue may be delivering at this very instant, on a pool
    /// thread. Idempotent; the destructor calls it.
    procedure DetachDelivery;

    /// Basic.Publish arrived: starts assembly, waiting for the content header.
    /// Drops any previous unfinished assembly.
    procedure BeginContent(const APublish: TAMQPBasicPublish);
    /// The content header arrived, already decoded (AHeader) AND the frame's raw
    /// payload (AHeaderPayload — decision D1 of Phase 2: the final message carries
    /// that payload for faithful re-emission, see AMQP.Server.Message). Result =
    /// content ALREADY complete (body-size 0, message with no body — a legitimate
    /// case, common in RPC).
    function SetContentHeader(const AHeader: TAMQPContentHeader;
      const AHeaderPayload: TBytes): Boolean;
    /// Appends a body frame. Result = content complete.
    /// Raises EAMQPConnectionError (505) if the body exceeds the declared
    /// body-size — the stream is out of sync with what the peer promised.
    function AppendBody(const AData: TBytes): Boolean;
    /// Builds the finished message. Only makes sense with assembly complete; the
    /// Headers table still belongs to THIS channel (see IAMQPMessageSink).
    function CurrentMessage(const AUserId: string): TAMQPServerMessage;
    /// Returns to amqasIdle, freeing what was accumulated. Idempotent.
    procedure ResetContent;

    // --- WS5 ties ---
    procedure AddConsumerTag(const ATag, AQueue: string);
    function ConsumerQueue(const ATag: string; out AQueue: string): Boolean;
    procedure RemoveConsumerTag(const ATag: string);
    /// Queues where this channel has a consumer or a pending delivery — the
    /// teardown has to notify each one.
    function TouchedQueues: TArray<string>;

    property Id: Word read FId;
    property State: TAMQPServerChannelState read FState write FState;
    /// Delivery target to hang on a queue consumer (nil before
    /// AttachDelivery). It is an interface: whoever keeps it keeps the object alive.
    property DeliveryTarget: IAMQPDeliveryTarget read FTarget;
    /// The same target as an object, for what only the connection does (Basic.Get
    /// delivery-tag, lowering the prefetch on ack). nil before AttachDelivery.
    property Delivery: TAMQPChannelDeliveryTarget read FTargetObj;
    /// Identity of this channel's target. Stays valid after
    /// DetachDelivery (see the field). 0 before AttachDelivery.
    property DeliveryId: NativeUInt read FDeliveryId;
    /// This channel's confirm tracker, or nil (broker without durability).
    property Confirms: IAMQPConfirmTracker read FConfirms write FConfirms;
    /// Channel.Flow: False = the peer asked us to stop delivering content
    /// on this channel. Phase 2 honors it on delivery.
    property Active: Boolean read FActive write SetActive;
    /// Confirm.Select received (publisher confirms): every publish now
    /// generates a Basic.Ack. Turning it on resets the seq-no sequence (spec 1.3.1
    /// of the extension: counting starts when the mode is turned on).
    property ConfirmMode: Boolean read FConfirmMode write SetConfirmMode;
    /// Consumes and returns this channel's next publish seq-no. The
    /// sequence starts at 1 and counts EVERY publish while confirm mode
    /// is on — routed or not, because that is how the client
    /// correlates the ack with the publish it numbered.
    function NextPublishSeq: UInt64;
    /// Basic.Qos. Phase 2 uses it to limit unacknowledged deliveries.
    property PrefetchCount: Word read FPrefetchCount write SetPrefetchCount;

    /// Last queue declared ON THIS channel. The spec lets the client omit the
    /// queue name in Queue.Bind/Purge/Delete and Basic.Consume/Get, and in that
    /// case the last one declared on the channel applies (spec 1.7.2.1).
    property LastQueue: string read FLastQueue write FLastQueue;
    property AsmState: TAMQPChannelAsmState read FAsmState;
    /// Body-size declared in the content header (valid in amqasBody).
    property BodySize: UInt64 read FBodySize;
    /// How much of the body has arrived.
    property BodyLen: Integer read FBodyLen;
  end;

implementation

constructor TAMQPServerChannel.Create(AId: Word);
begin
  inherited Create;
  FId := AId;
  FState := amqchOpen;
  FActive := True;
  FAsmState := amqasIdle;
  FProps := TAMQPBasicProperties.Empty;
  FConsumers := TDictionary<string, string>.Create;
end;

destructor TAMQPServerChannel.Destroy;
begin
  DetachDelivery; // never free the channel leaving a live delivery pointing here
  ResetContent; // a channel closed in the middle of a publish must not leak the table
  FConsumers.Free;
  inherited;
end;

procedure TAMQPServerChannel.AttachDelivery(AWriter: TAMQPFrameWriter;
  AMaxPayload: Cardinal);
begin
  if FTarget <> nil then
    Exit;
  FTargetObj := TAMQPChannelDeliveryTarget.Create(AWriter, FId, AMaxPayload);
  FTarget := FTargetObj; // the interface is what counts the references
  FDeliveryId := FTargetObj.ChannelId;
  FTargetObj.SetActive(FActive);
  FTargetObj.SetPrefetch(FPrefetchCount);
end;

procedure TAMQPServerChannel.DetachDelivery;
begin
  if FTargetObj <> nil then
    FTargetObj.Detach;
  // The confirm tracker dies along with it, for the same reason: the journal
  // thread may be inside it right now. Detach releases the writer under lock.
  // The reference stays (the broker's registry still holds it until Unregister).
  if FConfirms <> nil then
    FConfirms.Detach;
  // The REFERENCE stays here on purpose (only the Detach happens): the
  // connection teardown detaches all targets BEFORE sweeping the channels, and
  // that sweep still needs to ask the target in which queues this channel
  // has open deliveries. The interface refcount frees the object
  // when the channel and the queues release the last reference.
end;

procedure TAMQPServerChannel.AddConsumerTag(const ATag, AQueue: string);
begin
  FConsumers.AddOrSetValue(ATag, AQueue);
end;

function TAMQPServerChannel.ConsumerQueue(const ATag: string;
  out AQueue: string): Boolean;
begin
  Result := FConsumers.TryGetValue(ATag, AQueue);
end;

procedure TAMQPServerChannel.RemoveConsumerTag(const ATag: string);
begin
  FConsumers.Remove(ATag);
end;

function TAMQPServerChannel.TouchedQueues: TArray<string>;
var
  LName: string;
  LSet: TDictionary<string, Boolean>;
begin
  LSet := TDictionary<string, Boolean>.Create;
  try
    for LName in FConsumers.Values do
      LSet.AddOrSetValue(LName, True);
    if FTargetObj <> nil then
      for LName in FTargetObj.PendingQueues do
        LSet.AddOrSetValue(LName, True);
    Result := LSet.Keys.ToArray;
  finally
    LSet.Free;
  end;
end;

procedure TAMQPServerChannel.SetConfirmMode(AValue: Boolean);
begin
  if AValue and (not FConfirmMode) then
    FPublishSeq := 0; // the sequence is born with the mode
  FConfirmMode := AValue;
end;

function TAMQPServerChannel.NextPublishSeq: UInt64;
begin
  Inc(FPublishSeq);
  Result := FPublishSeq;
end;

procedure TAMQPServerChannel.SetActive(AValue: Boolean);
begin
  FActive := AValue;
  if FTargetObj <> nil then
    FTargetObj.SetActive(AValue);
end;

procedure TAMQPServerChannel.SetPrefetchCount(AValue: Word);
begin
  FPrefetchCount := AValue;
  if FTargetObj <> nil then
    FTargetObj.SetPrefetch(AValue);
end;

function TAMQPServerChannel.IsOpen: Boolean;
begin
  Result := FState = amqchOpen;
end;

procedure TAMQPServerChannel.FreeProps;
begin
  // DecodeContentHeader creates the headers table when the property is
  // present; the owner is whoever decoded it, that is, this channel.
  if FProps.Has(bpHeaders) and Assigned(FProps.Headers) then
    FProps.Headers.Free;
  FProps := TAMQPBasicProperties.Empty;
end;

procedure TAMQPServerChannel.ResetContent;
begin
  FreeProps;
  FAsmState := amqasIdle;
  FHeaderPayload := nil;
  FBodySize := 0;
  FBody := nil;
  FBodyLen := 0;
end;

procedure TAMQPServerChannel.BeginContent(const APublish: TAMQPBasicPublish);
begin
  ResetContent;
  FPublish := APublish;
  FAsmState := amqasHeader;
end;

function TAMQPServerChannel.SetContentHeader(const AHeader: TAMQPContentHeader;
  const AHeaderPayload: TBytes): Boolean;
begin
  FreeProps;
  FProps := AHeader.Properties;
  FHeaderPayload := AHeaderPayload;
  FBodySize := AHeader.BodySize;
  FBody := nil;
  FBodyLen := 0;
  // No body: the message is already complete and no body frame will come.
  if FBodySize = 0 then
  begin
    FAsmState := amqasIdle;
    Exit(True);
  end;
  FAsmState := amqasBody;
  Result := False;
end;

function TAMQPServerChannel.AppendBody(const AData: TBytes): Boolean;
var
  LLen: Integer;
begin
  LLen := Length(AData);
  // Grows as it arrives: never allocate the declared body-size at once,
  // or a hostile peer brings the broker down just by announcing a giant body.
  if UInt64(FBodyLen) + UInt64(LLen) > FBodySize then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('body exceeds declared body-size (%d): already %d + %d octets',
        [Int64(FBodySize), FBodyLen, LLen]), 0, 0);

  if LLen > 0 then
  begin
    SetLength(FBody, FBodyLen + LLen);
    Move(AData[0], FBody[FBodyLen], LLen);
    Inc(FBodyLen, LLen);
  end;

  Result := UInt64(FBodyLen) = FBodySize;
  if Result then
    FAsmState := amqasIdle;
end;


function TAMQPServerChannel.CurrentMessage(
  const AUserId: string): TAMQPServerMessage;
begin
  Result.Exchange := FPublish.Exchange;
  Result.RoutingKey := FPublish.RoutingKey;
  Result.Mandatory := FPublish.Mandatory;
  Result.Immediate := FPublish.Immediate;
  Result.Properties := FProps;
  Result.Body := Copy(FBody, 0, FBodyLen); // FBody may have allocation slack
  Result.UserId := AUserId;
  Result.HeaderPayload := FHeaderPayload;
end;

end.
