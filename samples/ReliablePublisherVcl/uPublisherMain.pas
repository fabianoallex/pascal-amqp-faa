unit uPublisherMain;

{ Reliable publisher: a showcase of the lib's publisher confirms on one screen.

  The channel is put in confirm mode (ConfirmSelect) right after connecting; each
  Publish gets a seq-no and becomes a row in the list, whose status changes when
  the broker confirms (OnConfirm/ack), rejects (nack) or returns an unroutable
  `mandatory` publish (OnBasicReturn). The batch is published by its own
  thread (TBatchThread), with a configurable interval between messages, so you
  can bring the broker down IN THE MIDDLE of the batch (docker stop/start on
  RabbitMQ) and watch the whole sequence: send failure, automatic reconnection,
  replay of the topology + confirm mode and, with "Resend unconfirmed on
  reconnect" checked (RepublishUnconfirmedOnReconnect), the automatic resend of
  what was left unconfirmed. At the end of the batch the thread calls
  WaitForConfirms and logs the result (all confirmed or not, and how long it took).

  Two subtleties of confirms that the screen makes visible:
  - A connection drop does NOT fire OnConfirm: the publishes pending at the drop
    are marked in the list by the reconnection event ("Resent (new no.)" with
    resend on, "Lost in the drop" without). The resend happens with NEW
    seq-nos assigned by the lib, so the confirms of those resends arrive
    without a matching row; they go into the "Resends confirmed" counter.
  - An unroutable mandatory publish is RETURNED (Basic.Return) and still
    CONFIRMED (ack): confirm means "the broker took responsibility",
    not "it reached a queue". The row becomes "Returned (no route)".

  When sending a message fails (socket down), the batch thread does NOT
  republish on its own: the seq-no was assigned and the content went into the
  resend buffer BEFORE the send, so republishing by hand would duplicate the
  message. It only waits for the reconnection (TEvent signaled by OnReconnect)
  and moves on to the NEXT message of the batch.

  Compiles in both worlds from the SAME source (same pattern as the other
  GUI samples): uses without a namespace prefix, callbacks as named
  methods ('of object', FPC rule) and UI updates coming from other
  threads through disposable "marshal" objects + TThread.Queue: FPC only
  accepts a TThreadMethod without parameters, and a shared field on the form
  would race between concurrent callbacks (see BackOfficeVcl). Exception: the
  connection events can NOT post TThread.Queue directly; they run on the
  lib's reconnection thread, which dies right afterwards, and on FPC that
  drops the pending post; see the comment on TConnectionEventWork. }

interface

uses
  // On FPC, the LCL emulation layer (LCLIntf/LCLType/LMessages) covers the
  // WinAPI calls of the log autoscroll on any widgetset (win32, gtk2...).
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes, StrUtils,
  SyncObjs, Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.Threading, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  TfrmPublisher = class;

  { Publishes the batch off the UI thread, with a pause between messages, so the
    screen stays alive and there is time to bring the broker down mid-batch. }
  TBatchThread = class(TThread)
  private
    FForm: TfrmPublisher;
    FPubChannel: TAMQPChannel;
    FQueue: string;
    FCount: Integer;
    FIntervalMs: Integer;
    FStop: Boolean;
    FReconnected: TEvent; // signaled by the connection's OnReconnect
    function WaitForReconnect: Boolean;
    function InterruptibleSleep(AMs: Integer): Boolean;
  protected
    procedure Execute; override;
  public
    constructor Create(AForm: TfrmPublisher; AChan: TAMQPChannel;
      const AQueue: string; ACount, AIntervalMs: Integer);
    destructor Destroy; override;
    procedure Shutdown;
    procedure SignalReconnect;
  end;

  TfrmPublisher = class(TForm)
    gbConnection: TGroupBox;
    lblHost: TLabel;
    edtHost: TEdit;
    lblPort: TLabel;
    edtPort: TEdit;
    lblVHost: TLabel;
    edtVHost: TEdit;
    lblUser: TLabel;
    edtUser: TEdit;
    lblPassword: TLabel;
    edtPassword: TEdit;
    chkUseTls: TCheckBox;
    chkTlsVerifyPeer: TCheckBox;
    chkRepublish: TCheckBox;
    btnConnect: TButton;
    lblStatus: TLabel;
    gbPublishing: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblCount: TLabel;
    edtCount: TEdit;
    lblInterval: TLabel;
    edtInterval: TEdit;
    btnPublish: TButton;
    btnNoRoute: TButton;
    lvMessages: TListView;
    lblTotals: TLabel;
    btnClearList: TButton;
    btnClearLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnPublishClick(Sender: TObject);
    procedure btnNoRouteClick(Sender: TObject);
    procedure btnClearListClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FQueue: string;          // queue declared on connect (the batch publishes to it)
    FRepublishing: Boolean; // snapshot of the checkbox at connect time
    FBatch: TBatchThread;
    // Protects FBatch between the UI (creates/destroys) and the reconnection thread
    // (SignalReconnect). Never call WaitFor while holding this lock.
    FBatchLock: TCriticalSection;
    // Row maps of the list; touched ONLY on the UI thread (through marshals).
    FBySeq: TDictionary<UInt64, TListItem>;
    FByKey: TDictionary<string, TListItem>;
    FPublished: Integer;
    FConfirmed: Integer;
    FNacks: Integer;
    FReturned: Integer;
    FConfirmedResends: Integer;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConnected(AConnected: Boolean);
    procedure SetBatchRunning(ARunning: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RefreshTotals;
    /// Ends the batch thread (if any), waiting for it to exit. Called only on the
    /// UI thread (disconnect/close). The "Stop batch" click does NOT use
    /// this: it only signals and leaves the join to BatchFinished, without freezing the UI.
    procedure StopBatch;
    // UI updates (run on the UI thread, called by the marshals):
    procedure MsgPublished(ASeqNo: UInt64; const AKey: string);
    procedure MsgConfirmed(ASeqNo: UInt64; AAck: Boolean);
    procedure MsgReturned(const AKey, AReason: string);
    procedure ConnectionDropped;
    procedure ConnectionRestored;
    procedure ConnectionFailed;
    procedure BatchFinished;
    // Marshalling to the UI thread (callable from any thread):
    procedure QueueLog(const AMsg: string);
    procedure QueuePublished(ASeqNo: UInt64; const AKey: string);
    procedure QueueBatchFinished;
    // Lib callbacks (run on pool / reconnection threads):
    procedure OnConfirmed(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
    procedure OnReturned(AChannel: TAMQPChannel; const AReturned: TAMQPReturnedMessage);
    procedure OnConnDisconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnectFailed(AConnection: TAMQPConnection);
  end;

var
  frmPublisher: TfrmPublisher;

implementation

{$IFDEF FPC}
  {$R *.lfm}
{$ELSE}
  {$R *.dfm}
{$ENDIF}

{$IFDEF FPC}
const
  // The LCL has no Messages unit; LM_VSCROLL has the same value as WM_VSCROLL.
  WM_VSCROLL = LM_VSCROLL;
{$ENDIF}

const
  cStatusPending   = 'Awaiting confirm';
  cStatusConfirmed = 'Confirmed';
  cStatusNack       = 'Nack from broker';
  cStatusReturned  = 'Returned (no route)';
  cStatusResent  = 'Resent (new no.)';
  cStatusLost    = 'Lost in the drop (no resend)';

// Builds the properties and publishes an invoice (body = the key itself, as in
// the other samples). Used by the batch thread and by the mandatory button.
function PublishInvoice(AChan: TAMQPChannel; const ARoute, AKey: string;
  AMandatory: Boolean): UInt64;
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LProps.SetPersistent; // survives a broker restart (the queue is durable too)
  LProps.SetMessageId(AKey);
  Result := AChan.Publish('', ARoute, AmqpUtf8Encode(AKey), LProps, AMandatory);
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See BackOfficeVcl.
  TLogMarshal = class
    Form: TfrmPublisher;
    LogText: string;
    procedure Execute;
  end;

  TPublishedMarshal = class
    Form: TfrmPublisher;
    Seq: UInt64;
    InvoiceKey: string;
    procedure Execute;
  end;

  TConfirmedMarshal = class
    Form: TfrmPublisher;
    Seq: UInt64;
    Ack: Boolean;
    procedure Execute;
  end;

  TReturnedMarshal = class
    Form: TfrmPublisher;
    InvoiceKey, Reason: string;
    procedure Execute;
  end;

  TConnectionEvent = (ceDropped, ceRestored, ceFailed);

  TConnectionMarshal = class
    Form: TfrmPublisher;
    Event: TConnectionEvent;
    procedure Execute;
  end;

  TBatchEndMarshal = class
    Form: TfrmPublisher;
    procedure Execute;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(LogText);
  Free;
end;

procedure TPublishedMarshal.Execute;
begin
  Form.MsgPublished(Seq, InvoiceKey);
  Free;
end;

procedure TConfirmedMarshal.Execute;
begin
  Form.MsgConfirmed(Seq, Ack);
  Free;
end;

procedure TReturnedMarshal.Execute;
begin
  Form.MsgReturned(InvoiceKey, Reason);
  Free;
end;

procedure TConnectionMarshal.Execute;
begin
  case Event of
    ceDropped:   Form.ConnectionDropped;
    ceRestored: Form.ConnectionRestored;
    ceFailed: Form.ConnectionFailed;
  end;
  Free;
end;

procedure TBatchEndMarshal.Execute;
begin
  Form.BatchFinished;
  Free;
end;

type
  { The connection events (OnDisconnect/OnReconnect/OnReconnectFailed) run
    on the lib's RECONNECTION thread, which ends right after OnReconnect, and on
    FPC a TThread.Queue posted by a thread that dies before the main thread
    pumps the queue is DROPPED: TThread.Destroy calls
    RemoveQueuedEvents(Self), which matches by the poster's ThreadID (stamped
    by InternalQueue even when nil is passed in AThread). On Delphi the removal
    matches only by the Thread field (which stays nil), so there the direct post
    would work. Observed symptom: the OnDisconnect marshal arrived (the
    reconnection thread is still alive during the retries) and the OnReconnect one
    vanished. Hopping through a PcPool worker (PERSISTENT threads) guarantees
    that the post to the UI survives on both compilers. }
  TConnectionEventWork = class(TPcWorkItem)
  private
    FForm: TfrmPublisher;
    FEvent: TConnectionEvent;
  public
    constructor Create(AForm: TfrmPublisher; AEvent: TConnectionEvent);
    procedure Execute; override;
  end;

constructor TConnectionEventWork.Create(AForm: TfrmPublisher;
  AEvent: TConnectionEvent);
begin
  inherited Create;
  FForm := AForm;
  FEvent := AEvent;
end;

procedure TConnectionEventWork.Execute;
var
  LMarshal: TConnectionMarshal;
begin
  LMarshal := TConnectionMarshal.Create;
  LMarshal.Form := FForm;
  LMarshal.Event := FEvent;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ TBatchThread }

constructor TBatchThread.Create(AForm: TfrmPublisher; AChan: TAMQPChannel;
  const AQueue: string; ACount, AIntervalMs: Integer);
begin
  FForm := AForm;
  FPubChannel := AChan;
  FQueue := AQueue;
  FCount := ACount;
  FIntervalMs := AIntervalMs;
  FReconnected := TEvent.Create(nil, False, False, ''); // auto-reset
  inherited Create(False);
end;

destructor TBatchThread.Destroy;
begin
  inherited;
  FReconnected.Free;
end;

procedure TBatchThread.Shutdown;
begin
  FStop := True;
end;

procedure TBatchThread.SignalReconnect;
begin
  FReconnected.SetEvent;
end;

function TBatchThread.WaitForReconnect: Boolean;
begin
  while not FStop do
    if FReconnected.WaitFor(300) = wrSignaled then
      Exit(True);
  Result := False;
end;

function TBatchThread.InterruptibleSleep(AMs: Integer): Boolean;
var
  LRemaining, LSlice: Integer;
begin
  LRemaining := AMs;
  while (LRemaining > 0) and (not FStop) do
  begin
    if LRemaining < 100 then
      LSlice := LRemaining
    else
      LSlice := 100;
    Sleep(LSlice);
    Dec(LRemaining, LSlice);
  end;
  Result := not FStop;
end;

procedure TBatchThread.Execute;
var
  I: Integer;
  LKey, LRunId: string;
  LSeq: UInt64;
  LStart: UInt64;
begin
  LRunId := FormatDateTime('hhnnsszzz', Now);
  for I := 1 to FCount do
  begin
    if FStop then
      Break;
    LKey := Format('NFE-%s-%.4d', [LRunId, I]);
    try
      LSeq := PublishInvoice(FPubChannel, FQueue, LKey, False);
      FForm.QueuePublished(LSeq, LKey);
    except
      on E: Exception do
      begin
        // The seq-no was assigned and the content went into the resend buffer (if
        // enabled) BEFORE the send failed; the lib republishes on its own after
        // reconnecting. Publishing again here would DUPLICATE the message; we only
        // wait for the connection to come back and move on to the next one.
        FForm.QueueLog(Format('Failed to send %s: %s — waiting for reconnection.',
          [LKey, E.Message]));
        if not WaitForReconnect then
          Break;
        FForm.QueueLog('Connection restored; resuming the batch.');
      end;
    end;
    if not InterruptibleSleep(FIntervalMs) then
      Break;
  end;

  if not FStop then
  begin
    FForm.QueueLog('Batch sent. Waiting for pending confirms (WaitForConfirms)...');
    LStart := PcTickMs;
    try
      if FPubChannel.WaitForConfirms(5000) then
        FForm.QueueLog(Format('WaitForConfirms: all confirmed (%d ms).',
          [Int64(PcTickMs - LStart)]))
      else
        FForm.QueueLog('WaitForConfirms: not all confirmed (nack, lost in the drop or timeout).');
    except
      on E: Exception do
        FForm.QueueLog('WaitForConfirms failed: ' + E.Message);
    end;
  end;
  FForm.QueueBatchFinished;
end;

{ TfrmPublisher }

procedure TfrmPublisher.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FBySeq := TDictionary<UInt64, TListItem>.Create;
  FByKey := TDictionary<string, TListItem>.Create;
  FBatchLock := TCriticalSection.Create;
  SetConnected(False);
  RefreshTotals;
end;

procedure TfrmPublisher.FormDestroy(Sender: TObject);
begin
  FBatchLock.Free;
  FByKey.Free;
  FBySeq.Free;
end;

procedure TfrmPublisher.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  StopBatch;
  FreeAndNil(FChannel);
  // The Free above drains the callbacks in flight (OnConfirm/OnBasicReturn); the
  // marshals they posted are still in the TThread.Queue queue, and pumping
  // here drains them with the form still alive (see BackOfficeVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread, so no more connection
  // events are born. A TConnectionEventWork already queued on the pool may be
  // posting the last marshal RIGHT NOW: the short wait + pump drain
  // that straggler with the form still alive.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmPublisher.ScrollAtBottom(AHandle: HWND): Boolean;
var
  LInfo: TScrollInfo;
begin
  FillChar(LInfo, SizeOf(LInfo), 0);
  LInfo.cbSize := SizeOf(LInfo);
  LInfo.fMask := SIF_ALL;
  if not GetScrollInfo(AHandle, SB_VERT, LInfo) then
    Exit(True); // no scrollbar yet (all content fits) = treat as "at the end"
  Result := (LInfo.nPos + Integer(LInfo.nPage)) >= LInfo.nMax;
end;

procedure TfrmPublisher.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmPublisher.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmPublisher.btnClearListClick(Sender: TObject);
begin
  lvMessages.Items.Clear;
  FBySeq.Clear;
  FByKey.Clear;
  FPublished := 0;
  FConfirmed := 0;
  FNacks := 0;
  FReturned := 0;
  FConfirmedResends := 0;
  RefreshTotals;
end;

procedure TfrmPublisher.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmPublisher.BuildParams: TAMQPConnectionParams;
begin
  if chkUseTls.Checked then
    Result := TAMQPConnectionParams.LocalhostTls
  else
    Result := TAMQPConnectionParams.Localhost;
  Result.Host := Trim(edtHost.Text);
  Result.Port := StrToIntDef(Trim(edtPort.Text), Result.Port);
  Result.VirtualHost := edtVHost.Text;
  Result.User := edtUser.Text;
  Result.Password := edtPassword.Text;
  Result.UseTls := chkUseTls.Checked;
  Result.TlsVerifyPeer := chkTlsVerifyPeer.Checked;
  // The point of the sample: automatic reconnection always on, and the resend of
  // unconfirmed publishes opt-in through the checkbox.
  Result.AutoReconnect := True;
  Result.ReconnectDelayMs := 2000;
  Result.MaxReconnectAttempts := 0; // keeps trying until it comes back
  Result.RepublishUnconfirmedOnReconnect := chkRepublish.Checked;
  Result.ConnectionName := 'ReliablePublisherVcl';
end;

procedure TfrmPublisher.SetConnected(AConnected: Boolean);
begin
  if AConnected then
  begin
    btnConnect.Caption := 'Disconnect';
    // With TLS, shows the engine that actually LOADED (OpenSSL publishes version
    // and DLL/soname on the 1st connection; see AmqpTlsBackendInfo).
    if chkUseTls.Checked then
      lblStatus.Caption := 'Connected — TLS: ' + AmqpTlsBackendInfo
    else
      lblStatus.Caption := 'Connected (no TLS)';
    lblStatus.Font.Color := clGreen;
  end
  else
  begin
    btnConnect.Caption := 'Connect';
    lblStatus.Caption := 'Disconnected';
    lblStatus.Font.Color := clRed;
  end;
  edtHost.Enabled := not AConnected;
  edtPort.Enabled := not AConnected;
  edtVHost.Enabled := not AConnected;
  edtUser.Enabled := not AConnected;
  edtPassword.Enabled := not AConnected;
  chkUseTls.Enabled := not AConnected;
  chkTlsVerifyPeer.Enabled := (not AConnected) and chkUseTls.Checked;
  chkRepublish.Enabled := not AConnected;
  edtQueue.Enabled := not AConnected;
  btnPublish.Enabled := AConnected;
  btnNoRoute.Enabled := AConnected;
end;

procedure TfrmPublisher.SetBatchRunning(ARunning: Boolean);
begin
  if ARunning then
    btnPublish.Caption := 'Stop batch'
  else
    btnPublish.Caption := 'Publish batch';
  btnPublish.Enabled := Assigned(FConn);
  edtCount.Enabled := not ARunning;
  edtInterval.Enabled := not ARunning;
end;

procedure TfrmPublisher.RefreshTotals;
begin
  lblTotals.Caption := Format(
    'Published: %d | Confirmed: %d | Nacks: %d | Returned: %d | Resends confirmed: %d',
    [FPublished, FConfirmed, FNacks, FReturned, FConfirmedResends]);
end;

procedure TfrmPublisher.StopBatch;
var
  LBatch: TBatchThread;
begin
  FBatchLock.Enter;
  try
    LBatch := FBatch;
    FBatch := nil;
  finally
    FBatchLock.Leave;
  end;
  if LBatch = nil then
    Exit;
  LBatch.Shutdown;
  LBatch.WaitFor;
  LBatch.Free;
  SetBatchRunning(False);
end;

{ --- updates on the UI thread --------------------------------------------- }

procedure TfrmPublisher.MsgPublished(ASeqNo: UInt64; const AKey: string);
var
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(lvMessages.Handle);
  LItem := lvMessages.Items.Add;
  LItem.Caption := IntToStr(Int64(ASeqNo));
  LItem.SubItems.Add(AKey);
  LItem.SubItems.Add(cStatusPending);
  LItem.SubItems.Add(FormatDateTime('hh:nn:ss', Now));
  LItem.SubItems.Add('');
  FBySeq.AddOrSetValue(ASeqNo, LItem);
  FByKey.AddOrSetValue(AKey, LItem);
  Inc(FPublished);
  RefreshTotals;
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmPublisher.MsgConfirmed(ASeqNo: UInt64; AAck: Boolean);
var
  LItem: TListItem;
begin
  if not FBySeq.TryGetValue(ASeqNo, LItem) then
  begin
    // Seq-no without a row: it is the confirm of an automatic post-drop resend
    // (the lib resends with new seq-nos; the old->new mapping is internal).
    if AAck then
    begin
      Inc(FConfirmedResends);
      Log(Format('Resend confirmed by the broker (no. %d).', [Int64(ASeqNo)]));
    end
    else
    begin
      Inc(FNacks);
      Log(Format('Broker nacked resend no. %d.', [Int64(ASeqNo)]));
    end;
    RefreshTotals;
    Exit;
  end;

  if AAck then
  begin
    Inc(FConfirmed);
    // Does not downgrade a more informative status (e.g. "Returned (no route)":
    // the broker returns AND confirms an unroutable mandatory).
    if LItem.SubItems[1] = cStatusPending then
      LItem.SubItems[1] := cStatusConfirmed;
  end
  else
  begin
    Inc(FNacks);
    LItem.SubItems[1] := cStatusNack;
    Log(Format('Broker nacked publish no. %d.', [Int64(ASeqNo)]));
  end;
  LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
  RefreshTotals;
end;

procedure TfrmPublisher.MsgReturned(const AKey, AReason: string);
var
  LItem: TListItem;
begin
  Log('Basic.Return: "' + AKey + '" returned (' + AReason + ').');
  if FByKey.TryGetValue(AKey, LItem) then
  begin
    LItem.SubItems[1] := cStatusReturned;
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
  end;
  Inc(FReturned);
  RefreshTotals;
end;

procedure TfrmPublisher.ConnectionDropped;
begin
  lblStatus.Caption := 'Connection lost — reconnecting...';
  lblStatus.Font.Color := clMaroon;
  if FRepublishing then
    Log('Connection lost. Automatic reconnection in progress; unconfirmed ' +
      'publishes will be resent after reconnecting.')
  else
    Log('Connection lost. Automatic reconnection in progress; unconfirmed ' +
      'publishes will NOT be resent (checkbox unchecked).');
end;

procedure TfrmPublisher.ConnectionRestored;
var
  I: Integer;
  LItem: TListItem;
begin
  SetConnected(True);
  Log('Reconnected: topology and confirm mode restored' +
    IfThen(FRepublishing, '; automatic resend triggered.', '.'));
  // Rows left pending at the drop will never get the confirm for the old
  // seq-no; mark their outcome here.
  for I := 0 to lvMessages.Items.Count - 1 do
  begin
    LItem := lvMessages.Items[I];
    if LItem.SubItems[1] = cStatusPending then
    begin
      if FRepublishing then
        LItem.SubItems[1] := cStatusResent
      else
        LItem.SubItems[1] := cStatusLost;
      LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    end;
  end;
end;

procedure TfrmPublisher.ConnectionFailed;
begin
  lblStatus.Caption := 'Reconnection exhausted';
  lblStatus.Font.Color := clRed;
  Log('Reconnection gave up (MaxReconnectAttempts reached).');
end;

procedure TfrmPublisher.BatchFinished;
var
  LBatch: TBatchThread;
begin
  FBatchLock.Enter;
  try
    LBatch := FBatch;
    FBatch := nil;
  finally
    FBatchLock.Leave;
  end;
  if LBatch <> nil then
  begin
    LBatch.WaitFor; // the thread already posted this marshal at the end of Execute; quick join
    LBatch.Free;
  end;
  SetBatchRunning(False);
  Log('Batch finished.');
end;

{ --- marshalling (callable from any thread) ------------------------------- }

procedure TfrmPublisher.QueueLog(const AMsg: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.LogText := AMsg;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmPublisher.QueuePublished(ASeqNo: UInt64; const AKey: string);
var
  LMarshal: TPublishedMarshal;
begin
  LMarshal := TPublishedMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Seq := ASeqNo;
  LMarshal.InvoiceKey := AKey;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmPublisher.QueueBatchFinished;
var
  LMarshal: TBatchEndMarshal;
begin
  LMarshal := TBatchEndMarshal.Create;
  LMarshal.Form := Self;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ --- lib callbacks (pool / reconnection threads) -------------------------- }

procedure TfrmPublisher.OnConfirmed(AChannel: TAMQPChannel; ASeqNo: UInt64;
  AAck: Boolean);
var
  LMarshal: TConfirmedMarshal;
begin
  LMarshal := TConfirmedMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Seq := ASeqNo;
  LMarshal.Ack := AAck;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmPublisher.OnReturned(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
var
  LMarshal: TReturnedMarshal;
begin
  LMarshal := TReturnedMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.InvoiceKey := AReturned.BodyAsText;
  LMarshal.Reason := Format('%d %s', [AReturned.ReplyCode, AReturned.ReplyText]);
  TThread.Queue(nil, LMarshal.Execute);
end;

// The three connection events hop through PcPool instead of posting directly:
// the reconnection thread dies right after OnReconnect and, on FPC, would take
// the pending post with it (see the comment on TConnectionEventWork).
procedure TfrmPublisher.OnConnDisconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceDropped));
end;

procedure TfrmPublisher.OnConnReconnected(AConnection: TAMQPConnection);
begin
  // Wakes the batch thread (if it is waiting for the connection to come back). Under
  // the lock: the UI may be destroying the thread at this very moment.
  FBatchLock.Enter;
  try
    if Assigned(FBatch) then
      FBatch.SignalReconnect;
  finally
    FBatchLock.Leave;
  end;
  PcPool.Queue(TConnectionEventWork.Create(Self, ceRestored));
end;

procedure TfrmPublisher.OnConnReconnectFailed(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceFailed));
end;

{ --- actions ---------------------------------------------------------------- }

procedure TfrmPublisher.btnConnectClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
      StopBatch; // the batch thread publishes on the channel we are about to close
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      Log('Disconnected.');
    except
      on E: Exception do
        Log('Error while disconnecting: ' + E.Message);
    end;
    SetConnected(False);
    Exit;
  end;

  LParams := BuildParams;
  FQueue := Trim(edtQueue.Text);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnConnDisconnected;
    FConn.OnReconnect := OnConnReconnected;
    FConn.OnReconnectFailed := OnConnReconnectFailed;
    FConn.Open;

    FChannel := FConn.CreateChannel;
    FChannel.OnConfirm := OnConfirmed;
    FChannel.OnBasicReturn := OnReturned;
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(FQueue, True)); // durable
    FChannel.ConfirmSelect;

    FRepublishing := chkRepublish.Checked;
    // New connection = new seq-no numbering (restarts at 1): clears the list
    // so the old rows do not collide with the new seq-nos.
    btnClearListClick(nil);

    Log(Format('Connected to %s:%d%s. Confirm mode on for queue "%s"; ' +
      'resend on reconnect: %s.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FQueue,
       IfThen(FRepublishing, 'on', 'off')]));
    Log('Tip: publish a batch and take the broker down halfway ' +
      '(docker stop delphi-amqp-faa-rabbitmq) to see the reconnection and the resend.');
    SetConnected(True);
  except
    on E: Exception do
    begin
      Log('Failed to connect: ' + E.Message);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      SetConnected(False);
    end;
  end;
end;

procedure TfrmPublisher.btnPublishClick(Sender: TObject);
var
  LCount, LInterval: Integer;
begin
  FBatchLock.Enter;
  try
    if Assigned(FBatch) then
    begin
      // Only signals; the join is left to BatchFinished (the thread may be in a
      // WaitForConfirms; a WaitFor here would freeze the UI for seconds).
      FBatch.Shutdown;
      btnPublish.Enabled := False;
      btnPublish.Caption := 'Stopping...';
      Exit;
    end;
  finally
    FBatchLock.Leave;
  end;

  if not Assigned(FChannel) then
  begin
    Log('Connect before publishing.');
    Exit;
  end;
  LCount := StrToIntDef(Trim(edtCount.Text), 0);
  if LCount <= 0 then
  begin
    Log('Invalid count.');
    Exit;
  end;
  LInterval := StrToIntDef(Trim(edtInterval.Text), 300);

  FBatchLock.Enter;
  try
    FBatch := TBatchThread.Create(Self, FChannel, FQueue, LCount, LInterval);
  finally
    FBatchLock.Leave;
  end;
  SetBatchRunning(True);
  Log(Format('Batch of %d messages started (interval of %d ms).',
    [LCount, LInterval]));
end;

procedure TfrmPublisher.btnNoRouteClick(Sender: TObject);
var
  LKey: string;
  LSeq: UInt64;
begin
  if not Assigned(FChannel) then
  begin
    Log('Connect before publishing.');
    Exit;
  end;
  LKey := 'NO-ROUTE-' + FormatDateTime('hhnnsszzz', Now);
  try
    // mandatory=True for a route that does not exist: the broker returns the
    // message (Basic.Return) and STILL confirms it (ack); confirm
    // means "I took responsibility", not "I routed it to a queue".
    LSeq := PublishInvoice(FChannel, 'missing-queue-demo', LKey, True);
    MsgPublished(LSeq, LKey); // we are already on the UI thread
    Log('Published "' + LKey + '" (mandatory) to a route that does not exist — ' +
      'wait for the Basic.Return.');
  except
    on E: Exception do
      Log('Error publishing unroutable: ' + E.Message);
  end;
end;

end.
