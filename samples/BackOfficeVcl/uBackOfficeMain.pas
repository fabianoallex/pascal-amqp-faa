unit uBackOfficeMain;

{ Single screen: editable connection fields + queue/prefetch, a
  Connect/Disconnect button that declares the queue, sets the Qos and calls
  Channel.Consume. The consumer callback runs on the thread pool (the lib's
  native dispatch) and only touches the ListView/label through TThread.Queue -
  the FItems dictionary is only accessed from the marshalled procedure on the
  main thread, so it needs no lock of its own. Same simulated "processing" as
  the console sample (a random Sleep playing the part of fetching the XML).

  "Manual confirmation" mode: instead of Sleep+automatic ack, the consumer
  thread stays blocked on a TEvent per message (FPending, protected by
  FPendingLock) until the user clicks Accept/Reject on the screen for the
  invoice selected in the ListView. On disconnect/close, CancelPendingApprovals
  releases any thread still blocked (as nack+requeue) before cancelling the
  consumer and closing the channel.

  FShuttingDown closes a shutdown race: the nack+requeue fired by
  CancelPendingApprovals can make the broker redeliver the message to the consumer
  (still active until Cancel-Ok), and that new callback would park on a TEvent
  that nobody will signal anymore - while the channel's Destroy waits, on the
  main thread, for every callback to finish (deadlock: frozen UI).
  With the flag (set under FPendingLock before waking the events), any
  delivery that arrives after shutdown has begun leaves without ack/nack - the
  channel close returns the message to the queue.

  Compiles in both worlds from the SAME source (same pattern as the other
  samples): uses without a namespace prefix, conditional only where the feature
  really differs (lfm vs dfm).

  Two adaptations relative to the Delphi-only version: TAMQPConsumerCallback is
  'of object' in this lib (it does not accept an anonymous method; see CLAUDE.md),
  so the Consume callback became the OnDelivery method below. And FPC only
  accepts TThread.Queue with an 'of object' method WITHOUT PARAMETERS
  (TThreadMethod) - there is no overload with Delphi's anonymous closure - so
  each TThread.Queue(nil, procedure ... end) became a call to QueueInvoiceReceived/
  QueueInvoiceStatus, which packs the data per call into a disposable "marshal"
  object (TReceivedMarshal/TStatusMarshal). A shared field on the
  form would NOT work here: several pool workers call this concurrently,
  and the value would be overwritten before the queued method ran on the UI. }

interface

uses
  // On FPC, the LCL emulation layer (LCLIntf/LCLType/LMessages) covers the
  // WinAPI calls of the log autoscroll on any widgetset (win32, gtk2...).
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes,
  SyncObjs, Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Connection, AMQP.Transport, AMQP.Queue.Methods;

type
  TPendingApproval = class
    Event: TEvent;
    Accept: Boolean;
    Requeue: Boolean;
  end;

  TfrmBackOffice = class(TForm)
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
    btnConnect: TButton;
    lblStatus: TLabel;
    gbConsume: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblPrefetch: TLabel;
    edtPrefetch: TEdit;
    chkManual: TCheckBox;
    chkDedicated: TCheckBox;
    gbApproval: TGroupBox;
    btnAccept: TButton;
    btnReject: TButton;
    chkRequeue: TCheckBox;
    lvInvoices: TListView;
    lblTotals: TLabel;
    btnClearLog: TButton;
    mmoLog: TMemo;
    Button1: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
    procedure Button1Click(Sender: TObject);
    procedure btnAcceptClick(Sender: TObject);
    procedure btnRejectClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FConsumerTag: string;
    FItems: TDictionary<string, TListItem>;
    FReceived: Integer;
    FReady: Integer;
    FRejected: Integer;
    FManualMode: Boolean;
    FPending: TDictionary<string, TPendingApproval>;
    FPendingLock: TCriticalSection;
    FShuttingDown: Boolean; // protected by FPendingLock
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConnected(AConnected: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RefreshTotals;
    procedure InvoiceReceived(const AKey, AWorker: string);
    procedure InvoiceStatus(const AKey, AStatus: string);
    // Marshalling to the UI thread (see the note at the top of the file).
    procedure QueueInvoiceReceived(const AKey, AWorker: string);
    procedure QueueInvoiceStatus(const AKey, AStatus: string);
    procedure ResolveSelected(AAccept, ARequeue: Boolean);
    procedure CancelPendingApprovals;
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  end;

var
  frmBackOffice: TfrmBackOffice;

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

type
  // One per call of QueueInvoiceReceived/QueueInvoiceStatus: TThread.Queue on FPC
  // only accepts a 'procedure of object' WITHOUT PARAMETERS, so each call's
  // data travels in its own object (not in a shared field of the form,
  // which would race between concurrent workers). It frees itself after running.
  TReceivedMarshal = class
    Form: TfrmBackOffice;
    InvoiceKey, Worker: string;
    procedure Execute;
  end;

  TStatusMarshal = class
    Form: TfrmBackOffice;
    InvoiceKey, Status: string;
    procedure Execute;
  end;

procedure TReceivedMarshal.Execute;
begin
  Form.InvoiceReceived(InvoiceKey, Worker);
  Free;
end;

procedure TStatusMarshal.Execute;
begin
  Form.InvoiceStatus(InvoiceKey, Status);
  Free;
end;

procedure TfrmBackOffice.FormCreate(Sender: TObject);
begin
  Randomize;
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FItems := TDictionary<string, TListItem>.Create;
  FPending := TDictionary<string, TPendingApproval>.Create;
  FPendingLock := TCriticalSection.Create;
  SetConnected(False);
end;

procedure TfrmBackOffice.FormDestroy(Sender: TObject);
begin
  FPendingLock.Free;
  FPending.Free;
  FItems.Free;
end;

procedure TfrmBackOffice.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  CancelPendingApprovals;
  if Assigned(FChannel) and (FConsumerTag <> '') then
    try
      FChannel.Cancel(FConsumerTag);
    except
    end;
  FreeAndNil(FChannel);
  // DrainInFlight (inside the Free above) blocks this thread in a Sleep until the
  // running callback finishes - the message loop is stalled in the
  // meantime, so any TThread.Queue posted by the callback (status/received
  // marshal) is stuck in the queue and would leak if the application closed
  // without ever returning to the message loop. Pumping here drains that pending
  // one safely, because the form and its controls are still alive.
  Application.ProcessMessages;
  FreeAndNil(FConn);
end;

function TfrmBackOffice.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmBackOffice.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmBackOffice.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmBackOffice.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmBackOffice.BuildParams: TAMQPConnectionParams;
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
end;

procedure TfrmBackOffice.Button1Click(Sender: TObject);
begin
  lvInvoices.Items.Clear;
end;

procedure TfrmBackOffice.ResolveSelected(AAccept, ARequeue: Boolean);
var
  LKey: string;
  LApproval: TPendingApproval;
begin
  if lvInvoices.Selected = nil then
  begin
    Log('Select an invoice in the list before accepting/rejecting.');
    Exit;
  end;
  LKey := lvInvoices.Selected.Caption;
  FPendingLock.Enter;
  try
    if not FPending.TryGetValue(LKey, LApproval) then
    begin
      Log('Invoice "' + LKey + '" is not awaiting approval.');
      Exit;
    end;
    LApproval.Accept := AAccept;
    LApproval.Requeue := ARequeue;
  finally
    FPendingLock.Leave;
  end;
  LApproval.Event.SetEvent;
end;

procedure TfrmBackOffice.btnAcceptClick(Sender: TObject);
begin
  ResolveSelected(True, False);
end;

procedure TfrmBackOffice.btnRejectClick(Sender: TObject);
begin
  ResolveSelected(False, chkRequeue.Checked);
end;

procedure TfrmBackOffice.CancelPendingApprovals;
var
  LApproval: TPendingApproval;
begin
  FPendingLock.Enter;
  try
    FShuttingDown := True; // deliveries from here on do not park on the TEvent
    for LApproval in FPending.Values do
    begin
      LApproval.Accept := False;
      LApproval.Requeue := True;
      LApproval.Event.SetEvent;
    end;
  finally
    FPendingLock.Leave;
  end;
end;

procedure TfrmBackOffice.SetConnected(AConnected: Boolean);
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
  edtQueue.Enabled := not AConnected;
  edtPrefetch.Enabled := not AConnected;
  chkManual.Enabled := not AConnected;
  chkDedicated.Enabled := not AConnected;
  btnAccept.Enabled := AConnected;
  btnReject.Enabled := AConnected;
end;

procedure TfrmBackOffice.RefreshTotals;
begin
  lblTotals.Caption := Format('Received: %d   |   Ready: %d   |   Rejected: %d',
    [FReceived, FReady, FRejected]);
end;

procedure TfrmBackOffice.InvoiceReceived(const AKey, AWorker: string);
var
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(lvInvoices.Handle);
  LItem := lvInvoices.Items.Add;
  LItem.Caption := AKey;
  LItem.SubItems.Add('Received');
  LItem.SubItems.Add(AWorker);
  LItem.SubItems.Add(FormatDateTime('hh:nn:ss', Now));
  LItem.SubItems.Add('');
  FItems.AddOrSetValue(AKey, LItem);
  Inc(FReceived);
  RefreshTotals;
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmBackOffice.InvoiceStatus(const AKey, AStatus: string);
var
  LItem: TListItem;
begin
  if not FItems.TryGetValue(AKey, LItem) then
    Exit;
  LItem.SubItems[0] := AStatus;
  if AStatus = 'Ready' then
  begin
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    Inc(FReady);
    RefreshTotals;
  end
  else if (AStatus = 'Rejected') or (AStatus = 'Rejected (requeue)') then
  begin
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    Inc(FRejected);
    RefreshTotals;
  end;
end;

procedure TfrmBackOffice.QueueInvoiceReceived(const AKey, AWorker: string);
var
  LMarshal: TReceivedMarshal;
begin
  LMarshal := TReceivedMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.InvoiceKey := AKey;
  LMarshal.Worker := AWorker;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmBackOffice.QueueInvoiceStatus(const AKey, AStatus: string);
var
  LMarshal: TStatusMarshal;
begin
  LMarshal := TStatusMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.InvoiceKey := AKey;
  LMarshal.Status := AStatus;
  TThread.Queue(nil, LMarshal.Execute);
end;

// ANoAck=False (the Consume default): we only ack after processing - an
// "at least once" guarantee out of the box, with no extra step.
procedure TfrmBackOffice.OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LKey, LWorker: string;
  LDelay: Integer;
  LApproval: TPendingApproval;
begin
  LKey := ADelivery.BodyAsText;
  LWorker := Format('%d', [TThread.CurrentThread.ThreadID]);

  QueueInvoiceReceived(LKey, LWorker);

  if FManualMode then
  begin
    FPendingLock.Enter;
    try
      if FShuttingDown then
        LApproval := nil // CancelPendingApprovals already ran; nobody would wake the TEvent
      else
      begin
        LApproval := TPendingApproval.Create;
        LApproval.Event := TEvent.Create(nil, True, False, '');
        FPending.Add(LKey, LApproval);
      end;
    finally
      FPendingLock.Leave;
    end;

    if LApproval = nil then
    begin
      // Leaves without ack/nack: closing the channel returns the message to the queue.
      // A Nack here would restart the immediate redelivery cycle.
      QueueInvoiceStatus(LKey, 'Returned (disconnected)');
      Exit;
    end;

    QueueInvoiceStatus(LKey, 'Awaiting approval');

    LApproval.Event.WaitFor(INFINITE);

    FPendingLock.Enter;
    try
      FPending.Remove(LKey);
    finally
      FPendingLock.Leave;
    end;

    try
      if LApproval.Accept then
      begin
        AChannel.Ack(ADelivery.DeliveryTag);
        QueueInvoiceStatus(LKey, 'Ready');
      end
      else
      begin
        AChannel.Nack(ADelivery.DeliveryTag, LApproval.Requeue);
        if LApproval.Requeue then
          QueueInvoiceStatus(LKey, 'Rejected (requeue)')
        else
          QueueInvoiceStatus(LKey, 'Rejected');
      end;
    finally
      LApproval.Event.Free;
      LApproval.Free;
    end;
    Exit;
  end;

  LDelay := 300 + Random(2200);
  QueueInvoiceStatus(LKey, 'Processing');

  Sleep(LDelay);

  try
    QueueInvoiceStatus(LKey, 'Ready');
    AChannel.Ack(ADelivery.DeliveryTag);
  except
    AChannel.Nack(ADelivery.DeliveryTag, True);
    QueueInvoiceStatus(LKey, 'Error');
  end;
end;

procedure TfrmBackOffice.btnConnectClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LPrefetch: Integer;
  LQueue: string;
  LMsg: string;
begin
  if Assigned(FConn) then
  begin
    try
      CancelPendingApprovals;
      if (FChannel <> nil) and (FConsumerTag <> '') then
        FChannel.Cancel(FConsumerTag);
      FConsumerTag := '';
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
  LQueue := Trim(edtQueue.Text);
  LPrefetch := StrToIntDef(Trim(edtPrefetch.Text), 10);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.Open;

    FChannel := FConn.CreateChannel(chkDedicated.Checked);
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(LQueue, True));
    FChannel.Qos(LPrefetch);
    FManualMode := chkManual.Checked;
    // No lock: no callback is alive here (the previous channel was drained in Free).
    FShuttingDown := False;

    FConsumerTag := FChannel.Consume(LQueue, OnDelivery);

    LMsg := Format('Connected to %s:%d%s. Consuming "%s" (prefetch %d).',
      [LParams.Host, LParams.Port, LParams.VirtualHost, LQueue, LPrefetch]);
    if FManualMode then
      LMsg := LMsg + ' Manual confirmation on.';
    if chkDedicated.Checked then
      LMsg := LMsg + ' Dedicated thread (no concurrency, order preserved).';
    Log(LMsg);

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

end.
