unit uRetryMain;

{ Dead-letter + retry with backoff: the most used reprocessing pattern on
  RabbitMQ, built only with queue arguments (TAMQPFieldTable), with no
  scheduling code in the client.

  Topology (created on connect, from the configured base queue):
    <queue>       work queue (durable), with dead-letter pointing to
                  <queue>-retry through the default exchange ('' + routing key = name
                  of the target queue, so no exchange of its own needs declaring);
    <queue>-retry wait queue WITHOUT a consumer: x-message-ttl = backoff and
                  dead-letter back to <queue>; the message "sleeps" here
                  and the broker hands it back on its own when the TTL expires;
    <queue>-dlq   the final destination (dead-letter queue) for whoever ran out
                  of attempts.

  Cycle: the consumer reads the current attempt from the x-death header (which the
  broker increments on every dead-letter; we count the reason='rejected' entries
  of the work queue) and simulates the processing, configurable to fail the
  first N attempts. Failed with attempts left => Nack(requeue=False),
  which the work queue's DLX sends to the retry queue (backoff). Failed on the
  last attempt => the consumer itself publishes the message to the DLQ and
  Acks it (that is the pattern: the broker does not count attempts by itself;
  the consumer decides "enough"). A second consumer (automatic ack) watches the
  DLQ only to mark the arrival on screen; in production the DLQ is kept for
  inspection.

  The backoff uses the QUEUE TTL (x-message-ttl on the retry queue), not a
  per-message Expiration: a per-message TTL only expires at the HEAD of the queue
  (a long-TTL message holds back the short-TTL ones behind it). With a single
  queue TTL, the expiry order is the arrival order: a reliable fixed backoff.
  (Exponential backoff = N retry queues with growing TTLs; out of scope for
  this sample.)

  Connecting recreates the topology from scratch (DeleteQueue + declare):
  changing the backoff changes the queue arguments, and redeclaring an existing
  queue with different arguments is PRECONDITION_FAILED (it closes the channel).
  Deleting a missing queue is a no-op on RabbitMQ, so the path is idempotent.

  Compiles in both worlds from the SAME source (GUI samples pattern):
  named callbacks ('of object'), disposable marshals + TThread.Queue
  for the UI (see BackOfficeVcl) and connection events hopping through PcPool
  (the dropped TThread.Queue gotcha; see TConnectionEventWork and CLAUDE.md). }

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
  Generics.Collections, Rtti,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  TfrmRetry = class(TForm)
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
    gbTopology: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblBackoff: TLabel;
    edtBackoff: TEdit;
    lblMax: TLabel;
    edtMax: TEdit;
    gbProcess: TGroupBox;
    lblFailFirst: TLabel;
    edtFailFirst: TEdit;
    lblCount: TLabel;
    edtCount: TEdit;
    btnPublish: TButton;
    btnConsume: TButton;
    lblCounters: TLabel;
    lvInvoices: TListView;
    btnClearList: TButton;
    btnClearLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnPublishClick(Sender: TObject);
    procedure btnConsumeClick(Sender: TObject);
    procedure btnClearListClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FWorkQueue: string;
    FRetryQueue: string;
    FDlqQueue: string;
    FBackoffMs: Integer;
    FMaxAttempts: Integer;
    FFailFirst: Integer; // captured when consumption starts
    FConsumeTag: string;       // '' = consumption stopped
    FDlqTag: string;
    // Rows by key; touched ONLY on the UI thread (through marshals).
    FItems: TDictionary<string, TListItem>;
    FPublished: Integer;
    FReady: Integer;
    FInDlq: Integer;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConnected(AConnected: Boolean);
    procedure SetConsuming(AConsuming: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RefreshCounters;
    procedure CreateTopology;
    // UI updates (UI thread, through marshals):
    procedure InvoiceStatus(const AKey: string; AAttempt: Integer;
      const AStatus: string; AReady, ADlq: Boolean);
    procedure ConnectionDropped;
    procedure ConnectionRestored;
    procedure ConnectionFailed;
    // Marshalling (callable from any thread):
    procedure QueueLog(const AMsg: string);
    procedure QueueStatus(const AKey: string; AAttempt: Integer;
      const AStatus: string; AReady: Boolean = False; ADlq: Boolean = False);
    // Lib callbacks (pool / reconnection threads):
    procedure OnInvoice(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnDlqInvoice(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnConnDisconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnectFailed(AConnection: TAMQPConnection);
  end;

var
  frmRetry: TfrmRetry;

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

{ Sums in the x-death header the rejections (reason='rejected') coming from the
  work queue: that is the number of attempts of this message that already FAILED.
  The broker updates x-death on every dead-letter; the pass through the retry
  queue shows up as reason='expired' on the -retry queue and is filtered out
  here. TValue fields: the indexer's result goes into a local variable before
  the accessor (.AsString chained straight on the indexer gives an internal
  error on FPC 3.2; gotcha in CLAUDE.md). }
function CountRejections(const AProps: TAMQPBasicProperties;
  const AWorkQueue: string): Integer;
var
  LXDeath, LEntry, LField: TValue;
  LTab: TAMQPFieldTable;
  I: Integer;
begin
  Result := 0;
  if not AProps.Has(bpHeaders) then
    Exit;
  if AProps.Headers = nil then
    Exit;
  if not AProps.Headers.TryGetValue('x-death', LXDeath) then
    Exit;
  if not LXDeath.IsArray then
    Exit;
  for I := 0 to LXDeath.GetArrayLength - 1 do
  begin
    // AmqpUnwrapValue: GetArrayElement returns the element re-wrapped
    // in a tkRecord TValue, on Delphi and on FPC (gotcha in CLAUDE.md).
    LEntry := AmqpUnwrapValue(LXDeath.GetArrayElement(I));
    if not (LEntry.IsObject and (LEntry.AsObject is TAMQPFieldTable)) then
      Continue;
    LTab := TAMQPFieldTable(LEntry.AsObject);
    if not LTab.TryGetValue('queue', LField) then
      Continue;
    if LField.AsString <> AWorkQueue then
      Continue;
    if not LTab.TryGetValue('reason', LField) then
      Continue;
    if LField.AsString <> 'rejected' then
      Continue;
    if LTab.TryGetValue('count', LField) then
      Inc(Result, Integer(LField.AsInt64));
  end;
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See BackOfficeVcl.
  TLogMarshal = class
    Form: TfrmRetry;
    LogText: string;
    procedure Execute;
  end;

  TStatusMarshal = class
    Form: TfrmRetry;
    InvoiceKey: string;
    Attempt: Integer;
    Status: string;
    Ready, Dlq: Boolean;
    procedure Execute;
  end;

  TConnectionEvent = (ceDropped, ceRestored, ceFailed);

  TConnectionMarshal = class
    Form: TfrmRetry;
    Event: TConnectionEvent;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConnectionEventWork = class(TPcWorkItem)
  private
    FForm: TfrmRetry;
    FEvent: TConnectionEvent;
  public
    constructor Create(AForm: TfrmRetry; AEvent: TConnectionEvent);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(LogText);
  Free;
end;

procedure TStatusMarshal.Execute;
begin
  Form.InvoiceStatus(InvoiceKey, Attempt, Status, Ready, Dlq);
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

constructor TConnectionEventWork.Create(AForm: TfrmRetry; AEvent: TConnectionEvent);
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

{ TfrmRetry }

procedure TfrmRetry.FormCreate(Sender: TObject);
begin
  Randomize;
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FItems := TDictionary<string, TListItem>.Create;
  SetConnected(False);
  RefreshCounters;
end;

procedure TfrmRetry.FormDestroy(Sender: TObject);
begin
  FItems.Free;
end;

procedure TfrmRetry.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(FChannel) then
  begin
    if FConsumeTag <> '' then
      try FChannel.Cancel(FConsumeTag); except end;
    if FDlqTag <> '' then
      try FChannel.Cancel(FDlqTag); except end;
  end;
  FreeAndNil(FChannel);
  // The Free above drains the callbacks in flight; the marshals they posted
  // are still in the TThread.Queue queue, and pumping here drains them with the
  // form still alive (see BackOfficeVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread; a TConnectionEventWork
  // already queued on the pool may be posting the last marshal RIGHT NOW.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmRetry.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmRetry.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmRetry.QueueLog(const AMsg: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.LogText := AMsg;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmRetry.QueueStatus(const AKey: string; AAttempt: Integer;
  const AStatus: string; AReady, ADlq: Boolean);
var
  LMarshal: TStatusMarshal;
begin
  LMarshal := TStatusMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.InvoiceKey := AKey;
  LMarshal.Attempt := AAttempt;
  LMarshal.Status := AStatus;
  LMarshal.Ready := AReady;
  LMarshal.Dlq := ADlq;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmRetry.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmRetry.btnClearListClick(Sender: TObject);
begin
  lvInvoices.Items.Clear;
  FItems.Clear;
  FPublished := 0;
  FReady := 0;
  FInDlq := 0;
  RefreshCounters;
end;

procedure TfrmRetry.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmRetry.BuildParams: TAMQPConnectionParams;
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
  Result.AutoReconnect := True;
  Result.ReconnectDelayMs := 2000;
  Result.MaxReconnectAttempts := 0;
  Result.ConnectionName := 'RetryDlqVcl';
end;

procedure TfrmRetry.SetConnected(AConnected: Boolean);
begin
  if AConnected then
  begin
    btnConnect.Caption := 'Desconectar';
    // With TLS, shows the engine that actually LOADED (OpenSSL publishes version
    // and DLL/soname on the 1st connection; see AmqpTlsBackendInfo).
    if chkUseTls.Checked then
      lblStatus.Caption := 'Conectado — TLS: ' + AmqpTlsBackendInfo
    else
      lblStatus.Caption := 'Conectado (sem TLS)';
    lblStatus.Font.Color := clGreen;
  end
  else
  begin
    btnConnect.Caption := 'Conectar';
    lblStatus.Caption := 'Desconectado';
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
  edtBackoff.Enabled := not AConnected;
  edtMax.Enabled := not AConnected;
  btnPublish.Enabled := AConnected;
  btnConsume.Enabled := AConnected;
end;

procedure TfrmRetry.SetConsuming(AConsuming: Boolean);
begin
  if AConsuming then
    btnConsume.Caption := 'Parar consumo'
  else
    btnConsume.Caption := 'Iniciar consumo';
  edtFailFirst.Enabled := not AConsuming;
end;

procedure TfrmRetry.RefreshCounters;
begin
  lblCounters.Caption := Format('Publicadas: %d | Prontas: %d | Na DLQ: %d',
    [FPublished, FReady, FInDlq]);
end;

{ Recreates the topology from scratch. The queue arguments are the heart of the
  pattern: dead-letter through the default exchange ('' + routing key = target
  queue) and a queue TTL on the retry queue. The Puts are separate statements
  (chaining that ends in an inline TValue.From<T> gives an internal error on
  FPC 3.2; gotcha in CLAUDE.md); the caller owns the Arguments table and frees
  it after the declare. }
procedure TfrmRetry.CreateTopology;
var
  LDel: TAMQPQueueDelete;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
begin
  // DeleteQueue: deleting a missing queue is a no-op; an existing queue with old
  // args (the backoff changed) is removed, since redeclaring with different args
  // would be PRECONDITION_FAILED.
  LDel := Default(TAMQPQueueDelete);
  LDel.QueueName := FWorkQueue;
  FChannel.DeleteQueue(LDel);
  LDel.QueueName := FRetryQueue;
  FChannel.DeleteQueue(LDel);
  LDel.QueueName := FDlqQueue;
  FChannel.DeleteQueue(LDel);

  // Work queue: rejections (Nack without requeue) fall into the retry queue.
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-dead-letter-exchange', TValue.From<string>(''));
    LArgs.Put('x-dead-letter-routing-key', TValue.From<string>(FRetryQueue));
    LDecl := TAMQPQueueDeclare.Create(FWorkQueue, True);
    LDecl.Arguments := LArgs;
    FChannel.DeclareQueue(LDecl);
  finally
    LArgs.Free;
  end;

  // Retry: no consumer; queue TTL = backoff, and the expiry dead-letters
  // back to the work queue.
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-message-ttl', TValue.From<Integer>(FBackoffMs));
    LArgs.Put('x-dead-letter-exchange', TValue.From<string>(''));
    LArgs.Put('x-dead-letter-routing-key', TValue.From<string>(FWorkQueue));
    LDecl := TAMQPQueueDeclare.Create(FRetryQueue, True);
    LDecl.Arguments := LArgs;
    FChannel.DeclareQueue(LDecl);
  finally
    LArgs.Free;
  end;

  // DLQ: a plain queue; whoever runs out of attempts is published here by the consumer.
  FChannel.DeclareQueue(TAMQPQueueDeclare.Create(FDlqQueue, True));
end;

{ --- consumption ------------------------------------------------------------ }

// Runs on a pool thread. Current attempt = previous rejections (x-death) + 1.
procedure TfrmRetry.OnInvoice(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LKey, LDlqStatus: string;
  LAttempt: Integer;
  LProps: TAMQPBasicProperties;
begin
  LKey := ADelivery.BodyAsText;
  LAttempt := CountRejections(ADelivery.Properties, FWorkQueue) + 1;

  QueueStatus(LKey, LAttempt, Format('Processando (tentativa %d)', [LAttempt]));
  Sleep(200 + Random(300)); // simulated processing

  if LAttempt > FFailFirst then
  begin
    // Success: ack and end the cycle.
    AChannel.Ack(ADelivery.DeliveryTag);
    QueueStatus(LKey, LAttempt,
      Format('Pronta (tentativa %d)', [LAttempt]), True);
    Exit;
  end;

  if LAttempt >= FMaxAttempts then
  begin
    // Exhausted: the CONSUMER decides to stop (the broker does not count attempts
    // on its own); it publishes to the DLQ and acks the original.
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetPersistent;
    AChannel.Publish('', FDlqQueue, AmqpUtf8Encode(LKey), LProps);
    AChannel.Ack(ADelivery.DeliveryTag);
    LDlqStatus := Format('Falhou (tentativa %d) — enviada à DLQ', [LAttempt]);
    QueueStatus(LKey, LAttempt, LDlqStatus);
    QueueLog(Format('Nota %s esgotou as %d tentativas; movida para "%s".',
      [LKey, FMaxAttempts, FDlqQueue]));
    Exit;
  end;

  // Failed with attempts left: Nack WITHOUT requeue => the work queue's DLX
  // sends it to the retry queue, which hands it back after the backoff.
  AChannel.Nack(ADelivery.DeliveryTag, False);
  QueueStatus(LKey, LAttempt,
    Format('Falhou (tentativa %d) — retry em %d ms', [LAttempt, FBackoffMs]));
end;

// DLQ watcher (automatic ack), only to mark the arrival on screen.
procedure TfrmRetry.OnDlqInvoice(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
begin
  QueueStatus(ADelivery.BodyAsText, 0, 'Na DLQ', False, True);
end;

procedure TfrmRetry.btnConsumeClick(Sender: TObject);
begin
  if not Assigned(FChannel) then
    Exit;

  if FConsumeTag <> '' then
  begin
    try
      FChannel.Cancel(FConsumeTag);
      if FDlqTag <> '' then
        FChannel.Cancel(FDlqTag);
      Log('Consumo parado.');
    except
      on E: Exception do
        Log('Erro ao parar o consumo: ' + E.Message);
    end;
    FConsumeTag := '';
    FDlqTag := '';
    SetConsuming(False);
    Exit;
  end;

  FFailFirst := StrToIntDef(Trim(edtFailFirst.Text), 2);
  if FFailFirst < 0 then
    FFailFirst := 0;
  try
    FConsumeTag := FChannel.Consume(FWorkQueue, OnInvoice);
    FDlqTag := FChannel.Consume(FDlqQueue, OnDlqInvoice, True); // NoAck: only watches
    Log(Format('Consumindo "%s" (falha simulada nas %d primeiras tentativas; ' +
      'máx %d; backoff %d ms).',
      [FWorkQueue, FFailFirst, FMaxAttempts, FBackoffMs]));
    SetConsuming(True);
  except
    on E: Exception do
      Log('Erro ao iniciar o consumo: ' + E.Message);
  end;
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmRetry.btnPublishClick(Sender: TObject);
var
  I, LCount: Integer;
  LKey, LRunId: string;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not Assigned(FChannel) then
    Exit;
  LCount := StrToIntDef(Trim(edtCount.Text), 3);
  if LCount < 1 then
    LCount := 1;
  LRunId := FormatDateTime('hhnnsszzz', Now);
  for I := 1 to LCount do
  begin
    LKey := Format('NFE-%s-%.3d', [LRunId, I]);
    try
      FChannel.PublishText('', FWorkQueue, LKey); // persistent by default
    except
      on E: Exception do
      begin
        Log('Erro ao publicar ' + LKey + ': ' + E.Message);
        Break;
      end;
    end;
    LAtBottom := ScrollAtBottom(lvInvoices.Handle);
    LItem := lvInvoices.Items.Add;
    LItem.Caption := LKey;
    LItem.SubItems.Add('—');
    LItem.SubItems.Add('Publicada');
    LItem.SubItems.Add(FormatDateTime('hh:nn:ss', Now));
    FItems.AddOrSetValue(LKey, LItem);
    Inc(FPublished);
    if LAtBottom then
      LItem.MakeVisible(False);
  end;
  RefreshCounters;
  Log(Format('%d nota(s) publicada(s) em "%s".', [LCount, FWorkQueue]));
end;

{ --- UI updates ------------------------------------------------------------- }

procedure TfrmRetry.InvoiceStatus(const AKey: string; AAttempt: Integer;
  const AStatus: string; AReady, ADlq: Boolean);
var
  LItem: TListItem;
begin
  if FItems.TryGetValue(AKey, LItem) then
  begin
    if AAttempt > 0 then
      LItem.SubItems[0] := IntToStr(AAttempt);
    LItem.SubItems[1] := AStatus;
    LItem.SubItems[2] := FormatDateTime('hh:nn:ss', Now);
  end;
  if AReady then
  begin
    Inc(FReady);
    Log(Format('Nota %s pronta (tentativa %d).', [AKey, AAttempt]));
  end;
  if ADlq then
  begin
    Inc(FInDlq);
    Log(Format('Nota %s confirmada na DLQ.', [AKey]));
  end;
  if AReady or ADlq then
    RefreshCounters;
end;

procedure TfrmRetry.ConnectionDropped;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento (filas duráveis: nada se perde).');
end;

procedure TfrmRetry.ConnectionRestored;
begin
  SetConnected(True);
  Log('Reconectado: topologia e consumers restaurados.');
end;

procedure TfrmRetry.ConnectionFailed;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConnectionEventWork).
procedure TfrmRetry.OnConnDisconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceDropped));
end;

procedure TfrmRetry.OnConnReconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceRestored));
end;

procedure TfrmRetry.OnConnReconnectFailed(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceFailed));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmRetry.btnConnectClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
      if FConsumeTag <> '' then
        FChannel.Cancel(FConsumeTag);
      if FDlqTag <> '' then
        FChannel.Cancel(FDlqTag);
      FConsumeTag := '';
      FDlqTag := '';
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      Log('Desconectado.');
    except
      on E: Exception do
        Log('Erro ao desconectar: ' + E.Message);
    end;
    SetConsuming(False);
    SetConnected(False);
    Exit;
  end;

  LParams := BuildParams;
  FWorkQueue := Trim(edtQueue.Text);
  FRetryQueue := FWorkQueue + '-retry';
  FDlqQueue := FWorkQueue + '-dlq';
  FBackoffMs := StrToIntDef(Trim(edtBackoff.Text), 4000);
  if FBackoffMs < 100 then
    FBackoffMs := 100;
  FMaxAttempts := StrToIntDef(Trim(edtMax.Text), 3);
  if FMaxAttempts < 1 then
    FMaxAttempts := 1;
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnConnDisconnected;
    FConn.OnReconnect := OnConnReconnected;
    FConn.OnReconnectFailed := OnConnReconnectFailed;
    FConn.Open;

    FChannel := FConn.CreateChannel;
    FChannel.Qos(8);
    CreateTopology;

    Log(Format('Conectado a %s:%d%s. Topologia recriada: "%s" (trabalho, DLX' +
      '->retry), "%s" (espera, TTL %d ms, DLX->trabalho), "%s" (DLQ). Máx %d ' +
      'tentativas.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FWorkQueue,
       FRetryQueue, FBackoffMs, FDlqQueue, FMaxAttempts]));
    SetConnected(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      SetConnected(False);
    end;
  end;
end;

end.
