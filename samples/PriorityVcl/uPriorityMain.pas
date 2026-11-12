unit uPriorityMain;

{ PRIORITY queue: a queue declared with the `x-max-priority` argument
  delivers the highest-priority messages first (`Priority` property,
  0..N), jumping ahead of lower-priority messages that are still
  waiting, regardless of arrival order.

  The detail that decides whether the demo works: reordering only happens
  among messages that are in the queue TOGETHER at the moment the broker picks
  the next one. That is why the script is:
    1. leave the consumer STOPPED and publish a batch of mixed priorities
       (builds the backlog);
    2. only then start the SLOW consumer: the messages come out in descending
       priority order, not in the order they were published.

  Two conditions are essential to see the effect, and the sample has both:
    - the consumer needs a LOW `Qos(prefetch)` (1): with a high prefetch the
      broker dumps a batch into the client buffer at once and priority does not
      reorder what has already left the queue. The Prefetch field is editable on
      purpose: raise it to, say, 20 and watch the priority ordering vanish;
    - the consumer must be slower than the producer (delay per message +
      manual ack), otherwise it empties the queue without ever building a backlog.

  Connecting recreates the queue from scratch (DeleteQueue → DeclareQueue):
  redeclaring an existing queue with a different `x-max-priority` is
  PRECONDITION_FAILED (same reason as in RetryDlqVcl). It is the sample that
  exercises the `Priority` property (`SetPriority` on publish, read on delivery).

  Compiles in both worlds from the SAME source (GUI samples pattern):
  named callbacks ('of object'), disposable marshals + TThread.Queue for
  the UI (see BackOfficeVcl) and connection events hopping through PcPool (the
  dropped TThread.Queue gotcha; see TConnectionEventWork and CLAUDE.md). }

interface

uses
  // On FPC, the LCL emulation layer (LCLIntf/LCLType/LMessages) covers the
  // WinAPI calls of the log autoscroll on any widgetset (win32, gtk2...).
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes, Rtti,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  TfrmPriority = class(TForm)
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
    gbPublish: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblMaxPrio: TLabel;
    edtMaxPrio: TEdit;
    lblPrio: TLabel;
    edtPrio: TEdit;
    lblMessage: TLabel;
    edtMessage: TEdit;
    btnPublish: TButton;
    btnBurst: TButton;
    gbConsumer: TGroupBox;
    lblDelay: TLabel;
    edtDelay: TEdit;
    lblPrefetch: TLabel;
    edtPrefetch: TEdit;
    lblConsumed: TLabel;
    btnConsumer: TButton;
    lvMessages: TListView;
    btnClearList: TButton;
    btnClearLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnConnectClick(Sender: TObject);
    procedure btnPublishClick(Sender: TObject);
    procedure btnBurstClick(Sender: TObject);
    procedure btnConsumerClick(Sender: TObject);
    procedure btnClearListClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;      // producer channel (declare + publish)
    FConsumeChannel: TAMQPChannel; // separate consumer channel (its own Qos)
    FQueue: string;
    FMaxPrio: Integer;
    FConsumerTag: string;      // '' = consumer stopped
    FDelayMs: Integer;           // consumer delay per message
    FConsumed: Integer;
    FOutputSeq: Integer;          // OUTPUT order (draining)
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConnected(AConnected: Boolean);
    procedure SetConsuming(AConsuming: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure PublishOne(APriority: Integer; const ABody: string);
    // UI updates (UI thread, through marshals):
    procedure MessageConsumed(APriority: Integer; const ABody: string);
    procedure ConnectionDropped;
    procedure ConnectionRestored;
    procedure ConnectionFailed;
    // Lib callbacks (pool / reconnection threads):
    procedure OnMessage(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnConnDisconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnectFailed(AConnection: TAMQPConnection);
  end;

var
  frmPriority: TfrmPriority;

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
  // Priorities of the "batch": deliberately out of order, so the drain comes out
  // in descending order, quite different from the publish order.
  cBatchPriorities: array[0..9] of Integer = (2, 8, 5, 9, 1, 7, 3, 9, 0, 6);

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See BackOfficeVcl.
  TLogMarshal = class
    Form: TfrmPriority;
    LogText: string;
    procedure Execute;
  end;

  TConsumeMarshal = class
    Form: TfrmPriority;
    MsgPriority: Integer;
    BodyText: string;
    procedure Execute;
  end;

  TConnectionEvent = (ceDropped, ceRestored, ceFailed);

  TConnectionMarshal = class
    Form: TfrmPriority;
    Event: TConnectionEvent;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConnectionEventWork = class(TPcWorkItem)
  private
    FForm: TfrmPriority;
    FEvent: TConnectionEvent;
  public
    constructor Create(AForm: TfrmPriority; AEvent: TConnectionEvent);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(LogText);
  Free;
end;

procedure TConsumeMarshal.Execute;
begin
  Form.MessageConsumed(MsgPriority, BodyText);
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

constructor TConnectionEventWork.Create(AForm: TfrmPriority; AEvent: TConnectionEvent);
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

{ TfrmPriority }

procedure TfrmPriority.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  SetConnected(False);
end;

procedure TfrmPriority.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(FConsumeChannel) and (FConsumerTag <> '') then
    try
      FConsumeChannel.Cancel(FConsumerTag);
    except
    end;
  FreeAndNil(FConsumeChannel);
  FreeAndNil(FChannel);
  // The Free calls above drain the callbacks in flight; the marshals they posted
  // are still in the TThread.Queue queue, and pumping here drains them with the
  // form still alive (see BackOfficeVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread; a TConnectionEventWork
  // already queued on the pool may be posting the last marshal RIGHT NOW.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmPriority.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmPriority.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmPriority.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmPriority.btnClearListClick(Sender: TObject);
begin
  lvMessages.Items.Clear;
  FOutputSeq := 0;
end;

procedure TfrmPriority.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmPriority.BuildParams: TAMQPConnectionParams;
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
  Result.ConnectionName := 'PrioridadeVcl';
end;

procedure TfrmPriority.SetConnected(AConnected: Boolean);
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
  edtMaxPrio.Enabled := not AConnected;
  btnPublish.Enabled := AConnected;
  btnBurst.Enabled := AConnected;
  btnConsumer.Enabled := AConnected;
end;

procedure TfrmPriority.SetConsuming(AConsuming: Boolean);
begin
  if AConsuming then
    btnConsumer.Caption := 'Parar consumidor'
  else
    btnConsumer.Caption := 'Iniciar consumidor';
  edtDelay.Enabled := not AConsuming;
  edtPrefetch.Enabled := not AConsuming;
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmPriority.PublishOne(APriority: Integer; const ABody: string);
var
  LProps: TAMQPBasicProperties;
begin
  if APriority < 0 then
    APriority := 0;
  if APriority > FMaxPrio then
    APriority := FMaxPrio; // the broker also clamps to the cap, but we make it explicit
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LProps.SetPriority(Byte(APriority));
  // Publishes straight to the queue through the default exchange (routing key = queue name).
  FChannel.Publish('', FQueue, AmqpUtf8Encode(ABody), LProps);
end;

procedure TfrmPriority.btnPublishClick(Sender: TObject);
var
  LPrio: Integer;
  LMsgBody: string;
begin
  if not Assigned(FChannel) then
    Exit;
  LPrio := StrToIntDef(Trim(edtPrio.Text), 0);
  LMsgBody := Trim(edtMessage.Text);
  if LMsgBody = '' then
    LMsgBody := 'msg-' + FormatDateTime('hhnnsszzz', Now);
  try
    PublishOne(LPrio, LMsgBody);
    Log(Format('Publicada "%s" com prioridade %d.', [LMsgBody, LPrio]));
  except
    on E: Exception do
      Log('Erro ao publicar: ' + E.Message);
  end;
end;

procedure TfrmPriority.btnBurstClick(Sender: TObject);
var
  I, LPrio: Integer;
  LMsgBody, LMap: string;
begin
  if not Assigned(FChannel) then
    Exit;
  try
    LMap := '';
    for I := Low(cBatchPriorities) to High(cBatchPriorities) do
    begin
      LPrio := cBatchPriorities[I];
      // The body identifies the publish ORDER (1..N); when draining it comes out
      // shuffled, proving that priority is what rules.
      LMsgBody := Format('pub#%d', [I + 1]);
      PublishOne(LPrio, LMsgBody);
      // Records the composition in the log (pub#N=priority) for later checking
      // from the log text alone, without depending on the list.
      if LMap <> '' then
        LMap := LMap + ' ';
      LMap := LMap + Format('%s=p%d', [LMsgBody, LPrio]);
    end;
    Log(Format('Lote publicado (%d msgs, pub#N=prioridade): %s',
      [Length(cBatchPriorities), LMap]));
    Log('Inicie o consumidor lento e observe a saída em ordem decrescente de ' +
      'prioridade.');
  except
    on E: Exception do
      Log('Erro ao publicar o lote: ' + E.Message);
  end;
end;

{ --- consumer --------------------------------------------------------------- }

// Runs on a pool thread. With prefetch=1 and manual ack, the broker only delivers
// the NEXT message (the highest-priority one still in the queue) after the Ack;
// that is why the Sleep goes BEFORE the Ack: it holds the pace and keeps the backlog.
procedure TfrmPriority.OnMessage(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TConsumeMarshal;
begin
  LMarshal := TConsumeMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.MsgPriority := ADelivery.Properties.Priority;
  LMarshal.BodyText := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);

  Sleep(FDelayMs);                    // deliberately "slow" consumer
  AChannel.Ack(ADelivery.DeliveryTag);
end;

procedure TfrmPriority.MessageConsumed(APriority: Integer; const ABody: string);
var
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  Inc(FConsumed);
  lblConsumed.Caption := Format('Consumidas: %d', [FConsumed]);
  Inc(FOutputSeq);
  LAtBottom := ScrollAtBottom(lvMessages.Handle);
  LItem := lvMessages.Items.Add;
  LItem.Caption := IntToStr(FOutputSeq);
  LItem.SubItems.Add(IntToStr(APriority));
  LItem.SubItems.Add(ABody);
  LItem.SubItems.Add(FormatDateTime('hh:nn:ss.zzz', Now));
  if LAtBottom then
    LItem.MakeVisible(False);
  // Log of the OUTPUT order: this way the log text already shows the drain by
  // priority (output #N: priority P, body), without depending on the list.
  Log(Format('Consumida (saída #%d): prioridade %d — %s',
    [FOutputSeq, APriority, ABody]));
end;

procedure TfrmPriority.btnConsumerClick(Sender: TObject);
var
  LPrefetch: Integer;
begin
  if not Assigned(FConn) then
    Exit;

  if FConsumerTag <> '' then
  begin
    try
      FConsumeChannel.Cancel(FConsumerTag);
      Log('Consumidor parado.');
    except
      on E: Exception do
        Log('Erro ao parar o consumidor: ' + E.Message);
    end;
    FConsumerTag := '';
    SetConsuming(False);
    Exit;
  end;

  FDelayMs := StrToIntDef(Trim(edtDelay.Text), 600);
  if FDelayMs < 0 then
    FDelayMs := 0;
  LPrefetch := StrToIntDef(Trim(edtPrefetch.Text), 1);
  if LPrefetch < 1 then
    LPrefetch := 1;
  try
    FConsumeChannel.Qos(Word(LPrefetch));
    FConsumerTag := FConsumeChannel.Consume(FQueue, OnMessage); // manual ack
    Log(Format('Consumidor iniciado: prefetch=%d, delay=%d ms por mensagem.',
      [LPrefetch, FDelayMs]));
    if LPrefetch > 1 then
      Log('Aviso: prefetch > 1 despeja um lote no cliente de uma vez — a saída ' +
        'deixa de respeitar estritamente a prioridade. Use 1 para ver o efeito.');
    SetConsuming(True);
  except
    on E: Exception do
      Log('Erro ao iniciar o consumidor: ' + E.Message);
  end;
end;

{ --- connection events ------------------------------------------------------ }

procedure TfrmPriority.ConnectionDropped;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; a fila e o consumidor ' +
    'serão restaurados no recovery.');
end;

procedure TfrmPriority.ConnectionRestored;
begin
  SetConnected(True);
  if FConsumerTag <> '' then
    SetConsuming(True);
  Log('Reconectado: fila, argumentos e consumidor restaurados.');
end;

procedure TfrmPriority.ConnectionFailed;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConnectionEventWork).
procedure TfrmPriority.OnConnDisconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceDropped));
end;

procedure TfrmPriority.OnConnReconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceRestored));
end;

procedure TfrmPriority.OnConnReconnectFailed(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceFailed));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmPriority.btnConnectClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LDel: TAMQPQueueDelete;
begin
  if Assigned(FConn) then
  begin
    try
      if (FConsumeChannel <> nil) and (FConsumerTag <> '') then
        FConsumeChannel.Cancel(FConsumerTag);
      FConsumerTag := '';
      FreeAndNil(FConsumeChannel);
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
  FQueue := Trim(edtQueue.Text);
  FMaxPrio := StrToIntDef(Trim(edtMaxPrio.Text), 9);
  if FMaxPrio < 1 then
    FMaxPrio := 1;
  if FMaxPrio > 255 then
    FMaxPrio := 255; // AMQP cap (x-max-priority accepts up to 255; RabbitMQ recommends <= 10)
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnConnDisconnected;
    FConn.OnReconnect := OnConnReconnected;
    FConn.OnReconnectFailed := OnConnReconnectFailed;
    FConn.Open;

    FChannel := FConn.CreateChannel;

    // Recreates the queue from scratch: redeclaring an existing queue with a different
    // x-max-priority is PRECONDITION_FAILED. DeleteQueue reconciles the recovery
    // topology (see RetryDlqVcl).
    LDel := Default(TAMQPQueueDelete);
    LDel.QueueName := FQueue;
    FChannel.DeleteQueue(LDel);

    LArgs := TAMQPFieldTable.Create;
    try
      LArgs.Put('x-max-priority', TValue.From<Integer>(FMaxPrio));
      LDecl := TAMQPQueueDeclare.Create(FQueue, True); // durable; recreated on every connection
      LDecl.Arguments := LArgs;
      FChannel.DeclareQueue(LDecl);
    finally
      LArgs.Free;
    end;

    // Separate consumer channel: its Qos(prefetch) does not interfere with the
    // producer channel.
    FConsumeChannel := FConn.CreateChannel;
    FConsumed := 0;
    lblConsumed.Caption := 'Consumidas: 0';

    Log(Format('Conectado a %s:%d%s. Fila "%s" recriada com x-max-priority=%d.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FQueue, FMaxPrio]));
    Log('Roteiro: com o consumidor PARADO, clique "Publicar lote" (prioridades ' +
      'misturadas). Depois inicie o consumidor lento e veja a saída em ordem ' +
      'decrescente de prioridade.');
    SetConnected(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FConsumeChannel);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      SetConnected(False);
    end;
  end;
end;

end.
