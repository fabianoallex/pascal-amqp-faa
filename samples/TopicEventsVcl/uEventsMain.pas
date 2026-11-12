unit uEventsMain;

{ Pub/sub with a TOPIC exchange: events published with a hierarchical routing
  key (e.g. 'nota.aprovada', 'pdv.7.venda') reach EVERY subscriber whose
  binding key matches: '*' matches exactly one word, '#' matches zero or
  more. Unlike the work queues of the other samples (each message goes to
  ONE consumer), here each subscriber has its OWN queue (exclusive +
  auto-delete, disposable) bound to the exchange by the binding key: one event
  can be delivered to several subscribers at once, or to none.

  The screen lets you create subscribers dynamically (free binding key) and
  publish events with any routing key; the event list shows WHO received
  WHAT (the same event appears once per matching subscriber). The publish
  uses mandatory + OnBasicReturn: an event that matches no subscriber is
  returned by the broker and shows up in the log; pub/sub is fire-and-forget,
  and without a subscriber the event is lost (on purpose).

  Removing a subscriber undoes in this order: Cancel (stops the deliveries),
  Unbind (removes the binding and drops it from the recovery topology) and
  DeleteQueue (deletes the queue and removes the recorded declare); without it,
  a reconnection would replay a bind for a missing queue (404 on the channel).
  It is also the only sample that exercises DeclareExchange/BindQueue/UnbindQueue.

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
  Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  { One subscriber: its own exclusive queue bound to the exchange by the binding
    key. Created and destroyed ONLY on the UI thread; the map by consumer tag is
    read only in the marshals (also on the UI thread). }
  TSubscriber = class
    Number: Integer;
    BindingKey: string;
    SubscriberQueue: string;
    Tag: string;
    Received: Integer;
    Item: TListItem;
  end;

  TfrmEvents = class(TForm)
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
    lblExchange: TLabel;
    edtExchange: TEdit;
    lblRoutingKey: TLabel;
    edtRoutingKey: TEdit;
    lblMessage: TLabel;
    edtMessage: TEdit;
    btnPublish: TButton;
    gbSubscribe: TGroupBox;
    lblBindingKey: TLabel;
    edtBindingKey: TEdit;
    btnSubscribe: TButton;
    btnRemove: TButton;
    lvSubscribers: TListView;
    lvEvents: TListView;
    btnClearList: TButton;
    btnClearLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnPublishClick(Sender: TObject);
    procedure btnSubscribeClick(Sender: TObject);
    procedure btnRemoveClick(Sender: TObject);
    procedure btnClearListClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FExchange: string;
    FNextNumber: Integer;
    // Subscribers by consumer tag; touched ONLY on the UI thread.
    FSubscribers: TDictionary<string, TSubscriber>;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConnected(AConnected: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RemoveSubscriber(ASubscriber: TSubscriber);
    procedure RemoveAllSubscribers;
    // UI updates (UI thread, through marshals):
    procedure EventReceived(const ATag, ARoutingKey, ABody: string);
    procedure EventReturned(const ARoutingKey, ABody: string);
    procedure ConnectionDropped;
    procedure ConnectionRestored;
    procedure ConnectionFailed;
    // Lib callbacks (pool / reconnection threads):
    procedure OnEvent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnReturned(AChannel: TAMQPChannel; const AReturned: TAMQPReturnedMessage);
    procedure OnConnDisconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnectFailed(AConnection: TAMQPConnection);
  end;

var
  frmEvents: TfrmEvents;

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

function NewSuffix: string;
var
  LGuid: TGUID;
begin
  CreateGUID(LGuid);
  Result := Copy(GUIDToString(LGuid), 2, 8);
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See BackOfficeVcl.
  TLogMarshal = class
    Form: TfrmEvents;
    LogText: string;
    procedure Execute;
  end;

  TEventMarshal = class
    Form: TfrmEvents;
    Tag, RoutingKey, BodyText: string;
    procedure Execute;
  end;

  TReturnedEventMarshal = class
    Form: TfrmEvents;
    RoutingKey, BodyText: string;
    procedure Execute;
  end;

  TConnectionEvent = (ceDropped, ceRestored, ceFailed);

  TConnectionMarshal = class
    Form: TfrmEvents;
    Event: TConnectionEvent;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConnectionEventWork = class(TPcWorkItem)
  private
    FForm: TfrmEvents;
    FEvent: TConnectionEvent;
  public
    constructor Create(AForm: TfrmEvents; AEvent: TConnectionEvent);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(LogText);
  Free;
end;

procedure TEventMarshal.Execute;
begin
  Form.EventReceived(Tag, RoutingKey, BodyText);
  Free;
end;

procedure TReturnedEventMarshal.Execute;
begin
  Form.EventReturned(RoutingKey, BodyText);
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

constructor TConnectionEventWork.Create(AForm: TfrmEvents; AEvent: TConnectionEvent);
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

{ TfrmEvents }

procedure TfrmEvents.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FSubscribers := TDictionary<string, TSubscriber>.Create;
  SetConnected(False);
end;

procedure TfrmEvents.FormDestroy(Sender: TObject);
var
  LSubscriber: TSubscriber;
begin
  for LSubscriber in FSubscribers.Values do
    LSubscriber.Free;
  FSubscribers.Free;
end;

procedure TfrmEvents.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  // Only cancels the consumers (the queues are exclusive + auto-delete: they die
  // with the connection). The full undo is the Remove button's job.
  if Assigned(FChannel) then
    RemoveAllSubscribers;
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

function TfrmEvents.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmEvents.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmEvents.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmEvents.btnClearListClick(Sender: TObject);
begin
  lvEvents.Items.Clear;
end;

procedure TfrmEvents.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmEvents.BuildParams: TAMQPConnectionParams;
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
  Result.ConnectionName := 'EventosTopicVcl';
end;

procedure TfrmEvents.SetConnected(AConnected: Boolean);
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
  edtExchange.Enabled := not AConnected;
  btnPublish.Enabled := AConnected;
  btnSubscribe.Enabled := AConnected;
  btnRemove.Enabled := AConnected;
end;

{ --- subscribers ------------------------------------------------------------ }

procedure TfrmEvents.btnSubscribeClick(Sender: TObject);
var
  LBind: string;
  LSubscriber: TSubscriber;
  LDeclare: TAMQPQueueDeclare;
  LQueueBind: TAMQPQueueBind;
begin
  if not Assigned(FChannel) then
    Exit;
  LBind := Trim(edtBindingKey.Text);
  if LBind = '' then
  begin
    Log('Informe a binding key (ex.: nota.#, nota.aprovada, pdv.*.venda).');
    Exit;
  end;

  LSubscriber := TSubscriber.Create;
  try
    Inc(FNextNumber);
    LSubscriber.Number := FNextNumber;
    LSubscriber.BindingKey := LBind;
    LSubscriber.SubscriberQueue := Format('eventos-sub%d-%s', [LSubscriber.Number, NewSuffix]);

    // The subscriber's disposable queue: exclusive (this connection only) and
    // auto-delete (dies when the consumer cancels / the connection drops).
    LDeclare := TAMQPQueueDeclare.Create(LSubscriber.SubscriberQueue, False);
    LDeclare.Exclusive := True;
    LDeclare.AutoDelete := True;
    FChannel.DeclareQueue(LDeclare);

    LQueueBind := Default(TAMQPQueueBind);
    LQueueBind.QueueName := LSubscriber.SubscriberQueue;
    LQueueBind.ExchangeName := FExchange;
    LQueueBind.RoutingKey := LBind; // binding key ('*' = 1 word; '#' = 0+)
    FChannel.BindQueue(LQueueBind);

    LSubscriber.Tag := FChannel.Consume(LSubscriber.SubscriberQueue, OnEvent, True); // NoAck
  except
    on E: Exception do
    begin
      Log('Erro ao assinar "' + LBind + '": ' + E.Message);
      LSubscriber.Free;
      Exit;
    end;
  end;

  LSubscriber.Item := lvSubscribers.Items.Add;
  LSubscriber.Item.Caption := IntToStr(LSubscriber.Number);
  LSubscriber.Item.SubItems.Add(LSubscriber.BindingKey);
  LSubscriber.Item.SubItems.Add(LSubscriber.SubscriberQueue);
  LSubscriber.Item.SubItems.Add('0');
  LSubscriber.Item.Data := LSubscriber;
  FSubscribers.Add(LSubscriber.Tag, LSubscriber);
  Log(Format('Assinante %d criado: binding "%s" na fila "%s".',
    [LSubscriber.Number, LSubscriber.BindingKey, LSubscriber.SubscriberQueue]));
end;

// Undoes in the reverse order of creation, and reconciles the recovery topology:
// without the Unbind/DeleteQueue, a reconnection would replay the recorded bind
// for a queue that no longer exists (a 404 would close the channel mid-recovery).
procedure TfrmEvents.RemoveSubscriber(ASubscriber: TSubscriber);
var
  LUnbind: TAMQPQueueUnbind;
  LDelete: TAMQPQueueDelete;
begin
  try
    FChannel.Cancel(ASubscriber.Tag); // stops the deliveries (and clears the consumer from recovery)
    LUnbind := Default(TAMQPQueueUnbind);
    LUnbind.QueueName := ASubscriber.SubscriberQueue;
    LUnbind.ExchangeName := FExchange;
    LUnbind.RoutingKey := ASubscriber.BindingKey;
    FChannel.UnbindQueue(LUnbind);   // undoes the binding (and removes it from recovery)
    LDelete := Default(TAMQPQueueDelete);
    LDelete.QueueName := ASubscriber.SubscriberQueue;
    FChannel.DeleteQueue(LDelete);   // deletes the queue (and removes the declare from recovery)
  except
    on E: Exception do
      Log('Erro ao remover assinante ' + IntToStr(ASubscriber.Number) + ': ' + E.Message);
  end;
  FSubscribers.Remove(ASubscriber.Tag);
  if ASubscriber.Item <> nil then
    ASubscriber.Item.Delete;
  Log(Format('Assinante %d removido (binding "%s").',
    [ASubscriber.Number, ASubscriber.BindingKey]));
  ASubscriber.Free;
end;

procedure TfrmEvents.btnRemoveClick(Sender: TObject);
begin
  if not Assigned(FChannel) then
    Exit;
  if (lvSubscribers.Selected = nil) or (lvSubscribers.Selected.Data = nil) then
  begin
    Log('Selecione um assinante na lista para remover.');
    Exit;
  end;
  RemoveSubscriber(TSubscriber(lvSubscribers.Selected.Data));
end;

// On shutdown/disconnect: only cancels the consumers (drains the deliveries in
// flight); the exclusive/auto-delete queues die on their own with the connection.
procedure TfrmEvents.RemoveAllSubscribers;
var
  LSubscriber: TSubscriber;
  LList: TArray<TSubscriber>;
begin
  LList := FSubscribers.Values.ToArray;
  for LSubscriber in LList do
  begin
    try
      FChannel.Cancel(LSubscriber.Tag);
    except
    end;
    FSubscribers.Remove(LSubscriber.Tag);
    LSubscriber.Free;
  end;
  lvSubscribers.Items.Clear;
end;

{ --- events ----------------------------------------------------------------- }

// Runs on a pool thread; one event fires this callback once PER matched
// subscriber (each has its own queue and consumer).
procedure TfrmEvents.OnEvent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TEventMarshal;
begin
  LMarshal := TEventMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Tag := ADelivery.ConsumerTag; // identifies the subscriber
  LMarshal.RoutingKey := ADelivery.RoutingKey;
  LMarshal.BodyText := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmEvents.EventReceived(const ATag, ARoutingKey, ABody: string);
var
  LSubscriber: TSubscriber;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not FSubscribers.TryGetValue(ATag, LSubscriber) then
    Exit; // late delivery for a subscriber already removed: drop it silently

  Inc(LSubscriber.Received);
  if LSubscriber.Item <> nil then
    LSubscriber.Item.SubItems[2] := IntToStr(LSubscriber.Received);

  LAtBottom := ScrollAtBottom(lvEvents.Handle);
  LItem := lvEvents.Items.Add;
  LItem.Caption := FormatDateTime('hh:nn:ss.zzz', Now);
  LItem.SubItems.Add(Format('%d ("%s")', [LSubscriber.Number, LSubscriber.BindingKey]));
  LItem.SubItems.Add(ARoutingKey);
  LItem.SubItems.Add(ABody);
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmEvents.OnReturned(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
var
  LMarshal: TReturnedEventMarshal;
begin
  LMarshal := TReturnedEventMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.RoutingKey := AReturned.RoutingKey;
  LMarshal.BodyText := AReturned.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmEvents.EventReturned(const ARoutingKey, ABody: string);
begin
  Log(Format('Evento "%s" (rk=%s) DEVOLVIDO: nenhum assinante com binding ' +
    'que case — pub/sub sem assinante perde o evento.', [ABody, ARoutingKey]));
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmEvents.btnPublishClick(Sender: TObject);
var
  LRk, LMsgBody: string;
  LProps: TAMQPBasicProperties;
begin
  if not Assigned(FChannel) then
    Exit;
  LRk := Trim(edtRoutingKey.Text);
  if LRk = '' then
  begin
    Log('Informe a routing key (ex.: nota.aprovada).');
    Exit;
  end;
  LMsgBody := Trim(edtMessage.Text);
  if LMsgBody = '' then
    LMsgBody := 'EVT-' + FormatDateTime('hhnnsszzz', Now);
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  try
    // mandatory=True: with NO matching binding, the broker returns the event
    // (Basic.Return) instead of dropping it silently, so it is visible in the log.
    FChannel.Publish(FExchange, LRk, AmqpUtf8Encode(LMsgBody), LProps, True);
    Log(Format('Publicado "%s" com routing key "%s".', [LMsgBody, LRk]));
  except
    on E: Exception do
      Log('Erro ao publicar: ' + E.Message);
  end;
end;

{ --- connection events ------------------------------------------------------ }

procedure TfrmEvents.ConnectionDropped;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; filas exclusivas e ' +
    'bindings serão recriados no recovery.');
end;

procedure TfrmEvents.ConnectionRestored;
begin
  SetConnected(True);
  Log('Reconectado: exchange, filas, bindings e consumers restaurados.');
end;

procedure TfrmEvents.ConnectionFailed;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConnectionEventWork).
procedure TfrmEvents.OnConnDisconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceDropped));
end;

procedure TfrmEvents.OnConnReconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceRestored));
end;

procedure TfrmEvents.OnConnReconnectFailed(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceFailed));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmEvents.btnConnectClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
      RemoveAllSubscribers;
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      Log('Desconectado.');
    except
      on E: Exception do
        Log('Erro ao desconectar: ' + E.Message);
    end;
    SetConnected(False);
    Exit;
  end;

  LParams := BuildParams;
  FExchange := Trim(edtExchange.Text);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnConnDisconnected;
    FConn.OnReconnect := OnConnReconnected;
    FConn.OnReconnectFailed := OnConnReconnectFailed;
    FConn.Open;

    FChannel := FConn.CreateChannel;
    FChannel.OnBasicReturn := OnReturned;
    FChannel.DeclareExchange(TAMQPExchangeDeclare.Create(FExchange,
      AMQP_EXCHANGE_TYPE_TOPIC));

    Log(Format('Conectado a %s:%d%s. Exchange topic "%s" declarado.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FExchange]));
    Log('Dica: crie assinantes com bindings diferentes (nota.#, ' +
      'nota.aprovada, pdv.*.venda) e publique routing keys variadas — ' +
      'o mesmo evento chega a todo assinante que casar.');
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
