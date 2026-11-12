unit uHeadersMain;

{ Pub/sub with a HEADERS exchange: routing does not use the routing key but the
  MATCHING of the message's header fields against criteria declared on each
  binding. Each binding carries an `x-match` argument:
    - `all`: the message matches if ALL the criterion's fields match (logical AND);
    - `any`: it matches if ANY of them matches (logical OR).
  The broker ignores the routing key on this exchange type.

  It is the counterpart of TopicEventsVcl (routing by hierarchical routing key):
  here the subscriber filters by independent attributes of the event; in the
  example, `tipo` (nfe/nfce/cte) and `regiao` (sul/sudeste/...). A subscriber "all,
  tipo=nfe, regiao=sul" only receives invoices from the south; an "any, tipo=nfe,
  regiao=sul" receives any nfe OR any event from the south.

  The only sample that exercises:
    - *binding arguments*: the `TAMQPFieldTable` in BindQueue (x-match + criteria),
      unlike RetryDlqVcl, which uses Arguments on the queue DECLARE;
    - the message's Headers property (SetHeaders on publish, and reading the
      headers on delivery).

  As with topic, each subscriber has its OWN queue (exclusive + auto-delete)
  bound to the exchange, and the same event may reach several subscribers or
  none (mandatory + OnBasicReturn makes the drop visible). Removing a
  subscriber undoes in the order Cancel -> Unbind -> DeleteQueue, reconciling the
  recovery topology (Unbind must repeat the SAME arguments as the bind: on a
  headers exchange the argument identifies the binding, not the routing key).

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
  Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  { One subscriber: its own exclusive queue bound to the exchange by a binding with
    arguments (x-match + criteria). The criteria are kept in plain fields
    (not in a live TAMQPFieldTable) to rebuild the identical table
    on unbind. Created/destroyed ONLY on the UI thread. }
  TSubscriber = class
    Number: Integer;
    Match: string;      // 'all' ou 'any'
    CritType: string;    // '' = not part of the criterion
    CritRegion: string;  // '' = not part of the criterion
    SubscriberQueue: string;
    Tag: string;
    Received: Integer;
    Item: TListItem;
  end;

  TfrmHeaders = class(TForm)
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
    lblType: TLabel;
    edtType: TEdit;
    lblRegion: TLabel;
    edtRegion: TEdit;
    lblMessage: TLabel;
    edtMessage: TEdit;
    btnPublish: TButton;
    gbSubscribe: TGroupBox;
    lblMatch: TLabel;
    cmbMatch: TComboBox;
    lblCritType: TLabel;
    edtCritType: TEdit;
    lblCritRegion: TLabel;
    edtCritRegion: TEdit;
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
    procedure EventReceived(const ATag, AType, ARegion, ABody: string);
    procedure EventReturned(const AType, ARegion, ABody: string);
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
  frmHeaders: TfrmHeaders;

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

// Readable text of a binding's criteria (e.g. 'tipo=nfe, regiao=sul').
function DescribeCriteria(const AType, ARegion: string): string;
begin
  Result := '';
  if AType <> '' then
    Result := 'tipo=' + AType;
  if ARegion <> '' then
  begin
    if Result <> '' then
      Result := Result + ', ';
    Result := Result + 'regiao=' + ARegion;
  end;
  if Result = '' then
    Result := '(sem critério)';
end;

// Builds the binding's argument table: x-match + the non-empty criteria.
// The caller OWNS the table and frees it after the bind/unbind. The Puts are
// separate statements on purpose (chaining that ends in an inline
// TValue.From<T> gives an internal error on FPC 3.2; gotcha in CLAUDE.md).
function BuildArguments(const AMatch, AType, ARegion: string): TAMQPFieldTable;
begin
  Result := TAMQPFieldTable.Create;
  Result.Put('x-match', TValue.From<string>(AMatch));
  if AType <> '' then
    Result.Put('tipo', TValue.From<string>(AType));
  if ARegion <> '' then
    Result.Put('regiao', TValue.From<string>(ARegion));
end;

// Reads a string header from the delivery's table. The value is assigned to a
// local TValue BEFORE .AsString: chaining .AsString straight on the indexer/result
// triggers an internal error on FPC 3.2 (gotcha in CLAUDE.md).
function ReadHeader(AHeaders: TAMQPFieldTable; const AKey: string): string;
var
  LValue: TValue;
begin
  Result := '';
  if (AHeaders <> nil) and AHeaders.TryGetValue(AKey, LValue) then
    Result := LValue.AsString;
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See BackOfficeVcl.
  TLogMarshal = class
    Form: TfrmHeaders;
    LogText: string;
    procedure Execute;
  end;

  TEventMarshal = class
    Form: TfrmHeaders;
    Tag, Kind, Region, BodyText: string;
    procedure Execute;
  end;

  TReturnedEventMarshal = class
    Form: TfrmHeaders;
    Kind, Region, BodyText: string;
    procedure Execute;
  end;

  TConnectionEvent = (ceDropped, ceRestored, ceFailed);

  TConnectionMarshal = class
    Form: TfrmHeaders;
    Event: TConnectionEvent;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConnectionEventWork = class(TPcWorkItem)
  private
    FForm: TfrmHeaders;
    FEvent: TConnectionEvent;
  public
    constructor Create(AForm: TfrmHeaders; AEvent: TConnectionEvent);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(LogText);
  Free;
end;

procedure TEventMarshal.Execute;
begin
  Form.EventReceived(Tag, Kind, Region, BodyText);
  Free;
end;

procedure TReturnedEventMarshal.Execute;
begin
  Form.EventReturned(Kind, Region, BodyText);
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

constructor TConnectionEventWork.Create(AForm: TfrmHeaders; AEvent: TConnectionEvent);
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

{ TfrmHeaders }

procedure TfrmHeaders.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  cmbMatch.ItemIndex := 0; // 'all'
  FSubscribers := TDictionary<string, TSubscriber>.Create;
  SetConnected(False);
end;

procedure TfrmHeaders.FormDestroy(Sender: TObject);
var
  LSubscriber: TSubscriber;
begin
  for LSubscriber in FSubscribers.Values do
    LSubscriber.Free;
  FSubscribers.Free;
end;

procedure TfrmHeaders.FormClose(Sender: TObject; var Action: TCloseAction);
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

function TfrmHeaders.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmHeaders.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmHeaders.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmHeaders.btnClearListClick(Sender: TObject);
begin
  lvEvents.Items.Clear;
end;

procedure TfrmHeaders.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmHeaders.BuildParams: TAMQPConnectionParams;
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
  Result.ConnectionName := 'EventosHeadersVcl';
end;

procedure TfrmHeaders.SetConnected(AConnected: Boolean);
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

procedure TfrmHeaders.btnSubscribeClick(Sender: TObject);
var
  LSubscriber: TSubscriber;
  LDeclare: TAMQPQueueDeclare;
  LQueueBind: TAMQPQueueBind;
  LArgs: TAMQPFieldTable;
begin
  if not Assigned(FChannel) then
    Exit;

  LSubscriber := TSubscriber.Create;
  try
    Inc(FNextNumber);
    LSubscriber.Number := FNextNumber;
    LSubscriber.Match := cmbMatch.Text;               // 'all' ou 'any'
    LSubscriber.CritType := Trim(edtCritType.Text);
    LSubscriber.CritRegion := Trim(edtCritRegion.Text);
    LSubscriber.SubscriberQueue := Format('headers-sub%d-%s', [LSubscriber.Number, NewSuffix]);

    // The subscriber's disposable queue: exclusive (this connection only) and
    // auto-delete (dies when the consumer cancels / the connection drops).
    LDeclare := TAMQPQueueDeclare.Create(LSubscriber.SubscriberQueue, False);
    LDeclare.Exclusive := True;
    LDeclare.AutoDelete := True;
    FChannel.DeclareQueue(LDeclare);

    // Binding WITHOUT a routing key (ignored on a headers exchange): what decides is
    // the argument table (x-match + criteria). The caller owns the table.
    LArgs := BuildArguments(LSubscriber.Match, LSubscriber.CritType, LSubscriber.CritRegion);
    try
      LQueueBind := Default(TAMQPQueueBind);
      LQueueBind.QueueName := LSubscriber.SubscriberQueue;
      LQueueBind.ExchangeName := FExchange;
      LQueueBind.RoutingKey := '';
      LQueueBind.Arguments := LArgs;
      FChannel.BindQueue(LQueueBind);
    finally
      LArgs.Free;
    end;

    LSubscriber.Tag := FChannel.Consume(LSubscriber.SubscriberQueue, OnEvent, True); // NoAck
  except
    on E: Exception do
    begin
      Log('Erro ao assinar (' + LSubscriber.Match + ', ' +
        DescribeCriteria(LSubscriber.CritType, LSubscriber.CritRegion) + '): ' + E.Message);
      LSubscriber.Free;
      Exit;
    end;
  end;

  LSubscriber.Item := lvSubscribers.Items.Add;
  LSubscriber.Item.Caption := IntToStr(LSubscriber.Number);
  LSubscriber.Item.SubItems.Add(LSubscriber.Match);
  LSubscriber.Item.SubItems.Add(DescribeCriteria(LSubscriber.CritType, LSubscriber.CritRegion));
  LSubscriber.Item.SubItems.Add(LSubscriber.SubscriberQueue);
  LSubscriber.Item.SubItems.Add('0');
  LSubscriber.Item.Data := LSubscriber;
  FSubscribers.Add(LSubscriber.Tag, LSubscriber);
  Log(Format('Assinante %d criado: x-match=%s, %s (fila "%s").',
    [LSubscriber.Number, LSubscriber.Match,
     DescribeCriteria(LSubscriber.CritType, LSubscriber.CritRegion), LSubscriber.SubscriberQueue]));
  if (LSubscriber.Match = 'any') and (LSubscriber.CritType = '') and (LSubscriber.CritRegion = '') then
    Log('Aviso: x-match=any sem nenhum critério não casa com nada — nenhum evento chegará.');
end;

// Undoes in the reverse order of creation, and reconciles the recovery topology:
// without the Unbind/DeleteQueue, a reconnection would replay the recorded bind
// for a queue that no longer exists (a 404 would close the channel in the middle
// of recovery). On a headers exchange the Unbind must repeat the SAME arguments
// as the bind (the argument, not the routing key, identifies the binding).
procedure TfrmHeaders.RemoveSubscriber(ASubscriber: TSubscriber);
var
  LUnbind: TAMQPQueueUnbind;
  LDelete: TAMQPQueueDelete;
  LArgs: TAMQPFieldTable;
begin
  try
    FChannel.Cancel(ASubscriber.Tag); // stops the deliveries (and clears the consumer from recovery)
    LArgs := BuildArguments(ASubscriber.Match, ASubscriber.CritType, ASubscriber.CritRegion);
    try
      LUnbind := Default(TAMQPQueueUnbind);
      LUnbind.QueueName := ASubscriber.SubscriberQueue;
      LUnbind.ExchangeName := FExchange;
      LUnbind.RoutingKey := '';
      LUnbind.Arguments := LArgs;      // must match the bind's
      FChannel.UnbindQueue(LUnbind);   // undoes the binding (and removes it from recovery)
    finally
      LArgs.Free;
    end;
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
  Log(Format('Assinante %d removido (x-match=%s, %s).',
    [ASubscriber.Number, ASubscriber.Match,
     DescribeCriteria(ASubscriber.CritType, ASubscriber.CritRegion)]));
  ASubscriber.Free;
end;

procedure TfrmHeaders.btnRemoveClick(Sender: TObject);
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
procedure TfrmHeaders.RemoveAllSubscribers;
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
// subscriber (each has its own queue and consumer). The headers come in the
// delivery's properties, read HERE, before the delivery is released.
procedure TfrmHeaders.OnEvent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TEventMarshal;
begin
  LMarshal := TEventMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Tag := ADelivery.ConsumerTag; // identifies the subscriber
  LMarshal.Kind := ReadHeader(ADelivery.Properties.Headers, 'tipo');
  LMarshal.Region := ReadHeader(ADelivery.Properties.Headers, 'regiao');
  LMarshal.BodyText := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmHeaders.EventReceived(const ATag, AType, ARegion, ABody: string);
var
  LSubscriber: TSubscriber;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not FSubscribers.TryGetValue(ATag, LSubscriber) then
    Exit; // late delivery for a subscriber already removed: drop it silently

  Inc(LSubscriber.Received);
  if LSubscriber.Item <> nil then
    LSubscriber.Item.SubItems[3] := IntToStr(LSubscriber.Received);

  LAtBottom := ScrollAtBottom(lvEvents.Handle);
  LItem := lvEvents.Items.Add;
  LItem.Caption := FormatDateTime('hh:nn:ss.zzz', Now);
  LItem.SubItems.Add(Format('%d (%s: %s)',
    [LSubscriber.Number, LSubscriber.Match,
     DescribeCriteria(LSubscriber.CritType, LSubscriber.CritRegion)]));
  LItem.SubItems.Add(AType);
  LItem.SubItems.Add(ARegion);
  LItem.SubItems.Add(ABody);
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmHeaders.OnReturned(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
var
  LMarshal: TReturnedEventMarshal;
begin
  LMarshal := TReturnedEventMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Kind := ReadHeader(AReturned.Properties.Headers, 'tipo');
  LMarshal.Region := ReadHeader(AReturned.Properties.Headers, 'regiao');
  LMarshal.BodyText := AReturned.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmHeaders.EventReturned(const AType, ARegion, ABody: string);
begin
  Log(Format('Evento "%s" (tipo=%s, regiao=%s) DEVOLVIDO: nenhum binding casou ' +
    '— pub/sub sem assinante perde o evento.',
    [ABody, AType, ARegion]));
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmHeaders.btnPublishClick(Sender: TObject);
var
  LType, LRegion, LMsgBody: string;
  LProps: TAMQPBasicProperties;
  LHeaders: TAMQPFieldTable;
begin
  if not Assigned(FChannel) then
    Exit;
  LType := Trim(edtType.Text);
  LRegion := Trim(edtRegion.Text);
  LMsgBody := Trim(edtMessage.Text);
  if LMsgBody = '' then
    LMsgBody := 'EVT-' + FormatDateTime('hhnnsszzz', Now);

  // The message headers are what the broker matches against the bindings'
  // criteria. The caller owns the table (SetHeaders does not copy it); it is
  // freed after the publish, which serializes the frame synchronously.
  LHeaders := TAMQPFieldTable.Create;
  try
    if LType <> '' then
      LHeaders.Put('tipo', TValue.From<string>(LType));
    if LRegion <> '' then
      LHeaders.Put('regiao', TValue.From<string>(LRegion));

    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetHeaders(LHeaders);
    try
      // empty routing key (ignored on a headers exchange). mandatory=True: with
      // NO matching binding, the broker returns the event (Basic.Return) instead
      // of dropping it silently, so it is visible in the log.
      FChannel.Publish(FExchange, '', AmqpUtf8Encode(LMsgBody), LProps, True);
      Log(Format('Publicado "%s" com headers tipo=%s, regiao=%s.',
        [LMsgBody, LType, LRegion]));
    except
      on E: Exception do
        Log('Erro ao publicar: ' + E.Message);
    end;
  finally
    LHeaders.Free;
  end;
end;

{ --- connection events ------------------------------------------------------ }

procedure TfrmHeaders.ConnectionDropped;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; filas exclusivas e ' +
    'bindings serão recriados no recovery.');
end;

procedure TfrmHeaders.ConnectionRestored;
begin
  SetConnected(True);
  Log('Reconectado: exchange, filas, bindings e consumers restaurados.');
end;

procedure TfrmHeaders.ConnectionFailed;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConnectionEventWork).
procedure TfrmHeaders.OnConnDisconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceDropped));
end;

procedure TfrmHeaders.OnConnReconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceRestored));
end;

procedure TfrmHeaders.OnConnReconnectFailed(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceFailed));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmHeaders.btnConnectClick(Sender: TObject);
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
      AMQP_EXCHANGE_TYPE_HEADERS));

    Log(Format('Conectado a %s:%d%s. Exchange headers "%s" declarado.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FExchange]));
    Log('Dica: crie assinantes com x-match all/any e critérios variados ' +
      '(tipo, regiao) e publique eventos com headers diferentes — o mesmo ' +
      'evento chega a todo assinante cujo critério casar.');
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
