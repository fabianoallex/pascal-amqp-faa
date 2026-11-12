unit uQueryMain;

{ RPC request/reply over AMQP: the client asks for an invoice's status and the
  server replies, the classic pattern with ReplyTo + CorrelationId.

  One window with BOTH roles, each optional:
  - Server: consumes the request queue (durable) and, for each query,
    simulates the lookup (random Sleep up to the configured maximum delay) and
    publishes the reply to the queue named by the request's ReplyTo, echoing
    the CorrelationId. Manual ack only after replying.
  - Client: consumes a REPLY queue (automatic ack) and, for each query,
    publishes the request with ReplyTo = that queue, CorrelationId = new GUID and
    Expiration = timeout (the
    broker drops requests that age in the queue past the timeout; there is no
    point in the server replying to someone who already gave up). The reply
    resolves the matching row in the list by CorrelationId, with the round-trip
    time; a TTimer expires queries without a reply, and a reply that arrives
    AFTER the timeout is dropped with a warning in the log (a design decision the
    RPC pattern always demands).

  You can run a single instance (both roles on the same connection, separate
  channels) or two instances, one as server, the other as client.
  Querying with no server running is instructive too: the request stays in the
  request queue until the TTL (Expiration) runs out, and the client reports a
  timeout.

  The "Usar Direct Reply-to" checkbox switches the client's reply mechanism
  between the two classic RPC patterns, side by side:

  - Unchecked (named queue): declares an exclusive + auto-delete reply queue
    with a FIXED name per instance ('consulta-resp-' + GUID), instead of a
    broker-generated name (declare with name ''). The reconnection topology
    replay resends the recorded declare; with name '' the broker would generate
    a NEW name and the recorded consumer would point at the old one, breaking
    the replay. With a fixed name, the declare+consume pair survives reconnection.

  - Checked (Direct Reply-to, 'amq.rabbitmq.reply-to'): RabbitMQ's magic
    pseudo-queue; NOTHING is declared. The client just consumes from it in no-ack
    before publishing; the broker routes the reply back by a fast path, with no
    real queue. It is the pattern the README recommends for RPC (less topology,
    and also immune to the recovery problem thanks to the stable name). Broker
    restrictions: consume and publish the request on the SAME channel, and in
    no-ack. The server side is IDENTICAL in both modes: it publishes to the
    default exchange with routing key = the ReplyTo it received (which the
    broker rewrites to an opaque name).

  Compiles in both worlds from the SAME source (GUI samples pattern):
  callbacks as named methods ('of object', FPC rule) and UI updates through
  disposable "marshal" objects + TThread.Queue (see BackOfficeVcl). Connection
  events hop through PcPool before posting: on FPC a TThread.Queue posted by
  the reconnection thread (which dies right after OnReconnect) would be
  dropped; see TConnectionEventWork and the gotcha in CLAUDE.md. }

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
  Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls, ExtCtrls,
  AMQP.Wire, PascalCommon.Threading, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  { Query in flight on the client, indexed by CorrelationId. Created and
    resolved ONLY on the UI thread (marshals + timer). }
  TPendingQuery = class
    InvoiceKey: string;
    SentTick: UInt64;
    DeadlineTick: UInt64;
    Item: TListItem;
  end;

  TfrmQuery = class(TForm)
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
    gbServer: TGroupBox;
    lblRequestQueue: TLabel;
    edtRequestQueue: TEdit;
    lblDelay: TLabel;
    edtDelay: TEdit;
    btnServer: TButton;
    lblServed: TLabel;
    gbClient: TGroupBox;
    lblKey: TLabel;
    edtKey: TEdit;
    lblQueryCount: TLabel;
    edtQueryCount: TEdit;
    lblTimeout: TLabel;
    edtTimeout: TEdit;
    btnQuery: TButton;
    chkDirectReplyTo: TCheckBox;
    lvQueries: TListView;
    btnClearList: TButton;
    btnClearLog: TButton;
    mmoLog: TMemo;
    tmrTimeout: TTimer;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnServerClick(Sender: TObject);
    procedure btnQueryClick(Sender: TObject);
    procedure btnClearListClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
    procedure tmrTimeoutTimer(Sender: TObject);
  private
    FConn: TAMQPConnection;
    // Separate channels per role: the server's consumption does not compete for
    // the channel RPC (Publish/DeclareQueue) with the client.
    FServerChannel: TAMQPChannel;
    FClientChannel: TAMQPChannel;
    FRequestQueue: string;   // captured on connect (the client publishes to it)
    FRepliesQueue: string; // client reply queue (see FDirectMode)
    FDirectMode: Boolean;   // True = Direct Reply-to; False = named queue
    FServerTag: string;   // server consumer tag ('' = server stopped)
    FDelayMaxMs: Integer;   // maximum simulated server delay
    FServed: Integer;
    // Client queries in flight; touched ONLY on the UI thread.
    FPending: TDictionary<string, TPendingQuery>;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConnected(AConnected: Boolean);
    procedure SetServerRunning(ARunning: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RefreshServed;
    procedure CancelPending(const AReason: string);
    // UI updates (UI thread, through marshals):
    procedure ReplyReceived(const ACorrelationId, AStatus: string);
    procedure RequestServed(const AKey, AStatus: string);
    procedure ConnectionDropped;
    procedure ConnectionRestored;
    procedure ConnectionFailed;
    // Marshalling (callable from any thread):
    procedure QueueLog(const AMsg: string);
    // Lib callbacks (pool / reconnection threads):
    procedure OnRequest(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnReply(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnConnDisconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnected(AConnection: TAMQPConnection);
    procedure OnConnReconnectFailed(AConnection: TAMQPConnection);
  end;

var
  frmQuery: TfrmQuery;

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
  cStatusWaiting = 'Aguardando resposta';
  cStatusTimeout    = 'Timeout';

function NewGuidStr: string;
var
  LGuid: TGUID;
begin
  CreateGUID(LGuid);
  Result := GUIDToString(LGuid); // '{XXXXXXXX-...}'
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See BackOfficeVcl.
  TLogMarshal = class
    Form: TfrmQuery;
    LogText: string;
    procedure Execute;
  end;

  TReplyMarshal = class
    Form: TfrmQuery;
    CorrelationId, Status: string;
    procedure Execute;
  end;

  TServedMarshal = class
    Form: TfrmQuery;
    InvoiceKey, Status: string;
    procedure Execute;
  end;

  TConnectionEvent = (ceDropped, ceRestored, ceFailed);

  TConnectionMarshal = class
    Form: TfrmQuery;
    Event: TConnectionEvent;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which ends
    right after OnReconnect, and on FPC a TThread.Queue posted by a
    thread that dies before the pump is DROPPED (TThread.Destroy removes its
    pending posts, matching by ThreadID; on Delphi the removal matches only
    by the AThread parameter, which is nil here). Hopping through a PcPool
    worker (PERSISTENT threads) guarantees delivery. Gotcha in CLAUDE.md;
    found in ReliablePublisherVcl. }
  TConnectionEventWork = class(TPcWorkItem)
  private
    FForm: TfrmQuery;
    FEvent: TConnectionEvent;
  public
    constructor Create(AForm: TfrmQuery; AEvent: TConnectionEvent);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(LogText);
  Free;
end;

procedure TReplyMarshal.Execute;
begin
  Form.ReplyReceived(CorrelationId, Status);
  Free;
end;

procedure TServedMarshal.Execute;
begin
  Form.RequestServed(InvoiceKey, Status);
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

constructor TConnectionEventWork.Create(AForm: TfrmQuery;
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

{ TfrmQuery }

procedure TfrmQuery.FormCreate(Sender: TObject);
begin
  Randomize;
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FPending := TDictionary<string, TPendingQuery>.Create;
  SetConnected(False);
  RefreshServed;
end;

procedure TfrmQuery.FormDestroy(Sender: TObject);
var
  LPendingQuery: TPendingQuery;
begin
  for LPendingQuery in FPending.Values do
    LPendingQuery.Free;
  FPending.Free;
end;

procedure TfrmQuery.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(FServerChannel) and (FServerTag <> '') then
    try
      FServerChannel.Cancel(FServerTag);
    except
    end;
  FreeAndNil(FServerChannel);
  FreeAndNil(FClientChannel);
  // The Free calls above drain the callbacks in flight; the marshals they posted
  // are still in the TThread.Queue queue, and pumping here drains them with the
  // form still alive (see BackOfficeVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread, so no more connection
  // events are born. A TConnectionEventWork already queued on the pool may be
  // posting the last marshal RIGHT NOW: the short wait + pump drain
  // that straggler.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmQuery.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmQuery.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmQuery.QueueLog(const AMsg: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.LogText := AMsg;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmQuery.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmQuery.btnClearListClick(Sender: TObject);
var
  LPendingQuery: TPendingQuery;
begin
  // Queries still in flight remain valid; we only detach their rows
  // from the list (the reply/timeout becomes just a log line).
  for LPendingQuery in FPending.Values do
    LPendingQuery.Item := nil;
  lvQueries.Items.Clear;
end;

procedure TfrmQuery.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmQuery.BuildParams: TAMQPConnectionParams;
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
  Result.ConnectionName := 'ConsultaStatusVcl';
end;

procedure TfrmQuery.SetConnected(AConnected: Boolean);
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
  edtRequestQueue.Enabled := not AConnected;
  chkDirectReplyTo.Enabled := not AConnected; // mechanism chosen on connect
  btnServer.Enabled := AConnected;
  btnQuery.Enabled := AConnected;
end;

procedure TfrmQuery.SetServerRunning(ARunning: Boolean);
begin
  if ARunning then
    btnServer.Caption := 'Parar servidor'
  else
    btnServer.Caption := 'Iniciar servidor';
  edtDelay.Enabled := not ARunning;
end;

procedure TfrmQuery.RefreshServed;
begin
  lblServed.Caption := Format('Atendidas: %d', [FServed]);
end;

{ --- server ---------------------------------------------------------------- }

// Runs on a pool thread (one per query in flight: concurrent queries
// are served in parallel, as in BackOfficeVcl).
procedure TfrmQuery.OnRequest(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LKey, LStatus, LReplyTo, LCorr: string;
  LProps: TAMQPBasicProperties;
  LMarshal: TServedMarshal;
begin
  LKey := ADelivery.BodyAsText;
  LReplyTo := ADelivery.Properties.ReplyTo;
  LCorr := ADelivery.Properties.CorrelationId;

  Sleep(100 + Random(FDelayMaxMs)); // simulated query to the "database"

  case Random(10) of
    0..6: LStatus := 'Autorizada';
    7..8: LStatus := 'Rejeitada';
  else
    LStatus := 'Em processamento';
  end;

  // Without ReplyTo there is nowhere to reply: serve it and drop it (a real
  // server would log the malformed request).
  if LReplyTo <> '' then
  begin
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetCorrelationId(LCorr); // echo: this is how the client correlates
    AChannel.Publish('', LReplyTo, AmqpUtf8Encode(LStatus), LProps);
  end;
  AChannel.Ack(ADelivery.DeliveryTag); // only after replying

  LMarshal := TServedMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.InvoiceKey := LKey;
  LMarshal.Status := LStatus;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmQuery.RequestServed(const AKey, AStatus: string);
begin
  Inc(FServed);
  RefreshServed;
  Log(Format('Servidor: consulta da nota %s respondida: %s.', [AKey, AStatus]));
end;

procedure TfrmQuery.btnServerClick(Sender: TObject);
begin
  if not Assigned(FConn) then
    Exit;

  if FServerTag <> '' then
  begin
    try
      FServerChannel.Cancel(FServerTag);
      Log('Servidor parado.');
    except
      on E: Exception do
        Log('Erro ao parar o servidor: ' + E.Message);
    end;
    FServerTag := '';
    SetServerRunning(False);
    Exit;
  end;

  FDelayMaxMs := StrToIntDef(Trim(edtDelay.Text), 800);
  if FDelayMaxMs < 1 then
    FDelayMaxMs := 1;
  try
    FServerTag := FServerChannel.Consume(FRequestQueue, OnRequest);
    Log(Format('Servidor iniciado: consumindo "%s" (demora simulada até %d ms).',
      [FRequestQueue, FDelayMaxMs]));
    SetServerRunning(True);
  except
    on E: Exception do
      Log('Erro ao iniciar o servidor: ' + E.Message);
  end;
end;

{ --- client ---------------------------------------------------------------- }

procedure TfrmQuery.btnQueryClick(Sender: TObject);
var
  I, LCount, LTimeout: Integer;
  LKey, LCorr: string;
  LProps: TAMQPBasicProperties;
  LPendingQuery: TPendingQuery;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not Assigned(FClientChannel) then
    Exit;
  LKey := Trim(edtKey.Text);
  if LKey = '' then
  begin
    Log('Informe a chave da nota.');
    Exit;
  end;
  LCount := StrToIntDef(Trim(edtQueryCount.Text), 1);
  if LCount < 1 then
    LCount := 1;
  LTimeout := StrToIntDef(Trim(edtTimeout.Text), 3000);
  if LTimeout < 200 then
    LTimeout := 200;

  for I := 1 to LCount do
  begin
    LCorr := NewGuidStr;

    LAtBottom := ScrollAtBottom(lvQueries.Handle);
    LItem := lvQueries.Items.Add;
    LItem.Caption := LKey;
    LItem.SubItems.Add(Copy(LCorr, 2, 8)); // slice of the GUID, just to make it visible
    LItem.SubItems.Add(cStatusWaiting);
    LItem.SubItems.Add(FormatDateTime('hh:nn:ss.zzz', Now));
    LItem.SubItems.Add('');
    if LAtBottom then
      LItem.MakeVisible(False);

    LPendingQuery := TPendingQuery.Create;
    LPendingQuery.InvoiceKey := LKey;
    LPendingQuery.SentTick := PcTickMs;
    LPendingQuery.DeadlineTick := LPendingQuery.SentTick + UInt64(LTimeout);
    LPendingQuery.Item := LItem;
    FPending.Add(LCorr, LPendingQuery);

    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetReplyTo(FRepliesQueue);
    LProps.SetCorrelationId(LCorr);
    // Request TTL = client timeout: if it ages in the queue (server
    // stopped/slow), the broker drops it; nobody replies to someone who gave up.
    LProps.SetExpiration(IntToStr(LTimeout));
    try
      FClientChannel.Publish('', FRequestQueue, AmqpUtf8Encode(LKey), LProps);
    except
      on E: Exception do
      begin
        Log('Erro ao publicar a consulta: ' + E.Message);
        LItem.SubItems[1] := 'Erro no envio';
        FPending.Remove(LCorr);
        LPendingQuery.Free;
        Break;
      end;
    end;
  end;
end;

// Runs on a pool thread (consumer of the reply queue, automatic ack).
procedure TfrmQuery.OnReply(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TReplyMarshal;
begin
  LMarshal := TReplyMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.CorrelationId := ADelivery.Properties.CorrelationId;
  LMarshal.Status := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmQuery.ReplyReceived(const ACorrelationId, AStatus: string);
var
  LPendingQuery: TPendingQuery;
  LElapsedMs: UInt64;
begin
  if not FPending.TryGetValue(ACorrelationId, LPendingQuery) then
  begin
    // Already expired (timeout) or unknown correlation: the RPC pattern's rule
    // is to drop it; whoever was waiting is gone.
    Log(Format('Resposta tardia descartada (correlação %s): %s.',
      [Copy(ACorrelationId, 2, 8), AStatus]));
    Exit;
  end;
  FPending.Remove(ACorrelationId);
  LElapsedMs := PcTickMs - LPendingQuery.SentTick;
  if LPendingQuery.Item <> nil then
  begin
    LPendingQuery.Item.SubItems[1] := AStatus;
    LPendingQuery.Item.SubItems[3] := Format('%d ms', [Int64(LElapsedMs)]);
  end;
  Log(Format('Resposta da nota %s: %s (%d ms).',
    [LPendingQuery.InvoiceKey, AStatus, Int64(LElapsedMs)]));
  LPendingQuery.Free;
end;

procedure TfrmQuery.tmrTimeoutTimer(Sender: TObject);
var
  LCorr: string;
  LExpired: TList<string>;
  LPendingQuery: TPendingQuery;
  LNow: UInt64;
begin
  if FPending.Count = 0 then
    Exit;
  LNow := PcTickMs;
  LExpired := TList<string>.Create;
  try
    for LCorr in FPending.Keys do
      if LNow >= FPending[LCorr].DeadlineTick then
        LExpired.Add(LCorr);
    for LCorr in LExpired do
    begin
      LPendingQuery := FPending[LCorr];
      FPending.Remove(LCorr);
      if LPendingQuery.Item <> nil then
      begin
        LPendingQuery.Item.SubItems[1] := cStatusTimeout;
        LPendingQuery.Item.SubItems[3] := '—';
      end;
      Log(Format('Timeout na consulta da nota %s (correlação %s).',
        [LPendingQuery.InvoiceKey, Copy(LCorr, 2, 8)]));
      LPendingQuery.Free;
    end;
  finally
    LExpired.Free;
  end;
end;

procedure TfrmQuery.CancelPending(const AReason: string);
var
  LPendingQuery: TPendingQuery;
begin
  for LPendingQuery in FPending.Values do
  begin
    if LPendingQuery.Item <> nil then
    begin
      LPendingQuery.Item.SubItems[1] := AReason;
      LPendingQuery.Item.SubItems[3] := '—';
    end;
    LPendingQuery.Free;
  end;
  FPending.Clear;
end;

{ --- connection events ----------------------------------------------------- }

procedure TfrmQuery.ConnectionDropped;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; consultas em voo ' +
    'vão expirar por timeout (a fila de respostas morre com a conexão).');
end;

procedure TfrmQuery.ConnectionRestored;
begin
  SetConnected(True);
  Log('Reconectado: filas e consumers restaurados (o servidor, se ativo, volta a atender).');
end;

procedure TfrmQuery.ConnectionFailed;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly:
// the reconnection thread dies right after OnReconnect and, on FPC, would take
// the pending post with it (see the comment on TConnectionEventWork).
procedure TfrmQuery.OnConnDisconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceDropped));
end;

procedure TfrmQuery.OnConnReconnected(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceRestored));
end;

procedure TfrmQuery.OnConnReconnectFailed(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConnectionEventWork.Create(Self, ceFailed));
end;

{ --- connection ------------------------------------------------------------ }

procedure TfrmQuery.btnConnectClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LDeclare: TAMQPQueueDeclare;
begin
  if Assigned(FConn) then
  begin
    try
      if (FServerChannel <> nil) and (FServerTag <> '') then
        FServerChannel.Cancel(FServerTag);
      FServerTag := '';
      FreeAndNil(FServerChannel);
      FreeAndNil(FClientChannel);
      FreeAndNil(FConn);
      Log('Desconectado.');
    except
      on E: Exception do
        Log('Erro ao desconectar: ' + E.Message);
    end;
    CancelPending('Cancelada (desconexão)');
    SetServerRunning(False);
    SetConnected(False);
    Exit;
  end;

  LParams := BuildParams;
  FRequestQueue := Trim(edtRequestQueue.Text);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnConnDisconnected;
    FConn.OnReconnect := OnConnReconnected;
    FConn.OnReconnectFailed := OnConnReconnectFailed;
    FConn.Open;

    // Server channel: durable request queue + prefetch. Declared already on
    // connect (even without starting the server): queries published with no
    // server running stay in the queue until the request TTL runs out.
    FServerChannel := FConn.CreateChannel;
    FServerChannel.DeclareQueue(TAMQPQueueDeclare.Create(FRequestQueue, True));
    FServerChannel.Qos(16);

    // Client channel: two reply mechanisms, chosen in the checkbox.
    // In both the consume is NoAck (lost reply = timeout, the client does not
    // reprocess) and the request publish goes out on THIS same channel.
    FDirectMode := chkDirectReplyTo.Checked;
    FClientChannel := FConn.CreateChannel;
    if FDirectMode then
    begin
      // Direct Reply-to (amq.rabbitmq.reply-to): the broker's fixed-name
      // pseudo-queue; it is NOT declared. It is enough to consume from it in
      // no-ack BEFORE publishing; the broker rewrites the request's ReplyTo to an
      // opaque name and routes the reply back by a fast path, with no real
      // queue. The server does not change: it publishes to the default exchange
      // with routing key = the ReplyTo it received. It is the pattern the README
      // recommends for RPC. Restrictions (RabbitMQ): consume and publish the
      // request on the SAME channel (guaranteed here: both use FClientChannel) and
      // in no-ack. It survives recovery thanks to the stable name, without
      // managing a queue per instance.
      FRepliesQueue := 'amq.rabbitmq.reply-to';
      FClientChannel.Consume(FRepliesQueue, OnReply, True); // NoAck is mandatory
    end
    else
    begin
      // Named queue exclusive to this instance, with a FIXED name (not a ''
      // generated by the broker, which does not survive the recovery replay: the
      // redeclare would generate a new name and the recorded consumer would point
      // at the old one). Exclusive + auto-delete: the broker removes it when the
      // connection dies.
      FRepliesQueue := 'consulta-resp-' + Copy(NewGuidStr, 2, 8);
      LDeclare := TAMQPQueueDeclare.Create(FRepliesQueue, False);
      LDeclare.Exclusive := True;
      LDeclare.AutoDelete := True;
      FClientChannel.DeclareQueue(LDeclare);
      FClientChannel.Consume(FRepliesQueue, OnReply, True); // NoAck=True
    end;

    Log(Format('Conectado a %s:%d%s. Pedidos em "%s"; respostas via %s ("%s").',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FRequestQueue,
       IfThen(FDirectMode, 'Direct Reply-to', 'fila nomeada'), FRepliesQueue]));
    Log('Dica: rode duas instâncias — servidor numa, cliente na outra. ' +
      'Consultar sem servidor ativo termina em timeout (e o pedido expira ' +
      'na fila junto).');
    SetConnected(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FServerChannel);
      FreeAndNil(FClientChannel);
      FreeAndNil(FConn);
      SetConnected(False);
    end;
  end;
end;

end.
