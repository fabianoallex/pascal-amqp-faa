unit uConsultaMain;

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
  disposable "marshal" objects + TThread.Queue (see RetaguardaVcl). Connection
  events hop through PcPool before posting: on FPC a TThread.Queue posted by
  the reconnection thread (which dies right after OnReconnect) would be
  dropped; see TConexaoEventoWork and the gotcha in CLAUDE.md. }

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
  TConsultaPendente = class
    Chave: string;
    EnviadaTick: UInt64;
    DeadlineTick: UInt64;
    Item: TListItem;
  end;

  TfrmConsulta = class(TForm)
    gbConexao: TGroupBox;
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
    btnConectar: TButton;
    lblStatus: TLabel;
    gbServidor: TGroupBox;
    lblFilaPedidos: TLabel;
    edtFilaPedidos: TEdit;
    lblDelay: TLabel;
    edtDelay: TEdit;
    btnServidor: TButton;
    lblAtendidas: TLabel;
    gbCliente: TGroupBox;
    lblChave: TLabel;
    edtChave: TEdit;
    lblQtdConsultas: TLabel;
    edtQtdConsultas: TEdit;
    lblTimeout: TLabel;
    edtTimeout: TEdit;
    btnConsultar: TButton;
    chkDirectReplyTo: TCheckBox;
    lvConsultas: TListView;
    btnLimparLista: TButton;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    tmrTimeout: TTimer;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnServidorClick(Sender: TObject);
    procedure btnConsultarClick(Sender: TObject);
    procedure btnLimparListaClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
    procedure tmrTimeoutTimer(Sender: TObject);
  private
    FConn: TAMQPConnection;
    // Separate channels per role: the server's consumption does not compete for
    // the channel RPC (Publish/DeclareQueue) with the client.
    FCanalServidor: TAMQPChannel;
    FCanalCliente: TAMQPChannel;
    FFilaPedidos: string;   // captured on connect (the client publishes to it)
    FFilaRespostas: string; // client reply queue (see FModoDireto)
    FModoDireto: Boolean;   // True = Direct Reply-to; False = named queue
    FTagServidor: string;   // server consumer tag ('' = server stopped)
    FDelayMaxMs: Integer;   // maximum simulated server delay
    FAtendidas: Integer;
    // Client queries in flight; touched ONLY on the UI thread.
    FPendentes: TDictionary<string, TConsultaPendente>;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    procedure SetServidorRodando(ARodando: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure AtualizarAtendidas;
    procedure CancelarPendentes(const AMotivo: string);
    // UI updates (UI thread, through marshals):
    procedure RespostaRecebida(const ACorrelationId, AStatus: string);
    procedure PedidoAtendido(const AChave, AStatus: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Marshalling (callable from any thread):
    procedure QueueLog(const AMsg: string);
    // Lib callbacks (pool / reconnection threads):
    procedure OnPedido(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnResposta(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnDesconectado(AConnection: TAMQPConnection);
    procedure OnReconectado(AConnection: TAMQPConnection);
    procedure OnReconexaoFalhou(AConnection: TAMQPConnection);
  end;

var
  frmConsulta: TfrmConsulta;

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
  cStatusAguardando = 'Aguardando resposta';
  cStatusTimeout    = 'Timeout';

function NovoGuid: string;
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
  // callbacks). It frees itself after running. See RetaguardaVcl.
  TLogMarshal = class
    Form: TfrmConsulta;
    Texto: string;
    procedure Execute;
  end;

  TRespostaMarshal = class
    Form: TfrmConsulta;
    CorrelationId, Status: string;
    procedure Execute;
  end;

  TAtendidoMarshal = class
    Form: TfrmConsulta;
    Chave, Status: string;
    procedure Execute;
  end;

  TConexaoEvento = (ceCaiu, ceVoltou, ceFalhou);

  TConexaoMarshal = class
    Form: TfrmConsulta;
    Evento: TConexaoEvento;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which ends
    right after OnReconnect, and on FPC a TThread.Queue posted by a
    thread that dies before the pump is DROPPED (TThread.Destroy removes its
    pending posts, matching by ThreadID; on Delphi the removal matches only
    by the AThread parameter, which is nil here). Hopping through a PcPool
    worker (PERSISTENT threads) guarantees delivery. Gotcha in CLAUDE.md;
    found in PublicadorConfiavelVcl. }
  TConexaoEventoWork = class(TPcWorkItem)
  private
    FForm: TfrmConsulta;
    FEvento: TConexaoEvento;
  public
    constructor Create(AForm: TfrmConsulta; AEvento: TConexaoEvento);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(Texto);
  Free;
end;

procedure TRespostaMarshal.Execute;
begin
  Form.RespostaRecebida(CorrelationId, Status);
  Free;
end;

procedure TAtendidoMarshal.Execute;
begin
  Form.PedidoAtendido(Chave, Status);
  Free;
end;

procedure TConexaoMarshal.Execute;
begin
  case Evento of
    ceCaiu:   Form.ConexaoCaiu;
    ceVoltou: Form.ConexaoVoltou;
    ceFalhou: Form.ConexaoFalhou;
  end;
  Free;
end;

constructor TConexaoEventoWork.Create(AForm: TfrmConsulta;
  AEvento: TConexaoEvento);
begin
  inherited Create;
  FForm := AForm;
  FEvento := AEvento;
end;

procedure TConexaoEventoWork.Execute;
var
  LMarshal: TConexaoMarshal;
begin
  LMarshal := TConexaoMarshal.Create;
  LMarshal.Form := FForm;
  LMarshal.Evento := FEvento;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ TfrmConsulta }

procedure TfrmConsulta.FormCreate(Sender: TObject);
begin
  Randomize;
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FPendentes := TDictionary<string, TConsultaPendente>.Create;
  SetConectado(False);
  AtualizarAtendidas;
end;

procedure TfrmConsulta.FormDestroy(Sender: TObject);
var
  LPendente: TConsultaPendente;
begin
  for LPendente in FPendentes.Values do
    LPendente.Free;
  FPendentes.Free;
end;

procedure TfrmConsulta.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(FCanalServidor) and (FTagServidor <> '') then
    try
      FCanalServidor.Cancel(FTagServidor);
    except
    end;
  FreeAndNil(FCanalServidor);
  FreeAndNil(FCanalCliente);
  // The Free calls above drain the callbacks in flight; the marshals they posted
  // are still in the TThread.Queue queue, and pumping here drains them with the
  // form still alive (see RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread, so no more connection
  // events are born. A TConexaoEventoWork already queued on the pool may be
  // posting the last marshal RIGHT NOW: the short wait + pump drain
  // that straggler.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmConsulta.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmConsulta.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmConsulta.QueueLog(const AMsg: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Texto := AMsg;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmConsulta.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmConsulta.btnLimparListaClick(Sender: TObject);
var
  LPendente: TConsultaPendente;
begin
  // Queries still in flight remain valid; we only detach their rows
  // from the list (the reply/timeout becomes just a log line).
  for LPendente in FPendentes.Values do
    LPendente.Item := nil;
  lvConsultas.Items.Clear;
end;

procedure TfrmConsulta.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmConsulta.BuildParams: TAMQPConnectionParams;
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

procedure TfrmConsulta.SetConectado(AConectado: Boolean);
begin
  if AConectado then
  begin
    btnConectar.Caption := 'Desconectar';
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
    btnConectar.Caption := 'Conectar';
    lblStatus.Caption := 'Desconectado';
    lblStatus.Font.Color := clRed;
  end;
  edtHost.Enabled := not AConectado;
  edtPort.Enabled := not AConectado;
  edtVHost.Enabled := not AConectado;
  edtUser.Enabled := not AConectado;
  edtPassword.Enabled := not AConectado;
  chkUseTls.Enabled := not AConectado;
  chkTlsVerifyPeer.Enabled := (not AConectado) and chkUseTls.Checked;
  edtFilaPedidos.Enabled := not AConectado;
  chkDirectReplyTo.Enabled := not AConectado; // mechanism chosen on connect
  btnServidor.Enabled := AConectado;
  btnConsultar.Enabled := AConectado;
end;

procedure TfrmConsulta.SetServidorRodando(ARodando: Boolean);
begin
  if ARodando then
    btnServidor.Caption := 'Parar servidor'
  else
    btnServidor.Caption := 'Iniciar servidor';
  edtDelay.Enabled := not ARodando;
end;

procedure TfrmConsulta.AtualizarAtendidas;
begin
  lblAtendidas.Caption := Format('Atendidas: %d', [FAtendidas]);
end;

{ --- server ---------------------------------------------------------------- }

// Runs on a pool thread (one per query in flight: concurrent queries
// are served in parallel, as in RetaguardaVcl).
procedure TfrmConsulta.OnPedido(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LChave, LStatus, LReplyTo, LCorr: string;
  LProps: TAMQPBasicProperties;
  LMarshal: TAtendidoMarshal;
begin
  LChave := ADelivery.BodyAsText;
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

  LMarshal := TAtendidoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Chave := LChave;
  LMarshal.Status := LStatus;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmConsulta.PedidoAtendido(const AChave, AStatus: string);
begin
  Inc(FAtendidas);
  AtualizarAtendidas;
  Log(Format('Servidor: consulta da nota %s respondida: %s.', [AChave, AStatus]));
end;

procedure TfrmConsulta.btnServidorClick(Sender: TObject);
begin
  if not Assigned(FConn) then
    Exit;

  if FTagServidor <> '' then
  begin
    try
      FCanalServidor.Cancel(FTagServidor);
      Log('Servidor parado.');
    except
      on E: Exception do
        Log('Erro ao parar o servidor: ' + E.Message);
    end;
    FTagServidor := '';
    SetServidorRodando(False);
    Exit;
  end;

  FDelayMaxMs := StrToIntDef(Trim(edtDelay.Text), 800);
  if FDelayMaxMs < 1 then
    FDelayMaxMs := 1;
  try
    FTagServidor := FCanalServidor.Consume(FFilaPedidos, OnPedido);
    Log(Format('Servidor iniciado: consumindo "%s" (demora simulada até %d ms).',
      [FFilaPedidos, FDelayMaxMs]));
    SetServidorRodando(True);
  except
    on E: Exception do
      Log('Erro ao iniciar o servidor: ' + E.Message);
  end;
end;

{ --- client ---------------------------------------------------------------- }

procedure TfrmConsulta.btnConsultarClick(Sender: TObject);
var
  I, LQtd, LTimeout: Integer;
  LChave, LCorr: string;
  LProps: TAMQPBasicProperties;
  LPendente: TConsultaPendente;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not Assigned(FCanalCliente) then
    Exit;
  LChave := Trim(edtChave.Text);
  if LChave = '' then
  begin
    Log('Informe a chave da nota.');
    Exit;
  end;
  LQtd := StrToIntDef(Trim(edtQtdConsultas.Text), 1);
  if LQtd < 1 then
    LQtd := 1;
  LTimeout := StrToIntDef(Trim(edtTimeout.Text), 3000);
  if LTimeout < 200 then
    LTimeout := 200;

  for I := 1 to LQtd do
  begin
    LCorr := NovoGuid;

    LAtBottom := ScrollAtBottom(lvConsultas.Handle);
    LItem := lvConsultas.Items.Add;
    LItem.Caption := LChave;
    LItem.SubItems.Add(Copy(LCorr, 2, 8)); // slice of the GUID, just to make it visible
    LItem.SubItems.Add(cStatusAguardando);
    LItem.SubItems.Add(FormatDateTime('hh:nn:ss.zzz', Now));
    LItem.SubItems.Add('');
    if LAtBottom then
      LItem.MakeVisible(False);

    LPendente := TConsultaPendente.Create;
    LPendente.Chave := LChave;
    LPendente.EnviadaTick := PcTickMs;
    LPendente.DeadlineTick := LPendente.EnviadaTick + UInt64(LTimeout);
    LPendente.Item := LItem;
    FPendentes.Add(LCorr, LPendente);

    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetReplyTo(FFilaRespostas);
    LProps.SetCorrelationId(LCorr);
    // Request TTL = client timeout: if it ages in the queue (server
    // stopped/slow), the broker drops it; nobody replies to someone who gave up.
    LProps.SetExpiration(IntToStr(LTimeout));
    try
      FCanalCliente.Publish('', FFilaPedidos, AmqpUtf8Encode(LChave), LProps);
    except
      on E: Exception do
      begin
        Log('Erro ao publicar a consulta: ' + E.Message);
        LItem.SubItems[1] := 'Erro no envio';
        FPendentes.Remove(LCorr);
        LPendente.Free;
        Break;
      end;
    end;
  end;
end;

// Runs on a pool thread (consumer of the reply queue, automatic ack).
procedure TfrmConsulta.OnResposta(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TRespostaMarshal;
begin
  LMarshal := TRespostaMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.CorrelationId := ADelivery.Properties.CorrelationId;
  LMarshal.Status := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmConsulta.RespostaRecebida(const ACorrelationId, AStatus: string);
var
  LPendente: TConsultaPendente;
  LTempoMs: UInt64;
begin
  if not FPendentes.TryGetValue(ACorrelationId, LPendente) then
  begin
    // Already expired (timeout) or unknown correlation: the RPC pattern's rule
    // is to drop it; whoever was waiting is gone.
    Log(Format('Resposta tardia descartada (correlação %s): %s.',
      [Copy(ACorrelationId, 2, 8), AStatus]));
    Exit;
  end;
  FPendentes.Remove(ACorrelationId);
  LTempoMs := PcTickMs - LPendente.EnviadaTick;
  if LPendente.Item <> nil then
  begin
    LPendente.Item.SubItems[1] := AStatus;
    LPendente.Item.SubItems[3] := Format('%d ms', [Int64(LTempoMs)]);
  end;
  Log(Format('Resposta da nota %s: %s (%d ms).',
    [LPendente.Chave, AStatus, Int64(LTempoMs)]));
  LPendente.Free;
end;

procedure TfrmConsulta.tmrTimeoutTimer(Sender: TObject);
var
  LCorr: string;
  LExpiradas: TList<string>;
  LPendente: TConsultaPendente;
  LAgora: UInt64;
begin
  if FPendentes.Count = 0 then
    Exit;
  LAgora := PcTickMs;
  LExpiradas := TList<string>.Create;
  try
    for LCorr in FPendentes.Keys do
      if LAgora >= FPendentes[LCorr].DeadlineTick then
        LExpiradas.Add(LCorr);
    for LCorr in LExpiradas do
    begin
      LPendente := FPendentes[LCorr];
      FPendentes.Remove(LCorr);
      if LPendente.Item <> nil then
      begin
        LPendente.Item.SubItems[1] := cStatusTimeout;
        LPendente.Item.SubItems[3] := '—';
      end;
      Log(Format('Timeout na consulta da nota %s (correlação %s).',
        [LPendente.Chave, Copy(LCorr, 2, 8)]));
      LPendente.Free;
    end;
  finally
    LExpiradas.Free;
  end;
end;

procedure TfrmConsulta.CancelarPendentes(const AMotivo: string);
var
  LPendente: TConsultaPendente;
begin
  for LPendente in FPendentes.Values do
  begin
    if LPendente.Item <> nil then
    begin
      LPendente.Item.SubItems[1] := AMotivo;
      LPendente.Item.SubItems[3] := '—';
    end;
    LPendente.Free;
  end;
  FPendentes.Clear;
end;

{ --- connection events ----------------------------------------------------- }

procedure TfrmConsulta.ConexaoCaiu;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; consultas em voo ' +
    'vão expirar por timeout (a fila de respostas morre com a conexão).');
end;

procedure TfrmConsulta.ConexaoVoltou;
begin
  SetConectado(True);
  Log('Reconectado: filas e consumers restaurados (o servidor, se ativo, volta a atender).');
end;

procedure TfrmConsulta.ConexaoFalhou;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly:
// the reconnection thread dies right after OnReconnect and, on FPC, would take
// the pending post with it (see the comment on TConexaoEventoWork).
procedure TfrmConsulta.OnDesconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmConsulta.OnReconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmConsulta.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- connection ------------------------------------------------------------ }

procedure TfrmConsulta.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LDeclare: TAMQPQueueDeclare;
begin
  if Assigned(FConn) then
  begin
    try
      if (FCanalServidor <> nil) and (FTagServidor <> '') then
        FCanalServidor.Cancel(FTagServidor);
      FTagServidor := '';
      FreeAndNil(FCanalServidor);
      FreeAndNil(FCanalCliente);
      FreeAndNil(FConn);
      Log('Desconectado.');
    except
      on E: Exception do
        Log('Erro ao desconectar: ' + E.Message);
    end;
    CancelarPendentes('Cancelada (desconexão)');
    SetServidorRodando(False);
    SetConectado(False);
    Exit;
  end;

  LParams := BuildParams;
  FFilaPedidos := Trim(edtFilaPedidos.Text);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnDesconectado;
    FConn.OnReconnect := OnReconectado;
    FConn.OnReconnectFailed := OnReconexaoFalhou;
    FConn.Open;

    // Server channel: durable request queue + prefetch. Declared already on
    // connect (even without starting the server): queries published with no
    // server running stay in the queue until the request TTL runs out.
    FCanalServidor := FConn.CreateChannel;
    FCanalServidor.DeclareQueue(TAMQPQueueDeclare.Create(FFilaPedidos, True));
    FCanalServidor.Qos(16);

    // Client channel: two reply mechanisms, chosen in the checkbox.
    // In both the consume is NoAck (lost reply = timeout, the client does not
    // reprocess) and the request publish goes out on THIS same channel.
    FModoDireto := chkDirectReplyTo.Checked;
    FCanalCliente := FConn.CreateChannel;
    if FModoDireto then
    begin
      // Direct Reply-to (amq.rabbitmq.reply-to): the broker's fixed-name
      // pseudo-queue; it is NOT declared. It is enough to consume from it in
      // no-ack BEFORE publishing; the broker rewrites the request's ReplyTo to an
      // opaque name and routes the reply back by a fast path, with no real
      // queue. The server does not change: it publishes to the default exchange
      // with routing key = the ReplyTo it received. It is the pattern the README
      // recommends for RPC. Restrictions (RabbitMQ): consume and publish the
      // request on the SAME channel (guaranteed here: both use FCanalCliente) and
      // in no-ack. It survives recovery thanks to the stable name, without
      // managing a queue per instance.
      FFilaRespostas := 'amq.rabbitmq.reply-to';
      FCanalCliente.Consume(FFilaRespostas, OnResposta, True); // NoAck is mandatory
    end
    else
    begin
      // Named queue exclusive to this instance, with a FIXED name (not a ''
      // generated by the broker, which does not survive the recovery replay: the
      // redeclare would generate a new name and the recorded consumer would point
      // at the old one). Exclusive + auto-delete: the broker removes it when the
      // connection dies.
      FFilaRespostas := 'consulta-resp-' + Copy(NovoGuid, 2, 8);
      LDeclare := TAMQPQueueDeclare.Create(FFilaRespostas, False);
      LDeclare.Exclusive := True;
      LDeclare.AutoDelete := True;
      FCanalCliente.DeclareQueue(LDeclare);
      FCanalCliente.Consume(FFilaRespostas, OnResposta, True); // NoAck=True
    end;

    Log(Format('Conectado a %s:%d%s. Pedidos em "%s"; respostas via %s ("%s").',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FFilaPedidos,
       IfThen(FModoDireto, 'Direct Reply-to', 'fila nomeada'), FFilaRespostas]));
    Log('Dica: rode duas instâncias — servidor numa, cliente na outra. ' +
      'Consultar sem servidor ativo termina em timeout (e o pedido expira ' +
      'na fila junto).');
    SetConectado(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FCanalServidor);
      FreeAndNil(FCanalCliente);
      FreeAndNil(FConn);
      SetConectado(False);
    end;
  end;
end;

end.
