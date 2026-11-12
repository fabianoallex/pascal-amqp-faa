unit uEventosMain;

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
  for the UI (see RetaguardaVcl) and connection events hopping through PcPool
  (the dropped TThread.Queue gotcha; see TConexaoEventoWork and CLAUDE.md). }

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
  TAssinante = class
    Numero: Integer;
    BindingKey: string;
    Fila: string;
    Tag: string;
    Recebidas: Integer;
    Item: TListItem;
  end;

  TfrmEventos = class(TForm)
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
    gbPublicar: TGroupBox;
    lblExchange: TLabel;
    edtExchange: TEdit;
    lblRoutingKey: TLabel;
    edtRoutingKey: TEdit;
    lblMensagem: TLabel;
    edtMensagem: TEdit;
    btnPublicar: TButton;
    gbAssinar: TGroupBox;
    lblBindingKey: TLabel;
    edtBindingKey: TEdit;
    btnAssinar: TButton;
    btnRemover: TButton;
    lvAssinantes: TListView;
    lvEventos: TListView;
    btnLimparLista: TButton;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnPublicarClick(Sender: TObject);
    procedure btnAssinarClick(Sender: TObject);
    procedure btnRemoverClick(Sender: TObject);
    procedure btnLimparListaClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FExchange: string;
    FProximoNumero: Integer;
    // Subscribers by consumer tag; touched ONLY on the UI thread.
    FAssinantes: TDictionary<string, TAssinante>;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RemoverAssinante(AAssinante: TAssinante);
    procedure RemoverTodosAssinantes;
    // UI updates (UI thread, through marshals):
    procedure EventoRecebido(const ATag, ARoutingKey, ACorpo: string);
    procedure EventoDevolvido(const ARoutingKey, ACorpo: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Lib callbacks (pool / reconnection threads):
    procedure OnEvento(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnDevolvida(AChannel: TAMQPChannel; const AReturned: TAMQPReturnedMessage);
    procedure OnDesconectado(AConnection: TAMQPConnection);
    procedure OnReconectado(AConnection: TAMQPConnection);
    procedure OnReconexaoFalhou(AConnection: TAMQPConnection);
  end;

var
  frmEventos: TfrmEventos;

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

function NovoSufixo: string;
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
  // callbacks). It frees itself after running. See RetaguardaVcl.
  TLogMarshal = class
    Form: TfrmEventos;
    Texto: string;
    procedure Execute;
  end;

  TEventoMarshal = class
    Form: TfrmEventos;
    Tag, RoutingKey, Corpo: string;
    procedure Execute;
  end;

  TDevolvidoMarshal = class
    Form: TfrmEventos;
    RoutingKey, Corpo: string;
    procedure Execute;
  end;

  TConexaoEvento = (ceCaiu, ceVoltou, ceFalhou);

  TConexaoMarshal = class
    Form: TfrmEventos;
    Evento: TConexaoEvento;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConexaoEventoWork = class(TPcWorkItem)
  private
    FForm: TfrmEventos;
    FEvento: TConexaoEvento;
  public
    constructor Create(AForm: TfrmEventos; AEvento: TConexaoEvento);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(Texto);
  Free;
end;

procedure TEventoMarshal.Execute;
begin
  Form.EventoRecebido(Tag, RoutingKey, Corpo);
  Free;
end;

procedure TDevolvidoMarshal.Execute;
begin
  Form.EventoDevolvido(RoutingKey, Corpo);
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

constructor TConexaoEventoWork.Create(AForm: TfrmEventos; AEvento: TConexaoEvento);
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

{ TfrmEventos }

procedure TfrmEventos.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FAssinantes := TDictionary<string, TAssinante>.Create;
  SetConectado(False);
end;

procedure TfrmEventos.FormDestroy(Sender: TObject);
var
  LAssinante: TAssinante;
begin
  for LAssinante in FAssinantes.Values do
    LAssinante.Free;
  FAssinantes.Free;
end;

procedure TfrmEventos.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  // Only cancels the consumers (the queues are exclusive + auto-delete: they die
  // with the connection). The full undo is the Remove button's job.
  if Assigned(FChannel) then
    RemoverTodosAssinantes;
  FreeAndNil(FChannel);
  // The Free above drains the callbacks in flight; the marshals they posted
  // are still in the TThread.Queue queue, and pumping here drains them with the
  // form still alive (see RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread; a TConexaoEventoWork
  // already queued on the pool may be posting the last marshal RIGHT NOW.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmEventos.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmEventos.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmEventos.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmEventos.btnLimparListaClick(Sender: TObject);
begin
  lvEventos.Items.Clear;
end;

procedure TfrmEventos.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmEventos.BuildParams: TAMQPConnectionParams;
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

procedure TfrmEventos.SetConectado(AConectado: Boolean);
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
  edtExchange.Enabled := not AConectado;
  btnPublicar.Enabled := AConectado;
  btnAssinar.Enabled := AConectado;
  btnRemover.Enabled := AConectado;
end;

{ --- subscribers ------------------------------------------------------------ }

procedure TfrmEventos.btnAssinarClick(Sender: TObject);
var
  LBind: string;
  LAssinante: TAssinante;
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

  LAssinante := TAssinante.Create;
  try
    Inc(FProximoNumero);
    LAssinante.Numero := FProximoNumero;
    LAssinante.BindingKey := LBind;
    LAssinante.Fila := Format('eventos-sub%d-%s', [LAssinante.Numero, NovoSufixo]);

    // The subscriber's disposable queue: exclusive (this connection only) and
    // auto-delete (dies when the consumer cancels / the connection drops).
    LDeclare := TAMQPQueueDeclare.Create(LAssinante.Fila, False);
    LDeclare.Exclusive := True;
    LDeclare.AutoDelete := True;
    FChannel.DeclareQueue(LDeclare);

    LQueueBind := Default(TAMQPQueueBind);
    LQueueBind.QueueName := LAssinante.Fila;
    LQueueBind.ExchangeName := FExchange;
    LQueueBind.RoutingKey := LBind; // binding key ('*' = 1 word; '#' = 0+)
    FChannel.BindQueue(LQueueBind);

    LAssinante.Tag := FChannel.Consume(LAssinante.Fila, OnEvento, True); // NoAck
  except
    on E: Exception do
    begin
      Log('Erro ao assinar "' + LBind + '": ' + E.Message);
      LAssinante.Free;
      Exit;
    end;
  end;

  LAssinante.Item := lvAssinantes.Items.Add;
  LAssinante.Item.Caption := IntToStr(LAssinante.Numero);
  LAssinante.Item.SubItems.Add(LAssinante.BindingKey);
  LAssinante.Item.SubItems.Add(LAssinante.Fila);
  LAssinante.Item.SubItems.Add('0');
  LAssinante.Item.Data := LAssinante;
  FAssinantes.Add(LAssinante.Tag, LAssinante);
  Log(Format('Assinante %d criado: binding "%s" na fila "%s".',
    [LAssinante.Numero, LAssinante.BindingKey, LAssinante.Fila]));
end;

// Undoes in the reverse order of creation, and reconciles the recovery topology:
// without the Unbind/DeleteQueue, a reconnection would replay the recorded bind
// for a queue that no longer exists (a 404 would close the channel mid-recovery).
procedure TfrmEventos.RemoverAssinante(AAssinante: TAssinante);
var
  LUnbind: TAMQPQueueUnbind;
  LDelete: TAMQPQueueDelete;
begin
  try
    FChannel.Cancel(AAssinante.Tag); // stops the deliveries (and clears the consumer from recovery)
    LUnbind := Default(TAMQPQueueUnbind);
    LUnbind.QueueName := AAssinante.Fila;
    LUnbind.ExchangeName := FExchange;
    LUnbind.RoutingKey := AAssinante.BindingKey;
    FChannel.UnbindQueue(LUnbind);   // undoes the binding (and removes it from recovery)
    LDelete := Default(TAMQPQueueDelete);
    LDelete.QueueName := AAssinante.Fila;
    FChannel.DeleteQueue(LDelete);   // deletes the queue (and removes the declare from recovery)
  except
    on E: Exception do
      Log('Erro ao remover assinante ' + IntToStr(AAssinante.Numero) + ': ' + E.Message);
  end;
  FAssinantes.Remove(AAssinante.Tag);
  if AAssinante.Item <> nil then
    AAssinante.Item.Delete;
  Log(Format('Assinante %d removido (binding "%s").',
    [AAssinante.Numero, AAssinante.BindingKey]));
  AAssinante.Free;
end;

procedure TfrmEventos.btnRemoverClick(Sender: TObject);
begin
  if not Assigned(FChannel) then
    Exit;
  if (lvAssinantes.Selected = nil) or (lvAssinantes.Selected.Data = nil) then
  begin
    Log('Selecione um assinante na lista para remover.');
    Exit;
  end;
  RemoverAssinante(TAssinante(lvAssinantes.Selected.Data));
end;

// On shutdown/disconnect: only cancels the consumers (drains the deliveries in
// flight); the exclusive/auto-delete queues die on their own with the connection.
procedure TfrmEventos.RemoverTodosAssinantes;
var
  LAssinante: TAssinante;
  LLista: TArray<TAssinante>;
begin
  LLista := FAssinantes.Values.ToArray;
  for LAssinante in LLista do
  begin
    try
      FChannel.Cancel(LAssinante.Tag);
    except
    end;
    FAssinantes.Remove(LAssinante.Tag);
    LAssinante.Free;
  end;
  lvAssinantes.Items.Clear;
end;

{ --- events ----------------------------------------------------------------- }

// Runs on a pool thread; one event fires this callback once PER matched
// subscriber (each has its own queue and consumer).
procedure TfrmEventos.OnEvento(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TEventoMarshal;
begin
  LMarshal := TEventoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Tag := ADelivery.ConsumerTag; // identifies the subscriber
  LMarshal.RoutingKey := ADelivery.RoutingKey;
  LMarshal.Corpo := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmEventos.EventoRecebido(const ATag, ARoutingKey, ACorpo: string);
var
  LAssinante: TAssinante;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not FAssinantes.TryGetValue(ATag, LAssinante) then
    Exit; // late delivery for a subscriber already removed: drop it silently

  Inc(LAssinante.Recebidas);
  if LAssinante.Item <> nil then
    LAssinante.Item.SubItems[2] := IntToStr(LAssinante.Recebidas);

  LAtBottom := ScrollAtBottom(lvEventos.Handle);
  LItem := lvEventos.Items.Add;
  LItem.Caption := FormatDateTime('hh:nn:ss.zzz', Now);
  LItem.SubItems.Add(Format('%d ("%s")', [LAssinante.Numero, LAssinante.BindingKey]));
  LItem.SubItems.Add(ARoutingKey);
  LItem.SubItems.Add(ACorpo);
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmEventos.OnDevolvida(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
var
  LMarshal: TDevolvidoMarshal;
begin
  LMarshal := TDevolvidoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.RoutingKey := AReturned.RoutingKey;
  LMarshal.Corpo := AReturned.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmEventos.EventoDevolvido(const ARoutingKey, ACorpo: string);
begin
  Log(Format('Evento "%s" (rk=%s) DEVOLVIDO: nenhum assinante com binding ' +
    'que case — pub/sub sem assinante perde o evento.', [ACorpo, ARoutingKey]));
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmEventos.btnPublicarClick(Sender: TObject);
var
  LRk, LCorpo: string;
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
  LCorpo := Trim(edtMensagem.Text);
  if LCorpo = '' then
    LCorpo := 'EVT-' + FormatDateTime('hhnnsszzz', Now);
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  try
    // mandatory=True: with NO matching binding, the broker returns the event
    // (Basic.Return) instead of dropping it silently, so it is visible in the log.
    FChannel.Publish(FExchange, LRk, AmqpUtf8Encode(LCorpo), LProps, True);
    Log(Format('Publicado "%s" com routing key "%s".', [LCorpo, LRk]));
  except
    on E: Exception do
      Log('Erro ao publicar: ' + E.Message);
  end;
end;

{ --- connection events ------------------------------------------------------ }

procedure TfrmEventos.ConexaoCaiu;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; filas exclusivas e ' +
    'bindings serão recriados no recovery.');
end;

procedure TfrmEventos.ConexaoVoltou;
begin
  SetConectado(True);
  Log('Reconectado: exchange, filas, bindings e consumers restaurados.');
end;

procedure TfrmEventos.ConexaoFalhou;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConexaoEventoWork).
procedure TfrmEventos.OnDesconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmEventos.OnReconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmEventos.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmEventos.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
      RemoverTodosAssinantes;
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      Log('Desconectado.');
    except
      on E: Exception do
        Log('Erro ao desconectar: ' + E.Message);
    end;
    SetConectado(False);
    Exit;
  end;

  LParams := BuildParams;
  FExchange := Trim(edtExchange.Text);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnDesconectado;
    FConn.OnReconnect := OnReconectado;
    FConn.OnReconnectFailed := OnReconexaoFalhou;
    FConn.Open;

    FChannel := FConn.CreateChannel;
    FChannel.OnBasicReturn := OnDevolvida;
    FChannel.DeclareExchange(TAMQPExchangeDeclare.Create(FExchange,
      AMQP_EXCHANGE_TYPE_TOPIC));

    Log(Format('Conectado a %s:%d%s. Exchange topic "%s" declarado.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FExchange]));
    Log('Dica: crie assinantes com bindings diferentes (nota.#, ' +
      'nota.aprovada, pdv.*.venda) e publique routing keys variadas — ' +
      'o mesmo evento chega a todo assinante que casar.');
    SetConectado(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      SetConectado(False);
    end;
  end;
end;

end.
