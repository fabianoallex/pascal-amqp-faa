unit uHeadersMain;

{ Pub/sub with a HEADERS exchange: routing does not use the routing key but the
  MATCHING of the message's header fields against criteria declared on each
  binding. Each binding carries an `x-match` argument:
    - `all`: the message matches if ALL the criterion's fields match (logical AND);
    - `any`: it matches if ANY of them matches (logical OR).
  The broker ignores the routing key on this exchange type.

  It is the counterpart of EventosTopicVcl (routing by hierarchical routing key):
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
  the UI (see RetaguardaVcl) and connection events hopping through PcPool (the
  dropped TThread.Queue gotcha; see TConexaoEventoWork and CLAUDE.md). }

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
  TAssinante = class
    Numero: Integer;
    Match: string;      // 'all' ou 'any'
    CriTipo: string;    // '' = not part of the criterion
    CriRegiao: string;  // '' = not part of the criterion
    Fila: string;
    Tag: string;
    Recebidas: Integer;
    Item: TListItem;
  end;

  TfrmHeaders = class(TForm)
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
    lblTipo: TLabel;
    edtTipo: TEdit;
    lblRegiao: TLabel;
    edtRegiao: TEdit;
    lblMensagem: TLabel;
    edtMensagem: TEdit;
    btnPublicar: TButton;
    gbAssinar: TGroupBox;
    lblMatch: TLabel;
    cmbMatch: TComboBox;
    lblCriTipo: TLabel;
    edtCriTipo: TEdit;
    lblCriRegiao: TLabel;
    edtCriRegiao: TEdit;
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
    procedure EventoRecebido(const ATag, ATipo, ARegiao, ACorpo: string);
    procedure EventoDevolvido(const ATipo, ARegiao, ACorpo: string);
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

function NovoSufixo: string;
var
  LGuid: TGUID;
begin
  CreateGUID(LGuid);
  Result := Copy(GUIDToString(LGuid), 2, 8);
end;

// Readable text of a binding's criteria (e.g. 'tipo=nfe, regiao=sul').
function DescCriterios(const ATipo, ARegiao: string): string;
begin
  Result := '';
  if ATipo <> '' then
    Result := 'tipo=' + ATipo;
  if ARegiao <> '' then
  begin
    if Result <> '' then
      Result := Result + ', ';
    Result := Result + 'regiao=' + ARegiao;
  end;
  if Result = '' then
    Result := '(sem critério)';
end;

// Builds the binding's argument table: x-match + the non-empty criteria.
// The caller OWNS the table and frees it after the bind/unbind. The Puts are
// separate statements on purpose (chaining that ends in an inline
// TValue.From<T> gives an internal error on FPC 3.2; gotcha in CLAUDE.md).
function MontarArgumentos(const AMatch, ATipo, ARegiao: string): TAMQPFieldTable;
begin
  Result := TAMQPFieldTable.Create;
  Result.Put('x-match', TValue.From<string>(AMatch));
  if ATipo <> '' then
    Result.Put('tipo', TValue.From<string>(ATipo));
  if ARegiao <> '' then
    Result.Put('regiao', TValue.From<string>(ARegiao));
end;

// Reads a string header from the delivery's table. The value is assigned to a
// local TValue BEFORE .AsString: chaining .AsString straight on the indexer/result
// triggers an internal error on FPC 3.2 (gotcha in CLAUDE.md).
function LerHeader(AHeaders: TAMQPFieldTable; const AKey: string): string;
var
  LValor: TValue;
begin
  Result := '';
  if (AHeaders <> nil) and AHeaders.TryGetValue(AKey, LValor) then
    Result := LValor.AsString;
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See RetaguardaVcl.
  TLogMarshal = class
    Form: TfrmHeaders;
    Texto: string;
    procedure Execute;
  end;

  TEventoMarshal = class
    Form: TfrmHeaders;
    Tag, Tipo, Regiao, Corpo: string;
    procedure Execute;
  end;

  TDevolvidoMarshal = class
    Form: TfrmHeaders;
    Tipo, Regiao, Corpo: string;
    procedure Execute;
  end;

  TConexaoEvento = (ceCaiu, ceVoltou, ceFalhou);

  TConexaoMarshal = class
    Form: TfrmHeaders;
    Evento: TConexaoEvento;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConexaoEventoWork = class(TPcWorkItem)
  private
    FForm: TfrmHeaders;
    FEvento: TConexaoEvento;
  public
    constructor Create(AForm: TfrmHeaders; AEvento: TConexaoEvento);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(Texto);
  Free;
end;

procedure TEventoMarshal.Execute;
begin
  Form.EventoRecebido(Tag, Tipo, Regiao, Corpo);
  Free;
end;

procedure TDevolvidoMarshal.Execute;
begin
  Form.EventoDevolvido(Tipo, Regiao, Corpo);
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

constructor TConexaoEventoWork.Create(AForm: TfrmHeaders; AEvento: TConexaoEvento);
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

{ TfrmHeaders }

procedure TfrmHeaders.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  cmbMatch.ItemIndex := 0; // 'all'
  FAssinantes := TDictionary<string, TAssinante>.Create;
  SetConectado(False);
end;

procedure TfrmHeaders.FormDestroy(Sender: TObject);
var
  LAssinante: TAssinante;
begin
  for LAssinante in FAssinantes.Values do
    LAssinante.Free;
  FAssinantes.Free;
end;

procedure TfrmHeaders.FormClose(Sender: TObject; var Action: TCloseAction);
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

procedure TfrmHeaders.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmHeaders.btnLimparListaClick(Sender: TObject);
begin
  lvEventos.Items.Clear;
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

procedure TfrmHeaders.SetConectado(AConectado: Boolean);
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

procedure TfrmHeaders.btnAssinarClick(Sender: TObject);
var
  LAssinante: TAssinante;
  LDeclare: TAMQPQueueDeclare;
  LQueueBind: TAMQPQueueBind;
  LArgs: TAMQPFieldTable;
begin
  if not Assigned(FChannel) then
    Exit;

  LAssinante := TAssinante.Create;
  try
    Inc(FProximoNumero);
    LAssinante.Numero := FProximoNumero;
    LAssinante.Match := cmbMatch.Text;               // 'all' ou 'any'
    LAssinante.CriTipo := Trim(edtCriTipo.Text);
    LAssinante.CriRegiao := Trim(edtCriRegiao.Text);
    LAssinante.Fila := Format('headers-sub%d-%s', [LAssinante.Numero, NovoSufixo]);

    // The subscriber's disposable queue: exclusive (this connection only) and
    // auto-delete (dies when the consumer cancels / the connection drops).
    LDeclare := TAMQPQueueDeclare.Create(LAssinante.Fila, False);
    LDeclare.Exclusive := True;
    LDeclare.AutoDelete := True;
    FChannel.DeclareQueue(LDeclare);

    // Binding WITHOUT a routing key (ignored on a headers exchange): what decides is
    // the argument table (x-match + criteria). The caller owns the table.
    LArgs := MontarArgumentos(LAssinante.Match, LAssinante.CriTipo, LAssinante.CriRegiao);
    try
      LQueueBind := Default(TAMQPQueueBind);
      LQueueBind.QueueName := LAssinante.Fila;
      LQueueBind.ExchangeName := FExchange;
      LQueueBind.RoutingKey := '';
      LQueueBind.Arguments := LArgs;
      FChannel.BindQueue(LQueueBind);
    finally
      LArgs.Free;
    end;

    LAssinante.Tag := FChannel.Consume(LAssinante.Fila, OnEvento, True); // NoAck
  except
    on E: Exception do
    begin
      Log('Erro ao assinar (' + LAssinante.Match + ', ' +
        DescCriterios(LAssinante.CriTipo, LAssinante.CriRegiao) + '): ' + E.Message);
      LAssinante.Free;
      Exit;
    end;
  end;

  LAssinante.Item := lvAssinantes.Items.Add;
  LAssinante.Item.Caption := IntToStr(LAssinante.Numero);
  LAssinante.Item.SubItems.Add(LAssinante.Match);
  LAssinante.Item.SubItems.Add(DescCriterios(LAssinante.CriTipo, LAssinante.CriRegiao));
  LAssinante.Item.SubItems.Add(LAssinante.Fila);
  LAssinante.Item.SubItems.Add('0');
  LAssinante.Item.Data := LAssinante;
  FAssinantes.Add(LAssinante.Tag, LAssinante);
  Log(Format('Assinante %d criado: x-match=%s, %s (fila "%s").',
    [LAssinante.Numero, LAssinante.Match,
     DescCriterios(LAssinante.CriTipo, LAssinante.CriRegiao), LAssinante.Fila]));
  if (LAssinante.Match = 'any') and (LAssinante.CriTipo = '') and (LAssinante.CriRegiao = '') then
    Log('Aviso: x-match=any sem nenhum critério não casa com nada — nenhum evento chegará.');
end;

// Undoes in the reverse order of creation, and reconciles the recovery topology:
// without the Unbind/DeleteQueue, a reconnection would replay the recorded bind
// for a queue that no longer exists (a 404 would close the channel in the middle
// of recovery). On a headers exchange the Unbind must repeat the SAME arguments
// as the bind (the argument, not the routing key, identifies the binding).
procedure TfrmHeaders.RemoverAssinante(AAssinante: TAssinante);
var
  LUnbind: TAMQPQueueUnbind;
  LDelete: TAMQPQueueDelete;
  LArgs: TAMQPFieldTable;
begin
  try
    FChannel.Cancel(AAssinante.Tag); // stops the deliveries (and clears the consumer from recovery)
    LArgs := MontarArgumentos(AAssinante.Match, AAssinante.CriTipo, AAssinante.CriRegiao);
    try
      LUnbind := Default(TAMQPQueueUnbind);
      LUnbind.QueueName := AAssinante.Fila;
      LUnbind.ExchangeName := FExchange;
      LUnbind.RoutingKey := '';
      LUnbind.Arguments := LArgs;      // must match the bind's
      FChannel.UnbindQueue(LUnbind);   // undoes the binding (and removes it from recovery)
    finally
      LArgs.Free;
    end;
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
  Log(Format('Assinante %d removido (x-match=%s, %s).',
    [AAssinante.Numero, AAssinante.Match,
     DescCriterios(AAssinante.CriTipo, AAssinante.CriRegiao)]));
  AAssinante.Free;
end;

procedure TfrmHeaders.btnRemoverClick(Sender: TObject);
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
procedure TfrmHeaders.RemoverTodosAssinantes;
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
// subscriber (each has its own queue and consumer). The headers come in the
// delivery's properties, read HERE, before the delivery is released.
procedure TfrmHeaders.OnEvento(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TEventoMarshal;
begin
  LMarshal := TEventoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Tag := ADelivery.ConsumerTag; // identifies the subscriber
  LMarshal.Tipo := LerHeader(ADelivery.Properties.Headers, 'tipo');
  LMarshal.Regiao := LerHeader(ADelivery.Properties.Headers, 'regiao');
  LMarshal.Corpo := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmHeaders.EventoRecebido(const ATag, ATipo, ARegiao, ACorpo: string);
var
  LAssinante: TAssinante;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not FAssinantes.TryGetValue(ATag, LAssinante) then
    Exit; // late delivery for a subscriber already removed: drop it silently

  Inc(LAssinante.Recebidas);
  if LAssinante.Item <> nil then
    LAssinante.Item.SubItems[3] := IntToStr(LAssinante.Recebidas);

  LAtBottom := ScrollAtBottom(lvEventos.Handle);
  LItem := lvEventos.Items.Add;
  LItem.Caption := FormatDateTime('hh:nn:ss.zzz', Now);
  LItem.SubItems.Add(Format('%d (%s: %s)',
    [LAssinante.Numero, LAssinante.Match,
     DescCriterios(LAssinante.CriTipo, LAssinante.CriRegiao)]));
  LItem.SubItems.Add(ATipo);
  LItem.SubItems.Add(ARegiao);
  LItem.SubItems.Add(ACorpo);
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmHeaders.OnDevolvida(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
var
  LMarshal: TDevolvidoMarshal;
begin
  LMarshal := TDevolvidoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Tipo := LerHeader(AReturned.Properties.Headers, 'tipo');
  LMarshal.Regiao := LerHeader(AReturned.Properties.Headers, 'regiao');
  LMarshal.Corpo := AReturned.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmHeaders.EventoDevolvido(const ATipo, ARegiao, ACorpo: string);
begin
  Log(Format('Evento "%s" (tipo=%s, regiao=%s) DEVOLVIDO: nenhum binding casou ' +
    '— pub/sub sem assinante perde o evento.',
    [ACorpo, ATipo, ARegiao]));
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmHeaders.btnPublicarClick(Sender: TObject);
var
  LTipo, LRegiao, LCorpo: string;
  LProps: TAMQPBasicProperties;
  LHeaders: TAMQPFieldTable;
begin
  if not Assigned(FChannel) then
    Exit;
  LTipo := Trim(edtTipo.Text);
  LRegiao := Trim(edtRegiao.Text);
  LCorpo := Trim(edtMensagem.Text);
  if LCorpo = '' then
    LCorpo := 'EVT-' + FormatDateTime('hhnnsszzz', Now);

  // The message headers are what the broker matches against the bindings'
  // criteria. The caller owns the table (SetHeaders does not copy it); it is
  // freed after the publish, which serializes the frame synchronously.
  LHeaders := TAMQPFieldTable.Create;
  try
    if LTipo <> '' then
      LHeaders.Put('tipo', TValue.From<string>(LTipo));
    if LRegiao <> '' then
      LHeaders.Put('regiao', TValue.From<string>(LRegiao));

    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetHeaders(LHeaders);
    try
      // empty routing key (ignored on a headers exchange). mandatory=True: with
      // NO matching binding, the broker returns the event (Basic.Return) instead
      // of dropping it silently, so it is visible in the log.
      FChannel.Publish(FExchange, '', AmqpUtf8Encode(LCorpo), LProps, True);
      Log(Format('Publicado "%s" com headers tipo=%s, regiao=%s.',
        [LCorpo, LTipo, LRegiao]));
    except
      on E: Exception do
        Log('Erro ao publicar: ' + E.Message);
    end;
  finally
    LHeaders.Free;
  end;
end;

{ --- connection events ------------------------------------------------------ }

procedure TfrmHeaders.ConexaoCaiu;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; filas exclusivas e ' +
    'bindings serão recriados no recovery.');
end;

procedure TfrmHeaders.ConexaoVoltou;
begin
  SetConectado(True);
  Log('Reconectado: exchange, filas, bindings e consumers restaurados.');
end;

procedure TfrmHeaders.ConexaoFalhou;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConexaoEventoWork).
procedure TfrmHeaders.OnDesconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmHeaders.OnReconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmHeaders.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmHeaders.btnConectarClick(Sender: TObject);
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
      AMQP_EXCHANGE_TYPE_HEADERS));

    Log(Format('Conectado a %s:%d%s. Exchange headers "%s" declarado.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FExchange]));
    Log('Dica: crie assinantes com x-match all/any e critérios variados ' +
      '(tipo, regiao) e publique eventos com headers diferentes — o mesmo ' +
      'evento chega a todo assinante cujo critério casar.');
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
