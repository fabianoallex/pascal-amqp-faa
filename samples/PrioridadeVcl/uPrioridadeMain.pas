unit uPrioridadeMain;

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
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  TfrmPrioridade = class(TForm)
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
    lblFila: TLabel;
    edtFila: TEdit;
    lblMaxPrio: TLabel;
    edtMaxPrio: TEdit;
    lblPrio: TLabel;
    edtPrio: TEdit;
    lblMensagem: TLabel;
    edtMensagem: TEdit;
    btnPublicar: TButton;
    btnBurst: TButton;
    gbConsumidor: TGroupBox;
    lblDelay: TLabel;
    edtDelay: TEdit;
    lblPrefetch: TLabel;
    edtPrefetch: TEdit;
    lblConsumidas: TLabel;
    btnConsumidor: TButton;
    lvMensagens: TListView;
    btnLimparLista: TButton;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnConectarClick(Sender: TObject);
    procedure btnPublicarClick(Sender: TObject);
    procedure btnBurstClick(Sender: TObject);
    procedure btnConsumidorClick(Sender: TObject);
    procedure btnLimparListaClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;      // producer channel (declare + publish)
    FCanalConsumo: TAMQPChannel; // separate consumer channel (its own Qos)
    FFila: string;
    FMaxPrio: Integer;
    FTagConsumidor: string;      // '' = consumer stopped
    FDelayMs: Integer;           // consumer delay per message
    FConsumidas: Integer;
    FSaidaSeq: Integer;          // OUTPUT order (draining)
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    procedure SetConsumindo(AConsumindo: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure PublicarUma(APrioridade: Integer; const ACorpo: string);
    // UI updates (UI thread, through marshals):
    procedure MensagemConsumida(APrioridade: Integer; const ACorpo: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Lib callbacks (pool / reconnection threads):
    procedure OnMensagem(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnDesconectado(AConnection: TAMQPConnection);
    procedure OnReconectado(AConnection: TAMQPConnection);
    procedure OnReconexaoFalhou(AConnection: TAMQPConnection);
  end;

var
  frmPrioridade: TfrmPrioridade;

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
  cLotePrioridades: array[0..9] of Integer = (2, 8, 5, 9, 1, 7, 3, 9, 0, 6);

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See RetaguardaVcl.
  TLogMarshal = class
    Form: TfrmPrioridade;
    Texto: string;
    procedure Execute;
  end;

  TConsumoMarshal = class
    Form: TfrmPrioridade;
    Prioridade: Integer;
    Corpo: string;
    procedure Execute;
  end;

  TConexaoEvento = (ceCaiu, ceVoltou, ceFalhou);

  TConexaoMarshal = class
    Form: TfrmPrioridade;
    Evento: TConexaoEvento;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConexaoEventoWork = class(TPcWorkItem)
  private
    FForm: TfrmPrioridade;
    FEvento: TConexaoEvento;
  public
    constructor Create(AForm: TfrmPrioridade; AEvento: TConexaoEvento);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(Texto);
  Free;
end;

procedure TConsumoMarshal.Execute;
begin
  Form.MensagemConsumida(Prioridade, Corpo);
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

constructor TConexaoEventoWork.Create(AForm: TfrmPrioridade; AEvento: TConexaoEvento);
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

{ TfrmPrioridade }

procedure TfrmPrioridade.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  SetConectado(False);
end;

procedure TfrmPrioridade.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(FCanalConsumo) and (FTagConsumidor <> '') then
    try
      FCanalConsumo.Cancel(FTagConsumidor);
    except
    end;
  FreeAndNil(FCanalConsumo);
  FreeAndNil(FChannel);
  // The Free calls above drain the callbacks in flight; the marshals they posted
  // are still in the TThread.Queue queue, and pumping here drains them with the
  // form still alive (see RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread; a TConexaoEventoWork
  // already queued on the pool may be posting the last marshal RIGHT NOW.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmPrioridade.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmPrioridade.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmPrioridade.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmPrioridade.btnLimparListaClick(Sender: TObject);
begin
  lvMensagens.Items.Clear;
  FSaidaSeq := 0;
end;

procedure TfrmPrioridade.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmPrioridade.BuildParams: TAMQPConnectionParams;
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

procedure TfrmPrioridade.SetConectado(AConectado: Boolean);
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
  edtFila.Enabled := not AConectado;
  edtMaxPrio.Enabled := not AConectado;
  btnPublicar.Enabled := AConectado;
  btnBurst.Enabled := AConectado;
  btnConsumidor.Enabled := AConectado;
end;

procedure TfrmPrioridade.SetConsumindo(AConsumindo: Boolean);
begin
  if AConsumindo then
    btnConsumidor.Caption := 'Parar consumidor'
  else
    btnConsumidor.Caption := 'Iniciar consumidor';
  edtDelay.Enabled := not AConsumindo;
  edtPrefetch.Enabled := not AConsumindo;
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmPrioridade.PublicarUma(APrioridade: Integer; const ACorpo: string);
var
  LProps: TAMQPBasicProperties;
begin
  if APrioridade < 0 then
    APrioridade := 0;
  if APrioridade > FMaxPrio then
    APrioridade := FMaxPrio; // the broker also clamps to the cap, but we make it explicit
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LProps.SetPriority(Byte(APrioridade));
  // Publishes straight to the queue through the default exchange (routing key = queue name).
  FChannel.Publish('', FFila, AmqpUtf8Encode(ACorpo), LProps);
end;

procedure TfrmPrioridade.btnPublicarClick(Sender: TObject);
var
  LPrio: Integer;
  LCorpo: string;
begin
  if not Assigned(FChannel) then
    Exit;
  LPrio := StrToIntDef(Trim(edtPrio.Text), 0);
  LCorpo := Trim(edtMensagem.Text);
  if LCorpo = '' then
    LCorpo := 'msg-' + FormatDateTime('hhnnsszzz', Now);
  try
    PublicarUma(LPrio, LCorpo);
    Log(Format('Publicada "%s" com prioridade %d.', [LCorpo, LPrio]));
  except
    on E: Exception do
      Log('Erro ao publicar: ' + E.Message);
  end;
end;

procedure TfrmPrioridade.btnBurstClick(Sender: TObject);
var
  I, LPrio: Integer;
  LCorpo, LMapa: string;
begin
  if not Assigned(FChannel) then
    Exit;
  try
    LMapa := '';
    for I := Low(cLotePrioridades) to High(cLotePrioridades) do
    begin
      LPrio := cLotePrioridades[I];
      // The body identifies the publish ORDER (1..N); when draining it comes out
      // shuffled, proving that priority is what rules.
      LCorpo := Format('pub#%d', [I + 1]);
      PublicarUma(LPrio, LCorpo);
      // Records the composition in the log (pub#N=priority) for later checking
      // from the log text alone, without depending on the list.
      if LMapa <> '' then
        LMapa := LMapa + ' ';
      LMapa := LMapa + Format('%s=p%d', [LCorpo, LPrio]);
    end;
    Log(Format('Lote publicado (%d msgs, pub#N=prioridade): %s',
      [Length(cLotePrioridades), LMapa]));
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
procedure TfrmPrioridade.OnMensagem(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TConsumoMarshal;
begin
  LMarshal := TConsumoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Prioridade := ADelivery.Properties.Priority;
  LMarshal.Corpo := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);

  Sleep(FDelayMs);                    // deliberately "slow" consumer
  AChannel.Ack(ADelivery.DeliveryTag);
end;

procedure TfrmPrioridade.MensagemConsumida(APrioridade: Integer; const ACorpo: string);
var
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  Inc(FConsumidas);
  lblConsumidas.Caption := Format('Consumidas: %d', [FConsumidas]);
  Inc(FSaidaSeq);
  LAtBottom := ScrollAtBottom(lvMensagens.Handle);
  LItem := lvMensagens.Items.Add;
  LItem.Caption := IntToStr(FSaidaSeq);
  LItem.SubItems.Add(IntToStr(APrioridade));
  LItem.SubItems.Add(ACorpo);
  LItem.SubItems.Add(FormatDateTime('hh:nn:ss.zzz', Now));
  if LAtBottom then
    LItem.MakeVisible(False);
  // Log of the OUTPUT order: this way the log text already shows the drain by
  // priority (output #N: priority P, body), without depending on the list.
  Log(Format('Consumida (saída #%d): prioridade %d — %s',
    [FSaidaSeq, APrioridade, ACorpo]));
end;

procedure TfrmPrioridade.btnConsumidorClick(Sender: TObject);
var
  LPrefetch: Integer;
begin
  if not Assigned(FConn) then
    Exit;

  if FTagConsumidor <> '' then
  begin
    try
      FCanalConsumo.Cancel(FTagConsumidor);
      Log('Consumidor parado.');
    except
      on E: Exception do
        Log('Erro ao parar o consumidor: ' + E.Message);
    end;
    FTagConsumidor := '';
    SetConsumindo(False);
    Exit;
  end;

  FDelayMs := StrToIntDef(Trim(edtDelay.Text), 600);
  if FDelayMs < 0 then
    FDelayMs := 0;
  LPrefetch := StrToIntDef(Trim(edtPrefetch.Text), 1);
  if LPrefetch < 1 then
    LPrefetch := 1;
  try
    FCanalConsumo.Qos(Word(LPrefetch));
    FTagConsumidor := FCanalConsumo.Consume(FFila, OnMensagem); // manual ack
    Log(Format('Consumidor iniciado: prefetch=%d, delay=%d ms por mensagem.',
      [LPrefetch, FDelayMs]));
    if LPrefetch > 1 then
      Log('Aviso: prefetch > 1 despeja um lote no cliente de uma vez — a saída ' +
        'deixa de respeitar estritamente a prioridade. Use 1 para ver o efeito.');
    SetConsumindo(True);
  except
    on E: Exception do
      Log('Erro ao iniciar o consumidor: ' + E.Message);
  end;
end;

{ --- connection events ------------------------------------------------------ }

procedure TfrmPrioridade.ConexaoCaiu;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento; a fila e o consumidor ' +
    'serão restaurados no recovery.');
end;

procedure TfrmPrioridade.ConexaoVoltou;
begin
  SetConectado(True);
  if FTagConsumidor <> '' then
    SetConsumindo(True);
  Log('Reconectado: fila, argumentos e consumidor restaurados.');
end;

procedure TfrmPrioridade.ConexaoFalhou;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConexaoEventoWork).
procedure TfrmPrioridade.OnDesconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmPrioridade.OnReconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmPrioridade.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmPrioridade.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LDel: TAMQPQueueDelete;
begin
  if Assigned(FConn) then
  begin
    try
      if (FCanalConsumo <> nil) and (FTagConsumidor <> '') then
        FCanalConsumo.Cancel(FTagConsumidor);
      FTagConsumidor := '';
      FreeAndNil(FCanalConsumo);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      Log('Desconectado.');
    except
      on E: Exception do
        Log('Erro ao desconectar: ' + E.Message);
    end;
    SetConsumindo(False);
    SetConectado(False);
    Exit;
  end;

  LParams := BuildParams;
  FFila := Trim(edtFila.Text);
  FMaxPrio := StrToIntDef(Trim(edtMaxPrio.Text), 9);
  if FMaxPrio < 1 then
    FMaxPrio := 1;
  if FMaxPrio > 255 then
    FMaxPrio := 255; // AMQP cap (x-max-priority accepts up to 255; RabbitMQ recommends <= 10)
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnDesconectado;
    FConn.OnReconnect := OnReconectado;
    FConn.OnReconnectFailed := OnReconexaoFalhou;
    FConn.Open;

    FChannel := FConn.CreateChannel;

    // Recreates the queue from scratch: redeclaring an existing queue with a different
    // x-max-priority is PRECONDITION_FAILED. DeleteQueue reconciles the recovery
    // topology (see RetryDlqVcl).
    LDel := Default(TAMQPQueueDelete);
    LDel.QueueName := FFila;
    FChannel.DeleteQueue(LDel);

    LArgs := TAMQPFieldTable.Create;
    try
      LArgs.Put('x-max-priority', TValue.From<Integer>(FMaxPrio));
      LDecl := TAMQPQueueDeclare.Create(FFila, True); // durable; recreated on every connection
      LDecl.Arguments := LArgs;
      FChannel.DeclareQueue(LDecl);
    finally
      LArgs.Free;
    end;

    // Separate consumer channel: its Qos(prefetch) does not interfere with the
    // producer channel.
    FCanalConsumo := FConn.CreateChannel;
    FConsumidas := 0;
    lblConsumidas.Caption := 'Consumidas: 0';

    Log(Format('Conectado a %s:%d%s. Fila "%s" recriada com x-max-priority=%d.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FFila, FMaxPrio]));
    Log('Roteiro: com o consumidor PARADO, clique "Publicar lote" (prioridades ' +
      'misturadas). Depois inicie o consumidor lento e veja a saída em ordem ' +
      'decrescente de prioridade.');
    SetConectado(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FCanalConsumo);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      SetConectado(False);
    end;
  end;
end;

end.
