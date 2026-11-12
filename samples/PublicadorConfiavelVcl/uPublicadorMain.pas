unit uPublicadorMain;

{ Reliable publisher: a showcase of the lib's publisher confirms on one screen.

  The channel is put in confirm mode (ConfirmSelect) right after connecting; each
  Publish gets a seq-no and becomes a row in the list, whose status changes when
  the broker confirms (OnConfirm/ack), rejects (nack) or returns an unroutable
  `mandatory` publish (OnBasicReturn). The batch is published by its own
  thread (TLoteThread), with a configurable interval between messages, so you
  can bring the broker down IN THE MIDDLE of the batch (docker stop/start on
  RabbitMQ) and watch the whole sequence: send failure, automatic reconnection,
  replay of the topology + confirm mode and, with "Reenviar não confirmadas na
  reconexão" checked (RepublishUnconfirmedOnReconnect), the automatic resend of
  what was left unconfirmed. At the end of the batch the thread calls
  WaitForConfirms and logs the result (all confirmed or not, and how long it took).

  Two subtleties of confirms that the screen makes visible:
  - A connection drop does NOT fire OnConfirm: the publishes pending at the drop
    are marked in the list by the reconnection event ("Reenviada (novo nº)" with
    resend on, "Perdida na queda" without). The resend happens with NEW
    seq-nos assigned by the lib, so the confirms of those resends arrive
    without a matching row; they go into the "reenvios confirmados" counter.
  - An unroutable mandatory publish is RETURNED (Basic.Return) and still
    CONFIRMED (ack): confirm means "the broker took responsibility",
    not "it reached a queue". The row becomes "Devolvida (sem rota)".

  When sending a message fails (socket down), the batch thread does NOT
  republish on its own: the seq-no was assigned and the content went into the
  resend buffer BEFORE the send, so republishing by hand would duplicate the
  message. It only waits for the reconnection (TEvent signaled by OnReconnect)
  and moves on to the NEXT message of the batch.

  Compiles in both worlds from the SAME source (same pattern as the other
  GUI samples): uses without a namespace prefix, callbacks as named
  methods ('of object', FPC rule) and UI updates coming from other
  threads through disposable "marshal" objects + TThread.Queue: FPC only
  accepts a TThreadMethod without parameters, and a shared field on the form
  would race between concurrent callbacks (see RetaguardaVcl). Exception: the
  connection events can NOT post TThread.Queue directly; they run on the
  lib's reconnection thread, which dies right afterwards, and on FPC that
  drops the pending post; see the comment on TConexaoEventoWork. }

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
  SyncObjs, Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.Threading, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  TfrmPublicador = class;

  { Publishes the batch off the UI thread, with a pause between messages, so the
    screen stays alive and there is time to bring the broker down mid-batch. }
  TLoteThread = class(TThread)
  private
    FForm: TfrmPublicador;
    FCanal: TAMQPChannel;
    FFila: string;
    FQtd: Integer;
    FIntervaloMs: Integer;
    FParar: Boolean;
    FReconectado: TEvent; // signaled by the connection's OnReconnect
    function AguardarReconexao: Boolean;
    function PausaInterrompivel(AMs: Integer): Boolean;
  protected
    procedure Execute; override;
  public
    constructor Create(AForm: TfrmPublicador; ACanal: TAMQPChannel;
      const AFila: string; AQtd, AIntervaloMs: Integer);
    destructor Destroy; override;
    procedure Parar;
    procedure SinalizarReconexao;
  end;

  TfrmPublicador = class(TForm)
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
    chkRepublicar: TCheckBox;
    btnConectar: TButton;
    lblStatus: TLabel;
    gbPublicacao: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblQtd: TLabel;
    edtQtd: TEdit;
    lblIntervalo: TLabel;
    edtIntervalo: TEdit;
    btnPublicar: TButton;
    btnSemRota: TButton;
    lvMensagens: TListView;
    lblContagem: TLabel;
    btnLimparLista: TButton;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnPublicarClick(Sender: TObject);
    procedure btnSemRotaClick(Sender: TObject);
    procedure btnLimparListaClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FFila: string;          // queue declared on connect (the batch publishes to it)
    FRepublicando: Boolean; // snapshot of the checkbox at connect time
    FLote: TLoteThread;
    // Protects FLote between the UI (creates/destroys) and the reconnection thread
    // (SinalizarReconexao). Never call WaitFor while holding this lock.
    FLoteLock: TCriticalSection;
    // Row maps of the list; touched ONLY on the UI thread (through marshals).
    FPorSeq: TDictionary<UInt64, TListItem>;
    FPorChave: TDictionary<string, TListItem>;
    FPublicadas: Integer;
    FConfirmadas: Integer;
    FNacks: Integer;
    FDevolvidas: Integer;
    FReenviosConfirmados: Integer;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    procedure SetLoteRodando(ARodando: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure AtualizarContagem;
    /// Ends the batch thread (if any), waiting for it to exit. Called only on the
    /// UI thread (disconnect/close). The "Parar lote" click does NOT use
    /// this: it only signals and leaves the join to LoteConcluido, without freezing the UI.
    procedure PararLote;
    // UI updates (run on the UI thread, called by the marshals):
    procedure MsgPublicada(ASeqNo: UInt64; const AChave: string);
    procedure MsgConfirmada(ASeqNo: UInt64; AAck: Boolean);
    procedure MsgDevolvida(const AChave, AMotivo: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    procedure LoteConcluido;
    // Marshalling to the UI thread (callable from any thread):
    procedure QueueLog(const AMsg: string);
    procedure QueuePublicada(ASeqNo: UInt64; const AChave: string);
    procedure QueueLoteConcluido;
    // Lib callbacks (run on pool / reconnection threads):
    procedure OnConfirmado(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
    procedure OnDevolvida(AChannel: TAMQPChannel; const AReturned: TAMQPReturnedMessage);
    procedure OnDesconectado(AConnection: TAMQPConnection);
    procedure OnReconectado(AConnection: TAMQPConnection);
    procedure OnReconexaoFalhou(AConnection: TAMQPConnection);
  end;

var
  frmPublicador: TfrmPublicador;

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
  cStatusPendente   = 'Aguardando confirmação';
  cStatusConfirmada = 'Confirmada';
  cStatusNack       = 'Nack do broker';
  cStatusDevolvida  = 'Devolvida (sem rota)';
  cStatusReenviada  = 'Reenviada (novo nº)';
  cStatusPerdida    = 'Perdida na queda (sem reenvio)';

// Builds the properties and publishes an invoice (body = the key itself, as in
// the other samples). Used by the batch thread and by the mandatory button.
function PublicarNota(ACanal: TAMQPChannel; const ARota, AChave: string;
  AMandatory: Boolean): UInt64;
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LProps.SetPersistent; // survives a broker restart (the queue is durable too)
  LProps.SetMessageId(AChave);
  Result := ACanal.Publish('', ARota, AmqpUtf8Encode(AChave), LProps, AMandatory);
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See RetaguardaVcl.
  TLogMarshal = class
    Form: TfrmPublicador;
    Texto: string;
    procedure Execute;
  end;

  TPublicadaMarshal = class
    Form: TfrmPublicador;
    Seq: UInt64;
    Chave: string;
    procedure Execute;
  end;

  TConfirmadaMarshal = class
    Form: TfrmPublicador;
    Seq: UInt64;
    Ack: Boolean;
    procedure Execute;
  end;

  TDevolvidaMarshal = class
    Form: TfrmPublicador;
    Chave, Motivo: string;
    procedure Execute;
  end;

  TConexaoEvento = (ceCaiu, ceVoltou, ceFalhou);

  TConexaoMarshal = class
    Form: TfrmPublicador;
    Evento: TConexaoEvento;
    procedure Execute;
  end;

  TLoteFimMarshal = class
    Form: TfrmPublicador;
    procedure Execute;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(Texto);
  Free;
end;

procedure TPublicadaMarshal.Execute;
begin
  Form.MsgPublicada(Seq, Chave);
  Free;
end;

procedure TConfirmadaMarshal.Execute;
begin
  Form.MsgConfirmada(Seq, Ack);
  Free;
end;

procedure TDevolvidaMarshal.Execute;
begin
  Form.MsgDevolvida(Chave, Motivo);
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

procedure TLoteFimMarshal.Execute;
begin
  Form.LoteConcluido;
  Free;
end;

type
  { The connection events (OnDisconnect/OnReconnect/OnReconnectFailed) run
    on the lib's RECONNECTION thread, which ends right after OnReconnect, and on
    FPC a TThread.Queue posted by a thread that dies before the main thread
    pumps the queue is DROPPED: TThread.Destroy calls
    RemoveQueuedEvents(Self), which matches by the poster's ThreadID (stamped
    by InternalQueue even when nil is passed in AThread). On Delphi the removal
    matches only by the Thread field (which stays nil), so there the direct post
    would work. Observed symptom: the OnDisconnect marshal arrived (the
    reconnection thread is still alive during the retries) and the OnReconnect one
    vanished. Hopping through a PcPool worker (PERSISTENT threads) guarantees
    that the post to the UI survives on both compilers. }
  TConexaoEventoWork = class(TPcWorkItem)
  private
    FForm: TfrmPublicador;
    FEvento: TConexaoEvento;
  public
    constructor Create(AForm: TfrmPublicador; AEvento: TConexaoEvento);
    procedure Execute; override;
  end;

constructor TConexaoEventoWork.Create(AForm: TfrmPublicador;
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

{ TLoteThread }

constructor TLoteThread.Create(AForm: TfrmPublicador; ACanal: TAMQPChannel;
  const AFila: string; AQtd, AIntervaloMs: Integer);
begin
  FForm := AForm;
  FCanal := ACanal;
  FFila := AFila;
  FQtd := AQtd;
  FIntervaloMs := AIntervaloMs;
  FReconectado := TEvent.Create(nil, False, False, ''); // auto-reset
  inherited Create(False);
end;

destructor TLoteThread.Destroy;
begin
  inherited;
  FReconectado.Free;
end;

procedure TLoteThread.Parar;
begin
  FParar := True;
end;

procedure TLoteThread.SinalizarReconexao;
begin
  FReconectado.SetEvent;
end;

function TLoteThread.AguardarReconexao: Boolean;
begin
  while not FParar do
    if FReconectado.WaitFor(300) = wrSignaled then
      Exit(True);
  Result := False;
end;

function TLoteThread.PausaInterrompivel(AMs: Integer): Boolean;
var
  LRestante, LFatia: Integer;
begin
  LRestante := AMs;
  while (LRestante > 0) and (not FParar) do
  begin
    if LRestante < 100 then
      LFatia := LRestante
    else
      LFatia := 100;
    Sleep(LFatia);
    Dec(LRestante, LFatia);
  end;
  Result := not FParar;
end;

procedure TLoteThread.Execute;
var
  I: Integer;
  LChave, LExecucao: string;
  LSeq: UInt64;
  LInicio: UInt64;
begin
  LExecucao := FormatDateTime('hhnnsszzz', Now);
  for I := 1 to FQtd do
  begin
    if FParar then
      Break;
    LChave := Format('NFE-%s-%.4d', [LExecucao, I]);
    try
      LSeq := PublicarNota(FCanal, FFila, LChave, False);
      FForm.QueuePublicada(LSeq, LChave);
    except
      on E: Exception do
      begin
        // The seq-no was assigned and the content went into the resend buffer (if
        // enabled) BEFORE the send failed; the lib republishes on its own after
        // reconnecting. Publishing again here would DUPLICATE the message; we only
        // wait for the connection to come back and move on to the next one.
        FForm.QueueLog(Format('Falha ao enviar %s: %s — aguardando reconexão.',
          [LChave, E.Message]));
        if not AguardarReconexao then
          Break;
        FForm.QueueLog('Conexão restabelecida; retomando o lote.');
      end;
    end;
    if not PausaInterrompivel(FIntervaloMs) then
      Break;
  end;

  if not FParar then
  begin
    FForm.QueueLog('Lote enviado. Aguardando confirmações pendentes (WaitForConfirms)...');
    LInicio := PcTickMs;
    try
      if FCanal.WaitForConfirms(5000) then
        FForm.QueueLog(Format('WaitForConfirms: todas confirmadas (%d ms).',
          [Int64(PcTickMs - LInicio)]))
      else
        FForm.QueueLog('WaitForConfirms: nem todas confirmadas (nack, perda na queda ou timeout).');
    except
      on E: Exception do
        FForm.QueueLog('WaitForConfirms falhou: ' + E.Message);
    end;
  end;
  FForm.QueueLoteConcluido;
end;

{ TfrmPublicador }

procedure TfrmPublicador.FormCreate(Sender: TObject);
begin
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FPorSeq := TDictionary<UInt64, TListItem>.Create;
  FPorChave := TDictionary<string, TListItem>.Create;
  FLoteLock := TCriticalSection.Create;
  SetConectado(False);
  AtualizarContagem;
end;

procedure TfrmPublicador.FormDestroy(Sender: TObject);
begin
  FLoteLock.Free;
  FPorChave.Free;
  FPorSeq.Free;
end;

procedure TfrmPublicador.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  PararLote;
  FreeAndNil(FChannel);
  // The Free above drains the callbacks in flight (OnConfirm/OnBasicReturn); the
  // marshals they posted are still in the TThread.Queue queue, and pumping
  // here drains them with the form still alive (see RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // Freeing the connection ends the reconnection thread, so no more connection
  // events are born. A TConexaoEventoWork already queued on the pool may be
  // posting the last marshal RIGHT NOW: the short wait + pump drain
  // that straggler with the form still alive.
  Sleep(100);
  Application.ProcessMessages;
end;

function TfrmPublicador.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmPublicador.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmPublicador.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmPublicador.btnLimparListaClick(Sender: TObject);
begin
  lvMensagens.Items.Clear;
  FPorSeq.Clear;
  FPorChave.Clear;
  FPublicadas := 0;
  FConfirmadas := 0;
  FNacks := 0;
  FDevolvidas := 0;
  FReenviosConfirmados := 0;
  AtualizarContagem;
end;

procedure TfrmPublicador.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmPublicador.BuildParams: TAMQPConnectionParams;
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
  // The point of the sample: automatic reconnection always on, and the resend of
  // unconfirmed publishes opt-in through the checkbox.
  Result.AutoReconnect := True;
  Result.ReconnectDelayMs := 2000;
  Result.MaxReconnectAttempts := 0; // keeps trying until it comes back
  Result.RepublishUnconfirmedOnReconnect := chkRepublicar.Checked;
  Result.ConnectionName := 'PublicadorConfiavelVcl';
end;

procedure TfrmPublicador.SetConectado(AConectado: Boolean);
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
  chkRepublicar.Enabled := not AConectado;
  edtQueue.Enabled := not AConectado;
  btnPublicar.Enabled := AConectado;
  btnSemRota.Enabled := AConectado;
end;

procedure TfrmPublicador.SetLoteRodando(ARodando: Boolean);
begin
  if ARodando then
    btnPublicar.Caption := 'Parar lote'
  else
    btnPublicar.Caption := 'Publicar lote';
  btnPublicar.Enabled := Assigned(FConn);
  edtQtd.Enabled := not ARodando;
  edtIntervalo.Enabled := not ARodando;
end;

procedure TfrmPublicador.AtualizarContagem;
begin
  lblContagem.Caption := Format(
    'Publicadas: %d | Confirmadas: %d | Nacks: %d | Devolvidas: %d | Reenvios confirmados: %d',
    [FPublicadas, FConfirmadas, FNacks, FDevolvidas, FReenviosConfirmados]);
end;

procedure TfrmPublicador.PararLote;
var
  LLote: TLoteThread;
begin
  FLoteLock.Enter;
  try
    LLote := FLote;
    FLote := nil;
  finally
    FLoteLock.Leave;
  end;
  if LLote = nil then
    Exit;
  LLote.Parar;
  LLote.WaitFor;
  LLote.Free;
  SetLoteRodando(False);
end;

{ --- updates on the UI thread --------------------------------------------- }

procedure TfrmPublicador.MsgPublicada(ASeqNo: UInt64; const AChave: string);
var
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(lvMensagens.Handle);
  LItem := lvMensagens.Items.Add;
  LItem.Caption := IntToStr(Int64(ASeqNo));
  LItem.SubItems.Add(AChave);
  LItem.SubItems.Add(cStatusPendente);
  LItem.SubItems.Add(FormatDateTime('hh:nn:ss', Now));
  LItem.SubItems.Add('');
  FPorSeq.AddOrSetValue(ASeqNo, LItem);
  FPorChave.AddOrSetValue(AChave, LItem);
  Inc(FPublicadas);
  AtualizarContagem;
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmPublicador.MsgConfirmada(ASeqNo: UInt64; AAck: Boolean);
var
  LItem: TListItem;
begin
  if not FPorSeq.TryGetValue(ASeqNo, LItem) then
  begin
    // Seq-no without a row: it is the confirm of an automatic post-drop resend
    // (the lib resends with new seq-nos; the old->new mapping is internal).
    if AAck then
    begin
      Inc(FReenviosConfirmados);
      Log(Format('Reenvio confirmado pelo broker (nº %d).', [Int64(ASeqNo)]));
    end
    else
    begin
      Inc(FNacks);
      Log(Format('Broker nack-ou o reenvio nº %d.', [Int64(ASeqNo)]));
    end;
    AtualizarContagem;
    Exit;
  end;

  if AAck then
  begin
    Inc(FConfirmadas);
    // Does not downgrade a more informative status (e.g. "Devolvida (sem rota)":
    // the broker returns AND confirms an unroutable mandatory).
    if LItem.SubItems[1] = cStatusPendente then
      LItem.SubItems[1] := cStatusConfirmada;
  end
  else
  begin
    Inc(FNacks);
    LItem.SubItems[1] := cStatusNack;
    Log(Format('Broker nack-ou o publish nº %d.', [Int64(ASeqNo)]));
  end;
  LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
  AtualizarContagem;
end;

procedure TfrmPublicador.MsgDevolvida(const AChave, AMotivo: string);
var
  LItem: TListItem;
begin
  Log('Basic.Return: "' + AChave + '" devolvida (' + AMotivo + ').');
  if FPorChave.TryGetValue(AChave, LItem) then
  begin
    LItem.SubItems[1] := cStatusDevolvida;
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
  end;
  Inc(FDevolvidas);
  AtualizarContagem;
end;

procedure TfrmPublicador.ConexaoCaiu;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  if FRepublicando then
    Log('Conexão caiu. Reconexão automática em andamento; publishes sem ' +
      'confirmação serão reenviados após reconectar.')
  else
    Log('Conexão caiu. Reconexão automática em andamento; publishes sem ' +
      'confirmação NÃO serão reenviados (checkbox desmarcado).');
end;

procedure TfrmPublicador.ConexaoVoltou;
var
  I: Integer;
  LItem: TListItem;
begin
  SetConectado(True);
  Log('Reconectado: topologia e confirm mode restaurados' +
    IfThen(FRepublicando, '; reenvio automático disparado.', '.'));
  // Rows left pending at the drop will never get the confirm for the old
  // seq-no; mark their outcome here.
  for I := 0 to lvMensagens.Items.Count - 1 do
  begin
    LItem := lvMensagens.Items[I];
    if LItem.SubItems[1] = cStatusPendente then
    begin
      if FRepublicando then
        LItem.SubItems[1] := cStatusReenviada
      else
        LItem.SubItems[1] := cStatusPerdida;
      LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    end;
  end;
end;

procedure TfrmPublicador.ConexaoFalhou;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

procedure TfrmPublicador.LoteConcluido;
var
  LLote: TLoteThread;
begin
  FLoteLock.Enter;
  try
    LLote := FLote;
    FLote := nil;
  finally
    FLoteLock.Leave;
  end;
  if LLote <> nil then
  begin
    LLote.WaitFor; // the thread already posted this marshal at the end of Execute; quick join
    LLote.Free;
  end;
  SetLoteRodando(False);
  Log('Lote finalizado.');
end;

{ --- marshalling (callable from any thread) ------------------------------- }

procedure TfrmPublicador.QueueLog(const AMsg: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Texto := AMsg;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmPublicador.QueuePublicada(ASeqNo: UInt64; const AChave: string);
var
  LMarshal: TPublicadaMarshal;
begin
  LMarshal := TPublicadaMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Seq := ASeqNo;
  LMarshal.Chave := AChave;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmPublicador.QueueLoteConcluido;
var
  LMarshal: TLoteFimMarshal;
begin
  LMarshal := TLoteFimMarshal.Create;
  LMarshal.Form := Self;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ --- lib callbacks (pool / reconnection threads) -------------------------- }

procedure TfrmPublicador.OnConfirmado(AChannel: TAMQPChannel; ASeqNo: UInt64;
  AAck: Boolean);
var
  LMarshal: TConfirmadaMarshal;
begin
  LMarshal := TConfirmadaMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Seq := ASeqNo;
  LMarshal.Ack := AAck;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmPublicador.OnDevolvida(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
var
  LMarshal: TDevolvidaMarshal;
begin
  LMarshal := TDevolvidaMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Chave := AReturned.BodyAsText;
  LMarshal.Motivo := Format('%d %s', [AReturned.ReplyCode, AReturned.ReplyText]);
  TThread.Queue(nil, LMarshal.Execute);
end;

// The three connection events hop through PcPool instead of posting directly:
// the reconnection thread dies right after OnReconnect and, on FPC, would take
// the pending post with it (see the comment on TConexaoEventoWork).
procedure TfrmPublicador.OnDesconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmPublicador.OnReconectado(AConnection: TAMQPConnection);
begin
  // Wakes the batch thread (if it is waiting for the connection to come back). Under
  // the lock: the UI may be destroying the thread at this very moment.
  FLoteLock.Enter;
  try
    if Assigned(FLote) then
      FLote.SinalizarReconexao;
  finally
    FLoteLock.Leave;
  end;
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmPublicador.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- actions ---------------------------------------------------------------- }

procedure TfrmPublicador.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
      PararLote; // the batch thread publishes on the channel we are about to close
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
  FFila := Trim(edtQueue.Text);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnDesconectado;
    FConn.OnReconnect := OnReconectado;
    FConn.OnReconnectFailed := OnReconexaoFalhou;
    FConn.Open;

    FChannel := FConn.CreateChannel;
    FChannel.OnConfirm := OnConfirmado;
    FChannel.OnBasicReturn := OnDevolvida;
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(FFila, True)); // durable
    FChannel.ConfirmSelect;

    FRepublicando := chkRepublicar.Checked;
    // New connection = new seq-no numbering (restarts at 1): clears the list
    // so the old rows do not collide with the new seq-nos.
    btnLimparListaClick(nil);

    Log(Format('Conectado a %s:%d%s. Confirm mode ativo na fila "%s"; ' +
      'reenvio na reconexão: %s.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FFila,
       IfThen(FRepublicando, 'ligado', 'desligado')]));
    Log('Dica: publique um lote e derrube o broker no meio ' +
      '(docker stop delphi-amqp-faa-rabbitmq) para ver a reconexão e o reenvio.');
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

procedure TfrmPublicador.btnPublicarClick(Sender: TObject);
var
  LQtd, LIntervalo: Integer;
begin
  FLoteLock.Enter;
  try
    if Assigned(FLote) then
    begin
      // Only signals; the join is left to LoteConcluido (the thread may be in a
      // WaitForConfirms; a WaitFor here would freeze the UI for seconds).
      FLote.Parar;
      btnPublicar.Enabled := False;
      btnPublicar.Caption := 'Parando...';
      Exit;
    end;
  finally
    FLoteLock.Leave;
  end;

  if not Assigned(FChannel) then
  begin
    Log('Conecte antes de publicar.');
    Exit;
  end;
  LQtd := StrToIntDef(Trim(edtQtd.Text), 0);
  if LQtd <= 0 then
  begin
    Log('Quantidade inválida.');
    Exit;
  end;
  LIntervalo := StrToIntDef(Trim(edtIntervalo.Text), 300);

  FLoteLock.Enter;
  try
    FLote := TLoteThread.Create(Self, FChannel, FFila, LQtd, LIntervalo);
  finally
    FLoteLock.Leave;
  end;
  SetLoteRodando(True);
  Log(Format('Lote de %d mensagens iniciado (intervalo de %d ms).',
    [LQtd, LIntervalo]));
end;

procedure TfrmPublicador.btnSemRotaClick(Sender: TObject);
var
  LChave: string;
  LSeq: UInt64;
begin
  if not Assigned(FChannel) then
  begin
    Log('Conecte antes de publicar.');
    Exit;
  end;
  LChave := 'SEM-ROTA-' + FormatDateTime('hhnnsszzz', Now);
  try
    // mandatory=True for a route that does not exist: the broker returns the
    // message (Basic.Return) and STILL confirms it (ack); confirm
    // means "I took responsibility", not "I routed it to a queue".
    LSeq := PublicarNota(FChannel, 'fila-inexistente-demo', LChave, True);
    MsgPublicada(LSeq, LChave); // we are already on the UI thread
    Log('Publicada "' + LChave + '" (mandatory) para rota inexistente — ' +
      'aguarde o Basic.Return.');
  except
    on E: Exception do
      Log('Erro ao publicar sem rota: ' + E.Message);
  end;
end;

end.
