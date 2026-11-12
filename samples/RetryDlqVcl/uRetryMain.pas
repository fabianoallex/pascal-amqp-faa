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
  Generics.Collections, Rtti,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, PascalCommon.ThreadPool, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  TfrmRetry = class(TForm)
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
    gbTopologia: TGroupBox;
    lblFila: TLabel;
    edtFila: TEdit;
    lblBackoff: TLabel;
    edtBackoff: TEdit;
    lblMax: TLabel;
    edtMax: TEdit;
    gbProcesso: TGroupBox;
    lblFalhar: TLabel;
    edtFalhar: TEdit;
    lblQtd: TLabel;
    edtQtd: TEdit;
    btnPublicar: TButton;
    btnConsumir: TButton;
    lblContadores: TLabel;
    lvNotas: TListView;
    btnLimparLista: TButton;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnPublicarClick(Sender: TObject);
    procedure btnConsumirClick(Sender: TObject);
    procedure btnLimparListaClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FFilaTrabalho: string;
    FFilaRetry: string;
    FFilaDlq: string;
    FBackoffMs: Integer;
    FMaxTentativas: Integer;
    FFalharPrimeiras: Integer; // captured when consumption starts
    FTagConsumo: string;       // '' = consumption stopped
    FTagDlq: string;
    // Rows by key; touched ONLY on the UI thread (through marshals).
    FItens: TDictionary<string, TListItem>;
    FPublicadas: Integer;
    FProntas: Integer;
    FNaDlq: Integer;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    procedure SetConsumindo(AConsumindo: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure AtualizarContadores;
    procedure CriarTopologia;
    // UI updates (UI thread, through marshals):
    procedure NotaStatus(const AChave: string; ATentativa: Integer;
      const AStatus: string; APronta, ADlq: Boolean);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Marshalling (callable from any thread):
    procedure QueueLog(const AMsg: string);
    procedure QueueStatus(const AChave: string; ATentativa: Integer;
      const AStatus: string; APronta: Boolean = False; ADlq: Boolean = False);
    // Lib callbacks (pool / reconnection threads):
    procedure OnNota(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnNotaDlq(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure OnDesconectado(AConnection: TAMQPConnection);
    procedure OnReconectado(AConnection: TAMQPConnection);
    procedure OnReconexaoFalhou(AConnection: TAMQPConnection);
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
function ContarRejeicoes(const AProps: TAMQPBasicProperties;
  const AFilaTrabalho: string): Integer;
var
  LXDeath, LEntrada, LCampo: TValue;
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
    LEntrada := AmqpUnwrapValue(LXDeath.GetArrayElement(I));
    if not (LEntrada.IsObject and (LEntrada.AsObject is TAMQPFieldTable)) then
      Continue;
    LTab := TAMQPFieldTable(LEntrada.AsObject);
    if not LTab.TryGetValue('queue', LCampo) then
      Continue;
    if LCampo.AsString <> AFilaTrabalho then
      Continue;
    if not LTab.TryGetValue('reason', LCampo) then
      Continue;
    if LCampo.AsString <> 'rejected' then
      Continue;
    if LTab.TryGetValue('count', LCampo) then
      Inc(Result, Integer(LCampo.AsInt64));
  end;
end;

type
  // One object per call: TThread.Queue on FPC only accepts a 'procedure of
  // object' WITHOUT PARAMETERS, so the data travels in a disposable object
  // (not in a shared field of the form, which would race between concurrent
  // callbacks). It frees itself after running. See RetaguardaVcl.
  TLogMarshal = class
    Form: TfrmRetry;
    Texto: string;
    procedure Execute;
  end;

  TStatusMarshal = class
    Form: TfrmRetry;
    Chave: string;
    Tentativa: Integer;
    Status: string;
    Pronta, Dlq: Boolean;
    procedure Execute;
  end;

  TConexaoEvento = (ceCaiu, ceVoltou, ceFalhou);

  TConexaoMarshal = class
    Form: TfrmRetry;
    Evento: TConexaoEvento;
    procedure Execute;
  end;

  { Connection events run on the lib's RECONNECTION thread, which dies right
    after OnReconnect; on FPC a TThread.Queue posted by a thread that dies
    before the pump is DROPPED (gotcha in CLAUDE.md). Hop through a persistent
    PcPool worker. }
  TConexaoEventoWork = class(TPcWorkItem)
  private
    FForm: TfrmRetry;
    FEvento: TConexaoEvento;
  public
    constructor Create(AForm: TfrmRetry; AEvento: TConexaoEvento);
    procedure Execute; override;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(Texto);
  Free;
end;

procedure TStatusMarshal.Execute;
begin
  Form.NotaStatus(Chave, Tentativa, Status, Pronta, Dlq);
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

constructor TConexaoEventoWork.Create(AForm: TfrmRetry; AEvento: TConexaoEvento);
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

{ TfrmRetry }

procedure TfrmRetry.FormCreate(Sender: TObject);
begin
  Randomize;
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FItens := TDictionary<string, TListItem>.Create;
  SetConectado(False);
  AtualizarContadores;
end;

procedure TfrmRetry.FormDestroy(Sender: TObject);
begin
  FItens.Free;
end;

procedure TfrmRetry.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(FChannel) then
  begin
    if FTagConsumo <> '' then
      try FChannel.Cancel(FTagConsumo); except end;
    if FTagDlq <> '' then
      try FChannel.Cancel(FTagDlq); except end;
  end;
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
  LMarshal.Texto := AMsg;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmRetry.QueueStatus(const AChave: string; ATentativa: Integer;
  const AStatus: string; APronta, ADlq: Boolean);
var
  LMarshal: TStatusMarshal;
begin
  LMarshal := TStatusMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Chave := AChave;
  LMarshal.Tentativa := ATentativa;
  LMarshal.Status := AStatus;
  LMarshal.Pronta := APronta;
  LMarshal.Dlq := ADlq;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmRetry.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmRetry.btnLimparListaClick(Sender: TObject);
begin
  lvNotas.Items.Clear;
  FItens.Clear;
  FPublicadas := 0;
  FProntas := 0;
  FNaDlq := 0;
  AtualizarContadores;
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

procedure TfrmRetry.SetConectado(AConectado: Boolean);
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
  edtBackoff.Enabled := not AConectado;
  edtMax.Enabled := not AConectado;
  btnPublicar.Enabled := AConectado;
  btnConsumir.Enabled := AConectado;
end;

procedure TfrmRetry.SetConsumindo(AConsumindo: Boolean);
begin
  if AConsumindo then
    btnConsumir.Caption := 'Parar consumo'
  else
    btnConsumir.Caption := 'Iniciar consumo';
  edtFalhar.Enabled := not AConsumindo;
end;

procedure TfrmRetry.AtualizarContadores;
begin
  lblContadores.Caption := Format('Publicadas: %d | Prontas: %d | Na DLQ: %d',
    [FPublicadas, FProntas, FNaDlq]);
end;

{ Recreates the topology from scratch. The queue arguments are the heart of the
  pattern: dead-letter through the default exchange ('' + routing key = target
  queue) and a queue TTL on the retry queue. The Puts are separate statements
  (chaining that ends in an inline TValue.From<T> gives an internal error on
  FPC 3.2; gotcha in CLAUDE.md); the caller owns the Arguments table and frees
  it after the declare. }
procedure TfrmRetry.CriarTopologia;
var
  LDel: TAMQPQueueDelete;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
begin
  // DeleteQueue: deleting a missing queue is a no-op; an existing queue with old
  // args (the backoff changed) is removed, since redeclaring with different args
  // would be PRECONDITION_FAILED.
  LDel := Default(TAMQPQueueDelete);
  LDel.QueueName := FFilaTrabalho;
  FChannel.DeleteQueue(LDel);
  LDel.QueueName := FFilaRetry;
  FChannel.DeleteQueue(LDel);
  LDel.QueueName := FFilaDlq;
  FChannel.DeleteQueue(LDel);

  // Work queue: rejections (Nack without requeue) fall into the retry queue.
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-dead-letter-exchange', TValue.From<string>(''));
    LArgs.Put('x-dead-letter-routing-key', TValue.From<string>(FFilaRetry));
    LDecl := TAMQPQueueDeclare.Create(FFilaTrabalho, True);
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
    LArgs.Put('x-dead-letter-routing-key', TValue.From<string>(FFilaTrabalho));
    LDecl := TAMQPQueueDeclare.Create(FFilaRetry, True);
    LDecl.Arguments := LArgs;
    FChannel.DeclareQueue(LDecl);
  finally
    LArgs.Free;
  end;

  // DLQ: a plain queue; whoever runs out of attempts is published here by the consumer.
  FChannel.DeclareQueue(TAMQPQueueDeclare.Create(FFilaDlq, True));
end;

{ --- consumption ------------------------------------------------------------ }

// Runs on a pool thread. Current attempt = previous rejections (x-death) + 1.
procedure TfrmRetry.OnNota(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LChave, LStatusDlq: string;
  LTentativa: Integer;
  LProps: TAMQPBasicProperties;
begin
  LChave := ADelivery.BodyAsText;
  LTentativa := ContarRejeicoes(ADelivery.Properties, FFilaTrabalho) + 1;

  QueueStatus(LChave, LTentativa, Format('Processando (tentativa %d)', [LTentativa]));
  Sleep(200 + Random(300)); // simulated processing

  if LTentativa > FFalharPrimeiras then
  begin
    // Success: ack and end the cycle.
    AChannel.Ack(ADelivery.DeliveryTag);
    QueueStatus(LChave, LTentativa,
      Format('Pronta (tentativa %d)', [LTentativa]), True);
    Exit;
  end;

  if LTentativa >= FMaxTentativas then
  begin
    // Exhausted: the CONSUMER decides to stop (the broker does not count attempts
    // on its own); it publishes to the DLQ and acks the original.
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetPersistent;
    AChannel.Publish('', FFilaDlq, AmqpUtf8Encode(LChave), LProps);
    AChannel.Ack(ADelivery.DeliveryTag);
    LStatusDlq := Format('Falhou (tentativa %d) — enviada à DLQ', [LTentativa]);
    QueueStatus(LChave, LTentativa, LStatusDlq);
    QueueLog(Format('Nota %s esgotou as %d tentativas; movida para "%s".',
      [LChave, FMaxTentativas, FFilaDlq]));
    Exit;
  end;

  // Failed with attempts left: Nack WITHOUT requeue => the work queue's DLX
  // sends it to the retry queue, which hands it back after the backoff.
  AChannel.Nack(ADelivery.DeliveryTag, False);
  QueueStatus(LChave, LTentativa,
    Format('Falhou (tentativa %d) — retry em %d ms', [LTentativa, FBackoffMs]));
end;

// DLQ watcher (automatic ack), only to mark the arrival on screen.
procedure TfrmRetry.OnNotaDlq(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
begin
  QueueStatus(ADelivery.BodyAsText, 0, 'Na DLQ', False, True);
end;

procedure TfrmRetry.btnConsumirClick(Sender: TObject);
begin
  if not Assigned(FChannel) then
    Exit;

  if FTagConsumo <> '' then
  begin
    try
      FChannel.Cancel(FTagConsumo);
      if FTagDlq <> '' then
        FChannel.Cancel(FTagDlq);
      Log('Consumo parado.');
    except
      on E: Exception do
        Log('Erro ao parar o consumo: ' + E.Message);
    end;
    FTagConsumo := '';
    FTagDlq := '';
    SetConsumindo(False);
    Exit;
  end;

  FFalharPrimeiras := StrToIntDef(Trim(edtFalhar.Text), 2);
  if FFalharPrimeiras < 0 then
    FFalharPrimeiras := 0;
  try
    FTagConsumo := FChannel.Consume(FFilaTrabalho, OnNota);
    FTagDlq := FChannel.Consume(FFilaDlq, OnNotaDlq, True); // NoAck: only watches
    Log(Format('Consumindo "%s" (falha simulada nas %d primeiras tentativas; ' +
      'máx %d; backoff %d ms).',
      [FFilaTrabalho, FFalharPrimeiras, FMaxTentativas, FBackoffMs]));
    SetConsumindo(True);
  except
    on E: Exception do
      Log('Erro ao iniciar o consumo: ' + E.Message);
  end;
end;

{ --- publishing ------------------------------------------------------------- }

procedure TfrmRetry.btnPublicarClick(Sender: TObject);
var
  I, LQtd: Integer;
  LChave, LExecucao: string;
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  if not Assigned(FChannel) then
    Exit;
  LQtd := StrToIntDef(Trim(edtQtd.Text), 3);
  if LQtd < 1 then
    LQtd := 1;
  LExecucao := FormatDateTime('hhnnsszzz', Now);
  for I := 1 to LQtd do
  begin
    LChave := Format('NFE-%s-%.3d', [LExecucao, I]);
    try
      FChannel.PublishText('', FFilaTrabalho, LChave); // persistent by default
    except
      on E: Exception do
      begin
        Log('Erro ao publicar ' + LChave + ': ' + E.Message);
        Break;
      end;
    end;
    LAtBottom := ScrollAtBottom(lvNotas.Handle);
    LItem := lvNotas.Items.Add;
    LItem.Caption := LChave;
    LItem.SubItems.Add('—');
    LItem.SubItems.Add('Publicada');
    LItem.SubItems.Add(FormatDateTime('hh:nn:ss', Now));
    FItens.AddOrSetValue(LChave, LItem);
    Inc(FPublicadas);
    if LAtBottom then
      LItem.MakeVisible(False);
  end;
  AtualizarContadores;
  Log(Format('%d nota(s) publicada(s) em "%s".', [LQtd, FFilaTrabalho]));
end;

{ --- UI updates ------------------------------------------------------------- }

procedure TfrmRetry.NotaStatus(const AChave: string; ATentativa: Integer;
  const AStatus: string; APronta, ADlq: Boolean);
var
  LItem: TListItem;
begin
  if FItens.TryGetValue(AChave, LItem) then
  begin
    if ATentativa > 0 then
      LItem.SubItems[0] := IntToStr(ATentativa);
    LItem.SubItems[1] := AStatus;
    LItem.SubItems[2] := FormatDateTime('hh:nn:ss', Now);
  end;
  if APronta then
  begin
    Inc(FProntas);
    Log(Format('Nota %s pronta (tentativa %d).', [AChave, ATentativa]));
  end;
  if ADlq then
  begin
    Inc(FNaDlq);
    Log(Format('Nota %s confirmada na DLQ.', [AChave]));
  end;
  if APronta or ADlq then
    AtualizarContadores;
end;

procedure TfrmRetry.ConexaoCaiu;
begin
  lblStatus.Caption := 'Conexão caiu — reconectando...';
  lblStatus.Font.Color := clMaroon;
  Log('Conexão caiu. Reconexão automática em andamento (filas duráveis: nada se perde).');
end;

procedure TfrmRetry.ConexaoVoltou;
begin
  SetConectado(True);
  Log('Reconectado: topologia e consumers restaurados.');
end;

procedure TfrmRetry.ConexaoFalhou;
begin
  lblStatus.Caption := 'Reconexão esgotada';
  lblStatus.Font.Color := clRed;
  Log('Reconexão desistiu (MaxReconnectAttempts atingido).');
end;

// The three connection events hop through PcPool instead of posting directly
// (see the comment on TConexaoEventoWork).
procedure TfrmRetry.OnDesconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmRetry.OnReconectado(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmRetry.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  PcPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- connection ------------------------------------------------------------- }

procedure TfrmRetry.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
      if FTagConsumo <> '' then
        FChannel.Cancel(FTagConsumo);
      if FTagDlq <> '' then
        FChannel.Cancel(FTagDlq);
      FTagConsumo := '';
      FTagDlq := '';
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
  FFilaTrabalho := Trim(edtFila.Text);
  FFilaRetry := FFilaTrabalho + '-retry';
  FFilaDlq := FFilaTrabalho + '-dlq';
  FBackoffMs := StrToIntDef(Trim(edtBackoff.Text), 4000);
  if FBackoffMs < 100 then
    FBackoffMs := 100;
  FMaxTentativas := StrToIntDef(Trim(edtMax.Text), 3);
  if FMaxTentativas < 1 then
    FMaxTentativas := 1;
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnDesconectado;
    FConn.OnReconnect := OnReconectado;
    FConn.OnReconnectFailed := OnReconexaoFalhou;
    FConn.Open;

    FChannel := FConn.CreateChannel;
    FChannel.Qos(8);
    CriarTopologia;

    Log(Format('Conectado a %s:%d%s. Topologia recriada: "%s" (trabalho, DLX' +
      '->retry), "%s" (espera, TTL %d ms, DLX->trabalho), "%s" (DLQ). Máx %d ' +
      'tentativas.',
      [LParams.Host, LParams.Port, LParams.VirtualHost, FFilaTrabalho,
       FFilaRetry, FBackoffMs, FFilaDlq, FMaxTentativas]));
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
