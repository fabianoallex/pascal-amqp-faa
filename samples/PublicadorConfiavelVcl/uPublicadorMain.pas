unit uPublicadorMain;

{ Publicador confiável: vitrine dos publisher confirms da lib numa tela só.

  O canal é colocado em confirm mode (ConfirmSelect) logo após conectar; cada
  Publish recebe um seq-no e vira uma linha na lista, que muda de status quando
  o broker confirma (OnConfirm/ack), rejeita (nack) ou devolve um publish
  `mandatory` sem rota (OnBasicReturn). O lote é publicado por uma thread
  própria (TLoteThread), com intervalo configurável entre mensagens — assim dá
  pra derrubar o broker NO MEIO do lote (docker stop/start no RabbitMQ) e
  assistir a sequência completa: falha de envio, reconexão automática, replay
  da topologia + confirm mode e, com "Reenviar não confirmadas na reconexão"
  marcado (RepublishUnconfirmedOnReconnect), o reenvio automático do que ficou
  sem confirmação. No fim do lote a thread chama WaitForConfirms e loga o
  resultado (todas confirmadas ou não, e em quanto tempo).

  Duas sutilezas de confirms que a tela deixa visíveis:
  - Queda de conexão NÃO dispara OnConfirm: os publishes pendentes na queda
    são marcados na lista pelo evento de reconexão ("Reenviada (novo nº)" com
    reenvio ligado, "Perdida na queda" sem). O reenvio acontece com seq-nos
    NOVOS atribuídos pela lib, então as confirmações desses reenvios chegam
    sem linha correspondente — entram no contador "reenvios confirmados".
  - Um publish mandatory sem rota é DEVOLVIDO (Basic.Return) e mesmo assim
    CONFIRMADO (ack): confirm significa "o broker assumiu a responsabilidade",
    não "chegou numa fila". A linha fica "Devolvida (sem rota)".

  Quando o envio de uma mensagem falha (socket caído), a thread do lote NÃO
  re-publica por conta própria: o seq-no foi atribuído e o conteúdo entrou no
  buffer de reenvio ANTES do envio, então republicar manualmente duplicaria a
  mensagem. Ela só espera a reconexão (TEvent sinalizado pelo OnReconnect) e
  segue para a PRÓXIMA mensagem do lote.

  Compila nos dois mundos a partir do MESMO fonte (mesmo padrão dos outros
  samples GUI): uses sem prefixo de namespace, callbacks como métodos
  nomeados ('of object', regra do FPC) e atualizações de UI vindas de outras
  threads via objetos de "marshal" descartáveis + TThread.Queue — o FPC só
  aceita TThreadMethod sem parâmetros, e um campo compartilhado na form teria
  corrida entre callbacks concorrentes (ver RetaguardaVcl). Exceção: os
  eventos de conexão NÃO podem postar TThread.Queue direto — eles rodam na
  thread de reconexão da lib, que morre logo em seguida, e no FPC isso
  descarta o post pendente; ver o comentário de TConexaoEventoWork. }

interface

uses
  // No FPC, a camada de emulacao da LCL (LCLIntf/LCLType/LMessages) cobre as
  // chamadas WinAPI do autoscroll do log em qualquer widgetset (win32, gtk2...).
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes, StrUtils,
  SyncObjs, Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, AMQP.Threading, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  TfrmPublicador = class;

  { Publica o lote fora da thread da UI, com pausa entre mensagens, para a
    tela continuar viva e dar tempo de derrubar o broker no meio do lote. }
  TLoteThread = class(TThread)
  private
    FForm: TfrmPublicador;
    FCanal: TAMQPChannel;
    FFila: string;
    FQtd: Integer;
    FIntervaloMs: Integer;
    FParar: Boolean;
    FReconectado: TEvent; // sinalizado pelo OnReconnect da conexão
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
    FFila: string;          // fila declarada na conexão (o lote publica nela)
    FRepublicando: Boolean; // snapshot do checkbox no momento da conexão
    FLote: TLoteThread;
    // Protege FLote entre a UI (cria/destrói) e a thread de reconexão
    // (SinalizarReconexao). Nunca se faz WaitFor segurando este lock.
    FLoteLock: TCriticalSection;
    // Mapas linha-da-lista; tocados SOMENTE na thread da UI (via marshals).
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
    /// Encerra a thread do lote (se houver) aguardando ela sair. Chamado só na
    /// thread da UI (desconectar/fechar). O clique em "Parar lote" NÃO usa
    /// isto: só sinaliza e deixa o join para LoteConcluido, sem travar a UI.
    procedure PararLote;
    // Atualizações de UI (rodam na thread da UI, chamadas pelos marshals):
    procedure MsgPublicada(ASeqNo: UInt64; const AChave: string);
    procedure MsgConfirmada(ASeqNo: UInt64; AAck: Boolean);
    procedure MsgDevolvida(const AChave, AMotivo: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    procedure LoteConcluido;
    // Marshalling para a thread da UI (chamável de qualquer thread):
    procedure QueueLog(const AMsg: string);
    procedure QueuePublicada(ASeqNo: UInt64; const AChave: string);
    procedure QueueLoteConcluido;
    // Callbacks da lib (rodam em threads do pool / de reconexão):
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
  // A LCL nao tem a unit Messages; LM_VSCROLL tem o mesmo valor do WM_VSCROLL.
  WM_VSCROLL = LM_VSCROLL;
{$ENDIF}

const
  cStatusPendente   = 'Aguardando confirmação';
  cStatusConfirmada = 'Confirmada';
  cStatusNack       = 'Nack do broker';
  cStatusDevolvida  = 'Devolvida (sem rota)';
  cStatusReenviada  = 'Reenviada (novo nº)';
  cStatusPerdida    = 'Perdida na queda (sem reenvio)';

// Monta as propriedades e publica uma nota (corpo = a própria chave, como nos
// outros samples). Usado pela thread do lote e pelo botão mandatory.
function PublicarNota(ACanal: TAMQPChannel; const ARota, AChave: string;
  AMandatory: Boolean): UInt64;
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LProps.SetPersistent; // sobrevive a restart do broker (fila também é durável)
  LProps.SetMessageId(AChave);
  Result := ACanal.Publish('', ARota, AmqpUtf8Encode(AChave), LProps, AMandatory);
end;

type
  // Um objeto por chamada: TThread.Queue no FPC so' aceita 'procedure of
  // object' SEM PARAMETROS, entao os dados viajam num objeto descartavel
  // (nao num campo compartilhado da form, que teria corrida entre callbacks
  // concorrentes). Se autodestroi apos rodar. Ver RetaguardaVcl.
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
  { Os eventos de conexão (OnDisconnect/OnReconnect/OnReconnectFailed) rodam
    na thread de RECONEXÃO da lib, que termina logo após o OnReconnect — e no
    FPC um TThread.Queue postado por uma thread que morre antes de a thread
    principal bombear a fila é DESCARTADO: TThread.Destroy chama
    RemoveQueuedEvents(Self), que casa pelo ThreadID do postador (carimbado
    pelo InternalQueue mesmo passando nil em AThread). No Delphi o descarte
    casa só pelo campo Thread (que fica nil), então lá o post direto
    funcionaria. Sintoma observado: o marshal do OnDisconnect chegava (a
    thread de reconexão ainda vive durante os retries) e o do OnReconnect
    sumia. O salto por um worker do AmqpPool — threads PERSISTENTES — garante
    que o post à UI sobreviva nos dois compiladores. }
  TConexaoEventoWork = class(TAMQPWorkItem)
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
        // O seq-no foi atribuído e o conteúdo entrou no buffer de reenvio (se
        // habilitado) ANTES do envio falhar — a lib re-publica sozinha após
        // reconectar. Publicar de novo aqui DUPLICARIA a mensagem; só
        // esperamos a conexão voltar e seguimos para a próxima.
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
    LInicio := AmqpTickMs;
    try
      if FCanal.WaitForConfirms(5000) then
        FForm.QueueLog(Format('WaitForConfirms: todas confirmadas (%d ms).',
          [Int64(AmqpTickMs - LInicio)]))
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
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
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
  // O Free acima drena os callbacks em voo (OnConfirm/OnBasicReturn); os
  // marshals que eles postaram ainda estao na fila do TThread.Queue — bombear
  // aqui os drena com a form ainda viva (ver RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // O Free da conexao encerra a thread de reconexao — nao nascem mais eventos
  // de conexao. Um TConexaoEventoWork ja enfileirado no pool pode estar
  // postando o ultimo marshal NESTE instante: a espera curta + bombeio drenam
  // esse retardatario com a form ainda viva.
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
    Exit(True); // sem scrollbar ainda (conteudo cabe todo) = considera "no fim"
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
  // O ponto do sample: reconexão automática sempre ligada, e o reenvio dos
  // publishes não confirmados opt-in pelo checkbox.
  Result.AutoReconnect := True;
  Result.ReconnectDelayMs := 2000;
  Result.MaxReconnectAttempts := 0; // insiste até voltar
  Result.RepublishUnconfirmedOnReconnect := chkRepublicar.Checked;
  Result.ConnectionName := 'PublicadorConfiavelVcl';
end;

procedure TfrmPublicador.SetConectado(AConectado: Boolean);
begin
  if AConectado then
  begin
    btnConectar.Caption := 'Desconectar';
    // Com TLS, mostra o motor que CARREGOU de fato (o OpenSSL publica versão
    // e DLL/soname na 1ª conexão — ver AmqpTlsBackendInfo).
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

{ --- atualizações na thread da UI ---------------------------------------- }

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
    // Seq-no sem linha: é a confirmação de um reenvio automático pós-queda
    // (a lib reenvia com seq-nos novos; o mapeamento antigo->novo é interno).
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
    // Não rebaixa um status mais informativo (ex.: "Devolvida (sem rota)" —
    // o broker devolve E confirma um mandatory sem rota).
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
  // Linhas que ficaram pendentes na queda nunca receberão o confirm do seq-no
  // antigo — marca o desfecho delas aqui.
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
    LLote.WaitFor; // a thread já postou este marshal no fim do Execute; join rápido
    LLote.Free;
  end;
  SetLoteRodando(False);
  Log('Lote finalizado.');
end;

{ --- marshalling (chamável de qualquer thread) ---------------------------- }

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

{ --- callbacks da lib (threads do pool / reconexão) ------------------------ }

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

// Os três eventos de conexão saltam pelo AmqpPool em vez de postar direto:
// a thread de reconexão morre logo após o OnReconnect e, no FPC, levaria o
// post pendente junto (ver o comentário de TConexaoEventoWork).
procedure TfrmPublicador.OnDesconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmPublicador.OnReconectado(AConnection: TAMQPConnection);
begin
  // Acorda a thread do lote (se estiver esperando a conexão voltar). Sob o
  // lock: a UI pode estar destruindo a thread neste exato momento.
  FLoteLock.Enter;
  try
    if Assigned(FLote) then
      FLote.SinalizarReconexao;
  finally
    FLoteLock.Leave;
  end;
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmPublicador.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- ações ----------------------------------------------------------------- }

procedure TfrmPublicador.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
      PararLote; // a thread do lote publica no canal que vamos fechar
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
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(FFila, True)); // durável
    FChannel.ConfirmSelect;

    FRepublicando := chkRepublicar.Checked;
    // Conexão nova = numeração de seq-no nova (recomeça em 1): limpa a lista
    // para as linhas antigas não colidirem com os seq-nos novos.
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
      // Só sinaliza; o join fica para LoteConcluido (a thread pode estar num
      // WaitForConfirms — um WaitFor aqui congelaria a UI por segundos).
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
    // mandatory=True para uma rota que não existe: o broker devolve a
    // mensagem (Basic.Return) e MESMO ASSIM a confirma (ack) — confirm
    // significa "assumi a responsabilidade", não "roteei para uma fila".
    LSeq := PublicarNota(FChannel, 'fila-inexistente-demo', LChave, True);
    MsgPublicada(LSeq, LChave); // já estamos na thread da UI
    Log('Publicada "' + LChave + '" (mandatory) para rota inexistente — ' +
      'aguarde o Basic.Return.');
  except
    on E: Exception do
      Log('Erro ao publicar sem rota: ' + E.Message);
  end;
end;

end.
