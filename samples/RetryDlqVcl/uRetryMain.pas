unit uRetryMain;

{ Dead-letter + retry com backoff: o padrão de reprocessamento mais usado no
  RabbitMQ, montado só com argumentos de fila (TAMQPFieldTable) — nenhum
  código de agendamento no cliente.

  Topologia (criada ao conectar, a partir da fila base configurada):
    <fila>        fila de trabalho (durável), com dead-letter apontando para
                  <fila>-retry via exchange default ('' + routing key = nome
                  da fila destino — dispensa declarar exchange próprio);
    <fila>-retry  fila de espera SEM consumidor: x-message-ttl = backoff e
                  dead-letter de volta para <fila> — a mensagem "dorme" aqui
                  e o broker a devolve sozinho quando o TTL vence;
    <fila>-dlq    o destino final (dead-letter queue) de quem esgotou as
                  tentativas.

  Ciclo: o consumidor lê a tentativa atual do header x-death (que o broker
  incrementa a cada dead-letter; contamos as entradas reason='rejected' da
  fila de trabalho) e simula o processamento — configurável para falhar as
  primeiras N tentativas. Falhou e ainda há tentativas => Nack(requeue=False),
  que o DLX da fila de trabalho manda para a retry (backoff). Falhou na
  última tentativa => o próprio consumidor publica a mensagem na DLQ e dá
  Ack (é o padrão: o broker não conta tentativas por si — quem decide "chega"
  é o consumidor). Um segundo consumer (ack automático) observa a DLQ só para
  marcar a chegada na tela — em produção a DLQ fica retida para inspeção.

  O backoff usa TTL DA FILA (x-message-ttl na retry), não Expiration por
  mensagem: TTL por mensagem só expira na CABEÇA da fila (uma mensagem de TTL
  longo segura as de TTL curto atrás dela). Com TTL único da fila, a ordem de
  expiração é a de chegada — backoff fixo confiável. (Backoff exponencial =
  N filas de retry com TTLs crescentes; fora do escopo deste sample.)

  Conectar recria a topologia do zero (DeleteQueue + declare): mudar o
  backoff muda os argumentos da fila, e redeclarar fila existente com
  argumentos diferentes é PRECONDITION_FAILED (fecha o canal). Apagar fila
  inexistente é no-op no RabbitMQ, então o caminho é idempotente.

  Compila nos dois mundos a partir do MESMO fonte (padrão dos samples GUI):
  callbacks nomeados ('of object'), marshals descartáveis + TThread.Queue
  para a UI (ver RetaguardaVcl) e eventos de conexão saltando pelo AmqpPool
  (gotcha do TThread.Queue descartado; ver TConexaoEventoWork e CLAUDE.md). }

interface

uses
  // No FPC, a camada de emulacao da LCL (LCLIntf/LCLType/LMessages) cobre as
  // chamadas WinAPI do autoscroll do log em qualquer widgetset (win32, gtk2...).
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes,
  Generics.Collections, Rtti,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, AMQP.Threading, AMQP.Connection, AMQP.Transport,
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
    FFalharPrimeiras: Integer; // capturado ao iniciar o consumo
    FTagConsumo: string;       // '' = consumo parado
    FTagDlq: string;
    // Linhas por chave; tocado SOMENTE na thread da UI (via marshals).
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
    // Atualizações de UI (thread da UI, via marshals):
    procedure NotaStatus(const AChave: string; ATentativa: Integer;
      const AStatus: string; APronta, ADlq: Boolean);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Marshalling (chamável de qualquer thread):
    procedure QueueLog(const AMsg: string);
    procedure QueueStatus(const AChave: string; ATentativa: Integer;
      const AStatus: string; APronta: Boolean = False; ADlq: Boolean = False);
    // Callbacks da lib (threads do pool / de reconexão):
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
  // A LCL nao tem a unit Messages; LM_VSCROLL tem o mesmo valor do WM_VSCROLL.
  WM_VSCROLL = LM_VSCROLL;
{$ENDIF}

{ Soma no header x-death as rejeições (reason='rejected') vindas da fila de
  trabalho — é o nº de tentativas já FALHADAS desta mensagem. O broker mantém
  o x-death a cada dead-letter; a passagem pela retry aparece como
  reason='expired' na fila -retry e é filtrada aqui. Campos TValue: o
  resultado do indexador vai para uma variável local antes do accessor
  (.AsString encadeado direto no indexador dá erro interno no FPC 3.2 —
  gotcha no CLAUDE.md). }
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
    // AmqpUnwrapValue: no FPC o GetArrayElement devolve o elemento
    // re-embrulhado num TValue tkRecord (gotcha no CLAUDE.md).
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
  // Um objeto por chamada: TThread.Queue no FPC so' aceita 'procedure of
  // object' SEM PARAMETROS, entao os dados viajam num objeto descartavel
  // (nao num campo compartilhado da form, que teria corrida entre callbacks
  // concorrentes). Se autodestroi apos rodar. Ver RetaguardaVcl.
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

  { Eventos de conexão rodam na thread de RECONEXÃO da lib, que morre logo
    após o OnReconnect — no FPC um TThread.Queue postado por thread que morre
    antes do bombeio é DESCARTADO (gotcha no CLAUDE.md). Salto por um worker
    persistente do AmqpPool. }
  TConexaoEventoWork = class(TAMQPWorkItem)
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
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
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
  // O Free acima drena os callbacks em voo; os marshals que eles postaram
  // ainda estao na fila do TThread.Queue — bombear aqui os drena com a form
  // ainda viva (ver RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // O Free da conexao encerra a thread de reconexao; um TConexaoEventoWork ja
  // enfileirado no pool pode estar postando o ultimo marshal NESTE instante.
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
    Exit(True); // sem scrollbar ainda (conteudo cabe todo) = considera "no fim"
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

{ Recria a topologia do zero. Os argumentos de fila são o coração do padrão:
  dead-letter via exchange default ('' + routing key = fila destino) e TTL de
  fila na retry. Os Puts ficam em comandos separados (encadear terminando em
  TValue.From<T> inline dá erro interno no FPC 3.2 — gotcha no CLAUDE.md); o
  chamador é dono da tabela de Arguments e a libera após o declare. }
procedure TfrmRetry.CriarTopologia;
var
  LDel: TAMQPQueueDelete;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
begin
  // DeleteQueue: apagar fila inexistente é no-op; fila existente com args
  // antigos (backoff mudou) é removida — redeclarar com args diferentes
  // seria PRECONDITION_FAILED.
  LDel := Default(TAMQPQueueDelete);
  LDel.QueueName := FFilaTrabalho;
  FChannel.DeleteQueue(LDel);
  LDel.QueueName := FFilaRetry;
  FChannel.DeleteQueue(LDel);
  LDel.QueueName := FFilaDlq;
  FChannel.DeleteQueue(LDel);

  // Fila de trabalho: rejeições (Nack sem requeue) caem na retry.
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

  // Retry: sem consumidor; TTL da fila = backoff, e o vencimento dead-lettera
  // de volta para a fila de trabalho.
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

  // DLQ: fila comum; quem esgota as tentativas é publicado aqui pelo consumidor.
  FChannel.DeclareQueue(TAMQPQueueDeclare.Create(FFilaDlq, True));
end;

{ --- consumo ---------------------------------------------------------------- }

// Roda numa thread do pool. Tentativa atual = rejeições anteriores (x-death) + 1.
procedure TfrmRetry.OnNota(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LChave, LStatusDlq: string;
  LTentativa: Integer;
  LProps: TAMQPBasicProperties;
begin
  LChave := ADelivery.BodyAsText;
  LTentativa := ContarRejeicoes(ADelivery.Properties, FFilaTrabalho) + 1;

  QueueStatus(LChave, LTentativa, Format('Processando (tentativa %d)', [LTentativa]));
  Sleep(200 + Random(300)); // processamento simulado

  if LTentativa > FFalharPrimeiras then
  begin
    // Sucesso: confirma e encerra o ciclo.
    AChannel.Ack(ADelivery.DeliveryTag);
    QueueStatus(LChave, LTentativa,
      Format('Pronta (tentativa %d)', [LTentativa]), True);
    Exit;
  end;

  if LTentativa >= FMaxTentativas then
  begin
    // Esgotou: o CONSUMIDOR decide parar (o broker não conta tentativas por
    // conta própria) — publica na DLQ e confirma a original.
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

  // Falhou e ainda há tentativas: Nack SEM requeue => o DLX da fila de
  // trabalho manda para a retry, que devolve após o backoff.
  AChannel.Nack(ADelivery.DeliveryTag, False);
  QueueStatus(LChave, LTentativa,
    Format('Falhou (tentativa %d) — retry em %d ms', [LTentativa, FBackoffMs]));
end;

// Observador da DLQ (ack automático), só para marcar a chegada na tela.
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
    FTagDlq := FChannel.Consume(FFilaDlq, OnNotaDlq, True); // NoAck: só observa
    Log(Format('Consumindo "%s" (falha simulada nas %d primeiras tentativas; ' +
      'máx %d; backoff %d ms).',
      [FFilaTrabalho, FFalharPrimeiras, FMaxTentativas, FBackoffMs]));
    SetConsumindo(True);
  except
    on E: Exception do
      Log('Erro ao iniciar o consumo: ' + E.Message);
  end;
end;

{ --- publicação ------------------------------------------------------------- }

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
      FChannel.PublishText('', FFilaTrabalho, LChave); // persistente por padrão
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

{ --- atualizações de UI ------------------------------------------------------ }

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

// Os três eventos de conexão saltam pelo AmqpPool em vez de postar direto
// (ver o comentário de TConexaoEventoWork).
procedure TfrmRetry.OnDesconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmRetry.OnReconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmRetry.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- conexão ----------------------------------------------------------------- }

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
