unit uConsultaMain;

{ RPC request/reply sobre AMQP: o cliente pergunta o status de uma nota e o
  servidor responde — o padrão clássico com ReplyTo + CorrelationId.

  Uma janela com os DOIS papéis, cada um opcional:
  - Servidor: consome a fila de pedidos (durável) e, para cada consulta,
    simula a busca (Sleep aleatório até a demora máxima configurada) e publica
    a resposta na fila indicada pelo ReplyTo do pedido, ecoando o
    CorrelationId. Ack manual só depois de responder.
  - Cliente: consome uma fila de RESPOSTAS (ack automático) e, a cada consulta,
    publica o pedido com ReplyTo = essa fila, CorrelationId = GUID novo e
    Expiration = timeout (o
    broker descarta pedidos que envelhecerem na fila além do timeout — não
    faz sentido o servidor responder a quem já desistiu). A resposta resolve
    a linha correspondente na lista pelo CorrelationId, com o tempo de ida e
    volta; um TTimer expira consultas sem resposta, e uma resposta que chegue
    DEPOIS do timeout é descartada com aviso no log (decisão de projeto que o
    padrão RPC sempre exige).

  Dá pra rodar uma instância só (os dois papéis na mesma conexão, canais
  separados) ou duas instâncias — uma como servidor, outra como cliente.
  Consultar sem servidor ativo também é didático: o pedido fica na fila de
  pedidos até o TTL (Expiration) estourar, e o cliente reporta timeout.

  O checkbox "Usar Direct Reply-to" alterna o mecanismo de resposta do cliente
  — os dois padrões clássicos de RPC, lado a lado:

  - Desmarcado (fila nomeada): declara uma fila de respostas exclusiva +
    auto-delete com nome FIXO por instância ('consulta-resp-' + GUID), em vez
    de nome gerado pelo broker (declare com nome ''). O replay de topologia da
    reconexão reenvia o declare gravado; com nome '' o broker geraria um nome
    NOVO e o consumer gravado apontaria para o antigo, quebrando o replay. Com
    nome fixo, o par declare+consume sobrevive à reconexão.

  - Marcado (Direct Reply-to, 'amq.rabbitmq.reply-to'): a pseudo-fila mágica do
    RabbitMQ — não se declara NADA. O cliente só consome dela em no-ack antes
    de publicar; o broker roteia a resposta de volta por um caminho rápido, sem
    fila real. É o padrão que o README recomenda para RPC (menos topologia,
    também imune ao problema de recovery pelo nome estável). Restrições do
    broker: consumir e publicar o pedido no MESMO canal, e em no-ack. O lado
    servidor é IDÊNTICO nos dois modos — publica no default exchange com routing
    key = o ReplyTo que recebeu (que o broker reescreve para um nome opaco).

  Compila nos dois mundos a partir do MESMO fonte (padrão dos samples GUI):
  callbacks como métodos nomeados ('of object', regra do FPC) e atualizações
  de UI via objetos de "marshal" descartáveis + TThread.Queue (ver
  RetaguardaVcl). Os eventos de conexão saltam pelo AmqpPool antes de postar
  — no FPC um TThread.Queue postado pela thread de reconexão (que morre logo
  após o OnReconnect) seria descartado; ver TConexaoEventoWork e o gotcha no
  CLAUDE.md. }

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
  Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls, ExtCtrls,
  AMQP.Wire, AMQP.Threading, AMQP.Connection, AMQP.Transport,
  AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  { Consulta em voo no cliente, indexada pelo CorrelationId. Criada e
    resolvida SOMENTE na thread da UI (marshals + timer). }
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
    // Canais separados por papel: o consumo do servidor não disputa o RPC de
    // canal (Publish/DeclareQueue) com o cliente.
    FCanalServidor: TAMQPChannel;
    FCanalCliente: TAMQPChannel;
    FFilaPedidos: string;   // capturada ao conectar (o cliente publica nela)
    FFilaRespostas: string; // fila de respostas do cliente (ver FModoDireto)
    FModoDireto: Boolean;   // True = Direct Reply-to; False = fila nomeada
    FTagServidor: string;   // consumer-tag do servidor ('' = servidor parado)
    FDelayMaxMs: Integer;   // demora máxima simulada do servidor
    FAtendidas: Integer;
    // Consultas em voo do cliente; tocado SOMENTE na thread da UI.
    FPendentes: TDictionary<string, TConsultaPendente>;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    procedure SetServidorRodando(ARodando: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure AtualizarAtendidas;
    procedure CancelarPendentes(const AMotivo: string);
    // Atualizações de UI (thread da UI, via marshals):
    procedure RespostaRecebida(const ACorrelationId, AStatus: string);
    procedure PedidoAtendido(const AChave, AStatus: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Marshalling (chamável de qualquer thread):
    procedure QueueLog(const AMsg: string);
    // Callbacks da lib (threads do pool / de reconexão):
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
  // A LCL nao tem a unit Messages; LM_VSCROLL tem o mesmo valor do WM_VSCROLL.
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
  // Um objeto por chamada: TThread.Queue no FPC so' aceita 'procedure of
  // object' SEM PARAMETROS, entao os dados viajam num objeto descartavel
  // (nao num campo compartilhado da form, que teria corrida entre callbacks
  // concorrentes). Se autodestroi apos rodar. Ver RetaguardaVcl.
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

  { Os eventos de conexão rodam na thread de RECONEXÃO da lib, que termina
    logo após o OnReconnect — e no FPC um TThread.Queue postado por uma
    thread que morre antes do bombeio é DESCARTADO (TThread.Destroy remove os
    posts pendentes dela, casando pelo ThreadID; no Delphi o descarte casa só
    pelo parâmetro AThread, que é nil aqui). O salto por um worker do
    AmqpPool — threads PERSISTENTES — garante a entrega. Gotcha no CLAUDE.md;
    achado no PublicadorConfiavelVcl. }
  TConexaoEventoWork = class(TAMQPWorkItem)
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
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
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
  // Os Free acima drenam os callbacks em voo; os marshals que eles postaram
  // ainda estao na fila do TThread.Queue — bombear aqui os drena com a form
  // ainda viva (ver RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // O Free da conexao encerra a thread de reconexao — nao nascem mais eventos
  // de conexao. Um TConexaoEventoWork ja enfileirado no pool pode estar
  // postando o ultimo marshal NESTE instante: a espera curta + bombeio drenam
  // esse retardatario.
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
    Exit(True); // sem scrollbar ainda (conteudo cabe todo) = considera "no fim"
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
  // Consultas ainda em voo continuam válidas — só desligamos as linhas da
  // lista delas (a resposta/timeout vira só log).
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
  edtFilaPedidos.Enabled := not AConectado;
  chkDirectReplyTo.Enabled := not AConectado; // mecanismo escolhido ao conectar
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

{ --- servidor -------------------------------------------------------------- }

// Roda numa thread do pool (uma por consulta em voo — consultas concorrentes
// são atendidas em paralelo, como no RetaguardaVcl).
procedure TfrmConsulta.OnPedido(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LChave, LStatus, LReplyTo, LCorr: string;
  LProps: TAMQPBasicProperties;
  LMarshal: TAtendidoMarshal;
begin
  LChave := ADelivery.BodyAsText;
  LReplyTo := ADelivery.Properties.ReplyTo;
  LCorr := ADelivery.Properties.CorrelationId;

  Sleep(100 + Random(FDelayMaxMs)); // consulta simulada ao "banco"

  case Random(10) of
    0..6: LStatus := 'Autorizada';
    7..8: LStatus := 'Rejeitada';
  else
    LStatus := 'Em processamento';
  end;

  // Sem ReplyTo não há para onde responder — atende e descarta (um servidor
  // real logaria o pedido malformado).
  if LReplyTo <> '' then
  begin
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetCorrelationId(LCorr); // ecoa: é assim que o cliente correlaciona
    AChannel.Publish('', LReplyTo, AmqpUtf8Encode(LStatus), LProps);
  end;
  AChannel.Ack(ADelivery.DeliveryTag); // só depois de responder

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

{ --- cliente --------------------------------------------------------------- }

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
    LItem.SubItems.Add(Copy(LCorr, 2, 8)); // trecho do GUID, só pra enxergar
    LItem.SubItems.Add(cStatusAguardando);
    LItem.SubItems.Add(FormatDateTime('hh:nn:ss.zzz', Now));
    LItem.SubItems.Add('');
    if LAtBottom then
      LItem.MakeVisible(False);

    LPendente := TConsultaPendente.Create;
    LPendente.Chave := LChave;
    LPendente.EnviadaTick := AmqpTickMs;
    LPendente.DeadlineTick := LPendente.EnviadaTick + UInt64(LTimeout);
    LPendente.Item := LItem;
    FPendentes.Add(LCorr, LPendente);

    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('text/plain');
    LProps.SetReplyTo(FFilaRespostas);
    LProps.SetCorrelationId(LCorr);
    // TTL do pedido = timeout do cliente: se envelhecer na fila (servidor
    // parado/lento), o broker o descarta — ninguém responde a quem desistiu.
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

// Roda numa thread do pool (consumer da fila de respostas, ack automático).
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
    // Já expirou (timeout) ou correlação desconhecida: a regra do padrão RPC
    // é descartar — quem esperava já foi embora.
    Log(Format('Resposta tardia descartada (correlação %s): %s.',
      [Copy(ACorrelationId, 2, 8), AStatus]));
    Exit;
  end;
  FPendentes.Remove(ACorrelationId);
  LTempoMs := AmqpTickMs - LPendente.EnviadaTick;
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
  LAgora := AmqpTickMs;
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

{ --- eventos de conexão ----------------------------------------------------- }

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

// Os três eventos de conexão saltam pelo AmqpPool em vez de postar direto:
// a thread de reconexão morre logo após o OnReconnect e, no FPC, levaria o
// post pendente junto (ver o comentário de TConexaoEventoWork).
procedure TfrmConsulta.OnDesconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmConsulta.OnReconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmConsulta.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- conexão ---------------------------------------------------------------- }

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

    // Canal do servidor: fila de pedidos durável + prefetch. Declarada já na
    // conexão (mesmo sem iniciar o servidor): consultas publicadas sem
    // servidor ativo ficam na fila até o TTL do pedido estourar.
    FCanalServidor := FConn.CreateChannel;
    FCanalServidor.DeclareQueue(TAMQPQueueDeclare.Create(FFilaPedidos, True));
    FCanalServidor.Qos(16);

    // Canal do cliente: dois mecanismos de resposta, escolhidos no checkbox.
    // Em ambos o consume é NoAck (resposta perdida = timeout, o cliente não
    // reprocessa) e o publish do pedido sai NESTE mesmo canal.
    FModoDireto := chkDirectReplyTo.Checked;
    FCanalCliente := FConn.CreateChannel;
    if FModoDireto then
    begin
      // Direct Reply-to (amq.rabbitmq.reply-to): pseudo-fila de nome fixo do
      // broker — NÃO se declara. Basta consumir dela em no-ack ANTES de
      // publicar; o broker reescreve o ReplyTo do pedido para um nome opaco e
      // roteia a resposta de volta por um caminho rápido, sem fila real. O
      // servidor não muda: publica no default exchange com routing key = o
      // ReplyTo que recebeu. É o padrão que o README recomenda para RPC.
      // Restrições (RabbitMQ): consumir e publicar o pedido no MESMO canal
      // (garantido aqui — os dois usam FCanalCliente) e em no-ack. Sobrevive
      // ao recovery pelo nome estável, sem gerenciar fila por instância.
      FFilaRespostas := 'amq.rabbitmq.reply-to';
      FCanalCliente.Consume(FFilaRespostas, OnResposta, True); // NoAck obrigatório
    end
    else
    begin
      // Fila nomeada exclusiva desta instância, com nome FIXO (não '' gerado
      // pelo broker — esse não sobrevive ao replay do recovery: o redeclare
      // geraria um nome novo e o consumer gravado apontaria para o antigo).
      // Exclusiva + auto-delete: o broker a remove quando a conexão morre.
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
