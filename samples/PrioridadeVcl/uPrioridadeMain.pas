unit uPrioridadeMain;

{ Fila com PRIORIDADE: uma fila declarada com o argumento `x-max-priority`
  entrega primeiro as mensagens de maior prioridade (propriedade `Priority`,
  0..N), furando a ordem de chegada das de prioridade menor que ainda estejam
  esperando.

  Detalhe que decide se a demonstração funciona: a reordenação só acontece
  entre mensagens que estão JUNTAS na fila no momento em que o broker escolhe a
  próxima. Por isso o roteiro é:
    1. deixe o consumidor PARADO e publique um lote de prioridades misturadas
       (constrói o backlog);
    2. só então inicie o consumidor LENTO — as mensagens saem em ordem
       decrescente de prioridade, não na ordem em que foram publicadas.

  Duas condições são essenciais para ver o efeito e estão no sample:
    - o consumidor precisa de `Qos(prefetch)` BAIXO (1): com prefetch alto o
      broker despeja um lote no buffer do cliente de uma vez e a prioridade não
      reordena o que já saiu da fila. O campo Prefetch é editável de propósito
      — suba para, digamos, 20 e veja a ordenação por prioridade sumir;
    - o consumidor precisa ser mais lento que o produtor (delay por mensagem +
      ack manual), senão ele esvazia a fila sem nunca formar backlog.

  Conectar recria a fila do zero (DeleteQueue → DeclareQueue): redeclarar uma
  fila existente com `x-max-priority` diferente é PRECONDITION_FAILED (mesmo
  motivo do RetryDlqVcl). É o sample que exercita a propriedade `Priority`
  (`SetPriority` no publish, leitura na entrega).

  Compila nos dois mundos a partir do MESMO fonte (padrão dos samples GUI):
  callbacks nomeados ('of object'), marshals descartáveis + TThread.Queue para
  a UI (ver RetaguardaVcl) e eventos de conexão saltando pelo AmqpPool (gotcha
  do TThread.Queue descartado; ver TConexaoEventoWork e CLAUDE.md). }

interface

uses
  // No FPC, a camada de emulacao da LCL (LCLIntf/LCLType/LMessages) cobre as
  // chamadas WinAPI do autoscroll do log em qualquer widgetset (win32, gtk2...).
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes, Rtti,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, AMQP.Threading, AMQP.Connection, AMQP.Transport,
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
    FChannel: TAMQPChannel;      // canal do produtor (declare + publish)
    FCanalConsumo: TAMQPChannel; // canal separado do consumidor (Qos próprio)
    FFila: string;
    FMaxPrio: Integer;
    FTagConsumidor: string;      // '' = consumidor parado
    FDelayMs: Integer;           // demora por mensagem do consumidor
    FConsumidas: Integer;
    FSaidaSeq: Integer;          // ordem de SAÍDA (drenagem)
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    procedure SetConsumindo(AConsumindo: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure PublicarUma(APrioridade: Integer; const ACorpo: string);
    // Atualizações de UI (thread da UI, via marshals):
    procedure MensagemConsumida(APrioridade: Integer; const ACorpo: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Callbacks da lib (threads do pool / de reconexão):
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
  // A LCL nao tem a unit Messages; LM_VSCROLL tem o mesmo valor do WM_VSCROLL.
  WM_VSCROLL = LM_VSCROLL;
{$ENDIF}

const
  // Prioridades do "lote": deliberadamente fora de ordem, para a drenagem sair
  // em ordem decrescente bem diferente da ordem de publicação.
  cLotePrioridades: array[0..9] of Integer = (2, 8, 5, 9, 1, 7, 3, 9, 0, 6);

type
  // Um objeto por chamada: TThread.Queue no FPC so' aceita 'procedure of
  // object' SEM PARAMETROS, entao os dados viajam num objeto descartavel
  // (nao num campo compartilhado da form, que teria corrida entre callbacks
  // concorrentes). Se autodestroi apos rodar. Ver RetaguardaVcl.
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

  { Eventos de conexão rodam na thread de RECONEXÃO da lib, que morre logo
    após o OnReconnect — no FPC um TThread.Queue postado por thread que morre
    antes do bombeio é DESCARTADO (gotcha no CLAUDE.md). Salto por um worker
    persistente do AmqpPool. }
  TConexaoEventoWork = class(TAMQPWorkItem)
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
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
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
  // Os Free acima drenam os callbacks em voo; os marshals que eles postaram
  // ainda estao na fila do TThread.Queue — bombear aqui os drena com a form
  // ainda viva (ver RetaguardaVcl.FormClose).
  Application.ProcessMessages;
  FreeAndNil(FConn);
  // O Free da conexao encerra a thread de reconexao; um TConexaoEventoWork ja
  // enfileirado no pool pode estar postando o ultimo marshal NESTE instante.
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
    Exit(True); // sem scrollbar ainda (conteudo cabe todo) = considera "no fim"
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

{ --- publicação ------------------------------------------------------------- }

procedure TfrmPrioridade.PublicarUma(APrioridade: Integer; const ACorpo: string);
var
  LProps: TAMQPBasicProperties;
begin
  if APrioridade < 0 then
    APrioridade := 0;
  if APrioridade > FMaxPrio then
    APrioridade := FMaxPrio; // o broker também limita ao teto, mas deixamos explícito
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LProps.SetPriority(Byte(APrioridade));
  // Publica direto na fila pelo default exchange (routing key = nome da fila).
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
  LCorpo: string;
begin
  if not Assigned(FChannel) then
    Exit;
  try
    for I := Low(cLotePrioridades) to High(cLotePrioridades) do
    begin
      LPrio := cLotePrioridades[I];
      // Corpo identifica a ORDEM de publicação (1..N) — na drenagem ela sai
      // embaralhada, provando que quem manda é a prioridade.
      LCorpo := Format('pub#%d', [I + 1]);
      PublicarUma(LPrio, LCorpo);
    end;
    Log(Format('Lote publicado: %d mensagens com prioridades misturadas. ' +
      'Inicie o consumidor lento e observe a saída em ordem decrescente de ' +
      'prioridade.', [Length(cLotePrioridades)]));
  except
    on E: Exception do
      Log('Erro ao publicar o lote: ' + E.Message);
  end;
end;

{ --- consumidor ------------------------------------------------------------- }

// Roda numa thread do pool. Com prefetch=1 e ack manual, o broker só entrega a
// PRÓXIMA mensagem (a de maior prioridade ainda na fila) depois do Ack — por
// isso o Sleep vai ANTES do Ack: segura o ritmo e mantém o backlog.
procedure TfrmPrioridade.OnMensagem(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TConsumoMarshal;
begin
  LMarshal := TConsumoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Prioridade := ADelivery.Properties.Priority;
  LMarshal.Corpo := ADelivery.BodyAsText;
  TThread.Queue(nil, LMarshal.Execute);

  Sleep(FDelayMs);                    // consumidor "lento" de propósito
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
    FTagConsumidor := FCanalConsumo.Consume(FFila, OnMensagem); // ack manual
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

{ --- eventos de conexão ------------------------------------------------------ }

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

// Os três eventos de conexão saltam pelo AmqpPool em vez de postar direto
// (ver o comentário de TConexaoEventoWork).
procedure TfrmPrioridade.OnDesconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmPrioridade.OnReconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmPrioridade.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- conexão ----------------------------------------------------------------- }

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
    FMaxPrio := 255; // teto do AMQP (x-max-priority aceita até 255; RabbitMQ recomenda <= 10)
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.OnDisconnect := OnDesconectado;
    FConn.OnReconnect := OnReconectado;
    FConn.OnReconnectFailed := OnReconexaoFalhou;
    FConn.Open;

    FChannel := FConn.CreateChannel;

    // Recria a fila do zero: redeclarar uma fila existente com x-max-priority
    // diferente é PRECONDITION_FAILED. DeleteQueue reconcilia a topologia de
    // recovery (ver RetryDlqVcl).
    LDel := Default(TAMQPQueueDelete);
    LDel.QueueName := FFila;
    FChannel.DeleteQueue(LDel);

    LArgs := TAMQPFieldTable.Create;
    try
      LArgs.Put('x-max-priority', TValue.From<Integer>(FMaxPrio));
      LDecl := TAMQPQueueDeclare.Create(FFila, True); // durável; recriada a cada conexão
      LDecl.Arguments := LArgs;
      FChannel.DeclareQueue(LDecl);
    finally
      LArgs.Free;
    end;

    // Canal separado do consumidor: o Qos(prefetch) dele não interfere no
    // canal do produtor.
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
