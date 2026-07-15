unit uEventosMain;

{ Pub/sub com exchange TOPIC: eventos publicados com uma routing key
  hierárquica (ex.: 'nota.aprovada', 'pdv.7.venda') chegam a TODO assinante
  cuja binding key case — '*' casa exatamente uma palavra, '#' casa zero ou
  mais. Diferente das filas de trabalho dos outros samples (cada mensagem vai
  a UM consumidor), aqui cada assinante tem a PRÓPRIA fila (exclusiva +
  auto-delete, descartável) ligada ao exchange pela binding key — um evento
  pode ser entregue a vários assinantes ao mesmo tempo, ou a nenhum.

  A tela permite criar assinantes dinamicamente (binding key livre) e
  publicar eventos com qualquer routing key; a lista de eventos mostra QUEM
  recebeu O QUÊ (o mesmo evento aparece uma vez por assinante que casou). O
  publish usa mandatory + OnBasicReturn: evento que não casa com nenhum
  assinante é devolvido pelo broker e aparece no log — pub/sub é
  fire-and-forget, sem assinante o evento se perde (de propósito).

  Remover um assinante desfaz na ordem: Cancel (para as entregas), Unbind
  (tira o binding e o remove da topologia de recovery) e DeleteQueue (apaga a
  fila e remove o declare gravado) — sem isso, uma reconexão replayaria um
  bind para fila inexistente (erro 404 no canal). É também o único sample que
  exercita DeclareExchange/BindQueue/UnbindQueue.

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
  Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, AMQP.Threading, AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  { Um assinante: fila exclusiva própria ligada ao exchange pela binding key.
    Criado e destruído SOMENTE na thread da UI; o mapa por consumer-tag é
    consultado só nos marshals (também na thread da UI). }
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
    // Assinantes por consumer-tag; tocado SOMENTE na thread da UI.
    FAssinantes: TDictionary<string, TAssinante>;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RemoverAssinante(AAssinante: TAssinante);
    procedure RemoverTodosAssinantes;
    // Atualizações de UI (thread da UI, via marshals):
    procedure EventoRecebido(const ATag, ARoutingKey, ACorpo: string);
    procedure EventoDevolvido(const ARoutingKey, ACorpo: string);
    procedure ConexaoCaiu;
    procedure ConexaoVoltou;
    procedure ConexaoFalhou;
    // Callbacks da lib (threads do pool / de reconexão):
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
  // A LCL nao tem a unit Messages; LM_VSCROLL tem o mesmo valor do WM_VSCROLL.
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
  // Um objeto por chamada: TThread.Queue no FPC so' aceita 'procedure of
  // object' SEM PARAMETROS, entao os dados viajam num objeto descartavel
  // (nao num campo compartilhado da form, que teria corrida entre callbacks
  // concorrentes). Se autodestroi apos rodar. Ver RetaguardaVcl.
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

  { Eventos de conexão rodam na thread de RECONEXÃO da lib, que morre logo
    após o OnReconnect — no FPC um TThread.Queue postado por thread que morre
    antes do bombeio é DESCARTADO (gotcha no CLAUDE.md). Salto por um worker
    persistente do AmqpPool. }
  TConexaoEventoWork = class(TAMQPWorkItem)
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
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
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
  // Só cancela os consumers (as filas são exclusivas + auto-delete: morrem
  // com a conexão). O desfazer completo fica no botão Remover.
  if Assigned(FChannel) then
    RemoverTodosAssinantes;
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

function TfrmEventos.ScrollAtBottom(AHandle: HWND): Boolean;
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
  edtExchange.Enabled := not AConectado;
  btnPublicar.Enabled := AConectado;
  btnAssinar.Enabled := AConectado;
  btnRemover.Enabled := AConectado;
end;

{ --- assinantes ------------------------------------------------------------- }

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

    // Fila descartável do assinante: exclusiva (só esta conexão) e
    // auto-delete (morre quando o consumer cancela / a conexão cai).
    LDeclare := TAMQPQueueDeclare.Create(LAssinante.Fila, False);
    LDeclare.Exclusive := True;
    LDeclare.AutoDelete := True;
    FChannel.DeclareQueue(LDeclare);

    LQueueBind := Default(TAMQPQueueBind);
    LQueueBind.QueueName := LAssinante.Fila;
    LQueueBind.ExchangeName := FExchange;
    LQueueBind.RoutingKey := LBind; // binding key ('*' = 1 palavra; '#' = 0+)
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

// Desfaz na ordem inversa da criação — e reconcilia a topologia de recovery:
// sem o Unbind/DeleteQueue, uma reconexão replayaria o bind gravado para uma
// fila que não existe mais (erro 404 fecharia o canal no meio do recovery).
procedure TfrmEventos.RemoverAssinante(AAssinante: TAssinante);
var
  LUnbind: TAMQPQueueUnbind;
  LDelete: TAMQPQueueDelete;
begin
  try
    FChannel.Cancel(AAssinante.Tag); // para as entregas (e limpa o recovery do consumer)
    LUnbind := Default(TAMQPQueueUnbind);
    LUnbind.QueueName := AAssinante.Fila;
    LUnbind.ExchangeName := FExchange;
    LUnbind.RoutingKey := AAssinante.BindingKey;
    FChannel.UnbindQueue(LUnbind);   // desfaz o binding (e o remove do recovery)
    LDelete := Default(TAMQPQueueDelete);
    LDelete.QueueName := AAssinante.Fila;
    FChannel.DeleteQueue(LDelete);   // apaga a fila (e remove o declare do recovery)
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

// No encerramento/desconexão: só cancela os consumers (drena as entregas em
// voo); as filas exclusivas/auto-delete morrem sozinhas com a conexão.
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

{ --- eventos ---------------------------------------------------------------- }

// Roda numa thread do pool; um mesmo evento dispara este callback uma vez POR
// assinante casado (cada um tem fila e consumer próprios).
procedure TfrmEventos.OnEvento(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TEventoMarshal;
begin
  LMarshal := TEventoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Tag := ADelivery.ConsumerTag; // identifica o assinante
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
    Exit; // entrega tardia de assinante já removido: descarta em silêncio

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

{ --- publicação ------------------------------------------------------------- }

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
    // mandatory=True: sem NENHUM binding casando, o broker devolve o evento
    // (Basic.Return) em vez de descartar em silêncio — visível no log.
    FChannel.Publish(FExchange, LRk, AmqpUtf8Encode(LCorpo), LProps, True);
    Log(Format('Publicado "%s" com routing key "%s".', [LCorpo, LRk]));
  except
    on E: Exception do
      Log('Erro ao publicar: ' + E.Message);
  end;
end;

{ --- eventos de conexão ------------------------------------------------------ }

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

// Os três eventos de conexão saltam pelo AmqpPool em vez de postar direto
// (ver o comentário de TConexaoEventoWork).
procedure TfrmEventos.OnDesconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmEventos.OnReconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmEventos.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- conexão ----------------------------------------------------------------- }

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
