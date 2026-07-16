unit uHeadersMain;

{ Pub/sub com exchange HEADERS: o roteamento não usa routing key, e sim o
  CASAMENTO de campos do header da mensagem contra critérios declarados em cada
  binding. Cada binding traz um argumento `x-match`:
    - `all` — a mensagem casa se TODOS os campos do critério baterem (E lógico);
    - `any` — casa se QUALQUER um bater (OU lógico).
  A routing key é ignorada pelo broker neste tipo de exchange.

  É o contraponto do EventosTopicVcl (roteamento por routing key hierárquica):
  aqui o assinante filtra por atributos independentes do evento — no exemplo,
  `tipo` (nfe/nfce/cte) e `regiao` (sul/sudeste/...). Um assinante "all,
  tipo=nfe, regiao=sul" só recebe nota fiscal do sul; um "any, tipo=nfe,
  regiao=sul" recebe qualquer nfe OU qualquer evento do sul.

  Único sample que exercita:
    - *binding arguments* — a `TAMQPFieldTable` no BindQueue (x-match + critérios),
      diferente do RetryDlqVcl, que usa Arguments no DECLARE da fila;
    - a propriedade Headers da mensagem (SetHeaders no publish, e a leitura dos
      headers na entrega).

  Como no topic, cada assinante tem a PRÓPRIA fila (exclusiva + auto-delete)
  ligada ao exchange, e o mesmo evento pode chegar a vários assinantes ou a
  nenhum (mandatory + OnBasicReturn torna o descarte visível). Remover um
  assinante desfaz na ordem Cancel -> Unbind -> DeleteQueue, reconciliando a
  topologia de recovery (o Unbind precisa repetir os MESMOS argumentos do bind:
  no headers exchange é o argumento que identifica o binding, não a routing key).

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
  Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Wire, AMQP.Threading, AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods, AMQP.Basic.Methods;

type
  { Um assinante: fila exclusiva própria ligada ao exchange por um binding com
    argumentos (x-match + critérios). Os critérios ficam guardados em campos
    simples (não numa TAMQPFieldTable viva) para reconstruir a tabela idêntica
    no unbind. Criado/destruído SOMENTE na thread da UI. }
  TAssinante = class
    Numero: Integer;
    Match: string;      // 'all' ou 'any'
    CriTipo: string;    // '' = não faz parte do critério
    CriRegiao: string;  // '' = não faz parte do critério
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
    // Assinantes por consumer-tag; tocado SOMENTE na thread da UI.
    FAssinantes: TDictionary<string, TAssinante>;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure RemoverAssinante(AAssinante: TAssinante);
    procedure RemoverTodosAssinantes;
    // Atualizações de UI (thread da UI, via marshals):
    procedure EventoRecebido(const ATag, ATipo, ARegiao, ACorpo: string);
    procedure EventoDevolvido(const ATipo, ARegiao, ACorpo: string);
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
  frmHeaders: TfrmHeaders;

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

// Texto legível dos critérios de um binding (ex.: 'tipo=nfe, regiao=sul').
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

// Monta a tabela de argumentos do binding: x-match + os critérios não-vazios.
// O chamador é DONO da tabela e a libera após o bind/unbind. Os Puts ficam em
// comandos separados de propósito (encadear terminando num TValue.From<T>
// inline dá erro interno no FPC 3.2 — gotcha no CLAUDE.md).
function MontarArgumentos(const AMatch, ATipo, ARegiao: string): TAMQPFieldTable;
begin
  Result := TAMQPFieldTable.Create;
  Result.Put('x-match', TValue.From<string>(AMatch));
  if ATipo <> '' then
    Result.Put('tipo', TValue.From<string>(ATipo));
  if ARegiao <> '' then
    Result.Put('regiao', TValue.From<string>(ARegiao));
end;

// Lê um header string da tabela da entrega. O valor é atribuído a uma TValue
// local ANTES do .AsString: encadear .AsString direto no indexador/retorno
// dispara erro interno no FPC 3.2 (gotcha no CLAUDE.md).
function LerHeader(AHeaders: TAMQPFieldTable; const AKey: string): string;
var
  LValor: TValue;
begin
  Result := '';
  if (AHeaders <> nil) and AHeaders.TryGetValue(AKey, LValor) then
    Result := LValor.AsString;
end;

type
  // Um objeto por chamada: TThread.Queue no FPC so' aceita 'procedure of
  // object' SEM PARAMETROS, entao os dados viajam num objeto descartavel
  // (nao num campo compartilhado da form, que teria corrida entre callbacks
  // concorrentes). Se autodestroi apos rodar. Ver RetaguardaVcl.
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

  { Eventos de conexão rodam na thread de RECONEXÃO da lib, que morre logo
    após o OnReconnect — no FPC um TThread.Queue postado por thread que morre
    antes do bombeio é DESCARTADO (gotcha no CLAUDE.md). Salto por um worker
    persistente do AmqpPool. }
  TConexaoEventoWork = class(TAMQPWorkItem)
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
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
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

function TfrmHeaders.ScrollAtBottom(AHandle: HWND): Boolean;
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

    // Fila descartável do assinante: exclusiva (só esta conexão) e
    // auto-delete (morre quando o consumer cancela / a conexão cai).
    LDeclare := TAMQPQueueDeclare.Create(LAssinante.Fila, False);
    LDeclare.Exclusive := True;
    LDeclare.AutoDelete := True;
    FChannel.DeclareQueue(LDeclare);

    // Binding SEM routing key (ignorada no headers exchange): o que decide é a
    // tabela de argumentos (x-match + critérios). O chamador é dono da tabela.
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

// Desfaz na ordem inversa da criação — e reconcilia a topologia de recovery:
// sem o Unbind/DeleteQueue, uma reconexão replayaria o bind gravado para uma
// fila que não existe mais (erro 404 fecharia o canal no meio do recovery). No
// headers exchange o Unbind precisa repetir os MESMOS argumentos do bind (é o
// argumento, não a routing key, que identifica o binding).
procedure TfrmHeaders.RemoverAssinante(AAssinante: TAssinante);
var
  LUnbind: TAMQPQueueUnbind;
  LDelete: TAMQPQueueDelete;
  LArgs: TAMQPFieldTable;
begin
  try
    FChannel.Cancel(AAssinante.Tag); // para as entregas (e limpa o recovery do consumer)
    LArgs := MontarArgumentos(AAssinante.Match, AAssinante.CriTipo, AAssinante.CriRegiao);
    try
      LUnbind := Default(TAMQPQueueUnbind);
      LUnbind.QueueName := AAssinante.Fila;
      LUnbind.ExchangeName := FExchange;
      LUnbind.RoutingKey := '';
      LUnbind.Arguments := LArgs;      // deve casar com o do bind
      FChannel.UnbindQueue(LUnbind);   // desfaz o binding (e o remove do recovery)
    finally
      LArgs.Free;
    end;
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

// No encerramento/desconexão: só cancela os consumers (drena as entregas em
// voo); as filas exclusivas/auto-delete morrem sozinhas com a conexão.
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

{ --- eventos ---------------------------------------------------------------- }

// Roda numa thread do pool; um mesmo evento dispara este callback uma vez POR
// assinante casado (cada um tem fila e consumer próprios). Os headers vêm nas
// propriedades da entrega — lidos AQUI, antes de a entrega ser liberada.
procedure TfrmHeaders.OnEvento(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LMarshal: TEventoMarshal;
begin
  LMarshal := TEventoMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Tag := ADelivery.ConsumerTag; // identifica o assinante
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
    Exit; // entrega tardia de assinante já removido: descarta em silêncio

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

{ --- publicação ------------------------------------------------------------- }

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

  // Os headers da mensagem são o que o broker casa contra os critérios dos
  // bindings. O chamador é dono da tabela (SetHeaders não a copia) — liberada
  // após o publish, que serializa o frame de forma síncrona.
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
      // routing key vazia (ignorada no headers exchange). mandatory=True: sem
      // NENHUM binding casando, o broker devolve o evento (Basic.Return) em vez
      // de descartar em silêncio — visível no log.
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

{ --- eventos de conexão ------------------------------------------------------ }

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

// Os três eventos de conexão saltam pelo AmqpPool em vez de postar direto
// (ver o comentário de TConexaoEventoWork).
procedure TfrmHeaders.OnDesconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceCaiu));
end;

procedure TfrmHeaders.OnReconectado(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceVoltou));
end;

procedure TfrmHeaders.OnReconexaoFalhou(AConnection: TAMQPConnection);
begin
  AmqpPool.Queue(TConexaoEventoWork.Create(Self, ceFalhou));
end;

{ --- conexão ----------------------------------------------------------------- }

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
