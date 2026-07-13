unit uRetaguardaMain;

{ Tela unica: campos de conexao editaveis + fila/prefetch, botao
  Conectar/Desconectar que declara a fila, seta o Qos e chama Channel.Consume.
  O callback do consumer roda no thread pool (despacho nativo da lib) e so
  toca a ListView/label via TThread.Queue - o dicionario FItems so e acessado
  a partir do procedimento marshalled na thread principal, entao nao precisa
  de lock proprio. Mesmo "processamento" simulado do sample console (Sleep
  aleatorio fazendo o papel de busca do XML).

  Modo "Confirmacao manual": em vez do Sleep+ack automatico, a thread do
  consumer fica bloqueada num TEvent por mensagem (FPending, protegido por
  FPendingLock) ate o usuario clicar Aceitar/Rejeitar na tela para a nota
  selecionada na ListView. Ao desconectar/fechar, CancelarPendencias libera
  qualquer thread ainda bloqueada (como nack+requeue) antes de cancelar o
  consumer e fechar o canal.

  FEncerrando fecha uma corrida do encerramento: o nack+requeue disparado por
  CancelarPendencias pode fazer o broker reentregar a mensagem ao consumer
  (ainda ativo ate o Cancel-Ok), e esse callback novo estacionaria num TEvent
  que ninguem mais vai sinalizar - enquanto o Destroy do canal espera, na
  thread principal, todos os callbacks terminarem (deadlock: UI congelada).
  Com o flag (setado sob FPendingLock antes de acordar os eventos), qualquer
  entrega que chegue depois do inicio do encerramento sai sem ack/nack - o
  fechamento do canal devolve a mensagem a fila.

  Compila nos dois mundos a partir do MESMO fonte (mesmo padrao dos outros
  samples): uses sem prefixo de namespace, condicional so' onde o recurso
  realmente difere (lfm vs dfm).

  Duas adaptacoes em relacao a versao Delphi-only: TAMQPConsumerCallback e'
  'of object' nesta lib (nao aceita metodo anonimo - ver CLAUDE.md), entao o
  callback do Consume virou o metodo OnDelivery abaixo. E o FPC so' aceita
  TThread.Queue com um metodo 'of object' SEM PARAMETROS (TThreadMethod) -
  nao existe o overload com closure anonima do Delphi - entao cada
  TThread.Queue(nil, procedure ... end) virou uma chamada a QueueNotaRecebida/
  QueueNotaStatus, que empacota os dados por chamada num objeto de "marshal"
  descartavel (TRecebidaMarshal/TStatusMarshal). Um campo compartilhado na
  form NAO serviria aqui: varios workers do pool chamam isso concorrentemente,
  e o valor seria sobrescrito antes do metodo enfileirado rodar na UI. }

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
  SyncObjs, Generics.Collections,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls,
  AMQP.Connection, AMQP.Transport, AMQP.Queue.Methods;

type
  TPendingApproval = class
    Event: TEvent;
    Aceitar: Boolean;
    Requeue: Boolean;
  end;

  TfrmRetaguarda = class(TForm)
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
    gbConsumo: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblPrefetch: TLabel;
    edtPrefetch: TEdit;
    chkManual: TCheckBox;
    chkDedicado: TCheckBox;
    gbAprovacao: TGroupBox;
    btnAceitar: TButton;
    btnRejeitar: TButton;
    chkRequeue: TCheckBox;
    lvNotas: TListView;
    lblContagem: TLabel;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    Button1: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
    procedure Button1Click(Sender: TObject);
    procedure btnAceitarClick(Sender: TObject);
    procedure btnRejeitarClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FConsumerTag: string;
    FItems: TDictionary<string, TListItem>;
    FRecebidas: Integer;
    FProntas: Integer;
    FRejeitadas: Integer;
    FManualMode: Boolean;
    FPending: TDictionary<string, TPendingApproval>;
    FPendingLock: TCriticalSection;
    FEncerrando: Boolean; // protegido por FPendingLock
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure AtualizarContagem;
    procedure NotaRecebida(const AChave, AWorker: string);
    procedure NotaStatus(const AChave, AStatus: string);
    // Marshalling para a thread da UI (ver nota no topo do arquivo).
    procedure QueueNotaRecebida(const AChave, AWorker: string);
    procedure QueueNotaStatus(const AChave, AStatus: string);
    procedure ResolverSelecionada(AAceitar, ARequeue: Boolean);
    procedure CancelarPendencias;
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  end;

var
  frmRetaguarda: TfrmRetaguarda;

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

type
  // Um por chamada de QueueNotaRecebida/QueueNotaStatus: TThread.Queue no FPC
  // so' aceita 'procedure of object' SEM PARAMETROS, entao os dados de cada
  // chamada viajam num objeto proprio (nao num campo compartilhado da form,
  // que teria corrida entre workers concorrentes). Se autodestroi apos rodar.
  TRecebidaMarshal = class
    Form: TfrmRetaguarda;
    Chave, Worker: string;
    procedure Execute;
  end;

  TStatusMarshal = class
    Form: TfrmRetaguarda;
    Chave, Status: string;
    procedure Execute;
  end;

procedure TRecebidaMarshal.Execute;
begin
  Form.NotaRecebida(Chave, Worker);
  Free;
end;

procedure TStatusMarshal.Execute;
begin
  Form.NotaStatus(Chave, Status);
  Free;
end;

procedure TfrmRetaguarda.FormCreate(Sender: TObject);
begin
  Randomize;
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  FItems := TDictionary<string, TListItem>.Create;
  FPending := TDictionary<string, TPendingApproval>.Create;
  FPendingLock := TCriticalSection.Create;
  SetConectado(False);
end;

procedure TfrmRetaguarda.FormDestroy(Sender: TObject);
begin
  FPendingLock.Free;
  FPending.Free;
  FItems.Free;
end;

procedure TfrmRetaguarda.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  CancelarPendencias;
  if Assigned(FChannel) and (FConsumerTag <> '') then
    try
      FChannel.Cancel(FConsumerTag);
    except
    end;
  FreeAndNil(FChannel);
  // DrainInFlight (dentro do Free acima) bloqueia esta thread num Sleep ate o
  // callback em andamento terminar - o loop de mensagens fica parado nesse
  // meio-tempo, entao qualquer TThread.Queue postado pelo callback (marshal
  // de status/recebida) fica preso na fila e vazaria se a aplicacao fechasse
  // sem nunca voltar ao loop de mensagens. Bombear aqui drena esse pendente
  // com seguranca, pois a form e seus controles ainda estao vivos.
  Application.ProcessMessages;
  FreeAndNil(FConn);
end;

function TfrmRetaguarda.ScrollAtBottom(AHandle: HWND): Boolean;
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

procedure TfrmRetaguarda.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmRetaguarda.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmRetaguarda.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmRetaguarda.BuildParams: TAMQPConnectionParams;
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
end;

procedure TfrmRetaguarda.Button1Click(Sender: TObject);
begin
  lvNotas.Items.Clear;
end;

procedure TfrmRetaguarda.ResolverSelecionada(AAceitar, ARequeue: Boolean);
var
  LChave: string;
  LApproval: TPendingApproval;
begin
  if lvNotas.Selected = nil then
  begin
    Log('Selecione uma nota na lista antes de aceitar/rejeitar.');
    Exit;
  end;
  LChave := lvNotas.Selected.Caption;
  FPendingLock.Enter;
  try
    if not FPending.TryGetValue(LChave, LApproval) then
    begin
      Log('Nota "' + LChave + '" nao esta aguardando aprovacao.');
      Exit;
    end;
    LApproval.Aceitar := AAceitar;
    LApproval.Requeue := ARequeue;
  finally
    FPendingLock.Leave;
  end;
  LApproval.Event.SetEvent;
end;

procedure TfrmRetaguarda.btnAceitarClick(Sender: TObject);
begin
  ResolverSelecionada(True, False);
end;

procedure TfrmRetaguarda.btnRejeitarClick(Sender: TObject);
begin
  ResolverSelecionada(False, chkRequeue.Checked);
end;

procedure TfrmRetaguarda.CancelarPendencias;
var
  LApproval: TPendingApproval;
begin
  FPendingLock.Enter;
  try
    FEncerrando := True; // entregas daqui em diante nao estacionam no TEvent
    for LApproval in FPending.Values do
    begin
      LApproval.Aceitar := False;
      LApproval.Requeue := True;
      LApproval.Event.SetEvent;
    end;
  finally
    FPendingLock.Leave;
  end;
end;

procedure TfrmRetaguarda.SetConectado(AConectado: Boolean);
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
  edtQueue.Enabled := not AConectado;
  edtPrefetch.Enabled := not AConectado;
  chkManual.Enabled := not AConectado;
  chkDedicado.Enabled := not AConectado;
  btnAceitar.Enabled := AConectado;
  btnRejeitar.Enabled := AConectado;
end;

procedure TfrmRetaguarda.AtualizarContagem;
begin
  lblContagem.Caption := Format('Recebidas: %d   |   Prontas: %d   |   Rejeitadas: %d',
    [FRecebidas, FProntas, FRejeitadas]);
end;

procedure TfrmRetaguarda.NotaRecebida(const AChave, AWorker: string);
var
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(lvNotas.Handle);
  LItem := lvNotas.Items.Add;
  LItem.Caption := AChave;
  LItem.SubItems.Add('Recebida');
  LItem.SubItems.Add(AWorker);
  LItem.SubItems.Add(FormatDateTime('hh:nn:ss', Now));
  LItem.SubItems.Add('');
  FItems.AddOrSetValue(AChave, LItem);
  Inc(FRecebidas);
  AtualizarContagem;
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmRetaguarda.NotaStatus(const AChave, AStatus: string);
var
  LItem: TListItem;
begin
  if not FItems.TryGetValue(AChave, LItem) then
    Exit;
  LItem.SubItems[0] := AStatus;
  if AStatus = 'Pronta' then
  begin
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    Inc(FProntas);
    AtualizarContagem;
  end
  else if (AStatus = 'Rejeitada') or (AStatus = 'Rejeitada (requeue)') then
  begin
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    Inc(FRejeitadas);
    AtualizarContagem;
  end;
end;

procedure TfrmRetaguarda.QueueNotaRecebida(const AChave, AWorker: string);
var
  LMarshal: TRecebidaMarshal;
begin
  LMarshal := TRecebidaMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Chave := AChave;
  LMarshal.Worker := AWorker;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure TfrmRetaguarda.QueueNotaStatus(const AChave, AStatus: string);
var
  LMarshal: TStatusMarshal;
begin
  LMarshal := TStatusMarshal.Create;
  LMarshal.Form := Self;
  LMarshal.Chave := AChave;
  LMarshal.Status := AStatus;
  TThread.Queue(nil, LMarshal.Execute);
end;

// ANoAck=False (padrao do Consume): so' confirmamos apos processar - garantia
// "pelo menos uma vez" de fabrica, sem passo extra.
procedure TfrmRetaguarda.OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  LChave, LWorker: string;
  LDelay: Integer;
  LApproval: TPendingApproval;
begin
  LChave := ADelivery.BodyAsText;
  LWorker := Format('%d', [TThread.CurrentThread.ThreadID]);

  QueueNotaRecebida(LChave, LWorker);

  if FManualMode then
  begin
    FPendingLock.Enter;
    try
      if FEncerrando then
        LApproval := nil // CancelarPendencias ja passou; ninguem acordaria o TEvent
      else
      begin
        LApproval := TPendingApproval.Create;
        LApproval.Event := TEvent.Create(nil, True, False, '');
        FPending.Add(LChave, LApproval);
      end;
    finally
      FPendingLock.Leave;
    end;

    if LApproval = nil then
    begin
      // Sai sem ack/nack: o fechamento do canal devolve a mensagem a fila.
      // Nack aqui reiniciaria o ciclo de redelivery imediato.
      QueueNotaStatus(LChave, 'Devolvida (desconexão)');
      Exit;
    end;

    QueueNotaStatus(LChave, 'Aguardando aprovação');

    LApproval.Event.WaitFor(INFINITE);

    FPendingLock.Enter;
    try
      FPending.Remove(LChave);
    finally
      FPendingLock.Leave;
    end;

    try
      if LApproval.Aceitar then
      begin
        AChannel.Ack(ADelivery.DeliveryTag);
        QueueNotaStatus(LChave, 'Pronta');
      end
      else
      begin
        AChannel.Nack(ADelivery.DeliveryTag, LApproval.Requeue);
        if LApproval.Requeue then
          QueueNotaStatus(LChave, 'Rejeitada (requeue)')
        else
          QueueNotaStatus(LChave, 'Rejeitada');
      end;
    finally
      LApproval.Event.Free;
      LApproval.Free;
    end;
    Exit;
  end;

  LDelay := 300 + Random(2200);
  QueueNotaStatus(LChave, 'Processando');

  Sleep(LDelay);

  try
    QueueNotaStatus(LChave, 'Pronta');
    AChannel.Ack(ADelivery.DeliveryTag);
  except
    AChannel.Nack(ADelivery.DeliveryTag, True);
    QueueNotaStatus(LChave, 'Erro');
  end;
end;

procedure TfrmRetaguarda.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LPrefetch: Integer;
  LQueue: string;
  LMsg: string;
begin
  if Assigned(FConn) then
  begin
    try
      CancelarPendencias;
      if (FChannel <> nil) and (FConsumerTag <> '') then
        FChannel.Cancel(FConsumerTag);
      FConsumerTag := '';
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
  LQueue := Trim(edtQueue.Text);
  LPrefetch := StrToIntDef(Trim(edtPrefetch.Text), 10);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.Open;

    FChannel := FConn.CreateChannel(chkDedicado.Checked);
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(LQueue, True));
    FChannel.Qos(LPrefetch);
    FManualMode := chkManual.Checked;
    // Sem lock: nenhum callback vivo aqui (o canal anterior foi drenado no Free).
    FEncerrando := False;

    FConsumerTag := FChannel.Consume(LQueue, OnDelivery);

    LMsg := Format('Conectado a %s:%d%s. Consumindo "%s" (prefetch %d).',
      [LParams.Host, LParams.Port, LParams.VirtualHost, LQueue, LPrefetch]);
    if FManualMode then
      LMsg := LMsg + ' Confirmação manual ativada.';
    if chkDedicado.Checked then
      LMsg := LMsg + ' Thread dedicada (sem concorrência, ordem preservada).';
    Log(LMsg);

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
