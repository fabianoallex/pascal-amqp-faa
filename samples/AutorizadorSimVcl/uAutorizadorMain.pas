unit uAutorizadorMain;

{ Tela única: campos de conexão (host/porta/vhost/usuário/senha/TLS) editáveis,
  botão Conectar/Desconectar, e um botão Publicar que dispara N publishes na
  fila configurada — mesmo corpo de mensagem do sample console (chave estilo
  NFE-<execução>-<sequência>). O publish roda direto na thread da UI (é rápido
  e não-concorrente aqui, ao contrário do consumo do RetaguardaVcl).

  Compila nos dois mundos a partir do MESMO fonte (mesmo padrão dos samples
  console): uses sem prefixo de namespace (resolvido via unit scope names no
  Delphi), condicional so' onde o recurso realmente difere (lfm vs dfm). }

interface

uses
  Windows, Messages, SysUtils, Classes,
  Graphics, Controls, Forms, Dialogs, StdCtrls,
  AMQP.Connection, AMQP.Transport, AMQP.Queue.Methods;

type
  TfrmAutorizador = class(TForm)
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
    gbPublicacao: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblQtd: TLabel;
    edtQtd: TEdit;
    btnPublicar: TButton;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnConectarClick(Sender: TObject);
    procedure btnPublicarClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    function MemoAtBottom(AMemo: TMemo): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    function BuildParams: TAMQPConnectionParams;
  end;

var
  frmAutorizador: TfrmAutorizador;

implementation

{$IFDEF FPC}
  {$R *.lfm}
{$ELSE}
  {$R *.dfm}
{$ENDIF}

procedure TfrmAutorizador.FormCreate(Sender: TObject);
begin
  Randomize;
  // Backend TLS deste build (SChannel/OpenSSL/nenhum), pra conferir de cara
  // qual motor um executável usa.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  SetConectado(False);
end;

procedure TfrmAutorizador.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  FreeAndNil(FChannel);
  FreeAndNil(FConn);
end;

function TfrmAutorizador.MemoAtBottom(AMemo: TMemo): Boolean;
var
  LInfo: TScrollInfo;
begin
  FillChar(LInfo, SizeOf(LInfo), 0);
  LInfo.cbSize := SizeOf(LInfo);
  LInfo.fMask := SIF_ALL;
  if not GetScrollInfo(AMemo.Handle, SB_VERT, LInfo) then
    Exit(True); // sem scrollbar ainda (conteúdo cabe todo) = considera "no fim"
  Result := (LInfo.nPos + Integer(LInfo.nPage)) >= LInfo.nMax;
end;

procedure TfrmAutorizador.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := MemoAtBottom(mmoLog);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmAutorizador.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmAutorizador.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmAutorizador.BuildParams: TAMQPConnectionParams;
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

procedure TfrmAutorizador.SetConectado(AConectado: Boolean);
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
  btnPublicar.Enabled := AConectado;
  edtHost.Enabled := not AConectado;
  edtPort.Enabled := not AConectado;
  edtVHost.Enabled := not AConectado;
  edtUser.Enabled := not AConectado;
  edtPassword.Enabled := not AConectado;
  chkUseTls.Enabled := not AConectado;
  chkTlsVerifyPeer.Enabled := (not AConectado) and chkUseTls.Checked;
end;

procedure TfrmAutorizador.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
begin
  if Assigned(FConn) then
  begin
    try
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
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.Open;
    FChannel := FConn.CreateChannel;
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(Trim(edtQueue.Text), True));
    Log(Format('Conectado a %s:%d%s.', [LParams.Host, LParams.Port, LParams.VirtualHost]));
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

procedure TfrmAutorizador.btnPublicarClick(Sender: TObject);
var
  I, Qtd: Integer;
  Chave, Execucao: string;
begin
  if not Assigned(FChannel) then
  begin
    Log('Conecte antes de publicar.');
    Exit;
  end;
  Qtd := StrToIntDef(Trim(edtQtd.Text), 0);
  if Qtd <= 0 then
  begin
    Log('Quantidade inválida.');
    Exit;
  end;
  Execucao := FormatDateTime('hhnnsszzz', Now);
  btnPublicar.Enabled := False;
  try
    for I := 1 to Qtd do
    begin
      Chave := Format('NFE-%s-%.4d', [Execucao, I]);
      try
        FChannel.PublishText('', Trim(edtQueue.Text), Chave);
        Log('Publicado retorno da nota ' + Chave);
      except
        on E: Exception do
        begin
          Log('Erro ao publicar ' + Chave + ': ' + E.Message);
          Break;
        end;
      end;
    end;
  finally
    btnPublicar.Enabled := True;
  end;
end;

end.
