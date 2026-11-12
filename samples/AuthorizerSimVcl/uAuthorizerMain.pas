unit uAuthorizerMain;

{ Single screen: editable connection fields (host/port/vhost/user/password/TLS),
  a Connect/Disconnect button, and a Publish button that fires N publishes to
  the configured queue, with the same message body as the console sample (key in
  the form NFE-<run>-<sequence>). The publish runs straight on the UI thread (it
  is fast and not concurrent here, unlike the consumption in BackOfficeVcl).

  Compiles in both worlds from the SAME source (same pattern as the console
  samples): uses without a namespace prefix (resolved through unit scope names
  in Delphi), conditional only where the feature really differs (lfm vs dfm). }

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
  Graphics, Controls, Forms, Dialogs, StdCtrls,
  AMQP.Connection, AMQP.Transport, AMQP.Queue.Methods;

type
  TfrmAuthorizer = class(TForm)
    gbConnection: TGroupBox;
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
    btnConnect: TButton;
    lblStatus: TLabel;
    gbPublishing: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblCount: TLabel;
    edtCount: TEdit;
    btnPublish: TButton;
    btnClearLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure btnConnectClick(Sender: TObject);
    procedure btnPublishClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    function MemoAtBottom(AMemo: TMemo): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConnected(AConnected: Boolean);
    function BuildParams: TAMQPConnectionParams;
  end;

var
  frmAuthorizer: TfrmAuthorizer;

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

procedure TfrmAuthorizer.FormCreate(Sender: TObject);
begin
  Randomize;
  // TLS backend of this build (SChannel/OpenSSL/none), to see at a glance
  // which engine an executable uses.
  Caption := Caption + '  [TLS: ' + AmqpTlsBackendName + ']';
  SetConnected(False);
end;

procedure TfrmAuthorizer.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  FreeAndNil(FChannel);
  FreeAndNil(FConn);
end;

function TfrmAuthorizer.MemoAtBottom(AMemo: TMemo): Boolean;
var
  LInfo: TScrollInfo;
begin
  FillChar(LInfo, SizeOf(LInfo), 0);
  LInfo.cbSize := SizeOf(LInfo);
  LInfo.fMask := SIF_ALL;
  if not GetScrollInfo(AMemo.Handle, SB_VERT, LInfo) then
    Exit(True); // no scrollbar yet (all content fits) = treat as "at the end"
  Result := (LInfo.nPos + Integer(LInfo.nPage)) >= LInfo.nMax;
end;

procedure TfrmAuthorizer.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := MemoAtBottom(mmoLog);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmAuthorizer.btnClearLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmAuthorizer.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmAuthorizer.BuildParams: TAMQPConnectionParams;
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

procedure TfrmAuthorizer.SetConnected(AConnected: Boolean);
begin
  if AConnected then
  begin
    btnConnect.Caption := 'Desconectar';
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
    btnConnect.Caption := 'Conectar';
    lblStatus.Caption := 'Desconectado';
    lblStatus.Font.Color := clRed;
  end;
  btnPublish.Enabled := AConnected;
  edtHost.Enabled := not AConnected;
  edtPort.Enabled := not AConnected;
  edtVHost.Enabled := not AConnected;
  edtUser.Enabled := not AConnected;
  edtPassword.Enabled := not AConnected;
  chkUseTls.Enabled := not AConnected;
  chkTlsVerifyPeer.Enabled := (not AConnected) and chkUseTls.Checked;
end;

procedure TfrmAuthorizer.btnConnectClick(Sender: TObject);
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
    SetConnected(False);
    Exit;
  end;

  LParams := BuildParams;
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.Open;
    FChannel := FConn.CreateChannel;
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(Trim(edtQueue.Text), True));
    Log(Format('Conectado a %s:%d%s.', [LParams.Host, LParams.Port, LParams.VirtualHost]));
    SetConnected(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      SetConnected(False);
    end;
  end;
end;

procedure TfrmAuthorizer.btnPublishClick(Sender: TObject);
var
  I, Count: Integer;
  InvoiceKey, RunId: string;
begin
  if not Assigned(FChannel) then
  begin
    Log('Conecte antes de publicar.');
    Exit;
  end;
  Count := StrToIntDef(Trim(edtCount.Text), 0);
  if Count <= 0 then
  begin
    Log('Quantidade inválida.');
    Exit;
  end;
  RunId := FormatDateTime('hhnnsszzz', Now);
  btnPublish.Enabled := False;
  try
    for I := 1 to Count do
    begin
      InvoiceKey := Format('NFE-%s-%.4d', [RunId, I]);
      try
        FChannel.PublishText('', Trim(edtQueue.Text), InvoiceKey);
        Log('Publicado retorno da nota ' + InvoiceKey);
      except
        on E: Exception do
        begin
          Log('Erro ao publicar ' + InvoiceKey + ': ' + E.Message);
          Break;
        end;
      end;
    end;
  finally
    btnPublish.Enabled := True;
  end;
end;

end.
