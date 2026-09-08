unit uServidorMain;

{ Painel da automacao (ARQUITETURA.md §11). Casca de UI sobre
  TPostoServidorApp: a montagem (broker + registro + comandos + Vigia +
  bombas) vive na App; aqui so' liga callbacks, desenha as listas e oferece
  os botoes do supervisor (liberar forcado / descartar), que agem DIRETO no
  Registro + Eventos (o painel e' parte do servidor).

  A UI e' construida em codigo (ConstroiUI); os .dfm/.lfm sao cascas vazias.
  Nenhuma thread toca a UI: a App sinaliza por callbacks (log com lock, flag
  suja); um TTimer na thread da UI drena o log e reconstroi as listas. }

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

interface

uses
  {$IFDEF FPC}
  Interfaces,
  {$ENDIF}
  SysUtils, Classes, SyncObjs,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ExtCtrls, ComCtrls,
  AMQP.Threading,
  Posto.Abastecida, Posto.Contratos,
  Posto.Servidor.Registro, Posto.Servidor.Vigia, Posto.Servidor.App;

type
  TfrmPainel = class(TForm)
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
  private
    pnlTopo: TPanel;
    lblPorta: TLabel;
    edtPorta: TEdit;
    lblSenhaPdv: TLabel;
    edtSenhaPdv: TEdit;
    btnIniciar: TButton;
    btnParar: TButton;
    chkBombas: TCheckBox;
    lblStatus: TLabel;
    pnlMeio: TPanel;
    lvAbastecidas: TListView;
    pnlDir: TPanel;
    lvPdvs: TListView;
    pnlBotoes: TPanel;
    btnLiberar: TButton;
    btnDescartar: TButton;
    mmoLog: TMemo;
    tmrUi: TTimer;
    FApp: TPostoServidorApp;
    FLogLock: TCriticalSection;
    FLogPend: TStringList;
    FSujo: Integer;
    procedure ConstroiUI;
    procedure QLog(const AMsg: string);
    procedure MarcaSujo;
    procedure SetRodando(AValor: Boolean);
    procedure AtualizaListas;
    procedure tmrUiTimer(Sender: TObject);
    procedure btnIniciarClick(Sender: TObject);
    procedure btnPararClick(Sender: TObject);
    procedure chkBombasClick(Sender: TObject);
    procedure btnLiberarClick(Sender: TObject);
    procedure btnDescartarClick(Sender: TObject);
    function IdSelecionado: string;
  public
  end;

var
  frmPainel: TfrmPainel;

implementation

{$IFDEF FPC}
  {$R *.lfm}
{$ELSE}
  {$R *.dfm}
{$ENDIF}

{ ------------------------------------------------------------------- UI --- }

procedure TfrmPainel.ConstroiUI;

  function NovaCol(ALv: TListView; const ATit: string; ALargura: Integer): TListColumn;
  begin
    Result := ALv.Columns.Add;
    Result.Caption := ATit;
    Result.Width := ALargura;
  end;

begin
  Caption := 'Posto - Painel da Automacao';
  Width := 960;
  Height := 660;
  Position := poScreenCenter;

  pnlTopo := TPanel.Create(Self);
  pnlTopo.Parent := Self;
  pnlTopo.Align := alTop;
  pnlTopo.Height := 76;
  pnlTopo.BevelOuter := bvNone;

  lblPorta := TLabel.Create(Self);
  lblPorta.Parent := pnlTopo;
  lblPorta.SetBounds(12, 12, 40, 20);
  lblPorta.Caption := 'Porta:';

  edtPorta := TEdit.Create(Self);
  edtPorta.Parent := pnlTopo;
  edtPorta.SetBounds(56, 8, 70, 26);
  edtPorta.Text := '5680';

  lblSenhaPdv := TLabel.Create(Self);
  lblSenhaPdv.Parent := pnlTopo;
  lblSenhaPdv.SetBounds(140, 12, 90, 20);
  lblSenhaPdv.Caption := 'Senha PDV:';

  edtSenhaPdv := TEdit.Create(Self);
  edtSenhaPdv.Parent := pnlTopo;
  edtSenhaPdv.SetBounds(232, 8, 90, 26);
  edtSenhaPdv.Text := 'pdv';

  btnIniciar := TButton.Create(Self);
  btnIniciar.Parent := pnlTopo;
  btnIniciar.SetBounds(340, 6, 110, 30);
  btnIniciar.Caption := 'Iniciar';
  btnIniciar.OnClick := btnIniciarClick;

  btnParar := TButton.Create(Self);
  btnParar.Parent := pnlTopo;
  btnParar.SetBounds(458, 6, 110, 30);
  btnParar.Caption := 'Parar';
  btnParar.Enabled := False;
  btnParar.OnClick := btnPararClick;

  chkBombas := TCheckBox.Create(Self);
  chkBombas.Parent := pnlTopo;
  chkBombas.SetBounds(584, 10, 180, 24);
  chkBombas.Caption := 'Gerar abastecidas';
  chkBombas.Checked := True;
  chkBombas.OnClick := chkBombasClick;

  lblStatus := TLabel.Create(Self);
  lblStatus.Parent := pnlTopo;
  lblStatus.SetBounds(12, 46, 900, 20);
  lblStatus.Caption := 'Parado.';
  lblStatus.Font.Style := [fsBold];

  pnlMeio := TPanel.Create(Self);
  pnlMeio.Parent := Self;
  pnlMeio.Align := alClient;
  pnlMeio.BevelOuter := bvNone;

  pnlDir := TPanel.Create(Self);
  pnlDir.Parent := pnlMeio;
  pnlDir.Align := alRight;
  pnlDir.Width := 280;
  pnlDir.BevelOuter := bvNone;

  lvPdvs := TListView.Create(Self);
  lvPdvs.Parent := pnlDir;
  lvPdvs.Align := alClient;
  lvPdvs.ViewStyle := vsReport;
  lvPdvs.ReadOnly := True;
  lvPdvs.RowSelect := True;
  NovaCol(lvPdvs, 'PDV', 90);
  NovaCol(lvPdvs, 'Estado', 100);
  NovaCol(lvPdvs, 'Desde', 80);

  pnlBotoes := TPanel.Create(Self);
  pnlBotoes.Parent := pnlMeio;
  pnlBotoes.Align := alBottom;
  pnlBotoes.Height := 40;
  pnlBotoes.BevelOuter := bvNone;

  btnLiberar := TButton.Create(Self);
  btnLiberar.Parent := pnlBotoes;
  btnLiberar.SetBounds(6, 6, 200, 30);
  btnLiberar.Caption := 'Liberar forcado (selecionada)';
  btnLiberar.OnClick := btnLiberarClick;

  btnDescartar := TButton.Create(Self);
  btnDescartar.Parent := pnlBotoes;
  btnDescartar.SetBounds(214, 6, 200, 30);
  btnDescartar.Caption := 'Descartar (selecionada)';
  btnDescartar.OnClick := btnDescartarClick;

  lvAbastecidas := TListView.Create(Self);
  lvAbastecidas.Parent := pnlMeio;
  lvAbastecidas.Align := alClient;
  lvAbastecidas.ViewStyle := vsReport;
  lvAbastecidas.ReadOnly := True;
  lvAbastecidas.RowSelect := True;
  NovaCol(lvAbastecidas, 'ID', 170);
  NovaCol(lvAbastecidas, 'Bomba/Bico', 90);
  NovaCol(lvAbastecidas, 'Produto', 70);
  NovaCol(lvAbastecidas, 'Litros', 70);
  NovaCol(lvAbastecidas, 'Valor', 80);
  NovaCol(lvAbastecidas, 'Estado', 90);
  NovaCol(lvAbastecidas, 'PDV', 80);
  NovaCol(lvAbastecidas, 'Venda', 120);

  mmoLog := TMemo.Create(Self);
  mmoLog.Parent := Self;
  mmoLog.Align := alBottom;
  mmoLog.Height := 190;
  mmoLog.ReadOnly := True;
  mmoLog.ScrollBars := ssVertical;
  mmoLog.WordWrap := False;

  tmrUi := TTimer.Create(Self);
  tmrUi.Interval := 400;
  tmrUi.OnTimer := tmrUiTimer;
  tmrUi.Enabled := True;
end;

procedure TfrmPainel.FormCreate(Sender: TObject);
begin
  FLogLock := TCriticalSection.Create;
  FLogPend := TStringList.Create;
  FApp := TPostoServidorApp.Create;
  FApp.OnLog := QLog;
  FApp.OnMudou := MarcaSujo;
  ConstroiUI;
  QLog('Pronto. Ajuste a porta e clique Iniciar.');
end;

procedure TfrmPainel.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  FreeAndNil(FApp);
  FLogPend.Free;
  FLogLock.Free;
end;

{ --------------------------------------------------------------- log/UI --- }

procedure TfrmPainel.QLog(const AMsg: string);
begin
  FLogLock.Enter;
  try
    FLogPend.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  finally
    FLogLock.Leave;
  end;
end;

procedure TfrmPainel.MarcaSujo;
begin
  AmqpAtomicSet(FSujo, 1);
end;

procedure TfrmPainel.tmrUiTimer(Sender: TObject);
var
  LLinhas: TArray<string>;
  I: Integer;
begin
  LLinhas := nil;
  FLogLock.Enter;
  try
    if FLogPend.Count > 0 then
    begin
      SetLength(LLinhas, FLogPend.Count);
      for I := 0 to FLogPend.Count - 1 do
        LLinhas[I] := FLogPend[I];
      FLogPend.Clear;
    end;
  finally
    FLogLock.Leave;
  end;

  if Length(LLinhas) > 0 then
  begin
    mmoLog.Lines.BeginUpdate;
    try
      for I := 0 to High(LLinhas) do
        mmoLog.Lines.Add(LLinhas[I]);
      while mmoLog.Lines.Count > 500 do
        mmoLog.Lines.Delete(0);
    finally
      mmoLog.Lines.EndUpdate;
    end;
  end;

  if FApp.Rodando and (AmqpAtomicGet(FSujo) <> 0) then
  begin
    AmqpAtomicSet(FSujo, 0);
    AtualizaListas;
  end;
end;

procedure TfrmPainel.AtualizaListas;
var
  LTodas: TArray<TAbastecidaEstado>;
  LPres: TArray<TInfoPresenca>;
  LE: TAbastecidaEstado;
  LP: TInfoPresenca;
  LIt: TListItem;
  LSelId: string;
  I: Integer;
  LSeg: Int64;
begin
  LSelId := IdSelecionado;
  LTodas := FApp.Registro.Todas;

  lvAbastecidas.Items.BeginUpdate;
  try
    lvAbastecidas.Items.Clear;
    for I := 0 to High(LTodas) do
    begin
      LE := LTodas[I];
      LIt := lvAbastecidas.Items.Add;
      LIt.Caption := LE.Dados.Id;
      LIt.SubItems.Add(Format('%d/%d', [LE.Dados.Bomba, LE.Dados.Bico]));
      LIt.SubItems.Add(LE.Dados.Produto);
      LIt.SubItems.Add(FormatFloat('0.000', LE.Dados.Litros));
      LIt.SubItems.Add(FormatFloat('0.00', LE.Dados.ValorTotal));
      LIt.SubItems.Add(EstadoParaStr(LE.Estado));
      LIt.SubItems.Add(LE.Pdv);
      LIt.SubItems.Add(LE.Venda);
      if LE.Dados.Id = LSelId then
        LIt.Selected := True;
    end;
  finally
    lvAbastecidas.Items.EndUpdate;
  end;

  LPres := FApp.Vigia.Presenca;
  lvPdvs.Items.BeginUpdate;
  try
    lvPdvs.Items.Clear;
    for I := 0 to High(LPres) do
    begin
      LP := LPres[I];
      LIt := lvPdvs.Items.Add;
      LIt.Caption := LP.Usuario;
      if LP.Conectado then
        LIt.SubItems.Add('conectado')
      else
        LIt.SubItems.Add('DESCONECTADO');
      LSeg := (AmqpWallMs - LP.DesdeMs) div 1000;
      LIt.SubItems.Add(Format('%d:%.2d', [LSeg div 60, LSeg mod 60]));
    end;
  finally
    lvPdvs.Items.EndUpdate;
  end;
end;

function TfrmPainel.IdSelecionado: string;
begin
  if lvAbastecidas.Selected <> nil then
    Result := lvAbastecidas.Selected.Caption
  else
    Result := '';
end;

{ --------------------------------------------------------- ciclo de vida --- }

procedure TfrmPainel.SetRodando(AValor: Boolean);
begin
  btnIniciar.Enabled := not AValor;
  btnParar.Enabled := AValor;
  edtPorta.Enabled := not AValor;
  edtSenhaPdv.Enabled := not AValor;
  // chkBombas fica sempre habilitado: liga/desliga a geracao com o servidor no ar
  if AValor then
  begin
    lblStatus.Caption := Format('No ar em 0.0.0.0:%d  (automacao ; PDVs pdv-* / %s)',
      [FApp.PortaEfetiva, edtSenhaPdv.Text]);
    lblStatus.Font.Color := clGreen;
  end
  else
  begin
    lblStatus.Caption := 'Parado.';
    lblStatus.Font.Color := clMaroon;
  end;
end;

procedure TfrmPainel.btnIniciarClick(Sender: TObject);
begin
  try
    FApp.Iniciar(StrToIntDef(Trim(edtPorta.Text), 5680), '',
      Trim(edtSenhaPdv.Text), chkBombas.Checked);
    SetRodando(True);
    MarcaSujo;
  except
    on E: Exception do
    begin
      QLog('FALHA ao iniciar: ' + E.Message);
      try FApp.Parar; except end;
      SetRodando(False);
    end;
  end;
end;

procedure TfrmPainel.btnPararClick(Sender: TObject);
begin
  if FApp.Rodando then
  begin
    FApp.Parar;
    SetRodando(False);
  end;
end;

procedure TfrmPainel.chkBombasClick(Sender: TObject);
begin
  if FApp.Rodando and (FApp.Bombas <> nil) then
  begin
    FApp.Bombas.DefinePausado(not chkBombas.Checked);
    if chkBombas.Checked then
      QLog('Geracao de abastecidas retomada.')
    else
      QLog('Geracao de abastecidas pausada.');
  end;
end;

{ --------------------------------------------------- acoes do supervisor --- }

procedure TfrmPainel.btnLiberarClick(Sender: TObject);
var
  LId: string;
  LR: TResultadoTransicao;
begin
  LId := IdSelecionado;
  if (LId = '') or not FApp.Rodando then
    Exit;
  LR := FApp.Registro.LiberarForcado(LId);
  if LR.Ok and LR.Mudou then
  begin
    FApp.Eventos.Disponivel(LId, MOT_LIBERACAO_MANUAL, LR.Versao);
    QLog(Format('LIBERACAO MANUAL: %s (era %s de %s), versao %d',
      [LId, EstadoParaStr(LR.EstadoAnterior), LR.PdvAnterior, LR.Versao]));
    MarcaSujo;
  end
  else
    QLog('Liberar forcado: nada a fazer para ' + LId);
end;

procedure TfrmPainel.btnDescartarClick(Sender: TObject);
var
  LId: string;
  LR: TResultadoTransicao;
begin
  LId := IdSelecionado;
  if (LId = '') or not FApp.Rodando then
    Exit;
  LR := FApp.Registro.Descartar(LId);
  if LR.Ok and LR.Mudou then
  begin
    FApp.Eventos.Descartada(LId, LR.Versao);
    QLog('DESCARTE: ' + LId + ' (versao ' + IntToStr(LR.Versao) + ')');
    MarcaSujo;
  end
  else if not LR.Ok then
    QLog('Descartar recusado (' + LR.Motivo + '): ' + LId);
end;

end.
