unit uPdvMain;

{ Frente de caixa (recorte de abastecidas) -- ARQUITETURA.md §3, §8, §9.

  Como o painel do servidor: UI construida em codigo (ConstroiUI), .dfm/.lfm
  sao cascas vazias. Nenhuma thread toca a UI: Sincronia e Modelo mexem no
  seu estado (com lock) e sinalizam "sujo" + enfileiram log; um TTimer na
  thread da UI drena o log e reconstroi as listas.

  Os eventos de conexao (OnDisconnect/OnReconnect) chegam por threads da lib
  que podem morrer logo em seguida -- por isso NAO usamos TThread.Queue aqui;
  so' flag atomica, log com lock, e disparo de workers do AmqpPool (gotcha
  do CLAUDE.md; ver EventosTopicVcl). }

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

interface

uses
  {$IFDEF FPC}
  Interfaces,
  {$ENDIF}
  SysUtils, Classes, SyncObjs,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ExtCtrls, ComCtrls,
  AMQP.Threading, AMQP.Connection,
  Posto.Abastecida, Posto.Contratos,
  Posto.PDV.Cliente, Posto.PDV.Sincronia, Posto.PDV.Modelo;

type
  TfrmPdv = class(TForm)
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
  private
    pnlTopo: TPanel;
    lblHost: TLabel;
    edtHost: TEdit;
    lblPorta: TLabel;
    edtPorta: TEdit;
    lblPdv: TLabel;
    edtPdv: TEdit;
    lblSenha: TLabel;
    edtSenha: TEdit;
    btnConectar: TButton;
    lblStatus: TLabel;
    pnlEsq: TPanel;
    lvPendentes: TListView;
    btnAdicionar: TButton;
    pnlDir: TPanel;
    lblVenda: TLabel;
    lvItens: TListView;
    pnlBotoesVenda: TPanel;
    btnRemover: TButton;
    btnFinalizar: TButton;
    btnCancelar: TButton;
    btnNova: TButton;
    mmoLog: TMemo;
    tmrUi: TTimer;
    FCliente: TPostoPdvCliente;
    FSincronia: TPostoSincronia;
    FModelo: TPostoModelo;
    FLogLock: TCriticalSection;
    FLogPend: TStringList;
    FSujo: Integer;
    FConectado: Boolean;
    procedure ConstroiUI;
    procedure QLog(const AMsg: string);
    procedure MarcaSujo;
    procedure SetConectado(AValor: Boolean);
    procedure AtualizaListas;
    procedure tmrUiTimer(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnAdicionarClick(Sender: TObject);
    procedure btnRemoverClick(Sender: TObject);
    procedure btnFinalizarClick(Sender: TObject);
    procedure btnCancelarClick(Sender: TObject);
    procedure btnNovaClick(Sender: TObject);
    procedure OnDesconectado(AConn: TAMQPConnection);
    procedure OnReconectado(AConn: TAMQPConnection);
    procedure OnReconexaoFalhou(AConn: TAMQPConnection);
    function IdPendenteSelecionado: string;
    function IdItemSelecionado: string;
  public
  end;

var
  frmPdv: TfrmPdv;

implementation

{$IFDEF FPC}
  {$R *.lfm}
{$ELSE}
  {$R *.dfm}
{$ENDIF}

{ ------------------------------------------------------------------- UI --- }

procedure TfrmPdv.ConstroiUI;

  function NovaCol(ALv: TListView; const ATit: string; AL: Integer): TListColumn;
  begin
    Result := ALv.Columns.Add;
    Result.Caption := ATit;
    Result.Width := AL;
  end;

begin
  Caption := 'Posto - PDV';
  Width := 900;
  Height := 640;
  Position := poScreenCenter;

  pnlTopo := TPanel.Create(Self);
  pnlTopo.Parent := Self;
  pnlTopo.Align := alTop;
  pnlTopo.Height := 76;
  pnlTopo.BevelOuter := bvNone;

  lblHost := TLabel.Create(Self);
  lblHost.Parent := pnlTopo;
  lblHost.SetBounds(12, 12, 34, 20);
  lblHost.Caption := 'Host:';
  edtHost := TEdit.Create(Self);
  edtHost.Parent := pnlTopo;
  edtHost.SetBounds(48, 8, 130, 26);
  edtHost.Text := '127.0.0.1';

  lblPorta := TLabel.Create(Self);
  lblPorta.Parent := pnlTopo;
  lblPorta.SetBounds(190, 12, 40, 20);
  lblPorta.Caption := 'Porta:';
  edtPorta := TEdit.Create(Self);
  edtPorta.Parent := pnlTopo;
  edtPorta.SetBounds(234, 8, 66, 26);
  edtPorta.Text := '5680';

  lblPdv := TLabel.Create(Self);
  lblPdv.Parent := pnlTopo;
  lblPdv.SetBounds(312, 12, 34, 20);
  lblPdv.Caption := 'PDV:';
  edtPdv := TEdit.Create(Self);
  edtPdv.Parent := pnlTopo;
  edtPdv.SetBounds(348, 8, 90, 26);
  edtPdv.Text := 'pdv-01';

  lblSenha := TLabel.Create(Self);
  lblSenha.Parent := pnlTopo;
  lblSenha.SetBounds(450, 12, 46, 20);
  lblSenha.Caption := 'Senha:';
  edtSenha := TEdit.Create(Self);
  edtSenha.Parent := pnlTopo;
  edtSenha.SetBounds(500, 8, 90, 26);
  edtSenha.Text := 'pdv';

  btnConectar := TButton.Create(Self);
  btnConectar.Parent := pnlTopo;
  btnConectar.SetBounds(604, 6, 120, 30);
  btnConectar.Caption := 'Conectar';
  btnConectar.OnClick := btnConectarClick;

  lblStatus := TLabel.Create(Self);
  lblStatus.Parent := pnlTopo;
  lblStatus.SetBounds(12, 46, 850, 20);
  lblStatus.Caption := 'Desconectado.';
  lblStatus.Font.Style := [fsBold];

  mmoLog := TMemo.Create(Self);
  mmoLog.Parent := Self;
  mmoLog.Align := alBottom;
  mmoLog.Height := 170;
  mmoLog.ReadOnly := True;
  mmoLog.ScrollBars := ssVertical;
  mmoLog.WordWrap := False;

  pnlEsq := TPanel.Create(Self);
  pnlEsq.Parent := Self;
  pnlEsq.Align := alClient;
  pnlEsq.BevelOuter := bvNone;

  btnAdicionar := TButton.Create(Self);
  btnAdicionar.Parent := pnlEsq;
  btnAdicionar.Align := alBottom;
  btnAdicionar.Height := 34;
  btnAdicionar.Caption := 'Adicionar a venda >>';
  btnAdicionar.OnClick := btnAdicionarClick;

  lvPendentes := TListView.Create(Self);
  lvPendentes.Parent := pnlEsq;
  lvPendentes.Align := alClient;
  lvPendentes.ViewStyle := vsReport;
  lvPendentes.ReadOnly := True;
  lvPendentes.RowSelect := True;
  NovaCol(lvPendentes, 'ID', 180);
  NovaCol(lvPendentes, 'B/B', 50);
  NovaCol(lvPendentes, 'Prod', 55);
  NovaCol(lvPendentes, 'Litros', 70);
  NovaCol(lvPendentes, 'Valor', 80);

  pnlDir := TPanel.Create(Self);
  pnlDir.Parent := Self;
  pnlDir.Align := alRight;
  pnlDir.Width := 430;
  pnlDir.BevelOuter := bvNone;

  lblVenda := TLabel.Create(Self);
  lblVenda.Parent := pnlDir;
  lblVenda.Align := alTop;
  lblVenda.Caption := 'Sem venda aberta';
  lblVenda.Font.Style := [fsBold];

  pnlBotoesVenda := TPanel.Create(Self);
  pnlBotoesVenda.Parent := pnlDir;
  pnlBotoesVenda.Align := alBottom;
  pnlBotoesVenda.Height := 76;
  pnlBotoesVenda.BevelOuter := bvNone;

  btnRemover := TButton.Create(Self);
  btnRemover.Parent := pnlBotoesVenda;
  btnRemover.SetBounds(4, 6, 130, 30);
  btnRemover.Caption := 'Remover item';
  btnRemover.OnClick := btnRemoverClick;

  btnNova := TButton.Create(Self);
  btnNova.Parent := pnlBotoesVenda;
  btnNova.SetBounds(142, 6, 130, 30);
  btnNova.Caption := 'Nova venda';
  btnNova.OnClick := btnNovaClick;

  btnFinalizar := TButton.Create(Self);
  btnFinalizar.Parent := pnlBotoesVenda;
  btnFinalizar.SetBounds(4, 40, 130, 30);
  btnFinalizar.Caption := 'Finalizar venda';
  btnFinalizar.OnClick := btnFinalizarClick;

  btnCancelar := TButton.Create(Self);
  btnCancelar.Parent := pnlBotoesVenda;
  btnCancelar.SetBounds(142, 40, 130, 30);
  btnCancelar.Caption := 'Cancelar venda';
  btnCancelar.OnClick := btnCancelarClick;

  lvItens := TListView.Create(Self);
  lvItens.Parent := pnlDir;
  lvItens.Align := alClient;
  lvItens.ViewStyle := vsReport;
  lvItens.ReadOnly := True;
  lvItens.RowSelect := True;
  NovaCol(lvItens, 'ID', 170);
  NovaCol(lvItens, 'Valor', 70);
  NovaCol(lvItens, 'Situacao', 170);

  tmrUi := TTimer.Create(Self);
  tmrUi.Interval := 400;
  tmrUi.OnTimer := tmrUiTimer;
  tmrUi.Enabled := True;
end;

procedure TfrmPdv.FormCreate(Sender: TObject);
begin
  FLogLock := TCriticalSection.Create;
  FLogPend := TStringList.Create;
  ConstroiUI;
  SetConectado(False);
  QLog('Informe host/porta/PDV e clique Conectar.');
end;

procedure TfrmPdv.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  FreeAndNil(FModelo);
  FreeAndNil(FSincronia);
  FreeAndNil(FCliente);
  FLogPend.Free;
  FLogLock.Free;
end;

{ --------------------------------------------------------------- log/UI --- }

procedure TfrmPdv.QLog(const AMsg: string);
begin
  FLogLock.Enter;
  try
    FLogPend.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  finally
    FLogLock.Leave;
  end;
end;

procedure TfrmPdv.MarcaSujo;
begin
  AmqpAtomicSet(FSujo, 1);
end;

procedure TfrmPdv.tmrUiTimer(Sender: TObject);
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
      while mmoLog.Lines.Count > 400 do
        mmoLog.Lines.Delete(0);
    finally
      mmoLog.Lines.EndUpdate;
    end;
  end;

  if FConectado and (AmqpAtomicGet(FSujo) <> 0) then
  begin
    AmqpAtomicSet(FSujo, 0);
    AtualizaListas;
  end;
end;

procedure TfrmPdv.AtualizaListas;
var
  LPend: TArray<TAbastecida>;
  LItens: TArray<TItemVenda>;
  LIt: TListItem;
  I: Integer;
  LSelP, LSelI, LSit: string;
  LItem: TItemVenda;
  LVenda: string;
begin
  LSelP := IdPendenteSelecionado;
  LSelI := IdItemSelecionado;

  LPend := FSincronia.Pendentes;
  lvPendentes.Items.BeginUpdate;
  try
    lvPendentes.Items.Clear;
    for I := 0 to High(LPend) do
    begin
      LIt := lvPendentes.Items.Add;
      LIt.Caption := LPend[I].Id;
      LIt.SubItems.Add(Format('%d/%d', [LPend[I].Bomba, LPend[I].Bico]));
      LIt.SubItems.Add(LPend[I].Produto);
      LIt.SubItems.Add(FormatFloat('0.000', LPend[I].Litros));
      LIt.SubItems.Add(FormatFloat('0.00', LPend[I].ValorTotal));
      if LPend[I].Id = LSelP then
        LIt.Selected := True;
    end;
  finally
    lvPendentes.Items.EndUpdate;
  end;

  LItens := FModelo.Itens;
  lvItens.Items.BeginUpdate;
  try
    lvItens.Items.Clear;
    for I := 0 to High(LItens) do
    begin
      LItem := LItens[I];
      LIt := lvItens.Items.Add;
      LIt.Caption := LItem.Id;
      LIt.SubItems.Add(FormatFloat('0.00', LItem.Abastecida.ValorTotal));
      if LItem.Conflito then
        LSit := 'CONFLITO: ' + LItem.Obs
      else if LItem.Confirmado then
        LSit := 'ok'
      else if LItem.Obs <> '' then
        LSit := LItem.Obs
      else
        LSit := 'pendente';
      LIt.SubItems.Add(LSit);
      if LItem.Id = LSelI then
        LIt.Selected := True;
    end;
  finally
    lvItens.Items.EndUpdate;
  end;

  LVenda := FModelo.VendaAtual;
  if LVenda = '' then
    lblVenda.Caption := 'Sem venda aberta'
  else
    lblVenda.Caption := Format('Venda %s  |  %d itens  |  outbox: %d',
      [LVenda, Length(LItens), FModelo.ItensNoOutbox]);
end;

function TfrmPdv.IdPendenteSelecionado: string;
begin
  if lvPendentes.Selected <> nil then
    Result := lvPendentes.Selected.Caption
  else
    Result := '';
end;

function TfrmPdv.IdItemSelecionado: string;
begin
  if lvItens.Selected <> nil then
    Result := lvItens.Selected.Caption
  else
    Result := '';
end;

{ --------------------------------------------------------- conexao/estado --- }

procedure TfrmPdv.SetConectado(AValor: Boolean);
begin
  FConectado := AValor;
  if AValor then
  begin
    btnConectar.Caption := 'Desconectar';
    lblStatus.Caption := 'Conectado como ' + edtPdv.Text;
    lblStatus.Font.Color := clGreen;
  end
  else
  begin
    btnConectar.Caption := 'Conectar';
    lblStatus.Caption := 'Desconectado.';
    lblStatus.Font.Color := clRed;
  end;
  edtHost.Enabled := not AValor;
  edtPorta.Enabled := not AValor;
  edtPdv.Enabled := not AValor;
  edtSenha.Enabled := not AValor;
  btnAdicionar.Enabled := AValor;
  btnRemover.Enabled := AValor;
  btnFinalizar.Enabled := AValor;
  btnCancelar.Enabled := AValor;
  btnNova.Enabled := AValor;
end;

procedure TfrmPdv.btnConectarClick(Sender: TObject);
begin
  if FConectado then
  begin
    FreeAndNil(FModelo);
    FreeAndNil(FSincronia);
    FreeAndNil(FCliente);
    SetConectado(False);
    QLog('Desconectado.');
    Exit;
  end;

  try
    FCliente := TPostoPdvCliente.Create(Trim(edtHost.Text),
      StrToIntDef(Trim(edtPorta.Text), 5680), Trim(edtPdv.Text),
      Trim(edtSenha.Text));
    FCliente.Conectar(OnDesconectado, OnReconectado, OnReconexaoFalhou);

    FSincronia := TPostoSincronia.Create(FCliente);
    FSincronia.OnMudou := MarcaSujo;

    FModelo := TPostoModelo.Create(FCliente, FSincronia, Trim(edtPdv.Text));
    FModelo.OnLog := QLog;
    FModelo.OnMudou := MarcaSujo;

    FSincronia.Iniciar;

    SetConectado(True);
    MarcaSujo;
    QLog('Conectado. Sincronizando abastecidas pendentes...');
  except
    on E: Exception do
    begin
      QLog('Falha ao conectar: ' + E.Message);
      FreeAndNil(FModelo);
      FreeAndNil(FSincronia);
      FreeAndNil(FCliente);
      SetConectado(False);
    end;
  end;
end;

// --- eventos de conexao (threads da lib; sem TThread.Queue) ---

procedure TfrmPdv.OnDesconectado(AConn: TAMQPConnection);
begin
  QLog('Conexao caiu -- reconectando. Operando em modo local ate voltar.');
end;

procedure TfrmPdv.OnReconectado(AConn: TAMQPConnection);
begin
  QLog('Reconectado. Re-sincronizando e reconciliando a venda...');
  if FSincronia <> nil then
    FSincronia.Ressincronizar;
  if FModelo <> nil then
    FModelo.ReconciliarAposReconexao;
  MarcaSujo;
end;

procedure TfrmPdv.OnReconexaoFalhou(AConn: TAMQPConnection);
begin
  QLog('Reconexao esgotada.');
end;

{ --------------------------------------------------------------- acoes --- }

procedure TfrmPdv.btnAdicionarClick(Sender: TObject);
var
  LId: string;
begin
  LId := IdPendenteSelecionado;
  if LId = '' then
  begin
    QLog('Selecione uma abastecida na lista da esquerda.');
    Exit;
  end;
  FModelo.AdicionarItem(LId);
end;

procedure TfrmPdv.btnRemoverClick(Sender: TObject);
var
  LId: string;
begin
  LId := IdItemSelecionado;
  if LId = '' then
    Exit;
  FModelo.RemoverItem(LId);
end;

procedure TfrmPdv.btnFinalizarClick(Sender: TObject);
begin
  FModelo.FinalizarVenda;
end;

procedure TfrmPdv.btnCancelarClick(Sender: TObject);
begin
  FModelo.CancelarVenda;
end;

procedure TfrmPdv.btnNovaClick(Sender: TObject);
begin
  FModelo.NovaVenda;
end;

end.
