unit uServerMain;

{ The automation panel (ARCHITECTURE.md §11). A UI shell over
  TFuelStationServerApp: the assembly (broker + registry + commands + Watcher +
  pumps) lives in the App; here it only wires callbacks, draws the lists and
  offers the supervisor's buttons (forced release / discard), which act DIRECTLY
  on Registry + Events (the panel is part of the server).

  The UI is built in code (BuildUI); the .dfm/.lfm are empty shells.
  No thread touches the UI: the App signals through callbacks (locked log, dirty
  flag); a TTimer on the UI thread drains the log and rebuilds the lists. }

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

interface

uses
  {$IFDEF FPC}
  Interfaces,
  {$ENDIF}
  SysUtils, Classes, SyncObjs,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ExtCtrls, ComCtrls,
  PascalCommon.Threading,
  AMQP.Threading,
  FuelStation.Fueling, FuelStation.Contracts,
  FuelStation.Server.Registry, FuelStation.Server.Watcher, FuelStation.Server.App;

type
  TfrmPanel = class(TForm)
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
  private
    pnlTop: TPanel;
    lblPort: TLabel;
    edtPort: TEdit;
    lblPosPassword: TLabel;
    edtPosPassword: TEdit;
    btnStart: TButton;
    btnStop: TButton;
    chkPumps: TCheckBox;
    lblStatus: TLabel;
    pnlMiddle: TPanel;
    lvFuelings: TListView;
    pnlRight: TPanel;
    lvPosTerminals: TListView;
    pnlButtons: TPanel;
    btnRelease: TButton;
    btnDiscard: TButton;
    mmoLog: TMemo;
    tmrUi: TTimer;
    FApp: TFuelStationServerApp;
    FLogLock: TCriticalSection;
    FLogPend: TStringList;
    FDirty: Integer;
    procedure BuildUI;
    procedure QLog(const AMsg: string);
    procedure MarkDirty;
    procedure SetRunning(AValue: Boolean);
    procedure RefreshLists;
    procedure tmrUiTimer(Sender: TObject);
    procedure btnStartClick(Sender: TObject);
    procedure btnStopClick(Sender: TObject);
    procedure chkPumpsClick(Sender: TObject);
    procedure btnReleaseClick(Sender: TObject);
    procedure btnDiscardClick(Sender: TObject);
    function SelectedId: string;
  public
  end;

var
  frmPanel: TfrmPanel;

implementation

{$IFDEF FPC}
  {$R *.lfm}
{$ELSE}
  {$R *.dfm}
{$ENDIF}

{ ------------------------------------------------------------------- UI --- }

procedure TfrmPanel.BuildUI;

  function NewColumn(ALv: TListView; const ATitle: string; AWidth: Integer): TListColumn;
  begin
    Result := ALv.Columns.Add;
    Result.Caption := ATitle;
    Result.Width := AWidth;
  end;

begin
  Caption := 'Posto - Painel da Automacao';
  Width := 960;
  Height := 660;
  Position := poScreenCenter;

  pnlTop := TPanel.Create(Self);
  pnlTop.Parent := Self;
  pnlTop.Align := alTop;
  pnlTop.Height := 76;
  pnlTop.BevelOuter := bvNone;

  lblPort := TLabel.Create(Self);
  lblPort.Parent := pnlTop;
  lblPort.SetBounds(12, 12, 40, 20);
  lblPort.Caption := 'Porta:';

  edtPort := TEdit.Create(Self);
  edtPort.Parent := pnlTop;
  edtPort.SetBounds(56, 8, 70, 26);
  edtPort.Text := '5680';

  lblPosPassword := TLabel.Create(Self);
  lblPosPassword.Parent := pnlTop;
  lblPosPassword.SetBounds(140, 12, 90, 20);
  lblPosPassword.Caption := 'Senha PDV:';

  edtPosPassword := TEdit.Create(Self);
  edtPosPassword.Parent := pnlTop;
  edtPosPassword.SetBounds(232, 8, 90, 26);
  edtPosPassword.Text := 'pdv';

  btnStart := TButton.Create(Self);
  btnStart.Parent := pnlTop;
  btnStart.SetBounds(340, 6, 110, 30);
  btnStart.Caption := 'Iniciar';
  btnStart.OnClick := btnStartClick;

  btnStop := TButton.Create(Self);
  btnStop.Parent := pnlTop;
  btnStop.SetBounds(458, 6, 110, 30);
  btnStop.Caption := 'Parar';
  btnStop.Enabled := False;
  btnStop.OnClick := btnStopClick;

  chkPumps := TCheckBox.Create(Self);
  chkPumps.Parent := pnlTop;
  chkPumps.SetBounds(584, 10, 180, 24);
  chkPumps.Caption := 'Gerar abastecidas';
  chkPumps.Checked := True;
  chkPumps.OnClick := chkPumpsClick;

  lblStatus := TLabel.Create(Self);
  lblStatus.Parent := pnlTop;
  lblStatus.SetBounds(12, 46, 900, 20);
  lblStatus.Caption := 'Parado.';
  lblStatus.Font.Style := [fsBold];

  pnlMiddle := TPanel.Create(Self);
  pnlMiddle.Parent := Self;
  pnlMiddle.Align := alClient;
  pnlMiddle.BevelOuter := bvNone;

  pnlRight := TPanel.Create(Self);
  pnlRight.Parent := pnlMiddle;
  pnlRight.Align := alRight;
  pnlRight.Width := 280;
  pnlRight.BevelOuter := bvNone;

  lvPosTerminals := TListView.Create(Self);
  lvPosTerminals.Parent := pnlRight;
  lvPosTerminals.Align := alClient;
  lvPosTerminals.ViewStyle := vsReport;
  lvPosTerminals.ReadOnly := True;
  lvPosTerminals.RowSelect := True;
  NewColumn(lvPosTerminals, 'PDV', 90);
  NewColumn(lvPosTerminals, 'Estado', 100);
  NewColumn(lvPosTerminals, 'Desde', 80);

  pnlButtons := TPanel.Create(Self);
  pnlButtons.Parent := pnlMiddle;
  pnlButtons.Align := alBottom;
  pnlButtons.Height := 40;
  pnlButtons.BevelOuter := bvNone;

  btnRelease := TButton.Create(Self);
  btnRelease.Parent := pnlButtons;
  btnRelease.SetBounds(6, 6, 200, 30);
  btnRelease.Caption := 'Liberar forcado (selecionada)';
  btnRelease.OnClick := btnReleaseClick;

  btnDiscard := TButton.Create(Self);
  btnDiscard.Parent := pnlButtons;
  btnDiscard.SetBounds(214, 6, 200, 30);
  btnDiscard.Caption := 'Descartar (selecionada)';
  btnDiscard.OnClick := btnDiscardClick;

  lvFuelings := TListView.Create(Self);
  lvFuelings.Parent := pnlMiddle;
  lvFuelings.Align := alClient;
  lvFuelings.ViewStyle := vsReport;
  lvFuelings.ReadOnly := True;
  lvFuelings.RowSelect := True;
  NewColumn(lvFuelings, 'ID', 170);
  NewColumn(lvFuelings, 'Bomba/Bico', 90);
  NewColumn(lvFuelings, 'Produto', 70);
  NewColumn(lvFuelings, 'Litros', 70);
  NewColumn(lvFuelings, 'Valor', 80);
  NewColumn(lvFuelings, 'Estado', 90);
  NewColumn(lvFuelings, 'PDV', 80);
  NewColumn(lvFuelings, 'Venda', 120);

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

procedure TfrmPanel.FormCreate(Sender: TObject);
begin
  FLogLock := TCriticalSection.Create;
  FLogPend := TStringList.Create;
  FApp := TFuelStationServerApp.Create;
  FApp.OnLog := QLog;
  FApp.OnChanged := MarkDirty;
  BuildUI;
  QLog('Pronto. Ajuste a porta e clique Iniciar.');
end;

procedure TfrmPanel.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  FreeAndNil(FApp);
  FLogPend.Free;
  FLogLock.Free;
end;

{ --------------------------------------------------------------- log/UI --- }

procedure TfrmPanel.QLog(const AMsg: string);
begin
  FLogLock.Enter;
  try
    FLogPend.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  finally
    FLogLock.Leave;
  end;
end;

procedure TfrmPanel.MarkDirty;
begin
  PcAtomicSet(FDirty, 1);
end;

procedure TfrmPanel.tmrUiTimer(Sender: TObject);
var
  LLines: TArray<string>;
  I: Integer;
begin
  LLines := nil;
  FLogLock.Enter;
  try
    if FLogPend.Count > 0 then
    begin
      SetLength(LLines, FLogPend.Count);
      for I := 0 to FLogPend.Count - 1 do
        LLines[I] := FLogPend[I];
      FLogPend.Clear;
    end;
  finally
    FLogLock.Leave;
  end;

  if Length(LLines) > 0 then
  begin
    mmoLog.Lines.BeginUpdate;
    try
      for I := 0 to High(LLines) do
        mmoLog.Lines.Add(LLines[I]);
      while mmoLog.Lines.Count > 500 do
        mmoLog.Lines.Delete(0);
    finally
      mmoLog.Lines.EndUpdate;
    end;
  end;

  if FApp.Running and (PcAtomicGet(FDirty) <> 0) then
  begin
    PcAtomicSet(FDirty, 0);
    RefreshLists;
  end;
end;

procedure TfrmPanel.RefreshLists;
var
  LAll: TArray<TTrackedFueling>;
  LPresence: TArray<TPresenceInfo>;
  LE: TTrackedFueling;
  LP: TPresenceInfo;
  LIt: TListItem;
  LSelId: string;
  I: Integer;
  LSeg: Int64;
begin
  LSelId := SelectedId;
  LAll := FApp.Registry.All;

  lvFuelings.Items.BeginUpdate;
  try
    lvFuelings.Items.Clear;
    for I := 0 to High(LAll) do
    begin
      LE := LAll[I];
      LIt := lvFuelings.Items.Add;
      LIt.Caption := LE.Data.Id;
      LIt.SubItems.Add(Format('%d/%d', [LE.Data.Pump, LE.Data.Nozzle]));
      LIt.SubItems.Add(LE.Data.Product);
      LIt.SubItems.Add(FormatFloat('0.000', LE.Data.Liters));
      LIt.SubItems.Add(FormatFloat('0.00', LE.Data.TotalAmount));
      LIt.SubItems.Add(StateToStr(LE.State));
      LIt.SubItems.Add(LE.PosName);
      LIt.SubItems.Add(LE.Sale);
      if LE.Data.Id = LSelId then
        LIt.Selected := True;
    end;
  finally
    lvFuelings.Items.EndUpdate;
  end;

  LPresence := FApp.Watcher.Presence;
  lvPosTerminals.Items.BeginUpdate;
  try
    lvPosTerminals.Items.Clear;
    for I := 0 to High(LPresence) do
    begin
      LP := LPresence[I];
      LIt := lvPosTerminals.Items.Add;
      LIt.Caption := LP.LoginUser;
      if LP.Connected then
        LIt.SubItems.Add('conectado')
      else
        LIt.SubItems.Add('DESCONECTADO');
      LSeg := (AmqpWallMs - LP.SinceMs) div 1000;
      LIt.SubItems.Add(Format('%d:%.2d', [LSeg div 60, LSeg mod 60]));
    end;
  finally
    lvPosTerminals.Items.EndUpdate;
  end;
end;

function TfrmPanel.SelectedId: string;
begin
  if lvFuelings.Selected <> nil then
    Result := lvFuelings.Selected.Caption
  else
    Result := '';
end;

{ ---------------------------------------------------------- life cycle --- }

procedure TfrmPanel.SetRunning(AValue: Boolean);
begin
  btnStart.Enabled := not AValue;
  btnStop.Enabled := AValue;
  edtPort.Enabled := not AValue;
  edtPosPassword.Enabled := not AValue;
  // chkPumps stays always enabled: turns generation on/off with the server up
  if AValue then
  begin
    lblStatus.Caption := Format('No ar em 0.0.0.0:%d  (automacao ; PDVs pdv-* / %s)',
      [FApp.EffectivePort, edtPosPassword.Text]);
    lblStatus.Font.Color := clGreen;
  end
  else
  begin
    lblStatus.Caption := 'Parado.';
    lblStatus.Font.Color := clMaroon;
  end;
end;

procedure TfrmPanel.btnStartClick(Sender: TObject);
begin
  try
    FApp.Startup(StrToIntDef(Trim(edtPort.Text), 5680), '',
      Trim(edtPosPassword.Text), chkPumps.Checked);
    SetRunning(True);
    MarkDirty;
  except
    on E: Exception do
    begin
      QLog('FALHA ao iniciar: ' + E.Message);
      try FApp.Shutdown; except end;
      SetRunning(False);
    end;
  end;
end;

procedure TfrmPanel.btnStopClick(Sender: TObject);
begin
  if FApp.Running then
  begin
    FApp.Shutdown;
    SetRunning(False);
  end;
end;

procedure TfrmPanel.chkPumpsClick(Sender: TObject);
begin
  if FApp.Running and (FApp.Pumps <> nil) then
  begin
    FApp.Pumps.SetPaused(not chkPumps.Checked);
    if chkPumps.Checked then
      QLog('Geracao de abastecidas retomada.')
    else
      QLog('Geracao de abastecidas pausada.');
  end;
end;

{ ------------------------------------------------- supervisor actions --- }

procedure TfrmPanel.btnReleaseClick(Sender: TObject);
var
  LId: string;
  LR: TTransitionResult;
begin
  LId := SelectedId;
  if (LId = '') or not FApp.Running then
    Exit;
  LR := FApp.Registry.ForceRelease(LId);
  if LR.Ok and LR.Changed then
  begin
    FApp.Events.Available(LId, RSN_MANUAL_RELEASE, LR.Version);
    QLog(Format('LIBERACAO MANUAL: %s (era %s de %s), versao %d',
      [LId, StateToStr(LR.PreviousState), LR.PreviousPos, LR.Version]));
    MarkDirty;
  end
  else
    QLog('Liberar forcado: nada a fazer para ' + LId);
end;

procedure TfrmPanel.btnDiscardClick(Sender: TObject);
var
  LId: string;
  LR: TTransitionResult;
begin
  LId := SelectedId;
  if (LId = '') or not FApp.Running then
    Exit;
  LR := FApp.Registry.Discard(LId);
  if LR.Ok and LR.Changed then
  begin
    FApp.Events.Discarded(LId, LR.Version);
    QLog('DESCARTE: ' + LId + ' (versao ' + IntToStr(LR.Version) + ')');
    MarkDirty;
  end
  else if not LR.Ok then
    QLog('Descartar recusado (' + LR.Reason + '): ' + LId);
end;

end.
