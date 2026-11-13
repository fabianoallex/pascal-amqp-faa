unit uPosMain;

{ Point of sale (fuelings slice) -- ARCHITECTURE.md §3, §8, §9.

  Like the server panel: UI built in code (BuildUI), .dfm/.lfm
  are empty shells. No thread touches the UI: Sincronia and Modelo change
  their state (under lock) and signal "dirty" + queue log lines; a TTimer on
  the UI thread drains the log and rebuilds the lists.

  The connection events (OnDisconnect/OnReconnect) arrive on lib threads
  that may die right afterwards -- that is why we do NOT use TThread.Queue here;
  only an atomic flag, a locked log, and firing PcPool workers (gotcha
  in CLAUDE.md; see TopicEventsVcl). }

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

interface

uses
  {$IFDEF FPC}
  Interfaces,
  {$ENDIF}
  SysUtils, Classes, SyncObjs,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ExtCtrls, ComCtrls,
  PascalCommon.Threading,
  PascalCommon.ThreadPool, AMQP.Connection,
  FuelStation.Fueling, FuelStation.Contracts,
  FuelStation.POS.Client, FuelStation.POS.Sync, FuelStation.POS.Model;

type
  TfrmPos = class(TForm)
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
  private
    pnlTop: TPanel;
    lblHost: TLabel;
    edtHost: TEdit;
    lblPort: TLabel;
    edtPort: TEdit;
    lblPosName: TLabel;
    edtPosName: TEdit;
    lblPassword: TLabel;
    edtPassword: TEdit;
    btnConnect: TButton;
    lblStatus: TLabel;
    pnlLeft: TPanel;
    lvPending: TListView;
    btnAdd: TButton;
    pnlRight: TPanel;
    lblSale: TLabel;
    lvItems: TListView;
    pnlSaleButtons: TPanel;
    btnRemove: TButton;
    btnFinish: TButton;
    btnCancel: TButton;
    btnNew: TButton;
    mmoLog: TMemo;
    tmrUi: TTimer;
    FClient: TFuelStationPosClient;
    FSync: TFuelStationSync;
    FModel: TFuelStationModel;
    FLogLock: TCriticalSection;
    FLogPend: TStringList;
    FDirty: Integer;
    FConnected: Boolean;
    procedure BuildUI;
    procedure QLog(const AMsg: string);
    procedure MarkDirty;
    procedure SetConnected(AValue: Boolean);
    procedure RefreshLists;
    procedure tmrUiTimer(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnAddClick(Sender: TObject);
    procedure btnRemoveClick(Sender: TObject);
    procedure btnFinishClick(Sender: TObject);
    procedure btnCancelClick(Sender: TObject);
    procedure btnNewClick(Sender: TObject);
    procedure OnConnDisconnected(AConn: TAMQPConnection);
    procedure OnConnReconnected(AConn: TAMQPConnection);
    procedure OnConnReconnectFailed(AConn: TAMQPConnection);
    function SelectedPendingId: string;
    function SelectedItemId: string;
  public
  end;

var
  frmPos: TfrmPos;

implementation

{$IFDEF FPC}
  {$R *.lfm}
{$ELSE}
  {$R *.dfm}
{$ENDIF}

{ ------------------------------------------------------------------- UI --- }

procedure TfrmPos.BuildUI;

  function NewColumn(ALv: TListView; const ATitle: string; AL: Integer): TListColumn;
  begin
    Result := ALv.Columns.Add;
    Result.Caption := ATitle;
    Result.Width := AL;
  end;

begin
  Caption := 'Fuel Station - POS';
  Width := 900;
  Height := 640;
  Position := poScreenCenter;

  pnlTop := TPanel.Create(Self);
  pnlTop.Parent := Self;
  pnlTop.Align := alTop;
  pnlTop.Height := 76;
  pnlTop.BevelOuter := bvNone;

  lblHost := TLabel.Create(Self);
  lblHost.Parent := pnlTop;
  lblHost.SetBounds(12, 12, 34, 20);
  lblHost.Caption := 'Host:';
  edtHost := TEdit.Create(Self);
  edtHost.Parent := pnlTop;
  edtHost.SetBounds(48, 8, 130, 26);
  edtHost.Text := '127.0.0.1';

  lblPort := TLabel.Create(Self);
  lblPort.Parent := pnlTop;
  lblPort.SetBounds(190, 12, 40, 20);
  lblPort.Caption := 'Port:';
  edtPort := TEdit.Create(Self);
  edtPort.Parent := pnlTop;
  edtPort.SetBounds(234, 8, 66, 26);
  edtPort.Text := '5680';

  lblPosName := TLabel.Create(Self);
  lblPosName.Parent := pnlTop;
  lblPosName.SetBounds(312, 12, 34, 20);
  lblPosName.Caption := 'POS:';
  edtPosName := TEdit.Create(Self);
  edtPosName.Parent := pnlTop;
  edtPosName.SetBounds(348, 8, 90, 26);
  edtPosName.Text := 'pos-01';

  lblPassword := TLabel.Create(Self);
  lblPassword.Parent := pnlTop;
  lblPassword.SetBounds(450, 12, 46, 20);
  lblPassword.Caption := 'Password:';
  edtPassword := TEdit.Create(Self);
  edtPassword.Parent := pnlTop;
  edtPassword.SetBounds(500, 8, 90, 26);
  edtPassword.Text := 'pos';

  btnConnect := TButton.Create(Self);
  btnConnect.Parent := pnlTop;
  btnConnect.SetBounds(604, 6, 120, 30);
  btnConnect.Caption := 'Connect';
  btnConnect.OnClick := btnConnectClick;

  lblStatus := TLabel.Create(Self);
  lblStatus.Parent := pnlTop;
  lblStatus.SetBounds(12, 46, 850, 20);
  lblStatus.Caption := 'Disconnected.';
  lblStatus.Font.Style := [fsBold];

  mmoLog := TMemo.Create(Self);
  mmoLog.Parent := Self;
  mmoLog.Align := alBottom;
  mmoLog.Height := 170;
  mmoLog.ReadOnly := True;
  mmoLog.ScrollBars := ssVertical;
  mmoLog.WordWrap := False;

  pnlLeft := TPanel.Create(Self);
  pnlLeft.Parent := Self;
  pnlLeft.Align := alClient;
  pnlLeft.BevelOuter := bvNone;

  btnAdd := TButton.Create(Self);
  btnAdd.Parent := pnlLeft;
  btnAdd.Align := alBottom;
  btnAdd.Height := 34;
  btnAdd.Caption := 'Add to sale >>';
  btnAdd.OnClick := btnAddClick;

  lvPending := TListView.Create(Self);
  lvPending.Parent := pnlLeft;
  lvPending.Align := alClient;
  lvPending.ViewStyle := vsReport;
  lvPending.ReadOnly := True;
  lvPending.RowSelect := True;
  NewColumn(lvPending, 'ID', 180);
  NewColumn(lvPending, 'P/N', 50);
  NewColumn(lvPending, 'Prod', 55);
  NewColumn(lvPending, 'Liters', 70);
  NewColumn(lvPending, 'Amount', 80);

  pnlRight := TPanel.Create(Self);
  pnlRight.Parent := Self;
  pnlRight.Align := alRight;
  pnlRight.Width := 430;
  pnlRight.BevelOuter := bvNone;

  lblSale := TLabel.Create(Self);
  lblSale.Parent := pnlRight;
  lblSale.Align := alTop;
  lblSale.Caption := 'No open sale';
  lblSale.Font.Style := [fsBold];

  pnlSaleButtons := TPanel.Create(Self);
  pnlSaleButtons.Parent := pnlRight;
  pnlSaleButtons.Align := alBottom;
  pnlSaleButtons.Height := 76;
  pnlSaleButtons.BevelOuter := bvNone;

  btnRemove := TButton.Create(Self);
  btnRemove.Parent := pnlSaleButtons;
  btnRemove.SetBounds(4, 6, 130, 30);
  btnRemove.Caption := 'Remove item';
  btnRemove.OnClick := btnRemoveClick;

  btnNew := TButton.Create(Self);
  btnNew.Parent := pnlSaleButtons;
  btnNew.SetBounds(142, 6, 130, 30);
  btnNew.Caption := 'New sale';
  btnNew.OnClick := btnNewClick;

  btnFinish := TButton.Create(Self);
  btnFinish.Parent := pnlSaleButtons;
  btnFinish.SetBounds(4, 40, 130, 30);
  btnFinish.Caption := 'Finish sale';
  btnFinish.OnClick := btnFinishClick;

  btnCancel := TButton.Create(Self);
  btnCancel.Parent := pnlSaleButtons;
  btnCancel.SetBounds(142, 40, 130, 30);
  btnCancel.Caption := 'Cancel sale';
  btnCancel.OnClick := btnCancelClick;

  lvItems := TListView.Create(Self);
  lvItems.Parent := pnlRight;
  lvItems.Align := alClient;
  lvItems.ViewStyle := vsReport;
  lvItems.ReadOnly := True;
  lvItems.RowSelect := True;
  NewColumn(lvItems, 'ID', 170);
  NewColumn(lvItems, 'Amount', 70);
  NewColumn(lvItems, 'Status', 170);

  tmrUi := TTimer.Create(Self);
  tmrUi.Interval := 400;
  tmrUi.OnTimer := tmrUiTimer;
  tmrUi.Enabled := True;
end;

procedure TfrmPos.FormCreate(Sender: TObject);
begin
  FLogLock := TCriticalSection.Create;
  FLogPend := TStringList.Create;
  BuildUI;
  SetConnected(False);
  QLog('Enter host/port/POS and click Connect.');
end;

procedure TfrmPos.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  FreeAndNil(FModel);
  FreeAndNil(FSync);
  FreeAndNil(FClient);
  FLogPend.Free;
  FLogLock.Free;
end;

{ --------------------------------------------------------------- log/UI --- }

procedure TfrmPos.QLog(const AMsg: string);
begin
  FLogLock.Enter;
  try
    FLogPend.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  finally
    FLogLock.Leave;
  end;
end;

procedure TfrmPos.MarkDirty;
begin
  PcAtomicSet(FDirty, 1);
end;

procedure TfrmPos.tmrUiTimer(Sender: TObject);
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
      while mmoLog.Lines.Count > 400 do
        mmoLog.Lines.Delete(0);
    finally
      mmoLog.Lines.EndUpdate;
    end;
  end;

  if FConnected and (PcAtomicGet(FDirty) <> 0) then
  begin
    PcAtomicSet(FDirty, 0);
    RefreshLists;
  end;
end;

procedure TfrmPos.RefreshLists;
var
  LPending: TArray<TFueling>;
  LItems: TArray<TSaleItem>;
  LIt: TListItem;
  I: Integer;
  LSelPending, LSelItem, LSituation: string;
  LItem: TSaleItem;
  LSale: string;
begin
  LSelPending := SelectedPendingId;
  LSelItem := SelectedItemId;

  LPending := FSync.Pending;
  lvPending.Items.BeginUpdate;
  try
    lvPending.Items.Clear;
    for I := 0 to High(LPending) do
    begin
      LIt := lvPending.Items.Add;
      LIt.Caption := LPending[I].Id;
      LIt.SubItems.Add(Format('%d/%d', [LPending[I].Pump, LPending[I].Nozzle]));
      LIt.SubItems.Add(LPending[I].Product);
      LIt.SubItems.Add(FormatFloat('0.000', LPending[I].Liters));
      LIt.SubItems.Add(FormatFloat('0.00', LPending[I].TotalAmount));
      if LPending[I].Id = LSelPending then
        LIt.Selected := True;
    end;
  finally
    lvPending.Items.EndUpdate;
  end;

  LItems := FModel.SaleItems;
  lvItems.Items.BeginUpdate;
  try
    lvItems.Items.Clear;
    for I := 0 to High(LItems) do
    begin
      LItem := LItems[I];
      LIt := lvItems.Items.Add;
      LIt.Caption := LItem.Id;
      LIt.SubItems.Add(FormatFloat('0.00', LItem.Fueling.TotalAmount));
      if LItem.Conflict then
        LSituation := 'CONFLICT: ' + LItem.Obs
      else if LItem.Confirmed then
        LSituation := 'ok'
      else if LItem.Obs <> '' then
        LSituation := LItem.Obs
      else
        LSituation := 'pending';
      LIt.SubItems.Add(LSituation);
      if LItem.Id = LSelItem then
        LIt.Selected := True;
    end;
  finally
    lvItems.Items.EndUpdate;
  end;

  LSale := FModel.CurrentSale;
  if LSale = '' then
    lblSale.Caption := 'No open sale'
  else
    lblSale.Caption := Format('Sale %s  |  %d items  |  outbox: %d',
      [LSale, Length(LItems), FModel.ItemsInOutbox]);
end;

function TfrmPos.SelectedPendingId: string;
begin
  if lvPending.Selected <> nil then
    Result := lvPending.Selected.Caption
  else
    Result := '';
end;

function TfrmPos.SelectedItemId: string;
begin
  if lvItems.Selected <> nil then
    Result := lvItems.Selected.Caption
  else
    Result := '';
end;

{ ------------------------------------------------------ connection/state --- }

procedure TfrmPos.SetConnected(AValue: Boolean);
begin
  FConnected := AValue;
  if AValue then
  begin
    btnConnect.Caption := 'Disconnect';
    lblStatus.Caption := 'Connected as ' + edtPosName.Text;
    lblStatus.Font.Color := clGreen;
  end
  else
  begin
    btnConnect.Caption := 'Connect';
    lblStatus.Caption := 'Disconnected.';
    lblStatus.Font.Color := clRed;
  end;
  edtHost.Enabled := not AValue;
  edtPort.Enabled := not AValue;
  edtPosName.Enabled := not AValue;
  edtPassword.Enabled := not AValue;
  btnAdd.Enabled := AValue;
  btnRemove.Enabled := AValue;
  btnFinish.Enabled := AValue;
  btnCancel.Enabled := AValue;
  btnNew.Enabled := AValue;
end;

procedure TfrmPos.btnConnectClick(Sender: TObject);
begin
  if FConnected then
  begin
    FreeAndNil(FModel);
    FreeAndNil(FSync);
    FreeAndNil(FClient);
    SetConnected(False);
    QLog('Disconnected.');
    Exit;
  end;

  try
    FClient := TFuelStationPosClient.Create(Trim(edtHost.Text),
      StrToIntDef(Trim(edtPort.Text), 5680), Trim(edtPosName.Text),
      Trim(edtPassword.Text));
    FClient.Connect(OnConnDisconnected, OnConnReconnected, OnConnReconnectFailed);

    FSync := TFuelStationSync.Create(FClient);
    FSync.OnChanged := MarkDirty;

    FModel := TFuelStationModel.Create(FClient, FSync, Trim(edtPosName.Text));
    FModel.OnLog := QLog;
    FModel.OnChanged := MarkDirty;

    FSync.Startup;

    SetConnected(True);
    MarkDirty;
    QLog('Connected. Syncing pending fuelings...');
  except
    on E: Exception do
    begin
      QLog('Failed to connect: ' + E.Message);
      FreeAndNil(FModel);
      FreeAndNil(FSync);
      FreeAndNil(FClient);
      SetConnected(False);
    end;
  end;
end;

// --- connection events (lib threads; no TThread.Queue) ---

procedure TfrmPos.OnConnDisconnected(AConn: TAMQPConnection);
begin
  QLog('Connection lost -- reconnecting. Running in local mode until it is back.');
end;

procedure TfrmPos.OnConnReconnected(AConn: TAMQPConnection);
begin
  QLog('Reconnected. Re-syncing and reconciling the sale...');
  if FSync <> nil then
    FSync.Resync;
  if FModel <> nil then
    FModel.ReconcileAfterReconnect;
  MarkDirty;
end;

procedure TfrmPos.OnConnReconnectFailed(AConn: TAMQPConnection);
begin
  QLog('Reconnection attempts exhausted.');
end;

{ ------------------------------------------------------------- actions --- }

procedure TfrmPos.btnAddClick(Sender: TObject);
var
  LId: string;
begin
  LId := SelectedPendingId;
  if LId = '' then
  begin
    QLog('Select a fueling in the left-hand list.');
    Exit;
  end;
  FModel.AddItem(LId);
end;

procedure TfrmPos.btnRemoveClick(Sender: TObject);
var
  LId: string;
begin
  LId := SelectedItemId;
  if LId = '' then
    Exit;
  FModel.RemoveItem(LId);
end;

procedure TfrmPos.btnFinishClick(Sender: TObject);
begin
  FModel.FinishSale;
end;

procedure TfrmPos.btnCancelClick(Sender: TObject);
begin
  FModel.CancelSale;
end;

procedure TfrmPos.btnNewClick(Sender: TObject);
begin
  FModel.NewSale;
end;

end.
