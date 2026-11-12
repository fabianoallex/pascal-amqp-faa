program FuelStationPos;

{ The station's POS: sees the pending fuelings and posts them to a sale
  (ARCHITECTURE.md). It only needs the client package -- it does not link the
  server sub-module. One source for both compilers:
    FPC:    lazbuild FuelStationPos.lpi
    Delphi: open FuelStationPos.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uPosMain in 'uPosMain.pas' {frmPos},
  FuelStation.Json in '..\common\FuelStation.Json.pas',
  FuelStation.Fueling in '..\common\FuelStation.Fueling.pas',
  FuelStation.Contracts in '..\common\FuelStation.Contracts.pas',
  FuelStation.POS.Client in 'FuelStation.POS.Client.pas',
  FuelStation.POS.Sync in 'FuelStation.POS.Sync.pas',
  FuelStation.POS.Model in 'FuelStation.POS.Model.pas';

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  Application.Initialize;
  Application.CreateForm(TfrmPos, frmPos);
  Application.Run;
end.
