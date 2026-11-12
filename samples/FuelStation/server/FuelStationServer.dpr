program FuelStationServer;

{ Pump automation server with the embedded AMQP broker + the supervisor's
  panel (ARCHITECTURE.md). One source for both compilers:
    FPC:    lazbuild FuelStationServer.lpi
    Delphi: open FuelStationServer.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uServerMain in 'uServerMain.pas' {frmPanel},
  FuelStation.Json in '..\common\FuelStation.Json.pas',
  FuelStation.Fueling in '..\common\FuelStation.Fueling.pas',
  FuelStation.Contracts in '..\common\FuelStation.Contracts.pas',
  FuelStation.Server.Registry in 'FuelStation.Server.Registry.pas',
  FuelStation.Server.Auth in 'FuelStation.Server.Auth.pas',
  FuelStation.Server.Events in 'FuelStation.Server.Events.pas',
  FuelStation.Server.Pumps in 'FuelStation.Server.Pumps.pas',
  FuelStation.Server.Commands in 'FuelStation.Server.Commands.pas',
  FuelStation.Server.Watcher in 'FuelStation.Server.Watcher.pas',
  FuelStation.Server.App in 'FuelStation.Server.App.pas';

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  Application.Initialize;
  Application.CreateForm(TfrmPanel, frmPanel);
  Application.Run;
end.
