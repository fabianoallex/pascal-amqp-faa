program ServidorPosto;

{ Pump automation server with the embedded AMQP broker + the supervisor's
  panel (ARCHITECTURE.md). One source for both compilers:
    FPC:    lazbuild ServidorPosto.lpi
    Delphi: open ServidorPosto.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uServidorMain in 'uServidorMain.pas' {frmPainel},
  Posto.Json in '..\comum\Posto.Json.pas',
  Posto.Abastecida in '..\comum\Posto.Abastecida.pas',
  Posto.Contratos in '..\comum\Posto.Contratos.pas',
  Posto.Servidor.Registro in 'Posto.Servidor.Registro.pas',
  Posto.Servidor.Auth in 'Posto.Servidor.Auth.pas',
  Posto.Servidor.Eventos in 'Posto.Servidor.Eventos.pas',
  Posto.Servidor.Bombas in 'Posto.Servidor.Bombas.pas',
  Posto.Servidor.Comandos in 'Posto.Servidor.Comandos.pas',
  Posto.Servidor.Vigia in 'Posto.Servidor.Vigia.pas',
  Posto.Servidor.App in 'Posto.Servidor.App.pas';

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  Application.Initialize;
  Application.CreateForm(TfrmPainel, frmPainel);
  Application.Run;
end.
