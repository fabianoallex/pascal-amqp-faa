program PdvPosto;

{ The station's POS: sees the pending fuelings and posts them to a sale
  (ARCHITECTURE.md). It only needs the client package -- it does not link the
  server sub-module. One source for both compilers:
    FPC:    lazbuild PdvPosto.lpi
    Delphi: open PdvPosto.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uPdvMain in 'uPdvMain.pas' {frmPdv},
  Posto.Json in '..\comum\Posto.Json.pas',
  Posto.Abastecida in '..\comum\Posto.Abastecida.pas',
  Posto.Contratos in '..\comum\Posto.Contratos.pas',
  Posto.PDV.Cliente in 'Posto.PDV.Cliente.pas',
  Posto.PDV.Sincronia in 'Posto.PDV.Sincronia.pas',
  Posto.PDV.Modelo in 'Posto.PDV.Modelo.pas';

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  Application.Initialize;
  Application.CreateForm(TfrmPdv, frmPdv);
  Application.Run;
end.
