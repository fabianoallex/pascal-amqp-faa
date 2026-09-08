program PdvPosto;

{ PDV do posto: ve as abastecidas pendentes e as lanca numa venda
  (ARQUITETURA.md). So' precisa do pacote cliente -- nao linka o submodulo
  server. Um fonte para os dois compiladores:
    FPC:    lazbuild PdvPosto.lpi
    Delphi: abrir PdvPosto.dproj no IDE }

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
