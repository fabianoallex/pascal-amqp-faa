program AutorizadorSimVcl;

{ VCL version of the AutorizadorSim sample: same simulation (publishes N invoice
  replies to the "sefaz-respostas" queue), now with the connection parameters
  editable on screen and a visual log, for a livelier way to test it
  together with RetaguardaVcl: just open both executables side by side.
  Companion of the RetaguardaVcl sample (flow POS -> authorizer -> back office).

  Compiles in both worlds from the SAME source:
    FPC:    fpc -Fu..\..\src -Fi..\..\src AutorizadorSimVcl.dpr (via lazbuild/LCL package)
    Delphi: dcc32 -NSSystem;Winapi;Vcl -U..\..\src -I..\..\src AutorizadorSimVcl.dpr }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uAutorizadorMain in 'uAutorizadorMain.pas' {frmAutorizador};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmAutorizador, frmAutorizador);
  Application.Run;
end.
