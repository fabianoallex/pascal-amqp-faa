program AuthorizerSimVcl;

{ VCL version of the AuthorizerSim sample: same simulation (publishes N invoice
  replies to the "authorizer-replies" queue), now with the connection parameters
  editable on screen and a visual log, for a livelier way to test it
  together with BackOfficeVcl: just open both executables side by side.
  Companion of the BackOfficeVcl sample (flow POS -> authorizer -> back office).

  Compiles in both worlds from the SAME source:
    FPC:    fpc -Fu..\..\src -Fi..\..\src AuthorizerSimVcl.dpr (via lazbuild/LCL package)
    Delphi: dcc32 -NSSystem;Winapi;Vcl -U..\..\src -I..\..\src AuthorizerSimVcl.dpr }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uAuthorizerMain in 'uAuthorizerMain.pas' {frmAuthorizer};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmAuthorizer, frmAuthorizer);
  Application.Run;
end.
