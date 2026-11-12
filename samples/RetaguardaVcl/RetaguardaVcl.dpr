program RetaguardaVcl;

{ VCL version of the Retaguarda sample: consumes the "sefaz-respostas" queue and
  shows each invoice's status (Recebida -> Processando -> Pronta) in a live
  list, instead of the console's scrolling log: you can see at a glance how many
  invoices arrived and how many were already processed, with several thread
  pool workers running in parallel (the same native dispatch of Channel.Consume,
  without a manual TTask.Run). Companion of the AutorizadorSimVcl sample (flow POS
  -> authorizer -> back office).

  Compiles in both worlds from the SAME source: same pattern as the other
  samples in this repository. }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uRetaguardaMain in 'uRetaguardaMain.pas' {frmRetaguarda};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmRetaguarda, frmRetaguarda);
  Application.Run;
end.
