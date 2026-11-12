program PrioridadeVcl;

{ Priority queue (GUI): queue declared with `x-max-priority`; publish with the
  `Priority` property; a slow consumer with a low prefetch drains the backlog
  in descending priority order. The only sample that exercises the
  Priority property (SetPriority).

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild PrioridadeVcl.lpi
    Delphi: open PrioridadeVcl.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uPrioridadeMain in 'uPrioridadeMain.pas' {frmPrioridade};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmPrioridade, frmPrioridade);
  Application.Run;
end.
