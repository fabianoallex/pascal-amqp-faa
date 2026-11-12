program PriorityVcl;

{ Priority queue (GUI): queue declared with `x-max-priority`; publish with the
  `Priority` property; a slow consumer with a low prefetch drains the backlog
  in descending priority order. The only sample that exercises the
  Priority property (SetPriority).

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild PriorityVcl.lpi
    Delphi: open PriorityVcl.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uPriorityMain in 'uPriorityMain.pas' {frmPriority};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmPriority, frmPriority);
  Application.Run;
end.
