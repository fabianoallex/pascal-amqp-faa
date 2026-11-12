program EventosTopicVcl;

{ Pub/sub with a topic exchange (GUI): dynamic subscribers, each with an
  exclusive/auto-delete queue bound to the exchange by the binding key ('*' = one
  word, '#' = zero or more); events published with hierarchical routing keys
  reach every subscriber that matches, or none (mandatory +
  Basic.Return makes the drop visible). The only sample that exercises
  DeclareExchange/BindQueue/UnbindQueue.

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild EventosTopicVcl.lpi
    Delphi: open EventosTopicVcl.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uEventosMain in 'uEventosMain.pas' {frmEventos};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmEventos, frmEventos);
  Application.Run;
end.
