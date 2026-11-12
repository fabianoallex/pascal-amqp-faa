program EventosHeadersVcl;

{ Pub/sub with a headers exchange (GUI): routing matches the message's header
  fields against each binding's criteria (x-match all/any), not the
  routing key. The only sample that exercises binding arguments (TAMQPFieldTable
  in BindQueue) and the message's Headers property. Counterpart of EventosTopicVcl.

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild EventosHeadersVcl.lpi
    Delphi: open EventosHeadersVcl.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uHeadersMain in 'uHeadersMain.pas' {frmHeaders};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmHeaders, frmHeaders);
  Application.Run;
end.
