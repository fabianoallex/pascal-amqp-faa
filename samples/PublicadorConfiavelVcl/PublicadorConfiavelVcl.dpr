program PublicadorConfiavelVcl;

{ Reliable publisher (GUI): shows publisher confirms end to end:
  ConfirmSelect, OnConfirm (ack/nack per seq-no), Basic.Return of an unroutable
  mandatory publish, WaitForConfirms at the end of the batch and the automatic
  resend of unconfirmed publishes after a connection drop
  (RepublishUnconfirmedOnReconnect). Publish a batch with an interval and bring
  the broker down in the middle (docker stop/start) to see the whole story in the log.

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild PublicadorConfiavelVcl.lpi
    Delphi: open PublicadorConfiavelVcl.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uPublicadorMain in 'uPublicadorMain.pas' {frmPublicador};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmPublicador, frmPublicador);
  Application.Run;
end.
