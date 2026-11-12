program RetryDlqVcl;

{ Dead-letter + retry with backoff (GUI): a work queue with a DLX pointing
  to a wait queue (x-message-ttl = backoff) that hands back to the work
  queue; after the attempts run out (counted through the x-death header), the
  consumer moves the message to the DLQ. Topology built only with queue
  arguments (TAMQPFieldTable) and recreated on every connection through DeleteQueue.

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild RetryDlqVcl.lpi
    Delphi: open RetryDlqVcl.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uRetryMain in 'uRetryMain.pas' {frmRetry};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmRetry, frmRetry);
  Application.Run;
end.
