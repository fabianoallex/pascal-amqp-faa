program ConsultaStatusVcl;

{ RPC request/reply over AMQP (GUI): the client asks for an invoice's status
  with ReplyTo (exclusive reply queue) + CorrelationId + Expiration, and the
  server consumes the request queue and replies echoing the correlation. Client
  timeout through a TTimer; a reply that arrives after the timeout is dropped
  with a warning. One window plays both roles; run two instances for the truly
  distributed scenario.

  Compiles in both worlds from the SAME source:
    FPC:    lazbuild ConsultaStatusVcl.lpi
    Delphi: open ConsultaStatusVcl.dproj in the IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // real threads on Unix: without it the lib's events/condvars fail at runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uConsultaMain in 'uConsultaMain.pas' {frmConsulta};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmConsulta, frmConsulta);
  Application.Run;
end.
