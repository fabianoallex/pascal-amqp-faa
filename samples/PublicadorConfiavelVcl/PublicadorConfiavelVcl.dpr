program PublicadorConfiavelVcl;

{ Publicador confiável (GUI): demonstra publisher confirms de ponta a ponta —
  ConfirmSelect, OnConfirm (ack/nack por seq-no), Basic.Return de publish
  mandatory sem rota, WaitForConfirms ao fim do lote e o reenvio automático
  dos publishes não confirmados após uma queda de conexão
  (RepublishUnconfirmedOnReconnect). Publique um lote com intervalo e derrube
  o broker no meio (docker stop/start) para ver a história completa no log.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild PublicadorConfiavelVcl.lpi
    Delphi: abrir PublicadorConfiavelVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
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
