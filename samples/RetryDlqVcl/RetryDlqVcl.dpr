program RetryDlqVcl;

{ Dead-letter + retry com backoff (GUI): fila de trabalho com DLX apontando
  para uma fila de espera (x-message-ttl = backoff) que devolve à fila de
  trabalho; após esgotar as tentativas (contadas pelo header x-death), o
  consumidor move a mensagem para a DLQ. Topologia montada só com argumentos
  de fila (TAMQPFieldTable) e recriada a cada conexão via DeleteQueue.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild RetryDlqVcl.lpi
    Delphi: abrir RetryDlqVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
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
