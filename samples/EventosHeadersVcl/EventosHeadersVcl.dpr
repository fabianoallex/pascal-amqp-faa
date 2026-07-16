program EventosHeadersVcl;

{ Pub/sub com exchange headers (GUI): o roteamento usa o casamento de campos do
  header da mensagem contra critérios de cada binding (x-match all/any), não a
  routing key. Único sample que exercita binding arguments (TAMQPFieldTable no
  BindQueue) e a propriedade Headers da mensagem. Contraponto do EventosTopicVcl.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild EventosHeadersVcl.lpi
    Delphi: abrir EventosHeadersVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
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
