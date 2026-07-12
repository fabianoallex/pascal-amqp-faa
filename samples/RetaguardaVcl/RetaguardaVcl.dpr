program RetaguardaVcl;

{ Versão VCL do sample Retaguarda: consome a fila "sefaz-respostas" e mostra
  o status de cada nota (Recebida -> Processando -> Pronta) numa lista ao
  vivo, em vez do log rolante do console — dá pra ver de relance quantas
  notas chegaram e quantas já foram processadas, com vários workers do
  thread pool rodando em paralelo (mesmo despacho nativo do Channel.Consume,
  sem TTask.Run manual). Companheiro do sample AutorizadorSimVcl (fluxo PDV
  -> autorizador -> retaguarda).

  Compila nos dois mundos a partir do MESMO fonte: mesmo padrão dos outros
  samples deste repositório. }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uRetaguardaMain in 'uRetaguardaMain.pas' {frmRetaguarda};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmRetaguarda, frmRetaguarda);
  Application.Run;
end.
