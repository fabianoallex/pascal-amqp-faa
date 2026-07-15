program EventosTopicVcl;

{ Pub/sub com exchange topic (GUI): assinantes dinâmicos, cada um com fila
  exclusiva/auto-delete ligada ao exchange pela binding key ('*' = uma
  palavra, '#' = zero ou mais); eventos publicados com routing keys
  hierárquicas chegam a todo assinante que casar — ou a nenhum (mandatory +
  Basic.Return torna o descarte visível). Único sample que exercita
  DeclareExchange/BindQueue/UnbindQueue.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild EventosTopicVcl.lpi
    Delphi: abrir EventosTopicVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
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
