program PrioridadeVcl;

{ Fila com prioridade (GUI): fila declarada com `x-max-priority`; publish com a
  propriedade `Priority`; consumidor lento com prefetch baixo drena o backlog
  em ordem decrescente de prioridade. Único sample que exercita a propriedade
  Priority (SetPriority).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild PrioridadeVcl.lpi
    Delphi: abrir PrioridadeVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uPrioridadeMain in 'uPrioridadeMain.pas' {frmPrioridade};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmPrioridade, frmPrioridade);
  Application.Run;
end.
