program AutorizadorSimVcl;

{ Versão VCL do sample AutorizadorSim: mesma simulação (publica N retornos de
  nota na fila "sefaz-respostas"), agora com os parâmetros de conexão
  editáveis na tela e um log visual, pra dar uma dinâmica melhor de testar
  junto com o RetaguardaVcl — basta abrir os dois executáveis lado a lado.
  Companheiro do sample RetaguardaVcl (fluxo PDV -> autorizador -> retaguarda).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    fpc -Fu..\..\src -Fi..\..\src AutorizadorSimVcl.dpr (via lazbuild/pacote LCL)
    Delphi: dcc32 -NSSystem;Winapi;Vcl -U..\..\src -I..\..\src AutorizadorSimVcl.dpr }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uAutorizadorMain in 'uAutorizadorMain.pas' {frmAutorizador};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmAutorizador, frmAutorizador);
  Application.Run;
end.
