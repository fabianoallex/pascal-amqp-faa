program ConsultaStatusVcl;

{ RPC request/reply sobre AMQP (GUI): cliente pergunta o status de uma nota
  com ReplyTo (fila de respostas exclusiva) + CorrelationId + Expiration, e o
  servidor consome a fila de pedidos e responde ecoando a correlação. Timeout
  no cliente via TTimer; resposta que chega após o timeout é descartada com
  aviso. Uma janela faz os dois papéis — rode duas instâncias para o cenário
  distribuído de verdade.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild ConsultaStatusVcl.lpi
    Delphi: abrir ConsultaStatusVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
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
