program AutorizadorSim;

{ Simulates the authorizer publishing N invoice replies almost at once,
  as if several POS terminals had issued invoices in a row. Companion of the
  Retaguarda sample (flow POS -> authorizer -> back office).

  Compiles in both worlds from the SAME source:
    FPC:    fpc -Fu..\..\src -Fi..\..\src AutorizadorSim.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src AutorizadorSim.dpr }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ELSE}
  {$APPTYPE CONSOLE}
{$ENDIF}

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  {$ENDIF}
  SysUtils,
  AMQP.Connection,
  AMQP.Queue.Methods;

const
  QUEUE_NAME = 'sefaz-respostas';
  QTD_NOTAS = 6;

procedure Main;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChannel: TAMQPChannel;
  I: Integer;
  Chave: string;
  Execucao: string;
begin
  // Suffix unique per run, just so they are not confused with keys from earlier
  // test rounds when reading the Retaguarda output.
  Execucao := FormatDateTime('hhnnsszzz', Now);

  LParams := TAMQPConnectionParams.Localhost;
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.Open;
    LChannel := LConn.CreateChannel;
    try
      LChannel.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_NAME, True));

      for I := 1 to QTD_NOTAS do
      begin
        Chave := Format('NFE-%s-%.4d', [Execucao, I]);
        LChannel.PublishText('', QUEUE_NAME, Chave);
        Writeln('[Autorizador] Publicado retorno da nota ', Chave);
      end;
    finally
      LChannel.Free;
    end;
  finally
    LConn.Free;
  end;
end;

begin
  try
    Main;
  except
    on E: Exception do
      Writeln('Erro: ', E.Message);
  end;
end.
