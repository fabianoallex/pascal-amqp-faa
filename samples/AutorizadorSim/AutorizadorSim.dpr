program AutorizadorSim;

{ Simula o autorizador publicando N retornos de nota quase ao mesmo tempo,
  como se vários PDVs tivessem emitido notas em sequência. Companheiro do
  sample Retaguarda (fluxo PDV -> autorizador -> retaguarda).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    fpc -Fu..\..\src -Fi..\..\src AutorizadorSim.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src AutorizadorSim.dpr }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ELSE}
  {$APPTYPE CONSOLE}
{$ENDIF}

uses
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
  // Sufixo unico por execucao, so pra nao confundir com chaves de rodadas
  // anteriores do teste ao olhar a saida do Retaguarda.
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
