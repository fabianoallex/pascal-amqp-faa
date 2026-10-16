unit AMQP.DisconnectInfoIntegrationTests;

{ Motivos de queda e códigos AMQP — precisa de um broker (RabbitMQ ou o
  embutido, pela aceitação).

  - OnDisconnect dispara também sem AutoReconnect (antes, a queda só zerava
    IsOpen, sem evento nenhum), uma vez, sem tentar reconectar, e
    LastDisconnectReason / LastDisconnectReplyCode já valem dentro dele.
  - EAMQPConnection / EAMQPChannel trazem ReplyCode / ReplyText quando o erro
    vem de um Connection.Close / Channel.Close do broker.
  - Um canal fechado pelo broker (406 num declare) não escreve mais: o publish
    levanta EAMQPChannel e Ack/Nack não fazem nada — antes os frames iam para o
    canal fechado e o broker derrubava a conexão inteira (504 CHANNEL_ERROR).

  Espelho FPCUnit em tests\Integration\fpc\AMQP.DisconnectInfoIntegrationTests.pas:
  os corpos dos testes são idênticos; só as pontes Checa*, o uses e o
  registro mudam de lado. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  PascalCommon.Threading,
  AMQP.Wire,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Queue.Methods;

type
  TAMQPDisconnectInfoIntegrationTests = class(TTestCase)
  private
    FConn: TAMQPConnection;
    FDisconnects: Integer;     // atômico
    FReasonInEvent: string;    // escritos antes de FDisconnects subir
    FCodeInEvent: Integer;
    procedure HandleDisconnect(AConnection: TAMQPConnection);
    function Espera(var ACounter: Integer; AValue, ATimeoutMs: Integer): Boolean;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure SemAutoReconnect_Queda_DisparaOnDisconnectUmaVez_ComOMotivo;
    procedure SenhaErrada_RecusaDoBroker_TrazReplyCode;
    procedure CanalFechadoPeloBroker_TrazReplyCode_E_NaoEscreveMais;
  end;

implementation

const
  ESPERA_MS = 10000;

// As pontes para as asserções do dialeto: só elas mudam entre os espelhos.
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaStr(const AMsg: string; const AEsperado, AObtido: string);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;

procedure TAMQPDisconnectInfoIntegrationTests.SetUp;
begin
  FConn := TAMQPConnection.Create(IntegrationParams);
  FConn.Open;
  FDisconnects := 0;
  FReasonInEvent := '';
  FCodeInEvent := -1;
end;

procedure TAMQPDisconnectInfoIntegrationTests.TearDown;
begin
  FConn.Free;
end;

procedure TAMQPDisconnectInfoIntegrationTests.HandleDisconnect(AConnection: TAMQPConnection);
begin
  FReasonInEvent := AConnection.LastDisconnectReason;
  FCodeInEvent := AConnection.LastDisconnectReplyCode;
  PcAtomicInc(FDisconnects);
end;

function TAMQPDisconnectInfoIntegrationTests.Espera(var ACounter: Integer;
  AValue, ATimeoutMs: Integer): Boolean;
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (PcAtomicGet(ACounter) < AValue) and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
  Result := PcAtomicGet(ACounter) >= AValue;
end;

procedure TAMQPDisconnectInfoIntegrationTests.SemAutoReconnect_Queda_DisparaOnDisconnectUmaVez_ComOMotivo;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
begin
  LParams := IntegrationParams;
  LParams.AutoReconnect := False;
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.OnDisconnect := HandleDisconnect;
    LConn.Open;
    LConn.DropConnectionForTest;
    ChecaOk('OnDisconnect deveria disparar sem AutoReconnect', Espera(FDisconnects, 1, ESPERA_MS));
    TThread.Sleep(1000); // tempo para uma reconexão indevida aparecer
    ChecaInt('uma única vez', 1, PcAtomicGet(FDisconnects));
    ChecaOk('sem AutoReconnect a conexão segue fechada', not LConn.IsOpen);
    ChecaOk('o motivo já valia dentro do OnDisconnect', FReasonInEvent <> '');
    ChecaInt('queda do lado do cliente: sem ReplyCode do broker', 0, FCodeInEvent);
    ChecaStr('LastDisconnectReason igual ao visto no evento', FReasonInEvent, LConn.LastDisconnectReason);
  finally
    LConn.Free; // Close/Destroy unem a thread da queda
  end;
end;

procedure TAMQPDisconnectInfoIntegrationTests.SenhaErrada_RecusaDoBroker_TrazReplyCode;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LRecusa: Boolean;
  LCode: Integer;
begin
  LParams := IntegrationParams;
  LParams.Password := 'senha-obviamente-errada';
  LRecusa := False;
  LCode := 0;
  LConn := TAMQPConnection.Create(LParams);
  try
    try
      LConn.Open;
    except
      // Broker antigo pode só derrubar o socket: aí não há código a conferir.
      on E: EAMQPConnection do
      begin
        LRecusa := Pos('refused by server', E.Message) > 0;
        LCode := E.ReplyCode;
      end;
      on Exception do ;
    end;
    if LRecusa then
      ChecaInt('recusa por Connection.Close: ReplyCode 403 (ACCESS_REFUSED)', 403, LCode);
  finally
    LConn.Free;
  end;
end;

procedure TAMQPDisconnectInfoIntegrationTests.CanalFechadoPeloBroker_TrazReplyCode_E_NaoEscreveMais;
var
  LChan, LOutro: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LCode: Integer;
begin
  LChan := FConn.CreateChannel;
  try
    // Mesma fila exclusiva declarada duas vezes com argumentos diferentes: 406.
    LDecl := Default(TAMQPQueueDeclare);
    LDecl.QueueName := 'test-reply-code-' + IntToStr(PcTickMs);
    LDecl.Exclusive := True;
    LDecl.Arguments := TAMQPFieldTable.Create;
    try
      LDecl.Arguments.Put('x-dead-letter-exchange', 'amq.direct');
      LChan.DeclareQueue(LDecl);
      LDecl.Arguments.Put('x-dead-letter-exchange', 'amq.fanout');
      LCode := 0;
      try
        LChan.DeclareQueue(LDecl);
      except
        on E: EAMQPChannel do
          LCode := E.ReplyCode;
      end;
    finally
      LDecl.Arguments.Free;
    end;
    ChecaInt('declare inequivalente: EAMQPChannel com ReplyCode 406', 406, LCode);
    ChecaOk('o canal deveria estar fechado', not LChan.IsOpen);

    LCode := 0;
    try
      LChan.PublishText('', LDecl.QueueName, 'x');
    except
      on E: EAMQPChannel do
        LCode := E.ReplyCode;
    end;
    ChecaInt('publish no canal fechado pelo broker: EAMQPChannel com o 406 do fechamento', 406, LCode);

    // Nem levantam nem escrevem: um frame no canal fechado derrubaria a conexão.
    LChan.Ack(1);
    LChan.Nack(2);
    TThread.Sleep(500);
    ChecaOk('a conexão deveria seguir aberta', FConn.IsOpen);
    LOutro := FConn.CreateChannel;
    try
      LDecl := Default(TAMQPQueueDeclare);
      LDecl.Exclusive := True;
      ChecaOk('outro canal deveria seguir funcionando', LOutro.DeclareQueue(LDecl).QueueName <> '');
    finally
      LOutro.Free;
    end;
  finally
    LChan.Free;
  end;
end;

initialization
  RegisterTest(TAMQPDisconnectInfoIntegrationTests);

end.
