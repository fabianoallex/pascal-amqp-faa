unit AMQP.HandshakeIntegrationTests;

{ Handshake integration tests — they need a real RabbitMQ on
  localhost:5672 (guest/guest). Start it with: docker compose -f docker/docker-compose.yml up -d
  Without a broker, the tests that open a connection will ERROR (not a library failure). }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  AMQP.TestSupport,
  SysUtils, Classes,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Connection.Methods,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPHandshakeIntegrationTests = class
  private
    FParams: TAMQPConnectionParams;
    procedure DoOpenWithParams;
  public
    [Test] procedure Conecta_FazHandshake_E_Fecha;
    [Test] procedure TuneNegociado_TemLimitesRazoaveis;
    [Test] procedure CredenciaisInvalidas_Levanta;
    [Test] procedure VirtualHostInexistente_Levanta;
    [Test] procedure Heartbeat_MantemConexaoViva;
  end;

implementation

{ TAMQPHandshakeIntegrationTests }

procedure TAMQPHandshakeIntegrationTests.DoOpenWithParams;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(FParams);
  try
    LConn.Open;
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.Conecta_FazHandshake_E_Fecha;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(IntegrationParams);
  try
    LConn.Open;
    TAssert.AssertTrue('deveria estar aberta após o handshake', LConn.IsOpen);
    LConn.Close;
    TAssert.AssertFalse('deveria estar fechada após Close', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.TuneNegociado_TemLimitesRazoaveis;
var
  LConn: TAMQPConnection;
  LTune: TAMQPConnectionTune;
begin
  LConn := TAMQPConnection.Create(IntegrationParams);
  try
    LConn.Open;
    LTune := LConn.NegotiatedTune;
    // The negotiated channel-max must never exceed the client's proposal
    // (2047) nor be 0 (RabbitMQ 3.13 proposes 2047 by default).
    TAssert.AssertTrue('channel-max deveria ser > 0', LTune.ChannelMax > 0);
    TAssert.AssertTrue('channel-max não deveria exceder 2047', LTune.ChannelMax <= 2047);
    // frame-max must respect the protocol minimum (4096).
    TAssert.AssertTrue('frame-max deveria ser >= 4096', LTune.FrameMax >= 4096);
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.CredenciaisInvalidas_Levanta;
var
  LRaised: Boolean;
begin
  FParams := IntegrationParams;
  FParams.Password := 'senha-obviamente-errada';
  // With the authentication_failure_close capability, the server replies
  // Connection.Close (403) — but we accept any exception (in old versions the
  // broker may just drop the socket). ChecaLevanta requires the EXACT class,
  // so "any exception" needs a manual try/except (Fail() inside the try would
  // be caught by the except itself, since an assertion failure is also an
  // Exception).
  LRaised := False;
  try
    DoOpenWithParams;
  except
    on E: Exception do
      LRaised := True;
  end;
  TAssert.AssertTrue('deveria ter levantado uma exceção para credenciais inválidas', LRaised);
end;

procedure TAMQPHandshakeIntegrationTests.VirtualHostInexistente_Levanta;
begin
  FParams := IntegrationParams;
  FParams.VirtualHost := '/vhost-que-nao-existe';
  ChecaLevanta(EAMQPConnection, DoOpenWithParams);
end;

procedure TAMQPHandshakeIntegrationTests.Heartbeat_MantemConexaoViva;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
begin
  LParams := IntegrationParams;
  LParams.Heartbeat := 2; // negotiates 2s; the client sends a heartbeat every 1s
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.Open;
    TAssert.AssertTrue('heartbeat deveria ser > 0', LConn.NegotiatedTune.Heartbeat > 0);
    TAssert.AssertTrue('heartbeat negociado <= 2s', LConn.NegotiatedTune.Heartbeat <= 2);

    // Stays idle for more than 2x the interval. Without heartbeat, the broker
    // would drop the connection; with heartbeat, it stays alive.
    TThread.Sleep(5000);

    TAssert.AssertTrue('conexão deveria continuar aberta após ociosidade', LConn.IsOpen);

    // And it is still usable:
    LChan := LConn.CreateChannel;
    try
      LDecl := Default(TAMQPQueueDeclare);
      LDecl.Exclusive := True;
      LDecl.AutoDelete := True;
      TAssert.AssertTrue('canal deveria funcionar após o período ocioso',
        LChan.DeclareQueue(LDecl).QueueName <> '');
    finally
      LChan.Free;
    end;
  finally
    LConn.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPHandshakeIntegrationTests);

end.
