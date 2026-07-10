unit AMQP.HandshakeIntegrationTests;

{ Testes de integração do handshake — precisam de um RabbitMQ real em
  localhost:5672 (guest/guest). Suba com: docker compose -f docker/docker-compose.yml up -d
  Sem broker, os testes que abrem conexão vão ERRAR (não é falha da lib). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  AMQP.Connection,
  AMQP.Connection.Methods,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPHandshakeIntegrationTests = class
  public
    [Test] procedure Conecta_FazHandshake_E_Fecha;
    [Test] procedure TuneNegociado_TemLimitesRazoaveis;
    [Test] procedure CredenciaisInvalidas_Levanta;
    [Test] procedure VirtualHostInexistente_Levanta;
    [Test] procedure Heartbeat_MantemConexaoViva;
  end;

implementation

{ TAMQPHandshakeIntegrationTests }

procedure TAMQPHandshakeIntegrationTests.Conecta_FazHandshake_E_Fecha;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  try
    LConn.Open;
    Assert.IsTrue(LConn.IsOpen, 'deveria estar aberta após o handshake');
    LConn.Close;
    Assert.IsFalse(LConn.IsOpen, 'deveria estar fechada após Close');
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.TuneNegociado_TemLimitesRazoaveis;
var
  LConn: TAMQPConnection;
  LTune: TAMQPConnectionTune;
begin
  LConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  try
    LConn.Open;
    LTune := LConn.NegotiatedTune;
    // channel-max negociado nunca deve exceder o proposto pelo cliente (2047)
    // nem ser 0 (o RabbitMQ 3.13 propõe 2047 por padrão).
    Assert.IsTrue(LTune.ChannelMax > 0, 'channel-max deveria ser > 0');
    Assert.IsTrue(LTune.ChannelMax <= 2047, 'channel-max não deveria exceder 2047');
    // frame-max deve respeitar o mínimo do protocolo (4096).
    Assert.IsTrue(LTune.FrameMax >= 4096, 'frame-max deveria ser >= 4096');
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.CredenciaisInvalidas_Levanta;
var
  LParams: TAMQPConnectionParams;
begin
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Password := 'senha-obviamente-errada';
  // Com a capability authentication_failure_close, o servidor responde
  // Connection.Close (403) — mas aceitamos qualquer exceção (broker pode só
  // derrubar o socket em versões antigas).
  Assert.WillRaise(
    procedure
    var
      LConn: TAMQPConnection;
    begin
      LConn := TAMQPConnection.Create(LParams);
      try
        LConn.Open;
      finally
        LConn.Free;
      end;
    end);
end;

procedure TAMQPHandshakeIntegrationTests.VirtualHostInexistente_Levanta;
var
  LParams: TAMQPConnectionParams;
begin
  LParams := TAMQPConnectionParams.Localhost;
  LParams.VirtualHost := '/vhost-que-nao-existe';
  Assert.WillRaise(
    procedure
    var
      LConn: TAMQPConnection;
    begin
      LConn := TAMQPConnection.Create(LParams);
      try
        LConn.Open;
      finally
        LConn.Free;
      end;
    end,
    EAMQPConnection);
end;

procedure TAMQPHandshakeIntegrationTests.Heartbeat_MantemConexaoViva;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
begin
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Heartbeat := 2; // negocia 2s; cliente manda heartbeat a cada 1s
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.Open;
    Assert.IsTrue(LConn.NegotiatedTune.Heartbeat > 0, 'heartbeat deveria ser > 0');
    Assert.IsTrue(LConn.NegotiatedTune.Heartbeat <= 2, 'heartbeat negociado <= 2s');

    // Fica ocioso por mais que 2x o intervalo. Sem heartbeat, o broker
    // derrubaria a conexão; com heartbeat, ela continua viva.
    TThread.Sleep(5000);

    Assert.IsTrue(LConn.IsOpen, 'conexão deveria continuar aberta após ociosidade');

    // E continua utilizável:
    LChan := LConn.CreateChannel;
    try
      LDecl := Default(TAMQPQueueDeclare);
      LDecl.Exclusive := True;
      LDecl.AutoDelete := True;
      Assert.IsTrue(LChan.DeclareQueue(LDecl).QueueName <> '',
        'canal deveria funcionar após o período ocioso');
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
