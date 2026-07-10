unit AMQP.HandshakeIntegrationTests;

{ Testes de integração do handshake — precisam de um RabbitMQ real em
  localhost:5672 (guest/guest). Suba com: docker compose -f docker/docker-compose.yml up -d
  Sem broker, os testes que abrem conexão vão ERRAR (não é falha da lib). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  AMQP.Connection,
  AMQP.Connection.Methods,
  AMQP.Queue.Methods;

type
  TAMQPHandshakeIntegrationTests = class(TTestCase)
  private
    FParams: TAMQPConnectionParams;
    procedure DoOpenWithParams;
  published
    procedure Conecta_FazHandshake_E_Fecha;
    procedure TuneNegociado_TemLimitesRazoaveis;
    procedure CredenciaisInvalidas_Levanta;
    procedure VirtualHostInexistente_Levanta;
    procedure Heartbeat_MantemConexaoViva;
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
  LConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  try
    LConn.Open;
    AssertTrue('deveria estar aberta após o handshake', LConn.IsOpen);
    LConn.Close;
    AssertFalse('deveria estar fechada após Close', LConn.IsOpen);
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
    AssertTrue('channel-max deveria ser > 0', LTune.ChannelMax > 0);
    AssertTrue('channel-max não deveria exceder 2047', LTune.ChannelMax <= 2047);
    // frame-max deve respeitar o mínimo do protocolo (4096).
    AssertTrue('frame-max deveria ser >= 4096', LTune.FrameMax >= 4096);
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.CredenciaisInvalidas_Levanta;
var
  LRaised: Boolean;
begin
  FParams := TAMQPConnectionParams.Localhost;
  FParams.Password := 'senha-obviamente-errada';
  // Com a capability authentication_failure_close, o servidor responde
  // Connection.Close (403) — mas aceitamos qualquer exceção (broker pode só
  // derrubar o socket em versões antigas). AssertException do FPCUnit exige
  // a classe EXATA (não aceita subclasse — diferente do Assert.WillRaise sem
  // classe do DUnitX), então "qualquer exceção" precisa de try/except manual
  // (Fail() dentro do try seria capturado pelo próprio except, já que
  // EAssertionFailedError também é Exception).
  LRaised := False;
  try
    DoOpenWithParams;
  except
    on E: Exception do
      LRaised := True;
  end;
  AssertTrue('deveria ter levantado uma exceção para credenciais inválidas', LRaised);
end;

procedure TAMQPHandshakeIntegrationTests.VirtualHostInexistente_Levanta;
begin
  FParams := TAMQPConnectionParams.Localhost;
  FParams.VirtualHost := '/vhost-que-nao-existe';
  AssertException(EAMQPConnection, DoOpenWithParams);
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
    AssertTrue('heartbeat deveria ser > 0', LConn.NegotiatedTune.Heartbeat > 0);
    AssertTrue('heartbeat negociado <= 2s', LConn.NegotiatedTune.Heartbeat <= 2);

    // Fica ocioso por mais que 2x o intervalo. Sem heartbeat, o broker
    // derrubaria a conexão; com heartbeat, ela continua viva.
    TThread.Sleep(5000);

    AssertTrue('conexão deveria continuar aberta após ociosidade', LConn.IsOpen);

    // E continua utilizável:
    LChan := LConn.CreateChannel;
    try
      LDecl := Default(TAMQPQueueDeclare);
      LDecl.Exclusive := True;
      LDecl.AutoDelete := True;
      AssertTrue('canal deveria funcionar após o período ocioso',
        LChan.DeclareQueue(LDecl).QueueName <> '');
    finally
      LChan.Free;
    end;
  finally
    LConn.Free;
  end;
end;

initialization
  RegisterTest(TAMQPHandshakeIntegrationTests);

end.
