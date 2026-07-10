unit AMQP.TlsIntegrationTests;

{ Integração TLS (amqps://) — precisa de um RabbitMQ com listener TLS em
  localhost:5671, cert self-signed. Sobe com:
    docker compose -f docker/docker-compose.yml -f docker/docker-compose.tls.yml up -d
  (ver o cabeçalho de docker/docker-compose.tls.yml para gerar os certs).

  Se o broker TLS NÃO estiver no ar, o Setup não consegue conectar e os testes
  são ignorados (saem sem asserção — o runner roda com FailsOnNoAsserts=False),
  para não quebrar a suíte quando só o broker plain (5672) está up. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  AMQP.Connection,
  AMQP.Transport.Tls,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPTlsIntegrationTests = class
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FAvailable: Boolean; // broker TLS acessível? (senão, testes são ignorados)
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure Tls_PublishEBusca;
    [Test] procedure Tls_VerifyPeerTrue_RejeitaCertSelfSigned;
  end;

implementation

procedure TAMQPTlsIntegrationTests.Setup;
begin
  FAvailable := False;
  // TlsVerifyPeer=False: aceita o cert self-signed do broker de dev.
  FConn := TAMQPConnection.Create(TAMQPConnectionParams.LocalhostTls);
  try
    FConn.Open;
    FChan := FConn.CreateChannel;
    FAvailable := True;
  except
    // Broker TLS indisponível (ou handshake falhou): ignora os testes.
    FreeAndNil(FConn);
  end;
end;

procedure TAMQPTlsIntegrationTests.TearDown;
begin
  FChan.Free;
  FConn.Free;
end;

function TAMQPTlsIntegrationTests.DeclareTempQueue: string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  LDecl.AutoDelete := True;
  Result := FChan.DeclareQueue(LDecl).QueueName;
end;

function TAMQPTlsIntegrationTests.GetWithRetry(const AQueue: string): TAMQPGetResult;
var
  I: Integer;
begin
  for I := 1 to 20 do
  begin
    Result := FChan.BasicGet(AQueue, True);
    if Result.Found then
      Exit;
    TThread.Sleep(25);
  end;
end;

procedure TAMQPTlsIntegrationTests.Tls_PublishEBusca;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  if not FAvailable then
    Exit; // broker TLS indisponível: teste ignorado

  LQueue := DeclareTempQueue;
  FChan.PublishText('', LQueue, 'olá sobre TLS');

  LResult := GetWithRetry(LQueue);
  Assert.IsTrue(LResult.Found, 'mensagem deveria ter sido entregue sobre TLS');
  Assert.AreEqual('olá sobre TLS', LResult.BodyAsText);
end;

procedure TAMQPTlsIntegrationTests.Tls_VerifyPeerTrue_RejeitaCertSelfSigned;
var
  LParams: TAMQPConnectionParams;
begin
  if not FAvailable then
    Exit; // sem broker TLS não dá para provar a validação: teste ignorado

  // Mesmo broker (cert self-signed), mas AGORA exigindo validação da cadeia +
  // hostname: o handshake TLS deve FALHAR (prova que TlsVerifyPeer=True valida).
  LParams := TAMQPConnectionParams.LocalhostTls;
  LParams.TlsVerifyPeer := True;

  Assert.WillRaise(
    procedure
    var
      LConn: TAMQPConnection;
    begin
      LConn := TAMQPConnection.Create(LParams);
      try
        LConn.Open; // deve levantar EAMQPTls (cert não confiável)
      finally
        LConn.Free;
      end;
    end,
    EAMQPTls,
    'validar o cert self-signed deveria recusar a conexão');
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPTlsIntegrationTests);

end.
