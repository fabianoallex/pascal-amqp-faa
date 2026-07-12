unit AMQP.TlsIntegrationTests;

{ Integração TLS (amqps://) — precisa de um RabbitMQ com listener TLS em
  localhost:5671, cert self-signed. Sobe com:
    docker compose -f docker/docker-compose.yml -f docker/docker-compose.tls.yml up -d
  (ver o cabeçalho de docker/docker-compose.tls.yml para gerar os certs).

  Se o broker TLS NÃO estiver no ar, o Setup não consegue conectar e os testes
  saem sem asserção (ignorados), para não quebrar a suíte quando só o broker
  plain (5672) está up. O mesmo caminho cobre builds sem backend TLS (fora do
  Windows sem -dAMQP_OPENSSL): o Open levanta EAMQPConnection e os testes se
  auto-ignoram. Para rodá-los de verdade fora do Windows, compile o runner com
  -dAMQP_OPENSSL (precisa de libssl/libcrypto instaladas). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  AMQP.Connection,
  AMQP.Transport, // EAMQPTls (vale pra SChannel e OpenSSL)
  AMQP.Queue.Methods;

type
  TAMQPTlsIntegrationTests = class(TTestCase)
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FAvailable: Boolean; // broker TLS acessível? (senão, testes são ignorados)
    FVerifyPeerParams: TAMQPConnectionParams;
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
    procedure DoOpenWithVerifyPeer;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure Tls_PublishEBusca;
    procedure Tls_VerifyPeerTrue_RejeitaCertSelfSigned;
  end;

implementation

procedure TAMQPTlsIntegrationTests.SetUp;
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
  AssertTrue('mensagem deveria ter sido entregue sobre TLS', LResult.Found);
  AssertEquals('olá sobre TLS', LResult.BodyAsText);
end;

procedure TAMQPTlsIntegrationTests.DoOpenWithVerifyPeer;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(FVerifyPeerParams);
  try
    LConn.Open; // deve levantar EAMQPTls (cert não confiável)
  finally
    LConn.Free;
  end;
end;

procedure TAMQPTlsIntegrationTests.Tls_VerifyPeerTrue_RejeitaCertSelfSigned;
begin
  if not FAvailable then
    Exit; // sem broker TLS não dá para provar a validação: teste ignorado

  // Mesmo broker (cert self-signed), mas AGORA exigindo validação da cadeia +
  // hostname: o handshake TLS deve FALHAR (prova que TlsVerifyPeer=True valida).
  FVerifyPeerParams := TAMQPConnectionParams.LocalhostTls;
  FVerifyPeerParams.TlsVerifyPeer := True;

  AssertException('validar o cert self-signed deveria recusar a conexão',
    EAMQPTls, DoOpenWithVerifyPeer);
end;

initialization
  RegisterTest(TAMQPTlsIntegrationTests);

end.
