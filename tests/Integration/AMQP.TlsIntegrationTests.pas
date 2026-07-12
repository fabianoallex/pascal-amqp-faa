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
  System.SyncObjs,
  AMQP.Connection,
  AMQP.Transport, // EAMQPTls
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPTlsIntegrationTests = class
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FAvailable: Boolean; // broker TLS acessível? (senão, testes são ignorados)
    FCount: Integer;      // mensagens processadas (atômico)
    FCurrent: Integer;    // callbacks rodando agora (atômico)
    FPeak: Integer;       // pico de concorrência observado (atômico)
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
    function BuildBigBody(ASize: Integer): string;
    procedure WaitCount(AExpected, ATimeoutMs: Integer);
    procedure HandleDeliveryConcurrent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure Tls_PublishEBusca;
    [Test] procedure Tls_PayloadGrande_Integro;
    [Test] procedure Tls_ConsumoConcorrente_ComAck;
    [Test] procedure Tls_ContraPortaPlain_LevantaEAMQPTls;
    [Test] procedure Tls_VerifyPeerTrue_RejeitaCertSelfSigned;
  end;

implementation

procedure TAMQPTlsIntegrationTests.Setup;
begin
  FAvailable := False;
  FCount := 0;
  FCurrent := 0;
  FPeak := 0;
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

// Corpo determinístico só-ASCII (byte a byte igual à string): corrupção ou
// reordenação de qualquer trecho quebra a comparação de igualdade.
function TAMQPTlsIntegrationTests.BuildBigBody(ASize: Integer): string;
var
  I: Integer;
begin
  SetLength(Result, ASize);
  for I := 1 to ASize do
    Result[I] := Chr(Ord('a') + ((I * 31 + I div 97) mod 26));
end;

procedure TAMQPTlsIntegrationTests.WaitCount(AExpected, ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (TInterlocked.CompareExchange(FCount, 0, 0) < AExpected) and
        (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
end;

procedure TAMQPTlsIntegrationTests.HandleDeliveryConcurrent(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LCur, LOldPeak, LWaited: Integer;
begin
  LCur := TInterlocked.Increment(FCurrent);
  // atualiza o pico de concorrência (CAS)
  repeat
    LOldPeak := FPeak;
    if LCur <= LOldPeak then
      Break;
  until TInterlocked.CompareExchange(FPeak, LCur, LOldPeak) = LOldPeak;

  // Segura o callback até OBSERVAR outro rodando junto (ou timeout): prova a
  // sobreposição sem depender de janela de timing (um sleep fixo flakeia
  // quando os workers do pool demoram a subir sob carga). Se o pico >= 2 já
  // foi registrado, a prova está feita e ninguém mais precisa esperar.
  LWaited := 0;
  while (TInterlocked.CompareExchange(FCurrent, 0, 0) < 2) and
        (TInterlocked.CompareExchange(FPeak, 0, 0) < 2) and (LWaited < 2000) do
  begin
    TThread.Sleep(10);
    Inc(LWaited, 10);
  end;

  AChannel.Ack(ADelivery.DeliveryTag);
  TInterlocked.Decrement(FCurrent);
  TInterlocked.Increment(FCount);
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

procedure TAMQPTlsIntegrationTests.Tls_PayloadGrande_Integro;
const
  // Bem maior que um registro TLS (16KB) e que o frame_max negociado
  // (128KB): exercita o chunking da escrita TLS, a remontagem na leitura
  // através de vários registros e a fragmentação do corpo em frames AMQP.
  BODY_SIZE = 300000;
var
  LQueue, LBody: string;
  LResult: TAMQPGetResult;
begin
  if not FAvailable then
    Exit; // broker TLS indisponível: teste ignorado

  LQueue := DeclareTempQueue;
  LBody := BuildBigBody(BODY_SIZE);
  FChan.PublishText('', LQueue, LBody);

  LResult := GetWithRetry(LQueue);
  Assert.IsTrue(LResult.Found, 'mensagem grande deveria ter sido entregue sobre TLS');
  Assert.AreEqual(BODY_SIZE, Length(LResult.BodyAsText), 'tamanho do corpo');
  // Igualdade direta (sem AreEqual) pra não imprimir 300KB no relatório em falha.
  Assert.IsTrue(LResult.BodyAsText = LBody, 'corpo deveria voltar íntegro, byte a byte');
end;

procedure TAMQPTlsIntegrationTests.Tls_ConsumoConcorrente_ComAck;
const
  N = 8;
var
  LQueue: string;
  I: Integer;
begin
  if not FAvailable then
    Exit; // broker TLS indisponível: teste ignorado

  // Publishers + consumer na MESMA conexão TLS: estressa o leitor decifrando
  // enquanto os escritores cifram (FLock/FSendLock do stream TLS).
  LQueue := DeclareTempQueue;
  for I := 1 to N do
    FChan.PublishText('', LQueue, Format('tls-msg-%d', [I]));

  // prefetch alto: deixa o servidor entregar todas, permitindo concorrência.
  FChan.Qos(N);
  FChan.Consume(LQueue, HandleDeliveryConcurrent);

  // Timeout > N × timeout do callback: execução serializada de verdade chega
  // ao fim e falha na asserção de pico (a mensagem certa), não na contagem.
  WaitCount(N, 30000);

  Assert.AreEqual(N, TInterlocked.CompareExchange(FCount, 0, 0),
    'todas as mensagens deveriam ter sido processadas sobre TLS');
  Assert.IsTrue(TInterlocked.CompareExchange(FPeak, 0, 0) > 1,
    'processamento deveria ser concorrente (pico > 1), não serializado');
end;

procedure TAMQPTlsIntegrationTests.Tls_ContraPortaPlain_LevantaEAMQPTls;
var
  LParams: TAMQPConnectionParams;
begin
  if not FAvailable then
    Exit; // garante que HÁ broker no ar (senão a falha seria de socket, não de TLS)

  // Handshake TLS contra a porta plain (5672): o broker responde com o header
  // AMQP e fecha — deve virar EAMQPTls rápido, sem travar a suíte.
  LParams := TAMQPConnectionParams.LocalhostTls;
  LParams.Port := 5672;

  Assert.WillRaise(
    procedure
    var
      LConn: TAMQPConnection;
    begin
      LConn := TAMQPConnection.Create(LParams);
      try
        LConn.Open; // deve levantar EAMQPTls (handshake contra porta sem TLS)
      finally
        LConn.Free;
      end;
    end,
    EAMQPTls,
    'handshake TLS contra a porta plain deveria falhar com EAMQPTls');
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
