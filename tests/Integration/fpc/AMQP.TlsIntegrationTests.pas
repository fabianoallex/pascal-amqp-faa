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
  AMQP.Threading, // AmqpAtomic* (TInterlocked não existe no FPC)
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
    FPlainPortParams: TAMQPConnectionParams;
    FCount: Integer;      // mensagens processadas (atômico)
    FCurrent: Integer;    // callbacks rodando agora (atômico)
    FPeak: Integer;       // pico de concorrência observado (atômico)
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
    function BuildBigBody(ASize: Integer): string;
    procedure WaitCount(AExpected, ATimeoutMs: Integer);
    procedure HandleDeliveryConcurrent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure DoOpenWithVerifyPeer;
    procedure DoOpenAgainstPlainPort;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure Tls_PublishEBusca;
    procedure Tls_PayloadGrande_Integro;
    procedure Tls_ConsumoConcorrente_ComAck;
    procedure Tls_ContraPortaPlain_LevantaEAMQPTls;
    procedure Tls_VerifyPeerTrue_RejeitaCertSelfSigned;
  end;

implementation

procedure TAMQPTlsIntegrationTests.SetUp;
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
  while (AmqpAtomicGet(FCount) < AExpected) and (LWaited < ATimeoutMs) do
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
  LCur := AmqpAtomicInc(FCurrent);
  // atualiza o pico de concorrência (CAS)
  repeat
    LOldPeak := FPeak;
    if LCur <= LOldPeak then
      Break;
  until AmqpAtomicCompareExchange(FPeak, LCur, LOldPeak) = LOldPeak;

  // Segura o callback até OBSERVAR outro rodando junto (ou timeout): prova a
  // sobreposição sem depender de janela de timing (um sleep fixo flakeia
  // quando os workers do pool demoram a subir sob carga). Se o pico >= 2 já
  // foi registrado, a prova está feita e ninguém mais precisa esperar.
  LWaited := 0;
  while (AmqpAtomicGet(FCurrent) < 2) and (AmqpAtomicGet(FPeak) < 2) and
        (LWaited < 2000) do
  begin
    TThread.Sleep(10);
    Inc(LWaited, 10);
  end;

  AChannel.Ack(ADelivery.DeliveryTag);
  AmqpAtomicDec(FCurrent);
  AmqpAtomicInc(FCount);
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
  AssertTrue('mensagem grande deveria ter sido entregue sobre TLS', LResult.Found);
  AssertEquals('tamanho do corpo', BODY_SIZE, Length(LResult.BodyAsText));
  // Igualdade direta (sem AssertEquals) pra não imprimir 300KB no relatório em falha.
  AssertTrue('corpo deveria voltar íntegro, byte a byte', LResult.BodyAsText = LBody);
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

  AssertEquals('todas as mensagens deveriam ter sido processadas sobre TLS',
    N, AmqpAtomicGet(FCount));
  AssertTrue('processamento deveria ser concorrente (pico > 1), não serializado',
    AmqpAtomicGet(FPeak) > 1);
end;

procedure TAMQPTlsIntegrationTests.DoOpenAgainstPlainPort;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(FPlainPortParams);
  try
    LConn.Open; // deve levantar EAMQPTls (handshake contra porta sem TLS)
  finally
    LConn.Free;
  end;
end;

procedure TAMQPTlsIntegrationTests.Tls_ContraPortaPlain_LevantaEAMQPTls;
begin
  if not FAvailable then
    Exit; // garante que HÁ broker no ar (senão a falha seria de socket, não de TLS)

  // Handshake TLS contra a porta plain (5672): o broker responde com o header
  // AMQP e fecha — deve virar EAMQPTls rápido, sem travar a suíte.
  FPlainPortParams := TAMQPConnectionParams.LocalhostTls;
  FPlainPortParams.Port := 5672;

  AssertException('handshake TLS contra a porta plain deveria falhar com EAMQPTls',
    EAMQPTls, DoOpenAgainstPlainPort);
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
