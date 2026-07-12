unit AMQP.ConsumeIntegrationTests;

{ Integração de consumo concorrente — precisa de RabbitMQ em localhost:5672.
  Valida o critério de aceite do MVP: Basic.Consume com despacho para thread
  pool, ack manual e processamento NÃO serializado de mensagens diferentes. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  AMQP.Connection,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPConsumeIntegrationTests = class
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FReceived: TThreadList<string>;
    FCount: Integer;      // mensagens processadas (atômico)
    FCurrent: Integer;    // callbacks rodando agora (atômico)
    FPeak: Integer;       // pico de concorrência observado (atômico)
    function DeclareTempQueue: string;
    procedure WaitCount(AExpected, ATimeoutMs: Integer);
    // Callbacks de consumer são 'of object' na lib (ver CLAUDE.md) — sem
    // métodos anônimos; o estado vai para os campos acima.
    procedure HandleDeliverySimple(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure HandleDeliveryConcurrent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure ConsomeUmaMensagem_CorpoCorreto;
    [Test] procedure ConsomeTodas_ComAck_E_Concorrencia;
  end;

implementation

procedure TAMQPConsumeIntegrationTests.Setup;
begin
  FConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  FConn.Open;
  FChan := FConn.CreateChannel;
  FReceived := TThreadList<string>.Create;
  FCount := 0;
  FCurrent := 0;
  FPeak := 0;
end;

procedure TAMQPConsumeIntegrationTests.TearDown;
begin
  FChan.Free;  // fecha o canal e drena callbacks em voo
  FConn.Free;
  FReceived.Free;
end;

function TAMQPConsumeIntegrationTests.DeclareTempQueue: string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  LDecl.AutoDelete := True;
  Result := FChan.DeclareQueue(LDecl).QueueName;
end;

procedure TAMQPConsumeIntegrationTests.WaitCount(AExpected, ATimeoutMs: Integer);
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

procedure TAMQPConsumeIntegrationTests.HandleDeliverySimple(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  FReceived.Add(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
  TInterlocked.Increment(FCount);
end;

procedure TAMQPConsumeIntegrationTests.HandleDeliveryConcurrent(AChannel: TAMQPChannel;
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

  FReceived.Add(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
  TInterlocked.Decrement(FCurrent);
  TInterlocked.Increment(FCount);
end;

procedure TAMQPConsumeIntegrationTests.ConsomeUmaMensagem_CorpoCorreto;
var
  LQueue: string;
  LList: TList<string>;
begin
  LQueue := DeclareTempQueue;
  FChan.PublishText('', LQueue, 'resposta-nfe-123');

  FChan.Consume(LQueue, HandleDeliverySimple);

  WaitCount(1, 5000);

  LList := FReceived.LockList;
  try
    Assert.AreEqual(1, LList.Count, 'deveria ter recebido 1 mensagem');
    Assert.AreEqual('resposta-nfe-123', LList[0]);
  finally
    FReceived.UnlockList;
  end;
end;

procedure TAMQPConsumeIntegrationTests.ConsomeTodas_ComAck_E_Concorrencia;
const
  N = 8;
var
  LQueue: string;
  I: Integer;
  LList: TList<string>;
begin
  LQueue := DeclareTempQueue;
  for I := 1 to N do
    FChan.PublishText('', LQueue, Format('msg-%d', [I]));

  // prefetch alto: deixa o servidor entregar todas, permitindo concorrência.
  FChan.Qos(N);
  FChan.Consume(LQueue, HandleDeliveryConcurrent);

  // Timeout > N × timeout do callback: execução serializada de verdade chega
  // ao fim e falha na asserção de pico (a mensagem certa), não na contagem.
  WaitCount(N, 30000);

  LList := FReceived.LockList;
  try
    Assert.AreEqual(N, LList.Count, 'todas as mensagens deveriam ter sido processadas');
  finally
    FReceived.UnlockList;
  end;
  Assert.IsTrue(TInterlocked.CompareExchange(FPeak, 0, 0) > 1,
    'processamento deveria ser concorrente (pico > 1), não serializado');
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPConsumeIntegrationTests);

end.
