unit AMQP.ConsumeIntegrationTests;

{ Concurrent consumption integration — needs RabbitMQ on localhost:5672.
  Validates the MVP acceptance criterion: Basic.Consume dispatching to a thread
  pool, manual ack and NON-serialized processing of different messages. }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Classes, Generics.Collections,
  PascalCommon.Threading,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPConsumeIntegrationTests = class
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FReceived: TThreadList<string>;
    FCount: Integer;      // messages processed (atomic)
    FCurrent: Integer;    // callbacks running right now (atomic)
    FPeak: Integer;       // observed concurrency peak (atomic)
    function DeclareTempQueue: string;
    procedure WaitCount(AExpected, ATimeoutMs: Integer);
    // Consumer callbacks are 'of object' in the library (see CLAUDE.md) — no
    // anonymous methods; TInterlocked does not exist in FPC -> PcAtomic*.
    procedure HandleDeliverySimple(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure HandleDeliveryConcurrent(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure HandleDeliveryDedicated(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure ConsomeUmaMensagem_CorpoCorreto;
    [Test] procedure ConsomeTodas_ComAck_E_Concorrencia;
    [Test] procedure CanalDedicado_ProcessaSequencialEEmOrdem;
  end;

implementation

procedure TAMQPConsumeIntegrationTests.SetUp;
begin
  FConn := TAMQPConnection.Create(IntegrationParams);
  FConn.Open;
  FChan := FConn.CreateChannel;
  FReceived := TThreadList<string>.Create;
  FCount := 0;
  FCurrent := 0;
  FPeak := 0;
end;

procedure TAMQPConsumeIntegrationTests.TearDown;
begin
  FChan.Free;  // closes the channel and drains in-flight callbacks
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
  while (PcAtomicGet(FCount) < AExpected) and (LWaited < ATimeoutMs) do
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
  PcAtomicInc(FCount);
end;

procedure TAMQPConsumeIntegrationTests.HandleDeliveryConcurrent(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LCur, LOldPeak, LWaited: Integer;
begin
  LCur := PcAtomicInc(FCurrent);
  // updates the concurrency peak (CAS)
  repeat
    LOldPeak := FPeak;
    if LCur <= LOldPeak then
      Break;
  until PcAtomicCompareExchange(FPeak, LCur, LOldPeak) = LOldPeak;

  // Holds the callback until it OBSERVES another one running alongside (or a
  // timeout): proves the overlap without depending on a timing window (a fixed
  // sleep flakes when the pool workers are slow to start under load). If a
  // peak >= 2 has already been recorded, the proof is done and nobody else
  // needs to wait.
  LWaited := 0;
  while (PcAtomicGet(FCurrent) < 2) and (PcAtomicGet(FPeak) < 2) and
        (LWaited < 2000) do
  begin
    TThread.Sleep(10);
    Inc(LWaited, 10);
  end;

  FReceived.Add(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
  PcAtomicDec(FCurrent);
  PcAtomicInc(FCount);
end;

procedure TAMQPConsumeIntegrationTests.HandleDeliveryDedicated(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LCur, LOldPeak: Integer;
begin
  LCur := PcAtomicInc(FCurrent);
  // same peak update as the concurrency test; here we expect it never to go
  // above 1 (a dedicated worker is sequential).
  repeat
    LOldPeak := FPeak;
    if LCur <= LOldPeak then
      Break;
  until PcAtomicCompareExchange(FPeak, LCur, LOldPeak) = LOldPeak;

  // Gives plenty of time for another worker of the global pool to overlap, if
  // dispatch were going there instead of to the channel's dedicated worker.
  TThread.Sleep(30);

  FReceived.Add(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
  PcAtomicDec(FCurrent);
  PcAtomicInc(FCount);
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
    TAssert.AssertEquals('deveria ter recebido 1 mensagem', 1, LList.Count);
    TAssert.AssertEquals('resposta-nfe-123', LList[0]);
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

  // high prefetch: lets the server deliver all of them, allowing concurrency.
  FChan.Qos(N);
  FChan.Consume(LQueue, HandleDeliveryConcurrent);

  // Timeout > N × the callback's timeout: truly serialized execution reaches
  // the end and fails on the peak assertion (the right message), not on the count.
  WaitCount(N, 30000);

  LList := FReceived.LockList;
  try
    TAssert.AssertEquals('todas as mensagens deveriam ter sido processadas', N, LList.Count);
  finally
    FReceived.UnlockList;
  end;
  TAssert.AssertTrue('processamento deveria ser concorrente (pico > 1), não serializado',
    PcAtomicGet(FPeak) > 1);
end;

procedure TAMQPConsumeIntegrationTests.CanalDedicado_ProcessaSequencialEEmOrdem;
const
  N = 8;
var
  LQueue: string;
  I: Integer;
  LList: TList<string>;
begin
  // Replaces the SetUp channel with one that has a dedicated worker (CreateChannel(True)).
  FChan.Free;
  FChan := FConn.CreateChannel(True);

  LQueue := DeclareTempQueue;
  for I := 1 to N do
    FChan.PublishText('', LQueue, Format('msg-%d', [I]));

  FChan.Qos(N);
  FChan.Consume(LQueue, HandleDeliveryDedicated);

  WaitCount(N, 30000);

  LList := FReceived.LockList;
  try
    TAssert.AssertEquals('todas as mensagens deveriam ter sido processadas', N, LList.Count);
    for I := 1 to N do
      TAssert.AssertEquals('worker dedicado deveria preservar a ordem de entrega',
        Format('msg-%d', [I]), LList[I - 1]);
  finally
    FReceived.UnlockList;
  end;
  TAssert.AssertEquals('worker dedicado nunca deveria rodar 2 callbacks ao mesmo tempo (pico deveria ser 1)',
    1, PcAtomicGet(FPeak));
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPConsumeIntegrationTests);

end.
