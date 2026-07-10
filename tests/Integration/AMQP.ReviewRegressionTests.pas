unit AMQP.ReviewRegressionTests;

{ Regressões dos achados da revisão que dão para testar de forma determinística:
  - bug_006: Close/Free do canal espera (drena) callbacks em voo (sem UAF).
  - bug_002: CreateChannel concorrente gera IDs distintos, sem erro nem leak.
  Precisa de RabbitMQ em localhost:5672. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Threading,
  System.Generics.Collections,
  AMQP.Connection,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPReviewRegressionTests = class
  private
    FConn: TAMQPConnection;
    FStarted: Integer;
    FFinished: Integer;
    // Callback de consumer é 'of object' na lib (ver CLAUDE.md) — sem métodos
    // anônimos.
    procedure HandleSlowDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure CloseDoCanal_EsperaCallbackEmVoo;   // bug_006
    [Test] procedure CreateChannelConcorrente_IdsDistintos; // bug_002
  end;

implementation

procedure TAMQPReviewRegressionTests.Setup;
begin
  FConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  FConn.Open;
  FStarted := 0;
  FFinished := 0;
end;

procedure TAMQPReviewRegressionTests.TearDown;
begin
  FConn.Free;
end;

procedure TAMQPReviewRegressionTests.HandleSlowDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  TInterlocked.Exchange(FStarted, 1);
  TThread.Sleep(6000);
  TInterlocked.Exchange(FFinished, 1);
end;

procedure TAMQPReviewRegressionTests.CloseDoCanal_EsperaCallbackEmVoo;
var
  LChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LQueue: string;
  LWaited: Integer;
begin
  LChan := FConn.CreateChannel;
  try
    LDecl := Default(TAMQPQueueDeclare);
    LDecl.Exclusive := True;
    LDecl.AutoDelete := True;
    LQueue := LChan.DeclareQueue(LDecl).QueueName;

    // Consumer (no-ack, para não ackar num canal em fechamento) cujo callback
    // demora ~6 s — DE PROPÓSITO mais que o antigo timeout de drain (5 s): o
    // código com bug retornaria do Close com o callback ainda em voo
    // (FFinished=0); o corrigido espera até o fim (FFinished=1).
    LChan.Consume(LQueue, HandleSlowDelivery, True {no-ack});

    LChan.PublishText('', LQueue, 'processa-devagar');

    // Espera o callback começar (mas ainda dormindo).
    LWaited := 0;
    while (TInterlocked.CompareExchange(FStarted, 0, 0) = 0) and (LWaited < 5000) do
    begin
      TThread.Sleep(20);
      Inc(LWaited, 20);
    end;
    Assert.AreEqual(1, TInterlocked.CompareExchange(FStarted, 0, 0),
      'o callback deveria ter começado');
    Assert.AreEqual(0, TInterlocked.CompareExchange(FFinished, 0, 0),
      'o callback ainda deveria estar em voo (dormindo)');

    // Fecha o canal COM o callback em voo: DrainInFlight deve esperar terminar.
    LChan.Close;

    Assert.AreEqual(1, TInterlocked.CompareExchange(FFinished, 0, 0),
      'Close deveria ter drenado (esperado) o callback em voo antes de retornar');
  finally
    LChan.Free;
  end;
end;

procedure TAMQPReviewRegressionTests.CreateChannelConcorrente_IdsDistintos;
const
  N = 8;
var
  LChannels: array[0 .. N - 1] of TAMQPChannel;
  LErrors: array[0 .. N - 1] of string;
  LSeen: TDictionary<Word, Boolean>;
  I: Integer;
  LId: Word;
begin
  for I := 0 to N - 1 do
  begin
    LChannels[I] := nil;
    LErrors[I] := '';
  end;

  // Cria N canais concorrentemente. TParallel.For passa o índice por parâmetro
  // (sem o problema de captura de variável de laço). O TProc<Integer> aqui é do
  // próprio System.Threading (Delphi), não da lib — não entra na regra do
  // CLAUDE.md sobre callbacks 'of object'.
  TParallel.For(0, N - 1,
    procedure(AIndex: Integer)
    begin
      try
        LChannels[AIndex] := FConn.CreateChannel;
      except
        on E: Exception do
          LErrors[AIndex] := E.Message;
      end;
    end);

  LSeen := TDictionary<Word, Boolean>.Create;
  try
    for I := 0 to N - 1 do
    begin
      Assert.AreEqual('', LErrors[I],
        Format('thread %d falhou ao criar canal: %s', [I, LErrors[I]]));
      Assert.IsNotNull(LChannels[I], Format('canal %d não foi criado', [I]));
      LId := LChannels[I].ChannelId;
      Assert.IsFalse(LSeen.ContainsKey(LId),
        Format('channel-id duplicado: %d', [LId]));
      LSeen.Add(LId, True);
    end;
    Assert.AreEqual(N, LSeen.Count, 'deveria haver N canais com IDs distintos');
  finally
    LSeen.Free;
    for I := 0 to N - 1 do
      LChannels[I].Free; // Free em nil é seguro
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPReviewRegressionTests);

end.
