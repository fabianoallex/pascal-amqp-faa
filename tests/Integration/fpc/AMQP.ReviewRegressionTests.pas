unit AMQP.ReviewRegressionTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Integration/AMQP.ReviewRegressionTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }

{ Regressions for the review findings that can be tested deterministically:
  - bug_006: the channel's Close/Free waits for (drains) in-flight callbacks (no UAF).
  - bug_002: concurrent CreateChannel produces distinct IDs, with no error or leak.
  Needs RabbitMQ on localhost:5672. }


interface

uses
  fpcunit, testregistry,
  SysUtils, Classes, Generics.Collections,
  PascalCommon.Threading,
  PascalCommon.ThreadPool,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Queue.Methods;

type
  // Named types (not "array of X" directly as a parameter, which would be an
  // open array — VALUE semantics, copies the elements): a named dynamic array
  // type passed as a parameter shares the same buffer with the caller.
  TAMQPChannelArr = array of TAMQPChannel;
  TAMQPStringArr = array of string;

  // System.Threading (TParallel.For) does not exist in FPC (see CLAUDE.md) —
  // fires N concurrent channel creations through PcPool (the library's own
  // thread pool) instead. The pool takes ownership of the item: it is not
  // freed manually after Queue.
  TCreateChannelWorkItem = class(TPcWorkItem)
  private
    FConn: TAMQPConnection;
    FIndex: Integer;
    FChannels: TAMQPChannelArr; // shared with the caller (same buffer)
    FErrors: TAMQPStringArr;
    FDone: PInteger;
  public
    constructor Create(AConn: TAMQPConnection; AIndex: Integer;
      AChannels: TAMQPChannelArr; AErrors: TAMQPStringArr; ADone: PInteger);
    procedure Execute; override;
  end;

  TAMQPReviewRegressionTests = class(TTestCase)
  private
    FConn: TAMQPConnection;
    FStarted: Integer;
    FFinished: Integer;
    // The consumer callback is 'of object' in the library (see CLAUDE.md) — no
    // anonymous methods. TInterlocked does not exist in FPC -> PcAtomic*.
    procedure HandleSlowDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure CloseDoCanal_EsperaCallbackEmVoo;   // bug_006
    procedure CreateChannelConcorrente_IdsDistintos; // bug_002
  end;

implementation

{ TCreateChannelWorkItem }

constructor TCreateChannelWorkItem.Create(AConn: TAMQPConnection; AIndex: Integer;
  AChannels: TAMQPChannelArr; AErrors: TAMQPStringArr; ADone: PInteger);
begin
  inherited Create;
  FConn := AConn;
  FIndex := AIndex;
  FChannels := AChannels; // copies only the reference (same buffer as the caller)
  FErrors := AErrors;
  FDone := ADone;
end;

procedure TCreateChannelWorkItem.Execute;
begin
  try
    FChannels[FIndex] := FConn.CreateChannel;
  except
    on E: Exception do
      FErrors[FIndex] := E.Message;
  end;
  PcAtomicInc(FDone^);
end;

{ TAMQPReviewRegressionTests }

procedure TAMQPReviewRegressionTests.SetUp;
begin
  FConn := TAMQPConnection.Create(IntegrationParams);
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
  PcAtomicSet(FStarted, 1);
  TThread.Sleep(6000);
  PcAtomicSet(FFinished, 1);
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

    // Consumer (no-ack, so as not to ack on a closing channel) whose callback
    // takes ~6 s — ON PURPOSE longer than the old drain timeout (5 s): the
    // buggy code would return from Close with the callback still in flight
    // (FFinished=0); the fixed one waits until the end (FFinished=1).
    LChan.Consume(LQueue, HandleSlowDelivery, True {no-ack});

    LChan.PublishText('', LQueue, 'processa-devagar');

    // Waits for the callback to start (but still sleeping).
    LWaited := 0;
    while (PcAtomicGet(FStarted) = 0) and (LWaited < 5000) do
    begin
      TThread.Sleep(20);
      Inc(LWaited, 20);
    end;
    TAssert.AssertEquals('o callback deveria ter começado', 1, PcAtomicGet(FStarted));
    TAssert.AssertEquals('o callback ainda deveria estar em voo (dormindo)',
      0, PcAtomicGet(FFinished));

    // Closes the channel WITH the callback in flight: DrainInFlight must wait for it to finish.
    LChan.Close;

    TAssert.AssertEquals('Close deveria ter drenado (esperado) o callback em voo antes de retornar',
      1, PcAtomicGet(FFinished));
  finally
    LChan.Free;
  end;
end;

procedure TAMQPReviewRegressionTests.CreateChannelConcorrente_IdsDistintos;
const
  N = 8;
var
  LChannels: TAMQPChannelArr;
  LErrors: TAMQPStringArr;
  LSeen: TDictionary<Word, Boolean>;
  I, LDone, LWaited: Integer;
  LId: Word;
begin
  SetLength(LChannels, N);
  SetLength(LErrors, N);
  for I := 0 to N - 1 do
  begin
    LChannels[I] := nil;
    LErrors[I] := '';
  end;

  // Creates N channels concurrently through PcPool (the library's own thread
  // pool, see the TCreateChannelWorkItem comment above).
  LDone := 0;
  for I := 0 to N - 1 do
    PcPool.Queue(TCreateChannelWorkItem.Create(FConn, I, LChannels, LErrors, @LDone));

  LWaited := 0;
  while (PcAtomicGet(LDone) < N) and (LWaited < 10000) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
  TAssert.AssertEquals('todos os N itens deveriam terminar', N, PcAtomicGet(LDone));

  LSeen := TDictionary<Word, Boolean>.Create;
  try
    for I := 0 to N - 1 do
    begin
      TAssert.AssertEquals(Format('thread %d falhou ao criar canal: %s', [I, LErrors[I]]),
        '', LErrors[I]);
      TAssert.AssertTrue(Format('canal %d não foi criado', [I]), Assigned(LChannels[I]));
      LId := LChannels[I].ChannelId;
      TAssert.AssertFalse(Format('channel-id duplicado: %d', [LId]), LSeen.ContainsKey(LId));
      LSeen.Add(LId, True);
    end;
    TAssert.AssertEquals('deveria haver N canais com IDs distintos', N, LSeen.Count);
  finally
    LSeen.Free;
    for I := 0 to N - 1 do
      LChannels[I].Free; // Free on nil is safe
  end;
end;

initialization
  RegisterTest(TAMQPReviewRegressionTests);

end.
