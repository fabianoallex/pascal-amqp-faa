unit AMQP.ReviewRegressionTests;

{ Regressões dos achados da revisão que dão para testar de forma determinística:
  - bug_006: Close/Free do canal espera (drena) callbacks em voo (sem UAF).
  - bug_002: CreateChannel concorrente gera IDs distintos, sem erro nem leak.
  Precisa de RabbitMQ em localhost:5672. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes, Generics.Collections,
  AMQP.Threading,
  AMQP.Connection,
  AMQP.Queue.Methods;

type
  // Tipos nomeados (nao "array of X" direto como parametro, que seria open
  // array — semantica de VALOR, copia os elementos): um dynamic array named
  // type passado por parametro compartilha o mesmo buffer com o chamador.
  TAMQPChannelArr = array of TAMQPChannel;
  TAMQPStringArr = array of string;

  // System.Threading (TParallel.For) nao existe no FPC (ver CLAUDE.md) —
  // dispara N criacoes de canal concorrentes via AmqpPool (o thread pool
  // proprio da lib) em vez disso. O pool assume a posse do item: nao se
  // libera manualmente apos Queue.
  TCreateChannelWorkItem = class(TAMQPWorkItem)
  private
    FConn: TAMQPConnection;
    FIndex: Integer;
    FChannels: TAMQPChannelArr; // compartilhado com o chamador (mesmo buffer)
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
    // Callback de consumer é 'of object' na lib (ver CLAUDE.md) — sem métodos
    // anônimos. TInterlocked não existe no FPC -> AmqpAtomic*.
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
  FChannels := AChannels; // copia so' a referencia (mesmo buffer do chamador)
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
  AmqpAtomicInc(FDone^);
end;

{ TAMQPReviewRegressionTests }

procedure TAMQPReviewRegressionTests.SetUp;
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
  AmqpAtomicSet(FStarted, 1);
  TThread.Sleep(6000);
  AmqpAtomicSet(FFinished, 1);
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
    while (AmqpAtomicGet(FStarted) = 0) and (LWaited < 5000) do
    begin
      TThread.Sleep(20);
      Inc(LWaited, 20);
    end;
    AssertEquals('o callback deveria ter começado', 1, AmqpAtomicGet(FStarted));
    AssertEquals('o callback ainda deveria estar em voo (dormindo)',
      0, AmqpAtomicGet(FFinished));

    // Fecha o canal COM o callback em voo: DrainInFlight deve esperar terminar.
    LChan.Close;

    AssertEquals('Close deveria ter drenado (esperado) o callback em voo antes de retornar',
      1, AmqpAtomicGet(FFinished));
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

  // Cria N canais concorrentemente via AmqpPool (thread pool proprio da lib,
  // ver comentario do TCreateChannelWorkItem acima).
  LDone := 0;
  for I := 0 to N - 1 do
    AmqpPool.Queue(TCreateChannelWorkItem.Create(FConn, I, LChannels, LErrors, @LDone));

  LWaited := 0;
  while (AmqpAtomicGet(LDone) < N) and (LWaited < 10000) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
  AssertEquals('todos os N itens deveriam terminar', N, AmqpAtomicGet(LDone));

  LSeen := TDictionary<Word, Boolean>.Create;
  try
    for I := 0 to N - 1 do
    begin
      AssertEquals(Format('thread %d falhou ao criar canal: %s', [I, LErrors[I]]),
        '', LErrors[I]);
      AssertNotNull(Format('canal %d não foi criado', [I]), LChannels[I]);
      LId := LChannels[I].ChannelId;
      AssertFalse(Format('channel-id duplicado: %d', [LId]), LSeen.ContainsKey(LId));
      LSeen.Add(LId, True);
    end;
    AssertEquals('deveria haver N canais com IDs distintos', N, LSeen.Count);
  finally
    LSeen.Free;
    for I := 0 to N - 1 do
      LChannels[I].Free; // Free em nil é seguro
  end;
end;

initialization
  RegisterTest(TAMQPReviewRegressionTests);

end.
