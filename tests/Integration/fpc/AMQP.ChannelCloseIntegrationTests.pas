unit AMQP.ChannelCloseIntegrationTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Integration/AMQP.ChannelCloseIntegrationTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }

{ Fechar um canal com callback de consumer em voo (v0.4.2) — precisa de um
  broker (RabbitMQ ou o embutido, pela aceitação).

  Até a v0.4.1, TAMQPChannel.Close mandava Channel.Close, esperava o Close-Ok
  e só então esperava os callbacks. Um callback que terminava nessa janela e
  dava Ack mandava Basic.Ack num canal que o broker já tinha fechado: pela
  spec é erro de CONEXÃO (504 CHANNEL_ERROR), e caíam todos os canais.
  O mesmo acontecia quando o BROKER fechava o canal (um 404 qualquer) com um
  callback em voo. Achado pelo pascal-agenda-faa.

  Agora: (1) Close cancela os consumers e espera os callbacks ANTES do
  Channel.Close, e o Ack deles vale; (2) Ack/Nack num canal fechado ou
  fechando não saem para o fio (o broker já devolveu a entrega à fila).

  Cada teste usa dois canais na mesma conexão: A consome, B confere depois
  que a conexão sobreviveu (uma RPC em B, depois do Ack atrasado, só volta se
  o broker não derrubou a conexão — ele lê os frames em ordem).

  Mestre DUnitX no dialeto do FPCUnit (TAssert.*); o espelho FPCUnit em
  fpc\ é gerado por tools/gen_fpc_mirror.py. }


interface

uses
  fpcunit, testregistry,
  SysUtils, Classes, SyncObjs,
  PascalCommon.Threading,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Queue.Methods;

type
  TAMQPChannelCloseIntegrationTests = class(TTestCase)
  private
    FConn: TAMQPConnection;
    FChanA: TAMQPChannel;
    FChanB: TAMQPChannel;
    FQueue: string;
    FStarted: TEvent;   // o primeiro callback começou
    FCalls: Integer;    // callbacks que começaram
    FDone: Integer;     // callbacks que terminaram (depois do Ack)
    FErrors: Integer;   // chamadas do OnConsumerError
    FSleepMs: Integer;
    procedure HandleDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure HandleError(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery;
      AError: Exception; var AAction: TAMQPConsumerErrorAction);
    procedure PreparaConsumo(AMensagens: Integer);
    /// Mensagens prontas na fila, pelo canal B (passive declare). Levanta se a
    /// conexão tiver caído.
    function ProntasNaFila: Cardinal;
    /// Espera a fila ter AEsperadas prontas. No RabbitMQ a devolução de uma
    /// entrega, quando o canal fecha, é assíncrona em relação ao canal B.
    function EsperaProntas(AEsperadas: Cardinal): Cardinal;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure Close_ComCallbackEmVoo_AckValeEConexaoSobrevive;
    procedure Close_ComVariasEntregasEmVoo_TodasConfirmadas;
    procedure Free_ComCallbackEmVoo_ConexaoSobrevive;
    procedure FechadoPeloBroker_AckAtrasadoNaoDerrubaAConexao;
    procedure AckDepoisDoClose_NaoDerrubaAConexao;
  end;

implementation

const
  ESPERA_MS = 10000;

procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;

procedure TAMQPChannelCloseIntegrationTests.SetUp;
begin
  FConn := TAMQPConnection.Create(IntegrationParams);
  FConn.Open;
  FChanA := FConn.CreateChannel;
  FChanB := FConn.CreateChannel;
  FStarted := TEvent.Create(nil, True, False, '');
  FCalls := 0;
  FDone := 0;
  FErrors := 0;
  FSleepMs := 300;
end;

procedure TAMQPChannelCloseIntegrationTests.TearDown;
begin
  FChanA.Free; // nil-safe (um teste o libera no meio)
  FChanB.Free;
  FConn.Free;
  FStarted.Free;
end;

procedure TAMQPChannelCloseIntegrationTests.HandleDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  PcAtomicInc(FCalls);
  FStarted.SetEvent;
  TThread.Sleep(FSleepMs);
  AChannel.Ack(ADelivery.DeliveryTag);
  PcAtomicInc(FDone);
end;

procedure TAMQPChannelCloseIntegrationTests.HandleError(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery; AError: Exception;
  var AAction: TAMQPConsumerErrorAction);
begin
  PcAtomicInc(FErrors);
end;

procedure TAMQPChannelCloseIntegrationTests.PreparaConsumo(AMensagens: Integer);
var
  LDecl: TAMQPQueueDeclare;
  I: Integer;
begin
  // Exclusiva da conexão: os dois canais a enxergam, e ela some no fim.
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  FQueue := FChanB.DeclareQueue(LDecl).QueueName;
  for I := 1 to AMensagens do
    FChanB.PublishText('', FQueue, 'msg-' + IntToStr(I));
  FChanA.OnConsumerError := HandleError;
  FChanA.Qos(AMensagens);
  FChanA.Consume(FQueue, HandleDelivery);
  ChecaOk('o callback deveria ter começado', FStarted.WaitFor(ESPERA_MS) = wrSignaled);
end;

function TAMQPChannelCloseIntegrationTests.ProntasNaFila: Cardinal;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.QueueName := FQueue;
  LDecl.Passive := True;
  Result := FChanB.DeclareQueue(LDecl).MessageCount;
end;

function TAMQPChannelCloseIntegrationTests.EsperaProntas(AEsperadas: Cardinal): Cardinal;
var
  LWaited: Integer;
begin
  LWaited := 0;
  repeat
    Result := ProntasNaFila;
    if (Result = AEsperadas) or (LWaited >= ESPERA_MS) then
      Exit;
    TThread.Sleep(50);
    Inc(LWaited, 50);
  until False;
end;

procedure TAMQPChannelCloseIntegrationTests.Close_ComCallbackEmVoo_AckValeEConexaoSobrevive;
begin
  // O cenário do pascal-agenda-faa: o consumidor para logo depois de o
  // handler começar. Até a v0.4.1 o Ack saía depois do Close-Ok e o broker
  // derrubava a conexão (504).
  PreparaConsumo(1);
  FChanA.Close;
  ChecaInt('o callback deveria ter terminado dentro do Close', 1, PcAtomicGet(FDone));
  ChecaOk('o canal A deveria estar fechado', not FChanA.IsOpen);
  // Uma RPC em B depois do Ack: só responde se a conexão sobreviveu. E o Ack
  // valeu (saiu antes do Channel.Close): nada volta para a fila.
  ChecaInt('o Ack do callback deveria ter valido', 0, Integer(ProntasNaFila));
  ChecaOk('a conexão deveria seguir aberta', FConn.IsOpen);
  ChecaInt('o OnConsumerError não deveria ter sido chamado', 0, PcAtomicGet(FErrors));
end;

procedure TAMQPChannelCloseIntegrationTests.Close_ComVariasEntregasEmVoo_TodasConfirmadas;
begin
  // Entregas já despachadas para o pool (em execução ou ainda na fila dele)
  // terminam e confirmam: fechar não gera reentrega do que já foi entregue.
  FSleepMs := 100;
  PreparaConsumo(5);
  FChanA.Close;
  ChecaInt('todo callback entregue deveria ter terminado', PcAtomicGet(FCalls), PcAtomicGet(FDone));
  ChecaInt('nada deveria voltar para a fila', 5 - PcAtomicGet(FDone), Integer(ProntasNaFila));
  ChecaOk('a conexão deveria seguir aberta', FConn.IsOpen);
  ChecaInt('o OnConsumerError não deveria ter sido chamado', 0, PcAtomicGet(FErrors));
end;

procedure TAMQPChannelCloseIntegrationTests.Free_ComCallbackEmVoo_ConexaoSobrevive;
begin
  // O destrutor fecha pelo mesmo caminho.
  PreparaConsumo(1);
  FreeAndNil(FChanA);
  ChecaInt('o callback deveria ter terminado dentro do Free', 1, PcAtomicGet(FDone));
  ChecaInt('o Ack do callback deveria ter valido', 0, Integer(ProntasNaFila));
  ChecaOk('a conexão deveria seguir aberta', FConn.IsOpen);
end;

procedure TAMQPChannelCloseIntegrationTests.FechadoPeloBroker_AckAtrasadoNaoDerrubaAConexao;
var
  LDecl: TAMQPQueueDeclare;
  LRaised: Boolean;
  LGet: TAMQPGetResult;
  LWaited: Integer;
begin
  // O broker fecha o canal A (404) com um callback em voo. O Ack que ele dá
  // depois cai num canal fechado: não pode sair para o fio.
  PreparaConsumo(1);
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.QueueName := 'amqp-faa-fila-que-nao-existe-' + IntToStr(PcTickMs);
  LDecl.Passive := True;
  LRaised := False;
  try
    FChanA.DeclareQueue(LDecl);
  except
    on EAMQPChannel do
      LRaised := True;
  end;
  ChecaOk('o passive declare deveria ter dado 404', LRaised);
  ChecaOk('o canal A deveria estar fechado pelo broker', not FChanA.IsOpen);

  LWaited := 0;
  while (PcAtomicGet(FDone) < 1) and (LWaited < ESPERA_MS) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
  ChecaInt('o callback deveria ter terminado', 1, PcAtomicGet(FDone));
  ChecaInt('o Ack num canal fechado não deveria levantar', 0, PcAtomicGet(FErrors));
  // O canal fechou com a entrega sem Ack: o broker a devolveu, marcada.
  ChecaInt('a entrega sem Ack deveria ter voltado para a fila', 1, Integer(EsperaProntas(1)));
  LGet := FChanB.BasicGet(FQueue, True);
  ChecaOk('a entrega sem Ack deveria ter voltado para a fila', LGet.Found);
  ChecaOk('a entrega devolvida deveria vir como redelivered', LGet.Redelivered);
  ChecaOk('a conexão deveria seguir aberta', FConn.IsOpen);
end;

procedure TAMQPChannelCloseIntegrationTests.AckDepoisDoClose_NaoDerrubaAConexao;
var
  LGet: TAMQPGetResult;
  LDecl: TAMQPQueueDeclare;
begin
  // Ack manual, de outra thread, depois de o canal ter fechado: no-op.
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  FQueue := FChanB.DeclareQueue(LDecl).QueueName;
  FChanB.PublishText('', FQueue, 'manual');
  // Publish em B e Get em A não têm ordem entre si: espera a mensagem chegar.
  ChecaInt('a mensagem deveria ter chegado', 1, Integer(EsperaProntas(1)));
  LGet := FChanA.BasicGet(FQueue, False);
  ChecaOk('deveria haver uma mensagem', LGet.Found);
  FChanA.Close;
  // Logo depois do Close-Ok o RabbitMQ ainda tolera o frame (o canal fica um
  // instante em "closing" do lado dele); com o atraso de um callback, não.
  TThread.Sleep(FSleepMs);
  FChanA.Ack(LGet.DeliveryTag);
  FChanA.Nack(LGet.DeliveryTag, True);
  // A entrega voltou com o Close (sem Ack); a RPC prova a conexão de pé.
  ChecaInt('a entrega sem Ack deveria ter voltado para a fila', 1, Integer(EsperaProntas(1)));
  ChecaOk('a conexão deveria seguir aberta', FConn.IsOpen);
end;

initialization
  RegisterTest(TAMQPChannelCloseIntegrationTests);

end.
