unit AMQP.ChannelIntegrationTests;

{ Integração de canais/publish/get — precisa de RabbitMQ em localhost:5672.
  Sobe com: docker compose -f docker/docker-compose.yml up -d }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes, Rtti,
  AMQP.Threading,
  AMQP.Connection,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  TAMQPChannelIntegrationTests = class(TTestCase)
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    // Estado capturado pelos callbacks (of object — a lib não aceita métodos
    // anônimos aqui, ver CLAUDE.md); cada teste reseta o que usa antes de armar
    // o callback correspondente. TInterlocked não existe no FPC -> AmqpAtomic*.
    FReturned: TAMQPReturnedMessage;
    FGotReturn: Integer;
    FConfirmedSeq: UInt64;
    FAckFlag: Integer;
    FAckCount: Integer;
    FNackCount: Integer;
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
    procedure HandleBasicReturn(AChannel: TAMQPChannel; const AReturned: TAMQPReturnedMessage);
    procedure HandleConfirmSingle(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
    procedure HandleConfirmAckNack(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean);
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure PublicaEBuscaComPropriedades;
    procedure PublishText_EBusca;
    procedure GetEmpty_QuandoFilaVazia;
    procedure DeclareQueue_RetornaNomeGerado;
    procedure Unbind_ParaDeRotearParaAFila;
    procedure ExchangeBind_RoteiaSourceParaDest;
    procedure PublishMandatory_SemRota_DisparaOnBasicReturn;
    procedure Confirm_PublishRoteavel_Ackado;
    procedure Confirm_WaitForConfirms_VariosPublishes;
    procedure Confirm_WaitForConfirms_DetectaNackJaResolvido;
  end;

implementation

procedure TAMQPChannelIntegrationTests.SetUp;
begin
  FConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  FConn.Open;
  FChan := FConn.CreateChannel;
  FGotReturn := 0;
  FConfirmedSeq := 0;
  FAckFlag := 0;
  FAckCount := 0;
  FNackCount := 0;
end;

procedure TAMQPChannelIntegrationTests.TearDown;
begin
  FChan.Free;
  FConn.Free;
end;

function TAMQPChannelIntegrationTests.DeclareTempQueue: string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;   // some no fechamento da conexão
  LDecl.AutoDelete := True;
  Result := FChan.DeclareQueue(LDecl).QueueName; // '' => nome gerado pelo servidor
end;

function TAMQPChannelIntegrationTests.GetWithRetry(const AQueue: string): TAMQPGetResult;
var
  I: Integer;
begin
  // A entrega após publish é assíncrona; tenta por até ~0,5s.
  for I := 1 to 20 do
  begin
    Result := FChan.BasicGet(AQueue, True);
    if Result.Found then
      Exit;
    TThread.Sleep(25);
  end;
end;

procedure TAMQPChannelIntegrationTests.HandleBasicReturn(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
begin
  FReturned := AReturned;
  AmqpAtomicSet(FGotReturn, 1);
end;

procedure TAMQPChannelIntegrationTests.HandleConfirmSingle(AChannel: TAMQPChannel;
  ASeqNo: UInt64; AAck: Boolean);
begin
  FConfirmedSeq := ASeqNo;
  if AAck then
    AmqpAtomicSet(FAckFlag, 1);
end;

procedure TAMQPChannelIntegrationTests.HandleConfirmAckNack(AChannel: TAMQPChannel;
  ASeqNo: UInt64; AAck: Boolean);
begin
  if AAck then
    AmqpAtomicInc(FAckCount)
  else
    AmqpAtomicInc(FNackCount);
end;

procedure TAMQPChannelIntegrationTests.PublicaEBuscaComPropriedades;
var
  LQueue: string;
  LProps: TAMQPBasicProperties;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;

  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  LProps.SetPersistent;
  LProps.SetCorrelationId('corr-99');
  LProps.SetMessageId('msg-77');

  // Exchange padrão ('') roteia pela routing-key = nome da fila.
  FChan.Publish('', LQueue, TEncoding.UTF8.GetBytes('{"chave":"NFe123"}'), LProps);

  LResult := GetWithRetry(LQueue);
  AssertTrue('mensagem deveria ter sido entregue', LResult.Found);
  AssertEquals('corpo', '{"chave":"NFe123"}', LResult.BodyAsText);
  AssertEquals('content-type', 'application/json', LResult.Properties.ContentType);
  AssertEquals('correlation-id', 'corr-99', LResult.Properties.CorrelationId);
  AssertEquals('message-id', 'msg-77', LResult.Properties.MessageId);
  AssertEquals('delivery-mode persistente', 2, Integer(LResult.Properties.DeliveryMode));
end;

procedure TAMQPChannelIntegrationTests.PublishText_EBusca;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;
  FChan.PublishText('', LQueue, 'olá mundo NFe');

  LResult := GetWithRetry(LQueue);
  AssertTrue(LResult.Found);
  AssertEquals('olá mundo NFe', LResult.BodyAsText);
  AssertEquals('text/plain', LResult.Properties.ContentType);
end;

procedure TAMQPChannelIntegrationTests.GetEmpty_QuandoFilaVazia;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;
  LResult := FChan.BasicGet(LQueue, True);
  AssertFalse('fila recém-criada deveria estar vazia', LResult.Found);
end;

procedure TAMQPChannelIntegrationTests.DeclareQueue_RetornaNomeGerado;
var
  LQueue: string;
begin
  LQueue := DeclareTempQueue;
  AssertTrue('servidor deveria gerar um nome de fila', LQueue <> '');
end;

procedure TAMQPChannelIntegrationTests.Unbind_ParaDeRotearParaAFila;
var
  LExchange, LQueue, LRk: string;
  LEx: TAMQPExchangeDeclare;
  LBind: TAMQPQueueBind;
  LUnbind: TAMQPQueueUnbind;
  LResult: TAMQPGetResult;
begin
  LExchange := 'test-unbind-ex-' + TGUID.NewGuid.ToString;
  LRk := 'rota.teste';
  // Exchange NÃO durável (some ao fechar a conexão) e NÃO auto-delete: se fosse
  // auto-delete, o unbind removeria o último binding e o broker apagaria a
  // exchange — o publish seguinte cairia numa exchange inexistente e o servidor
  // fecharia o canal de forma assíncrona (corrida com o TearDown). Mantendo-a
  // viva, o publish sem binding é simplesmente descartado (é o que queremos).
  LEx := TAMQPExchangeDeclare.Create(LExchange, AMQP_EXCHANGE_TYPE_DIRECT, False);
  FChan.DeclareExchange(LEx);

  LQueue := DeclareTempQueue;

  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := LQueue;
  LBind.ExchangeName := LExchange;
  LBind.RoutingKey := LRk;
  FChan.BindQueue(LBind);

  // Com o bind, a mensagem chega à fila.
  FChan.PublishText(LExchange, LRk, 'antes-do-unbind');
  LResult := GetWithRetry(LQueue);
  AssertTrue('com o bind, a mensagem deveria chegar', LResult.Found);
  AssertEquals('antes-do-unbind', LResult.BodyAsText);

  // Desfaz o binding.
  LUnbind := Default(TAMQPQueueUnbind);
  LUnbind.QueueName := LQueue;
  LUnbind.ExchangeName := LExchange;
  LUnbind.RoutingKey := LRk;
  FChan.UnbindQueue(LUnbind);

  // Sem o bind, a mesma publicação não é mais roteada para a fila.
  FChan.PublishText(LExchange, LRk, 'depois-do-unbind');
  TThread.Sleep(200); // dá tempo de o broker rotear (ou descartar)
  LResult := FChan.BasicGet(LQueue, True);
  AssertFalse('após o unbind, a fila não deveria mais receber', LResult.Found);
end;

procedure TAMQPChannelIntegrationTests.ExchangeBind_RoteiaSourceParaDest;
var
  LSource, LDest, LQueue, LRk: string;
  LEx: TAMQPExchangeDeclare;
  LQBind: TAMQPQueueBind;
  LEBind: TAMQPExchangeBinding;
  LResult: TAMQPGetResult;
begin
  // source (direct) --[rk]--> dest (fanout) --> fila. Exchanges auto-delete: como
  // os bindings existem durante todo o teste, elas sobrevivem; no teardown a fila
  // exclusiva some -> dest perde o binding -> some -> source perde o binding ->
  // some (cascata limpa, sem lixo no broker e sem publicar em exchange apagada).
  LRk := 'rota.ex';
  LSource := 'test-exsrc-' + TGUID.NewGuid.ToString;
  LDest := 'test-exdst-' + TGUID.NewGuid.ToString;

  LEx := TAMQPExchangeDeclare.Create(LSource, AMQP_EXCHANGE_TYPE_DIRECT, False);
  LEx.AutoDelete := True;
  FChan.DeclareExchange(LEx);

  LEx := TAMQPExchangeDeclare.Create(LDest, AMQP_EXCHANGE_TYPE_FANOUT, False);
  LEx.AutoDelete := True;
  FChan.DeclareExchange(LEx);

  LQueue := DeclareTempQueue;
  LQBind := Default(TAMQPQueueBind);
  LQBind.QueueName := LQueue;
  LQBind.ExchangeName := LDest;
  LQBind.RoutingKey := ''; // fanout ignora a routing-key
  FChan.BindQueue(LQBind);

  LEBind := Default(TAMQPExchangeBinding);
  LEBind.Destination := LDest;
  LEBind.Source := LSource;
  LEBind.RoutingKey := LRk;
  FChan.BindExchange(LEBind);

  // Publica na source com a routing-key do binding: source -> dest -> fila.
  FChan.PublishText(LSource, LRk, 'via-exchange-bind');
  LResult := GetWithRetry(LQueue);
  AssertTrue('a mensagem deveria rotear source->dest->fila', LResult.Found);
  AssertEquals('via-exchange-bind', LResult.BodyAsText);
end;

procedure TAMQPChannelIntegrationTests.PublishMandatory_SemRota_DisparaOnBasicReturn;
var
  I: Integer;
begin
  FChan.OnBasicReturn := HandleBasicReturn;

  // Exchange padrão + routing-key sem fila nenhuma ligada: não roteável.
  FChan.Publish('', 'rota.inexistente.' + TGUID.NewGuid.ToString,
    TEncoding.UTF8.GetBytes('sem destino'), TAMQPBasicProperties.Empty, True {mandatory});

  for I := 1 to 80 do
  begin
    if AmqpAtomicGet(FGotReturn) = 1 then
      Break;
    TThread.Sleep(25);
  end;

  AssertEquals('OnBasicReturn deveria disparar', 1, AmqpAtomicGet(FGotReturn));
  AssertEquals('sem destino', FReturned.BodyAsText);
end;

procedure TAMQPChannelIntegrationTests.Confirm_PublishRoteavel_Ackado;
var
  LQueue: string;
  LSeq: UInt64;
  I: Integer;
begin
  LQueue := DeclareTempQueue;
  FChan.OnConfirm := HandleConfirmSingle;
  FChan.ConfirmSelect;

  // Exchange padrão + routing-key = nome da fila: roteável, o broker confirma.
  LSeq := FChan.PublishText('', LQueue, 'confirma isso');
  AssertTrue('primeiro publish em confirm mode deveria receber seq-no 1', UInt64(1) = LSeq);
  AssertTrue('broker deveria confirmar (ack) o publish', FChan.WaitForConfirm(LSeq, 5000));

  for I := 1 to 80 do
  begin
    if AmqpAtomicGet(FAckFlag) = 1 then
      Break;
    TThread.Sleep(25);
  end;
  AssertEquals('OnConfirm deveria disparar com ack', 1, AmqpAtomicGet(FAckFlag));
  AssertTrue('OnConfirm deveria trazer o seq-no 1', UInt64(1) = FConfirmedSeq);
end;

procedure TAMQPChannelIntegrationTests.Confirm_WaitForConfirms_VariosPublishes;
var
  LQueue: string;
  I: Integer;
  LLast: UInt64;
begin
  LQueue := DeclareTempQueue;
  FChan.ConfirmSelect;

  LLast := 0;
  for I := 1 to 10 do
    LLast := FChan.PublishText('', LQueue, 'msg ' + IntToStr(I));
  AssertTrue('décimo publish deveria receber seq-no 10', UInt64(10) = LLast);

  AssertTrue('todos os publishes roteáveis deveriam ser confirmados',
    FChan.WaitForConfirms(5000));
end;

procedure TAMQPChannelIntegrationTests.Confirm_WaitForConfirms_DetectaNackJaResolvido;
var
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LQueue: string;
  I: Integer;
begin
  // Fila que REJEITA publishes quando cheia: x-max-length=1 + x-overflow=reject-publish.
  // Em confirm mode, o publish que estoura o limite volta como Basic.Nack.
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-max-length', TValue.From<Integer>(1));
    LArgs.Put('x-overflow', 'reject-publish');
    LDecl := Default(TAMQPQueueDeclare);
    LDecl.Exclusive := True;
    LDecl.AutoDelete := True;
    LDecl.Arguments := LArgs;
    LQueue := FChan.DeclareQueue(LDecl).QueueName;
  finally
    LArgs.Free;
  end;

  FChan.OnConfirm := HandleConfirmAckNack;
  FChan.ConfirmSelect;

  FChan.PublishText('', LQueue, 'A'); // enche a fila -> ack
  FChan.PublishText('', LQueue, 'B'); // estoura o limite -> nack

  // Espera AMBOS resolverem ANTES de chamar WaitForConfirms — é justamente isso
  // que expõe o achado #1: o nack já chegou quando WaitForConfirms é chamado.
  for I := 1 to 200 do
  begin
    if (AmqpAtomicGet(FAckCount) + AmqpAtomicGet(FNackCount)) >= 2 then
      Break;
    TThread.Sleep(25);
  end;

  // Prova que o cenário funcionou (1 ack + 1 nack) — se o broker não nack-ar,
  // esta asserção falha com mensagem clara, em vez de mascarar o teste real.
  AssertEquals('o publish B deveria ter sido nack-ado (reject-publish)',
    1, AmqpAtomicGet(FNackCount));

  // O achado #1: como um publish do lote foi nack-ado, WaitForConfirms deve
  // retornar False — mesmo o nack tendo chegado ANTES da chamada.
  AssertFalse('WaitForConfirms deve reportar False quando um publish do lote foi nack-ado',
    FChan.WaitForConfirms(2000));
end;

initialization
  RegisterTest(TAMQPChannelIntegrationTests);

end.
