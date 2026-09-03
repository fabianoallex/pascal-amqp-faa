unit AMQP.ServerEngineDispatchTests;

{ Despacho real na FSM -- WS5 da Fase 2 (ver CLAUDE.md).

  Estes sao os testes que fecham o circuito: o CLIENTE REAL da lib
  (TAMQPConnection) contra o broker embutido, agora com engine de verdade
  atras. Ate a WS4 o broker respondia os *-Ok sem rotear nada; aqui ele
  declara, roteia, entrega, confirma e apaga.

  Duas fixtures:
  - TEngineDispatchTests: o caminho feliz ponta a ponta (publish/consume/get/
    prefetch/nack/cancel/purge/delete e a devolucao das nao-confirmadas quando
    a conexao cai).
  - TErrorTableTests: uma asserção por linha da tabela de erros do plano --
    reply-code exato e escopo (canal) certo.

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerEngineDispatchTests.pas --
  mantenha os dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods,
  AMQP.Threading,
  AMQP.Connection,
  AMQP.Server.Types,
  AMQP.Server.Broker;

type
  [TestFixture]
  TEngineDispatchTests = class
  private
    FBroker: TAMQPServer;
    FLock: TCriticalSection;
    FCorpos: TStringList;
    FRedelivered: Integer;
    FAckManual: Boolean;
    procedure OnDelivery(AChannel: TAMQPChannel;
      const ADelivery: TAMQPDelivery);
    function Params: TAMQPConnectionParams;
    /// Espera ACount entregas chegarem ao callback (a entrega e assincrona).
    function EsperaEntregas(ACount, ATimeoutMs: Integer): Boolean;
    /// Corpos recebidos, na ordem, separados por '|'.
    function Corpos: string;
    function QuantasRecebidas: Integer;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure PublicaEConsomeComAck;
    [Test] procedure PublicaEBuscaComGet;
    [Test] procedure GetEmFilaVazia_DevolveVazio;
    [Test] procedure RoteiaPorExchangeTopic;
    [Test] procedure FanoutEntregaAsDuasFilas;
    [Test] procedure PrefetchLimitaEntregasNaoConfirmadas;
    [Test] procedure NackComRequeue_Reentrega;
    [Test] procedure Cancel_ParaDeEntregar;
    [Test] procedure DeclareDevolveContagens;
    [Test] procedure DeleteDevolveContagemEApaga;
    [Test] procedure ConexaoCai_DevolveNaoConfirmadas;
    [Test] procedure AckRemoveDeVez_NaoVoltaAposQueda;
    [Test] procedure FilaAnonima_GanhaNomeGerado;
    [Test] procedure PublishSemRota_NaoEErro;
  end;

  [TestFixture]
  TErrorTableTests = class
  private
    FBroker: TAMQPServer;
    function Params: TAMQPConnectionParams;
    procedure OnDelivery(AChannel: TAMQPChannel;
      const ADelivery: TAMQPDelivery);
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure Erro404_ConsumeDeFilaInexistente;
    [Test] procedure Erro404_GetDeFilaInexistente;
    [Test] procedure Erro404_BindComFilaInexistente;
    [Test] procedure Erro404_PublishEmExchangeInexistente;
    [Test] procedure Erro406_RedeclareDivergente;
    [Test] procedure Erro406_DeleteIfEmptyComMensagem;
    [Test] procedure Erro406_DeleteIfUnusedComConsumidor;
    [Test] procedure Erro406_TipoDeExchangeInvalido;
    [Test] procedure Erro403_NomeReservadoNoDeclare;
    [Test] procedure Erro403_UserIdDivergente;
    [Test] procedure Erro405_FilaExclusivaDeOutraConexao;
    [Test] procedure ErroDeCanal_NaoDerrubaAConexao;
  end;

  [TestFixture]
  TConfirmTests = class
  private
    FBroker: TAMQPServer;
    FLock: TCriticalSection;
    FDevolvidas: TStringList;
    procedure OnReturn(AChannel: TAMQPChannel;
      const AReturned: TAMQPReturnedMessage);
    function Params: TAMQPConnectionParams;
    function EsperaDevolucoes(ACount, ATimeoutMs: Integer): Boolean;
    function QuantasDevolvidas: Integer;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure Confirm_PublishRecebeAck;
    [Test] procedure Confirm_SeqNoComecaEmUmESegue;
    [Test] procedure Confirm_PublishSemRotaTambemEConfirmado;
    [Test] procedure Confirm_LoteGrandeTodoConfirmado;
    [Test] procedure Mandatory_SemRota_DisparaBasicReturn;
    [Test] procedure Mandatory_ComRota_NaoDisparaReturn;
    [Test] procedure Mandatory_SemConfirmMode_AindaDevolve;
  end;

implementation

{ --- helpers --- }

function NovoBroker: TAMQPServer;
begin
  Result := TAMQPServer.Create;
  Result.BindAddress := '127.0.0.1';
  Result.Port := 0;
  Result.Start;
end;

function ParamsDe(ABroker: TAMQPServer): TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := '127.0.0.1';
  Result.Port := ABroker.Port;
end;

function NovoBind(const AQueue, AExchange, ARoutingKey: string): TAMQPQueueBind;
begin
  Result.QueueName := AQueue;
  Result.ExchangeName := AExchange;
  Result.RoutingKey := ARoutingKey;
  Result.NoWait := False;
  Result.Arguments := nil;
end;

function NovoDelete(const AQueue: string;
  AIfUnused, AIfEmpty: Boolean): TAMQPQueueDelete;
begin
  Result.QueueName := AQueue;
  Result.IfUnused := AIfUnused;
  Result.IfEmpty := AIfEmpty;
  Result.NoWait := False;
end;

// Extrai o reply-code da mensagem que o cliente monta ao receber um
// Channel.Close do servidor ('canal N fechado pelo servidor: CODIGO texto').
function CodigoDoErro(const AMensagem: string): Integer;
var
  I, J: Integer;
  LDigitos: string;
begin
  Result := 0;
  I := Pos('servidor: ', AMensagem);
  if I <= 0 then
    Exit;
  Inc(I, Length('servidor: '));
  LDigitos := '';
  J := I;
  while (J <= Length(AMensagem)) and (AMensagem[J] >= '0')
    and (AMensagem[J] <= '9') do
  begin
    LDigitos := LDigitos + AMensagem[J];
    Inc(J);
  end;
  if LDigitos <> '' then
    Result := StrToIntDef(LDigitos, 0);
end;

{ --- TEngineDispatchTests --- }

procedure TEngineDispatchTests.Setup;
begin
  FBroker := NovoBroker;
  FLock := TCriticalSection.Create;
  FCorpos := TStringList.Create;
  FRedelivered := 0;
  FAckManual := True;
end;

procedure TEngineDispatchTests.TearDown;
begin
  FBroker.Free;
  FCorpos.Free;
  FLock.Free;
end;

function TEngineDispatchTests.Params: TAMQPConnectionParams;
begin
  Result := ParamsDe(FBroker);
end;

procedure TEngineDispatchTests.OnDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  FLock.Enter;
  try
    FCorpos.Add(ADelivery.BodyAsText);
    if ADelivery.Redelivered then
      Inc(FRedelivered);
  finally
    FLock.Leave;
  end;
  if FAckManual then
    AChannel.Ack(ADelivery.DeliveryTag);
end;

function TEngineDispatchTests.QuantasRecebidas: Integer;
begin
  FLock.Enter;
  try
    Result := FCorpos.Count;
  finally
    FLock.Leave;
  end;
end;

function TEngineDispatchTests.EsperaEntregas(ACount,
  ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  while (QuantasRecebidas < ACount) and (AmqpTickMs < LDeadline) do
    Sleep(10);
  Result := QuantasRecebidas >= ACount;
end;

function TEngineDispatchTests.Corpos: string;
var
  I: Integer;
begin
  Result := '';
  FLock.Enter;
  try
    for I := 0 to FCorpos.Count - 1 do
    begin
      if Result <> '' then
        Result := Result + '|';
      Result := Result + FCorpos[I];
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TEngineDispatchTests.PublicaEConsomeComAck;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.ack'));
    LCh.Consume('q.ack', OnDelivery, False);
    LCh.PublishText('', 'q.ack', 'ola');

    Assert.IsTrue(EsperaEntregas(1, 5000), 'a mensagem chegou ao consumidor');
    Assert.AreEqual('ola', Corpos, 'corpo integro');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.PublicaEBuscaComGet;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LGet: TAMQPGetResult;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.get'));
    LCh.PublishText('', 'q.get', 'primeira');
    LCh.PublishText('', 'q.get', 'segunda');

    LGet := LCh.BasicGet('q.get', True);
    Assert.IsTrue(LGet.Found, 'achou a primeira');
    Assert.AreEqual('primeira', LGet.BodyAsText, 'corpo');
    Assert.AreEqual(1, Integer(LGet.MessageCount),
      'message-count do Get-Ok: quantas sobraram');

    LGet := LCh.BasicGet('q.get', True);
    Assert.IsTrue(LGet.Found, 'achou a segunda');
    Assert.AreEqual('segunda', LGet.BodyAsText, 'corpo');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.GetEmFilaVazia_DevolveVazio;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LGet: TAMQPGetResult;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.vazia'));
    LGet := LCh.BasicGet('q.vazia', True);
    Assert.IsFalse(LGet.Found, 'fila vazia devolve Get-Empty');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.RoteiaPorExchangeTopic;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LGet: TAMQPGetResult;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareExchange(TAMQPExchangeDeclare.Create('ex.topic',
      AMQP_EXCHANGE_TYPE_TOPIC));
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.aprovada'));
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.todas'));
    LCh.BindQueue(NovoBind('q.aprovada', 'ex.topic', 'nota.aprovada'));
    LCh.BindQueue(NovoBind('q.todas', 'ex.topic', 'nota.#'));

    LCh.PublishText('ex.topic', 'nota.aprovada', 'A');
    LCh.PublishText('ex.topic', 'nota.cancelada', 'C');

    // A fila especifica so' recebe o que casa; a curinga recebe as duas.
    LGet := LCh.BasicGet('q.aprovada', True);
    Assert.IsTrue(LGet.Found, 'q.aprovada recebeu');
    Assert.AreEqual('A', LGet.BodyAsText, 'so a que casa');
    Assert.IsFalse(LCh.BasicGet('q.aprovada', True).Found,
      'e nada alem dela');

    LGet := LCh.BasicGet('q.todas', True);
    Assert.IsTrue(LGet.Found, 'q.todas recebeu a primeira');
    Assert.AreEqual('A', LGet.BodyAsText, 'ordem preservada');
    LGet := LCh.BasicGet('q.todas', True);
    Assert.IsTrue(LGet.Found, 'q.todas recebeu a segunda');
    Assert.AreEqual('C', LGet.BodyAsText, 'ordem preservada');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.FanoutEntregaAsDuasFilas;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareExchange(TAMQPExchangeDeclare.Create('ex.fan',
      AMQP_EXCHANGE_TYPE_FANOUT));
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.f1'));
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.f2'));
    LCh.BindQueue(NovoBind('q.f1', 'ex.fan', ''));
    LCh.BindQueue(NovoBind('q.f2', 'ex.fan', ''));

    LCh.PublishText('ex.fan', 'qualquer', 'broadcast');

    // UMA mensagem, DUAS filas -- corpo compartilhado sem copia.
    Assert.AreEqual('broadcast', LCh.BasicGet('q.f1', True).BodyAsText,
      'chegou na primeira');
    Assert.AreEqual('broadcast', LCh.BasicGet('q.f2', True).BodyAsText,
      'e na segunda');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.PrefetchLimitaEntregasNaoConfirmadas;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  I: Integer;
begin
  FAckManual := False; // ninguem confirma: o prefetch tem de segurar
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.qos'));
    LCh.Qos(1);
    LCh.Consume('q.qos', OnDelivery, False); // ack manual, e nao vamos ackar
    for I := 1 to 5 do
      LCh.PublishText('', 'q.qos', 'm' + IntToStr(I));

    Assert.IsTrue(EsperaEntregas(1, 5000), 'entregou a primeira');
    Sleep(300); // tempo de sobra para entregar mais, se o prefetch falhasse
    Assert.AreEqual(1, QuantasRecebidas,
      'com prefetch=1 e nenhum ack, so uma entrega fica em voo');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.NackComRequeue_Reentrega;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LGet: TAMQPGetResult;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.nack'));
    LCh.PublishText('', 'q.nack', 'volta');

    LGet := LCh.BasicGet('q.nack', False); // com ack
    Assert.IsTrue(LGet.Found, 'buscou');
    Assert.IsFalse(LGet.Redelivered, 'primeira vez');
    LCh.Nack(LGet.DeliveryTag, True); // requeue

    LGet := LCh.BasicGet('q.nack', True);
    Assert.IsTrue(LGet.Found, 'voltou para a fila');
    Assert.AreEqual('volta', LGet.BodyAsText, 'mesma mensagem');
    Assert.IsTrue(LGet.Redelivered, 'marcada como reentrega');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.Cancel_ParaDeEntregar;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LTag: string;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.cancel'));
    LTag := LCh.Consume('q.cancel', OnDelivery, True);
    LCh.PublishText('', 'q.cancel', 'antes');
    Assert.IsTrue(EsperaEntregas(1, 5000), 'entregou antes do cancel');

    LCh.Cancel(LTag);
    LCh.PublishText('', 'q.cancel', 'depois');
    Sleep(300);
    Assert.AreEqual(1, QuantasRecebidas, 'nada foi entregue apos o cancel');
    // E a mensagem continua na fila, esperando outro consumidor.
    Assert.AreEqual('depois', LCh.BasicGet('q.cancel', True).BodyAsText,
      'a mensagem ficou na fila');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.DeclareDevolveContagens;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LOk: TAMQPQueueDeclareOk;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.cont'));
    LCh.PublishText('', 'q.cont', 'a');
    LCh.PublishText('', 'q.cont', 'b');

    // Redeclare equivalente devolve as contagens de verdade.
    LOk := LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.cont'));
    Assert.AreEqual('q.cont', LOk.QueueName, 'nome');
    Assert.AreEqual(2, Integer(LOk.MessageCount), 'message-count');
    Assert.AreEqual(0, Integer(LOk.ConsumerCount), 'consumer-count');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.DeleteDevolveContagemEApaga;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.del'));
    LCh.PublishText('', 'q.del', 'a');
    Assert.AreEqual(1, Integer(LCh.DeleteQueue(NovoDelete('q.del', False, False))),
      'delete devolve quantas mensagens foram junto');

    // Apagada de verdade: um consume nela agora e 404.
    LCodigo := 0;
    try
      LCh.Consume('q.del', OnDelivery, True);
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(404, LCodigo, 'a fila sumiu mesmo');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.ConexaoCai_DevolveNaoConfirmadas;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LGet: TAMQPGetResult;
begin
  // Primeira conexao: busca sem confirmar e some.
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.orfa'));
    LCh.PublishText('', 'q.orfa', 'pendente');
    LGet := LCh.BasicGet('q.orfa', False); // com ack, e NAO vamos ackar
    Assert.IsTrue(LGet.Found, 'buscou');
  finally
    LConn.Free; // conexao cai com a entrega em aberto
  end;

  // Segunda conexao: a mensagem tem de estar de volta na fila, EXATAMENTE
  // uma vez e marcada como reentrega.
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.orfa'));
    LGet := LCh.BasicGet('q.orfa', True);
    Assert.IsTrue(LGet.Found, 'a nao-confirmada voltou para a fila');
    Assert.AreEqual('pendente', LGet.BodyAsText, 'mesma mensagem');
    Assert.IsTrue(LGet.Redelivered, 'marcada como reentrega');
    Assert.IsFalse(LCh.BasicGet('q.orfa', True).Found,
      'e voltou UMA vez, nao duas');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.FilaAnonima_GanhaNomeGerado;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LOk: TAMQPQueueDeclareOk;
  LDecl: TAMQPQueueDeclare;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LDecl := TAMQPQueueDeclare.Create('');
    LOk := LCh.DeclareQueue(LDecl);
    Assert.IsTrue(LOk.QueueName <> '', 'o broker gerou um nome');
    Assert.IsTrue(Pos('amq.gen-', LOk.QueueName) = 1,
      'com o prefixo reservado do servidor');

    // E a fila gerada funciona.
    LCh.PublishText('', LOk.QueueName, 'anon');
    Assert.AreEqual('anon',
      LCh.BasicGet(LOk.QueueName, True).BodyAsText, 'entregou');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.PublishSemRota_NaoEErro;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareExchange(TAMQPExchangeDeclare.Create('ex.sem.bind',
      AMQP_EXCHANGE_TYPE_DIRECT));
    // Exchange existe, mas nenhum binding casa: a mensagem e' descartada em
    // silencio (sem mandatory nao ha Basic.Return -- isso e' WS6).
    LCh.PublishText('ex.sem.bind', 'nada', 'perdida');
    Sleep(100);
    Assert.IsTrue(LCh.IsOpen, 'publish sem rota nao fecha o canal');
    Assert.IsTrue(FBroker.Engine.UnroutedCount >= 1,
      'e o broker contabiliza a publicacao sem rota');
  finally
    LConn.Free;
  end;
end;

{ --- TErrorTableTests --- }

procedure TErrorTableTests.Setup;
begin
  FBroker := NovoBroker;
end;

procedure TErrorTableTests.TearDown;
begin
  FBroker.Free;
end;

function TErrorTableTests.Params: TAMQPConnectionParams;
begin
  Result := ParamsDe(FBroker);
end;

procedure TErrorTableTests.OnDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  // nunca chamado nos testes de erro
end;

procedure TErrorTableTests.Erro404_ConsumeDeFilaInexistente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCodigo := 0;
    try
      LCh.Consume('nao.existe', OnDelivery, True);
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(404, LCodigo, 'consume de fila inexistente e 404');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro404_GetDeFilaInexistente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCodigo := 0;
    try
      LCh.BasicGet('nao.existe', True);
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(404, LCodigo, 'get de fila inexistente e 404');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro404_BindComFilaInexistente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCodigo := 0;
    try
      LCh.BindQueue(NovoBind('nao.existe', 'amq.direct', 'k'));
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(404, LCodigo, 'bind com fila inexistente e 404');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro404_PublishEmExchangeInexistente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
  LDeadline: UInt64;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    // Publish nao e' RPC: o erro chega como Channel.Close assincrono, entao
    // esperamos o canal cair em vez de esperar excecao no publish.
    LCh.PublishText('ex.que.nao.existe', 'k', 'x');
    LDeadline := AmqpTickMs + 3000;
    while LCh.IsOpen and (AmqpTickMs < LDeadline) do
      Sleep(10);
    Assert.IsFalse(LCh.IsOpen,
      'publish em exchange inexistente fecha o canal');
    LCodigo := 0;
    try
      LCh.BasicGet('qualquer', True); // canal morto: a lib levanta
    except
      on E: EAMQPChannel do
        LCodigo := 1;
    end;
    Assert.AreEqual(1, LCodigo, 'e o canal fica inutilizavel');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro406_RedeclareDivergente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
  LDecl: TAMQPQueueDeclare;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.div', True)); // duravel
    LCodigo := 0;
    try
      LDecl := TAMQPQueueDeclare.Create('q.div', False); // nao duravel
      LCh.DeclareQueue(LDecl);
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(406, LCodigo, 'redeclare divergente e 406');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro406_DeleteIfEmptyComMensagem;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.ie'));
    LCh.PublishText('', 'q.ie', 'x');
    LCodigo := 0;
    try
      LCh.DeleteQueue(NovoDelete('q.ie', False, True)); // if-empty
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(406, LCodigo, 'if-empty com mensagem e 406');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro406_DeleteIfUnusedComConsumidor;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.iu'));
    LCh.Consume('q.iu', OnDelivery, True);
    LCodigo := 0;
    try
      LCh.DeleteQueue(NovoDelete('q.iu', True, False)); // if-unused
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(406, LCodigo, 'if-unused com consumidor e 406');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro406_TipoDeExchangeInvalido;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCodigo := 0;
    try
      LCh.DeclareExchange(TAMQPExchangeDeclare.Create('ex.bad', 'nao-existe'));
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    // Desvio deliberado do RabbitMQ (que usa 503 e derruba a CONEXAO):
    // aqui e' erro de canal. Ver CLAUDE.md.
    Assert.AreEqual(406, LCodigo, 'tipo de exchange invalido e 406 de canal');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro403_NomeReservadoNoDeclare;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LCodigo: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCodigo := 0;
    try
      LCh.DeclareQueue(TAMQPQueueDeclare.Create('amq.minha'));
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    Assert.AreEqual(403, LCodigo, 'nome reservado e 403');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro403_UserIdDivergente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LProps: TAMQPBasicProperties;
  LDeadline: UInt64;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.uid'));
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetUserId('outro'); // autenticado e 'guest'
    LCh.Publish('', 'q.uid', AmqpUtf8Encode('x'), LProps);

    // Como o publish nao e' RPC, o 403 chega como Channel.Close assincrono.
    LDeadline := AmqpTickMs + 3000;
    while LCh.IsOpen and (AmqpTickMs < LDeadline) do
      Sleep(10);
    Assert.IsFalse(LCh.IsOpen,
      'user-id diferente do usuario autenticado fecha o canal (403)');
  finally
    LConn.Free;
  end;
end;

procedure TErrorTableTests.Erro405_FilaExclusivaDeOutraConexao;
var
  LConn1, LConn2: TAMQPConnection;
  LCh1, LCh2: TAMQPChannel;
  LCodigo: Integer;
  LDecl: TAMQPQueueDeclare;
begin
  LConn1 := TAMQPConnection.Create(Params);
  try
    LConn1.Open;
    LCh1 := LConn1.CreateChannel;
    LDecl := TAMQPQueueDeclare.Create('q.excl');
    LDecl.Exclusive := True;
    LCh1.DeclareQueue(LDecl);

    LConn2 := TAMQPConnection.Create(Params);
    try
      LConn2.Open;
      LCh2 := LConn2.CreateChannel;
      LCodigo := 0;
      try
        LCh2.Consume('q.excl', OnDelivery, True);
      except
        on E: EAMQPChannel do
          LCodigo := CodigoDoErro(E.Message);
      end;
      Assert.AreEqual(405, LCodigo,
        'fila exclusiva de outra conexao e 405 RESOURCE_LOCKED');
    finally
      LConn2.Free;
    end;
  finally
    LConn1.Free;
  end;
end;

procedure TErrorTableTests.ErroDeCanal_NaoDerrubaAConexao;
var
  LConn: TAMQPConnection;
  LCh1, LCh2: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh1 := LConn.CreateChannel;
    try
      LCh1.BasicGet('nao.existe', True);
    except
      on E: EAMQPChannel do
        ; // esperado: 404
    end;
    Assert.IsFalse(LCh1.IsOpen, 'o canal ofensor morreu');
    Assert.IsTrue(LConn.IsOpen, 'mas a conexao continua viva');

    // E outro canal na mesma conexao funciona normalmente.
    LCh2 := LConn.CreateChannel;
    LCh2.DeclareQueue(TAMQPQueueDeclare.Create('q.depois'));
    LCh2.PublishText('', 'q.depois', 'ok');
    Assert.AreEqual('ok', LCh2.BasicGet('q.depois', True).BodyAsText,
      'canal novo opera normalmente');
  finally
    LConn.Free;
  end;
end;

procedure TEngineDispatchTests.AckRemoveDeVez_NaoVoltaAposQueda;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  // O ack do cliente chega ao CANAL, mas quem guarda a nao-confirmada e a
  // FILA -- e o caminho entre os dois (mapa delivery-tag -> fila do canal)
  // e' invisivel para quem so' olha "a mensagem foi entregue?". Este teste
  // olha o efeito: mensagem confirmada nao pode voltar quando a conexao cai.
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.acked'));
    LCh.Consume('q.acked', OnDelivery, False); // o callback confirma
    LCh.PublishText('', 'q.acked', 'x');
    Assert.IsTrue(EsperaEntregas(1, 5000), 'entregou');
    Sleep(300); // o ack ate a fila e' assincrono
  finally
    LConn.Free; // conexao cai DEPOIS do ack
  end;

  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.acked'));
    Assert.IsFalse(LCh.BasicGet('q.acked', True).Found,
      'mensagem confirmada nao volta para a fila quando a conexao cai');
  finally
    LConn.Free;
  end;
end;

{ --- TConfirmTests --- }

procedure TConfirmTests.Setup;
begin
  FBroker := NovoBroker;
  FLock := TCriticalSection.Create;
  FDevolvidas := TStringList.Create;
end;

procedure TConfirmTests.TearDown;
begin
  FBroker.Free;
  FDevolvidas.Free;
  FLock.Free;
end;

function TConfirmTests.Params: TAMQPConnectionParams;
begin
  Result := ParamsDe(FBroker);
end;

procedure TConfirmTests.OnReturn(AChannel: TAMQPChannel;
  const AReturned: TAMQPReturnedMessage);
begin
  FLock.Enter;
  try
    FDevolvidas.Add(IntToStr(AReturned.ReplyCode) + ':' + AReturned.BodyAsText);
  finally
    FLock.Leave;
  end;
end;

function TConfirmTests.QuantasDevolvidas: Integer;
begin
  FLock.Enter;
  try
    Result := FDevolvidas.Count;
  finally
    FLock.Leave;
  end;
end;

function TConfirmTests.EsperaDevolucoes(ACount, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  while (QuantasDevolvidas < ACount) and (AmqpTickMs < LDeadline) do
    Sleep(10);
  Result := QuantasDevolvidas >= ACount;
end;

procedure TConfirmTests.Confirm_PublishRecebeAck;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.conf'));
    LCh.ConfirmSelect;
    LCh.PublishText('', 'q.conf', 'confirmada');
    Assert.IsTrue(LCh.WaitForConfirms(5000),
      'o broker confirmou o publish -- ate a WS5 isto expirava');
  finally
    LConn.Free;
  end;
end;

procedure TConfirmTests.Confirm_SeqNoComecaEmUmESegue;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.seq'));
    LCh.ConfirmSelect;
    // O seq-no e do CLIENTE; o teste garante que o broker confirma cada um
    // deles individualmente (a sequencia do broker tem de bater com a dele).
    Assert.AreEqual(1, Integer(LCh.PublishText('', 'q.seq', 'a')), 'primeiro');
    Assert.AreEqual(2, Integer(LCh.PublishText('', 'q.seq', 'b')), 'segundo');
    Assert.AreEqual(3, Integer(LCh.PublishText('', 'q.seq', 'c')), 'terceiro');
    Assert.IsTrue(LCh.WaitForConfirm(2, 5000), 'o seq-no 2 foi confirmado');
    Assert.IsTrue(LCh.WaitForConfirms(5000), 'e o lote todo tambem');
  finally
    LConn.Free;
  end;
end;

procedure TConfirmTests.Confirm_PublishSemRotaTambemEConfirmado;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareExchange(TAMQPExchangeDeclare.Create('ex.vazio',
      AMQP_EXCHANGE_TYPE_DIRECT));
    LCh.ConfirmSelect;
    LCh.PublishText('ex.vazio', 'sem.bind', 'ninguem quer');
    // Sem rota NAO e falha: a mensagem foi tratada, entao leva ack (nack e
    // reservado a falha interna do broker).
    Assert.IsTrue(LCh.WaitForConfirms(5000),
      'publish sem rota tambem e confirmado');
  finally
    LConn.Free;
  end;
end;

procedure TConfirmTests.Confirm_LoteGrandeTodoConfirmado;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  I: Integer;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.lote'));
    LCh.ConfirmSelect;
    for I := 1 to 50 do
      LCh.PublishText('', 'q.lote', 'm' + IntToStr(I));
    Assert.IsTrue(LCh.WaitForConfirms(10000), 'as 50 confirmadas');
    Assert.AreEqual(50, Integer(LCh.DeleteQueue(NovoDelete('q.lote', False,
      False))), 'e as 50 chegaram na fila');
  finally
    LConn.Free;
  end;
end;

procedure TConfirmTests.Mandatory_SemRota_DisparaBasicReturn;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.OnBasicReturn := OnReturn;
    LCh.DeclareExchange(TAMQPExchangeDeclare.Create('ex.mand',
      AMQP_EXCHANGE_TYPE_DIRECT));
    LCh.Publish('ex.mand', 'sem.bind', AmqpUtf8Encode('devolve'),
      TAMQPBasicProperties.Empty, True); // mandatory

    Assert.IsTrue(EsperaDevolucoes(1, 5000), 'o broker devolveu a mensagem');
    FLock.Enter;
    try
      Assert.AreEqual('312:devolve', FDevolvidas[0],
        'com NO_ROUTE e o corpo intacto');
    finally
      FLock.Leave;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TConfirmTests.Mandatory_ComRota_NaoDisparaReturn;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.OnBasicReturn := OnReturn;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.mand'));
    LCh.Publish('', 'q.mand', AmqpUtf8Encode('tem rota'),
      TAMQPBasicProperties.Empty, True); // mandatory, mas COM rota
    Sleep(300);
    Assert.AreEqual(0, QuantasDevolvidas, 'nada devolvido');
    Assert.AreEqual('tem rota', LCh.BasicGet('q.mand', True).BodyAsText,
      'a mensagem foi para a fila');
  finally
    LConn.Free;
  end;
end;

procedure TConfirmTests.Mandatory_SemConfirmMode_AindaDevolve;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  // mandatory e confirm mode sao independentes: um publish mandatory sem
  // confirm mode continua gerando Basic.Return.
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.OnBasicReturn := OnReturn;
    LCh.DeclareExchange(TAMQPExchangeDeclare.Create('ex.mand2',
      AMQP_EXCHANGE_TYPE_DIRECT));
    LCh.Publish('ex.mand2', 'nada', AmqpUtf8Encode('x'),
      TAMQPBasicProperties.Empty, True);
    Assert.IsTrue(EsperaDevolucoes(1, 5000),
      'Basic.Return nao depende de confirm mode');
  finally
    LConn.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TEngineDispatchTests);
  TDUnitX.RegisterTestFixture(TErrorTableTests);
  TDUnitX.RegisterTestFixture(TConfirmTests);

end.
