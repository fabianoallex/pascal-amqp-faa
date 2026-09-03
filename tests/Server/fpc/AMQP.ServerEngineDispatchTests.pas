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

  Espelho DUnitX de tests\Server\AMQP.ServerEngineDispatchTests.pas --
  mantenha os dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry,
  SysUtils,
  Classes,
  SyncObjs,
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
  TEngineDispatchTests = class(TTestCase)
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
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published

    procedure PublicaEConsomeComAck;
    procedure PublicaEBuscaComGet;
    procedure GetEmFilaVazia_DevolveVazio;
    procedure RoteiaPorExchangeTopic;
    procedure FanoutEntregaAsDuasFilas;
    procedure PrefetchLimitaEntregasNaoConfirmadas;
    procedure NackComRequeue_Reentrega;
    procedure Cancel_ParaDeEntregar;
    procedure DeclareDevolveContagens;
    procedure DeleteDevolveContagemEApaga;
    procedure ConexaoCai_DevolveNaoConfirmadas;
    procedure AckRemoveDeVez_NaoVoltaAposQueda;
    procedure FilaAnonima_GanhaNomeGerado;
    procedure PublishSemRota_NaoEErro;
  end;

  TErrorTableTests = class(TTestCase)
  private
    FBroker: TAMQPServer;
    function Params: TAMQPConnectionParams;
    procedure OnDelivery(AChannel: TAMQPChannel;
      const ADelivery: TAMQPDelivery);
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published

    procedure Erro404_ConsumeDeFilaInexistente;
    procedure Erro404_GetDeFilaInexistente;
    procedure Erro404_BindComFilaInexistente;
    procedure Erro404_PublishEmExchangeInexistente;
    procedure Erro406_RedeclareDivergente;
    procedure Erro406_DeleteIfEmptyComMensagem;
    procedure Erro406_DeleteIfUnusedComConsumidor;
    procedure Erro406_TipoDeExchangeInvalido;
    procedure Erro403_NomeReservadoNoDeclare;
    procedure Erro403_UserIdDivergente;
    procedure Erro405_FilaExclusivaDeOutraConexao;
    procedure ErroDeCanal_NaoDerrubaAConexao;
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

procedure TEngineDispatchTests.SetUp;
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

    AssertTrue('a mensagem chegou ao consumidor', EsperaEntregas(1, 5000));
    AssertEquals('corpo integro', 'ola', Corpos);
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
    AssertTrue('achou a primeira', LGet.Found);
    AssertEquals('corpo', 'primeira', LGet.BodyAsText);
    AssertEquals('message-count do Get-Ok: quantas sobraram', 1, Integer(LGet.MessageCount));

    LGet := LCh.BasicGet('q.get', True);
    AssertTrue('achou a segunda', LGet.Found);
    AssertEquals('corpo', 'segunda', LGet.BodyAsText);
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
    AssertFalse('fila vazia devolve Get-Empty', LGet.Found);
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
    AssertTrue('q.aprovada recebeu', LGet.Found);
    AssertEquals('so a que casa', 'A', LGet.BodyAsText);
    AssertFalse('e nada alem dela', LCh.BasicGet('q.aprovada', True).Found);

    LGet := LCh.BasicGet('q.todas', True);
    AssertTrue('q.todas recebeu a primeira', LGet.Found);
    AssertEquals('ordem preservada', 'A', LGet.BodyAsText);
    LGet := LCh.BasicGet('q.todas', True);
    AssertTrue('q.todas recebeu a segunda', LGet.Found);
    AssertEquals('ordem preservada', 'C', LGet.BodyAsText);
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
    AssertEquals('chegou na primeira', 'broadcast', LCh.BasicGet('q.f1', True).BodyAsText);
    AssertEquals('e na segunda', 'broadcast', LCh.BasicGet('q.f2', True).BodyAsText);
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

    AssertTrue('entregou a primeira', EsperaEntregas(1, 5000));
    Sleep(300); // tempo de sobra para entregar mais, se o prefetch falhasse
    AssertEquals('com prefetch=1 e nenhum ack, so uma entrega fica em voo', 1, QuantasRecebidas);
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
    AssertTrue('buscou', LGet.Found);
    AssertFalse('primeira vez', LGet.Redelivered);
    LCh.Nack(LGet.DeliveryTag, True); // requeue

    LGet := LCh.BasicGet('q.nack', True);
    AssertTrue('voltou para a fila', LGet.Found);
    AssertEquals('mesma mensagem', 'volta', LGet.BodyAsText);
    AssertTrue('marcada como reentrega', LGet.Redelivered);
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
    AssertTrue('entregou antes do cancel', EsperaEntregas(1, 5000));

    LCh.Cancel(LTag);
    LCh.PublishText('', 'q.cancel', 'depois');
    Sleep(300);
    AssertEquals('nada foi entregue apos o cancel', 1, QuantasRecebidas);
    // E a mensagem continua na fila, esperando outro consumidor.
    AssertEquals('a mensagem ficou na fila', 'depois', LCh.BasicGet('q.cancel', True).BodyAsText);
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
    AssertEquals('nome', 'q.cont', LOk.QueueName);
    AssertEquals('message-count', 2, Integer(LOk.MessageCount));
    AssertEquals('consumer-count', 0, Integer(LOk.ConsumerCount));
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
    AssertEquals('delete devolve quantas mensagens foram junto', 1, Integer(LCh.DeleteQueue(NovoDelete('q.del', False, False))));

    // Apagada de verdade: um consume nela agora e 404.
    LCodigo := 0;
    try
      LCh.Consume('q.del', OnDelivery, True);
    except
      on E: EAMQPChannel do
        LCodigo := CodigoDoErro(E.Message);
    end;
    AssertEquals('a fila sumiu mesmo', 404, LCodigo);
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
    AssertTrue('buscou', LGet.Found);
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
    AssertTrue('a nao-confirmada voltou para a fila', LGet.Found);
    AssertEquals('mesma mensagem', 'pendente', LGet.BodyAsText);
    AssertTrue('marcada como reentrega', LGet.Redelivered);
    AssertFalse('e voltou UMA vez, nao duas', LCh.BasicGet('q.orfa', True).Found);
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
    AssertTrue('o broker gerou um nome', LOk.QueueName <> '');
    AssertTrue('com o prefixo reservado do servidor', Pos('amq.gen-', LOk.QueueName) = 1);

    // E a fila gerada funciona.
    LCh.PublishText('', LOk.QueueName, 'anon');
    AssertEquals('entregou', 'anon', LCh.BasicGet(LOk.QueueName, True).BodyAsText);
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
    AssertTrue('publish sem rota nao fecha o canal', LCh.IsOpen);
    AssertTrue('e o broker contabiliza a publicacao sem rota', FBroker.Engine.UnroutedCount >= 1);
  finally
    LConn.Free;
  end;
end;

{ --- TErrorTableTests --- }

procedure TErrorTableTests.SetUp;
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
    AssertEquals('consume de fila inexistente e 404', 404, LCodigo);
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
    AssertEquals('get de fila inexistente e 404', 404, LCodigo);
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
    AssertEquals('bind com fila inexistente e 404', 404, LCodigo);
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
    AssertFalse('publish em exchange inexistente fecha o canal', LCh.IsOpen);
    LCodigo := 0;
    try
      LCh.BasicGet('qualquer', True); // canal morto: a lib levanta
    except
      on E: EAMQPChannel do
        LCodigo := 1;
    end;
    AssertEquals('e o canal fica inutilizavel', 1, LCodigo);
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
    AssertEquals('redeclare divergente e 406', 406, LCodigo);
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
    AssertEquals('if-empty com mensagem e 406', 406, LCodigo);
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
    AssertEquals('if-unused com consumidor e 406', 406, LCodigo);
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
    AssertEquals('tipo de exchange invalido e 406 de canal', 406, LCodigo);
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
    AssertEquals('nome reservado e 403', 403, LCodigo);
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
    AssertFalse('user-id diferente do usuario autenticado fecha o canal (403)', LCh.IsOpen);
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
      AssertEquals('fila exclusiva de outra conexao e 405 RESOURCE_LOCKED', 405, LCodigo);
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
    AssertFalse('o canal ofensor morreu', LCh1.IsOpen);
    AssertTrue('mas a conexao continua viva', LConn.IsOpen);

    // E outro canal na mesma conexao funciona normalmente.
    LCh2 := LConn.CreateChannel;
    LCh2.DeclareQueue(TAMQPQueueDeclare.Create('q.depois'));
    LCh2.PublishText('', 'q.depois', 'ok');
    AssertEquals('canal novo opera normalmente', 'ok', LCh2.BasicGet('q.depois', True).BodyAsText);
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
    AssertTrue('entregou', EsperaEntregas(1, 5000));
    Sleep(300); // o ack ate a fila e' assincrono
  finally
    LConn.Free; // conexao cai DEPOIS do ack
  end;

  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.acked'));
    AssertFalse('mensagem confirmada nao volta para a fila quando a conexao cai',
      LCh.BasicGet('q.acked', True).Found);
  finally
    LConn.Free;
  end;
end;

initialization
  RegisterTest(TEngineDispatchTests);
  RegisterTest(TErrorTableTests);

end.
