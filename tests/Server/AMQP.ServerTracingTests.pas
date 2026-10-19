unit AMQP.ServerTracingTests;

{ Spans do cliente ponta a ponta (fase D do design de observabilidade da
  pascal-api-infra-faa; ver docs/tracing.md): o CLIENTE REAL contra o broker
  embutido, com o tracer da pascal-common-faa ligado e um exportador que so'
  guarda os spans.

  O broker NAO abre span (decisao do usuario, 2026-10-12): ele repassa o
  content header byte a byte (D1), entao o traceparent atravessa intacto --
  inclusive no dead-letter, que reescreve o header (x-death) e tem de manter
  o resto.

  O tracer e' estado global do processo: cada teste liga no SetUp e desliga
  no TearDown, com AAutoFlush = False (o teste exporta com FlushNow; nenhuma
  thread de export). O span 'process' termina numa thread do pool DEPOIS do
  callback voltar, entao o teste espera pelo span (EsperaSpan), nunca pelo
  callback.

  Mestre DUnitX no dialeto do FPCUnit (TAssert.*); o espelho FPCUnit em
  fpc\ e' gerado por tools/gen_fpc_mirror.py. }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils,
  Classes,
  Rtti,
  SyncObjs,
  Generics.Collections,
  AMQP.Wire,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods,
  PascalCommon.Threading,
  PascalCommon.Tracing,
  AMQP.Connection,
  AMQP.Tracing,
  AMQP.Server.Broker;

type
  { Exportador que so' guarda: os spans chegam na ordem em que terminaram. }
  TColetorDeSpans = class(TInterfacedObject, IPcSpanExporter)
  private
    FLock: TCriticalSection;
    FSpans: TList<TPcSpanData>;
  public
    constructor Create;
    destructor Destroy; override;
    function ExportSpans(const ASpans: TPcSpanDataArray; out AError: string): Boolean;
    function Count: Integer;
    /// Os spans com esse nome, na ordem em que terminaram.
    function ComNome(const AName: string): TPcSpanDataArray;
  end;

  [TestFixture]
  TTracingTests = class
  private
    FBroker: TAMQPServer;
    FColetor: TColetorDeSpans;
    FColetorRef: IPcSpanExporter;
    FLock: TCriticalSection;
    FTraceParentVisto: string;
    FRecebidas: Integer;
    FRepassarPara: string;
    // o que o OnConsumerError viu como span corrente ('' = nenhum)
    FTraceNoHandler: string;
    FSpanNoHandler: string;
    FHandlerChamado: Integer;
    function Params: TAMQPConnectionParams;
    procedure AoEntregar(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure AoFalhar(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery;
      AError: Exception; var AAction: TAMQPConsumerErrorAction);
    /// Exporta e espera ACount spans com esse nome.
    function EsperaSpans(const AName: string; ACount, ATimeoutMs: Integer): Boolean;
    /// O AIndex-esimo span com esse nome (falha o teste se nao houver).
    function Span(const AName: string; AIndex: Integer = 0): TPcSpanData;
    /// O span com esse nome e esse valor de atributo (falha se nao houver).
    function SpanCom(const AName, AKey, AValue: string): TPcSpanData;
    function TraceParentVisto: string;
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure PublishEProcess_NaTraceDoSpanCorrente;
    [Test] procedure Consumidor_VeOTraceParentDoPublish;
    [Test] procedure TraceParentDaAplicacao_Vence;
    [Test] procedure PublishDentroDoCallback_EFilhoDoProcess;
    [Test] procedure CallbackQueLevanta_ProcessComErro;
    [Test] procedure OnConsumerError_RodaDentroDoProcess;
    [Test] procedure Get_SpanReceiveFilhoDoCorrente;
    [Test] procedure DeadLetter_PreservaOTraceParent;
    [Test] procedure FilaGerada_NomeTemporarioNoSpan;
    [Test] procedure TracerDesligado_NaoInjetaHeader;
  end;

implementation

const
  TP_DA_APLICACAO = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
  CORPO_QUE_LEVANTA = 'levanta';

{ --- helpers --- }

function NovoBroker: TAMQPServer;
begin
  Result := TAMQPServer.Create;
  Result.BindAddress := '127.0.0.1';
  Result.Port := 0;
  Result.Start;
end;

function Attr(const ASpan: TPcSpanData; const AKey: string): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(ASpan.Attributes) do
    if ASpan.Attributes[I].Key = AKey then
    begin
      case ASpan.Attributes[I].ValueType of
        satString: Result := ASpan.Attributes[I].StringValue;
        satInt: Result := IntToStr(ASpan.Attributes[I].IntValue);
        // BoolToStr com os textos so' existe no FPC.
        satBool:
          if ASpan.Attributes[I].BoolValue then
            Result := 'true'
          else
            Result := 'false';
      else
        Result := FloatToStr(ASpan.Attributes[I].DoubleValue);
      end;
      Exit;
    end;
end;

function TraceParentDe(const ASpan: TPcSpanData): string;
begin
  Result := '00-' + ASpan.TraceId + '-' + ASpan.SpanId + '-01';
end;

procedure LiberaHeaders(const AResult: TAMQPGetResult);
begin
  if bpHeaders in AResult.Properties.Flags then
    AResult.Properties.Headers.Free;
end;

{ --- TColetorDeSpans --- }

constructor TColetorDeSpans.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FSpans := TList<TPcSpanData>.Create;
end;

destructor TColetorDeSpans.Destroy;
begin
  FSpans.Free;
  FLock.Free;
  inherited;
end;

function TColetorDeSpans.ExportSpans(const ASpans: TPcSpanDataArray;
  out AError: string): Boolean;
var
  I: Integer;
begin
  AError := '';
  FLock.Enter;
  try
    for I := 0 to High(ASpans) do
      FSpans.Add(ASpans[I]);
  finally
    FLock.Leave;
  end;
  Result := True;
end;

function TColetorDeSpans.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FSpans.Count;
  finally
    FLock.Leave;
  end;
end;

function TColetorDeSpans.ComNome(const AName: string): TPcSpanDataArray;
var
  I: Integer;
begin
  Result := nil;
  FLock.Enter;
  try
    for I := 0 to FSpans.Count - 1 do
      if FSpans[I].Name = AName then
      begin
        SetLength(Result, Length(Result) + 1);
        Result[High(Result)] := FSpans[I];
      end;
  finally
    FLock.Leave;
  end;
end;

{ --- TTracingTests --- }

procedure TTracingTests.SetUp;
begin
  FBroker := NovoBroker;
  FLock := TCriticalSection.Create;
  FTraceParentVisto := '';
  FRecebidas := 0;
  FRepassarPara := '';
  FTraceNoHandler := '';
  FSpanNoHandler := '';
  FHandlerChamado := 0;
  FColetor := TColetorDeSpans.Create;
  FColetorRef := FColetor;
  TPcTracing.Start(TPcTracingOptions.Default('amqp-tests', '0'), FColetorRef, False);
end;

procedure TTracingTests.TearDown;
begin
  TPcTracing.Shutdown;
  FBroker.Free;
  FColetorRef := nil;
  FLock.Free;
end;

function TTracingTests.Params: TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := '127.0.0.1';
  Result.Port := FBroker.Port;
end;

procedure TTracingTests.AoEntregar(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LParent, LState: string;
begin
  AmqpTraceParentOf(ADelivery.Properties, LParent, LState);
  FLock.Enter;
  try
    FTraceParentVisto := LParent;
    Inc(FRecebidas);
  finally
    FLock.Leave;
  end;
  // Repassa o que chegou de outra fila (o que chega na de destino para aqui).
  if (FRepassarPara <> '') and (ADelivery.RoutingKey <> FRepassarPara) then
    AChannel.PublishText('', FRepassarPara, 'repassada');
  if ADelivery.BodyAsText = CORPO_QUE_LEVANTA then
    raise Exception.Create('falhou de proposito');
end;

procedure TTracingTests.AoFalhar(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery; AError: Exception;
  var AAction: TAMQPConsumerErrorAction);
var
  LCorrente: IPcSpan;
begin
  // O que um log do aplicativo enxergaria aqui: o span corrente da thread.
  LCorrente := TPcTracing.Current;
  FLock.Enter;
  try
    if LCorrente <> nil then
    begin
      FTraceNoHandler := LCorrente.TraceId;
      FSpanNoHandler := LCorrente.SpanId;
    end;
  finally
    FLock.Leave;
  end;
  PcAtomicInc(FHandlerChamado);
end;

function TTracingTests.TraceParentVisto: string;
begin
  FLock.Enter;
  try
    Result := FTraceParentVisto;
  finally
    FLock.Leave;
  end;
end;

function TTracingTests.EsperaSpans(const AName: string; ACount,
  ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  repeat
    TPcTracing.FlushNow;
    Result := Length(FColetor.ComNome(AName)) >= ACount;
    if Result or (PcTickMs >= LDeadline) then
      Exit;
    Sleep(10);
  until False;
end;

function TTracingTests.Span(const AName: string; AIndex: Integer): TPcSpanData;
var
  LSpans: TPcSpanDataArray;
begin
  LSpans := FColetor.ComNome(AName);
  if AIndex > High(LSpans) then
    TAssert.Fail('sem o span ' + AName + ' #' + IntToStr(AIndex));
  Result := LSpans[AIndex];
end;

function TTracingTests.SpanCom(const AName, AKey, AValue: string): TPcSpanData;
var
  LSpans: TPcSpanDataArray;
  I: Integer;
begin
  LSpans := FColetor.ComNome(AName);
  for I := 0 to High(LSpans) do
    if Attr(LSpans[I], AKey) = AValue then
      Exit(LSpans[I]);
  TAssert.Fail('sem o span ' + AName + ' com ' + AKey + '=' + AValue);
end;

procedure TTracingTests.PublishEProcess_NaTraceDoSpanCorrente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LPai: IPcSpan;
  LTraceId, LPaiId: string;
  LPub, LProc: TPcSpanData;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.trace', False));
    LCh.Consume('q.trace', AoEntregar, True);

    LPai := TPcTracing.StartSpan('GET /pedidos', skServer);
    LTraceId := LPai.TraceId;
    LPaiId := LPai.SpanId;
    try
      LCh.PublishText('', 'q.trace', 'oi');
    finally
      LPai.Finish;
      LPai := nil;
    end;

    TAssert.AssertTrue('o process terminou', EsperaSpans('process q.trace', 1, 5000));
    LPub := Span('publish amq.default');
    LProc := Span('process q.trace');

    TAssert.AssertEquals('publish na trace do pai', LTraceId, LPub.TraceId);
    TAssert.AssertEquals('publish filho do pai', LPaiId, LPub.ParentSpanId);
    TAssert.AssertTrue('publish e producer', LPub.Kind = skProducer);
    TAssert.AssertEquals('process na mesma trace', LTraceId, LProc.TraceId);
    TAssert.AssertEquals('process filho do publish', LPub.SpanId, LProc.ParentSpanId);
    TAssert.AssertTrue('process e consumer', LProc.Kind = skConsumer);

    TAssert.AssertEquals('sistema', 'rabbitmq', Attr(LPub, 'messaging.system'));
    TAssert.AssertEquals('operacao do publish', 'send', Attr(LPub, 'messaging.operation.type'));
    TAssert.AssertEquals('routing key', 'q.trace',
      Attr(LPub, 'messaging.rabbitmq.destination.routing_key'));
    TAssert.AssertEquals('tamanho', '2', Attr(LPub, 'messaging.message.body.size'));
    TAssert.AssertEquals('porta', IntToStr(FBroker.Port), Attr(LPub, 'server.port'));
    TAssert.AssertEquals('operacao do process', 'process', Attr(LProc, 'messaging.operation.type'));
    TAssert.AssertEquals('fila', 'q.trace', Attr(LProc, 'messaging.destination.name'));
    TAssert.AssertEquals('delivery tag', '1', Attr(LProc, 'messaging.rabbitmq.message.delivery_tag'));
    TAssert.AssertTrue('sem erro', LProc.Status <> ssError);
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.Consumidor_VeOTraceParentDoPublish;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.visto', False));
    LCh.Consume('q.visto', AoEntregar, True);
    LCh.PublishText('', 'q.visto', 'oi');

    TAssert.AssertTrue('o process terminou', EsperaSpans('process q.visto', 1, 5000));
    TAssert.AssertEquals('o header traz o contexto do publish',
      TraceParentDe(Span('publish amq.default')), TraceParentVisto);
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.TraceParentDaAplicacao_Vence;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LProps: TAMQPBasicProperties;
  LTabela: TAMQPFieldTable;
  LValue: TValue;
  LProc: TPcSpanData;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.app', False));
    LCh.Consume('q.app', AoEntregar, True);

    LTabela := TAMQPFieldTable.Create;
    try
      LValue := TValue.From<string>(TP_DA_APLICACAO);
      LTabela.Put('traceparent', LValue);
      LProps := TAMQPBasicProperties.Empty;
      LProps.SetHeaders(LTabela);
      LCh.Publish('', 'q.app', AmqpUtf8Encode('oi'), LProps);
      TAssert.AssertEquals('a tabela do chamador nao mudou', 1, Integer(LTabela.Count));
    finally
      LTabela.Free;
    end;

    TAssert.AssertTrue('o process terminou', EsperaSpans('process q.app', 1, 5000));
    LProc := Span('process q.app');
    TAssert.AssertEquals('chegou o da aplicacao', TP_DA_APLICACAO, TraceParentVisto);
    TAssert.AssertEquals('process na trace da aplicacao',
      '4bf92f3577b34da6a3ce929d0e0e4736', LProc.TraceId);
    TAssert.AssertEquals('process filho do span da aplicacao', '00f067aa0ba902b7',
      LProc.ParentSpanId);
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.PublishDentroDoCallback_EFilhoDoProcess;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LProcA, LProcB, LPub2: TPcSpanData;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.a', False));
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.b', False));
    FRepassarPara := 'q.b';
    LCh.Consume('q.a', AoEntregar, True);
    LCh.Consume('q.b', AoEntregar, True);
    LCh.PublishText('', 'q.a', 'oi');

    TAssert.AssertTrue('o process do repasse terminou', EsperaSpans('process q.b', 1, 5000));
    LProcA := Span('process q.a');
    LPub2 := SpanCom('publish amq.default',
      'messaging.rabbitmq.destination.routing_key', 'q.b');
    LProcB := Span('process q.b');
    TAssert.AssertEquals('publish filho do process', LProcA.SpanId, LPub2.ParentSpanId);
    TAssert.AssertEquals('process do repasse filho dele', LPub2.SpanId, LProcB.ParentSpanId);
    TAssert.AssertEquals('uma trace so', LProcA.TraceId, LProcB.TraceId);
  finally
    FRepassarPara := '';
    LConn.Free;
  end;
end;

procedure TTracingTests.CallbackQueLevanta_ProcessComErro;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LProc: TPcSpanData;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.erro', False));
    LCh.Consume('q.erro', AoEntregar, True);
    LCh.PublishText('', 'q.erro', CORPO_QUE_LEVANTA);

    TAssert.AssertTrue('o process terminou', EsperaSpans('process q.erro', 1, 5000));
    LProc := Span('process q.erro');
    TAssert.AssertTrue('status de erro', LProc.Status = ssError);
    TAssert.AssertEquals('mensagem', 'falhou de proposito', LProc.StatusMessage);
    TAssert.AssertEquals('classe', 'Exception', Attr(LProc, 'error.type'));
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.OnConsumerError_RodaDentroDoProcess;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LProc: TPcSpanData;
begin
  // O handler de erro loga com o trace id da mensagem: o 'process' segue
  // corrente durante ele e só termina depois, ainda marcado com o erro.
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.OnConsumerError := AoFalhar;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.handler', False));
    LCh.Consume('q.handler', AoEntregar, True);
    LCh.PublishText('', 'q.handler', CORPO_QUE_LEVANTA);

    TAssert.AssertTrue('o process terminou', EsperaSpans('process q.handler', 1, 5000));
    TAssert.AssertEquals('o handler rodou', 1, PcAtomicGet(FHandlerChamado));
    LProc := Span('process q.handler');
    FLock.Enter;
    try
      TAssert.AssertEquals('no handler, a trace corrente e a da mensagem', LProc.TraceId,
        FTraceNoHandler);
      TAssert.AssertEquals('no handler, o span corrente e o process', LProc.SpanId,
        FSpanNoHandler);
    finally
      FLock.Leave;
    end;
    TAssert.AssertTrue('status de erro', LProc.Status = ssError);
    TAssert.AssertEquals('mensagem', 'falhou de proposito', LProc.StatusMessage);
    TAssert.AssertEquals('classe', 'Exception', Attr(LProc, 'error.type'));
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.Get_SpanReceiveFilhoDoCorrente;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LPai: IPcSpan;
  LPaiId: string;
  LGet: TAMQPGetResult;
  LCheio, LVazio: TPcSpanData;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.get', False));
    LCh.PublishText('', 'q.get', 'abc');

    LPai := TPcTracing.StartSpan('job', skInternal);
    LPaiId := LPai.SpanId;
    try
      LGet := LCh.BasicGet('q.get', True);
      LiberaHeaders(LGet);
      TAssert.AssertTrue('achou', LGet.Found);
      LGet := LCh.BasicGet('q.get', True);
      TAssert.AssertFalse('vazia', LGet.Found);
    finally
      LPai.Finish;
      LPai := nil;
    end;

    TAssert.AssertTrue('os dois receive', EsperaSpans('receive q.get', 2, 5000));
    LCheio := Span('receive q.get', 0);
    LVazio := Span('receive q.get', 1);
    TAssert.AssertTrue('client', LCheio.Kind = skClient);
    TAssert.AssertEquals('filho do corrente', LPaiId, LCheio.ParentSpanId);
    TAssert.AssertEquals('operacao', 'receive', Attr(LCheio, 'messaging.operation.type'));
    TAssert.AssertEquals('tamanho', '3', Attr(LCheio, 'messaging.message.body.size'));
    TAssert.AssertEquals('nao vazio', '', Attr(LCheio, 'pascalamqp.get.empty'));
    TAssert.AssertEquals('vazio marcado', 'true', Attr(LVazio, 'pascalamqp.get.empty'));
    TAssert.AssertEquals('vazio sem tamanho', '', Attr(LVazio, 'messaging.message.body.size'));
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.DeadLetter_PreservaOTraceParent;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LArgs: TAMQPFieldTable;
  LVal: TValue;
  LGet: TAMQPGetResult;
  LParent, LState: string;
  LDeadline: UInt64;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.dlq', False));
    LArgs := TAMQPFieldTable.Create;
    try
      LVal := TValue.From<string>('');
      LArgs.Put('x-dead-letter-exchange', LVal);
      LVal := TValue.From<string>('q.dlq');
      LArgs.Put('x-dead-letter-routing-key', LVal);
      LDecl := TAMQPQueueDeclare.Create('q.origem', False);
      LDecl.Arguments := LArgs;
      LCh.DeclareQueue(LDecl);
    finally
      LArgs.Free;
    end;
    LCh.PublishText('', 'q.origem', 'rejeitada');

    LGet := LCh.BasicGet('q.origem', False);
    LiberaHeaders(LGet);
    TAssert.AssertTrue('achou na origem', LGet.Found);
    LCh.Nack(LGet.DeliveryTag, False);

    // O dead-letter passa pelo ator da fila: assincrono.
    LDeadline := PcTickMs + 5000;
    repeat
      LGet := LCh.BasicGet('q.dlq', True);
      if LGet.Found or (PcTickMs >= LDeadline) then
        Break;
      Sleep(10);
    until False;
    try
      TAssert.AssertTrue('chegou na dlq', LGet.Found);
      TAssert.AssertTrue('com traceparent', AmqpTraceParentOf(LGet.Properties, LParent, LState));
      TAssert.AssertTrue('e o x-death do dead-letter',
        LGet.Properties.Headers.ContainsKey('x-death'));
    finally
      LiberaHeaders(LGet);
    end;
    TAssert.AssertTrue('o publish exportado', EsperaSpans('publish amq.default', 1, 5000));
    TAssert.AssertEquals('o mesmo do publish original',
      TraceParentDe(Span('publish amq.default')), LParent);
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.FilaGerada_NomeTemporarioNoSpan;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LOk: TAMQPQueueDeclareOk;
  LDecl: TAMQPQueueDeclare;
  LProc: TPcSpanData;
begin
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LDecl := TAMQPQueueDeclare.Create('', False);
    LDecl.Exclusive := True;
    LOk := LCh.DeclareQueue(LDecl);
    LCh.Consume(LOk.QueueName, AoEntregar, True);
    LCh.PublishText('', LOk.QueueName, 'oi');

    TAssert.AssertTrue('o process terminou', EsperaSpans('process (temporary)', 1, 5000));
    LProc := Span('process (temporary)');
    TAssert.AssertEquals('o nome real no atributo', LOk.QueueName,
      Attr(LProc, 'messaging.destination.name'));
    TAssert.AssertEquals('marcada temporaria', 'true',
      Attr(LProc, 'messaging.destination.temporary'));
  finally
    LConn.Free;
  end;
end;

procedure TTracingTests.TracerDesligado_NaoInjetaHeader;
var
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
  LGet: TAMQPGetResult;
begin
  TPcTracing.Shutdown;
  LConn := TAMQPConnection.Create(Params);
  try
    LConn.Open;
    LCh := LConn.CreateChannel;
    LCh.DeclareQueue(TAMQPQueueDeclare.Create('q.off', False));
    LCh.PublishText('', 'q.off', 'oi');
    LGet := LCh.BasicGet('q.off', True);
    try
      TAssert.AssertTrue('achou', LGet.Found);
      TAssert.AssertFalse('sem headers', bpHeaders in LGet.Properties.Flags);
    finally
      LiberaHeaders(LGet);
    end;
    TAssert.AssertEquals('nenhum span', 0, FColetor.Count);
  finally
    LConn.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TTracingTests);

end.
