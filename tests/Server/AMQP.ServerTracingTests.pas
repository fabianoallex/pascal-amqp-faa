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

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerTracingTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.Rtti,
  System.SyncObjs,
  System.Generics.Collections,
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
    [Setup]    procedure Setup;
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

procedure TTracingTests.Setup;
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
    Assert.Fail('sem o span ' + AName + ' #' + IntToStr(AIndex));
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
  Assert.Fail('sem o span ' + AName + ' com ' + AKey + '=' + AValue);
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

    Assert.IsTrue(EsperaSpans('process q.trace', 1, 5000), 'o process terminou');
    LPub := Span('publish amq.default');
    LProc := Span('process q.trace');

    Assert.AreEqual(LTraceId, LPub.TraceId, 'publish na trace do pai');
    Assert.AreEqual(LPaiId, LPub.ParentSpanId, 'publish filho do pai');
    Assert.IsTrue(LPub.Kind = skProducer, 'publish e producer');
    Assert.AreEqual(LTraceId, LProc.TraceId, 'process na mesma trace');
    Assert.AreEqual(LPub.SpanId, LProc.ParentSpanId, 'process filho do publish');
    Assert.IsTrue(LProc.Kind = skConsumer, 'process e consumer');

    Assert.AreEqual('rabbitmq', Attr(LPub, 'messaging.system'), 'sistema');
    Assert.AreEqual('send', Attr(LPub, 'messaging.operation.type'), 'operacao do publish');
    Assert.AreEqual('q.trace', Attr(LPub, 'messaging.rabbitmq.destination.routing_key'), 'routing key');
    Assert.AreEqual('2', Attr(LPub, 'messaging.message.body.size'), 'tamanho');
    Assert.AreEqual(IntToStr(FBroker.Port), Attr(LPub, 'server.port'), 'porta');
    Assert.AreEqual('process', Attr(LProc, 'messaging.operation.type'), 'operacao do process');
    Assert.AreEqual('q.trace', Attr(LProc, 'messaging.destination.name'), 'fila');
    Assert.AreEqual('1', Attr(LProc, 'messaging.rabbitmq.message.delivery_tag'), 'delivery tag');
    Assert.IsTrue(LProc.Status <> ssError, 'sem erro');
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

    Assert.IsTrue(EsperaSpans('process q.visto', 1, 5000), 'o process terminou');
    Assert.AreEqual(TraceParentDe(Span('publish amq.default')), TraceParentVisto, 'o header traz o contexto do publish');
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
      Assert.AreEqual(1, Integer(LTabela.Count), 'a tabela do chamador nao mudou');
    finally
      LTabela.Free;
    end;

    Assert.IsTrue(EsperaSpans('process q.app', 1, 5000), 'o process terminou');
    LProc := Span('process q.app');
    Assert.AreEqual(TP_DA_APLICACAO, TraceParentVisto, 'chegou o da aplicacao');
    Assert.AreEqual('4bf92f3577b34da6a3ce929d0e0e4736', LProc.TraceId, 'process na trace da aplicacao');
    Assert.AreEqual('00f067aa0ba902b7', LProc.ParentSpanId, 'process filho do span da aplicacao');
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

    Assert.IsTrue(EsperaSpans('process q.b', 1, 5000), 'o process do repasse terminou');
    LProcA := Span('process q.a');
    LPub2 := SpanCom('publish amq.default',
      'messaging.rabbitmq.destination.routing_key', 'q.b');
    LProcB := Span('process q.b');
    Assert.AreEqual(LProcA.SpanId, LPub2.ParentSpanId, 'publish filho do process');
    Assert.AreEqual(LPub2.SpanId, LProcB.ParentSpanId, 'process do repasse filho dele');
    Assert.AreEqual(LProcA.TraceId, LProcB.TraceId, 'uma trace so');
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

    Assert.IsTrue(EsperaSpans('process q.erro', 1, 5000), 'o process terminou');
    LProc := Span('process q.erro');
    Assert.IsTrue(LProc.Status = ssError, 'status de erro');
    Assert.AreEqual('falhou de proposito', LProc.StatusMessage, 'mensagem');
    Assert.AreEqual('Exception', Attr(LProc, 'error.type'), 'classe');
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

    Assert.IsTrue(EsperaSpans('process q.handler', 1, 5000), 'o process terminou');
    Assert.AreEqual(1, PcAtomicGet(FHandlerChamado), 'o handler rodou');
    LProc := Span('process q.handler');
    FLock.Enter;
    try
      Assert.AreEqual(LProc.TraceId, FTraceNoHandler,
        'no handler, a trace corrente e a da mensagem');
      Assert.AreEqual(LProc.SpanId, FSpanNoHandler,
        'no handler, o span corrente e o process');
    finally
      FLock.Leave;
    end;
    Assert.IsTrue(LProc.Status = ssError, 'status de erro');
    Assert.AreEqual('falhou de proposito', LProc.StatusMessage, 'mensagem');
    Assert.AreEqual('Exception', Attr(LProc, 'error.type'), 'classe');
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
      Assert.IsTrue(LGet.Found, 'achou');
      LGet := LCh.BasicGet('q.get', True);
      Assert.IsFalse(LGet.Found, 'vazia');
    finally
      LPai.Finish;
      LPai := nil;
    end;

    Assert.IsTrue(EsperaSpans('receive q.get', 2, 5000), 'os dois receive');
    LCheio := Span('receive q.get', 0);
    LVazio := Span('receive q.get', 1);
    Assert.IsTrue(LCheio.Kind = skClient, 'client');
    Assert.AreEqual(LPaiId, LCheio.ParentSpanId, 'filho do corrente');
    Assert.AreEqual('receive', Attr(LCheio, 'messaging.operation.type'), 'operacao');
    Assert.AreEqual('3', Attr(LCheio, 'messaging.message.body.size'), 'tamanho');
    Assert.AreEqual('', Attr(LCheio, 'pascalamqp.get.empty'), 'nao vazio');
    Assert.AreEqual('true', Attr(LVazio, 'pascalamqp.get.empty'), 'vazio marcado');
    Assert.AreEqual('', Attr(LVazio, 'messaging.message.body.size'), 'vazio sem tamanho');
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
    Assert.IsTrue(LGet.Found, 'achou na origem');
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
      Assert.IsTrue(LGet.Found, 'chegou na dlq');
      Assert.IsTrue(AmqpTraceParentOf(LGet.Properties, LParent, LState), 'com traceparent');
      Assert.IsTrue(LGet.Properties.Headers.ContainsKey('x-death'), 'e o x-death do dead-letter');
    finally
      LiberaHeaders(LGet);
    end;
    Assert.IsTrue(EsperaSpans('publish amq.default', 1, 5000), 'o publish exportado');
    Assert.AreEqual(TraceParentDe(Span('publish amq.default')), LParent, 'o mesmo do publish original');
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

    Assert.IsTrue(EsperaSpans('process (temporary)', 1, 5000), 'o process terminou');
    LProc := Span('process (temporary)');
    Assert.AreEqual(LOk.QueueName, Attr(LProc, 'messaging.destination.name'), 'o nome real no atributo');
    Assert.AreEqual('true', Attr(LProc, 'messaging.destination.temporary'), 'marcada temporaria');
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
      Assert.IsTrue(LGet.Found, 'achou');
      Assert.IsFalse(bpHeaders in LGet.Properties.Flags, 'sem headers');
    finally
      LiberaHeaders(LGet);
    end;
    Assert.AreEqual(0, FColetor.Count, 'nenhum span');
  finally
    LConn.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TTracingTests);

end.
