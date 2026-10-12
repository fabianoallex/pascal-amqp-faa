unit AMQP.Tracing;

{$I amqp.inc}

{ Spans do OpenTelemetry para publish e consumo, sobre o PascalCommon.Tracing
  da pascal-common-faa (fase D do docs/observability-design.md da
  pascal-api-infra-faa). Racional em docs/tracing.md.

  Quem abre os spans é o próprio canal; a aplicação só liga o tracer com
  TPcTracing.Start. Esta unit tem as peças: nomes, atributos e a leitura do
  contexto de uma mensagem recebida.

  - Publish: span producer, filho do span corrente da thread (o de servidor
    de uma requisição HTTP, tipicamente). O 'traceparent' dele (e o
    'tracestate') vai nos headers da mensagem — injetado na CODIFICAÇÃO, a
    tabela do chamador nunca é alterada; se ela já tem 'traceparent', o da
    aplicação vence. O span termina quando os frames saem: o confirm chega
    depois, noutra thread, e um span corrente só termina na thread que o
    abriu (regra do PascalCommon.Tracing).
  - Consumo: span consumer 'process <fila>' em volta do callback, filho do
    'traceparent' da mensagem (StartSpanFromParent): o processamento entra na
    trace do produtor. Sem traceparent válido, é raiz de trace nova. Nasce e
    termina no work item, na thread do pool, e é o span corrente durante o
    callback — um publish feito ali dentro é filho dele.
  - Basic.Get: span client 'receive <fila>', filho do span corrente (o pull
    faz parte do trabalho de quem chamou). Quem quer processar a mensagem
    obtida na trace do produtor abre AmqpStartProcessSpan com as
    propriedades dela.
  - Desligado não custa nada: com TPcTracing.Enabled falso nenhuma função
    daqui cria span, ids ou atributos, e o publish não injeta header.
  - Fila gerada pelo broker ('amq.gen-...') vira '(temporary)' no NOME do
    span (cardinalidade) e fica inteira em messaging.destination.name.
  - Fora da trace: ack/nack/reject, Basic.Return, confirms, declares e o
    reenvio de não confirmadas na reconexão (os bytes reenviados já levam o
    traceparent original). }

interface

uses
  SysUtils,
  AMQP.Basic.Methods,
  PascalCommon.Tracing;

/// O 'traceparent' (e o 'tracestate', '' quando ausente) dos headers da
/// mensagem. False quando não há headers ou o traceparent não é string. Não
/// valida o formato: StartSpanFromParent trata o inválido como ausente.
function AmqpTraceParentOf(const AProps: TAMQPBasicProperties;
  out ATraceParent, ATraceState: string): Boolean;

/// Abre o span de um publish (kind producer), que vira o span corrente da
/// thread. nil quando o tracer está desligado.
function AmqpStartPublishSpan(const AExchange, ARoutingKey: string;
  ABodySize: Int64; const AProps: TAMQPBasicProperties;
  const AServerAddress: string = ''; AServerPort: Integer = 0): IPcSpan;

/// Abre o span do processamento de uma mensagem (kind consumer), filho do
/// traceparent dela, que vira o span corrente da thread. É o que o canal
/// abre em volta do callback do Consume; público para quem processa o
/// resultado de um BasicGet. ABodySize < 0 omite o tamanho. nil quando o
/// tracer está desligado.
function AmqpStartProcessSpan(const AQueue: string;
  const AProps: TAMQPBasicProperties; const ARoutingKey: string = '';
  ABodySize: Int64 = -1; ADeliveryTag: UInt64 = 0; ARedelivered: Boolean = False;
  const AServerAddress: string = ''; AServerPort: Integer = 0): IPcSpan;

/// Abre o span de um Basic.Get (kind client), filho do span corrente. nil
/// quando o tracer está desligado.
function AmqpStartReceiveSpan(const AQueue: string;
  const AServerAddress: string = ''; AServerPort: Integer = 0): IPcSpan;

/// Completa o span de um Basic.Get com o que veio: AFound falso marca
/// pascalamqp.get.empty. ASpan nil é no-op.
procedure AmqpSetReceiveResult(const ASpan: IPcSpan; AFound: Boolean;
  const AProps: TAMQPBasicProperties; const ARoutingKey: string;
  ABodySize: Int64; ADeliveryTag: UInt64; ARedelivered: Boolean);

/// Termina o span; com AError, status de erro com a mensagem e error.type com
/// a classe. ASpan nil é no-op.
procedure AmqpFinishSpan(const ASpan: IPcSpan; AError: Exception);

/// O nome de destino que vai no nome do span: '(temporary)' para fila gerada
/// pelo broker, 'amq.default' para o exchange padrão, o próprio nome nos
/// demais.
function AmqpSpanDestination(const AName: string; AIsExchange: Boolean): string;

implementation

uses
  TypInfo,
  Rtti,
  AMQP.Wire;

const
  AMQP_TEMPORARY_PREFIX = 'amq.gen-';

function HeaderText(ATable: TAMQPFieldTable; const AName: string;
  out AText: string): Boolean;
var
  LValue: TValue;
begin
  AText := '';
  Result := False;
  if not ATable.TryGetValue(AName, LValue) then
    Exit;
  // Variável local antes do accessor: AsString encadeado no indexador dá
  // erro interno no FPC 3.2.2 (ver CLAUDE.md).
  if LValue.Kind in [tkString, tkUString, tkLString, tkWString
    {$IFDEF FPC}, tkAString{$ENDIF}] then
  begin
    AText := LValue.AsString;
    Result := True;
  end;
end;

function AmqpTraceParentOf(const AProps: TAMQPBasicProperties;
  out ATraceParent, ATraceState: string): Boolean;
begin
  ATraceParent := '';
  ATraceState := '';
  Result := False;
  if not (bpHeaders in AProps.Flags) or not Assigned(AProps.Headers) then
    Exit;
  Result := HeaderText(AProps.Headers, 'traceparent', ATraceParent);
  if Result then
    HeaderText(AProps.Headers, 'tracestate', ATraceState);
end;

function AmqpSpanDestination(const AName: string; AIsExchange: Boolean): string;
begin
  if AIsExchange and (AName = '') then
    Result := 'amq.default'
  else if Copy(AName, 1, Length(AMQP_TEMPORARY_PREFIX)) = AMQP_TEMPORARY_PREFIX then
    Result := '(temporary)'
  else
    Result := AName;
end;

procedure SetCommon(const ASpan: IPcSpan; const AOperationType, AOperationName,
  ADestination: string; const AServerAddress: string; AServerPort: Integer);
begin
  ASpan.SetAttribute('messaging.system', 'rabbitmq');
  ASpan.SetAttribute('messaging.operation.type', AOperationType);
  ASpan.SetAttribute('messaging.operation.name', AOperationName);
  ASpan.SetAttribute('messaging.destination.name', ADestination);
  if Copy(ADestination, 1, Length(AMQP_TEMPORARY_PREFIX)) = AMQP_TEMPORARY_PREFIX then
    ASpan.SetBoolAttribute('messaging.destination.temporary', True);
  if AServerAddress <> '' then
    ASpan.SetAttribute('server.address', AServerAddress);
  if AServerPort > 0 then
    ASpan.SetIntAttribute('server.port', AServerPort);
end;

procedure SetMessage(const ASpan: IPcSpan; const AProps: TAMQPBasicProperties;
  const ARoutingKey: string; ABodySize: Int64);
begin
  if ARoutingKey <> '' then
    ASpan.SetAttribute('messaging.rabbitmq.destination.routing_key', ARoutingKey);
  if ABodySize >= 0 then
    ASpan.SetIntAttribute('messaging.message.body.size', ABodySize);
  if (bpMessageId in AProps.Flags) and (AProps.MessageId <> '') then
    ASpan.SetAttribute('messaging.message.id', AProps.MessageId);
  if (bpCorrelationId in AProps.Flags) and (AProps.CorrelationId <> '') then
    ASpan.SetAttribute('messaging.message.conversation_id', AProps.CorrelationId);
end;

procedure SetDelivery(const ASpan: IPcSpan; ADeliveryTag: UInt64;
  ARedelivered: Boolean);
begin
  if ADeliveryTag > 0 then
    ASpan.SetIntAttribute('messaging.rabbitmq.message.delivery_tag', Int64(ADeliveryTag));
  if ARedelivered then
    ASpan.SetBoolAttribute('pascalamqp.message.redelivered', True);
end;

function AmqpStartPublishSpan(const AExchange, ARoutingKey: string;
  ABodySize: Int64; const AProps: TAMQPBasicProperties;
  const AServerAddress: string; AServerPort: Integer): IPcSpan;
var
  LDestination: string;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  LDestination := AmqpSpanDestination(AExchange, True);
  Result := TPcTracing.StartSpan('publish ' + LDestination, skProducer);
  // O nome real (vazio para o exchange padrão) é o que o protocolo levou.
  SetCommon(Result, 'send', 'publish', AExchange, AServerAddress, AServerPort);
  SetMessage(Result, AProps, ARoutingKey, ABodySize);
end;

function AmqpStartProcessSpan(const AQueue: string;
  const AProps: TAMQPBasicProperties; const ARoutingKey: string;
  ABodySize: Int64; ADeliveryTag: UInt64; ARedelivered: Boolean;
  const AServerAddress: string; AServerPort: Integer): IPcSpan;
var
  LParent, LState: string;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  AmqpTraceParentOf(AProps, LParent, LState);
  Result := TPcTracing.StartSpanFromParent(LParent, LState,
    'process ' + AmqpSpanDestination(AQueue, False), skConsumer);
  SetCommon(Result, 'process', 'process', AQueue, AServerAddress, AServerPort);
  SetMessage(Result, AProps, ARoutingKey, ABodySize);
  SetDelivery(Result, ADeliveryTag, ARedelivered);
end;

function AmqpStartReceiveSpan(const AQueue: string;
  const AServerAddress: string; AServerPort: Integer): IPcSpan;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  Result := TPcTracing.StartSpan('receive ' + AmqpSpanDestination(AQueue, False),
    skClient);
  SetCommon(Result, 'receive', 'receive', AQueue, AServerAddress, AServerPort);
end;

procedure AmqpSetReceiveResult(const ASpan: IPcSpan; AFound: Boolean;
  const AProps: TAMQPBasicProperties; const ARoutingKey: string;
  ABodySize: Int64; ADeliveryTag: UInt64; ARedelivered: Boolean);
begin
  if ASpan = nil then
    Exit;
  if not AFound then
  begin
    ASpan.SetBoolAttribute('pascalamqp.get.empty', True);
    Exit;
  end;
  SetMessage(ASpan, AProps, ARoutingKey, ABodySize);
  SetDelivery(ASpan, ADeliveryTag, ARedelivered);
end;

procedure AmqpFinishSpan(const ASpan: IPcSpan; AError: Exception);
begin
  if ASpan = nil then
    Exit;
  if AError <> nil then
  begin
    ASpan.SetAttribute('error.type', AError.ClassName);
    ASpan.SetStatus(ssError, AError.Message);
  end;
  ASpan.Finish;
end;

end.
