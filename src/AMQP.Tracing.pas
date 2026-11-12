unit AMQP.Tracing;

{$I amqp.inc}

{ OpenTelemetry spans for publish and consume, on top of PascalCommon.Tracing
  from pascal-common-faa (phase D of docs/observability-design.md in
  pascal-api-infra-faa). Rationale in docs/tracing.md.

  The channel itself opens the spans; the application only turns the tracer on
  with TPcTracing.Start. This unit holds the pieces: names, attributes and reading
  the context of a received message.

  - Publish: producer span, child of the thread's current span (typically the
    server span of an HTTP request). Its 'traceparent' (and the
    'tracestate') goes in the message headers — injected at ENCODING time, the
    caller's table is never changed; if it already has a 'traceparent', the
    application's wins. The span ends when the frames go out: the confirm arrives
    later, on another thread, and a current span only ends on the thread that
    opened it (PascalCommon.Tracing rule).
  - Consume: consumer span 'process <queue>' around the callback, child of the
    message's 'traceparent' (StartSpanFromParent): the processing joins the
    producer's trace. Without a valid traceparent, it is the root of a new trace. It
    is born and ends in the work item, on the pool thread, and is the current span
    during the callback — a publish made inside it is its child.
  - Basic.Get: client span 'receive <queue>', child of the current span (the pull
    is part of the caller's work). Whoever wants to process the obtained message
    in the producer's trace opens AmqpStartProcessSpan with its
    properties.
  - Disabled costs nothing: with TPcTracing.Enabled false no function
    here creates a span, ids or attributes, and the publish injects no header.
  - A broker-generated queue ('amq.gen-...') becomes '(temporary)' in the span
    NAME (cardinality) and stays whole in messaging.destination.name.
  - Outside the trace: ack/nack/reject, Basic.Return, confirms, declares and the
    resend of unconfirmed messages on reconnection (the resent bytes already
    carry the original traceparent). }

interface

uses
  SysUtils,
  AMQP.Basic.Methods,
  PascalCommon.Tracing;

/// The 'traceparent' (and the 'tracestate', '' when absent) from the message
/// headers. False when there are no headers or the traceparent is not a string. Does
/// not validate the format: StartSpanFromParent treats an invalid one as absent.
function AmqpTraceParentOf(const AProps: TAMQPBasicProperties;
  out ATraceParent, ATraceState: string): Boolean;

/// Opens the span of a publish (kind producer), which becomes the thread's
/// current span. nil when the tracer is off.
function AmqpStartPublishSpan(const AExchange, ARoutingKey: string;
  ABodySize: Int64; const AProps: TAMQPBasicProperties;
  const AServerAddress: string = ''; AServerPort: Integer = 0): IPcSpan;

/// Opens the span for processing a message (kind consumer), child of its
/// traceparent, which becomes the thread's current span. It is what the channel
/// opens around the Consume callback; public for whoever processes the
/// result of a BasicGet. ABodySize < 0 omits the size. nil when the
/// tracer is off.
function AmqpStartProcessSpan(const AQueue: string;
  const AProps: TAMQPBasicProperties; const ARoutingKey: string = '';
  ABodySize: Int64 = -1; ADeliveryTag: UInt64 = 0; ARedelivered: Boolean = False;
  const AServerAddress: string = ''; AServerPort: Integer = 0): IPcSpan;

/// Opens the span of a Basic.Get (kind client), child of the current span. nil
/// when the tracer is off.
function AmqpStartReceiveSpan(const AQueue: string;
  const AServerAddress: string = ''; AServerPort: Integer = 0): IPcSpan;

/// Completes the span of a Basic.Get with what came back: AFound false sets
/// pascalamqp.get.empty. A nil ASpan is a no-op.
procedure AmqpSetReceiveResult(const ASpan: IPcSpan; AFound: Boolean;
  const AProps: TAMQPBasicProperties; const ARoutingKey: string;
  ABodySize: Int64; ADeliveryTag: UInt64; ARedelivered: Boolean);

/// Ends the span; with AError, error status with the message and error.type with
/// the class. A nil ASpan is a no-op.
procedure AmqpFinishSpan(const ASpan: IPcSpan; AError: Exception);

/// The destination name that goes in the span name: '(temporary)' for a
/// broker-generated queue, 'amq.default' for the default exchange, the name itself
/// otherwise.
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
  // Local variable before the accessor: AsString chained on the indexer causes
  // an internal error in FPC 3.2.2 (see CLAUDE.md).
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
  // The real name (empty for the default exchange) is what the protocol carried.
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
