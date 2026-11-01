unit AMQP.Outbox;

{$I amqp.inc}

(* pascal-db-faa's transactional outbox (PascalDb.Outbox) through an AMQP
  broker: TOutboxAmqpSink publishes each event the relay hands it;
  TOutboxAmqpConsumer runs one IOutboxHandler on a queue of its own. A
  bridge, outside the packages: it needs pascal-db-faa (0.18.0 or later),
  which the client and the broker don't. Manual: docs/outbox.md.

  The message: the event's payload as the body (application/json,
  persistent), MessageId the event's id, the type property its kind, the
  timestamp its CreatedAt, the header "tenant-id" its tenant; the routing key
  is the kind, on a topic exchange. The trace context goes in the headers by
  this library itself (AMQP.Tracing): the relay's span is current when the
  sink publishes, and the consumer's "process" span is the message's child,
  so the producer's request, the publish and the handler share one trace.

  - The sink publishes on a channel in confirm mode and waits for the
    broker's ack (ConfirmTimeoutMs); no ack, or the connection down, raises,
    and the relay leaves the event pending for a later try. It runs inside
    the relay's transaction (IOutboxSink's contract): a slow broker holds it.
  - Each consumer has three queues (OutboxAmqpDeclareQueues): the work queue,
    bound to the exchange by a binding key; its retry queue, with no
    consumer and a TTL (RetryDelayMs), that dead-letters back to the work
    queue; and its dead queue. A handler that fails: Nack without requeue,
    so the work queue's dead-letter takes the message to the retry queue,
    back after the delay. The tries so far are the work queue's "rejected"
    entries in the x-death header; at MaxAttempts the consumer publishes the
    message to the dead queue and acks it (the broker doesn't count tries).
    The queue's TTL, not one per message: a per-message expiration only
    expires at the head of a queue. Changing RetryDelayMs changes the retry
    queue's arguments, and redeclaring a queue with other arguments is
    PRECONDITION_FAILED: delete it first.
  - At least once: handle, then record the event as processed (the
    application's PROCESSED_EVENTS table: consumer, tenant, event id), then
    ack. A crash between the handler and the record runs the handler again;
    a redelivered event already recorded is acked without running it.
  - The consumer's callbacks run in parallel on the process's pool (PcPool);
    the handler must allow that. InFlight counts the callbacks running, for
    a caller that waits for quiet.
  - Metrics, in pascal-common-faa's PcMetrics: outbox.consumer.messages by
    outbox.consumer, outbox.event.kind and outbox.result (done, duplicate,
    ignored, retried, parked), counted after the ack, nack or park.
  - The time recorded with a processed event is pascal-common-faa's TClock
    in Unix seconds (UTC), replaceable in tests.

  The SQL is the application's, read by name through the factory's loader
  (the constants below), as PascalDb.Outbox's own: reference statements and
  the table in docs/outbox.md.

  From pascal-agenda-faa's Agenda.Outbox.Amqp (its M7 decision 11), the
  same API. *)

interface

uses
  SysUtils,
  PascalDb.Interfaces,
  PascalDb.Outbox,
  AMQP.Connection;

const
  PROCESSED_SQL_EXISTS = 'PROCESSED.EXISTS';
  PROCESSED_SQL_INSERT = 'PROCESSED.INSERT';
  PROCESSED_SQL_PURGE = 'PROCESSED.PURGE';

  /// The message header with the event's tenant.
  OUTBOX_AMQP_TENANT_HEADER = 'tenant-id';
  OUTBOX_AMQP_DEFAULT_CONFIRM_MS = 5000;
  OUTBOX_AMQP_DEFAULT_RETRY_MS = 30000;
  OUTBOX_AMQP_DEFAULT_PREFETCH = 10;

type
  EOutboxAmqpError = class(Exception);

  /// A consumer's queues and how it reads them.
  TOutboxAmqpQueue = record
    /// The work queue; the retry and dead queues are Name + '.retry' and
    /// Name + '.dead'.
    Name: string;
    Exchange: string;
    /// The topic binding key ('appointment.#', '#').
    BindingKey: string;
    RetryDelayMs: Integer;
    MaxAttempts: Integer;
    Prefetch: Integer;
    class function Create(const AName, AExchange, ABindingKey: string): TOutboxAmqpQueue; static;
    function RetryName: string;
    function DeadName: string;
  end;

  TOutboxAmqpSink = class(TInterfacedObject, IOutboxSink)
  private
    FChannel: TAMQPChannel;
    FExchange: string;
    FConfirmTimeoutMs: Integer;
  public
    /// AChannel: not owned, put in confirm mode here, used by one thread at a
    /// time (the relay's).
    constructor Create(AChannel: TAMQPChannel; const AExchange: string;
      AConfirmTimeoutMs: Integer = OUTBOX_AMQP_DEFAULT_CONFIRM_MS);
    procedure Deliver(const AQuery: IQuery; const AEvent: TOutboxEvent; ANow: Int64);
  end;

  TOutboxAmqpConsumer = class
  private
    FFactory: IDBFactory;
    FChannel: TAMQPChannel;
    FQueue: TOutboxAmqpQueue;
    FConsumerName: string;
    FHandler: IOutboxHandler;
    FTag: string;
    FInFlight: Integer;
    FStopping: Integer;
    FOnError: TOutboxErrorProc;
    function Processed(const AEvent: TOutboxEvent): Boolean;
    procedure MarkProcessed(const AEvent: TOutboxEvent);
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure Report(const AEvent: TOutboxEvent; const AMessage: string);
  public
    /// AChannel: not owned; its callbacks are this consumer's. AConsumerName
    /// is PROCESSED_EVENTS.CONSUMER (60 characters): keep it once events
    /// were recorded.
    constructor Create(const AFactory: IDBFactory; AChannel: TAMQPChannel;
      const AQueue: TOutboxAmqpQueue; const AConsumerName: string;
      const AHandler: IOutboxHandler);
    /// Qos and Consume.
    procedure Start;
    /// Cancels the consumer and waits (at most AWaitMs) for the callbacks
    /// running to finish; one that starts after this neither handles nor
    /// acks its message, which the channel's close gives back to the queue.
    /// Call it before closing the channel, so no new work starts while the
    /// process shuts down, and before freeing the consumer.
    procedure Stop(AWaitMs: Integer = 30000);
    /// Callbacks running now.
    function InFlight: Integer;
    property OnError: TOutboxErrorProc read FOnError write FOnError;
  end;

/// The topic exchange, durable.
procedure OutboxAmqpDeclareExchange(AChannel: TAMQPChannel; const AExchange: string);
/// AQueue's work, retry and dead queues, durable, and the work queue's
/// binding.
procedure OutboxAmqpDeclareQueues(AChannel: TAMQPChannel; const AQueue: TOutboxAmqpQueue);
/// Messages ready in AQueue's work and retry queues (not the ones delivered
/// and not acked yet).
function OutboxAmqpPending(AChannel: TAMQPChannel; const AQueue: TOutboxAmqpQueue): Cardinal;
/// The event a delivery carries ('' Id when it isn't one); Attempts is the
/// tries the work queue AQueueName rejected.
function OutboxAmqpEvent(const ADelivery: TAMQPDelivery; const AQueueName: string): TOutboxEvent;
/// Deletes PROCESSED_EVENTS rows recorded before ABefore; returns them.
function OutboxAmqpPurgeProcessed(const AFactory: IDBFactory; ABefore: Int64): Integer;

implementation

uses
  Rtti,
  DateUtils,
  PascalCommon.Threading,
  PascalCommon.SystemContext,
  PascalCommon.Metrics,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods;

// outbox.consumer.messages: one per message by result (done,
// duplicate: already handled; ignored: a kind the handler doesn't take;
// retried; parked). The kind label is "other" for a kind the handler
// doesn't take, so a stranger publishing to the exchange can't grow the
// series.
procedure CountMessage(const AConsumer, AKind, AResult: string);
begin
  PcMetrics.Counter('outbox.consumer.messages', '{message}',
    'Outbox events consumed from the broker, by consumer, kind and result (done, ' +
    'duplicate, ignored, retried, parked)',
    ['outbox.consumer', 'outbox.event.kind', 'outbox.result']).Labels([AConsumer, AKind,
    AResult]).Inc;
end;

// TClock is local time; DateTimeToUnix(.., False) converts it to UTC (the
// same meaning of the flag on both compilers).
function UnixNow: Int64;
begin
  Result := DateTimeToUnix(TClock.Now, False);
end;

{ TOutboxAmqpQueue }

class function TOutboxAmqpQueue.Create(const AName, AExchange,
  ABindingKey: string): TOutboxAmqpQueue;
begin
  Result.Name := AName;
  Result.Exchange := AExchange;
  Result.BindingKey := ABindingKey;
  Result.RetryDelayMs := OUTBOX_AMQP_DEFAULT_RETRY_MS;
  Result.MaxAttempts := OUTBOX_DEFAULT_MAX_ATTEMPTS;
  Result.Prefetch := OUTBOX_AMQP_DEFAULT_PREFETCH;
end;

function TOutboxAmqpQueue.RetryName: string;
begin
  Result := Name + '.retry';
end;

function TOutboxAmqpQueue.DeadName: string;
begin
  Result := Name + '.dead';
end;

{ Topology }

procedure OutboxAmqpDeclareExchange(AChannel: TAMQPChannel; const AExchange: string);
var
  LDeclare: TAMQPExchangeDeclare;
begin
  LDeclare := TAMQPExchangeDeclare.Create(AExchange, AMQP_EXCHANGE_TYPE_TOPIC);
  LDeclare.Durable := True;
  AChannel.DeclareExchange(LDeclare);
end;

procedure DeclareWithDeadLetter(AChannel: TAMQPChannel; const AName, ADeadLetterTo: string;
  ATtlMs: Integer);
var
  LArgs: TAMQPFieldTable;
  LDeclare: TAMQPQueueDeclare;
begin
  LArgs := TAMQPFieldTable.Create;
  try
    // The default exchange ('') routes by the queue's name: no exchange of
    // its own for the dead-letter hop.
    LArgs.Put('x-dead-letter-exchange', TValue.From<string>(''));
    LArgs.Put('x-dead-letter-routing-key', TValue.From<string>(ADeadLetterTo));
    if ATtlMs > 0 then
      LArgs.Put('x-message-ttl', TValue.From<Integer>(ATtlMs));
    LDeclare := TAMQPQueueDeclare.Create(AName, True);
    LDeclare.Arguments := LArgs;
    AChannel.DeclareQueue(LDeclare);
  finally
    LArgs.Free;
  end;
end;

procedure OutboxAmqpDeclareQueues(AChannel: TAMQPChannel; const AQueue: TOutboxAmqpQueue);
var
  LBind: TAMQPQueueBind;
begin
  DeclareWithDeadLetter(AChannel, AQueue.Name, AQueue.RetryName, 0);
  DeclareWithDeadLetter(AChannel, AQueue.RetryName, AQueue.Name, AQueue.RetryDelayMs);
  AChannel.DeclareQueue(TAMQPQueueDeclare.Create(AQueue.DeadName, True));
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := AQueue.Name;
  LBind.ExchangeName := AQueue.Exchange;
  LBind.RoutingKey := AQueue.BindingKey;
  AChannel.BindQueue(LBind);
end;

function ReadyIn(AChannel: TAMQPChannel; const AName: string): Cardinal;
var
  LDeclare: TAMQPQueueDeclare;
begin
  LDeclare := TAMQPQueueDeclare.Create(AName, True);
  LDeclare.Passive := True;
  Result := AChannel.DeclareQueue(LDeclare).MessageCount;
end;

function OutboxAmqpPending(AChannel: TAMQPChannel; const AQueue: TOutboxAmqpQueue): Cardinal;
begin
  Result := ReadyIn(AChannel, AQueue.Name) + ReadyIn(AChannel, AQueue.RetryName);
end;

{ The message }

// The work queue's rejections in x-death: the tries that failed. The
// indexer's TValue goes to a local before its accessor (FPC 3.2's internal
// error, a gotcha in this library's CLAUDE.md), and every array element is unwrapped.
function Rejections(const AProps: TAMQPBasicProperties; const AQueueName: string): Integer;
var
  LDeath, LEntry, LField: TValue;
  LTable: TAMQPFieldTable;
  I: Integer;
begin
  Result := 0;
  if not AProps.Has(bpHeaders) or (AProps.Headers = nil) then
    Exit;
  if not AProps.Headers.TryGetValue('x-death', LDeath) or not LDeath.IsArray then
    Exit;
  for I := 0 to LDeath.GetArrayLength - 1 do
  begin
    LEntry := AmqpUnwrapValue(LDeath.GetArrayElement(I));
    if not (LEntry.IsObject and (LEntry.AsObject is TAMQPFieldTable)) then
      Continue;
    LTable := TAMQPFieldTable(LEntry.AsObject);
    if not LTable.TryGetValue('queue', LField) or (LField.AsString <> AQueueName) then
      Continue;
    if not LTable.TryGetValue('reason', LField) or (LField.AsString <> 'rejected') then
      Continue;
    if LTable.TryGetValue('count', LField) then
      Inc(Result, Integer(LField.AsInt64));
  end;
end;

function OutboxAmqpEvent(const ADelivery: TAMQPDelivery; const AQueueName: string): TOutboxEvent;
var
  LValue: TValue;
begin
  Result.Id := ADelivery.Properties.MessageId;
  Result.Kind := ADelivery.Properties.MsgType;
  Result.Payload := AmqpUtf8Decode(ADelivery.Body);
  // The consumer's span is already the message's child.
  Result.TraceParent := '';
  Result.CreatedAt := Int64(ADelivery.Properties.Timestamp);
  Result.TenantId := '';
  if ADelivery.Properties.Has(bpHeaders) and (ADelivery.Properties.Headers <> nil) and
    ADelivery.Properties.Headers.TryGetValue(OUTBOX_AMQP_TENANT_HEADER, LValue) then
    Result.TenantId := LValue.AsString;
  Result.Attempts := Rejections(ADelivery.Properties, AQueueName);
  if (Result.TenantId = '') or (Result.Kind = '') then
    Result.Id := '';
end;

function OutboxAmqpPurgeProcessed(const AFactory: IDBFactory; ABefore: Int64): Integer;
var
  LQuery: IQuery;
  LTx: IScopeTransaction;
begin
  LTx := AFactory.GetPool.AcquireQuery(LQuery);
  LTx.StartTransaction;
  try
    LQuery.Sql := AFactory.SqlLoader[PROCESSED_SQL_PURGE].SQL;
    LQuery.Params.Int64s['BEFORE'] := ABefore;
    Result := Integer(LQuery.ExecSql);
    LTx.Commit;
  except
    LTx.Rollback;
    raise;
  end;
end;

{ TOutboxAmqpSink }

constructor TOutboxAmqpSink.Create(AChannel: TAMQPChannel; const AExchange: string;
  AConfirmTimeoutMs: Integer);
begin
  inherited Create;
  FChannel := AChannel;
  FExchange := AExchange;
  FConfirmTimeoutMs := AConfirmTimeoutMs;
  FChannel.ConfirmSelect;
end;

procedure TOutboxAmqpSink.Deliver(const AQuery: IQuery; const AEvent: TOutboxEvent;
  ANow: Int64);
var
  LProps: TAMQPBasicProperties;
  LHeaders: TAMQPFieldTable;
  LSeq: UInt64;
begin
  LHeaders := TAMQPFieldTable.Create;
  try
    LHeaders.Put(OUTBOX_AMQP_TENANT_HEADER, TValue.From<string>(AEvent.TenantId));
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('application/json');
    LProps.SetPersistent;
    LProps.SetMessageId(AEvent.Id);
    LProps.SetMsgType(AEvent.Kind);
    LProps.SetTimestamp(UInt64(AEvent.CreatedAt));
    LProps.SetHeaders(LHeaders);
    LSeq := FChannel.Publish(FExchange, AEvent.Kind, AmqpUtf8Encode(AEvent.Payload), LProps);
  finally
    LHeaders.Free;
  end;
  if not FChannel.WaitForConfirm(LSeq, Cardinal(FConfirmTimeoutMs)) then
    raise EOutboxAmqpError.CreateFmt('The broker didn''t confirm event %s within %d ms.',
      [AEvent.Id, FConfirmTimeoutMs]);
end;

{ TOutboxAmqpConsumer }

constructor TOutboxAmqpConsumer.Create(const AFactory: IDBFactory; AChannel: TAMQPChannel;
  const AQueue: TOutboxAmqpQueue; const AConsumerName: string;
  const AHandler: IOutboxHandler);
begin
  inherited Create;
  FFactory := AFactory;
  FChannel := AChannel;
  FQueue := AQueue;
  FConsumerName := AConsumerName;
  FHandler := AHandler;
end;

procedure TOutboxAmqpConsumer.Start;
begin
  FChannel.Qos(Word(FQueue.Prefetch));
  FTag := FChannel.Consume(FQueue.Name, OnDelivery);
end;

procedure TOutboxAmqpConsumer.Stop(AWaitMs: Integer);
var
  LWaited, LQuiet: Integer;
begin
  if FTag = '' then
    Exit;
  PcAtomicSet(FStopping, 1);
  FChannel.Cancel(FTag);
  FTag := '';
  // Quiet twice in a row: a delivery already queued on the pool may start
  // between two looks.
  LWaited := 0;
  LQuiet := 0;
  while (LQuiet < 2) and (LWaited < AWaitMs) do
  begin
    if PcAtomicGet(FInFlight) = 0 then
      Inc(LQuiet)
    else
      LQuiet := 0;
    Sleep(20);
    Inc(LWaited, 20);
  end;
end;

function TOutboxAmqpConsumer.InFlight: Integer;
begin
  Result := PcAtomicGet(FInFlight);
end;

procedure TOutboxAmqpConsumer.Report(const AEvent: TOutboxEvent; const AMessage: string);
begin
  if Assigned(FOnError) then
    FOnError(AEvent, FHandler.Name, AMessage);
end;

function TOutboxAmqpConsumer.Processed(const AEvent: TOutboxEvent): Boolean;
var
  LQuery: IQuery;
  LTx: IScopeTransaction;
begin
  LTx := FFactory.GetPool.AcquireQuery(LQuery);
  LQuery.Sql := FFactory.SqlLoader[PROCESSED_SQL_EXISTS].SQL;
  LQuery.Params.Strings['CONSUMER'] := FConsumerName;
  LQuery.Params.Strings['TENANT_ID'] := AEvent.TenantId;
  LQuery.Params.Strings['EVENT_ID'] := AEvent.Id;
  Result := not LQuery.Open.Eof;
end;

procedure TOutboxAmqpConsumer.MarkProcessed(const AEvent: TOutboxEvent);
var
  LQuery: IQuery;
  LTx: IScopeTransaction;
begin
  LTx := FFactory.GetPool.AcquireQuery(LQuery);
  LTx.StartTransaction;
  try
    LQuery.Sql := FFactory.SqlLoader[PROCESSED_SQL_INSERT].SQL;
    LQuery.Params.Strings['CONSUMER'] := FConsumerName;
    LQuery.Params.Strings['TENANT_ID'] := AEvent.TenantId;
    LQuery.Params.Strings['EVENT_ID'] := AEvent.Id;
    LQuery.Params.Int64s['NOW'] := UnixNow;
    LQuery.ExecSql;
    LTx.Commit;
  except
    LTx.Rollback;
    raise;
  end;
end;

procedure TOutboxAmqpConsumer.OnDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LEvent: TOutboxEvent;
  LProps: TAMQPBasicProperties;
  LError, LKind, LResult: string;
begin
  PcAtomicInc(FInFlight);
  try
    // Stopping: no handler, no ack; the channel's close requeues it.
    if PcAtomicGet(FStopping) <> 0 then
      Exit;
    LEvent := OutboxAmqpEvent(ADelivery, FQueue.Name);
    LError := '';
    LKind := 'other';
    LResult := 'ignored';
    try
      if LEvent.Id = '' then
        raise EOutboxAmqpError.CreateFmt('Not an outbox event (message id "%s").',
          [ADelivery.Properties.MessageId]);
      // Another kind on the same binding, or one already handled (the
      // broker's redelivery): acked, nothing run.
      if FHandler.Accepts(LEvent.Kind) then
      begin
        LKind := LEvent.Kind;
        LResult := 'duplicate';
        if not Processed(LEvent) then
        begin
          FHandler.Handle(LEvent);
          MarkProcessed(LEvent);
          LResult := 'done';
        end;
      end;
    except
      on E: Exception do
        LError := E.ClassName + ': ' + E.Message;
    end;
    if LError = '' then
    begin
      AChannel.Ack(ADelivery.DeliveryTag);
      CountMessage(FConsumerName, LKind, LResult);
      Exit;
    end;
    Report(LEvent, LError);
    if (LEvent.Id <> '') and (LEvent.Attempts + 1 < FQueue.MaxAttempts) then
    begin
      // Through the work queue's dead-letter to the retry queue.
      AChannel.Nack(ADelivery.DeliveryTag, False);
      CountMessage(FConsumerName, LKind, 'retried');
    end
    else
    begin
      CountMessage(FConsumerName, LKind, 'parked');
      // Out of tries (or not an event at all): parked in the dead queue, as
      // it came (the headers are the delivery's, alive during the callback).
      LProps := ADelivery.Properties;
      AChannel.Publish('', FQueue.DeadName, ADelivery.Body, LProps);
      AChannel.Ack(ADelivery.DeliveryTag);
    end;
  finally
    PcAtomicDec(FInFlight);
  end;
end;

end.
