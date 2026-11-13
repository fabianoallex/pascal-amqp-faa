program SmokeTest;

{ Smoke test of pascal-amqp-faa against a real RabbitMQ (docker/docker-compose.yml).

  Compiles in both worlds from the SAME source:
    FPC:    fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src SmokeTest.dpr

  Exercises: connection + handshake, channel, exchange/queue/bind declare, publisher
  confirms (publish + WaitForConfirm), BasicGet, Consume with manual ack (callback
  dispatched on the thread pool), Cancel and a clean teardown. Exits with exit
  code 0 if everything passed; 1 if something failed.

  Accepts --host <name> and --port <n> to point at another broker (that is how
  WS9 runs it against this lib's embedded broker, without this sample
  coming to depend on the server sub-module).

  With the --tls argument, it runs the same steps over TLS (localhost:5671 from
  docker-compose.tls.yml, self-signed cert): SChannel on Windows, OpenSSL
  elsewhere or when compiled with -dAMQP_OPENSSL (the default backend). }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ELSE}
  {$APPTYPE CONSOLE}
{$ENDIF}

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  {$ENDIF}
  SysUtils,
  Classes,
  SyncObjs,
  Rtti,
  PascalCommon.Threading,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Transport,
  AMQP.Connection;

const
  EXCHANGE = 'pascal-amqp-faa.smoke';
  QUEUE = 'pascal-amqp-faa.smoke.q';
  ROUTING_KEY = 'smoke';
  CONSUME_COUNT = 5;
  // Phase 3: TTL+DLX and priority. They run against RabbitMQ AND against the
  // embedded broker -- it is the double proof that the implemented semantics are the spec's.
  DLX = 'pascal-amqp-faa.smoke.dlx';
  QUEUE_WAIT = 'pascal-amqp-faa.smoke.wait';
  QUEUE_DEAD = 'pascal-amqp-faa.smoke.dead';
  QUEUE_PRIO = 'pascal-amqp-faa.smoke.prio';

type
  { Callbacks are `of object`: the test state lives in this class. }
  TSmoke = class
  private
    FExpected: Integer;
    FReceived: Integer;      // atomic through the test's simple lock
    FLock: TCriticalSection;
    FAllReceived: TEvent;
    FReconnected: TEvent;
    FLastBody: string;
  public
    constructor Create(AExpected: Integer);
    destructor Destroy; override;
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    /// OnReconnect handler: fires AFTER the full topology recovery;
    /// it is the right signal for "you may publish again" (IsOpen turns True earlier).
    procedure OnReconnected(AConnection: TAMQPConnection);
    function WaitAll(ATimeoutMs: Cardinal): Boolean;
    function WaitReconnected(ATimeoutMs: Cardinal): Boolean;
    property LastBody: string read FLastBody;
  end;

constructor TSmoke.Create(AExpected: Integer);
begin
  inherited Create;
  FExpected := AExpected;
  FLock := TCriticalSection.Create;
  FAllReceived := TEvent.Create(nil, True, False, '');
  FReconnected := TEvent.Create(nil, True, False, '');
end;

destructor TSmoke.Destroy;
begin
  FReconnected.Free;
  FAllReceived.Free;
  FLock.Free;
  inherited;
end;

procedure TSmoke.OnReconnected(AConnection: TAMQPConnection);
begin
  FReconnected.SetEvent;
end;

function TSmoke.WaitReconnected(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FReconnected.WaitFor(ATimeoutMs) = wrSignaled;
end;

procedure TSmoke.OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
begin
  AChannel.Ack(ADelivery.DeliveryTag);
  FLock.Enter;
  try
    Inc(FReceived);
    FLastBody := ADelivery.BodyAsText;
    if FReceived >= FExpected then
      FAllReceived.SetEvent;
  finally
    FLock.Leave;
  end;
end;

function TSmoke.WaitAll(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FAllReceived.WaitFor(ATimeoutMs) = wrSignaled;
end;

// Test parameters. Accepted arguments, in any order:
//   --tls          runs EVERYTHING over TLS (localhost:5671, self-signed cert from
//                  docker-compose.tls.yml; the build's default backend:
//                  SChannel on Windows, OpenSSL elsewhere or with -dAMQP_OPENSSL)
//   --host <name>  points at another host (default: the Localhost one)
//   --port <n>     points at another port (default: 5672, or 5671 with --tls)
//
//   --durable-publish <n>  publishes n PERSISTENT messages to a durable queue
//                          and exits. Does not run the 9 steps.
//   --durable-verify <n>   checks that the n messages are there, in order, and
//                          that the queue is empty afterwards. Does not run the 9 steps.
//
// The --durable-publish/--durable-verify pair is the RESTART step of Phase 4
// (WS10). It does not fit in a single program because the broker has to DIE in
// the middle: the orchestrator is tests\tools\smoke_restart.py, which brings up
// BrokerHostFpc with --datadir, calls the publish, KILLS the host, brings up
// another one on the same directory and calls the verify.
//
// Why here and not in a suite: this is the only place in the repository where
// durability is observed by a PURE client, over a socket, against
// a broker process that really died. The suites run broker and
// client in the same process, which is great for coverage and useless for this
// question.
//
// --host/--port exist so the SmokeTest can run against ANY broker --
// including this lib's own embedded broker, which the acceptance runner
// (tests\Acceptance\fpc\BrokerHostFpc) brings up on a fixed port. It is the
// WS9 step that was missing, without turning this sample into a program that
// depends on the server sub-module: it remains 100% client.
function SmokeParams: TAMQPConnectionParams;
var
  I: Integer;
  LArg: string;
begin
  Result := TAMQPConnectionParams.Localhost;
  for I := 1 to ParamCount do
    if SameText(ParamStr(I), '--tls') then
      Result := TAMQPConnectionParams.LocalhostTls;

  I := 1;
  while I <= ParamCount do
  begin
    LArg := ParamStr(I);
    if SameText(LArg, '--host') and (I < ParamCount) then
    begin
      Result.Host := ParamStr(I + 1);
      Inc(I);
    end
    else if SameText(LArg, '--port') and (I < ParamCount) then
    begin
      Result.Port := StrToIntDef(ParamStr(I + 1), Result.Port);
      Inc(I);
    end;
    Inc(I);
  end;
end;

// Reads a numeric argument of an option, or -1 if it was not given.
function NumericArg(const AOption: string): Integer;
var
  I: Integer;
begin
  Result := -1;
  for I := 1 to ParamCount - 1 do
    if SameText(ParamStr(I), AOption) then
      Exit(StrToIntDef(ParamStr(I + 1), -1));
end;

procedure Check(ACondition: Boolean; const AWhat: string);
begin
  if ACondition then
    WriteLn('  ok: ', AWhat)
  else
    raise Exception.Create('FAILED: ' + AWhat);
end;

// Waits until ACount messages show up in the queue, or gives up at the deadline.
// Without it this would be a fixed Sleep -- and the time for a TTL to expire plus
// the dead-letter to cross two queues varies between RabbitMQ and the embedded broker.
function WaitForCount(AChan: TAMQPChannel; const AQueue: string;
  ACount: Cardinal; ATimeoutMs: Cardinal): Boolean;
var
  LDecl: TAMQPQueueDeclare;
  LEnd: UInt64;
begin
  LEnd := PcTickMs + ATimeoutMs;
  repeat
    LDecl := TAMQPQueueDeclare.Create(AQueue);
    LDecl.Passive := True; // passive: only queries, does not redeclare
    if AChan.DeclareQueue(LDecl).MessageCount >= ACount then
      Exit(True);
    Sleep(25);
  until PcTickMs > LEnd;
  Result := False;
end;

// TTL + dead-letter: the message expires in the wait queue and the broker
// republishes it to the DLX, from where it lands in the final queue. It is the
// retry-with-wait pattern of the RetryDlqVcl sample, cut down to the essentials.
//
// It runs the SAME against RabbitMQ and against the embedded broker -- that is
// the point: the same steps against both say that the implemented semantics are
// the spec's, not ours.
procedure StepTtlDlx(AChan: TAMQPChannel);
var
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LBind: TAMQPQueueBind;
  LGet: TAMQPGetResult;
  LDel: TAMQPQueueDelete;
  LVal: TValue;
begin
  AChan.DeclareExchange(TAMQPExchangeDeclare.Create(DLX));
  // Delete first: redeclaring with different arguments would be
  // PRECONDITION_FAILED, and deleting a missing queue is a no-op.
  LDel := Default(TAMQPQueueDelete);
  LDel.QueueName := QUEUE_WAIT;
  AChan.DeleteQueue(LDel);
  LDel.QueueName := QUEUE_DEAD;
  AChan.DeleteQueue(LDel);

  // Final queue, bound to the DLX.
  AChan.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_DEAD));
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := QUEUE_DEAD;
  LBind.ExchangeName := DLX;
  // The key is the NAME OF THE WAIT QUEUE, not ROUTING_KEY: without
  // x-dead-letter-routing-key the dead message keeps its ORIGINAL key, and its
  // key is the queue name (it was published to the default exchange). Tying this
  // down here is what makes this step exercise the key-preservation rule,
  // instead of masking it with a fixed key on both sides.
  LBind.RoutingKey := QUEUE_WAIT;
  AChan.BindQueue(LBind);

  // Wait queue: short TTL + DLX. No consumer -- the TTL is what moves it.
  LArgs := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<Int64>(300);
    LArgs.Put('x-message-ttl', LVal);
    LVal := TValue.From<string>(DLX);
    LArgs.Put('x-dead-letter-exchange', LVal);
    LDecl := TAMQPQueueDeclare.Create(QUEUE_WAIT);
    LDecl.Arguments := LArgs;
    AChan.DeclareQueue(LDecl);
  finally
    LArgs.Free; // the declare does NOT free it: the owner is the caller
  end;

  AChan.PublishText('', QUEUE_WAIT, 'about-to-die');
  Check(WaitForCount(AChan, QUEUE_DEAD, 1, 10000),
    'message hit the TTL and landed in the DLX queue');

  LGet := AChan.BasicGet(QUEUE_DEAD, True);
  Check(LGet.Found, 'message found in the dead-letter queue');
  Check(LGet.BodyAsText = 'about-to-die', 'body intact after the dead-letter');
  Check(LGet.Properties.Has(bpHeaders), 'the dead-lettered message got headers');
  Check(LGet.Properties.Headers.ContainsKey('x-death'),
    'x-death header present');
end;

// Priority: with the consumer stopped, a batch of mixed priorities comes out
// in DESCENDING order. The two conditions that make this hold are here:
// backlog built BEFORE any read, and reading one at a time through Get.
procedure StepPriority(AChan: TAMQPChannel);
var
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LGet: TAMQPGetResult;
  LVal: TValue;
  LProps: TAMQPBasicProperties;
  LDel: TAMQPQueueDelete;
  I, LPrevious, LCurrent: Integer;
  LOrder: string;
const
  PRIOS: array[0..5] of Byte = (2, 8, 5, 9, 1, 7);
begin
  LDel := Default(TAMQPQueueDelete);
  LDel.QueueName := QUEUE_PRIO;
  AChan.DeleteQueue(LDel);
  LArgs := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<Int64>(9);
    LArgs.Put('x-max-priority', LVal);
    LDecl := TAMQPQueueDeclare.Create(QUEUE_PRIO);
    LDecl.Arguments := LArgs;
    AChan.DeclareQueue(LDecl);
  finally
    LArgs.Free;
  end;

  for I := Low(PRIOS) to High(PRIOS) do
  begin
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetPriority(PRIOS[I]);
    // PublishText takes no properties: priority requires the full Publish.
    LProps.SetContentType('text/plain');
    AChan.Publish('', QUEUE_PRIO,
      AmqpUtf8Encode('p' + IntToStr(PRIOS[I])), LProps);
  end;
  Check(WaitForCount(AChan, QUEUE_PRIO, Length(PRIOS), 10000),
    'priority batch enqueued');

  LOrder := '';
  LPrevious := 999;
  for I := Low(PRIOS) to High(PRIOS) do
  begin
    LGet := AChan.BasicGet(QUEUE_PRIO, True);
    if not LGet.Found then
      Break;
    LCurrent := LGet.Properties.Priority;
    LOrder := LOrder + IntToStr(LCurrent) + ' ';
    Check(LCurrent <= LPrevious,
      'descending priority order (got: ' + LOrder + ')');
    LPrevious := LCurrent;
  end;
  Check(LOrder = '9 8 7 5 2 1 ', 'drained in priority order: ' + LOrder);
end;

// Drops the socket in the middle of consumption and checks that the lib reconnects
// on its own, replays the topology (queue/bind/qos/confirm/consume) and delivers again.
procedure RunReconnectTest;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LSmoke: TSmoke;
  LBind: TAMQPQueueBind;
  LSeqNo: UInt64;
begin
  LParams := SmokeParams;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LSmoke := TSmoke.Create(1);
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.OnReconnect := LSmoke.OnReconnected;
    LConn.Open;
    LChan := LConn.CreateChannel;
    LChan.DeclareExchange(TAMQPExchangeDeclare.Create(EXCHANGE));
    LChan.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE));
    LBind := Default(TAMQPQueueBind);
    LBind.QueueName := QUEUE;
    LBind.ExchangeName := EXCHANGE;
    LBind.RoutingKey := ROUTING_KEY;
    LChan.BindQueue(LBind);
    LChan.ConfirmSelect;
    LChan.Consume(QUEUE, LSmoke.OnDelivery);

    LConn.DropConnectionForTest; // simulated abrupt drop

    // Waits for OnReconnect (fires after handshake + topology recovery).
    Check(LSmoke.WaitReconnected(15000), 'reconnected and recovered the topology');
    Check(LConn.IsOpen, 'connection open after reconnect');

    // The channel was recovered: confirmed publish and re-armed consumer deliver.
    LSeqNo := LChan.PublishText(EXCHANGE, ROUTING_KEY, 'after-reconnect');
    Check(LChan.WaitForConfirm(LSeqNo, 5000), 'post-reconnect publish confirmed');
    Check(LSmoke.WaitAll(10000), 'message delivered to the recovered consumer');
    Check(LSmoke.LastBody = 'after-reconnect', 'body intact after reconnect');

    LChan.Close;
    LChan.Free;
    LConn.Close;
  finally
    LConn.Free;
    LSmoke.Free;
  end;
end;

const
  DURABLE_QUEUE = 'smoke.durable';

// Publishes AN persistent messages to a durable queue and confirms. The body of
// each one is its own number: the verify checks ORDER and INTEGRITY, not just
// the count.
procedure DurablePublish(const AParams: TAMQPConnectionParams; AN: Integer);
var
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  I: Integer;
begin
  WriteLn('[durable-publish] ', AN, ' persistent to ', DURABLE_QUEUE);
  LConn := TAMQPConnection.Create(AParams);
  try
    LConn.Open;
    LChan := LConn.CreateChannel;
    LDecl := TAMQPQueueDeclare.Create(DURABLE_QUEUE, True); // durable
    LChan.DeclareQueue(LDecl);
    LChan.ConfirmSelect;
    for I := 1 to AN do
      LChan.PublishText('', DURABLE_QUEUE, IntToStr(I), True); // persistent
    Check(LChan.WaitForConfirms(30000), 'the broker confirmed the ' +
      IntToStr(AN) + ' persistent');
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

// Checks that the AN messages survived the BROKER restart, in order, and
// that nothing extra came.
procedure DurableVerify(const AParams: TAMQPConnectionParams; AN: Integer);
var
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LGet: TAMQPGetResult;
  I: Integer;
begin
  WriteLn('[durable-verify] expecting ', AN, ' back in ', DURABLE_QUEUE);
  LConn := TAMQPConnection.Create(AParams);
  try
    LConn.Open;
    LChan := LConn.CreateChannel;
    for I := 1 to AN do
    begin
      LGet := LChan.BasicGet(DURABLE_QUEUE, True);
      Check(LGet.Found, Format('message %d survived the restart', [I]));
      Check(LGet.BodyAsText = IntToStr(I),
        Format('message %d in order and intact (got "%s")',
          [I, LGet.BodyAsText]));
      // D27 in one line, seen by the CLIENT: every recovered entry comes back
      // marked as redelivered. Persisting the flag would be false precision --
      // after a crash the broker does not know whether anyone got to see it.
      Check(LGet.Redelivered, Format('message %d came back redelivered', [I]));
    end;
    LGet := LChan.BasicGet(DURABLE_QUEUE, True);
    Check(not LGet.Found, 'and nothing extra came back');
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

var
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LSmoke: TSmoke;
  LSeqNo: UInt64;
  LGet: TAMQPGetResult;
  LBind: TAMQPQueueBind;
  LTag: string;
  I: Integer;
begin
  ExitCode := 1;
  LSmoke := nil;
  // The durable modes REPLACE the 9 steps: they are half of a test that
  // only makes sense with a broker restart in the middle.
  if NumericArg('--durable-publish') >= 0 then
  begin
    try
      DurablePublish(SmokeParams, NumericArg('--durable-publish'));
      WriteLn('DURABLE PUBLISH: OK');
      ExitCode := 0;
    except
      on E: Exception do
      begin
        WriteLn('DURABLE PUBLISH: FAIL - ', E.ClassName, ': ', E.Message);
        ExitCode := 1;
      end;
    end;
    Exit;
  end;
  if NumericArg('--durable-verify') >= 0 then
  begin
    try
      DurableVerify(SmokeParams, NumericArg('--durable-verify'));
      WriteLn('DURABLE VERIFY: OK');
      ExitCode := 0;
    except
      on E: Exception do
      begin
        WriteLn('DURABLE VERIFY: FAIL - ', E.ClassName, ': ', E.Message);
        ExitCode := 1;
      end;
    end;
    Exit;
  end;
  LConn := TAMQPConnection.Create(SmokeParams);
  try
    try
      WriteLn('[1] connection + handshake');
      LConn.Open;
      Check(LConn.IsOpen, 'connection open');
      if SmokeParams.UseTls then
        WriteLn('  tls: ', AmqpTlsBackendInfo); // engine actually loaded

      WriteLn('[2] channel + topology');
      LChan := LConn.CreateChannel;
      LChan.DeclareExchange(TAMQPExchangeDeclare.Create(EXCHANGE));
      LChan.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE));
      LBind := Default(TAMQPQueueBind);
      LBind.QueueName := QUEUE;
      LBind.ExchangeName := EXCHANGE;
      LBind.RoutingKey := ROUTING_KEY;
      LChan.BindQueue(LBind);
      Check(LChan.IsOpen, 'channel open and topology declared');

      // Clean queue so the test is deterministic (drains leftovers from earlier
      // rounds; the lib does not expose queue.purge on the channel for now).
      repeat
        LGet := LChan.BasicGet(QUEUE, True);
      until not LGet.Found;

      WriteLn('[3] publisher confirms');
      LChan.ConfirmSelect;
      LSeqNo := LChan.PublishText(EXCHANGE, ROUTING_KEY, 'hello from pascal-amqp-faa');
      Check(LSeqNo = 1, Format('publish got seq-no 1 (got %d)', [LSeqNo]));
      Check(LChan.WaitForConfirm(LSeqNo, 5000), 'broker confirmed the publish');

      WriteLn('[4] basic.get');
      LGet := LChan.BasicGet(QUEUE, True);
      Check(LGet.Found, 'message found in the queue');
      Check(LGet.BodyAsText = 'hello from pascal-amqp-faa',
        'body intact (' + LGet.BodyAsText + ')');

      WriteLn('[5] consume with manual ack (thread pool)');
      LSmoke := TSmoke.Create(CONSUME_COUNT);
      LChan.Qos(10);
      LTag := LChan.Consume(QUEUE, LSmoke.OnDelivery);
      for I := 1 to CONSUME_COUNT do
        LChan.PublishText(EXCHANGE, ROUTING_KEY, Format('msg-%d', [I]));
      Check(LChan.WaitForConfirms(5000), 'all publishes confirmed');
      Check(LSmoke.WaitAll(10000),
        Format('%d messages delivered to the consumer', [CONSUME_COUNT]));
      LChan.Cancel(LTag);

      WriteLn('[6] ttl + dead-letter (Phase 3)');
      StepTtlDlx(LChan);

      WriteLn('[7] priority queue (Phase 3)');
      StepPriority(LChan);

      WriteLn('[8] teardown');
      LChan.Close;
      LChan.Free;
      LConn.Close;
      Check(not LConn.IsOpen, 'connection closed');

      WriteLn('[9] automatic reconnect + topology recovery');
      RunReconnectTest;

      WriteLn;
      WriteLn('SMOKE TEST: PASS');
      ExitCode := 0;
    except
      on E: Exception do
      begin
        WriteLn;
        WriteLn('SMOKE TEST: FAIL — ', E.ClassName, ': ', E.Message);
        ExitCode := 1;
      end;
    end;
  finally
    LSmoke.Free;
    LConn.Free;
  end;
end.
