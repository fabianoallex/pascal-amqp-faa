program AMQPServer;

{ Embedded AMQP broker with observability turned on (Phase 4.1).

  Same source for FPC and Delphi, like the SmokeTest. Brings up a broker on
  127.0.0.1:5672, subscribes to the events and prints each one. It stops on its
  own after --seconds (default 20), to serve as a verification step and not
  only as a demo.

    AMQPServer.exe [--port N] [--seconds N] [--types lifecycle|all]

  FPC:     lazbuild samples\Server\AMQPServer.lpi
  Delphi:  open samples\Server\AMQPServer.dproj in the IDE (the CE has no CLI).

  WHAT THIS SAMPLE SHOWS, and it is the only way to see D30 working:
  the handler runs on the broker's NOTIFIER thread, never on a connection's read
  thread and never on a pool worker. That is why it can write to the
  console -- which is slow and serialized -- without delaying the broker. The
  price is in the final report: under a burst, the DROPPED counter goes up, and
  that is exactly the best-effort contract of D31. Events are not an audit log.

  'all' also brings the actor-origin types (enqueue/deliver/expire/DLX/drop)
  and the journal one (Inc. 2 of this phase); 'lifecycle' keeps only the
  connection, channel and topology ones. }

{$IFDEF FPC}
{$MODE DELPHI}{$H+}
{$ENDIF}
{$APPTYPE CONSOLE}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  SysUtils,
  PascalCommon.Threading,
  AMQP.Server.Events,
  AMQP.Server.Broker;

type
  { Example handler. It needs no lock at all: the notifier is ONE
    thread, so the handler never runs concurrently with itself (D30). A
    handler that talks to state shared with OTHER threads, on the other hand,
    must protect itself. }
  TEventPrinter = class
  private
    FTotal: Integer;
  public
    procedure HandleServerEvent(const AEvent: TAMQPServerEvent);
    property Total: Integer read FTotal;
  end;

var
  GBroker: TAMQPServer;
  GEventPrinter: TEventPrinter;
  GPort: Integer = 5672;
  GSeconds: Integer = 20;
  GAllEvents: Boolean = False;

procedure TEventPrinter.HandleServerEvent(const AEvent: TAMQPServerEvent);
var
  LWhere: string;
begin
  Inc(FTotal);
  LWhere := AmqpEventTypeName(AEvent.EventType);
  if AEvent.ChannelNumber > 0 then
    LWhere := LWhere + Format(' [channel %d]', [AEvent.ChannelNumber]);

  Write(Format('%4d  %-34s conn=%d', [FTotal, LWhere, AEvent.ConnectionId]));
  if AEvent.RemoteAddr <> '' then
    Write(' from ', AEvent.RemoteAddr);
  if AEvent.Username <> '' then
    Write(Format(' user=%s vhost=%s', [AEvent.Username, AEvent.VHost]));
  if AEvent.QueueName <> '' then
    Write(' queue=', AEvent.QueueName);
  if AEvent.ConsumerTag <> '' then
    Write(' tag=', AEvent.ConsumerTag);
  if AEvent.RoutingKey <> '' then
    Write(Format(' route=%s/%s', [AEvent.ExchangeName, AEvent.RoutingKey]));
  if AEvent.MessageSize > 0 then
    Write(Format(' %d bytes', [AEvent.MessageSize]));
  if AEvent.DeliveryTag > 0 then
    Write(Format(' delivery-tag=%d', [AEvent.DeliveryTag]));
  if AEvent.Reason <> '' then
    Write(' (', AEvent.Reason, ')');
  Writeln;
end;

procedure ReadArguments;
var
  I: Integer;
  LArg: string;
begin
  I := 1;
  while I <= ParamCount do
  begin
    LArg := ParamStr(I);
    if (LArg = '--port') and (I < ParamCount) then
    begin
      Inc(I);
      GPort := StrToIntDef(ParamStr(I), GPort);
    end
    else if (LArg = '--seconds') and (I < ParamCount) then
    begin
      Inc(I);
      GSeconds := StrToIntDef(ParamStr(I), GSeconds);
    end
    else if (LArg = '--types') and (I < ParamCount) then
    begin
      Inc(I);
      GAllEvents := ParamStr(I) = 'all';
    end;
    Inc(I);
  end;
end;

var
  LEnd: UInt64;
begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  {$IFDEF FPC}
  SetMultiByteConversionCodePage(CP_UTF8);
  {$ENDIF}
  ReadArguments;

  GEventPrinter := TEventPrinter.Create;
  GBroker := TAMQPServer.Create;
  try
    GBroker.BindAddress := '127.0.0.1';
    GBroker.Port := GPort;

    // Subscribing BEFORE Start is the normal case: whoever observes wants the first
    // event, not the second.
    if GAllEvents then
      GBroker.Subscribe(GEventPrinter.HandleServerEvent)
    else
      // Filtering by type costs a mask and avoids building a record nobody
      // will look at -- it is what D32 exists to allow.
      GBroker.Subscribe(GEventPrinter.HandleServerEvent,
        [seConnectionEstablished, seConnectionAuthenticated,
         seConnectionClosed, seChannelOpened, seChannelClosed,
         seMessagePublished, seMessagePublishRejected,
         seConsumerRegistered, seConsumerCancelled,
         seMessageAcked, seMessageNacked, seMessageRejected]);

    GBroker.Start;
    Writeln(Format('broker on 127.0.0.1:%d for %d s -- point a client at it',
      [GBroker.Port, GSeconds]));
    Writeln('');

    LEnd := PcTickMs + UInt64(GSeconds) * 1000;
    while PcTickMs < LEnd do
      Sleep(200);

    // Drain before reporting: delivery is asynchronous (D30), so without the
    // barrier the report would come out ahead of the last events.
    GBroker.DrainEvents(2000);
    Writeln('');
    Writeln(Format('events delivered:  %d', [GEventPrinter.Total]));
    Writeln(Format('events emitted:    %d', [GBroker.EventsEmitted]));
    // If this number is not zero, the handler did not keep up with the load -- it is
    // D31's best-effort charging its price, and the only way to know it was
    // charged.
    Writeln(Format('DROPPED:           %d', [GBroker.EventsDropped]));
    Writeln(Format('failed handlers:   %d', [GBroker.EventsFailed]));
    if GBroker.LastEventFailure <> '' then
      Writeln('last failure: ', GBroker.LastEventFailure);

    GBroker.Stop;
    // Unsubscribe BEFORE freeing the method's owner: it is D33's contract, and the
    // sample follows what the documentation tells the user to do.
    GBroker.Unsubscribe(GEventPrinter.HandleServerEvent);
  finally
    GBroker.Free;
    GEventPrinter.Free;
  end;
end.
