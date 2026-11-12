program AMQPServer;

{ Embedded AMQP broker with observability turned on (Phase 4.1).

  Same source for FPC and Delphi, like the SmokeTest. Brings up a broker on
  127.0.0.1:5672, subscribes to the events and prints each one. It stops on its
  own after --segundos (default 20), to serve as a verification step and not
  only as a demo.

    AMQPServer.exe [--porta N] [--segundos N] [--tipos lifecycle|todos]

  FPC:     lazbuild samples\Server\AMQPServer.lpi
  Delphi:  open samples\Server\AMQPServer.dproj in the IDE (the CE has no CLI).

  WHAT THIS SAMPLE SHOWS, and it is the only way to see D30 working:
  the handler runs on the broker's NOTIFIER thread, never on a connection's read
  thread and never on a pool worker. That is why it can write to the
  console -- which is slow and serialized -- without delaying the broker. The
  price is in the final report: under a burst, the DROPPED counter goes up, and
  that is exactly the best-effort contract of D31. Events are not an audit log.

  'todos' also brings the actor-origin types (enqueue/deliver/expire/DLX/drop)
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
  TImpressor = class
  private
    FTotal: Integer;
  public
    procedure AoEvento(const AEvent: TAMQPServerEvent);
    property Total: Integer read FTotal;
  end;

var
  GBroker: TAMQPServer;
  GImpressor: TImpressor;
  GPorta: Integer = 5672;
  GSegundos: Integer = 20;
  GTodos: Boolean = False;

procedure TImpressor.AoEvento(const AEvent: TAMQPServerEvent);
var
  LOnde: string;
begin
  Inc(FTotal);
  LOnde := AmqpEventTypeName(AEvent.EventType);
  if AEvent.ChannelNumber > 0 then
    LOnde := LOnde + Format(' [canal %d]', [AEvent.ChannelNumber]);

  Write(Format('%4d  %-34s conn=%d', [FTotal, LOnde, AEvent.ConnectionId]));
  if AEvent.RemoteAddr <> '' then
    Write(' de ', AEvent.RemoteAddr);
  if AEvent.Username <> '' then
    Write(Format(' user=%s vhost=%s', [AEvent.Username, AEvent.VHost]));
  if AEvent.QueueName <> '' then
    Write(' fila=', AEvent.QueueName);
  if AEvent.ConsumerTag <> '' then
    Write(' tag=', AEvent.ConsumerTag);
  if AEvent.RoutingKey <> '' then
    Write(Format(' rota=%s/%s', [AEvent.ExchangeName, AEvent.RoutingKey]));
  if AEvent.MessageSize > 0 then
    Write(Format(' %d bytes', [AEvent.MessageSize]));
  if AEvent.DeliveryTag > 0 then
    Write(Format(' tag-entrega=%d', [AEvent.DeliveryTag]));
  if AEvent.Reason <> '' then
    Write(' (', AEvent.Reason, ')');
  Writeln;
end;

procedure LeArgumentos;
var
  I: Integer;
  LArg: string;
begin
  I := 1;
  while I <= ParamCount do
  begin
    LArg := ParamStr(I);
    if (LArg = '--porta') and (I < ParamCount) then
    begin
      Inc(I);
      GPorta := StrToIntDef(ParamStr(I), GPorta);
    end
    else if (LArg = '--segundos') and (I < ParamCount) then
    begin
      Inc(I);
      GSegundos := StrToIntDef(ParamStr(I), GSegundos);
    end
    else if (LArg = '--tipos') and (I < ParamCount) then
    begin
      Inc(I);
      GTodos := ParamStr(I) = 'todos';
    end;
    Inc(I);
  end;
end;

var
  LFim: UInt64;
begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  {$IFDEF FPC}
  SetMultiByteConversionCodePage(CP_UTF8);
  {$ENDIF}
  LeArgumentos;

  GImpressor := TImpressor.Create;
  GBroker := TAMQPServer.Create;
  try
    GBroker.BindAddress := '127.0.0.1';
    GBroker.Port := GPorta;

    // Subscribing BEFORE Start is the normal case: whoever observes wants the first
    // event, not the second.
    if GTodos then
      GBroker.Subscribe(GImpressor.AoEvento)
    else
      // Filtering by type costs a mask and avoids building a record nobody
      // will look at -- it is what D32 exists to allow.
      GBroker.Subscribe(GImpressor.AoEvento,
        [seConnectionEstablished, seConnectionAuthenticated,
         seConnectionClosed, seChannelOpened, seChannelClosed,
         seMessagePublished, seMessagePublishRejected,
         seConsumerRegistered, seConsumerCancelled,
         seMessageAcked, seMessageNacked, seMessageRejected]);

    GBroker.Start;
    Writeln(Format('broker em 127.0.0.1:%d por %d s -- aponte um cliente nele',
      [GBroker.Port, GSegundos]));
    Writeln('');

    LFim := PcTickMs + UInt64(GSegundos) * 1000;
    while PcTickMs < LFim do
      Sleep(200);

    // Drain before reporting: delivery is asynchronous (D30), so without the
    // barrier the report would come out ahead of the last events.
    GBroker.DrainEvents(2000);
    Writeln('');
    Writeln(Format('eventos entregues:  %d', [GImpressor.Total]));
    Writeln(Format('eventos emitidos:   %d', [GBroker.EventsEmitted]));
    // If this number is not zero, the handler did not keep up with the load -- it is
    // D31's best-effort charging its price, and the only way to know it was
    // charged.
    Writeln(Format('DESCARTADOS:        %d', [GBroker.EventsDropped]));
    Writeln(Format('handlers que falharam: %d', [GBroker.EventsFailed]));
    if GBroker.LastEventFailure <> '' then
      Writeln('ultima falha: ', GBroker.LastEventFailure);

    GBroker.Stop;
    // Unsubscribe BEFORE freeing the method's owner: it is D33's contract, and the
    // sample follows what the documentation tells the user to do.
    GBroker.Unsubscribe(GImpressor.AoEvento);
  finally
    GBroker.Free;
    GImpressor.Free;
  end;
end.
