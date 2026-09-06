program AMQPServer;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  System.SyncObjs,
  AMQP.Server.Broker,
  AMQP.Server.Events,
  AMQP.Threading;

type
  TEventLogger = class
  private
    FLock: TCriticalSection;
    FEventCount: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    procedure OnEvent(const Event: TAMQPServerEvent);
    property EventCount: Integer read FEventCount;
  end;

var
  LBroker: TAMQPServer;
  LLogger: TEventLogger;

constructor TEventLogger.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FEventCount := 0;
end;

destructor TEventLogger.Destroy;
begin
  FLock.Free;
  inherited;
end;

procedure TEventLogger.OnEvent(const Event: TAMQPServerEvent);
begin
  FLock.Enter;
  try
    Inc(FEventCount);

    case Event.EventType of
      seConnectionEstablished:
        Writeln(Format('[%d] CONNECT: %s (ID: %d)',
          [FEventCount, Event.RemoteAddr, Event.ConnectionId]));

      seConnectionAuthenticated:
        Writeln(Format('[%d] AUTH: user=%s vhost=%s',
          [FEventCount, Event.Username, Event.VHost]));

      seConnectionClosed:
        Writeln(Format('[%d] CLOSE: %s (%s)',
          [FEventCount, Event.RemoteAddr, Event.Reason]));

      seMessagePublished:
        Writeln(Format('[%d] PUBLISH: %s -> %s.%s (%d bytes, user=%s)',
          [FEventCount, Event.RemoteAddr, Event.ExchangeName, Event.RoutingKey,
           Event.MessageSize, Event.Username]));

      seMessagePublishRejected:
        Writeln(Format('[%d] PUBLISH REJECTED: %s (%s)',
          [FEventCount, Event.ExchangeName, Event.Reason]));

      seMessageEnqueued:
        Writeln(Format('[%d] ENQUEUE: %s (priority=%d, ttl=%d ms)',
          [FEventCount, Event.QueueName, Event.MessagePriority,
           Event.MessageExpirationMs]));

      seMessageDropped:
        Writeln(Format('[%d] DROP: %s (%s)',
          [FEventCount, Event.QueueName, Event.Reason]));

      seConsumerRegistered:
        Writeln(Format('[%d] CONSUME: %s by %s',
          [FEventCount, Event.QueueName, Event.ConsumerTag]));

      seConsumerCancelled:
        Writeln(Format('[%d] CANCEL: %s (%s)',
          [FEventCount, Event.QueueName, Event.ConsumerTag]));

      seMessageDelivered:
        Writeln(Format('[%d] DELIVER: %s to %s (tag=%d, redelivered=%s)',
          [FEventCount, Event.QueueName, Event.ConsumerTag, Event.DeliveryTag,
           BoolToStr(Event.Redelivered, True)]));

      seMessageAcked:
        Writeln(Format('[%d] ACK: %s (tag=%d)',
          [FEventCount, Event.QueueName, Event.DeliveryTag]));

      seMessageNacked:
        Writeln(Format('[%d] NACK: %s (tag=%d, %s)',
          [FEventCount, Event.QueueName, Event.DeliveryTag, Event.Reason]));

      seMessageRejected:
        Writeln(Format('[%d] REJECT: %s (tag=%d, %s)',
          [FEventCount, Event.QueueName, Event.DeliveryTag, Event.Reason]));

      seMessageExpired:
        Writeln(Format('[%d] EXPIRED: %s (ttl=%d ms)',
          [FEventCount, Event.QueueName, Event.MessageExpirationMs]));

      seMessageDeadLettered:
        Writeln(Format('[%d] DLX: %s -> DLX (%s)',
          [FEventCount, Event.QueueName, Event.Reason]));

      seJournalFlushed:
        Writeln(Format('[%d] FSYNC: LSN=%d', [FEventCount, Event.JournalLsn]));
    end;
  finally
    FLock.Leave;
  end;
end;

begin
  ReportMemoryLeaksOnShutdown := True;

  LLogger := TEventLogger.Create;
  LBroker := TAMQPServer.Create;
  try
    LBroker.BindAddress := '127.0.0.1';
    LBroker.Port := 5672;

    { Registra handler de observabilidade }
    LBroker.Subscribe(LLogger.OnEvent);

    Writeln('Iniciando broker AMQP em 127.0.0.1:5672');
    Writeln('Capturando eventos de observabilidade...');
    Writeln('Pressione Ctrl+C para parar.');
    Writeln('');

    LBroker.Start;

    while True do
    begin
      Sleep(5000);
      Writeln(Format('Status: %d conexoes, %d eventos capturados',
        [LBroker.ConnectionCount, LLogger.EventCount]));
    end;
  finally
    LBroker.Free;
    LLogger.Free;
  end;
end.

{
begin
  try
    { TODO -oUser -cConsole Main : Insert code here }
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
}