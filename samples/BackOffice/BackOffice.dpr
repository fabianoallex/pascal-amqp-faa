program BackOffice;

{ Scenario: POS -> authorizer -> back office. The authorizer (sample
  AuthorizerSim) publishes each invoice's reply to a queue; the back office
  consumes it and "fetches the XML" (here, simulated with a random Sleep), as if
  several POS terminals were waiting for a reply at the same time.

  Difference from the same scenario with other AMQP libs for Delphi:
  this lib's Channel.Consume already dispatches each delivery to its own
  thread pool (PcPool, inside TAMQPChannel.DispatchDelivery) - the callback
  below runs concurrently for different messages without any manual
  dispatch, and the read thread is never blocked waiting for
  ProcessKey to finish.

  Optional --dedicated argument: uses CreateChannel(True), which swaps the
  global (concurrent) pool for a fixed worker of the channel itself - deliveries
  processed one at a time, in the order they arrived.

  Compiles in both worlds from the SAME source:
    FPC:    fpc -Fu..\..\src -Fi..\..\src BackOffice.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src BackOffice.dpr

  Callbacks are `of object` (not anonymous; see CLAUDE.md): the state
  (ReadyInvoices/Lock) lives in TBackOfficeState, not in unit globals. }

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
  SyncObjs,
  Classes,
  Generics.Collections,
  AMQP.Connection,
  AMQP.Queue.Methods;

const
  QUEUE_NAME = 'authorizer-replies';

type
  TBackOfficeState = class
  private
    FReadyInvoices: TDictionary<string, string>;
    FLock: TCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
    // Writes to the console under the lock - several pool threads
    // calling Writeln at the same time scramble the output without it
    // (TCriticalSection is reentrant on the same thread).
    procedure Log(const S: string);
    // Simulates fetching the XML (API or database). Already runs on a pool thread.
    procedure ProcessKey(const InvoiceKey: string);
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure PrintStatus;
  end;

constructor TBackOfficeState.Create;
begin
  inherited Create;
  FReadyInvoices := TDictionary<string, string>.Create;
  FLock := TCriticalSection.Create;
end;

destructor TBackOfficeState.Destroy;
begin
  FLock.Free;
  FReadyInvoices.Free;
  inherited;
end;

procedure TBackOfficeState.Log(const S: string);
begin
  FLock.Enter;
  try
    Writeln(S);
  finally
    FLock.Leave;
  end;
end;

procedure TBackOfficeState.ProcessKey(const InvoiceKey: string);
var
  Xml: string;
  DelayMs: Integer;
begin
  DelayMs := 300 + Random(1200);
  Log(Format('[worker %d] fetching the XML of invoice %s (~%dms)',
    [TThread.CurrentThread.ThreadID, InvoiceKey, DelayMs]));
  Sleep(DelayMs);

  Xml := Format('<xml of invoice %s>', [InvoiceKey]);

  FLock.Enter;
  try
    FReadyInvoices.AddOrSetValue(InvoiceKey, Xml);
  finally
    FLock.Leave;
  end;

  Log(Format('[worker %d] invoice %s ready', [TThread.CurrentThread.ThreadID, InvoiceKey]));
end;

// ANoAck=False (the Consume default): we only ack after processing - an
// "at least once" guarantee out of the box, with no extra step.
procedure TBackOfficeState.OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  InvoiceKey: string;
begin
  InvoiceKey := ADelivery.BodyAsText;
  Log('[BackOffice] reply received from the queue: ' + InvoiceKey);
  try
    ProcessKey(InvoiceKey);
    AChannel.Ack(ADelivery.DeliveryTag);
  except
    AChannel.Nack(ADelivery.DeliveryTag, True); // requeue on error
  end;
end;

procedure TBackOfficeState.PrintStatus;
var
  Par: TPair<string, string>;
begin
  FLock.Enter;
  try
    Writeln('--- current status ---');
    if FReadyInvoices.Count = 0 then
      Writeln('(no invoice ready yet)')
    else
      for Par in FReadyInvoices do
        Writeln('  ', Par.Key, ' -> ', Par.Value);
  finally
    FLock.Leave;
  end;
end;

procedure Main;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChannel: TAMQPChannel;
  LState: TBackOfficeState;
  LConsumerTag: string;
  Line: string;
  LDedicated: Boolean;
begin
  LDedicated := (ParamCount > 0) and SameText(ParamStr(1), '--dedicated');
  LState := TBackOfficeState.Create;
  try
    LParams := TAMQPConnectionParams.Localhost;
    LConn := TAMQPConnection.Create(LParams);
    try
      LConn.Open;
      LChannel := LConn.CreateChannel(LDedicated);
      try
        LChannel.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_NAME, True));
        LChannel.Qos(10); // prefetch: caps the unacknowledged messages in flight

        LConsumerTag := LChannel.Consume(QUEUE_NAME, LState.OnDelivery);

        Writeln('[*] Waiting for replies on queue "', QUEUE_NAME, '".');
        if LDedicated then
          Writeln('[*] Dedicated thread on: deliveries processed in order, no concurrency.');
        Writeln('[*] Press ENTER at any time to see the status (or type "quit" + ENTER to close).');

        repeat
          Readln(Line);
          if SameText(Trim(Line), 'quit') then
            Break;
          LState.PrintStatus;
        until False;

        LChannel.Cancel(LConsumerTag);
      finally
        LChannel.Free;
      end;
    finally
      LConn.Free;
    end;
  finally
    LState.Free;
  end;
end;

begin
  Randomize;
  try
    Main;
  except
    on E: Exception do
      Writeln('Error: ', E.Message);
  end;
end.
