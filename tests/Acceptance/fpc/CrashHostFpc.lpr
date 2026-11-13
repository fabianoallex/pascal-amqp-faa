program CrashHostFpc;

{ Crash-safety host -- WS8 of Phase 4 (durability, D28 layer 3).

  WHAT IT IS, AND WHAT IT IS NOT
  ------------------------------
  This program starts the broker with DataDir, does ONE job, prints a marker
  and STAYS ALIVE. The one who kills it is the driver
  (tests/tools/crash_matrix.py), from outside, without warning. It is real
  process death: no destructor runs, no Stop is called, the journal does not
  drain.

  WHAT THIS DOES NOT TEST, AND IT IS IN D28 IN THESE WORDS: **killing the
  process does NOT test fsync**. The operating system's cache survives the
  death of the process -- only a crash of the MACHINE loses it. What this
  matrix proves is something else, and it is worth having on its own: that
  what the broker wrote is enough to rebuild the state, that a torn tail does
  not prevent the boot, and that what was confirmed as consumed does not come
  back to life.

  The other two layers of D28 already live in the suite: the exhaustive
  truncation sweep (WS1) and the file test double that counts fsyncs and fails
  at the chosen moment (WS1/WS2, plus WS7's TAMQPFailingWalFile). This is the
  third, and it lives OUTSIDE the runner for the same reason as the SmokeTest:
  a test that needs to kill its own process does not fit in a runner that
  wants to count assertions.

  MODES

    CrashHostFpc publish <dir> <n> [start]
                                     starts, publishes n persistent messages
                                     with confirm, numbered from `start` (1
                                     if omitted), prints PUBLISHED n and
                                     waits to be killed. `start` exists so
                                     that several crashes in a row produce a
                                     GLOBALLY increasing sequence -- otherwise
                                     verify would report "out of order" just
                                     because each round restarted from 1
    CrashHostFpc consume <dir> <n>   starts, consumes and acks n, prints
                                     CONSUMED n and waits to be killed
    CrashHostFpc dump <dir>          starts, publishes nonstop printing
                                     CONFIRMED <seq> on each confirm -- lets
                                     the driver kill IN THE MIDDLE of a write
    CrashHostFpc verify <dir>        starts, counts what came back, prints
                                     RECOVERED n and EXITS (this one shuts
                                     down cleanly). It does NOT CONSUME: it
                                     gets in ack mode and never acks, so
                                     closing the connection returns everything
                                     to the queue. That is what allows
                                     checking between one crash and the next
                                     without changing what is being measured.

  The queue is always q.crash, durable, and each message's body is its own
  number -- so "verify" checks integrity, not just count.

  A CAVEAT THAT COST HALF AN HOUR: Halt is NOT used here outside the main
  block. `Halt` abandons the frames without finalizing the MANAGED locals
  (strings, TBytes, interfaces) of the routines on the stack -- and heaptrc
  reports that as a leak, correctly. The modes return control and the end of
  the program is what shuts down; an error becomes `ExitCode`. }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  SysUtils,
  AMQP.Queue.Methods,
  AMQP.Connection,
  AMQP.Server.Types,
  AMQP.Server.Broker;

const
  QUEUE_NAME = 'q.crash';

var
  GBroker: TAMQPServer;
  GConn: TAMQPConnection;
  GChan: TAMQPChannel;

procedure Say(const AMsg: string);
begin
  WriteLn(AMsg);
  Flush(Output);
end;

// Starts the broker on the directory and connects a client to it. Ephemeral
// port: the driver does not need to know it, because client and broker are in
// the same process.
procedure StartUp(const ADir: string);
var
  LParams: TAMQPConnectionParams;
begin
  GBroker := TAMQPServer.Create;
  GBroker.BindAddress := '127.0.0.1';
  GBroker.Port := 0;
  GBroker.DataDir := ADir;
  GBroker.Start;
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := GBroker.Port;
  GConn := TAMQPConnection.Create(LParams);
  GConn.Open;
  GChan := GConn.CreateChannel;
  GChan.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_NAME, True));
end;

/// Stays alive until someone kills the process. There is NO clean exit from
/// here: that is the point of the exercise.
procedure WaitForDeath;
begin
  while True do
    Sleep(50);
end;

procedure ModePublish(const ADir: string; AN, AStart: Integer);
var
  I: Integer;
begin
  StartUp(ADir);
  GChan.ConfirmSelect;
  for I := AStart to AStart + AN - 1 do
    GChan.PublishText('', QUEUE_NAME, IntToStr(I), True);
  if not GChan.WaitForConfirms(30000) then
  begin
    Say('ERROR not everything was confirmed');
    ExitCode := 2;
    Exit;
  end;
  Say('PUBLISHED ' + IntToStr(AN));
  WaitForDeath;
end;

procedure ModeConsume(const ADir: string; AN: Integer);
var
  I: Integer;
  LGet: TAMQPGetResult;
begin
  StartUp(ADir);
  for I := 1 to AN do
  begin
    LGet := GChan.BasicGet(QUEUE_NAME, False); // ack mode
    if not LGet.Found then
    begin
      Say('ERROR queue empty at ' + IntToStr(I));
      ExitCode := 2;
      Exit;
    end;
    GChan.Ack(LGet.DeliveryTag);
  end;
  // The ack travels and the actor still has to process it and write the DEQ.
  // Without this wait the test would measure the race, not durability.
  Sleep(1000);
  Say('CONSUMED ' + IntToStr(AN));
  WaitForDeath;
end;

procedure ModeFlood(const ADir: string);
var
  I: Integer;
begin
  StartUp(ADir);
  GChan.ConfirmSelect;
  I := 0;
  while True do
  begin
    Inc(I);
    GChan.PublishText('', QUEUE_NAME, IntToStr(I), True);
    if GChan.WaitForConfirms(30000) then
      Say('CONFIRMED ' + IntToStr(I))
    else
    begin
      Say('NACK ' + IntToStr(I));
      ExitCode := 3;
      Exit;
    end;
  end;
end;

procedure ModeVerify(const ADir: string);
var
  LGet: TAMQPGetResult;
  LN: Integer;
  LOutOfOrder: Boolean;
  LLast, LCurrent: Integer;
begin
  StartUp(ADir);
  LN := 0;
  LOutOfOrder := False;
  LLast := 0;
  while True do
  begin
    // ACK MODE, and it NEVER acks: closing the connection returns them all to
    // the queue. A no-ack here would empty the queue -- and the checker would
    // start changing what it measures, which would spoil any scenario with
    // more than one crash.
    LGet := GChan.BasicGet(QUEUE_NAME, False);
    if not LGet.Found then
      Break;
    Inc(LN);
    // The body IS the number: it lets us check INTEGRITY and ORDER, not just
    // count. A recovery that returned the right messages in the wrong order
    // would pass a test that only counted.
    if TryStrToInt(LGet.BodyAsText, LCurrent) then
    begin
      if LCurrent <= LLast then
        LOutOfOrder := True;
      LLast := LCurrent;
    end
    else
      LOutOfOrder := True;
  end;
  if LOutOfOrder then
    Say('OUT-OF-ORDER');
  Say('RECOVERED ' + IntToStr(LN));
  GConn.Close;
  GConn.Free;
  // The unacknowledged ones go back to the queue in the connection teardown,
  // and that happens in the ACTOR. Stopping the broker without letting that
  // round trip happen would lose from MEMORY what is still on disk -- it would
  // not be data loss, but the next verify would measure a race.
  Sleep(1000);
  GBroker.Stop;
  GBroker.Free;
  if LOutOfOrder then
    ExitCode := 4;
end;

var
  LMode, LDir: string;
  LN, LStart: Integer;
begin
  if ParamCount < 2 then
  begin
    Say('usage: CrashHostFpc <publish|consume|dump|verify> <dir> [n] [start]');
    ExitCode := 1;
    Exit;
  end;
  LMode := LowerCase(ParamStr(1));
  LDir := ParamStr(2);
  ForceDirectories(LDir);
  LN := 0;
  if ParamCount >= 3 then
    LN := StrToIntDef(ParamStr(3), 0);
  LStart := 1;
  if ParamCount >= 4 then
    LStart := StrToIntDef(ParamStr(4), 1);

  if LMode = 'publish' then
    ModePublish(LDir, LN, LStart)
  else if LMode = 'consume' then
    ModeConsume(LDir, LN)
  else if LMode = 'dump' then
    ModeFlood(LDir)
  else if LMode = 'verify' then
    ModeVerify(LDir)
  else
  begin
    Say('unknown mode: ' + LMode);
    ExitCode := 1;
  end;
end.
