unit AMQP.ServerTestDoubles;

{ Test doubles shared by the broker suites -- WS4 of Phase 2.

  Lives in a unit of its own because two fixtures need the same fake
  delivery target: the queue actor's (AMQP.ServerQueueTests, where consumers
  exist only to be counted and the target must REFUSE everything so the backlog
  stays where it is) and the delivery one (AMQP.ServerDeliveryTests, where the
  target records what it received).

  A single copy, compiled by the Delphi and FPC runners. }

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Wire,
  AMQP.Server.Message,
  AMQP.Server.Queue,
  AMQP.Server.Wal,
  AMQP.Protocol,
  AMQP.Frame,
  PascalCommon.ThreadPool,
  AMQP.Server.FrameIO;

type
  { A delivery recorded by the fake target. }
  TDeliveryRecord = record
    ConsumerTag: string;
    QueueName: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    BodyText: string;
  end;

  { Fake delivery target: records what it received and can be told to refuse or
    die. Refcounted like the real one -- the queue holds the interface, so the
    object survives as long as the queue has it in hand. }
  TFakeTarget = class(TInterfacedObject, IAMQPDeliveryTarget)
  private
    FLock: TCriticalSection;
    FDeliveries: TList<TDeliveryRecord>;
    FCancelled: TStringList;
    FId: NativeUInt;
    FNextTag: UInt64;
    FRefuse: Boolean;
    FAlive: Boolean;
  public
    constructor Create(AId: NativeUInt);
    destructor Destroy; override;

    function ChannelId: NativeUInt;
    function IsAlive: Boolean;
    function TryDeliver(const AConsumerTag, AQueueName: string;
      ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
      out ADeliveryTag: UInt64): Boolean;
    function TryCancel(const AConsumerTag: string): Boolean;

    function Count: Integer;
    function DeliveryAt(AIndex: Integer): TDeliveryRecord;
    /// Delivered bodies, in order, separated by '|'.
    function Bodies: string;
    /// Consumer tags that received Basic.Cancel (queue deleted under them).
    function Cancelled: string;
    function CancelledCount: Integer;
    property Refuse: Boolean read FRefuse write FRefuse;
    property Alive: Boolean read FAlive write FAlive;
  end;

/// A LIVE target that refuses every delivery. It is what the queue-actor tests use
/// when the consumer exists only to be counted: alive (otherwise the actor takes it
/// out of the round-robin) but without consuming the backlog.
function RefusingTarget(AId: NativeUInt): IAMQPDeliveryTarget;

type
  { In-memory WAL file: counts fsyncs, can fail on write and lets the
    test touch the raw bytes (simulates a crash and corruption without touching disk).

    It is LAYER 2 of D28 (Phase 4): since TAMQPWalSegment and TAMQPJournal talk to
    IAMQPWalFile and not to the file system, we can prove that the fsync
    happens at the batch boundary and that an I/O failure does not corrupt
    in-memory state -- neither would be observable from outside.

    Protected by a lock because in the journal the journal thread and the test
    thread touch it at the same time. }
  TAMQPFakeWalFile = class(TInterfacedObject, IAMQPWalFile)
  private
    FLock: TCriticalSection;
    FBuf: TBytes;
    FSyncs: Integer;
    FSyncOk: Boolean;
    FWriteCount: Integer;
    FFailWriteAt: Integer; // -1 = never
  public
    constructor Create;
    destructor Destroy; override;

    function Append(const ABytes: TBytes): Int64;
    function ReadAll: TBytes;
    function Sync: Boolean;
    function Size: Int64;
    procedure TruncateTo(ASize: Int64);

    /// Copy of the raw bytes, for the test to corrupt and put back.
    function Bytes: TBytes;
    procedure PutBytes(const ABytes: TBytes);
    function Syncs: Integer;
    function WriteCount: Integer;
    procedure SetSyncOk(AValue: Boolean);
    /// Number of the write (1-based) on which Append will raise. -1 = never.
    procedure SetFailWriteAt(AValue: Integer);
  end;

  { Wraps a REAL WAL file and lets the fsync be failed.

    Unlike TAMQPFakeWalFile (which lives only in memory and serves a single
    segment), this one exists because compaction needs REAL FILES:
    what it deletes are directory entries, and the test that matters is "a failed
    fsync deletes NOTHING". With a purely in-memory double there is no
    segment on disk to delete, and the test would pass vacuously. }
  TAMQPFailingWalFile = class(TInterfacedObject, IAMQPWalFile)
  private
    FInner: IAMQPWalFile;
    FSyncOk: Boolean;
  public
    constructor Create(const AInner: IAMQPWalFile);
    function Append(const ABytes: TBytes): Int64;
    function ReadAll: TBytes;
    function Sync: Boolean;
    function Size: Int64;
    procedure TruncateTo(ASize: Int64);
    /// False makes every fsync of this file fail.
    procedure SetSyncOk(AValue: Boolean);
  end;

  { Test stream: keeps what was written and can BLOCK the writer thread
    (to fill the writer's outbound queue on purpose).

    Lives here, and not in the delivery suite where it was born, since WS5 of Phase 4:
    the asynchronous confirm suite needs exactly the same pair -- a real
    writer over a recording stream -- to READ the frames the tracker
    emitted. Two copies would diverge. }
  TRecordingStream = class(TStream)
  private
    FLock: TCriticalSection;
    FLog: TBytes;
    FLen: Integer;
    FGate: TEvent; // signaled = passes
  public
    constructor Create;
    destructor Destroy; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64; override;
    procedure Block;
    procedure Unblock;
    function Bytes: TBytes;
  end;

/// Waits for the writer thread to empty (STOPPING it) and returns the frames written
/// so far. After this the writer can no longer be used for writing.
function WrittenFrames(AWriter: TAMQPFrameWriter;
  AStream: TRecordingStream): TArray<TAMQPFrame>;

implementation

function RefusingTarget(AId: NativeUInt): IAMQPDeliveryTarget;
var
  LTarget: TFakeTarget;
begin
  LTarget := TFakeTarget.Create(AId);
  LTarget.Refuse := True;
  Result := LTarget;
end;

{ TFakeTarget }

constructor TFakeTarget.Create(AId: NativeUInt);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FDeliveries := TList<TDeliveryRecord>.Create;
  FCancelled := TStringList.Create;
  FId := AId;
  FNextTag := 1;
  FAlive := True;
end;

destructor TFakeTarget.Destroy;
begin
  FDeliveries.Free;
  FCancelled.Free;
  FLock.Free;
  inherited;
end;

function TFakeTarget.ChannelId: NativeUInt;
begin
  Result := FId;
end;

function TFakeTarget.IsAlive: Boolean;
begin
  Result := FAlive;
end;

function TFakeTarget.TryDeliver(const AConsumerTag, AQueueName: string;
  ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
  out ADeliveryTag: UInt64): Boolean;
var
  LRec: TDeliveryRecord;
begin
  ADeliveryTag := 0;
  if FRefuse or (not FAlive) then
    Exit(False);
  FLock.Enter;
  try
    LRec.ConsumerTag := AConsumerTag;
    LRec.QueueName := AQueueName;
    LRec.DeliveryTag := FNextTag;
    LRec.Redelivered := ARedelivered;
    LRec.BodyText := AmqpUtf8Decode(AMessage.Body);
    FDeliveries.Add(LRec);
    ADeliveryTag := FNextTag;
    Inc(FNextTag);
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TFakeTarget.TryCancel(const AConsumerTag: string): Boolean;
begin
  FLock.Enter;
  try
    FCancelled.Add(AConsumerTag);
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TFakeTarget.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FDeliveries.Count;
  finally
    FLock.Leave;
  end;
end;

function TFakeTarget.DeliveryAt(AIndex: Integer): TDeliveryRecord;
begin
  FLock.Enter;
  try
    Result := FDeliveries[AIndex];
  finally
    FLock.Leave;
  end;
end;

function TFakeTarget.Cancelled: string;
begin
  FLock.Enter;
  try
    Result := FCancelled.CommaText;
  finally
    FLock.Leave;
  end;
end;

function TFakeTarget.CancelledCount: Integer;
begin
  FLock.Enter;
  try
    Result := FCancelled.Count;
  finally
    FLock.Leave;
  end;
end;

function TFakeTarget.Bodies: string;
var
  I: Integer;
begin
  Result := '';
  FLock.Enter;
  try
    for I := 0 to FDeliveries.Count - 1 do
    begin
      if Result <> '' then
        Result := Result + '|';
      Result := Result + FDeliveries[I].BodyText;
    end;
  finally
    FLock.Leave;
  end;
end;


{ TAMQPFakeWalFile }

constructor TAMQPFakeWalFile.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FSyncOk := True;
  FFailWriteAt := -1;
end;

destructor TAMQPFakeWalFile.Destroy;
begin
  FLock.Free;
  inherited Destroy;
end;

function TAMQPFakeWalFile.Append(const ABytes: TBytes): Int64;
var
  LBefore: Integer;
begin
  FLock.Enter;
  try
    Inc(FWriteCount);
    if FWriteCount = FFailWriteAt then
      raise EAMQPWal.Create('falha de escrita injetada');
    LBefore := Length(FBuf);
    SetLength(FBuf, LBefore + Length(ABytes));
    if Length(ABytes) > 0 then
      Move(ABytes[0], FBuf[LBefore], Length(ABytes));
    Result := LBefore;
  finally
    FLock.Leave;
  end;
end;

function TAMQPFakeWalFile.ReadAll: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FBuf, 0, Length(FBuf));
  finally
    FLock.Leave;
  end;
end;

function TAMQPFakeWalFile.Sync: Boolean;
begin
  FLock.Enter;
  try
    Inc(FSyncs);
    Result := FSyncOk;
  finally
    FLock.Leave;
  end;
end;

function TAMQPFakeWalFile.Size: Int64;
begin
  FLock.Enter;
  try
    Result := Length(FBuf);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPFakeWalFile.TruncateTo(ASize: Int64);
begin
  FLock.Enter;
  try
    SetLength(FBuf, Integer(ASize));
  finally
    FLock.Leave;
  end;
end;

function TAMQPFakeWalFile.Bytes: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FBuf, 0, Length(FBuf));
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPFakeWalFile.PutBytes(const ABytes: TBytes);
begin
  FLock.Enter;
  try
    FBuf := Copy(ABytes, 0, Length(ABytes));
  finally
    FLock.Leave;
  end;
end;

function TAMQPFakeWalFile.Syncs: Integer;
begin
  FLock.Enter;
  try
    Result := FSyncs;
  finally
    FLock.Leave;
  end;
end;

function TAMQPFakeWalFile.WriteCount: Integer;
begin
  FLock.Enter;
  try
    Result := FWriteCount;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPFakeWalFile.SetSyncOk(AValue: Boolean);
begin
  FLock.Enter;
  try
    FSyncOk := AValue;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPFakeWalFile.SetFailWriteAt(AValue: Integer);
begin
  FLock.Enter;
  try
    FFailWriteAt := AValue;
  finally
    FLock.Leave;
  end;
end;

{ TAMQPFailingWalFile }

constructor TAMQPFailingWalFile.Create(const AInner: IAMQPWalFile);
begin
  inherited Create;
  FInner := AInner;
  FSyncOk := True;
end;

function TAMQPFailingWalFile.Append(const ABytes: TBytes): Int64;
begin
  Result := FInner.Append(ABytes);
end;

function TAMQPFailingWalFile.ReadAll: TBytes;
begin
  Result := FInner.ReadAll;
end;

function TAMQPFailingWalFile.Sync: Boolean;
begin
  // The BYTES really go to disk; only the fsync's ANSWER is failed.
  // It models a disk that accepted the write and did not guarantee the barrier.
  FInner.Sync;
  Result := FSyncOk;
end;

function TAMQPFailingWalFile.Size: Int64;
begin
  Result := FInner.Size;
end;

procedure TAMQPFailingWalFile.TruncateTo(ASize: Int64);
begin
  FInner.TruncateTo(ASize);
end;

procedure TAMQPFailingWalFile.SetSyncOk(AValue: Boolean);
begin
  FSyncOk := AValue;
end;

{ TRecordingStream }

constructor TRecordingStream.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FGate := TEvent.Create(nil, True, True, ''); // manual-reset, open
end;

destructor TRecordingStream.Destroy;
begin
  FGate.Free;
  FLock.Free;
  inherited;
end;

procedure TRecordingStream.Block;
begin
  FGate.ResetEvent;
end;

procedure TRecordingStream.Unblock;
begin
  FGate.SetEvent;
end;

function TRecordingStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := 0;
end;

function TRecordingStream.Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64;
begin
  Result := 0;
end;

function TRecordingStream.Write(const Buffer; Count: Longint): Longint;
var
  P: PByte;
  I: Integer;
begin
  FGate.WaitFor(PC_WAIT_INFINITE);
  FLock.Enter;
  try
    if FLen + Count > Length(FLog) then
      SetLength(FLog, (FLen + Count) * 2 + 64);
    P := @Buffer;
    for I := 0 to Count - 1 do
    begin
      FLog[FLen] := P[I];
      Inc(FLen);
    end;
  finally
    FLock.Leave;
  end;
  Result := Count;
end;

function TRecordingStream.Bytes: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FLog, 0, FLen);
  finally
    FLock.Leave;
  end;
end;

function ReadFrames(const ABytes: TBytes): TArray<TAMQPFrame>;
var
  LMem: TBytesStream;
  LList: TList<TAMQPFrame>;
begin
  LList := TList<TAMQPFrame>.Create;
  try
    LMem := TBytesStream.Create(ABytes);
    try
      while LMem.Position < LMem.Size do
        LList.Add(TAMQPFrame.ReadFrom(LMem));
    finally
      LMem.Free;
    end;
    Result := LList.ToArray;
  finally
    LList.Free;
  end;
end;

function WrittenFrames(AWriter: TAMQPFrameWriter;
  AStream: TRecordingStream): TArray<TAMQPFrame>;
begin
  AWriter.Stop; // drains whatever is queued and ends the writer thread
  Result := ReadFrames(AStream.Bytes);
end;

end.
