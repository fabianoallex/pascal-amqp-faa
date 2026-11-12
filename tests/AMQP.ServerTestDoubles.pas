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
  TEntregaReg = record
    ConsumerTag: string;
    QueueName: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Corpo: string;
  end;

  { Fake delivery target: records what it received and can be told to refuse or
    die. Refcounted like the real one -- the queue holds the interface, so the
    object survives as long as the queue has it in hand. }
  TAlvoFalso = class(TInterfacedObject, IAMQPDeliveryTarget)
  private
    FLock: TCriticalSection;
    FEntregas: TList<TEntregaReg>;
    FCancelados: TStringList;
    FId: NativeUInt;
    FNextTag: UInt64;
    FRecusar: Boolean;
    FVivo: Boolean;
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
    function Entrega(AIndex: Integer): TEntregaReg;
    /// Delivered bodies, in order, separated by '|'.
    function Corpos: string;
    /// Consumer tags that received Basic.Cancel (queue deleted under them).
    function Cancelados: string;
    function CanceladosCount: Integer;
    property Recusar: Boolean read FRecusar write FRecusar;
    property Vivo: Boolean read FVivo write FVivo;
  end;

/// A LIVE target that refuses every delivery. It is what the queue-actor tests use
/// when the consumer exists only to be counted: alive (otherwise the actor takes it
/// out of the round-robin) but without consuming the backlog.
function AlvoQueRecusa(AId: NativeUInt): IAMQPDeliveryTarget;

type
  { In-memory WAL file: counts fsyncs, can fail on write and lets the
    test touch the raw bytes (simulates a crash and corruption without touching disk).

    It is LAYER 2 of D28 (Phase 4): since TAMQPWalSegment and TAMQPJournal talk to
    IAMQPWalFile and not to the file system, we can prove that the fsync
    happens at the batch boundary and that an I/O failure does not corrupt
    in-memory state -- neither would be observable from outside.

    Protected by a lock because in the journal the journal thread and the test
    thread touch it at the same time. }
  TAMQPWalFileFalso = class(TInterfacedObject, IAMQPWalFile)
  private
    FLock: TCriticalSection;
    FBuf: TBytes;
    FSyncs: Integer;
    FSyncOk: Boolean;
    FEscritas: Integer;
    FFalharEscritaNa: Integer; // -1 = never
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
    procedure PoeBytes(const ABytes: TBytes);
    function Syncs: Integer;
    function Escritas: Integer;
    procedure SetSyncOk(AValue: Boolean);
    /// Number of the write (1-based) on which Append will raise. -1 = never.
    procedure SetFalharEscritaNa(AValue: Integer);
  end;

  { Wraps a REAL WAL file and lets the fsync be failed.

    Unlike TAMQPWalFileFalso (which lives only in memory and serves a single
    segment), this one exists because compaction needs REAL FILES:
    what it deletes are directory entries, and the test that matters is "a failed
    fsync deletes NOTHING". With a purely in-memory double there is no
    segment on disk to delete, and the test would pass vacuously. }
  TAMQPWalFileComFalha = class(TInterfacedObject, IAMQPWalFile)
  private
    FInterno: IAMQPWalFile;
    FSyncOk: Boolean;
  public
    constructor Create(const AInterno: IAMQPWalFile);
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
  TStreamGravador = class(TStream)
  private
    FLock: TCriticalSection;
    FLog: TBytes;
    FLen: Integer;
    FPortao: TEvent; // signaled = passes
  public
    constructor Create;
    destructor Destroy; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64; override;
    procedure Bloquear;
    procedure Liberar;
    function Bytes: TBytes;
  end;

/// Waits for the writer thread to empty (STOPPING it) and returns the frames written
/// so far. After this the writer can no longer be used for writing.
function FramesEscritos(AWriter: TAMQPFrameWriter;
  AStream: TStreamGravador): TArray<TAMQPFrame>;

implementation

function AlvoQueRecusa(AId: NativeUInt): IAMQPDeliveryTarget;
var
  LAlvo: TAlvoFalso;
begin
  LAlvo := TAlvoFalso.Create(AId);
  LAlvo.Recusar := True;
  Result := LAlvo;
end;

{ TAlvoFalso }

constructor TAlvoFalso.Create(AId: NativeUInt);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FEntregas := TList<TEntregaReg>.Create;
  FCancelados := TStringList.Create;
  FId := AId;
  FNextTag := 1;
  FVivo := True;
end;

destructor TAlvoFalso.Destroy;
begin
  FEntregas.Free;
  FCancelados.Free;
  FLock.Free;
  inherited;
end;

function TAlvoFalso.ChannelId: NativeUInt;
begin
  Result := FId;
end;

function TAlvoFalso.IsAlive: Boolean;
begin
  Result := FVivo;
end;

function TAlvoFalso.TryDeliver(const AConsumerTag, AQueueName: string;
  ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
  out ADeliveryTag: UInt64): Boolean;
var
  LReg: TEntregaReg;
begin
  ADeliveryTag := 0;
  if FRecusar or (not FVivo) then
    Exit(False);
  FLock.Enter;
  try
    LReg.ConsumerTag := AConsumerTag;
    LReg.QueueName := AQueueName;
    LReg.DeliveryTag := FNextTag;
    LReg.Redelivered := ARedelivered;
    LReg.Corpo := AmqpUtf8Decode(AMessage.Body);
    FEntregas.Add(LReg);
    ADeliveryTag := FNextTag;
    Inc(FNextTag);
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.TryCancel(const AConsumerTag: string): Boolean;
begin
  FLock.Enter;
  try
    FCancelados.Add(AConsumerTag);
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FEntregas.Count;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Entrega(AIndex: Integer): TEntregaReg;
begin
  FLock.Enter;
  try
    Result := FEntregas[AIndex];
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Cancelados: string;
begin
  FLock.Enter;
  try
    Result := FCancelados.CommaText;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.CanceladosCount: Integer;
begin
  FLock.Enter;
  try
    Result := FCancelados.Count;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Corpos: string;
var
  I: Integer;
begin
  Result := '';
  FLock.Enter;
  try
    for I := 0 to FEntregas.Count - 1 do
    begin
      if Result <> '' then
        Result := Result + '|';
      Result := Result + FEntregas[I].Corpo;
    end;
  finally
    FLock.Leave;
  end;
end;


{ TAMQPWalFileFalso }

constructor TAMQPWalFileFalso.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FSyncOk := True;
  FFalharEscritaNa := -1;
end;

destructor TAMQPWalFileFalso.Destroy;
begin
  FLock.Free;
  inherited Destroy;
end;

function TAMQPWalFileFalso.Append(const ABytes: TBytes): Int64;
var
  LAntes: Integer;
begin
  FLock.Enter;
  try
    Inc(FEscritas);
    if FEscritas = FFalharEscritaNa then
      raise EAMQPWal.Create('falha de escrita injetada');
    LAntes := Length(FBuf);
    SetLength(FBuf, LAntes + Length(ABytes));
    if Length(ABytes) > 0 then
      Move(ABytes[0], FBuf[LAntes], Length(ABytes));
    Result := LAntes;
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.ReadAll: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FBuf, 0, Length(FBuf));
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Sync: Boolean;
begin
  FLock.Enter;
  try
    Inc(FSyncs);
    Result := FSyncOk;
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Size: Int64;
begin
  FLock.Enter;
  try
    Result := Length(FBuf);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.TruncateTo(ASize: Int64);
begin
  FLock.Enter;
  try
    SetLength(FBuf, Integer(ASize));
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Bytes: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FBuf, 0, Length(FBuf));
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.PoeBytes(const ABytes: TBytes);
begin
  FLock.Enter;
  try
    FBuf := Copy(ABytes, 0, Length(ABytes));
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Syncs: Integer;
begin
  FLock.Enter;
  try
    Result := FSyncs;
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Escritas: Integer;
begin
  FLock.Enter;
  try
    Result := FEscritas;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.SetSyncOk(AValue: Boolean);
begin
  FLock.Enter;
  try
    FSyncOk := AValue;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.SetFalharEscritaNa(AValue: Integer);
begin
  FLock.Enter;
  try
    FFalharEscritaNa := AValue;
  finally
    FLock.Leave;
  end;
end;

{ TAMQPWalFileComFalha }

constructor TAMQPWalFileComFalha.Create(const AInterno: IAMQPWalFile);
begin
  inherited Create;
  FInterno := AInterno;
  FSyncOk := True;
end;

function TAMQPWalFileComFalha.Append(const ABytes: TBytes): Int64;
begin
  Result := FInterno.Append(ABytes);
end;

function TAMQPWalFileComFalha.ReadAll: TBytes;
begin
  Result := FInterno.ReadAll;
end;

function TAMQPWalFileComFalha.Sync: Boolean;
begin
  // The BYTES really go to disk; only the fsync's ANSWER is failed.
  // It models a disk that accepted the write and did not guarantee the barrier.
  FInterno.Sync;
  Result := FSyncOk;
end;

function TAMQPWalFileComFalha.Size: Int64;
begin
  Result := FInterno.Size;
end;

procedure TAMQPWalFileComFalha.TruncateTo(ASize: Int64);
begin
  FInterno.TruncateTo(ASize);
end;

procedure TAMQPWalFileComFalha.SetSyncOk(AValue: Boolean);
begin
  FSyncOk := AValue;
end;

{ TStreamGravador }

constructor TStreamGravador.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FPortao := TEvent.Create(nil, True, True, ''); // manual-reset, open
end;

destructor TStreamGravador.Destroy;
begin
  FPortao.Free;
  FLock.Free;
  inherited;
end;

procedure TStreamGravador.Bloquear;
begin
  FPortao.ResetEvent;
end;

procedure TStreamGravador.Liberar;
begin
  FPortao.SetEvent;
end;

function TStreamGravador.Read(var Buffer; Count: Longint): Longint;
begin
  Result := 0;
end;

function TStreamGravador.Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64;
begin
  Result := 0;
end;

function TStreamGravador.Write(const Buffer; Count: Longint): Longint;
var
  P: PByte;
  I: Integer;
begin
  FPortao.WaitFor(PC_WAIT_INFINITE);
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

function TStreamGravador.Bytes: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FLog, 0, FLen);
  finally
    FLock.Leave;
  end;
end;

function LeFrames(const ABytes: TBytes): TArray<TAMQPFrame>;
var
  LMem: TBytesStream;
  LLista: TList<TAMQPFrame>;
begin
  LLista := TList<TAMQPFrame>.Create;
  try
    LMem := TBytesStream.Create(ABytes);
    try
      while LMem.Position < LMem.Size do
        LLista.Add(TAMQPFrame.ReadFrom(LMem));
    finally
      LMem.Free;
    end;
    Result := LLista.ToArray;
  finally
    LLista.Free;
  end;
end;

function FramesEscritos(AWriter: TAMQPFrameWriter;
  AStream: TStreamGravador): TArray<TAMQPFrame>;
begin
  AWriter.Stop; // drains whatever is queued and ends the writer thread
  Result := LeFrames(AStream.Bytes);
end;

end.
