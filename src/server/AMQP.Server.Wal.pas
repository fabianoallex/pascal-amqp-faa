unit AMQP.Server.Wal;

{$I amqp.inc}

{ FILE layer of the write-ahead log -- broker sub-module, WS1 of Phase 4
  (durability, see CLAUDE.md, decisions D19-D28).

  SCOPE: this unit does not know what a queue, a message or a confirm is.
  It knows how to write numbered records to an append-only file, return the
  longest VALID PREFIX of them and send what was written to disk. The meaning
  of the Kind field belongs to the layers above (D22: CONTENT, ENQ, DEQ, TOPO).
  Batching and fsync are orchestrated by the journal thread (WS2); the records
  are given meaning by recovery (WS6).

  THE NAME. The plan said AMQP.Server.Journal.File; that does not work: `File`
  is a reserved word in Pascal and a unit name segment cannot be called that.
  It became AMQP.Server.Wal (the format and the files); AMQP.Server.Journal is
  left for WS2 (the thread and the watermark).

  DECISION D23, THE HEART OF THIS UNIT
  ------------------------------------
  There is no transaction marker. Each record carries its own length and a
  CRC32, and reading returns the longest intact prefix -- the torn tail is
  dropped. Where atomicity matters (dead-letter, which retires one record and
  creates another), it is settled by the ORDER in which the layers above
  write: the new ENQ before the old DEQ, so that failing halfway produces a
  DUPLICATE and not a LOSS. The at-least-once contract already admits
  duplicates; it does not admit loss, and the client would have no way to
  find out.

  Consequence for the reader: ReadPrefix NEVER raises on a corrupted file.
  A torn tail is the NORMAL case after a crash -- it is what you expect to
  find. The reason the read stopped comes out in TAMQPWalStop for whoever wants
  to log it, and the only real error is an I/O error.

  THE FSYNC POINT IS TESTABLE ON PURPOSE
  --------------------------------------
  Layer 2 of D28 requires proving that fsync happens at the batch boundary and
  that an I/O failure does not corrupt in-memory state. That is why the segment
  does not talk to the file system directly: it talks to IAMQPWalFile. In
  production it is TAMQPWalOsFile; in the tests it is a test double that counts
  syncs and fails at the chosen moment. The seam exists since WS1 because
  threading it in later would mean rewriting this unit.

  FILE PRIMITIVES -- WHAT WS0 MEASURED
  ------------------------------------
  - fsync: on FPC, SysUtils.FileFlush ALREADY IS FlushFileBuffers on Windows
    and fpfsync on Unix, measured indistinguishable from fpfsync called by
    hand. Only Delphi needs its own code, and that is why the only platform
    conditional of this unit is in AmqpFileSync (plus the one for FileTruncate).
  - cost: FlushFileBuffers costs 329-674 us per call (Win64/NTFS), and every
    100 records it drops to 530-1278 us PER BATCH -- from ~2,000 to ~100,000
    records/s. That is the number behind D25 (self-tuning group commit).
    This unit does not decide WHEN to sync; it only offers the operation.
  - lock: ONE lock for the DIRECTORY (TAMQPWalDirLock), none for the segments.
    FileOpen with fmShareExclusive gives real exclusion on both OSes -- share
    mode on Windows, fpflock(LOCK_EX or LOCK_NB) on Unix. Locking each segment
    as well would add no safety and would get in the way of anyone wanting to
    look at the file with the broker running.

  For RTL calls, only the forms whose signature is IDENTICAL on both
  compilers: one-argument FileCreate and two-argument FileOpen. The overloads
  with share mode/rights diverge, and uncompiled Delphi code is debt -- it is
  what produced the four System.Net.Socket defects in Phase 1.

  FORMAT
  ------
  Segment header, 16 bytes:

    magic    8   'AMQPWAL1'
    version  2   = 1
    reserved 2   = 0
    segno    4   segment number

  Record, 18 bytes of framing plus the payload:

    lsn      8   sequence number, STRICTLY INCREASING within the segment
    kind     1   meaning of the payload (from the layers above)
    flags    1   reserved, = 0
    len      4   payload size
    payload  len
    crc32    4   over EVERYTHING above (lsn..payload), excluding the crc itself

  Integers in LITTLE-ENDIAN, unlike the rest of the lib. This is not wire
  data: it is a private format of this broker, written and read by the same
  machine, and both target platforms are little-endian -- network order would
  only cost swapped bytes in every record. The magic and the version exist so
  that a file from another origin (or from a future format) is rejected
  outright, instead of turning into torn records. }

interface

uses
  SysUtils,
  Classes
  {$IFNDEF FPC}
  , Windows
  {$ENDIF}
  ;

const
  /// Segment header signature. Changes together with the format.
  AMQP_WAL_MAGIC = 'AMQPWAL1';
  AMQP_WAL_VERSION = 1;

  AMQP_WAL_SEG_HEADER_SIZE = 16;
  /// lsn(8) + kind(1) + flags(1) + len(4)
  AMQP_WAL_REC_HEADER_SIZE = 14;
  /// crc32(4)
  AMQP_WAL_REC_TRAILER_SIZE = 4;
  AMQP_WAL_REC_OVERHEAD = AMQP_WAL_REC_HEADER_SIZE + AMQP_WAL_REC_TRAILER_SIZE;

  /// Payload cap of a record. It exists so that a corrupted `len` does not turn
  /// into a giant allocation BEFORE the CRC can refute it -- the "fits in what
  /// is left of the file" check covers almost everything, this cap covers the rest.
  AMQP_WAL_MAX_PAYLOAD = 64 * 1024 * 1024;

  /// Target size of a closed segment (D26). Not enforced here -- WS7 is what
  /// rolls the segment; the constant lives next to the format.
  AMQP_WAL_SEGMENT_BYTES = 16 * 1024 * 1024;

  AMQP_WAL_SEGMENT_EXT = '.wal';
  AMQP_WAL_SEGMENT_PREFIX = 'seg-';
  /// How many digits the segment number takes in the file name. TEN, not
  /// eight: with eight, an ASegNo above 99,999,999 would produce a name with
  /// too many digits -- which AmqpWalParseSegmentName itself would not
  /// recognize back, and the segment would silently vanish from the listing.
  /// Ten cover ALL of Cardinal, so no value of the type yields an unrecoverable name.
  AMQP_WAL_SEGMENT_DIGITS = 10;
  /// Name of the data directory's lock file.
  AMQP_WAL_LOCK_NAME = 'amqp.lock';

type
  EAMQPWal = class(Exception);

  /// A log record. Kind is opaque here on purpose (see the header).
  TAMQPWalRecord = record
    Lsn: UInt64;
    Kind: Byte;
    Payload: TBytes;
  end;

  TAMQPWalRecords = array of TAMQPWalRecord;

  { Why reading stopped. None of these values is an error: in a WAL, finding
    the torn tail is the normal case after a crash. It exists so the broker's
    log can say WHAT it found, and so the tests can tell "ended intact" from
    "dropped 37 bytes". }
  TAMQPWalStop = (
    /// Reached the end of the file with all records intact.
    awsEnd,
    /// Bytes were missing to complete the framing or the payload of the last one.
    awsTruncated,
    /// The CRC of the last record did not match -- write interrupted halfway.
    awsCrc,
    /// The `len` was larger than the cap (the "larger than what is left" case
    /// is indistinguishable from a tail, and comes out as awsCauda).
    awsSize,
    /// The LSN did not increase. Only happens with garbage that passed the CRC
    /// (or with a file reassembled by hand); the valid prefix ends here.
    awsLsn
  );

  { File seam -- see "The fsync point is testable on purpose".

    Every implementation is of ONE already open file, and it owns the
    descriptor: releasing the last interface reference closes it. Never hold
    the implementation by a CLASS reference -- it is refcounted. }
  IAMQPWalFile = interface
    ['{6D2A17C4-9B38-4E05-A17F-3C84E9D5B260}']
    /// Appends at the END and returns the offset where the bytes started.
    /// Partial writes are handled inside (loop until done or error).
    function Append(const ABytes: TBytes): Int64;
    /// Reads the whole file. Segments are small by construction (D26), and
    /// reading at once makes the prefix scan -- and the exhaustive truncation
    /// sweep of D28 -- simple and fast.
    function ReadAll: TBytes;
    /// Sends what was written to the device. False = the call failed, and in
    /// that case NO confirm that depended on this batch may be released.
    function Sync: Boolean;
    function Size: Int64;
    /// Cuts the file at ASize bytes (dropping the torn tail).
    procedure TruncateTo(ASize: Int64);
  end;

  { The real implementation, over an operating system descriptor. }
  TAMQPWalOsFile = class(TInterfacedObject, IAMQPWalFile)
  private
    FHandle: THandle;
    FPath: string;
  public
    /// ACreate = True creates (failing if it already exists), False opens an
    /// existing one. No exclusive share mode: what excludes another owner is
    /// the DIRECTORY lock (TAMQPWalDirLock).
    constructor Create(const APath: string; ACreate: Boolean);
    destructor Destroy; override;

    function Append(const ABytes: TBytes): Int64;
    function ReadAll: TBytes;
    function Sync: Boolean;
    function Size: Int64;
    procedure TruncateTo(ASize: Int64);

    property Path: string read FPath;
  end;

  { A log segment: header + N records, only grows at the end.

    Whoever opens an existing segment gets the append point already positioned
    at the end of the VALID PREFIX -- and, by default, with the torn tail
    truncated. This is not overcaution: writing after bytes that reading drops
    would produce an intact record that recovery never reaches, which is worse
    than the original crash. }
  TAMQPWalSegment = class
  private
    FFile: IAMQPWalFile;
    FSegNo: Cardinal;
    FEndOffset: Int64;
    FLastLsn: UInt64;
    FStop: TAMQPWalStop;
    FDiscarded: Int64;
    /// Reads the whole file and validates the header. Raises if it is not a
    /// segment of this format -- it is the ONLY error family reading raises,
    /// because a foreign file in the DataDir is an operational error, not a
    /// consequence of a crash.
    function ReadAndValidateHeader: TBytes;
    function Scan(const ABuf: TBytes; ACollect: Boolean;
      out ARecords: TAMQPWalRecords; out AStop: TAMQPWalStop;
      out AEndOffset: Int64; out ALastLsn: UInt64): Integer;
  public
    /// New segment: writes the header and syncs (the header is the only thing
    /// in the file that cannot be rebuilt by replay).
    constructor CreateNew(const AFile: IAMQPWalFile; ASegNo: Cardinal);
    /// Existing segment: validates the header, scans the valid prefix and
    /// positions the append. ATrim = True (default) truncates the torn tail.
    constructor OpenExisting(const AFile: IAMQPWalFile; ATrim: Boolean = True);
    destructor Destroy; override;

    /// Appends a record. ALsn must be GREATER than the last one written --
    /// it is the invariant reading checks, and violating it would write a file
    /// that recovery itself would truncate there. Returns the offset.
    function Append(ALsn: UInt64; AKind: Byte; const APayload: TBytes): Int64;
    /// fsync. False = the call failed.
    function Sync: Boolean;

    /// Reads the longest valid prefix. Does not raise on a torn tail.
    function ReadPrefix(out ARecords: TAMQPWalRecords;
      out AStop: TAMQPWalStop): Integer;

    /// Segment number, read from the header.
    property SegNo: Cardinal read FSegNo;
    /// First byte after the last intact record -- where the next Append
    /// writes.
    property EndOffset: Int64 read FEndOffset;
    /// LSN of the last intact record (0 = segment with no records).
    property LastLsn: UInt64 read FLastLsn;
    /// Why the opening scan stopped.
    property Stop: TAMQPWalStop read FStop;
    /// How many bytes of torn tail the opening found (and truncated, if
    /// ATrim). Zero is the case of a cleanly closed segment.
    property DiscardedBytes: Int64 read FDiscarded;
  end;

  { One owner per data directory.

    Two brokers on the same DataDir would silently corrupt the log: both would
    write their own LSNs to the same files. The lock is amqp.lock opened with
    fmShareExclusive -- share mode on Windows, advisory fpflock on Unix, both
    measured in WS0. Released in the destructor, and by the operating system
    if the process dies, which is the case that actually matters. }
  TAMQPWalDirLock = class
  private
    FHandle: THandle;
    FPath: string;
  public
    /// Raises EAMQPWal if there is already an owner. Creates the directory if missing.
    constructor Create(const ADir: string);
    destructor Destroy; override;
    property Path: string read FPath;
  end;

/// CRC-32 (IEEE 802.3, the zip/png/gzip one): reflected polynomial $EDB88320,
/// init $FFFFFFFF, final xor $FFFFFFFF. Chosen because it can be checked
/// against public vectors -- a home-made checksum would be impossible to validate.
function AmqpCrc32(const ABytes: TBytes): Cardinal; overload;
/// Incremental/partial form: without the init or the final xor, for callers
/// computing over a slice of a larger buffer.
function AmqpCrc32(ACrc: Cardinal; const ABytes: TBytes;
  AOffset, ALen: Integer): Cardinal; overload;

/// Portable fsync.
function AmqpFileSync(AHandle: THandle): Boolean;

/// Code of the last operating system error.
///
/// GetLastOSError exists on FPC (on both platforms) and does NOT exist on
/// Delphi: there System.SysUtils only offers RaiseLastOSError, which RAISES
/// instead of returning and so is no use for composing our own message. The
/// equivalent is Win32's GetLastError.
function AmqpLastOsError: Integer;

/// Canonical name of a segment: seg-00000001.wal. Leading zeros so that the
/// directory's alphabetical order matches the numeric one.
function AmqpWalSegmentName(ASegNo: Cardinal): string;
/// Reads the number back. False if the name is not a segment's.
function AmqpWalParseSegmentName(const AName: string;
  out ASegNo: Cardinal): Boolean;
/// Existing segments in ADir, SORTED by number.
function AmqpWalListSegments(const ADir: string): TArray<Cardinal>;

/// How many bytes a record with this payload takes in the file.
function AmqpWalRecordSize(APayloadLen: Integer): Integer;

implementation

uses
  Generics.Collections;

var
  GCrcTable: array[0..255] of Cardinal;

procedure BuildCrcTable;
var
  I, J: Integer;
  C: Cardinal;
begin
  for I := 0 to 255 do
  begin
    C := Cardinal(I);
    for J := 0 to 7 do
      if (C and 1) <> 0 then
        C := $EDB88320 xor (C shr 1)
      else
        C := C shr 1;
    GCrcTable[I] := C;
  end;
end;

function AmqpCrc32(ACrc: Cardinal; const ABytes: TBytes;
  AOffset, ALen: Integer): Cardinal;
var
  I: Integer;
begin
  Result := ACrc;
  for I := AOffset to AOffset + ALen - 1 do
    Result := GCrcTable[(Result xor ABytes[I]) and $FF] xor (Result shr 8);
end;

function AmqpCrc32(const ABytes: TBytes): Cardinal;
begin
  Result := AmqpCrc32($FFFFFFFF, ABytes, 0, Length(ABytes)) xor $FFFFFFFF;
end;

function AmqpFileSync(AHandle: THandle): Boolean;
begin
  {$IFDEF FPC}
  // Measured in WS0: on FPC this is FlushFileBuffers on Windows and fpfsync on
  // Unix, with timings indistinguishable from the direct calls.
  Result := FileFlush(AHandle);
  {$ELSE}
  Result := FlushFileBuffers(AHandle);
  {$ENDIF}
end;

function AmqpLastOsError: Integer;
begin
  {$IFDEF FPC}
  Result := GetLastOSError;
  {$ELSE}
  Result := Integer(GetLastError);
  {$ENDIF}
end;

function AmqpWalRecordSize(APayloadLen: Integer): Integer;
begin
  Result := AMQP_WAL_REC_OVERHEAD + APayloadLen;
end;

function AmqpWalSegmentName(ASegNo: Cardinal): string;
begin
  Result := AMQP_WAL_SEGMENT_PREFIX
    + Format('%.*d', [AMQP_WAL_SEGMENT_DIGITS, Int64(ASegNo)])
    + AMQP_WAL_SEGMENT_EXT;
end;

function AmqpWalParseSegmentName(const AName: string;
  out ASegNo: Cardinal): Boolean;
var
  LNum: string;
  LVal: Int64;
  I: Integer;
begin
  ASegNo := 0;
  Result := False;
  if Length(AName) <> Length(AMQP_WAL_SEGMENT_PREFIX) + AMQP_WAL_SEGMENT_DIGITS
    + Length(AMQP_WAL_SEGMENT_EXT) then
    Exit;
  if not SameText(Copy(AName, 1, Length(AMQP_WAL_SEGMENT_PREFIX)),
    AMQP_WAL_SEGMENT_PREFIX) then
    Exit;
  if not SameText(Copy(AName, Length(AName) - Length(AMQP_WAL_SEGMENT_EXT) + 1,
    Length(AMQP_WAL_SEGMENT_EXT)), AMQP_WAL_SEGMENT_EXT) then
    Exit;
  LNum := Copy(AName, Length(AMQP_WAL_SEGMENT_PREFIX) + 1,
    AMQP_WAL_SEGMENT_DIGITS);
  // Digit by digit: StrToInt64Def alone would accept ' 12' and '+12', and such
  // a name would become a phantom segment with another one's number.
  for I := 1 to Length(LNum) do
    if (LNum[I] < '0') or (LNum[I] > '9') then
      Exit;
  LVal := StrToInt64Def(LNum, -1);
  if (LVal < 0) or (LVal > Int64(High(Cardinal))) then
    Exit;
  ASegNo := Cardinal(LVal);
  Result := True;
end;

function AmqpWalListSegments(const ADir: string): TArray<Cardinal>;
var
  LRec: TSearchRec;
  LList: TList<Cardinal>;
  LNo, LTmp: Cardinal;
  I, J: Integer;
begin
  Result := nil;
  LList := TList<Cardinal>.Create;
  try
    if FindFirst(IncludeTrailingPathDelimiter(ADir) + '*'
      + AMQP_WAL_SEGMENT_EXT, faAnyFile, LRec) = 0 then
    begin
      try
        repeat
          if (LRec.Attr and faDirectory) = 0 then
            if AmqpWalParseSegmentName(LRec.Name, LNo) then
              LList.Add(LNo);
        until FindNext(LRec) <> 0;
      finally
        // QUALIFIED: with the Windows unit in scope (Delphi branch),
        // Winapi.Windows.FindClose(THandle) shadows
        // System.SysUtils.FindClose(var TSearchRec) and the error only shows
        // up in dcc32. Same family as DeleteFile.
        SysUtils.FindClose(LRec);
      end;
    end;
    SetLength(Result, LList.Count);
    for I := 0 to LList.Count - 1 do
      Result[I] := LList[I];
  finally
    LList.Free;
  end;
  // Insertion sort, by hand: TArray.Sort<T> does not exist in FPC 3.2's
  // Generics.Collections (forbidden table in CLAUDE.md). There are tens of
  // segments, not thousands.
  for I := 1 to High(Result) do
  begin
    LTmp := Result[I];
    J := I - 1;
    while (J >= 0) and (Result[J] > LTmp) do
    begin
      Result[J + 1] := Result[J];
      Dec(J);
    end;
    Result[J + 1] := LTmp;
  end;
end;

// --- little-endian packing (see "Format" in the header) --------------------

procedure PutU8(var ABuf: TBytes; AOffset: Integer; AValue: Byte);
begin
  ABuf[AOffset] := AValue;
end;

procedure PutU16(var ABuf: TBytes; AOffset: Integer; AValue: Word);
begin
  ABuf[AOffset] := Byte(AValue);
  ABuf[AOffset + 1] := Byte(AValue shr 8);
end;

procedure PutU32(var ABuf: TBytes; AOffset: Integer; AValue: Cardinal);
begin
  ABuf[AOffset] := Byte(AValue);
  ABuf[AOffset + 1] := Byte(AValue shr 8);
  ABuf[AOffset + 2] := Byte(AValue shr 16);
  ABuf[AOffset + 3] := Byte(AValue shr 24);
end;

procedure PutU64(var ABuf: TBytes; AOffset: Integer; AValue: UInt64);
begin
  PutU32(ABuf, AOffset, Cardinal(AValue and $FFFFFFFF));
  PutU32(ABuf, AOffset + 4, Cardinal(AValue shr 32));
end;

function GetU16(const ABuf: TBytes; AOffset: Integer): Word;
begin
  Result := Word(ABuf[AOffset]) or (Word(ABuf[AOffset + 1]) shl 8);
end;

function GetU32(const ABuf: TBytes; AOffset: Integer): Cardinal;
begin
  Result := Cardinal(ABuf[AOffset])
    or (Cardinal(ABuf[AOffset + 1]) shl 8)
    or (Cardinal(ABuf[AOffset + 2]) shl 16)
    or (Cardinal(ABuf[AOffset + 3]) shl 24);
end;

function GetU64(const ABuf: TBytes; AOffset: Integer): UInt64;
begin
  Result := UInt64(GetU32(ABuf, AOffset))
    or (UInt64(GetU32(ABuf, AOffset + 4)) shl 32);
end;

{ TAMQPWalOsFile }

constructor TAMQPWalOsFile.Create(const APath: string; ACreate: Boolean);
var
  H: THandle;
begin
  inherited Create;
  // BEFORE anything that can raise: a constructor that fails has its
  // destructor called, and the field zeroed by construction would be 0 --
  // which on Unix is a legitimate descriptor (standard input). Closing fd 0 by
  // mistake is the kind of damage that only shows up much later.
  FHandle := THandle(-1);
  FPath := APath;
  if ACreate then
  begin
    if FileExists(APath) then
      raise EAMQPWal.CreateFmt('segment already exists: %s', [APath]);
    H := FileCreate(APath);
    if H = THandle(-1) then
      raise EAMQPWal.CreateFmt('failed to create %s: %s',
        [APath, SysErrorMessage(AmqpLastOsError)]);
    FileClose(H);
  end
  else
    if not FileExists(APath) then
      raise EAMQPWal.CreateFmt('segment does not exist: %s', [APath]);

  FHandle := FileOpen(APath, fmOpenReadWrite or fmShareDenyNone);
  if FHandle = THandle(-1) then
    raise EAMQPWal.CreateFmt('failed to open %s: %s',
      [APath, SysErrorMessage(AmqpLastOsError)]);
end;

destructor TAMQPWalOsFile.Destroy;
begin
  if FHandle <> THandle(-1) then
  begin
    FileClose(FHandle);
    FHandle := THandle(-1);
  end;
  inherited Destroy;
end;

function TAMQPWalOsFile.Append(const ABytes: TBytes): Int64;
var
  LWritten, LTotal, LRemaining: Integer;
begin
  Result := FileSeek(FHandle, Int64(0), 2 { end });
  LTotal := 0;
  LRemaining := Length(ABytes);
  while LRemaining > 0 do
  begin
    LWritten := FileWrite(FHandle, ABytes[LTotal], LRemaining);
    if LWritten <= 0 then
      raise EAMQPWal.CreateFmt('write failed at %s after %d of %d bytes: %s',
        [FPath, LTotal, Length(ABytes), SysErrorMessage(AmqpLastOsError)]);
    Inc(LTotal, LWritten);
    Dec(LRemaining, LWritten);
  end;
end;

function TAMQPWalOsFile.ReadAll: TBytes;
var
  LSize: Int64;
  LRead, LTotal, LRemaining: Integer;
begin
  Result := nil;
  LSize := Size;
  if LSize <= 0 then
    Exit;
  if LSize > MaxInt then
    raise EAMQPWal.CreateFmt('segment too large to read at once: '
      + '%s (%d bytes)', [FPath, LSize]);
  SetLength(Result, Integer(LSize));
  FileSeek(FHandle, Int64(0), 0 { start });
  LTotal := 0;
  LRemaining := Integer(LSize);
  while LRemaining > 0 do
  begin
    LRead := FileRead(FHandle, Result[LTotal], LRemaining);
    if LRead <= 0 then
    begin
      // The file shrank under us -- it should not, there is a single owner.
      // Return what we got; the scan treats the rest as tail.
      SetLength(Result, LTotal);
      Exit;
    end;
    Inc(LTotal, LRead);
    Dec(LRemaining, LRead);
  end;
end;

function TAMQPWalOsFile.Sync: Boolean;
begin
  Result := AmqpFileSync(FHandle);
end;

function TAMQPWalOsFile.Size: Int64;
var
  LPos: Int64;
begin
  LPos := FileSeek(FHandle, Int64(0), 1 { current });
  Result := FileSeek(FHandle, Int64(0), 2 { end });
  FileSeek(FHandle, LPos, 0);
end;

procedure TAMQPWalOsFile.TruncateTo(ASize: Int64);
begin
  FileSeek(FHandle, ASize, 0);
  {$IFDEF FPC}
  if not FileTruncate(FHandle, ASize) then
  {$ELSE}
  if not SetEndOfFile(FHandle) then
  {$ENDIF}
    raise EAMQPWal.CreateFmt('failed to truncate %s at %d: %s',
      [FPath, ASize, SysErrorMessage(AmqpLastOsError)]);
end;

{ TAMQPWalSegment }

constructor TAMQPWalSegment.CreateNew(const AFile: IAMQPWalFile;
  ASegNo: Cardinal);
var
  LBuf: TBytes;
  I: Integer;
begin
  inherited Create;
  if AFile = nil then
    raise EAMQPWal.Create('segment has no file');
  FFile := AFile;
  FSegNo := ASegNo;
  if FFile.Size <> 0 then
    raise EAMQPWal.CreateFmt('CreateNew called on non-empty file (%d bytes)',
      [FFile.Size]);

  SetLength(LBuf, AMQP_WAL_SEG_HEADER_SIZE);
  for I := 1 to Length(AMQP_WAL_MAGIC) do
    LBuf[I - 1] := Byte(AMQP_WAL_MAGIC[I]);
  PutU16(LBuf, 8, AMQP_WAL_VERSION);
  PutU16(LBuf, 10, 0);
  PutU32(LBuf, 12, FSegNo);
  FFile.Append(LBuf);
  // The header is the only thing in the file that cannot be rebuilt by
  // replay: without it the whole segment is unrecognizable. It goes to disk now.
  FFile.Sync;

  FEndOffset := AMQP_WAL_SEG_HEADER_SIZE;
  FLastLsn := 0;
  FStop := awsEnd;
  FDiscarded := 0;
end;

constructor TAMQPWalSegment.OpenExisting(const AFile: IAMQPWalFile;
  ATrim: Boolean);
var
  LBuf: TBytes;
  LRecs: TAMQPWalRecords;
begin
  inherited Create;
  if AFile = nil then
    raise EAMQPWal.Create('segment has no file');
  FFile := AFile;
  LBuf := ReadAndValidateHeader;
  Scan(LBuf, False, LRecs, FStop, FEndOffset, FLastLsn);
  FDiscarded := Int64(Length(LBuf)) - FEndOffset;
  if ATrim and (FDiscarded > 0) then
  begin
    FFile.TruncateTo(FEndOffset);
    FFile.Sync;
  end;
end;

destructor TAMQPWalSegment.Destroy;
begin
  FFile := nil;
  inherited Destroy;
end;

function TAMQPWalSegment.ReadAndValidateHeader: TBytes;
var
  I: Integer;
  LVer: Word;
begin
  Result := FFile.ReadAll;
  if Length(Result) < AMQP_WAL_SEG_HEADER_SIZE then
    raise EAMQPWal.CreateFmt('segment header truncated (%d of %d bytes)',
      [Length(Result), AMQP_WAL_SEG_HEADER_SIZE]);
  for I := 1 to Length(AMQP_WAL_MAGIC) do
    if Result[I - 1] <> Byte(AMQP_WAL_MAGIC[I]) then
      raise EAMQPWal.Create('not a WAL segment from this broker'
        + '(signature does not match)');
  LVer := GetU16(Result, 8);
  if LVer <> AMQP_WAL_VERSION then
    raise EAMQPWal.CreateFmt('segment version %d, expected %d',
      [LVer, AMQP_WAL_VERSION]);
  FSegNo := GetU32(Result, 12);
end;

function TAMQPWalSegment.Scan(const ABuf: TBytes; ACollect: Boolean;
  out ARecords: TAMQPWalRecords; out AStop: TAMQPWalStop;
  out AEndOffset: Int64; out ALastLsn: UInt64): Integer;
var
  LPos, LLength, LCap: Integer;
  LLsn, LLastLsn: UInt64;
  LKind: Byte;
  LLen: Cardinal;
  LCrcRead, LCrcCalc: Cardinal;
begin
  ARecords := nil;
  LLength := Length(ABuf);
  LPos := AMQP_WAL_SEG_HEADER_SIZE;
  LLastLsn := 0;
  LCap := 0;
  Result := 0;
  AStop := awsEnd;

  while True do
  begin
    if LPos >= LLength then
    begin
      AStop := awsEnd;
      Break;
    end;
    if LLength - LPos < AMQP_WAL_REC_HEADER_SIZE then
    begin
      AStop := awsTruncated;
      Break;
    end;

    LLsn := GetU64(ABuf, LPos);
    LKind := ABuf[LPos + 8];
    LLen := GetU32(ABuf, LPos + 10);

    // Order of the checks: the size is validated BEFORE any read or
    // allocation driven by it, because it may itself be corrupted.
    if LLen > AMQP_WAL_MAX_PAYLOAD then
    begin
      AStop := awsSize;
      Break;
    end;
    if Int64(LLength - LPos) < Int64(AMQP_WAL_REC_OVERHEAD) + Int64(LLen) then
    begin
      AStop := awsTruncated;
      Break;
    end;

    LCrcRead := GetU32(ABuf, LPos + AMQP_WAL_REC_HEADER_SIZE + Integer(LLen));
    LCrcCalc := AmqpCrc32($FFFFFFFF, ABuf, LPos,
      AMQP_WAL_REC_HEADER_SIZE + Integer(LLen)) xor $FFFFFFFF;
    if LCrcRead <> LCrcCalc then
    begin
      AStop := awsCrc;
      Break;
    end;

    // Only after the CRC: an out-of-order LSN in an INTACT record is something
    // else (reassembled file, writer bug), and it is worth telling apart from garbage.
    if LLsn <= LLastLsn then
    begin
      AStop := awsLsn;
      Break;
    end;

    if ACollect then
    begin
      if Result = LCap then
      begin
        if LCap = 0 then
          LCap := 16
        else
          LCap := LCap * 2;
        SetLength(ARecords, LCap);
      end;
      ARecords[Result].Lsn := LLsn;
      ARecords[Result].Kind := LKind;
      ARecords[Result].Payload := Copy(ABuf,
        LPos + AMQP_WAL_REC_HEADER_SIZE, Integer(LLen));
    end;

    Inc(Result);
    LLastLsn := LLsn;
    Inc(LPos, AMQP_WAL_REC_OVERHEAD + Integer(LLen));
  end;

  AEndOffset := LPos;
  ALastLsn := LLastLsn;
  if ACollect then
    SetLength(ARecords, Result);
end;

function TAMQPWalSegment.Append(ALsn: UInt64; AKind: Byte;
  const APayload: TBytes): Int64;
var
  LBuf: TBytes;
  LLen: Integer;
  LCrc: Cardinal;
begin
  if ALsn <= FLastLsn then
    raise EAMQPWal.CreateFmt('LSN %d is not greater than the last written (%d) -- '
      + 'reading would truncate the segment here', [ALsn, FLastLsn]);
  LLen := Length(APayload);
  if LLen > AMQP_WAL_MAX_PAYLOAD then
    raise EAMQPWal.CreateFmt('payload of %d bytes exceeds the limit of %d',
      [LLen, AMQP_WAL_MAX_PAYLOAD]);

  SetLength(LBuf, AmqpWalRecordSize(LLen));
  PutU64(LBuf, 0, ALsn);
  PutU8(LBuf, 8, AKind);
  PutU8(LBuf, 9, 0);
  PutU32(LBuf, 10, Cardinal(LLen));
  if LLen > 0 then
    Move(APayload[0], LBuf[AMQP_WAL_REC_HEADER_SIZE], LLen);
  LCrc := AmqpCrc32($FFFFFFFF, LBuf, 0, AMQP_WAL_REC_HEADER_SIZE + LLen)
    xor $FFFFFFFF;
  PutU32(LBuf, AMQP_WAL_REC_HEADER_SIZE + LLen, LCrc);

  Result := FFile.Append(LBuf);
  // The end comes from the offset the file ITSELF returned, not from an
  // accumulator: that way the segment cannot diverge from the file (opening
  // with ATrim=False and writing right after would otherwise not add up).
  FEndOffset := Result + Length(LBuf);
  FLastLsn := ALsn;
end;

function TAMQPWalSegment.Sync: Boolean;
begin
  Result := FFile.Sync;
end;

function TAMQPWalSegment.ReadPrefix(out ARecords: TAMQPWalRecords;
  out AStop: TAMQPWalStop): Integer;
var
  LBuf: TBytes;
  LEnd: Int64;
  LLsn: UInt64;
begin
  LBuf := ReadAndValidateHeader;
  Result := Scan(LBuf, True, ARecords, AStop, LEnd, LLsn);
end;

{ TAMQPWalDirLock }

constructor TAMQPWalDirLock.Create(const ADir: string);
var
  H: THandle;
begin
  inherited Create;
  FHandle := THandle(-1); // see the note in TAMQPWalOsFile.Create
  if not ForceDirectories(ADir) then
    raise EAMQPWal.CreateFmt('failed to create data directory %s: %s',
      [ADir, SysErrorMessage(AmqpLastOsError)]);
  FPath := IncludeTrailingPathDelimiter(ADir) + AMQP_WAL_LOCK_NAME;
  if not FileExists(FPath) then
  begin
    H := FileCreate(FPath);
    if H = THandle(-1) then
      raise EAMQPWal.CreateFmt('failed to create %s: %s',
        [FPath, SysErrorMessage(AmqpLastOsError)]);
    FileClose(H);
  end;
  // fmShareExclusive: share mode on Windows, advisory fpflock on Unix -- WS0
  // measured both refusing the second owner and releasing on close.
  FHandle := FileOpen(FPath, fmOpenReadWrite or fmShareExclusive);
  if FHandle = THandle(-1) then
    raise EAMQPWal.CreateFmt('data directory %s is already owned'
      + '(another broker is using %s)', [ADir, AMQP_WAL_LOCK_NAME]);
end;

destructor TAMQPWalDirLock.Destroy;
begin
  if FHandle <> THandle(-1) then
  begin
    FileClose(FHandle);
    FHandle := THandle(-1);
  end;
  inherited Destroy;
end;

initialization
  // In the unit's initialization, not on demand behind a flag: the CRC is
  // computed by the publisher thread AND by the journal thread (D24), and
  // building the table on two threads at once would let one read entries the
  // other has not written yet -- a wrong CRC written to disk, with no symptom
  // at all until recovery drops the record.
  BuildCrcTable;

end.
