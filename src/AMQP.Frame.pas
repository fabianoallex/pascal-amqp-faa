unit AMQP.Frame;

{$I amqp.inc}

{ AMQP 0-9-1 framing layer (spec 2.3.5).

  Layout of a frame on the wire:

    +--------+----------+---------+ +--------------+ +-----------+
    | type   | channel  | size    | | payload      | | frame-end |
    | octet  | u16 (BE) | u32(BE) | | size octets  | | octet 0xCE|
    +--------+----------+---------+ +--------------+ +-----------+

  'size' is the size of the payload only; the frame-end octet (0xCE) that follows
  it is not counted. If the octet read at that position is not 0xCE, the
  stream is out of sync and we raise EAMQPFrame. }

interface

uses
  SysUtils,
  Classes,
  AMQP.Protocol;

type
  /// Error reading/building an AMQP frame at the byte level (unexpected end
  /// of stream, missing/wrong frame-end). Usually means the connection
  /// dropped in the middle of a frame, not an application bug.
  EAMQPFrame = class(Exception);

  { Subclass of EAMQPFrame raised when the stream ENDS in the middle of (or before)
    a frame — the normal "peer disconnected" case. It exists so the broker can
    tell this apart from a malformed frame: end of stream is a silent
    shutdown, a malformed frame is FRAME_ERROR (501) with Connection.Close.
    Code that only wants "failed to read frame" keeps catching EAMQPFrame. }
  EAMQPFrameEOF = class(EAMQPFrame);

  TAMQPFrame = record
    FrameType: Byte;
    Channel: Word;
    Payload: TBytes;

    class function Create(AFrameType: Byte; AChannel: Word;
      const APayload: TBytes): TAMQPFrame; static;

    /// Heartbeat frame (type 8, channel 0, empty payload).
    class function Heartbeat: TAMQPFrame; static;

    /// Serializes the whole frame (header + payload + frame-end) into AStream.
    procedure WriteTo(AStream: TStream);

    /// Reads a whole frame from AStream. If AMaxPayload > 0, rejects larger
    /// payloads (protection against an exceeded frame-max / corrupted stream).
    class function ReadFrom(AStream: TStream;
      AMaxPayload: Cardinal = 0): TAMQPFrame; static;

    function IsHeartbeat: Boolean;
    function IsMethod: Boolean;
  end;

/// Sends the protocol header ("AMQP" 0 0 9 1) — the first thing the
/// client writes to the socket, before any frame.
procedure WriteProtocolHeader(AStream: TStream);

implementation

// Reads exactly ACount octets from AStream, in a loop (a socket Read may
// return less than requested). Raises EAMQPFrame on premature end of stream.
procedure ReadFull(AStream: TStream; var ABuffer: TBytes; ACount: Integer);
var
  LTotal, LRead: Integer;
begin
  SetLength(ABuffer, ACount);
  LTotal := 0;
  while LTotal < ACount do
  begin
    LRead := AStream.Read(ABuffer[LTotal], ACount - LTotal);
    if LRead <= 0 then
      raise EAMQPFrameEOF.Create('end of stream while reading frame (connection closed?)');
    Inc(LTotal, LRead);
  end;
end;

{ TAMQPFrame }

class function TAMQPFrame.Create(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes): TAMQPFrame;
begin
  Result.FrameType := AFrameType;
  Result.Channel := AChannel;
  Result.Payload := APayload;
end;

class function TAMQPFrame.Heartbeat: TAMQPFrame;
begin
  Result := TAMQPFrame.Create(AMQP_FRAME_HEARTBEAT, AMQP_CHANNEL_CONNECTION, nil);
end;

function TAMQPFrame.IsHeartbeat: Boolean;
begin
  Result := FrameType = AMQP_FRAME_HEARTBEAT;
end;

function TAMQPFrame.IsMethod: Boolean;
begin
  Result := FrameType = AMQP_FRAME_METHOD;
end;

procedure TAMQPFrame.WriteTo(AStream: TStream);
var
  LHeader: array[0..6] of Byte;
  LSize: Cardinal;
  LEnd: Byte;
begin
  LSize := Cardinal(Length(Payload));
  LHeader[0] := FrameType;
  LHeader[1] := Byte(Channel shr 8);
  LHeader[2] := Byte(Channel);
  LHeader[3] := Byte(LSize shr 24);
  LHeader[4] := Byte(LSize shr 16);
  LHeader[5] := Byte(LSize shr 8);
  LHeader[6] := Byte(LSize);
  // Untyped/by-reference buffer: no @ before the variable (see CLAUDE.md).
  AStream.WriteBuffer(LHeader[0], SizeOf(LHeader));
  if LSize > 0 then
    AStream.WriteBuffer(Payload[0], Integer(LSize));
  LEnd := AMQP_FRAME_END;
  AStream.WriteBuffer(LEnd, 1);
end;

class function TAMQPFrame.ReadFrom(AStream: TStream;
  AMaxPayload: Cardinal): TAMQPFrame;
var
  LHeader: TBytes;
  LSize: Cardinal;
  LEnd: TBytes;
begin
  ReadFull(AStream, LHeader, 7);
  Result.FrameType := LHeader[0];
  Result.Channel := (Word(LHeader[1]) shl 8) or Word(LHeader[2]);
  LSize := (Cardinal(LHeader[3]) shl 24) or (Cardinal(LHeader[4]) shl 16) or
           (Cardinal(LHeader[5]) shl 8) or Cardinal(LHeader[6]);

  if (AMaxPayload > 0) and (LSize > AMaxPayload) then
    raise EAMQPFrame.CreateFmt(
      'frame payload (%u) exceeds the negotiated maximum (%u)', [LSize, AMaxPayload]);

  if LSize > 0 then
    ReadFull(AStream, Result.Payload, Integer(LSize))
  else
    Result.Payload := nil;

  ReadFull(AStream, LEnd, 1);
  if LEnd[0] <> AMQP_FRAME_END then
    raise EAMQPFrame.CreateFmt(
      'invalid frame-end octet: expected 0x%.2x, got 0x%.2x',
      [AMQP_FRAME_END, LEnd[0]]);
end;

{ Protocol header }

procedure WriteProtocolHeader(AStream: TStream);
begin
  AStream.WriteBuffer(AMQP_PROTOCOL_HEADER[0], Length(AMQP_PROTOCOL_HEADER));
end;

end.
