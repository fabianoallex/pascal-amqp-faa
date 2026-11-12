unit AMQP.Channel.Methods;

{$I amqp.inc}

{ Methods of the Channel class (20): channel open and close.

  Same convention as AMQP.Connection.Methods: Build* returns the complete
  method payload (header + args); Decode* takes a TAMQPReader already
  positioned after the method header. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

type
  { Arguments of Channel.Close / Connection.Close (same shape). }
  TAMQPCloseInfo = record
    ReplyCode: Word;
    ReplyText: string;
    ClassId: Word;
    MethodId: Word;
  end;

function BuildChannelOpen: TBytes;
procedure DecodeChannelOpenOk(const AReader: TAMQPReader);

function BuildChannelClose(const AClose: TAMQPCloseInfo): TBytes;
function BuildChannelCloseOk: TBytes;
function DecodeChannelClose(const AReader: TAMQPReader): TAMQPCloseInfo;

// --- Server side (broker sub-module) -------------------------------------
// Build of the server->client methods and decode of the client->server ones.
// Channel.Close / DecodeChannelClose above already serve both directions.

/// Channel.Open-Ok (11) — just one reserved longstr (channel-id), empty.
function BuildChannelOpenOk: TBytes;

/// Channel.Open (10) — consumes the reserved shortstr and discards it.
procedure DecodeChannelOpen(const AReader: TAMQPReader);

/// Channel.Close-Ok (41) — no arguments.
procedure DecodeChannelCloseOk(const AReader: TAMQPReader);

/// Channel.Flow (20) / Flow-Ok (21) — pauses/resumes flow on the channel.
/// active=False asks the peer to stop sending content; the peer confirms with
/// Flow-Ok echoing the same active. (RabbitMQ uses TCP backpressure and
/// connection.blocked instead, but the codec stays complete.)
function BuildChannelFlow(AActive: Boolean): TBytes;
function BuildChannelFlowOk(AActive: Boolean): TBytes;
function DecodeChannelFlow(const AReader: TAMQPReader): Boolean;   // active
function DecodeChannelFlowOk(const AReader: TAMQPReader): Boolean; // active

implementation

function BuildChannelOpen: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN);
  try
    W.WriteShortStr(''); // reserved-1 (obsolete)
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeChannelOpenOk(const AReader: TAMQPReader);
begin
  AReader.ReadLongStr; // reserved-1 — discarded
end;

function BuildChannelClose(const AClose: TAMQPCloseInfo): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE);
  try
    W.WriteShortUInt(AClose.ReplyCode);
    W.WriteShortStr(AClose.ReplyText);
    W.WriteShortUInt(AClose.ClassId);
    W.WriteShortUInt(AClose.MethodId);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildChannelCloseOk: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK);
  try
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeChannelClose(const AReader: TAMQPReader): TAMQPCloseInfo;
begin
  Result.ReplyCode := AReader.ReadShortUInt;
  Result.ReplyText := AReader.ReadShortStr;
  Result.ClassId := AReader.ReadShortUInt;
  Result.MethodId := AReader.ReadShortUInt;
end;

{ Server side }

function BuildChannelOpenOk: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK);
  try
    W.WriteLongStr(''); // reserved-1 (channel-id) — obsolete
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeChannelOpen(const AReader: TAMQPReader);
begin
  AReader.ReadShortStr; // reserved-1 — discarded
end;

procedure DecodeChannelCloseOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

function BuildChannelFlow(AActive: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW);
  try
    W.WriteBit(AActive);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildChannelFlowOk(AActive: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW_OK);
  try
    W.WriteBit(AActive);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeChannelFlow(const AReader: TAMQPReader): Boolean;
begin
  Result := AReader.ReadBit;
end;

function DecodeChannelFlowOk(const AReader: TAMQPReader): Boolean;
begin
  Result := AReader.ReadBit;
end;

end.
