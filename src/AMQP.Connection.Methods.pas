unit AMQP.Connection.Methods;

{$I amqp.inc}

{ Methods of the AMQP 0-9-1 Connection class (10) used in the handshake.

  Each Build* returns the complete payload of a method frame (class/method
  header + arguments), ready to become a method-type TAMQPFrame on
  channel 0. Each Decode* takes a TAMQPReader already positioned right AFTER the
  method header (that is, at the start of the arguments) — the caller reads the
  header first, with AMQP.Method.ReadMethodHeader, to dispatch.

  Handshake sequence:
    C: protocol-header
    S: Start          -> DecodeStart
    C: Start-Ok       <- BuildStartOk
    S: Tune           -> DecodeTune
    C: Tune-Ok        <- BuildTuneOk (see NegotiateTune)
    C: Open           <- BuildOpen
    S: Open-Ok        -> DecodeOpenOk

  channel-max gotcha (see CLAUDE.md): the value returned in Tune-Ok may never
  exceed the one the server proposed, nor be 0 when the server proposed a finite
  limit. NegotiateTune takes care of that. }

interface

uses
  SysUtils,
  Rtti,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

const
  AMQP_CLIENT_PRODUCT = 'pascal-amqp-faa';
  AMQP_CLIENT_VERSION = '0.6.0';
  {$IFDEF FPC}
  AMQP_CLIENT_PLATFORM = 'FreePascal';
  {$ELSE}
  AMQP_CLIENT_PLATFORM = 'Delphi';
  {$ENDIF}
  AMQP_LOCALE_DEFAULT = 'en_US';
  AMQP_AUTH_PLAIN = 'PLAIN';

type
  TAMQPConnectionStart = record
    VersionMajor: Byte;
    VersionMinor: Byte;
    ServerProperties: TAMQPFieldTable; // owner: caller (must free)
    Mechanisms: string;                // space-separated list (e.g. 'PLAIN AMQPLAIN')
    Locales: string;
    /// True if AMechanism is in the Mechanisms list.
    function SupportsMechanism(const AMechanism: string): Boolean;
  end;

  TAMQPConnectionTune = record
    ChannelMax: Word;
    FrameMax: Cardinal;
    Heartbeat: Word;
  end;

  TAMQPConnectionClose = record
    ReplyCode: Word;
    ReplyText: string;
    ClassId: Word;
    MethodId: Word;
  end;

// --- Building client-properties and credentials ---------------------------

/// Default client-properties (product/version/platform/capabilities).
/// The caller owns the returned table (must free it).
function DefaultClientProperties: TAMQPFieldTable;

/// PLAIN mechanism response: #0 + user + #0 + password.
function PlainAuthResponse(const AUser, APassword: string): string;

// --- Method encode / decode -----------------------------------------------

function DecodeStart(const AReader: TAMQPReader): TAMQPConnectionStart;

function BuildStartOk(AClientProperties: TAMQPFieldTable;
  const AMechanism, AResponse, ALocale: string): TBytes;

function DecodeTune(const AReader: TAMQPReader): TAMQPConnectionTune;

function BuildTuneOk(const ATune: TAMQPConnectionTune): TBytes;

function BuildOpen(const AVirtualHost: string): TBytes;

/// Reads Open-Ok (just one reserved shortstr, discarded).
procedure DecodeOpenOk(const AReader: TAMQPReader);

function DecodeClose(const AReader: TAMQPReader): TAMQPConnectionClose;

/// Reads Connection.Blocked (RabbitMQ extension): returns the reason given
/// by the broker when entering a resource alarm. Connection.Unblocked has no args.
function DecodeBlocked(const AReader: TAMQPReader): string;

function BuildClose(const AClose: TAMQPConnectionClose): TBytes;

function BuildCloseOk: TBytes;

// --- Tune negotiation ------------------------------------------------------

/// Negotiated value where 0 means "no limit": if either side is 0, the other
/// one wins (the larger); if both are finite, the smaller wins. So Tune-Ok never
/// exceeds a finite server limit and never sends 0 under a finite limit.
function NegotiateChannelMax(AServer, AClient: Word): Word;
function NegotiateFrameMax(AServer, AClient: Cardinal): Cardinal;
function NegotiateHeartbeat(AServer, AClient: Word): Word;

/// Combines the server's Tune with the client's preferences.
function NegotiateTune(const AServerTune: TAMQPConnectionTune;
  AClientChannelMax: Word; AClientFrameMax: Cardinal;
  AClientHeartbeat: Word): TAMQPConnectionTune;

// =========================================================================
//  Server side (broker sub-module) — build of the server->client methods
//  and decode of the client->server methods. The client uses nothing here;
//  it lives in this unit so the Connection class codec stays all together.
// =========================================================================

const
  AMQP_SERVER_PRODUCT  = 'pascal-amqp-faa (broker)';
  AMQP_SERVER_VERSION  = '0.6.0';
  AMQP_SERVER_PLATFORM = AMQP_CLIENT_PLATFORM; // 'Delphi' | 'FreePascal'
  /// Protocol version the broker announces in Connection.Start.
  AMQP_VERSION_MAJOR = 0;
  AMQP_VERSION_MINOR = 9;

type
  { Arguments of Connection.Start-Ok (client -> server). }
  TAMQPConnectionStartOk = record
    ClientProperties: TAMQPFieldTable; // owner: caller (must free)
    Mechanism: string;
    Response: TBytes;                  // raw SASL response (PLAIN has #0 embedded)
    Locale: string;
  end;

  { Argument of Connection.Open (client -> server). The two reserved
    fields (capabilities/insist, obsolete) are consumed and discarded. }
  TAMQPConnectionOpen = record
    VirtualHost: string;
  end;

/// The broker's default server-properties (product/version/platform/capabilities).
/// The caller owns the returned table (must free it).
function DefaultServerProperties: TAMQPFieldTable;

/// Connection.Start (10). AServerProperties is written but not freed here.
function BuildStart(AVersionMajor, AVersionMinor: Byte;
  AServerProperties: TAMQPFieldTable;
  const AMechanisms, ALocales: string): TBytes;

/// Connection.Secure (20) — multi-step SASL challenge. Phase 1 does not use it
/// (PLAIN is single-step), but the codec stays complete.
function BuildSecure(const AChallenge: string): TBytes;

/// Connection.Tune (30).
function BuildTune(const ATune: TAMQPConnectionTune): TBytes;

/// Connection.Open-Ok (41) — just one reserved shortstr (known-hosts), empty.
function BuildOpenOk: TBytes;

/// Connection.Blocked (60) / Unblocked (61) — RabbitMQ extension (the broker
/// announces it stopped/resumed accepting publishes because of a resource alarm).
function BuildBlocked(const AReason: string): TBytes;
function BuildUnblocked: TBytes;

function DecodeStartOk(const AReader: TAMQPReader): TAMQPConnectionStartOk;

/// Connection.Secure-Ok (21) — returns the raw SASL response.
function DecodeSecureOk(const AReader: TAMQPReader): TBytes;

/// Connection.Tune-Ok (31) — same layout as Tune.
function DecodeTuneOk(const AReader: TAMQPReader): TAMQPConnectionTune;

function DecodeOpen(const AReader: TAMQPReader): TAMQPConnectionOpen;

/// Connection.Close-Ok (51) — no arguments (consumes nothing). Exists so the
/// broker/client dispatcher treats every method the same way.
procedure DecodeCloseOk(const AReader: TAMQPReader);

implementation

// 0 = "no limit": if either side is 0, use the larger; otherwise the smaller.
function NegotiateLimit(AServer, AClient: UInt64): UInt64;
begin
  if (AServer = 0) or (AClient = 0) then
  begin
    if AServer > AClient then
      Result := AServer
    else
      Result := AClient;
  end
  else
  begin
    if AServer < AClient then
      Result := AServer
    else
      Result := AClient;
  end;
end;

{ TAMQPConnectionStart }

function TAMQPConnectionStart.SupportsMechanism(const AMechanism: string): Boolean;
var
  LRest, LItem: string;
  LSep: Integer;
begin
  // Manual split (no TStringHelper, so as not to depend on type helpers in FPC).
  Result := False;
  LRest := Mechanisms;
  while LRest <> '' do
  begin
    LSep := Pos(' ', LRest);
    if LSep > 0 then
    begin
      LItem := Copy(LRest, 1, LSep - 1);
      Delete(LRest, 1, LSep);
    end
    else
    begin
      LItem := LRest;
      LRest := '';
    end;
    if SameText(LItem, AMechanism) then
      Exit(True);
  end;
end;

{ Client-properties and credentials }

function DefaultClientProperties: TAMQPFieldTable;
var
  LCaps: TAMQPFieldTable;
begin
  LCaps := TAMQPFieldTable.Create;
  LCaps.Put('authentication_failure_close', True)
       .Put('consumer_cancel_notify', True)
       .Put('connection.blocked', True)
       .Put('publisher_confirms', True);

  Result := TAMQPFieldTable.Create;
  Result.Put('product', AMQP_CLIENT_PRODUCT)
        .Put('version', AMQP_CLIENT_VERSION)
        .Put('platform', AMQP_CLIENT_PLATFORM)
        .Put('capabilities', TValue.From<TAMQPFieldTable>(LCaps));
end;

function PlainAuthResponse(const AUser, APassword: string): string;
begin
  Result := #0 + AUser + #0 + APassword;
end;

{ Encode / decode }

function DecodeStart(const AReader: TAMQPReader): TAMQPConnectionStart;
begin
  Result.VersionMajor := AReader.ReadOctet;
  Result.VersionMinor := AReader.ReadOctet;
  Result.ServerProperties := AReader.ReadFieldTable;
  Result.Mechanisms := AReader.ReadLongStr;
  Result.Locales := AReader.ReadLongStr;
end;

function BuildStartOk(AClientProperties: TAMQPFieldTable;
  const AMechanism, AResponse, ALocale: string): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK);
  try
    W.WriteFieldTable(AClientProperties);
    W.WriteShortStr(AMechanism);
    W.WriteLongStr(AResponse);
    W.WriteShortStr(ALocale);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeTune(const AReader: TAMQPReader): TAMQPConnectionTune;
begin
  Result.ChannelMax := AReader.ReadShortUInt;
  Result.FrameMax := AReader.ReadLongUInt;
  Result.Heartbeat := AReader.ReadShortUInt;
end;

function BuildTuneOk(const ATune: TAMQPConnectionTune): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK);
  try
    W.WriteShortUInt(ATune.ChannelMax);
    W.WriteLongUInt(ATune.FrameMax);
    W.WriteShortUInt(ATune.Heartbeat);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildOpen(const AVirtualHost: string): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN);
  try
    W.WriteShortStr(AVirtualHost);
    W.WriteShortStr('');   // reserved-1 (capabilities, obsolete)
    W.WriteBit(False);     // reserved-2 (insist, obsolete)
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeOpenOk(const AReader: TAMQPReader);
begin
  AReader.ReadShortStr; // reserved-1 (known-hosts) — discarded
end;

function DecodeClose(const AReader: TAMQPReader): TAMQPConnectionClose;
begin
  Result.ReplyCode := AReader.ReadShortUInt;
  Result.ReplyText := AReader.ReadShortStr;
  Result.ClassId := AReader.ReadShortUInt;
  Result.MethodId := AReader.ReadShortUInt;
end;

function DecodeBlocked(const AReader: TAMQPReader): string;
begin
  Result := AReader.ReadShortStr; // reason
end;

function BuildClose(const AClose: TAMQPConnectionClose): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE);
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

function BuildCloseOk: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK);
  try
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

{ Negotiation }

function NegotiateChannelMax(AServer, AClient: Word): Word;
begin
  Result := Word(NegotiateLimit(AServer, AClient));
end;

function NegotiateFrameMax(AServer, AClient: Cardinal): Cardinal;
begin
  Result := Cardinal(NegotiateLimit(AServer, AClient));
end;

function NegotiateHeartbeat(AServer, AClient: Word): Word;
begin
  Result := Word(NegotiateLimit(AServer, AClient));
end;

function NegotiateTune(const AServerTune: TAMQPConnectionTune;
  AClientChannelMax: Word; AClientFrameMax: Cardinal;
  AClientHeartbeat: Word): TAMQPConnectionTune;
begin
  Result.ChannelMax := NegotiateChannelMax(AServerTune.ChannelMax, AClientChannelMax);
  Result.FrameMax := NegotiateFrameMax(AServerTune.FrameMax, AClientFrameMax);
  Result.Heartbeat := NegotiateHeartbeat(AServerTune.Heartbeat, AClientHeartbeat);
end;

{ ===================================================================== }
{  Server side                                                          }
{ ===================================================================== }

function DefaultServerProperties: TAMQPFieldTable;
var
  LCaps: TAMQPFieldTable;
begin
  LCaps := TAMQPFieldTable.Create;
  LCaps.Put('publisher_confirms', True)
       .Put('consumer_cancel_notify', True)
       .Put('connection.blocked', True)
       .Put('authentication_failure_close', True)
       .Put('basic.nack', True)
       .Put('exchange_exchange_bindings', True);

  Result := TAMQPFieldTable.Create;
  Result.Put('product', AMQP_SERVER_PRODUCT)
        .Put('version', AMQP_SERVER_VERSION)
        .Put('platform', AMQP_SERVER_PLATFORM)
        .Put('capabilities', TValue.From<TAMQPFieldTable>(LCaps));
end;

function BuildStart(AVersionMajor, AVersionMinor: Byte;
  AServerProperties: TAMQPFieldTable;
  const AMechanisms, ALocales: string): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START);
  try
    W.WriteOctet(AVersionMajor);
    W.WriteOctet(AVersionMinor);
    W.WriteFieldTable(AServerProperties);
    W.WriteLongStr(AMechanisms);
    W.WriteLongStr(ALocales);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildSecure(const AChallenge: string): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_SECURE);
  try
    W.WriteLongStr(AChallenge);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildTune(const ATune: TAMQPConnectionTune): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE);
  try
    W.WriteShortUInt(ATune.ChannelMax);
    W.WriteLongUInt(ATune.FrameMax);
    W.WriteShortUInt(ATune.Heartbeat);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildOpenOk: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK);
  try
    W.WriteShortStr(''); // reserved-1 (known-hosts) — obsolete
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildBlocked(const AReason: string): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED);
  try
    W.WriteShortStr(AReason);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildUnblocked: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_UNBLOCKED);
  try
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeStartOk(const AReader: TAMQPReader): TAMQPConnectionStartOk;
var
  LRespLen: Cardinal;
begin
  Result.ClientProperties := AReader.ReadFieldTable;
  Result.Mechanism := AReader.ReadShortStr;
  // response is a longstr, but SASL PLAIN carries #0 — we read the raw bytes
  // (ReadLongStr would go through AmqpUtf8Decode; read the u32 and separate vars
  // because evaluation order is not guaranteed).
  LRespLen := AReader.ReadLongUInt;
  Result.Response := AReader.ReadRaw(Integer(LRespLen));
  Result.Locale := AReader.ReadShortStr;
end;

function DecodeSecureOk(const AReader: TAMQPReader): TBytes;
var
  LLen: Cardinal;
begin
  LLen := AReader.ReadLongUInt;
  Result := AReader.ReadRaw(Integer(LLen));
end;

function DecodeTuneOk(const AReader: TAMQPReader): TAMQPConnectionTune;
begin
  Result.ChannelMax := AReader.ReadShortUInt;
  Result.FrameMax := AReader.ReadLongUInt;
  Result.Heartbeat := AReader.ReadShortUInt;
end;

function DecodeOpen(const AReader: TAMQPReader): TAMQPConnectionOpen;
begin
  Result.VirtualHost := AReader.ReadShortStr;
  AReader.ReadShortStr; // reserved-1 (capabilities) — discarded
  AReader.ReadBit;      // reserved-2 (insist) — discarded
end;

procedure DecodeCloseOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

end.
