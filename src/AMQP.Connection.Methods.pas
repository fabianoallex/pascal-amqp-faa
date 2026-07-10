unit AMQP.Connection.Methods;

{$I amqp.inc}

{ Métodos da classe Connection (10) do AMQP 0-9-1 usados no handshake.

  Cada Build* devolve o payload completo de um frame de método (cabeçalho
  class/method + argumentos), pronto para virar um TAMQPFrame de tipo método no
  canal 0. Cada Decode* recebe um TAMQPReader já posicionado logo APÓS o
  cabeçalho do método (isto é, no início dos argumentos) — o chamador lê o
  cabeçalho antes, com AMQP.Method.ReadMethodHeader, para despachar.

  Sequência do handshake:
    C: protocol-header
    S: Start          -> DecodeStart
    C: Start-Ok       <- BuildStartOk
    S: Tune           -> DecodeTune
    C: Tune-Ok        <- BuildTuneOk (ver NegotiateTune)
    C: Open           <- BuildOpen
    S: Open-Ok        -> DecodeOpenOk

  Gotcha do channel-max (ver CLAUDE.md): o valor devolvido em Tune-Ok nunca pode
  exceder o proposto pelo servidor nem ser 0 quando o servidor propôs um limite
  finito. NegotiateTune resolve isso. }

interface

uses
  SysUtils,
  Rtti,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

const
  AMQP_CLIENT_PRODUCT = 'pascal-amqp-faa';
  AMQP_CLIENT_VERSION = '0.1.0';
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
    ServerProperties: TAMQPFieldTable; // dono: chamador (deve liberar)
    Mechanisms: string;                // lista separada por espaços (ex.: 'PLAIN AMQPLAIN')
    Locales: string;
    /// True se AMechanism estiver na lista de Mechanisms.
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

// --- Construção de client-properties e credenciais ------------------------

/// Client-properties padrão (product/version/platform/capabilities).
/// O chamador é dono da tabela retornada (deve liberá-la).
function DefaultClientProperties: TAMQPFieldTable;

/// Resposta do mecanismo PLAIN: #0 + usuário + #0 + senha.
function PlainAuthResponse(const AUser, APassword: string): string;

// --- Encode / decode dos métodos ------------------------------------------

function DecodeStart(const AReader: TAMQPReader): TAMQPConnectionStart;

function BuildStartOk(AClientProperties: TAMQPFieldTable;
  const AMechanism, AResponse, ALocale: string): TBytes;

function DecodeTune(const AReader: TAMQPReader): TAMQPConnectionTune;

function BuildTuneOk(const ATune: TAMQPConnectionTune): TBytes;

function BuildOpen(const AVirtualHost: string): TBytes;

/// Lê Open-Ok (só um shortstr reservado, descartado).
procedure DecodeOpenOk(const AReader: TAMQPReader);

function DecodeClose(const AReader: TAMQPReader): TAMQPConnectionClose;

/// Lê Connection.Blocked (extensão RabbitMQ): devolve o motivo (reason) informado
/// pelo broker ao entrar em resource alarm. Connection.Unblocked não tem args.
function DecodeBlocked(const AReader: TAMQPReader): string;

function BuildClose(const AClose: TAMQPConnectionClose): TBytes;

function BuildCloseOk: TBytes;

// --- Negociação do Tune ----------------------------------------------------

/// Valor negociado onde 0 significa "sem limite": se qualquer lado é 0, vence o
/// outro (o maior); se ambos são finitos, vence o menor. Assim o Tune-Ok nunca
/// excede um limite finito do servidor e nunca manda 0 sob limite finito.
function NegotiateChannelMax(AServer, AClient: Word): Word;
function NegotiateFrameMax(AServer, AClient: Cardinal): Cardinal;
function NegotiateHeartbeat(AServer, AClient: Word): Word;

/// Combina o Tune do servidor com as preferências do cliente.
function NegotiateTune(const AServerTune: TAMQPConnectionTune;
  AClientChannelMax: Word; AClientFrameMax: Cardinal;
  AClientHeartbeat: Word): TAMQPConnectionTune;

implementation

// 0 = "sem limite": se algum lado é 0, usa o maior; senão o menor.
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
  // Split manual (sem TStringHelper, para nao depender de type helpers no FPC).
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

{ Client-properties e credenciais }

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
    W.WriteShortStr('');   // reserved-1 (capabilities, obsoleto)
    W.WriteBit(False);     // reserved-2 (insist, obsoleto)
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeOpenOk(const AReader: TAMQPReader);
begin
  AReader.ReadShortStr; // reserved-1 (known-hosts) — descartado
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

{ Negociação }

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

end.
