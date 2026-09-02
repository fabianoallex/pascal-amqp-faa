unit AMQP.Channel.Methods;

{$I amqp.inc}

{ Métodos da classe Channel (20): abertura e fechamento de canal.

  Convenção igual à de AMQP.Connection.Methods: Build* devolve o payload
  completo do método (cabeçalho + args); Decode* recebe um TAMQPReader já
  posicionado após o cabeçalho do método. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

type
  { Argumentos de Channel.Close / Connection.Close (mesma forma). }
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

// --- Lado servidor (sub-módulo broker) -----------------------------------
// Build dos métodos servidor->cliente e decode dos cliente->servidor.
// Channel.Close / DecodeChannelClose acima já servem aos dois sentidos.

/// Channel.Open-Ok (11) — só um longstr reservado (channel-id), vazio.
function BuildChannelOpenOk: TBytes;

/// Channel.Open (10) — consome o shortstr reservado e descarta.
procedure DecodeChannelOpen(const AReader: TAMQPReader);

/// Channel.Close-Ok (41) — sem argumentos.
procedure DecodeChannelCloseOk(const AReader: TAMQPReader);

/// Channel.Flow (20) / Flow-Ok (21) — pausa/retoma o fluxo no canal.
/// active=False pede ao par que pare de mandar conteúdo; o par confirma com
/// Flow-Ok ecoando o mesmo active. (O RabbitMQ usa TCP backpressure e
/// connection.blocked no lugar disto, mas o codec fica completo.)
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
    W.WriteShortStr(''); // reserved-1 (obsoleto)
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeChannelOpenOk(const AReader: TAMQPReader);
begin
  AReader.ReadLongStr; // reserved-1 — descartado
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

{ Lado servidor }

function BuildChannelOpenOk: TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK);
  try
    W.WriteLongStr(''); // reserved-1 (channel-id) — obsoleto
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeChannelOpen(const AReader: TAMQPReader);
begin
  AReader.ReadShortStr; // reserved-1 — descartado
end;

procedure DecodeChannelCloseOk(const AReader: TAMQPReader);
begin
  // sem argumentos
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
