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

end.
