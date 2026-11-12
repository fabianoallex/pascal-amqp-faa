unit AMQP.Exchange.Methods;

{$I amqp.inc}

{ Methods of the Exchange class (40): exchange declaration and removal. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

const
  AMQP_EXCHANGE_TYPE_DIRECT = 'direct';
  AMQP_EXCHANGE_TYPE_FANOUT = 'fanout';
  AMQP_EXCHANGE_TYPE_TOPIC  = 'topic';
  AMQP_EXCHANGE_TYPE_HEADERS = 'headers';

type
  TAMQPExchangeDeclare = record
    ExchangeName: string;
    ExchangeType: string; // AMQP_EXCHANGE_TYPE_DIRECT/FANOUT/TOPIC/HEADERS (or a custom type installed on the broker)
    Passive: Boolean;     // True => only checks that the exchange exists (404 if not); declares or changes nothing
    Durable: Boolean;     // survives a broker restart
    AutoDelete: Boolean;  // deleted when the last bind is removed (never, if it never had a bind)
    Internal: Boolean;    // True => only receives from other exchanges (exchange->exchange bind); direct publish is refused
    NoWait: Boolean;      // does not wait for the broker's Declare-Ok (fire-and-forget; errors only show up as Channel.Close)
    Arguments: TAMQPFieldTable; // may be nil (empty table)
    /// Default declaration: durable, non-passive exchange, type 'direct'.
    class function Create(const AName: string;
      const AType: string = AMQP_EXCHANGE_TYPE_DIRECT;
      ADurable: Boolean = True): TAMQPExchangeDeclare; static;
  end;

  TAMQPExchangeDelete = record
    ExchangeName: string;
    IfUnused: Boolean;
    NoWait: Boolean;
  end;

  { Exchange->exchange binding (RabbitMQ extension). Destination receives the
    messages routed from Source by RoutingKey. Serves both bind and
    unbind (same argument layout). }
  TAMQPExchangeBinding = record
    Destination: string;         // receiving exchange (destination)
    Source: string;              // source exchange
    RoutingKey: string;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable;  // may be nil
  end;

function BuildExchangeDeclare(const ADeclare: TAMQPExchangeDeclare): TBytes;
procedure DecodeExchangeDeclareOk(const AReader: TAMQPReader);

function BuildExchangeDelete(const ADelete: TAMQPExchangeDelete): TBytes;
procedure DecodeExchangeDeleteOk(const AReader: TAMQPReader);

function BuildExchangeBind(const ABind: TAMQPExchangeBinding): TBytes;
procedure DecodeExchangeBindOk(const AReader: TAMQPReader);

function BuildExchangeUnbind(const AUnbind: TAMQPExchangeBinding): TBytes;
procedure DecodeExchangeUnbindOk(const AReader: TAMQPReader);

// --- Server side (broker sub-module) -------------------------------------
// Decode of the client->server methods and build of the *-Ok ones. In the
// decoded records the Arguments field is always a new table (even if empty) and
// belongs to the caller (who must free it).

function DecodeExchangeDeclare(const AReader: TAMQPReader): TAMQPExchangeDeclare;
function BuildExchangeDeclareOk: TBytes;

function DecodeExchangeDelete(const AReader: TAMQPReader): TAMQPExchangeDelete;
function BuildExchangeDeleteOk: TBytes;

function DecodeExchangeBind(const AReader: TAMQPReader): TAMQPExchangeBinding;
function BuildExchangeBindOk: TBytes;

function DecodeExchangeUnbind(const AReader: TAMQPReader): TAMQPExchangeBinding;
function BuildExchangeUnbindOk: TBytes;

implementation

{ TAMQPExchangeDeclare }

class function TAMQPExchangeDeclare.Create(const AName, AType: string;
  ADurable: Boolean): TAMQPExchangeDeclare;
begin
  Result := Default(TAMQPExchangeDeclare);
  Result.ExchangeName := AName;
  Result.ExchangeType := AType;
  Result.Durable := ADurable;
end;

function BuildExchangeDeclare(const ADeclare: TAMQPExchangeDeclare): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE);
  try
    W.WriteShortUInt(0); // reserved-1 (ticket)
    W.WriteShortStr(ADeclare.ExchangeName);
    W.WriteShortStr(ADeclare.ExchangeType);
    W.WriteBit(ADeclare.Passive);
    W.WriteBit(ADeclare.Durable);
    W.WriteBit(ADeclare.AutoDelete);
    W.WriteBit(ADeclare.Internal);
    W.WriteBit(ADeclare.NoWait);
    W.WriteFieldTable(ADeclare.Arguments);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeExchangeDeclareOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

function BuildExchangeDelete(const ADelete: TAMQPExchangeDelete): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DELETE);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(ADelete.ExchangeName);
    W.WriteBit(ADelete.IfUnused);
    W.WriteBit(ADelete.NoWait);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeExchangeDeleteOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

// exchange.bind and exchange.unbind have the SAME argument layout
// (reserved-1, destination, source, routing-key, no-wait, arguments).
function BuildExchangeBindLike(AMethodId: Word;
  const ABinding: TAMQPExchangeBinding): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_EXCHANGE, AMethodId);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(ABinding.Destination);
    W.WriteShortStr(ABinding.Source);
    W.WriteShortStr(ABinding.RoutingKey);
    W.WriteBit(ABinding.NoWait);
    W.WriteFieldTable(ABinding.Arguments);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function BuildExchangeBind(const ABind: TAMQPExchangeBinding): TBytes;
begin
  Result := BuildExchangeBindLike(AMQP_EXCHANGE_BIND, ABind);
end;

procedure DecodeExchangeBindOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

function BuildExchangeUnbind(const AUnbind: TAMQPExchangeBinding): TBytes;
begin
  Result := BuildExchangeBindLike(AMQP_EXCHANGE_UNBIND, AUnbind);
end;

procedure DecodeExchangeUnbindOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

{ Server side }

function EmptyMethod(AMethodId: Word): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_EXCHANGE, AMethodId);
  try
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeExchangeDeclare(const AReader: TAMQPReader): TAMQPExchangeDeclare;
begin
  Result := Default(TAMQPExchangeDeclare);
  AReader.ReadShortUInt; // reserved-1 (ticket)
  Result.ExchangeName := AReader.ReadShortStr;
  Result.ExchangeType := AReader.ReadShortStr;
  Result.Passive := AReader.ReadBit;
  Result.Durable := AReader.ReadBit;
  Result.AutoDelete := AReader.ReadBit;
  Result.Internal := AReader.ReadBit;
  Result.NoWait := AReader.ReadBit;
  Result.Arguments := AReader.ReadFieldTable; // owner: caller
end;

function BuildExchangeDeclareOk: TBytes;
begin
  Result := EmptyMethod(AMQP_EXCHANGE_DECLARE_OK);
end;

function DecodeExchangeDelete(const AReader: TAMQPReader): TAMQPExchangeDelete;
begin
  Result := Default(TAMQPExchangeDelete);
  AReader.ReadShortUInt; // reserved-1
  Result.ExchangeName := AReader.ReadShortStr;
  Result.IfUnused := AReader.ReadBit;
  Result.NoWait := AReader.ReadBit;
end;

function BuildExchangeDeleteOk: TBytes;
begin
  Result := EmptyMethod(AMQP_EXCHANGE_DELETE_OK);
end;

// bind and unbind: same layout (reserved-1, destination, source, routing-key,
// no-wait, arguments).
function DecodeExchangeBindLike(const AReader: TAMQPReader): TAMQPExchangeBinding;
begin
  Result := Default(TAMQPExchangeBinding);
  AReader.ReadShortUInt; // reserved-1
  Result.Destination := AReader.ReadShortStr;
  Result.Source := AReader.ReadShortStr;
  Result.RoutingKey := AReader.ReadShortStr;
  Result.NoWait := AReader.ReadBit;
  Result.Arguments := AReader.ReadFieldTable; // owner: caller
end;

function DecodeExchangeBind(const AReader: TAMQPReader): TAMQPExchangeBinding;
begin
  Result := DecodeExchangeBindLike(AReader);
end;

function BuildExchangeBindOk: TBytes;
begin
  Result := EmptyMethod(AMQP_EXCHANGE_BIND_OK);
end;

function DecodeExchangeUnbind(const AReader: TAMQPReader): TAMQPExchangeBinding;
begin
  Result := DecodeExchangeBindLike(AReader);
end;

function BuildExchangeUnbindOk: TBytes;
begin
  // quirk of RabbitMQ's extended spec: unbind-ok is 51, not 41.
  Result := EmptyMethod(AMQP_EXCHANGE_UNBIND_OK);
end;

end.
