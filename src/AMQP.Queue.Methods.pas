unit AMQP.Queue.Methods;

{$I amqp.inc}

{ Methods of the Queue class (50): declare, bind, purge and delete queues. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

type
  TAMQPQueueDeclare = record
    QueueName: string;   // '' => the server generates a name (see the recovery limitation in the README)
    Passive: Boolean;    // True => only checks that the queue exists (404 if not); declares or changes nothing
    Durable: Boolean;    // survives a broker restart (the message itself still depends on DeliveryMode=2)
    Exclusive: Boolean;  // only this connection may use it; goes away when the connection closes
    AutoDelete: Boolean; // deleted when the last consumer disconnects (never, if it never had a consumer)
    NoWait: Boolean;     // does not wait for the broker's Declare-Ok (fire-and-forget; errors only show up as Channel.Close)
    Arguments: TAMQPFieldTable; // may be nil; e.g. x-dead-letter-exchange, x-message-ttl, x-max-priority
    /// Durable, non-exclusive, non-auto-delete queue.
    class function Create(const AName: string;
      ADurable: Boolean = True): TAMQPQueueDeclare; static;
  end;

  TAMQPQueueDeclareOk = record
    QueueName: string;
    MessageCount: Cardinal;
    ConsumerCount: Cardinal;
  end;

  { Binds QueueName to ExchangeName: messages routed by the exchange that match
    RoutingKey start being delivered to the queue. The meaning of RoutingKey
    depends on the exchange type: exact route in 'direct', ignored in
    'fanout', pattern with '*'/'#' in 'topic'. }
  TAMQPQueueBind = record
    QueueName: string;
    ExchangeName: string;
    RoutingKey: string;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable; // may be nil
  end;

  { Undoes a queue->exchange binding. Unlike queue.bind, queue.unbind has NO
    no-wait flag in AMQP 0-9-1 — it always waits for Unbind-Ok. }
  TAMQPQueueUnbind = record
    QueueName: string;
    ExchangeName: string;
    RoutingKey: string;
    Arguments: TAMQPFieldTable; // may be nil (must match the bind's)
  end;

  TAMQPQueueDelete = record
    QueueName: string;
    IfUnused: Boolean; // True => aborts (error) if the queue has any consumer
    IfEmpty: Boolean;  // True => aborts (error) if the queue has messages
    NoWait: Boolean;
  end;

  { Arguments of Queue.Purge (client -> server). }
  TAMQPQueuePurge = record
    QueueName: string;
    NoWait: Boolean;
  end;

function BuildQueueDeclare(const ADeclare: TAMQPQueueDeclare): TBytes;
function DecodeQueueDeclareOk(const AReader: TAMQPReader): TAMQPQueueDeclareOk;

function BuildQueueBind(const ABind: TAMQPQueueBind): TBytes;
procedure DecodeQueueBindOk(const AReader: TAMQPReader);

function BuildQueueUnbind(const AUnbind: TAMQPQueueUnbind): TBytes;
procedure DecodeQueueUnbindOk(const AReader: TAMQPReader);

function BuildQueuePurge(const AQueueName: string; ANoWait: Boolean = False): TBytes;
function DecodeQueuePurgeOk(const AReader: TAMQPReader): Cardinal; // message-count

function BuildQueueDelete(const ADelete: TAMQPQueueDelete): TBytes;
function DecodeQueueDeleteOk(const AReader: TAMQPReader): Cardinal; // message-count

// --- Server side (broker sub-module) -------------------------------------
// Decode of the client->server methods and build of the *-Ok ones. In the
// decoded records the Arguments field is always a new table (even if empty) and
// belongs to the caller (who must free it).

function DecodeQueueDeclare(const AReader: TAMQPReader): TAMQPQueueDeclare;
/// Queue.Declare-Ok (11): queue name (the generated one, if the client sent '') +
/// current counts.
function BuildQueueDeclareOk(const AQueueName: string;
  AMessageCount, AConsumerCount: Cardinal): TBytes;

function DecodeQueueBind(const AReader: TAMQPReader): TAMQPQueueBind;
function BuildQueueBindOk: TBytes;

function DecodeQueueUnbind(const AReader: TAMQPReader): TAMQPQueueUnbind;
function BuildQueueUnbindOk: TBytes;

function DecodeQueuePurge(const AReader: TAMQPReader): TAMQPQueuePurge;
function BuildQueuePurgeOk(AMessageCount: Cardinal): TBytes;

function DecodeQueueDelete(const AReader: TAMQPReader): TAMQPQueueDelete;
function BuildQueueDeleteOk(AMessageCount: Cardinal): TBytes;

implementation

{ TAMQPQueueDeclare }

class function TAMQPQueueDeclare.Create(const AName: string;
  ADurable: Boolean): TAMQPQueueDeclare;
begin
  Result := Default(TAMQPQueueDeclare);
  Result.QueueName := AName;
  Result.Durable := ADurable;
end;

function BuildQueueDeclare(const ADeclare: TAMQPQueueDeclare): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(ADeclare.QueueName);
    W.WriteBit(ADeclare.Passive);
    W.WriteBit(ADeclare.Durable);
    W.WriteBit(ADeclare.Exclusive);
    W.WriteBit(ADeclare.AutoDelete);
    W.WriteBit(ADeclare.NoWait);
    W.WriteFieldTable(ADeclare.Arguments);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeQueueDeclareOk(const AReader: TAMQPReader): TAMQPQueueDeclareOk;
begin
  Result.QueueName := AReader.ReadShortStr;
  Result.MessageCount := AReader.ReadLongUInt;
  Result.ConsumerCount := AReader.ReadLongUInt;
end;

function BuildQueueBind(const ABind: TAMQPQueueBind): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMQP_QUEUE_BIND);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(ABind.QueueName);
    W.WriteShortStr(ABind.ExchangeName);
    W.WriteShortStr(ABind.RoutingKey);
    W.WriteBit(ABind.NoWait);
    W.WriteFieldTable(ABind.Arguments);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeQueueBindOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

function BuildQueueUnbind(const AUnbind: TAMQPQueueUnbind): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMQP_QUEUE_UNBIND);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(AUnbind.QueueName);
    W.WriteShortStr(AUnbind.ExchangeName);
    W.WriteShortStr(AUnbind.RoutingKey);
    W.WriteFieldTable(AUnbind.Arguments); // no no-wait: unbind always replies Ok
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeQueueUnbindOk(const AReader: TAMQPReader);
begin
  // no arguments
end;

function BuildQueuePurge(const AQueueName: string; ANoWait: Boolean): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMQP_QUEUE_PURGE);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(AQueueName);
    W.WriteBit(ANoWait);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeQueuePurgeOk(const AReader: TAMQPReader): Cardinal;
begin
  Result := AReader.ReadLongUInt; // message-count
end;

function BuildQueueDelete(const ADelete: TAMQPQueueDelete): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMQP_QUEUE_DELETE);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(ADelete.QueueName);
    W.WriteBit(ADelete.IfUnused);
    W.WriteBit(ADelete.IfEmpty);
    W.WriteBit(ADelete.NoWait);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeQueueDeleteOk(const AReader: TAMQPReader): Cardinal;
begin
  Result := AReader.ReadLongUInt; // message-count
end;

{ Server side }

function EmptyMethod(AMethodId: Word): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMethodId);
  try
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function CountMethod(AMethodId: Word; ACount: Cardinal): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMethodId);
  try
    W.WriteLongUInt(ACount);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeQueueDeclare(const AReader: TAMQPReader): TAMQPQueueDeclare;
begin
  Result := Default(TAMQPQueueDeclare);
  AReader.ReadShortUInt; // reserved-1
  Result.QueueName := AReader.ReadShortStr;
  Result.Passive := AReader.ReadBit;
  Result.Durable := AReader.ReadBit;
  Result.Exclusive := AReader.ReadBit;
  Result.AutoDelete := AReader.ReadBit;
  Result.NoWait := AReader.ReadBit;
  Result.Arguments := AReader.ReadFieldTable; // owner: caller
end;

function BuildQueueDeclareOk(const AQueueName: string;
  AMessageCount, AConsumerCount: Cardinal): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK);
  try
    W.WriteShortStr(AQueueName);
    W.WriteLongUInt(AMessageCount);
    W.WriteLongUInt(AConsumerCount);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

function DecodeQueueBind(const AReader: TAMQPReader): TAMQPQueueBind;
begin
  Result := Default(TAMQPQueueBind);
  AReader.ReadShortUInt; // reserved-1
  Result.QueueName := AReader.ReadShortStr;
  Result.ExchangeName := AReader.ReadShortStr;
  Result.RoutingKey := AReader.ReadShortStr;
  Result.NoWait := AReader.ReadBit;
  Result.Arguments := AReader.ReadFieldTable; // owner: caller
end;

function BuildQueueBindOk: TBytes;
begin
  Result := EmptyMethod(AMQP_QUEUE_BIND_OK);
end;

function DecodeQueueUnbind(const AReader: TAMQPReader): TAMQPQueueUnbind;
begin
  Result := Default(TAMQPQueueUnbind);
  AReader.ReadShortUInt; // reserved-1
  Result.QueueName := AReader.ReadShortStr;
  Result.ExchangeName := AReader.ReadShortStr;
  Result.RoutingKey := AReader.ReadShortStr;
  // queue.unbind has no no-wait
  Result.Arguments := AReader.ReadFieldTable; // owner: caller
end;

function BuildQueueUnbindOk: TBytes;
begin
  Result := EmptyMethod(AMQP_QUEUE_UNBIND_OK);
end;

function DecodeQueuePurge(const AReader: TAMQPReader): TAMQPQueuePurge;
begin
  Result := Default(TAMQPQueuePurge);
  AReader.ReadShortUInt; // reserved-1
  Result.QueueName := AReader.ReadShortStr;
  Result.NoWait := AReader.ReadBit;
end;

function BuildQueuePurgeOk(AMessageCount: Cardinal): TBytes;
begin
  Result := CountMethod(AMQP_QUEUE_PURGE_OK, AMessageCount);
end;

function DecodeQueueDelete(const AReader: TAMQPReader): TAMQPQueueDelete;
begin
  Result := Default(TAMQPQueueDelete);
  AReader.ReadShortUInt; // reserved-1
  Result.QueueName := AReader.ReadShortStr;
  Result.IfUnused := AReader.ReadBit;
  Result.IfEmpty := AReader.ReadBit;
  Result.NoWait := AReader.ReadBit;
end;

function BuildQueueDeleteOk(AMessageCount: Cardinal): TBytes;
begin
  Result := CountMethod(AMQP_QUEUE_DELETE_OK, AMessageCount);
end;

end.
