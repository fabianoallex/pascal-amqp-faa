unit AMQP.Queue.Methods;

{$I amqp.inc}

{ Métodos da classe Queue (50): declarar, ligar (bind), purgar e remover filas. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

type
  TAMQPQueueDeclare = record
    QueueName: string;   // '' => o servidor gera um nome
    Passive: Boolean;
    Durable: Boolean;
    Exclusive: Boolean;
    AutoDelete: Boolean;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable; // pode ser nil
    /// Fila durável, não exclusiva, não auto-delete.
    class function Create(const AName: string;
      ADurable: Boolean = True): TAMQPQueueDeclare; static;
  end;

  TAMQPQueueDeclareOk = record
    QueueName: string;
    MessageCount: Cardinal;
    ConsumerCount: Cardinal;
  end;

  TAMQPQueueBind = record
    QueueName: string;
    ExchangeName: string;
    RoutingKey: string;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable; // pode ser nil
  end;

  { Desfaz um binding fila->exchange. Diferente de queue.bind, queue.unbind NÃO
    tem flag no-wait no AMQP 0-9-1 — sempre aguarda Unbind-Ok. }
  TAMQPQueueUnbind = record
    QueueName: string;
    ExchangeName: string;
    RoutingKey: string;
    Arguments: TAMQPFieldTable; // pode ser nil (deve casar com o do bind)
  end;

  TAMQPQueueDelete = record
    QueueName: string;
    IfUnused: Boolean;
    IfEmpty: Boolean;
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
  // sem argumentos
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
    W.WriteFieldTable(AUnbind.Arguments); // sem no-wait: unbind sempre responde Ok
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeQueueUnbindOk(const AReader: TAMQPReader);
begin
  // sem argumentos
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

end.
