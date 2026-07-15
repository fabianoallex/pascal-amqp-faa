unit AMQP.Exchange.Methods;

{$I amqp.inc}

{ Métodos da classe Exchange (40): declaração e remoção de exchange. }

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
    ExchangeType: string; // AMQP_EXCHANGE_TYPE_DIRECT/FANOUT/TOPIC/HEADERS (ou tipo custom instalado no broker)
    Passive: Boolean;     // True => só verifica se o exchange existe (erro 404 se não); não declara nem altera nada
    Durable: Boolean;     // sobrevive a um restart do broker
    AutoDelete: Boolean;  // apagado quando o último bind é removido (nunca, se nunca teve bind)
    Internal: Boolean;    // True => só recebe de outros exchanges (bind exchange->exchange); publish direto é recusado
    NoWait: Boolean;      // não aguarda Declare-Ok do broker (fire-and-forget; erros só aparecem como Channel.Close)
    Arguments: TAMQPFieldTable; // pode ser nil (tabela vazia)
    /// Declaração padrão: exchange durável, não passiva, tipo 'direct'.
    class function Create(const AName: string;
      const AType: string = AMQP_EXCHANGE_TYPE_DIRECT;
      ADurable: Boolean = True): TAMQPExchangeDeclare; static;
  end;

  TAMQPExchangeDelete = record
    ExchangeName: string;
    IfUnused: Boolean;
    NoWait: Boolean;
  end;

  { Binding exchange->exchange (extensão RabbitMQ). Destination recebe as
    mensagens roteadas de Source pela RoutingKey. Serve tanto para bind quanto
    para unbind (mesmo layout de argumentos). }
  TAMQPExchangeBinding = record
    Destination: string;         // exchange que recebe (destino)
    Source: string;              // exchange de origem
    RoutingKey: string;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable;  // pode ser nil
  end;

function BuildExchangeDeclare(const ADeclare: TAMQPExchangeDeclare): TBytes;
procedure DecodeExchangeDeclareOk(const AReader: TAMQPReader);

function BuildExchangeDelete(const ADelete: TAMQPExchangeDelete): TBytes;
procedure DecodeExchangeDeleteOk(const AReader: TAMQPReader);

function BuildExchangeBind(const ABind: TAMQPExchangeBinding): TBytes;
procedure DecodeExchangeBindOk(const AReader: TAMQPReader);

function BuildExchangeUnbind(const AUnbind: TAMQPExchangeBinding): TBytes;
procedure DecodeExchangeUnbindOk(const AReader: TAMQPReader);

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
  // sem argumentos
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
  // sem argumentos
end;

// exchange.bind e exchange.unbind têm o MESMO layout de argumentos
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
  // sem argumentos
end;

function BuildExchangeUnbind(const AUnbind: TAMQPExchangeBinding): TBytes;
begin
  Result := BuildExchangeBindLike(AMQP_EXCHANGE_UNBIND, AUnbind);
end;

procedure DecodeExchangeUnbindOk(const AReader: TAMQPReader);
begin
  // sem argumentos
end;

end.
