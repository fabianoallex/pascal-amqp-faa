unit AMQP.ServerTestDoubles;

{ Dublês compartilhados pelas suítes do broker -- WS4 da Fase 2.

  Vive numa unit própria porque duas fixtures precisam do mesmo alvo de
  entrega falso: a da fila-ator (AMQP.ServerQueueTests, onde consumidores
  existem só para serem contados e o alvo tem de RECUSAR tudo para o estoque
  ficar onde está) e a da entrega (AMQP.ServerDeliveryTests, onde o alvo
  registra o que recebeu).

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerTestDoubles.pas -- mantenha os
  dois em sincronia. }

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  AMQP.Wire,
  AMQP.Server.Message,
  AMQP.Server.Queue;

type
  { Uma entrega registrada pelo alvo falso. }
  TEntregaReg = record
    ConsumerTag: string;
    QueueName: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Corpo: string;
  end;

  { Alvo de entrega falso: registra o que recebeu e permite mandar recusar ou
    morrer. Refcontado como o de verdade -- a fila segura a interface, então o
    objeto sobrevive enquanto ela o tiver na mão. }
  TAlvoFalso = class(TInterfacedObject, IAMQPDeliveryTarget)
  private
    FLock: TCriticalSection;
    FEntregas: TList<TEntregaReg>;
    FId: NativeUInt;
    FNextTag: UInt64;
    FRecusar: Boolean;
    FVivo: Boolean;
  public
    constructor Create(AId: NativeUInt);
    destructor Destroy; override;

    function ChannelId: NativeUInt;
    function IsAlive: Boolean;
    function TryDeliver(const AConsumerTag, AQueueName: string;
      ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
      out ADeliveryTag: UInt64): Boolean;

    function Count: Integer;
    function Entrega(AIndex: Integer): TEntregaReg;
    /// Corpos entregues, na ordem, separados por '|'.
    function Corpos: string;
    property Recusar: Boolean read FRecusar write FRecusar;
    property Vivo: Boolean read FVivo write FVivo;
  end;

/// Alvo VIVO que recusa toda entrega. É o que os testes da fila-ator usam
/// quando o consumidor existe só para ser contado: vivo (senão o ator o tira
/// do rodízio) mas sem consumir o estoque.
function AlvoQueRecusa(AId: NativeUInt): IAMQPDeliveryTarget;

implementation

function AlvoQueRecusa(AId: NativeUInt): IAMQPDeliveryTarget;
var
  LAlvo: TAlvoFalso;
begin
  LAlvo := TAlvoFalso.Create(AId);
  LAlvo.Recusar := True;
  Result := LAlvo;
end;

{ TAlvoFalso }

constructor TAlvoFalso.Create(AId: NativeUInt);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FEntregas := TList<TEntregaReg>.Create;
  FId := AId;
  FNextTag := 1;
  FVivo := True;
end;

destructor TAlvoFalso.Destroy;
begin
  FEntregas.Free;
  FLock.Free;
  inherited;
end;

function TAlvoFalso.ChannelId: NativeUInt;
begin
  Result := FId;
end;

function TAlvoFalso.IsAlive: Boolean;
begin
  Result := FVivo;
end;

function TAlvoFalso.TryDeliver(const AConsumerTag, AQueueName: string;
  ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
  out ADeliveryTag: UInt64): Boolean;
var
  LReg: TEntregaReg;
begin
  ADeliveryTag := 0;
  if FRecusar or (not FVivo) then
    Exit(False);
  FLock.Enter;
  try
    LReg.ConsumerTag := AConsumerTag;
    LReg.QueueName := AQueueName;
    LReg.DeliveryTag := FNextTag;
    LReg.Redelivered := ARedelivered;
    LReg.Corpo := AmqpUtf8Decode(AMessage.Body);
    FEntregas.Add(LReg);
    ADeliveryTag := FNextTag;
    Inc(FNextTag);
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FEntregas.Count;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Entrega(AIndex: Integer): TEntregaReg;
begin
  FLock.Enter;
  try
    Result := FEntregas[AIndex];
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Corpos: string;
var
  I: Integer;
begin
  Result := '';
  FLock.Enter;
  try
    for I := 0 to FEntregas.Count - 1 do
    begin
      if Result <> '' then
        Result := Result + '|';
      Result := Result + FEntregas[I].Corpo;
    end;
  finally
    FLock.Leave;
  end;
end;

end.
