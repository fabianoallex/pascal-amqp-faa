{$I amqp.inc}

unit AMQP.Server.Events;

{ Sistema de eventos de observabilidade do broker (Fase 4.1).

  Fornece um padrão Observer para capturar tudo que acontece no broker:
  conexões, canais, publishes, enqueues, delivers, acks, expiração, etc.

  Essencial para auditoria, debugging e compliance em aplicações embarcadas.
  Sem bloquear o engine: callbacks rodam fora do crítico, sem segurar locks
  internos. O handler é chamado com um TAMQPServerEvent imutável.

  Integração:
    procedure TMyApp.OnAMQPEvent(const Event: TAMQPServerEvent);
    begin
      case Event.EventType of
        seMessageDelivered: RecordConsumption(Event);
        seMessageAcked: MarkAsUsed(Event);
        seMessageExpired: Alert(Event);
      end;
    end;

    Server.Subscribe(OnAMQPEvent);
    Server.Start;
}

interface

uses
  SysUtils;

type
  TAMQPServerEventType = (
    { Ciclo de vida da conexão }
    seConnectionEstablished,    { Nova conexão TCP aceita }
    seConnectionAuthenticated,  { SASL OK (antes do Channel.OpenOk) }
    seConnectionClosed,         { Conexão fechada }

    { Ciclo de vida do canal }
    seChannelOpened,
    seChannelClosed,

    { Publish }
    seMessagePublished,         { Mensagem chegou via Basic.Publish }
    seMessagePublishRejected,   { Publish recusado (fila cheia, sem rota) }

    { Enqueue }
    seMessageEnqueued,          { Mensagem entrou em fila }

    { Consume }
    seConsumerRegistered,       { Basic.Consume OK }
    seConsumerCancelled,        { Basic.Cancel OK }
    seMessageDelivered,         { Mensagem saiu da fila pro consumer }

    { Reconhecimento }
    seMessageAcked,             { Basic.Ack recebido }
    seMessageNacked,            { Basic.Nack recebido }
    seMessageRejected,          { Basic.Reject recebido }

    { Ciclo de vida especial }
    seMessageExpired,           { TTL expirou }
    seMessageDeadLettered,      { Entrou em DLX }
    seMessageDropped,           { Descarte por teto }

    { Durabilidade }
    seJournalFlushed            { Lote fsync'd }
  );

  { Evento estruturado capturado pelo broker. Imutável. }
  TAMQPServerEvent = record
    EventType: TAMQPServerEventType;

    { Relógio e timing }
    WallTime: TDateTime;   { Hora UTC (para correlação com logs externos) }
    TickMs: Int64;         { Monotônico em ms (para medir latência) }

    { Contexto de conexão }
    ConnectionId: UInt64;  { Identificador único da conexão }
    RemoteAddr: string;    { IP:porta do cliente }
    Username: string;      { Quem se autenticou }
    VHost: string;         { Virtual host }

    { Contexto de canal }
    ChannelNumber: Word;   { 0 = conexão; N = canal específico }

    { Contexto de fila/mensagem }
    QueueName: string;
    ConsumerTag: string;
    DeliveryTag: UInt64;   { Numbering para ack/nack dentro do canal }
    ExchangeName: string;
    RoutingKey: string;

    { Dados da mensagem }
    MessageSize: UInt64;
    MessagePriority: Byte;
    Redelivered: Boolean;
    Mandatory: Boolean;

    { TTL (em ms; 0 = sem limite) }
    MessageExpirationMs: UInt32;
    QueueExpirationMs: UInt32;

    { Correlação }
    CorrelationId: string;
    ReplyTo: string;

    { Razão do evento (quando aplicável) }
    Reason: string;

    { Para durabilidade: LSN do último write desta operação }
    JournalLsn: UInt64;
  end;

  TAMQPServerEventHandler = procedure(const Event: TAMQPServerEvent) of object;

implementation

end.
