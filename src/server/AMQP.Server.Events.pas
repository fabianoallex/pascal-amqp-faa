unit AMQP.Server.Events;

{$I amqp.inc}

{ Vocabulário de observabilidade do broker (Fase 4.1, D29-D36).

  Esta unit tem SÓ tipos: o que é um evento, que tipos existem e como se
  monta um. Quem os transporta é o TAMQPEventBus (AMQP.Server.EventBus);
  quem os expõe é o TAMQPServer. Ela não usa nada do submódulo de propósito
  -- é a base da pilha, e o AMQP.Server.Types depende dela para declarar o
  IAMQPEventSink.

  O QUE ESTE MECANISMO É (D29): um observador READ-ONLY. Nenhum handler
  influencia roteamento, autenticação, ack ou descarte -- quem precisa
  DECIDIR usa o IAMQPAuthorizer. Sem nenhum assinante o caminho quente paga
  uma leitura de máscara e nem chega a montar o record.

  O QUE ELE NÃO É (D36): log de auditoria. A entrega é best-effort e a D31
  admite perda por desenho, contada em DroppedCount. Quem precisa de tudo
  guarda tudo do lado de dentro do handler -- e mesmo assim perde o que for
  descartado sob pressão.

  POR QUE O RECORD É FLAT (D34): porque TODO evento é entregue por outra
  thread (D30). Um evento é uma cópia POR VALOR, sem um único ponteiro para
  conexão, canal ou mensagem -- qualquer um deles pode morrer entre a
  emissão e a entrega, e com record flat isso não é problema de ninguém.
  É a mesma razão da D1 (nada de TValue, cuja dor no FPC já está catalogada
  no CLAUDE.md).

  Três amarras da D34, todas com motivo:

  - O CORPO DA MENSAGEM NUNCA ENTRA no evento, só MessageSize. Carregá-lo
    derrubaria o COW da D1 e poria uma cópia de buffer no caminho mais
    quente que existe.
  - O relógio de parede é WallMs (Int64, epoch ms UTC) via AmqpWallMs, e
    NÃO TDateTime: a conversão DateTimeToUnix(LocalTimeToUniversal(...)) da
    D21 converte duas vezes e o erro é invisível em máquina UTC -- ou seja,
    invisível em todo container onde a suíte roda. TickMs (monotônico) fica
    só para medir latência, e não sobrevive a um restart.
  - Cada tipo popula um conjunto FIXO de campos; o resto sai zerado, porque
    AmqpNewEvent parte de um record zerado. A tabela campo-por-tipo está em
    docs/observabilidade.md e os testes a asseram.

  Acrescentar campo ao record é a direção compatível (handler existente
  continua compilando); tirar campo, não. }

interface

uses
  SysUtils,
  AMQP.Threading; // AmqpWallMs, AmqpTickMs

type
  { O que aconteceu. A ORDEM DESTE ENUM É API: o valor ordinal vira bit na
    máscara de assinatura (D32), então acrescentar tipo é sempre NO FIM.

    TETO DE 32 TIPOS (D32): a máscara é um Cardinal, lido sem lock no
    caminho quente. Passar de 32 é mudar o tipo da máscara -- decisão
    consciente, não um enum acrescentado por distração. 18 usados, 14 de
    folga.

    A coluna [thread] diz onde o evento NASCE, que é o que a D30 existe para
    tornar irrelevante: todos são entregues pela notificadora. }
  TAMQPServerEventType = (
    { --- ciclo de vida da conexão [thread de leitura] --- }
    seConnectionEstablished,   // socket aceito, antes de qualquer byte AMQP
    seConnectionAuthenticated, // SASL PLAIN OK e vhost aberto (Open-Ok)
    seConnectionClosed,        // thread de leitura saiu (Code = reply-code)

    { --- ciclo de vida do canal [thread de leitura] --- }
    seChannelOpened,
    seChannelClosed,           // Code = reply-code (0 = fechamento limpo)

    { --- publicação [thread de leitura] --- }
    seMessagePublished,        // conteúdo remontado e roteado
    seMessagePublishRejected,  // sem rota (mandatory) ou reject-publish

    { --- consumidores [thread de leitura] --- }
    seConsumerRegistered,      // Basic.Consume aceito
    seConsumerCancelled,       // Basic.Cancel, ou cancelamento pelo servidor

    { --- reconhecimento [thread de leitura] --- }
    seMessageAcked,
    seMessageNacked,
    seMessageRejected,

    { --- ciclo de vida da mensagem [ator da fila -- Inc. 2] --- }
    seMessageEnqueued,
    seMessageDelivered,
    seMessageExpired,          // TTL venceu (Reason = 'expired')
    seMessageDeadLettered,     // foi para a DLX (Reason = razão do x-death)
    seMessageDropped,          // descarte por teto (Reason = 'maxlen'...)

    { --- durabilidade [thread do journal -- Inc. 2] --- }
    seJournalFlushed           // um LOTE ficou durável (D25: nunca por registro)
  );

  { Conjunto de tipos que um assinante quer. Vira máscara Cardinal dentro do
    barramento -- ver AmqpEventMask. }
  TAMQPServerEventTypes = set of TAMQPServerEventType;

  { Um fato do broker, achatado. Ver o cabeçalho da unit para o porquê de
    cada amarra; docs/observabilidade.md tem a tabela de quem popula o quê.

    Todo campo que o tipo do evento não popula sai ZERADO (string vazia,
    numérico 0, Boolean False). }
  TAMQPServerEvent = record
    EventType: TAMQPServerEventType;

    { Relógios. WallMs correlaciona com log externo; TickMs mede latência
      (monotônico, não sobrevive a restart). }
    WallMs: Int64;
    TickMs: Int64;

    { Contexto da conexão. ConnectionId é o mesmo identificador que a fila
      exclusiva usa para saber de quem ela é. }
    ConnectionId: UInt64;
    RemoteAddr: string;
    Username: string;
    VHost: string;

    { Contexto do canal. 0 = evento de conexão. }
    ChannelNumber: Word;
    { reply-code do AMQP quando o evento é um fechamento (0 = limpo). }
    Code: Word;

    { Contexto de fila / roteamento. }
    QueueName: string;
    ConsumerTag: string;
    ExchangeName: string;
    RoutingKey: string;

    { Mensagem. NUNCA o corpo (D34) -- só o tamanho dele. }
    DeliveryTag: UInt64;
    MessageSize: UInt64;
    Priority: Byte;
    Redelivered: Boolean;
    Mandatory: Boolean;  // publish pediu Basic.Return se nao rotear
    Multiple: Boolean;   // ack/nack com multiple=true
    Requeue: Boolean;    // nack/reject com requeue=true

    { LSN do journal (seJournalFlushed) e contagem genérica -- quantos
      registros o lote levou, quantas mensagens o descarte comeu. }
    Lsn: UInt64;
    Count: Int64;

    { Por que, quando o tipo sozinho não diz. }
    Reason: string;
  end;

  { Para onde vao os eventos de observabilidade (Fase 4.1, D32).

    Vive AQUI, e nao em AMQP.Server.Types como a D32 dizia primeiro,
    porque o ator da fila e a thread do journal tambem emitem (Inc. 2) e
    nenhuma das duas units usa Types -- declara-la la' obrigaria Queue e
    Journal a puxar Auth, FrameIO e Basic.Methods para alcancar uma
    interface. A regra da casa aponta para ca' de qualquer jeito: assim
    como IAMQPMessageSink vive junto de TAMQPServerMessage, esta vive
    junto de TAMQPServerEvent.

    E' a forma da casa -- interface aqui, como IAMQPMessageSink e
    IAMQPConfirmRegistry --, e nao o TObject com cast que o WIP usava para
    fugir de ciclo de unit. nil = observabilidade desligada, exatamente como
    Confirms = nil significa "sem durabilidade".

    Implementada pelo TAMQPEventBus (AMQP.Server.EventBus), que e' quem sabe
    o contrato de entrega da D31. Quem CHAMA so' precisa saber duas coisas:

    - Wants e' baratissimo (leitura atomica de mascara) e serve para nao
      montar o record quando ninguem quer o tipo. Chamar Emit sem Wants nao
      e' erro, so' e' desperdicio;
    - Emit NUNCA bloqueia, NUNCA levanta e PODE DESCARTAR. Nenhum chamador
      precisa tratar erro, e nenhum chamador pode contar com a entrega. }
  IAMQPEventSink = interface
    ['{7B3C1D48-0A62-4F95-8E17-2C5D9B4A6E03}']
    /// True se algum assinante quer este tipo de evento.
    function Wants(AType: TAMQPServerEventType): Boolean;
    /// Enfileira o evento para a thread notificadora. Ver o contrato acima.
    procedure Emit(const AEvent: TAMQPServerEvent);
  end;

  { Assinante. Método de objeto (o FPC 3.2 não tem closure -- CLAUDE.md).

    CONTRATO DO HANDLER, em três linhas:
    - roda na thread notificadora, NUNCA na de leitura nem num worker do
      pool (D30). Não presuma qual é;
    - pode demorar: enquanto ele roda, o ring enche e passa a DESCARTAR o
      mais novo (D31). Handler lento perde evento, nunca segura o broker;
    - se levantar, a exceção é contada e o laço segue (D33). Nada de
      derrubar a notificadora, nada de engolir em silêncio. }
  TAMQPServerEventHandler = procedure(const AEvent: TAMQPServerEvent) of object;

const
  /// Todos os tipos -- o default de quem assina sem filtrar.
  AMQP_EVENT_ALL_TYPES = [Low(TAMQPServerEventType)..High(TAMQPServerEventType)];

  /// Tamanho do ring do barramento, em EVENTOS (D31). Acima disto o emissor
  /// descarta o mais novo e conta; ele nunca bloqueia e nunca levanta.
  ///
  /// 4096 é generoso de propósito: o custo é um array de records, e a única
  /// coisa que o teto protege é a memória de um broker cujo handler parou de
  /// drenar. Quem quiser outro valor mexe em TAMQPServer.EventQueueCapacity
  /// ANTES do Start.
  AMQP_EVENT_QUEUE_CAPACITY = 4096;

/// Record zerado, já com o tipo e os dois relógios. TODO emissor começa por
/// aqui -- é o que garante a promessa da D34 de que campo não populado sai
/// zerado, sem depender de o emissor lembrar de zerar.
function AmqpNewEvent(AType: TAMQPServerEventType): TAMQPServerEvent;

/// Conjunto -> máscara de bits. O ordinal do enum é o número do bit, que é
/// por que a ordem do enum é API.
function AmqpEventMask(const ATypes: TAMQPServerEventTypes): Cardinal;

/// Nome curto do tipo, para log e para mensagem de teste. Sem o prefixo
/// 'se' -- 'ConnectionEstablished', não 'seConnectionEstablished'.
function AmqpEventTypeName(AType: TAMQPServerEventType): string;

implementation

const
  { Espelha o enum. Um tipo novo sem entrada aqui é pego pelo AssertNames do
    teste, que compara a contagem com Ord(High(...)) + 1. }
  EVENT_NAMES: array[TAMQPServerEventType] of string = (
    'ConnectionEstablished',
    'ConnectionAuthenticated',
    'ConnectionClosed',
    'ChannelOpened',
    'ChannelClosed',
    'MessagePublished',
    'MessagePublishRejected',
    'ConsumerRegistered',
    'ConsumerCancelled',
    'MessageAcked',
    'MessageNacked',
    'MessageRejected',
    'MessageEnqueued',
    'MessageDelivered',
    'MessageExpired',
    'MessageDeadLettered',
    'MessageDropped',
    'JournalFlushed'
  );

function AmqpNewEvent(AType: TAMQPServerEventType): TAMQPServerEvent;
begin
  // Finalize + FillChar seria mais rápido, e seria ERRADO: o Result pode vir
  // de um slot com strings vivas. Um record local zerado por Default/Init é o
  // jeito portátil de soltar as referências antigas nos dois compiladores.
  Result := Default(TAMQPServerEvent);
  Result.EventType := AType;
  Result.WallMs := AmqpWallMs;
  Result.TickMs := Int64(AmqpTickMs);
end;

function AmqpEventMask(const ATypes: TAMQPServerEventTypes): Cardinal;
var
  LType: TAMQPServerEventType;
begin
  Result := 0;
  for LType := Low(TAMQPServerEventType) to High(TAMQPServerEventType) do
    if LType in ATypes then
      Result := Result or (Cardinal(1) shl Ord(LType));
end;

function AmqpEventTypeName(AType: TAMQPServerEventType): string;
begin
  Result := EVENT_NAMES[AType];
end;

end.
