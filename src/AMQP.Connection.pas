unit AMQP.Connection;

{$I amqp.inc}

{ Conexão AMQP 0-9-1 sobre socket TCP, com thread de leitura dedicada.

  Arquitetura de concorrência (item 3 do roadmap):
  - O handshake (Open) é feito de forma síncrona, lendo o socket inline, ANTES
    de qualquer thread existir.
  - Depois do Open-Ok, uma única thread de leitura (TAMQPReaderThread) passa a
    ser a ÚNICA que lê o socket. Ela só faz: ler frame -> demultiplexar por canal
    -> entregar. Nunca roda código do usuário nem bloqueia.
  - TODAS as escritas no socket passam por FWriteLock (uma TCriticalSection), de
    modo que RPCs, publishes, acks e as respostas da própria thread de leitura
    (Close-Ok) não se embaralham.
  - RPC (declare/bind/get/close/...) é feito por evento: o chamador registra o
    que espera, envia o método e aguarda um TEvent que a thread de leitura sinaliza
    ao entregar a resposta.

  Invariantes (para não introduzir corrida/deadlock):
  - Só a thread de leitura lê o socket depois do handshake.
  - Ordem de locks sempre "de fora pra dentro": FChannelsLock -> FWriteLock e
    FRpcLock -> FWriteLock. FWriteLock nunca envolve FChannelsLock/FRpcLock.
  - O monitor de confirms (FConfirmMon, publisher confirms) é o lock MAIS interno:
    é adquirido sozinho (WaitForConfirm) ou dentro do FWriteLock (Publish) ou
    dentro do FChannelsLock (ao resolver acks na thread de leitura); nunca se
    adquire outro lock segurando-o.
  - A thread de leitura escreve o slot de RPC e chama SetEvent; o chamador só lê
    o slot depois de WaitFor retornar (o evento é a barreira de sincronização).

  Consumo (Basic.Consume + despacho para thread pool) e heartbeat vêm nos
  próximos incrementos. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Transport,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  AMQP.Connection.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  EAMQPConnection = class(Exception);
  EAMQPChannel = class(Exception);

  { Adapta um TAMQPTcpSocket como TStream, para os frames trafegarem por
    TAMQPFrame.ReadFrom/WriteTo. Não é dono do socket. }
  TAMQPSocketStream = class(TStream)
  private
    FSocket: TAMQPTcpSocket;
  public
    constructor Create(ASocket: TAMQPTcpSocket);
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  end;

  TAMQPConnectionParams = record
    Host: string;
    Port: Word;
    VirtualHost: string;
    User: string;
    Password: string;
    // Preferências de tune do cliente (0 = sem limite; ver NegotiateTune).
    ChannelMax: Word;
    FrameMax: Cardinal;
    Heartbeat: Word;
    // Reconexão automática (opt-in).
    AutoReconnect: Boolean;
    ReconnectDelayMs: Cardinal;    // espera entre tentativas (padrão 2000)
    MaxReconnectAttempts: Integer; // 0 = infinitas
    ConnectionName: string;        // opcional; vai em client-properties
    // Reenvio automático (opt-in): em confirm mode, publishes deixados sem
    // confirmação por uma queda são re-publicados na reconexão (novos seq-nos).
    // At-least-once — pode duplicar. Custo: guarda o corpo até confirmar.
    RepublishUnconfirmedOnReconnect: Boolean;
    // TLS (amqps://) via SChannel nativo. Opt-in.
    UseTls: Boolean;               // True => faz handshake TLS antes do AMQP
    TlsVerifyPeer: Boolean;        // True (padrão): valida cadeia + hostname
    TlsServerName: string;         // '' => usa Host (SNI / nome para validação)
    /// Parâmetros padrão: localhost:5672, vhost '/', guest/guest (sem TLS).
    class function Localhost: TAMQPConnectionParams; static;
    /// Como Localhost, mas TLS na porta 5671 com validação DESLIGADA
    /// (TlsVerifyPeer=False) — conveniência para broker de dev com cert self-signed.
    class function LocalhostTls: TAMQPConnectionParams; static;
  end;

  { Resultado de Basic.Get. Se Found=False, a fila estava vazia. Se
    Properties tiver Headers, o chamador é dono dessa tabela (liberar). }
  TAMQPGetResult = record
    Found: Boolean;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    MessageCount: Cardinal;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  TAMQPConnection = class;
  TAMQPChannel = class;

  { Callback de eventos de conexão (desconexão / reconexão). Roda numa thread
    interna; mantenha-o curto e não bloqueante. }
  TAMQPConnectionEvent = procedure(AConnection: TAMQPConnection) of object;

  { Callback de Connection.Blocked (extensão RabbitMQ): o broker entrou em
    resource alarm (memória/disco) e parou de aceitar publishes; AReason traz o
    motivo. Despachado num thread do pool (não bloqueia a thread de leitura). O
    par Unblocked usa TAMQPConnectionEvent. }
  TAMQPConnectionBlockedEvent = procedure(AConnection: TAMQPConnection;
    const AReason: string) of object;

  { Mensagem entregue a um consumer (Basic.Deliver). Se Properties tiver
    Headers, a tabela é liberada automaticamente após o callback retornar. }
  TAMQPDelivery = record
    ConsumerTag: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  { Callback de consumer. Roda numa thread do pool (TTask), então NÃO bloqueia a
    thread de leitura. Use AChannel.Ack(ADelivery.DeliveryTag) para confirmar. }
  TAMQPConsumerCallback = procedure(AChannel: TAMQPChannel;
    const ADelivery: TAMQPDelivery) of object;

  { Mensagem devolvida pelo broker (Basic.Return): um publish `mandatory` que
    não pôde ser roteado a nenhuma fila. Se Properties tiver Headers, a tabela
    é liberada automaticamente após o callback retornar. }
  TAMQPReturnedMessage = record
    ReplyCode: Word;
    ReplyText: string;
    Exchange: string;
    RoutingKey: string;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  { Callback de Basic.Return. Roda numa thread do pool (TTask), assim como o
    callback de consumer — não bloqueia a thread de leitura. }
  TAMQPBasicReturnCallback = procedure(AChannel: TAMQPChannel;
    const AReturned: TAMQPReturnedMessage) of object;

  { Callback de publisher confirm: o broker (a)ck-ou ou (n)ack-ou o publish de
    número ASeqNo (o valor devolvido por Publish). Roda numa thread do pool,
    como os demais callbacks — não bloqueia a thread de leitura. Dispara só em
    confirmações reais do broker; queda de conexão NÃO dispara OnConfirm (o
    publish pendente é reportado como não confirmado via WaitForConfirm). }
  TAMQPConfirmCallback = procedure(AChannel: TAMQPChannel;
    ASeqNo: UInt64; AAck: Boolean) of object;

  { Ação de topologia gravada para replay após reconexão. Guardamos o payload
    já serializado do método (qos/declare/bind/consume) — assim argumentos e
    flags vêm de graça. Consumers guardam também o tag e o callback. }
  TAMQPRecoveryAction = record
    Payload: TBytes;
    AwaitReply: Boolean;    // True: espera o -Ok (CallRpc); False: só envia
    IsConsume: Boolean;
    ConsumerTag: string;
    Callback: TAMQPConsumerCallback;
  end;

  TAMQPRpcKind = (rkNone, rkMethod, rkMessage, rkError);

  // Estado da montagem de conteúdo (método + content header + body frames).
  // Tipo nomeado (não anônimo inline) por compatibilidade com o FPC.
  TAMQPAsmState = (asIdle, asHeader, asBody);

  { Frames já serializados de um publish, guardados para reenvio após reconexão
    (uso interno; ver RepublishUnconfirmedOnReconnect). Method/Header são os
    payloads dos frames; Body é o corpo completo (refatiado no reenvio). }
  TAMQPRawPublish = record
    Method: TBytes;
    Header: TBytes;
    Body: TBytes;
  end;

  { Canal de dados. Criado via TAMQPConnection.CreateChannel (que já o abre).
    O chamador é dono do canal e deve liberá-lo (Free). }
  TAMQPChannel = class
  private
    FConnection: TAMQPConnection;
    FChannelId: Word;
    FIsOpen: Boolean;
    FClosed: Boolean;
    // --- RPC (uma chamada por vez neste canal) ---
    FRpcLock: TCriticalSection;
    FRpcEvent: TEvent;
    FRpcKind: TAMQPRpcKind;
    FRpcMethodPayload: TBytes;
    FRpcMessage: TAMQPGetResult;
    FRpcError: string;
    // --- montagem de conteúdo (escrita só pela thread de leitura) ---
    FAsmState: TAMQPAsmState;
    FAsmMethodId: TAMQPMethodId;
    FAsmGetOk: TAMQPBasicGetOk;
    FAsmDeliver: TAMQPBasicDeliver;
    FAsmReturn: TAMQPBasicReturn;
    FAsmProps: TAMQPBasicProperties;
    FAsmBody: TBytes;
    FAsmRemaining: UInt64;
    // --- consumers ---
    FConsumers: TDictionary<string, TAMQPConsumerCallback>;
    FConsumersLock: TCriticalSection;
    FConsumerCounter: Integer;
    FOnBasicReturn: TAMQPBasicReturnCallback;
    FInFlight: Integer; // callbacks em execução no pool (atômico)
    // Pool privado (1 worker) quando o canal usa thread dedicada; nil => usa
    // o AmqpPool global (comportamento padrão). Ver CreateChannel.
    FDispatchPool: TAMQPThreadPool;
    // --- publisher confirms ---
    // FConfirmMon é lock + variável de condição (TAMQPMonitor) que protege
    // FUnconfirmed/FNacked/FPublishSeqNo/FConfirmBase e acorda WaitForConfirm(s).
    // Ordem de locks: é o lock MAIS interno — só se adquire sozinho ou dentro do
    // FWriteLock (Publish); nunca se adquire outro lock segurando-o.
    FConfirmMode: Boolean;
    FPublishSeqNo: UInt64;   // seq-no do usuário, monotônico (NÃO reseta na reconexão)
    FConfirmBase: UInt64;    // offset da sessão: userSeqNo = FConfirmBase + wireTag do broker
    FConfirmMon: TAMQPMonitor;
    FUnconfirmed: TDictionary<UInt64, Boolean>; // seq-nos aguardando confirmação
    FNacked: TDictionary<UInt64, Boolean>;      // seq-nos nack-ados (ou perdidos na queda)
    FOnConfirm: TAMQPConfirmCallback;
    // Reenvio automático (opt-in via params): buffer do conteúdo dos publishes
    // pendentes, por seq-no, para re-publicar após reconexão. Protegido pelo
    // FConfirmMon, como FUnconfirmed.
    FRepublish: Boolean;
    FResendBuffer: TDictionary<UInt64, TAMQPRawPublish>;
    // --- topologia gravada para reconexão ---
    FRecovery: TList<TAMQPRecoveryAction>;
    FRecoveryLock: TCriticalSection;
    procedure AddRecovery(const APayload: TBytes; AAwaitReply: Boolean;
      AIsConsume: Boolean = False; const AConsumerTag: string = '';
      const ACallback: TAMQPConsumerCallback = nil);
    /// Remove a gravação de recovery de um consumer (ao cancelá-lo).
    procedure RemoveConsumerRecovery(const AConsumerTag: string);
    /// Remove da topologia gravada o bind equivalente a (fila, exchange, rota),
    /// para que um unbind não seja desfeito por uma reconexão. Casa binds sem
    /// Arguments (o caso comum), nas duas variantes de no-wait.
    procedure RemoveBindRecovery(const AQueue, AExchange, ARoutingKey: string);
    /// Idem para bindings exchange->exchange (ver UnbindExchange).
    procedure RemoveExchangeBindRecovery(const ADestination, ASource, ARoutingKey: string);
    /// Remove do recovery toda ação cujo payload coincida com um dos candidatos.
    procedure RemoveRecoveryMatching(const ACandidates: array of TBytes);
    /// Reabre o canal e replaya a topologia gravada (chamado na reconexão).
    procedure Recover;
    procedure Open;
    /// Envia um método e aguarda a resposta (payload do método de resposta).
    function CallRpc(const ARequest: TBytes): TBytes;
    // sinalizadores chamados pela thread de leitura:
    procedure SignalMethod(const APayload: TBytes);
    procedure SignalMessage(const AMessage: TAMQPGetResult);
    procedure SignalError(const AMessage: string);
    procedure CompleteContent;
    /// Enfileira um work item no pool dedicado do canal (se houver) ou no
    /// AmqpPool global. Não chamar de "Dispatch": colidiria com o
    /// TObject.Dispatch usado no mecanismo de message dispatch.
    procedure DispatchToPool(AItem: TAMQPWorkItem);
    /// Despacha uma entrega para o callback do consumer, no thread pool.
    procedure DispatchDelivery(const ADeliver: TAMQPBasicDeliver;
      const AProps: TAMQPBasicProperties; const ABody: TBytes);
    /// Despacha um Basic.Return para OnBasicReturn, no thread pool.
    procedure DispatchReturn(const AReturn: TAMQPBasicReturn;
      const AProps: TAMQPBasicProperties; const ABody: TBytes);
    /// Despacha um confirm (ack/nack do broker) para OnConfirm, no thread pool.
    procedure DispatchConfirm(ASeqNo: UInt64; AAck: Boolean);
    /// Resolve os seq-nos pendentes cobertos por (tag, multiple); devolve a lista
    /// resolvida (para despachar OnConfirm fora do lock). Roda na thread de leitura.
    function ResolveConfirms(ATag: UInt64; AMultiple, AAck: Boolean): TArray<UInt64>;
    /// Marca todos os publishes pendentes como não confirmados (queda de conexão).
    procedure FailAllUnconfirmed;
    /// Envia os frames de um publish sob FWriteLock, atribuindo o seq-no (em
    /// confirm mode) e bufferizando para reenvio (se habilitado). Base de Publish
    /// e do reenvio pós-reconexão. Devolve o seq-no (0 fora do confirm mode).
    function DoSendPublish(const AMethod, AHeader, ABody: TBytes): UInt64;
    /// Re-publica os publishes que ficaram sem confirmação (chamado no Recover,
    /// se RepublishUnconfirmedOnReconnect). Reenvia na ordem original, com seq-nos
    /// novos; os antigos já foram reportados como não confirmados.
    procedure ResendUnconfirmed;
    /// Aguarda os callbacks em voo terminarem (usado ao fechar o canal).
    procedure DrainInFlight;
    /// Trata um frame entregue pela thread de leitura (roda NA thread de leitura).
    procedure HandleFrame(const AFrame: TAMQPFrame);
  public
    constructor Create(AConnection: TAMQPConnection; AChannelId: Word);
    destructor Destroy; override;

    procedure DeclareExchange(const ADeclare: TAMQPExchangeDeclare);
    function DeclareQueue(const ADeclare: TAMQPQueueDeclare): TAMQPQueueDeclareOk;
    procedure BindQueue(const ABind: TAMQPQueueBind);
    /// Desfaz um binding fila->exchange (queue.unbind). Sempre aguarda Unbind-Ok.
    /// Também remove o bind equivalente da topologia gravada para reconexão, para
    /// não ser recriado numa eventual reconexão (ver AddRecovery/Recover). Binds
    /// com Arguments não-triviais não são reconciliados automaticamente.
    procedure UnbindQueue(const AUnbind: TAMQPQueueUnbind);
    /// Liga uma exchange a outra (binding exchange->exchange, extensão RabbitMQ):
    /// Destination passa a receber o que for roteado de Source pela RoutingKey.
    procedure BindExchange(const ABind: TAMQPExchangeBinding);
    /// Desfaz um binding exchange->exchange e remove o bind equivalente da
    /// topologia de recovery (mesma reconciliação de UnbindQueue).
    procedure UnbindExchange(const AUnbind: TAMQPExchangeBinding);

    /// Coloca o canal em modo publisher confirms (confirm.select). A partir daí
    /// cada Publish recebe um seq-no e o broker o confirma via Basic.Ack/Nack —
    /// use OnConfirm (assíncrono) e/ou WaitForConfirm/WaitForConfirms. Idempotente.
    procedure ConfirmSelect;

    /// Publica ABody com as propriedades AProps. Sem confirm mode: fire-and-forget
    /// e retorna 0. Em confirm mode: retorna o seq-no atribuído (1, 2, 3, ...),
    /// que identifica este publish em OnConfirm/WaitForConfirm.
    function Publish(const AExchange, ARoutingKey: string; const ABody: TBytes;
      const AProps: TAMQPBasicProperties; AMandatory: Boolean = False): UInt64;
    /// Conveniência: publica um texto UTF-8 como content-type text/plain.
    /// Retorna o seq-no como Publish (0 fora do confirm mode).
    function PublishText(const AExchange, ARoutingKey, AText: string;
      APersistent: Boolean = True): UInt64;

    /// Bloqueia até o broker confirmar o publish ASeqNo. Result=True se ack-ado;
    /// False se nack-ado, se a conexão caiu antes da confirmação, ou timeout.
    /// Requer confirm mode. ASeqNo deve ser um valor devolvido por Publish.
    function WaitForConfirm(ASeqNo: UInt64; ATimeoutMs: Cardinal = 5000): Boolean;
    /// Bloqueia até todos os publishes pendentes serem confirmados. Result=True
    /// se todos foram ack-ados dentro do timeout; False se algum foi nack-ado (ou
    /// perdido numa queda) ou se estourou o timeout. Requer confirm mode.
    /// Consome o estado de nacks do lote — não misture com WaitForConfirm(seqno)
    /// para os mesmos publishes (use um ou outro por lote).
    function WaitForConfirms(ATimeoutMs: Cardinal = 5000): Boolean;

    /// Busca uma mensagem da fila. Result.Found=False se estava vazia.
    function BasicGet(const AQueue: string; ANoAck: Boolean = True): TAMQPGetResult;
    procedure Ack(ADeliveryTag: UInt64; AMultiple: Boolean = False);
    procedure Nack(ADeliveryTag: UInt64; ARequeue: Boolean = True;
      AMultiple: Boolean = False);

    /// Limita quantas mensagens não confirmadas o servidor entrega por vez
    /// (prefetch). Útil para dividir a carga entre os callbacks concorrentes.
    procedure Qos(APrefetchCount: Word; AGlobal: Boolean = False);
    /// Inicia o consumo da fila. Cada mensagem chega em ACallback, despachado
    /// num thread do pool. Devolve o consumer-tag (use em Cancel). Com
    /// ANoAck=False (padrão), confirme com AChannel.Ack no callback.
    function Consume(const AQueue: string; const ACallback: TAMQPConsumerCallback;
      ANoAck: Boolean = False; AExclusive: Boolean = False): string;
    /// Cancela um consumer pelo tag.
    procedure Cancel(const AConsumerTag: string);

    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'OK');

    property ChannelId: Word read FChannelId;
    property IsOpen: Boolean read FIsOpen;
    /// Disparado quando o broker devolve um publish `mandatory` não roteável
    /// (Basic.Return). Roda numa thread do pool (não bloqueia a leitura).
    property OnBasicReturn: TAMQPBasicReturnCallback read FOnBasicReturn write FOnBasicReturn;
    /// Disparado quando o broker confirma (ack/nack) um publish em confirm mode.
    /// Roda numa thread do pool. Configure antes de publicar. Opcional — dá para
    /// usar só WaitForConfirm/WaitForConfirms.
    property OnConfirm: TAMQPConfirmCallback read FOnConfirm write FOnConfirm;
  end;

  TAMQPReaderThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  { Thread de heartbeat: acorda periodicamente (espera interrompível via TEvent,
    NÃO TTimer) para enviar heartbeat quando o envio está ocioso e para detectar
    conexão morta (nenhum frame recebido em 2x o intervalo negociado). }
  TAMQPHeartbeatThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  TAMQPConnection = class
  private
    FParams: TAMQPConnectionParams;
    FSocket: TAMQPTcpSocket;
    FStream: TStream; // TAMQPSocketStream (plain) ou TAMQPSchannelStream/TAMQPOpenSslStream (TLS)
    FIsOpen: Boolean;
    FNegotiated: TAMQPConnectionTune;
    FNextChannel: Word;
    FWriteLock: TCriticalSection;
    FChannelsLock: TCriticalSection;
    FChannels: TDictionary<Word, TAMQPChannel>;
    FReadThread: TAMQPReaderThread;
    FCloseOkEvent: TEvent;
    // --- heartbeat ---
    FHeartbeatThread: TAMQPHeartbeatThread;
    FHbStopEvent: TEvent;
    FLastWriteTick: UInt64; // atualizado a cada frame enviado
    FLastReadTick: UInt64;  // atualizado a cada frame recebido
    // --- reconexão ---
    FDeliberateClose: Boolean; // True quando o usuário fechou (não reconectar)
    FReconnecting: Boolean;
    FOnDisconnect: TAMQPConnectionEvent;
    FOnReconnect: TAMQPConnectionEvent;
    FOnReconnectFailed: TAMQPConnectionEvent;
    // --- Connection.Blocked/Unblocked (resource alarm do broker) ---
    FOnBlocked: TAMQPConnectionBlockedEvent;
    FOnUnblocked: TAMQPConnectionEvent;
    FInFlightConn: Integer; // callbacks de conexão em execução no pool (atômico)
    // --- envio (serializado por FWriteLock) ---
    procedure SendFrameNoLock(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendFrame(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendMethod(AChannel: Word; const APayload: TBytes);
    // --- leitura síncrona, só durante o handshake ---
    function NextFrame: TAMQPFrame;
    function NextMethodOn(AExpectChannel: Word; out AId: TAMQPMethodId): TAMQPReader;
    function ExpectMethod(AExpectChannel, AClassId, AMethodId: Word): TAMQPReader;
    function BuildClientProperties: TAMQPFieldTable;
    procedure Handshake;
    procedure EstablishConnection;
    procedure CloseSocketStream;
    // --- threads ---
    procedure StartReadThread;
    procedure StopReadThread;
    procedure StartHeartbeatThread;
    procedure StopHeartbeatThread;
    procedure HeartbeatTick;
    procedure DispatchFrame(const AFrame: TAMQPFrame);
    procedure HandleConnectionFrame(const AFrame: TAMQPFrame);
    /// Despacha Connection.Blocked/Unblocked para o pool (não bloqueia a leitura).
    procedure DispatchBlocked(const AReason: string);
    procedure DispatchUnblocked;
    /// Aguarda callbacks de conexão em voo terminarem (usado ao destruir).
    procedure DrainConnCallbacks;
    procedure ReadThreadFinished(const AError: string);
    procedure UnregisterChannel(AChannelId: Word);
    // --- reconexão ---
    procedure RunReconnect;
    procedure RecoverAllChannels;
    procedure DrainAllChannels;
    procedure WaitReconnectStopped;
  public
    constructor Create(const AParams: TAMQPConnectionParams);
    destructor Destroy; override;

    /// Conecta o socket e executa o handshake. Levanta EAMQPConnection em falha.
    procedure Open;
    /// Abre um novo canal (já aberto). O chamador é dono e deve liberá-lo.
    /// ADedicatedConsumerThread: se True, as entregas/returns/confirms deste
    /// canal são despachados para uma única thread própria do canal (ordem
    /// garantida, nunca concorrente) em vez do pool global compartilhado.
    function CreateChannel(ADedicatedConsumerThread: Boolean = False): TAMQPChannel;
    /// Envia Connection.Close, aguarda Close-Ok e fecha o socket.
    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'Goodbye');
    /// Fecha o socket abruptamente para simular queda de rede (uso em testes).
    procedure DropConnectionForTest;

    property IsOpen: Boolean read FIsOpen;
    property NegotiatedTune: TAMQPConnectionTune read FNegotiated;
    /// Disparado (em thread interna) quando a conexão cai e a reconexão inicia.
    property OnDisconnect: TAMQPConnectionEvent read FOnDisconnect write FOnDisconnect;
    /// Disparado após reconectar e restaurar a topologia com sucesso.
    property OnReconnect: TAMQPConnectionEvent read FOnReconnect write FOnReconnect;
    /// Disparado quando a reconexão esgota as tentativas.
    property OnReconnectFailed: TAMQPConnectionEvent read FOnReconnectFailed write FOnReconnectFailed;
    /// Disparado quando o broker sinaliza Connection.Blocked (resource alarm:
    /// memória/disco cheios) e para de aceitar publishes. Roda num thread do pool.
    /// Típico: setar uma flag atômica para pausar o publish até o OnUnblocked.
    property OnBlocked: TAMQPConnectionBlockedEvent read FOnBlocked write FOnBlocked;
    /// Disparado quando o broker sai do resource alarm (Connection.Unblocked) e
    /// volta a aceitar publishes. Roda num thread do pool.
    property OnUnblocked: TAMQPConnectionEvent read FOnUnblocked write FOnUnblocked;
  end;

implementation

uses
  AMQP.Channel.Methods
  // TLS: OpenSSL (opt-in via AMQP_OPENSSL, qualquer plataforma) tem precedência
  // sobre SChannel (automático no Windows). Ver EstablishConnection.
  {$IFDEF AMQP_OPENSSL}
  , AMQP.Transport.OpenSSL
  {$ELSE}
    {$IFDEF AMQP_WINDOWS}
  , AMQP.Transport.Tls
    {$ENDIF}
  {$ENDIF}
  ;

const
  AMQP_RPC_TIMEOUT_MS = 30000; // tempo máximo aguardando resposta de RPC

type
  { Thread dedicada da reconexão (substitui TThread.CreateAnonymousThread,
    que não existe com method pointers / no FPC). Auto-libera ao terminar. }
  TAMQPReconnectThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  { Itens de trabalho despachados para o pool (AMQP.Threading). Substituem os
    closures de TTask.Run: cada item carrega os dados capturados em campos e o
    contador de "em voo" é decrementado no finally do Execute — o mesmo
    contrato dos closures originais. O pool libera o item após Execute. }
  TAMQPDeliveryWork = class(TAMQPWorkItem)
  private
    FChannel: TAMQPChannel;
    FCallback: TAMQPConsumerCallback;
    FDelivery: TAMQPDelivery;
  public
    constructor Create(AChannel: TAMQPChannel;
      const ACallback: TAMQPConsumerCallback; const ADelivery: TAMQPDelivery);
    procedure Execute; override;
  end;

  TAMQPReturnWork = class(TAMQPWorkItem)
  private
    FChannel: TAMQPChannel;
    FCallback: TAMQPBasicReturnCallback;
    FReturned: TAMQPReturnedMessage;
  public
    constructor Create(AChannel: TAMQPChannel;
      const ACallback: TAMQPBasicReturnCallback;
      const AReturned: TAMQPReturnedMessage);
    procedure Execute; override;
  end;

  TAMQPConfirmWork = class(TAMQPWorkItem)
  private
    FChannel: TAMQPChannel;
    FCallback: TAMQPConfirmCallback;
    FSeqNo: UInt64;
    FAck: Boolean;
  public
    constructor Create(AChannel: TAMQPChannel;
      const ACallback: TAMQPConfirmCallback; ASeqNo: UInt64; AAck: Boolean);
    procedure Execute; override;
  end;

  TAMQPBlockedWork = class(TAMQPWorkItem)
  private
    FConnection: TAMQPConnection;
    FCallback: TAMQPConnectionBlockedEvent;
    FReason: string;
  public
    constructor Create(AConnection: TAMQPConnection;
      const ACallback: TAMQPConnectionBlockedEvent; const AReason: string);
    procedure Execute; override;
  end;

  TAMQPUnblockedWork = class(TAMQPWorkItem)
  private
    FConnection: TAMQPConnection;
    FCallback: TAMQPConnectionEvent;
  public
    constructor Create(AConnection: TAMQPConnection;
      const ACallback: TAMQPConnectionEvent);
    procedure Execute; override;
  end;

{ TAMQPReconnectThread }

constructor TAMQPReconnectThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := True;
  inherited Create(False);
end;

procedure TAMQPReconnectThread.Execute;
begin
  FConnection.RunReconnect;
end;

{ TAMQPDeliveryWork }

constructor TAMQPDeliveryWork.Create(AChannel: TAMQPChannel;
  const ACallback: TAMQPConsumerCallback; const ADelivery: TAMQPDelivery);
begin
  inherited Create;
  FChannel := AChannel;
  FCallback := ACallback;
  FDelivery := ADelivery;
end;

procedure TAMQPDeliveryWork.Execute;
begin
  try
    FCallback(FChannel, FDelivery);
  finally
    if FDelivery.Properties.Has(bpHeaders) and Assigned(FDelivery.Properties.Headers) then
      FDelivery.Properties.Headers.Free;
    AmqpAtomicDec(FChannel.FInFlight);
  end;
end;

{ TAMQPReturnWork }

constructor TAMQPReturnWork.Create(AChannel: TAMQPChannel;
  const ACallback: TAMQPBasicReturnCallback;
  const AReturned: TAMQPReturnedMessage);
begin
  inherited Create;
  FChannel := AChannel;
  FCallback := ACallback;
  FReturned := AReturned;
end;

procedure TAMQPReturnWork.Execute;
begin
  try
    FCallback(FChannel, FReturned);
  finally
    if FReturned.Properties.Has(bpHeaders) and Assigned(FReturned.Properties.Headers) then
      FReturned.Properties.Headers.Free;
    AmqpAtomicDec(FChannel.FInFlight);
  end;
end;

{ TAMQPConfirmWork }

constructor TAMQPConfirmWork.Create(AChannel: TAMQPChannel;
  const ACallback: TAMQPConfirmCallback; ASeqNo: UInt64; AAck: Boolean);
begin
  inherited Create;
  FChannel := AChannel;
  FCallback := ACallback;
  FSeqNo := ASeqNo;
  FAck := AAck;
end;

procedure TAMQPConfirmWork.Execute;
begin
  try
    FCallback(FChannel, FSeqNo, FAck);
  finally
    AmqpAtomicDec(FChannel.FInFlight);
  end;
end;

{ TAMQPBlockedWork }

constructor TAMQPBlockedWork.Create(AConnection: TAMQPConnection;
  const ACallback: TAMQPConnectionBlockedEvent; const AReason: string);
begin
  inherited Create;
  FConnection := AConnection;
  FCallback := ACallback;
  FReason := AReason;
end;

procedure TAMQPBlockedWork.Execute;
begin
  try
    FCallback(FConnection, FReason);
  finally
    AmqpAtomicDec(FConnection.FInFlightConn);
  end;
end;

{ TAMQPUnblockedWork }

constructor TAMQPUnblockedWork.Create(AConnection: TAMQPConnection;
  const ACallback: TAMQPConnectionEvent);
begin
  inherited Create;
  FConnection := AConnection;
  FCallback := ACallback;
end;

procedure TAMQPUnblockedWork.Execute;
begin
  try
    FCallback(FConnection);
  finally
    AmqpAtomicDec(FConnection.FInFlightConn);
  end;
end;

{ TAMQPGetResult }

function TAMQPGetResult.BodyAsText: string;
begin
  Result := AmqpUtf8Decode(Body);
end;

{ TAMQPDelivery }

function TAMQPDelivery.BodyAsText: string;
begin
  Result := AmqpUtf8Decode(Body);
end;

{ TAMQPReturnedMessage }

function TAMQPReturnedMessage.BodyAsText: string;
begin
  Result := AmqpUtf8Decode(Body);
end;

{ TAMQPSocketStream }

constructor TAMQPSocketStream.Create(ASocket: TAMQPTcpSocket);
begin
  inherited Create;
  FSocket := ASocket;
end;

function TAMQPSocketStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Receive(Buffer, Count);
end;

function TAMQPSocketStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Send(Buffer, Count);
end;

function TAMQPSocketStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  raise EAMQPConnection.Create('TAMQPSocketStream não suporta Seek');
end;

{ TAMQPConnectionParams }

class function TAMQPConnectionParams.Localhost: TAMQPConnectionParams;
begin
  Result.Host := 'localhost';
  Result.Port := 5672;
  Result.VirtualHost := '/';
  Result.User := 'guest';
  Result.Password := 'guest';
  Result.ChannelMax := 2047;
  Result.FrameMax := 131072;
  Result.Heartbeat := 60;
  Result.AutoReconnect := False;
  Result.ReconnectDelayMs := 2000;
  Result.MaxReconnectAttempts := 0; // infinitas
  Result.ConnectionName := '';
  Result.RepublishUnconfirmedOnReconnect := False;
  Result.UseTls := False;
  Result.TlsVerifyPeer := True;
  Result.TlsServerName := '';
end;

class function TAMQPConnectionParams.LocalhostTls: TAMQPConnectionParams;
begin
  Result := Localhost;
  Result.Port := 5671;
  Result.UseTls := True;
  Result.TlsVerifyPeer := False; // dev: aceita cert self-signed
end;

{ TAMQPReaderThread }

constructor TAMQPReaderThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := False;
  inherited Create(False);
end;

procedure TAMQPReaderThread.Execute;
var
  LFrame: TAMQPFrame;
begin
  try
    while not Terminated do
    begin
      LFrame := TAMQPFrame.ReadFrom(FConnection.FStream);
      // atômico p/ a thread de heartbeat
      AmqpAtomicWrite64(FConnection.FLastReadTick, AmqpTickMs);
      FConnection.DispatchFrame(LFrame);
    end;
    FConnection.ReadThreadFinished('');
  except
    on E: Exception do
      // fim de stream (socket fechado) ou erro de protocolo: encerra a thread.
      FConnection.ReadThreadFinished(E.Message);
  end;
end;

{ TAMQPHeartbeatThread }

constructor TAMQPHeartbeatThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := False;
  inherited Create(False);
end;

procedure TAMQPHeartbeatThread.Execute;
var
  LWaitMs: Cardinal;
begin
  // Acorda a cada metade do intervalo (mínimo 1s); a espera é interrompível
  // via FHbStopEvent (não usamos TTimer).
  LWaitMs := (Cardinal(FConnection.FNegotiated.Heartbeat) * 1000) div 2;
  if LWaitMs < 1000 then
    LWaitMs := 1000;
  while not Terminated do
  begin
    if FConnection.FHbStopEvent.WaitFor(LWaitMs) = wrSignaled then
      Break; // parada solicitada
    if Terminated then
      Break;
    FConnection.HeartbeatTick;
  end;
end;

{ TAMQPConnection }

constructor TAMQPConnection.Create(const AParams: TAMQPConnectionParams);
begin
  inherited Create;
  FParams := AParams;
  FWriteLock := TCriticalSection.Create;
  FChannelsLock := TCriticalSection.Create;
  FChannels := TDictionary<Word, TAMQPChannel>.Create;
  FCloseOkEvent := TEvent.Create(nil, True, False, '');
  FHbStopEvent := TEvent.Create(nil, True, False, '');
end;

destructor TAMQPConnection.Destroy;
var
  LChan: TAMQPChannel;
begin
  // Close é idempotente: aborta reconexão, para threads e fecha o socket.
  try
    Close;
  except
  end;
  DrainConnCallbacks; // callbacks Blocked/Unblocked em voo capturaram Self
  // Libera canais que o usuário não liberou (cada Free chama UnregisterChannel,
  // que remove do dicionário; por isso iteramos sobre uma cópia).
  if Assigned(FChannels) then
  begin
    for LChan in FChannels.Values.ToArray do
      LChan.Free;
    FChannels.Free;
  end;
  FChannelsLock.Free;
  FWriteLock.Free;
  FCloseOkEvent.Free;
  FHbStopEvent.Free;
  inherited;
end;

procedure TAMQPConnection.SendFrameNoLock(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
var
  LFrame: TAMQPFrame;
begin
  // Assume FWriteLock já adquirido (envio de grupo de frames, ex.: Publish).
  if not Assigned(FStream) then
    raise EAMQPConnection.Create('conexão indisponível no momento (reconectando?)');
  LFrame := TAMQPFrame.Create(AFrameType, AChannel, APayload);
  LFrame.WriteTo(FStream);
  // Atômico: lido pela thread de heartbeat; no Win32 um store de 64 bits não é
  // atômico e poderia ser "torn" (ver AmqpAtomicRead64 em HeartbeatTick).
  AmqpAtomicWrite64(FLastWriteTick, AmqpTickMs);
end;

procedure TAMQPConnection.SendFrame(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
begin
  FWriteLock.Enter;
  try
    SendFrameNoLock(AFrameType, AChannel, APayload);
  finally
    FWriteLock.Leave;
  end;
end;

procedure TAMQPConnection.SendMethod(AChannel: Word; const APayload: TBytes);
begin
  SendFrame(AMQP_FRAME_METHOD, AChannel, APayload);
end;

{ --- Leitura síncrona (só no handshake) --- }

function TAMQPConnection.NextFrame: TAMQPFrame;
begin
  repeat
    Result := TAMQPFrame.ReadFrom(FStream);
  until not Result.IsHeartbeat;
end;

function TAMQPConnection.NextMethodOn(AExpectChannel: Word;
  out AId: TAMQPMethodId): TAMQPReader;
var
  LFrame: TAMQPFrame;
  LConnClose: TAMQPConnectionClose;
begin
  LFrame := NextFrame;
  if not LFrame.IsMethod then
    raise EAMQPConnection.CreateFmt(
      'frame inesperado (tipo %d, canal %d)', [LFrame.FrameType, LFrame.Channel]);

  Result := TAMQPReader.Create(LFrame.Payload);
  try
    AId := ReadMethodHeader(Result);
    if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
    begin
      LConnClose := DecodeClose(Result);
      raise EAMQPConnection.CreateFmt('conexão recusada pelo servidor: %d %s',
        [LConnClose.ReplyCode, LConnClose.ReplyText]);
    end;
    if LFrame.Channel <> AExpectChannel then
      raise EAMQPConnection.CreateFmt('resposta em canal inesperado: %d (esperava %d)',
        [LFrame.Channel, AExpectChannel]);
  except
    Result.Free;
    raise;
  end;
end;

function TAMQPConnection.ExpectMethod(AExpectChannel, AClassId,
  AMethodId: Word): TAMQPReader;
var
  LId: TAMQPMethodId;
begin
  Result := NextMethodOn(AExpectChannel, LId);
  try
    if not LId.Matches(AClassId, AMethodId) then
      raise EAMQPConnection.CreateFmt(
        'resposta inesperada: método %d/%d (esperava %d/%d)',
        [LId.ClassId, LId.MethodId, AClassId, AMethodId]);
  except
    Result.Free;
    raise;
  end;
end;

procedure TAMQPConnection.Handshake;
var
  LReader: TAMQPReader;
  LStart: TAMQPConnectionStart;
  LServerTune: TAMQPConnectionTune;
  LProps: TAMQPFieldTable;
begin
  WriteProtocolHeader(FStream);

  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START);
  try
    LStart := DecodeStart(LReader);
  finally
    LReader.Free;
  end;

  try
    if not LStart.SupportsMechanism(AMQP_AUTH_PLAIN) then
      raise EAMQPConnection.CreateFmt(
        'servidor não oferece o mecanismo %s (oferece: %s)',
        [AMQP_AUTH_PLAIN, LStart.Mechanisms]);

    LProps := BuildClientProperties;
    try
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildStartOk(LProps, AMQP_AUTH_PLAIN,
        PlainAuthResponse(FParams.User, FParams.Password), AMQP_LOCALE_DEFAULT));
    finally
      LProps.Free;
    end;
  finally
    LStart.ServerProperties.Free;
  end;

  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE);
  try
    LServerTune := DecodeTune(LReader);
  finally
    LReader.Free;
  end;

  FNegotiated := NegotiateTune(LServerTune,
    FParams.ChannelMax, FParams.FrameMax, FParams.Heartbeat);
  SendMethod(AMQP_CHANNEL_CONNECTION, BuildTuneOk(FNegotiated));

  SendMethod(AMQP_CHANNEL_CONNECTION, BuildOpen(FParams.VirtualHost));
  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK);
  try
    DecodeOpenOk(LReader);
  finally
    LReader.Free;
  end;
end;

function TAMQPConnection.BuildClientProperties: TAMQPFieldTable;
begin
  Result := DefaultClientProperties;
  if FParams.ConnectionName <> '' then
    Result.Put('connection_name', FParams.ConnectionName);
end;

procedure TAMQPConnection.EstablishConnection;
var
  LPlain: TAMQPSocketStream;
  LTarget: string;
begin
  // Cria socket, faz o handshake (síncrono, inline) e sobe as threads.
  // Usado tanto no Open inicial quanto na reconexão.
  FSocket := TAMQPTcpSocket.Create;
  FSocket.Connect(FParams.Host, FParams.Port);

  LPlain := TAMQPSocketStream.Create(FSocket);
  FStream := LPlain;
  if FParams.UseTls then
  begin
    {$IF Defined(AMQP_OPENSSL) or Defined(AMQP_WINDOWS)}
    // O stream TLS envolve o plain e faz o handshake TLS no construtor. Ele passa
    // a ser dono do plain: se o Create falhar, seu destrutor (auto-chamado)
    // libera o plain — por isso zeramos FStream antes, para o
    // CloseSocketStream não liberar de novo (double-free). O socket é liberado à
    // parte (o plain não é dono dele).
    LTarget := FParams.TlsServerName;
    if LTarget = '' then
      LTarget := FParams.Host;
    FStream := nil;
    {$IFDEF AMQP_OPENSSL}
    // OpenSSL (opt-in): mesmo contrato do SChannel; vence quando definido,
    // inclusive no Windows.
    FStream := TAMQPOpenSslStream.Create(LPlain, LTarget, FParams.TlsVerifyPeer);
    {$ELSE}
    FStream := TAMQPSchannelStream.Create(LPlain, LTarget, FParams.TlsVerifyPeer);
    {$ENDIF}
    {$ELSE}
    // Sem AMQP_OPENSSL e fora do Windows não há backend TLS neste build.
    LTarget := ''; // evita warning de variável não usada
    CloseSocketStream;
    raise EAMQPConnection.Create(
      'TLS não suportado neste build/plataforma (Windows usa SChannel; nas demais, compile com AMQP_OPENSSL)');
    {$ENDIF}
  end;

  Handshake; // antes de qualquer thread (agora sobre TLS, se habilitado)

  FLastWriteTick := AmqpTickMs;
  FLastReadTick := FLastWriteTick;
  StartReadThread;     // a partir daqui, só a thread lê o socket
  StartHeartbeatThread;
  FIsOpen := True;
end;

procedure TAMQPConnection.CloseSocketStream;
begin
  if Assigned(FSocket) then
    try
      FSocket.Close;
    except
    end;
  FreeAndNil(FStream);
  FreeAndNil(FSocket);
end;

procedure TAMQPConnection.Open;
begin
  if FIsOpen then
    raise EAMQPConnection.Create('conexão já está aberta');
  EstablishConnection;
end;

procedure TAMQPConnection.DropConnectionForTest;
begin
  if Assigned(FSocket) then
    try
      FSocket.Close; // a thread de leitura vai perceber e disparar a reconexão
    except
    end;
end;

{ --- Thread de leitura --- }

procedure TAMQPConnection.StartReadThread;
begin
  FReadThread := TAMQPReaderThread.Create(Self);
end;

procedure TAMQPConnection.StopReadThread;
begin
  if Assigned(FReadThread) then
  begin
    FReadThread.Terminate;
    if Assigned(FSocket) then
      try
        FSocket.Close; // desbloqueia o ReadFrom da thread
      except
      end;
    FReadThread.WaitFor;
    FreeAndNil(FReadThread);
  end;
end;

{ --- Thread de heartbeat --- }

procedure TAMQPConnection.StartHeartbeatThread;
begin
  if FNegotiated.Heartbeat = 0 then
    Exit; // heartbeat desabilitado na negociação
  FHbStopEvent.ResetEvent;
  FHeartbeatThread := TAMQPHeartbeatThread.Create(Self);
end;

procedure TAMQPConnection.StopHeartbeatThread;
begin
  if Assigned(FHeartbeatThread) then
  begin
    FHeartbeatThread.Terminate;
    FHbStopEvent.SetEvent; // interrompe a espera imediatamente
    FHeartbeatThread.WaitFor;
    FreeAndNil(FHeartbeatThread);
  end;
end;

procedure TAMQPConnection.HeartbeatTick;
var
  LIntervalMs: UInt64;
  LNow, LLastRead, LLastWrite: UInt64;
begin
  LIntervalMs := UInt64(FNegotiated.Heartbeat) * 1000;
  if LIntervalMs = 0 then
    Exit;
  LNow := AmqpTickMs;
  // Leituras atômicas (os ticks são escritos por outras threads; no Win32 um
  // load de 64 bits pode ser "torn" e produzir um delta absurdo).
  LLastRead := AmqpAtomicRead64(FLastReadTick);
  LLastWrite := AmqpAtomicRead64(FLastWriteTick);

  // Conexão morta: nenhum frame recebido em 2x o intervalo (o servidor também
  // manda heartbeats). Fecha o socket para desbloquear a thread de leitura.
  if (LNow - LLastRead) > (2 * LIntervalMs) then
  begin
    if Assigned(FSocket) then
      try
        FSocket.Close;
      except
      end;
    Exit;
  end;

  // Envio ocioso há >= metade do intervalo: manda um heartbeat.
  if (LNow - LLastWrite) >= (LIntervalMs div 2) then
    try
      SendFrame(AMQP_FRAME_HEARTBEAT, AMQP_CHANNEL_CONNECTION, nil);
    except
      // erro de escrita: a thread de leitura vai perceber o socket caído
    end;
end;

procedure TAMQPConnection.ReadThreadFinished(const AError: string);
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
  LMsg: string;
begin
  FIsOpen := False;
  // Acorda quem estiver esperando Close-Ok e qualquer RPC pendente nos canais.
  FCloseOkEvent.SetEvent;
  if AError <> '' then
    LMsg := 'conexão encerrada: ' + AError
  else
    LMsg := 'conexão encerrada';
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.SignalError(LMsg);

  // Queda inesperada: dispara a reconexão (numa thread própria, pois esta é a
  // thread de leitura que está terminando).
  if FParams.AutoReconnect and (not FDeliberateClose) and (not FReconnecting) then
  begin
    FReconnecting := True;
    TAMQPReconnectThread.Create(Self); // FreeOnTerminate: se auto-libera
  end;
end;

procedure TAMQPConnection.DrainAllChannels;
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
begin
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.DrainInFlight;
end;

procedure TAMQPConnection.RecoverAllChannels;
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
begin
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.Recover;
end;

procedure TAMQPConnection.WaitReconnectStopped;
var
  LWaited: Integer;
begin
  // Assume FDeliberateClose já True. Espera a thread de reconexão encerrar para
  // evitar corrida no teardown do socket/threads.
  LWaited := 0;
  while FReconnecting and (LWaited < 12000) do
  begin
    Sleep(20);
    Inc(LWaited, 20);
  end;
end;

procedure TAMQPConnection.RunReconnect;
var
  LAttempt: Integer;
  LDelay: Cardinal;
begin
  // Roda numa thread anônima dedicada; FReconnecting já está True.
  try
    StopHeartbeatThread;
    StopReadThread;    // aguarda a thread de leitura antiga terminar
    DrainAllChannels;  // callbacks antigos terminam (acks falham no socket morto)
    CloseSocketStream;

    if Assigned(FOnDisconnect) then
      try FOnDisconnect(Self); except end;

    LDelay := FParams.ReconnectDelayMs;
    if LDelay = 0 then
      LDelay := 2000;

    LAttempt := 0;
    while not FDeliberateClose do
    begin
      Sleep(LDelay);
      if FDeliberateClose then
        Break;
      Inc(LAttempt);
      try
        EstablishConnection; // novo socket + handshake + threads (FIsOpen := True)
        RecoverAllChannels;  // reabre canais e replaya a topologia gravada
        if Assigned(FOnReconnect) then
          try FOnReconnect(Self); except end;
        Exit; // sucesso
      except
        // tentativa falhou: limpa o estado parcial e tenta de novo
        FIsOpen := False;
        StopHeartbeatThread;
        StopReadThread;
        CloseSocketStream;
        if (FParams.MaxReconnectAttempts > 0) and
           (LAttempt >= FParams.MaxReconnectAttempts) then
        begin
          if Assigned(FOnReconnectFailed) then
            try FOnReconnectFailed(Self); except end;
          Break;
        end;
      end;
    end;
  finally
    FReconnecting := False;
  end;
end;

procedure TAMQPConnection.DispatchFrame(const AFrame: TAMQPFrame);
var
  LChan: TAMQPChannel;
begin
  if AFrame.IsHeartbeat then
    Exit; // heartbeat tratado no item 4

  if AFrame.Channel = AMQP_CHANNEL_CONNECTION then
  begin
    HandleConnectionFrame(AFrame);
    Exit;
  end;

  FChannelsLock.Enter;
  try
    if not FChannels.TryGetValue(AFrame.Channel, LChan) then
      LChan := nil;
    if Assigned(LChan) then
      LChan.HandleFrame(AFrame);
  finally
    FChannelsLock.Leave;
  end;
end;

procedure TAMQPConnection.HandleConnectionFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LClose: TAMQPConnectionClose;
begin
  if not AFrame.IsMethod then
    Exit;
  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);
    if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK) then
    begin
      FCloseOkEvent.SetEvent;
    end
    else if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
    begin
      LClose := DecodeClose(LReader);
      FIsOpen := False;
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildCloseOk);
      FCloseOkEvent.SetEvent;
      // (canais serão sinalizados quando a thread encerrar)
    end
    else if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_BLOCKED) then
      DispatchBlocked(DecodeBlocked(LReader))
    else if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_UNBLOCKED) then
      DispatchUnblocked;
  finally
    LReader.Free;
  end;
end;

procedure TAMQPConnection.DispatchBlocked(const AReason: string);
var
  LCallback: TAMQPConnectionBlockedEvent;
begin
  LCallback := FOnBlocked;
  if not Assigned(LCallback) then
    Exit;
  // Pool próprio: a thread de leitura NÃO roda o callback do usuário.
  AmqpAtomicInc(FInFlightConn);
  AmqpPool.Queue(TAMQPBlockedWork.Create(Self, LCallback, AReason));
end;

procedure TAMQPConnection.DispatchUnblocked;
var
  LCallback: TAMQPConnectionEvent;
begin
  LCallback := FOnUnblocked;
  if not Assigned(LCallback) then
    Exit;
  AmqpAtomicInc(FInFlightConn);
  AmqpPool.Queue(TAMQPUnblockedWork.Create(Self, LCallback));
end;

procedure TAMQPConnection.DrainConnCallbacks;
begin
  // Espera os callbacks Blocked/Unblocked em voo (capturaram Self) terminarem
  // antes de o objeto ser liberado — mesmo racional do DrainInFlight do canal.
  while AmqpAtomicGet(FInFlightConn) > 0 do
    Sleep(10);
end;

procedure TAMQPConnection.UnregisterChannel(AChannelId: Word);
begin
  FChannelsLock.Enter;
  try
    FChannels.Remove(AChannelId);
  finally
    FChannelsLock.Leave;
  end;
end;

function TAMQPConnection.CreateChannel(ADedicatedConsumerThread: Boolean): TAMQPChannel;
var
  LChan: TAMQPChannel;
begin
  if not FIsOpen then
    raise EAMQPConnection.Create('conexão não está aberta');

  // Alocação do id + inserção no dicionário sob o mesmo lock (evita duas threads
  // gerarem o mesmo channel-id e o segundo Add levantar/vazar o canal).
  FChannelsLock.Enter;
  try
    if (FNegotiated.ChannelMax > 0) and (FNextChannel >= FNegotiated.ChannelMax) then
      raise EAMQPConnection.CreateFmt(
        'limite de canais atingido (%d); reuso de canais ainda não implementado',
        [FNegotiated.ChannelMax]);
    Inc(FNextChannel);
    LChan := TAMQPChannel.Create(Self, FNextChannel);
    try
      if ADedicatedConsumerThread then
        LChan.FDispatchPool := TAMQPThreadPool.Create(1);
      FChannels.Add(LChan.ChannelId, LChan);
    except
      LChan.Free;
      raise;
    end;
  finally
    FChannelsLock.Leave;
  end;

  try
    LChan.Open; // RPC: usa a thread de leitura já rodando (fora do lock)
  except
    UnregisterChannel(LChan.ChannelId);
    LChan.Free;
    raise;
  end;
  Result := LChan;
end;

procedure TAMQPConnection.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPConnectionClose;
begin
  // Idempotente. Sinaliza fechamento deliberado e aguarda qualquer reconexão em
  // curso encerrar antes de mexer no socket/threads (evita corrida).
  FDeliberateClose := True;
  WaitReconnectStopped;

  if FIsOpen then
  begin
    FIsOpen := False;
    StopHeartbeatThread; // não enviar heartbeats durante o fechamento
    LClose.ReplyCode := AReplyCode;
    LClose.ReplyText := AReplyText;
    LClose.ClassId := 0;
    LClose.MethodId := 0;
    try
      FCloseOkEvent.ResetEvent;
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildClose(LClose));
      FCloseOkEvent.WaitFor(3000);
    except
    end;
  end;

  StopHeartbeatThread;
  StopReadThread; // termina a thread e fecha o socket
  CloseSocketStream;
end;

{ TAMQPChannel }

constructor TAMQPChannel.Create(AConnection: TAMQPConnection; AChannelId: Word);
begin
  inherited Create;
  FConnection := AConnection;
  FChannelId := AChannelId;
  FRpcLock := TCriticalSection.Create;
  FRpcEvent := TEvent.Create(nil, True, False, '');
  FConsumers := TDictionary<string, TAMQPConsumerCallback>.Create;
  FConsumersLock := TCriticalSection.Create;
  FConfirmMon := TAMQPMonitor.Create;
  FUnconfirmed := TDictionary<UInt64, Boolean>.Create;
  FNacked := TDictionary<UInt64, Boolean>.Create;
  FResendBuffer := TDictionary<UInt64, TAMQPRawPublish>.Create;
  FRepublish := AConnection.FParams.RepublishUnconfirmedOnReconnect;
  FRecovery := TList<TAMQPRecoveryAction>.Create;
  FRecoveryLock := TCriticalSection.Create;
  FAsmState := asIdle;
end;

destructor TAMQPChannel.Destroy;
begin
  if FIsOpen and (not FClosed) and FConnection.IsOpen then
    try
      Close;
    except
    end;
  DrainInFlight; // garante que nenhum callback ainda usa este canal
  FDispatchPool.Free; // nil-safe; se atribuído, já não há mais itens em voo
  FRecovery.Free;
  FRecoveryLock.Free;
  FUnconfirmed.Free;
  FNacked.Free;
  FResendBuffer.Free;
  FConfirmMon.Free;
  FConsumers.Free;
  FConsumersLock.Free;
  FRpcEvent.Free;
  FRpcLock.Free;
  inherited;
end;

procedure TAMQPChannel.AddRecovery(const APayload: TBytes; AAwaitReply: Boolean;
  AIsConsume: Boolean; const AConsumerTag: string;
  const ACallback: TAMQPConsumerCallback);
var
  LAction: TAMQPRecoveryAction;
begin
  LAction.Payload := APayload;
  LAction.AwaitReply := AAwaitReply;
  LAction.IsConsume := AIsConsume;
  LAction.ConsumerTag := AConsumerTag;
  LAction.Callback := ACallback;
  FRecoveryLock.Enter;
  try
    FRecovery.Add(LAction);
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.RemoveConsumerRecovery(const AConsumerTag: string);
var
  I: Integer;
begin
  FRecoveryLock.Enter;
  try
    for I := FRecovery.Count - 1 downto 0 do
      if FRecovery[I].IsConsume and (FRecovery[I].ConsumerTag = AConsumerTag) then
        FRecovery.Delete(I);
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.RemoveRecoveryMatching(const ACandidates: array of TBytes);

  function SameBytes(const A, B: TBytes): Boolean;
  begin
    Result := (Length(A) = Length(B)) and
      ((Length(A) = 0) or CompareMem(@A[0], @B[0], Length(A)));
  end;

var
  I, J: Integer;
begin
  FRecoveryLock.Enter;
  try
    for I := FRecovery.Count - 1 downto 0 do
      for J := 0 to High(ACandidates) do
        if SameBytes(FRecovery[I].Payload, ACandidates[J]) then
        begin
          FRecovery.Delete(I);
          Break;
        end;
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.RemoveBindRecovery(const AQueue, AExchange,
  ARoutingKey: string);
var
  LBind: TAMQPQueueBind;
  LCandFalse, LCandTrue: TBytes;
begin
  // O recovery guarda o payload serializado do bind; reconstruímos os candidatos
  // (nas duas variantes de no-wait, sem Arguments) e removemos os que casarem.
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := AQueue;
  LBind.ExchangeName := AExchange;
  LBind.RoutingKey := ARoutingKey;
  LBind.NoWait := False;
  LCandFalse := BuildQueueBind(LBind);
  LBind.NoWait := True;
  LCandTrue := BuildQueueBind(LBind);
  RemoveRecoveryMatching([LCandFalse, LCandTrue]);
end;

procedure TAMQPChannel.RemoveExchangeBindRecovery(const ADestination, ASource,
  ARoutingKey: string);
var
  LBind: TAMQPExchangeBinding;
  LCandFalse, LCandTrue: TBytes;
begin
  LBind := Default(TAMQPExchangeBinding);
  LBind.Destination := ADestination;
  LBind.Source := ASource;
  LBind.RoutingKey := ARoutingKey;
  LBind.NoWait := False;
  LCandFalse := BuildExchangeBind(LBind);
  LBind.NoWait := True;
  LCandTrue := BuildExchangeBind(LBind);
  RemoveRecoveryMatching([LCandFalse, LCandTrue]);
end;

procedure TAMQPChannel.Recover;
var
  LActions: TArray<TAMQPRecoveryAction>;
  LAction: TAMQPRecoveryAction;
begin
  // Roda na thread de reconexão. Segura FRpcLock durante todo o replay para que
  // RPCs do usuário no canal esperem a recuperação terminar. FRpcLock é
  // recursivo (critical section), então CallRpc interno funciona.
  FRpcLock.Enter;
  try
    FClosed := False;
    FAsmState := asIdle;
    // Sessão nova: o broker reinicia a numeração do WIRE em 1. Mantemos o seq-no
    // do USUÁRIO (FPublishSeqNo) monotônico e guardamos o offset da sessão para
    // converter os tags do broker (ver ResolveConfirms) — os novos publishes
    // recebem números ACIMA dos antigos, sem colisão. FNacked NÃO é limpo: os
    // publishes perdidos na queda seguem reportáveis como não confirmados
    // (FailAllUnconfirmed já os moveu de FUnconfirmed para FNacked).
    FConfirmMon.Enter;
    try
      FConfirmBase := FPublishSeqNo;
      FUnconfirmed.Clear;
      FConfirmMon.PulseAll;
    finally
      FConfirmMon.Leave;
    end;
    Open; // Channel.Open no novo socket (define FIsOpen := True)

    FConsumersLock.Enter;
    try
      FConsumers.Clear; // registros da sessão antiga; serão re-adicionados
    finally
      FConsumersLock.Leave;
    end;

    FRecoveryLock.Enter;
    try
      LActions := FRecovery.ToArray;
    finally
      FRecoveryLock.Leave;
    end;

    for LAction in LActions do
    begin
      if LAction.IsConsume then
      begin
        FConsumersLock.Enter;
        try
          FConsumers.AddOrSetValue(LAction.ConsumerTag, LAction.Callback);
        finally
          FConsumersLock.Leave;
        end;
      end;
      if LAction.AwaitReply then
        CallRpc(LAction.Payload)
      else
        FConnection.SendMethod(FChannelId, LAction.Payload);
    end;

    // Topologia restaurada e confirm mode re-armado (via replay do confirm.select):
    // re-publica o que ficou sem confirmação na queda (se habilitado).
    ResendUnconfirmed;
  finally
    FRpcLock.Leave;
  end;
end;

function TAMQPChannel.CallRpc(const ARequest: TBytes): TBytes;
begin
  FRpcLock.Enter;
  try
    if FClosed then
      raise EAMQPChannel.Create('canal fechado');
    FRpcKind := rkNone;
    FRpcError := '';
    FRpcEvent.ResetEvent;
    FConnection.SendMethod(FChannelId, ARequest);
    if FRpcEvent.WaitFor(AMQP_RPC_TIMEOUT_MS) <> wrSignaled then
      raise EAMQPChannel.Create('timeout aguardando resposta do servidor');
    case FRpcKind of
      rkMethod:
        Result := FRpcMethodPayload;
      rkError:
        raise EAMQPChannel.Create(FRpcError);
    else
      raise EAMQPChannel.Create('resposta de RPC inesperada');
    end;
  finally
    FRpcLock.Leave;
  end;
end;

procedure TAMQPChannel.SignalMethod(const APayload: TBytes);
begin
  FRpcMethodPayload := APayload;
  FRpcKind := rkMethod;
  FRpcEvent.SetEvent;
end;

procedure TAMQPChannel.SignalMessage(const AMessage: TAMQPGetResult);
begin
  FRpcMessage := AMessage;
  FRpcKind := rkMessage;
  FRpcEvent.SetEvent;
end;

procedure TAMQPChannel.SignalError(const AMessage: string);
begin
  FClosed := True;
  FIsOpen := False;
  FRpcError := AMessage;
  FRpcKind := rkError;
  FRpcEvent.SetEvent;
  FailAllUnconfirmed; // publishes pendentes viram "não confirmados"
end;

procedure TAMQPChannel.Open;
var
  LPayload: TBytes;
  LReader: TAMQPReader;
begin
  LPayload := CallRpc(BuildChannelOpen);
  LReader := TAMQPReader.Create(LPayload);
  try
    ReadMethodHeader(LReader);
    DecodeChannelOpenOk(LReader);
  finally
    LReader.Free;
  end;
  FIsOpen := True;
end;

procedure TAMQPChannel.DeclareExchange(const ADeclare: TAMQPExchangeDeclare);
var
  LPayload: TBytes;
begin
  LPayload := BuildExchangeDeclare(ADeclare);
  if ADeclare.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Declare-Ok (sem args, descartado)
  AddRecovery(LPayload, not ADeclare.NoWait);
end;

function TAMQPChannel.DeclareQueue(const ADeclare: TAMQPQueueDeclare): TAMQPQueueDeclareOk;
var
  LPayload: TBytes;
  LReader: TAMQPReader;
begin
  LPayload := BuildQueueDeclare(ADeclare);
  if ADeclare.NoWait then
  begin
    FConnection.SendMethod(FChannelId, LPayload);
    Result := Default(TAMQPQueueDeclareOk);
    Result.QueueName := ADeclare.QueueName;
  end
  else
  begin
    LReader := TAMQPReader.Create(CallRpc(LPayload));
    try
      ReadMethodHeader(LReader);
      Result := DecodeQueueDeclareOk(LReader);
    finally
      LReader.Free;
    end;
  end;
  AddRecovery(LPayload, not ADeclare.NoWait);
end;

procedure TAMQPChannel.BindQueue(const ABind: TAMQPQueueBind);
var
  LPayload: TBytes;
begin
  LPayload := BuildQueueBind(ABind);
  if ABind.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Bind-Ok (sem args, descartado)
  AddRecovery(LPayload, not ABind.NoWait);
end;

procedure TAMQPChannel.UnbindQueue(const AUnbind: TAMQPQueueUnbind);
begin
  CallRpc(BuildQueueUnbind(AUnbind)); // Unbind-Ok (sem args, descartado)
  RemoveBindRecovery(AUnbind.QueueName, AUnbind.ExchangeName, AUnbind.RoutingKey);
end;

procedure TAMQPChannel.BindExchange(const ABind: TAMQPExchangeBinding);
var
  LPayload: TBytes;
begin
  LPayload := BuildExchangeBind(ABind);
  if ABind.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Bind-Ok (sem args, descartado)
  AddRecovery(LPayload, not ABind.NoWait);
end;

procedure TAMQPChannel.UnbindExchange(const AUnbind: TAMQPExchangeBinding);
var
  LPayload: TBytes;
begin
  LPayload := BuildExchangeUnbind(AUnbind);
  if AUnbind.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Unbind-Ok (sem args, descartado)
  RemoveExchangeBindRecovery(AUnbind.Destination, AUnbind.Source, AUnbind.RoutingKey);
end;

function TAMQPChannel.DoSendPublish(const AMethod, AHeader, ABody: TBytes): UInt64;
var
  LMaxBody, LOffset, LLen: Integer;
  LRaw: TAMQPRawPublish;
begin
  if FConnection.FNegotiated.FrameMax = 0 then
    LMaxBody := 131072 - 8
  else
    LMaxBody := Integer(FConnection.FNegotiated.FrameMax) - 8;
  if LMaxBody < 1 then
    LMaxBody := 1;

  Result := 0;
  // Os frames de uma mensagem (método + header + body) devem sair juntos, sem
  // que frames de outra thread se intercalem — por isso, um único lock.
  FConnection.FWriteLock.Enter;
  try
    if FConfirmMode then
    begin
      // O seq-no acompanha a ordem de envio no wire: atribuído sob o FWriteLock,
      // que serializa os publishes. Registramos a pendência ANTES de enviar
      // (via FConfirmMon, o lock mais interno) para nunca perder um ack que
      // chegue rápido demais — o padrão de registrar o consumer antes do Consume.
      FConfirmMon.Enter;
      try
        Inc(FPublishSeqNo);
        Result := FPublishSeqNo;
        FUnconfirmed.AddOrSetValue(Result, True);
        if FRepublish then
        begin
          // Guarda o conteúdo para reenvio se a conexão cair antes do ack. Copy
          // do corpo: o chamador pode reutilizar/liberar o array após retornar.
          LRaw.Method := AMethod;
          LRaw.Header := AHeader;
          LRaw.Body := Copy(ABody, 0, Length(ABody));
          FResendBuffer.AddOrSetValue(Result, LRaw);
        end;
      finally
        FConfirmMon.Leave;
      end;
    end;

    FConnection.SendFrameNoLock(AMQP_FRAME_METHOD, FChannelId, AMethod);
    FConnection.SendFrameNoLock(AMQP_FRAME_HEADER, FChannelId, AHeader);

    LOffset := 0;
    while LOffset < Length(ABody) do
    begin
      if (Length(ABody) - LOffset) < LMaxBody then
        LLen := Length(ABody) - LOffset
      else
        LLen := LMaxBody;
      FConnection.SendFrameNoLock(AMQP_FRAME_BODY, FChannelId,
        Copy(ABody, LOffset, LLen));
      Inc(LOffset, LLen);
    end;
  finally
    FConnection.FWriteLock.Leave;
  end;
end;

function TAMQPChannel.Publish(const AExchange, ARoutingKey: string;
  const ABody: TBytes; const AProps: TAMQPBasicProperties; AMandatory: Boolean): UInt64;
begin
  Result := DoSendPublish(
    BuildBasicPublish(AExchange, ARoutingKey, AMandatory, False),
    BuildContentHeader(UInt64(Length(ABody)), AProps),
    ABody);
end;

procedure TAMQPChannel.ResendUnconfirmed;

  // Insertion sort local: TArray.Sort<T> não existe no Generics.Collections do
  // FPC e o buffer de reenvio é pequeno (publishes pendentes de uma queda).
  procedure SortKeys(var AKeys: TArray<UInt64>);
  var
    I, J: Integer;
    LKey: UInt64;
  begin
    for I := 1 to High(AKeys) do
    begin
      LKey := AKeys[I];
      J := I - 1;
      while (J >= 0) and (AKeys[J] > LKey) do
      begin
        AKeys[J + 1] := AKeys[J];
        Dec(J);
      end;
      AKeys[J + 1] := LKey;
    end;
  end;

var
  LKeys: TArray<UInt64>;
  LSnapshot: TArray<TAMQPRawPublish>;
  I: Integer;
begin
  if not (FConfirmMode and FRepublish) then
    Exit;
  // Tira um snapshot ordenado (ordem original de publicação) e limpa o buffer
  // sob o lock; reenvia FORA do lock (DoSendPublish re-adquire FWriteLock/FConfirmMon
  // e re-bufferiza sob os novos seq-nos).
  FConfirmMon.Enter;
  try
    LKeys := FResendBuffer.Keys.ToArray;
    if Length(LKeys) = 0 then
      Exit;
    SortKeys(LKeys);
    SetLength(LSnapshot, Length(LKeys));
    for I := 0 to High(LKeys) do
      LSnapshot[I] := FResendBuffer[LKeys[I]];
    FResendBuffer.Clear;
  finally
    FConfirmMon.Leave;
  end;

  for I := 0 to High(LSnapshot) do
    DoSendPublish(LSnapshot[I].Method, LSnapshot[I].Header, LSnapshot[I].Body);
end;

function TAMQPChannel.PublishText(const AExchange, ARoutingKey, AText: string;
  APersistent: Boolean): UInt64;
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  if APersistent then
    LProps.SetPersistent;
  Result := Publish(AExchange, ARoutingKey, AmqpUtf8Encode(AText), LProps);
end;

function TAMQPChannel.BasicGet(const AQueue: string; ANoAck: Boolean): TAMQPGetResult;
begin
  FRpcLock.Enter;
  try
    if FClosed then
      raise EAMQPChannel.Create('canal fechado');
    FRpcKind := rkNone;
    FRpcError := '';
    FRpcEvent.ResetEvent;
    FConnection.SendMethod(FChannelId, BuildBasicGet(AQueue, ANoAck));
    if FRpcEvent.WaitFor(AMQP_RPC_TIMEOUT_MS) <> wrSignaled then
      raise EAMQPChannel.Create('timeout aguardando resposta do servidor');
    case FRpcKind of
      rkMessage:
        Result := FRpcMessage;      // Get-Ok (Found=True veio da montagem)
      rkMethod:
        begin                       // Get-Empty
          Result := Default(TAMQPGetResult);
          Result.Found := False;
        end;
      rkError:
        raise EAMQPChannel.Create(FRpcError);
    else
      raise EAMQPChannel.Create('resposta de RPC inesperada');
    end;
  finally
    FRpcLock.Leave;
  end;
end;

procedure TAMQPChannel.Ack(ADeliveryTag: UInt64; AMultiple: Boolean);
begin
  FConnection.SendMethod(FChannelId, BuildBasicAck(ADeliveryTag, AMultiple));
end;

procedure TAMQPChannel.Nack(ADeliveryTag: UInt64; ARequeue, AMultiple: Boolean);
begin
  FConnection.SendMethod(FChannelId, BuildBasicNack(ADeliveryTag, AMultiple, ARequeue));
end;

procedure TAMQPChannel.ConfirmSelect;
begin
  if FConfirmMode then
    Exit;
  CallRpc(BuildConfirmSelect(False)); // Confirm.Select-Ok (sem args)
  FConfirmMon.Enter;
  try
    FUnconfirmed.Clear;
    FNacked.Clear;
    FResendBuffer.Clear;
    FPublishSeqNo := 0; // primeiro publish daqui em diante recebe o seq-no 1
    FConfirmBase := 0;
  finally
    FConfirmMon.Leave;
  end;
  FConfirmMode := True; // por último: a partir daqui Publish passa a numerar
  AddRecovery(BuildConfirmSelect(False), True); // re-arma o confirm mode na reconexão
end;

function TAMQPChannel.WaitForConfirm(ASeqNo: UInt64; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
  LRemaining: Int64;
begin
  if not FConfirmMode then
    raise EAMQPChannel.Create('canal não está em modo confirm (chame ConfirmSelect)');
  if ASeqNo = 0 then
    raise EAMQPChannel.Create('seq-no inválido (0) em WaitForConfirm');
  LDeadline := AmqpTickMs + ATimeoutMs;
  FConfirmMon.Enter;
  try
    while True do
    begin
      if FNacked.ContainsKey(ASeqNo) then
      begin
        FNacked.Remove(ASeqNo);
        Exit(False); // nack-ado (ou perdido numa queda de conexão)
      end;
      if not FUnconfirmed.ContainsKey(ASeqNo) then
        // não está pendente nem nack-ado: ack-ado (se já foi publicado) ou
        // seq-no que nunca existiu (> último atribuído => False, não espera à toa).
        Exit(ASeqNo <= FPublishSeqNo);
      LNow := AmqpTickMs;
      if LNow >= LDeadline then
        Exit(False);
      LRemaining := Int64(LDeadline) - Int64(LNow);
      FConfirmMon.Wait(Cardinal(LRemaining));
    end;
  finally
    FConfirmMon.Leave;
  end;
end;

function TAMQPChannel.WaitForConfirms(ATimeoutMs: Cardinal): Boolean;
var
  LDeadline, LNow: UInt64;
  LRemaining: Int64;
begin
  if not FConfirmMode then
    raise EAMQPChannel.Create('canal não está em modo confirm (chame ConfirmSelect)');
  LDeadline := AmqpTickMs + ATimeoutMs;
  FConfirmMon.Enter;
  try
    while FUnconfirmed.Count > 0 do
    begin
      LNow := AmqpTickMs;
      if LNow >= LDeadline then
        Exit(False);
      LRemaining := Int64(LDeadline) - Int64(LNow);
      FConfirmMon.Wait(Cardinal(LRemaining));
    end;
    // Todos os pendentes resolveram: sucesso sse NENHUM foi nack-ado. FNacked
    // guarda os nacks ainda não consumidos — inclusive os que chegaram ANTES
    // desta chamada. Ao limpar, consumimos o resultado deste lote.
    Result := FNacked.Count = 0;
    FNacked.Clear;
  finally
    FConfirmMon.Leave;
  end;
end;

procedure TAMQPChannel.Qos(APrefetchCount: Word; AGlobal: Boolean);
var
  LPayload: TBytes;
begin
  LPayload := BuildBasicQos(APrefetchCount, AGlobal);
  CallRpc(LPayload); // Qos-Ok (sem args)
  AddRecovery(LPayload, True);
end;

function TAMQPChannel.Consume(const AQueue: string;
  const ACallback: TAMQPConsumerCallback; ANoAck, AExclusive: Boolean): string;
var
  LTag: string;
  LConsume: TAMQPBasicConsume;
  LPayload: TBytes;
begin
  // Geramos o consumer-tag no cliente e registramos o callback ANTES de enviar,
  // para não perder deliveries que cheguem entre o Consume-Ok e o registro.
  LTag := Format('ctag-%d-%d', [FChannelId, AmqpAtomicInc(FConsumerCounter)]);
  FConsumersLock.Enter;
  try
    FConsumers.AddOrSetValue(LTag, ACallback);
  finally
    FConsumersLock.Leave;
  end;

  LConsume := TAMQPBasicConsume.Create(AQueue, LTag, ANoAck);
  LConsume.Exclusive := AExclusive;
  LPayload := BuildBasicConsume(LConsume);
  try
    CallRpc(LPayload); // Consume-Ok (devolve o mesmo tag)
  except
    FConsumersLock.Enter;
    try
      FConsumers.Remove(LTag);
    finally
      FConsumersLock.Leave;
    end;
    raise;
  end;
  // Grava para replay após reconexão (mesmo tag e callback).
  AddRecovery(LPayload, True, True, LTag, ACallback);
  Result := LTag;
end;

procedure TAMQPChannel.Cancel(const AConsumerTag: string);
begin
  // O cleanup local roda mesmo se CallRpc falhar (canal caiu / timeout): senão o
  // consumer ficaria em FRecovery e seria ressuscitado numa eventual reconexão.
  try
    CallRpc(BuildBasicCancel(AConsumerTag, False)); // Cancel-Ok
  finally
    FConsumersLock.Enter;
    try
      FConsumers.Remove(AConsumerTag);
    finally
      FConsumersLock.Leave;
    end;
    RemoveConsumerRecovery(AConsumerTag);
  end;
end;

procedure TAMQPChannel.DispatchToPool(AItem: TAMQPWorkItem);
begin
  if Assigned(FDispatchPool) then
    FDispatchPool.Queue(AItem)
  else
    AmqpPool.Queue(AItem);
end;

procedure TAMQPChannel.DispatchDelivery(const ADeliver: TAMQPBasicDeliver;
  const AProps: TAMQPBasicProperties; const ABody: TBytes);
var
  LCallback: TAMQPConsumerCallback;
  LDelivery: TAMQPDelivery;
begin
  FConsumersLock.Enter;
  try
    if not FConsumers.TryGetValue(ADeliver.ConsumerTag, LCallback) then
      LCallback := nil;
  finally
    FConsumersLock.Leave;
  end;

  LDelivery := Default(TAMQPDelivery);
  LDelivery.ConsumerTag := ADeliver.ConsumerTag;
  LDelivery.DeliveryTag := ADeliver.DeliveryTag;
  LDelivery.Redelivered := ADeliver.Redelivered;
  LDelivery.Exchange := ADeliver.Exchange;
  LDelivery.RoutingKey := ADeliver.RoutingKey;
  LDelivery.Properties := AProps;
  LDelivery.Body := ABody;

  if not Assigned(LCallback) then
  begin
    // sem consumer (cancelado): libera eventuais Headers e descarta
    if LDelivery.Properties.Has(bpHeaders) and Assigned(LDelivery.Properties.Headers) then
      LDelivery.Properties.Headers.Free;
    Exit;
  end;

  // Despacha para o pool; a thread de leitura NÃO roda o callback.
  AmqpAtomicInc(FInFlight);
  DispatchToPool(TAMQPDeliveryWork.Create(Self, LCallback, LDelivery));
end;

procedure TAMQPChannel.DispatchReturn(const AReturn: TAMQPBasicReturn;
  const AProps: TAMQPBasicProperties; const ABody: TBytes);
var
  LCallback: TAMQPBasicReturnCallback;
  LReturned: TAMQPReturnedMessage;
begin
  LCallback := FOnBasicReturn;

  LReturned := Default(TAMQPReturnedMessage);
  LReturned.ReplyCode := AReturn.ReplyCode;
  LReturned.ReplyText := AReturn.ReplyText;
  LReturned.Exchange := AReturn.Exchange;
  LReturned.RoutingKey := AReturn.RoutingKey;
  LReturned.Properties := AProps;
  LReturned.Body := ABody;

  if not Assigned(LCallback) then
  begin
    // sem handler: libera eventuais Headers e descarta (mesmo comportamento
    // de antes de o Basic.Return ser tratado).
    if LReturned.Properties.Has(bpHeaders) and Assigned(LReturned.Properties.Headers) then
      LReturned.Properties.Headers.Free;
    Exit;
  end;

  // Despacha para o pool; a thread de leitura NÃO roda o callback.
  AmqpAtomicInc(FInFlight);
  DispatchToPool(TAMQPReturnWork.Create(Self, LCallback, LReturned));
end;

procedure TAMQPChannel.DispatchConfirm(ASeqNo: UInt64; AAck: Boolean);
var
  LCallback: TAMQPConfirmCallback;
begin
  LCallback := FOnConfirm;
  if not Assigned(LCallback) then
    Exit;
  // Despacha para o pool; a thread de leitura NÃO roda o callback.
  AmqpAtomicInc(FInFlight);
  DispatchToPool(TAMQPConfirmWork.Create(Self, LCallback, ASeqNo, AAck));
end;

function TAMQPChannel.ResolveConfirms(ATag: UInt64;
  AMultiple, AAck: Boolean): TArray<UInt64>;
var
  LList: TList<UInt64>;
  LKey, LUserTag: UInt64;
begin
  // Roda na thread de leitura. ATag é o delivery-tag do broker (numeração do
  // WIRE, reinicia por sessão); somamos o offset da sessão para chegar ao seq-no
  // do usuário. `multiple` cobre "este e todos os anteriores ainda pendentes",
  // então resolvemos todo seq-no <= tag.
  LList := TList<UInt64>.Create;
  try
    FConfirmMon.Enter;
    try
      LUserTag := FConfirmBase + ATag;
      for LKey in FUnconfirmed.Keys do
        if (AMultiple and (LKey <= LUserTag)) or (LKey = LUserTag) then
          LList.Add(LKey);
      for LKey in LList do
      begin
        FUnconfirmed.Remove(LKey);
        FResendBuffer.Remove(LKey); // confirmado (ack ou nack do broker): não reenvia
        if not AAck then
          FNacked.AddOrSetValue(LKey, True);
      end;
      FConfirmMon.PulseAll; // acorda WaitForConfirm(s)
    finally
      FConfirmMon.Leave;
    end;
    Result := LList.ToArray;
  finally
    LList.Free;
  end;
end;

procedure TAMQPChannel.FailAllUnconfirmed;
var
  LKey: UInt64;
begin
  // Chamado quando a conexão cai (SignalError): publishes pendentes ficam em
  // estado ambíguo — reportamos como não confirmados (WaitForConfirm => False).
  // NÃO dispara OnConfirm (que é só para confirmações reais do broker).
  if not FConfirmMode then
    Exit;
  FConfirmMon.Enter;
  try
    for LKey in FUnconfirmed.Keys do
      FNacked.AddOrSetValue(LKey, True);
    FUnconfirmed.Clear;
    FConfirmMon.PulseAll;
  finally
    FConfirmMon.Leave;
  end;
end;

procedure TAMQPChannel.DrainInFlight;
begin
  // Espera SEM timeout: liberar o canal com um callback ainda em execução seria
  // use-after-free (o TTask capturou Self e ainda mexe em FInFlight/FConnection).
  // Um callback bem-comportado sempre termina — mesmo que faça IO de 5s+ (o caso
  // de uso alvo). Não chame Close de dentro do próprio callback (auto-espera).
  while AmqpAtomicGet(FInFlight) > 0 do
    Sleep(10);
end;

procedure TAMQPChannel.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPCloseInfo;
begin
  if FClosed or (not FIsOpen) then
    Exit;

  LClose.ReplyCode := AReplyCode;
  LClose.ReplyText := AReplyText;
  LClose.ClassId := 0;
  LClose.MethodId := 0;
  try
    CallRpc(BuildChannelClose(LClose)); // aguarda Channel.Close-Ok
  except
    // se falhar (conexão caiu etc.), segue fechando localmente
  end;
  FClosed := True;
  FIsOpen := False;
  // Após o Close-Ok o servidor não entrega mais; espera callbacks em voo
  // terminarem antes de o objeto poder ser liberado.
  DrainInFlight;
  FConnection.UnregisterChannel(FChannelId);
end;

{ TAMQPChannel — recepção (roda na thread de leitura) }

procedure TAMQPChannel.CompleteContent;
var
  LMsg: TAMQPGetResult;
begin
  FAsmState := asIdle;
  if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK) then
  begin
    LMsg := Default(TAMQPGetResult);
    LMsg.Found := True;
    LMsg.DeliveryTag := FAsmGetOk.DeliveryTag;
    LMsg.Redelivered := FAsmGetOk.Redelivered;
    LMsg.Exchange := FAsmGetOk.Exchange;
    LMsg.RoutingKey := FAsmGetOk.RoutingKey;
    LMsg.MessageCount := FAsmGetOk.MessageCount;
    LMsg.Properties := FAsmProps;
    LMsg.Body := FAsmBody;
    SignalMessage(LMsg);
  end
  else if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER) then
    DispatchDelivery(FAsmDeliver, FAsmProps, FAsmBody)
  else if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN) then
    DispatchReturn(FAsmReturn, FAsmProps, FAsmBody)
  else
  begin
    // Método desconhecido chegando por este caminho: descartamos o conteúdo.
    // Como ninguém assume a posse das Properties aqui, liberamos a tabela de
    // Headers se ela veio (senão vazaria).
    if FAsmProps.Has(bpHeaders) and Assigned(FAsmProps.Headers) then
    begin
      FAsmProps.Headers.Free;
      FAsmProps.Headers := nil;
    end;
  end;
  FAsmBody := nil;
end;

procedure TAMQPChannel.HandleFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LClose: TAMQPCloseInfo;
  LHeader: TAMQPContentHeader;
  LTag: string;
  LAck: TAMQPBasicAck;
  LNack: TAMQPBasicNack;
  LResolved: TArray<UInt64>;
  LSeq: UInt64;
begin
  case FAsmState of
    asHeader:
      begin
        if AFrame.FrameType <> AMQP_FRAME_HEADER then
          Exit; // frame fora de ordem: ignora (protocolo)
        LReader := TAMQPReader.Create(AFrame.Payload);
        try
          LHeader := DecodeContentHeader(LReader);
        finally
          LReader.Free;
        end;
        FAsmProps := LHeader.Properties;
        FAsmBody := nil;
        FAsmRemaining := LHeader.BodySize;
        if FAsmRemaining = 0 then
          CompleteContent
        else
          FAsmState := asBody;
        Exit;
      end;

    asBody:
      begin
        if AFrame.FrameType <> AMQP_FRAME_BODY then
          Exit;
        FAsmBody := FAsmBody + AFrame.Payload;
        if UInt64(Length(AFrame.Payload)) >= FAsmRemaining then
          FAsmRemaining := 0
        else
          Dec(FAsmRemaining, Length(AFrame.Payload));
        if FAsmRemaining = 0 then
          CompleteContent;
        Exit;
      end;
  end;

  // asIdle: espera um frame de método.
  if not AFrame.IsMethod then
    Exit;

  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);

    if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK) then
    begin
      FAsmMethodId := LId;
      FAsmGetOk := DecodeBasicGetOk(LReader);
      FAsmState := asHeader;
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER) then
    begin
      FAsmMethodId := LId;
      FAsmDeliver := DecodeBasicDeliver(LReader);
      FAsmState := asHeader; // conteúdo vem nos próximos frames
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN) then
    begin
      // publish mandatory não roteado: conteúdo vem nos próximos frames.
      FAsmMethodId := LId;
      FAsmReturn := DecodeBasicReturn(LReader);
      FAsmState := asHeader;
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL) then
    begin
      // servidor cancelou o consumer (ex.: fila removida).
      LTag := DecodeBasicCancel(LReader);
      FConsumersLock.Enter;
      try
        FConsumers.Remove(LTag);
      finally
        FConsumersLock.Leave;
      end;
      RemoveConsumerRecovery(LTag);
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_ACK) then
    begin
      // publisher confirm: ack de um ou mais publishes (frame de método puro,
      // sem content). Resolve os pendentes e despacha OnConfirm fora do lock.
      LAck := DecodeBasicAck(LReader);
      LResolved := ResolveConfirms(LAck.DeliveryTag, LAck.Multiple, True);
      for LSeq in LResolved do
        DispatchConfirm(LSeq, True);
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_NACK) then
    begin
      // publisher confirm: nack (o broker não conseguiu cuidar do publish).
      LNack := DecodeBasicNack(LReader);
      LResolved := ResolveConfirms(LNack.DeliveryTag, LNack.Multiple, False);
      for LSeq in LResolved do
        DispatchConfirm(LSeq, False);
    end
    else if LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE) then
    begin
      LClose := DecodeChannelClose(LReader);
      FConnection.SendMethod(FChannelId, BuildChannelCloseOk);
      SignalError(Format('canal %d fechado pelo servidor: %d %s',
        [FChannelId, LClose.ReplyCode, LClose.ReplyText]));
    end
    else
    begin
      // resposta de RPC (Open-Ok, Declare-Ok, Bind-Ok, Get-Empty, Close-Ok, ...)
      SignalMethod(AFrame.Payload);
    end;
  finally
    LReader.Free;
  end;
end;

end.
