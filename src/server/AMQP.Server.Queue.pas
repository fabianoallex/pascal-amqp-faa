unit AMQP.Server.Queue;

{$I amqp.inc}

{ A fila como ATOR -- sub-modulo broker, WS3 da Fase 2 (engine de roteamento
  em memoria, ver CLAUDE.md).

  Decisao D2 (travada): o ator NAO tem thread propria. Ele e' um work item
  agendado num TAMQPThreadPool -- N filas ociosas nao podem virar N threads.
  A regra que isso impoe: NENHUM comando pode bloquear o worker esperando I/O
  (por isso a entrega da WS4 e' post nao-bloqueante e o consumidor lento sai
  do rodizio, decisao D3).

  A REGRA DE OURO: nenhum campo de estado da fila (estoque, nao-confirmadas,
  consumidores, contadores) e' lido ou escrito fora do ator -- inclusive as
  contagens que vao para o cliente no Queue.Declare-Ok/Purge-Ok/Delete-Ok.
  Quem esta' de fora fala com a fila SO' por comando.

  Caixa de comandos serial
  ------------------------
  Post enfileira sob o lock e, se FScheduled estiver falso, marca e agenda UM
  work item. O runner faz laco: sob o lock, se a caixa esta' vazia zera
  FScheduled e sai; senao tira um comando e executa FORA do lock. FScheduled
  garante no maximo um work item agendado por fila -- quem esta' no laco e' o
  unico executor. O ponto sutil: o runner so' declara a caixa vazia SOB O
  LOCK, o que fecha a janela entre "vi vazio" e "zerei o flag" (um Post
  concorrente que ve FScheduled=True nao agenda, e o runner corrente encontra
  o comando na volta seguinte). Teto de AMQP_QUEUE_CMD_BATCH comandos por
  rodada -- ao estourar, reagenda em vez de monopolizar um worker do pool.

  Comandos assincronos x sincronos
  --------------------------------
  Assincronos (enqueue, add/remove consumidor, ack/nack/reject): o ator libera
  no finally. Sincronos (Get, Purge, Delete, Stats): carregam evento de
  conclusao e campos de resultado, porque a thread da conexao precisa da
  resposta para montar o *-Ok. O chamador espera com timeout generoso -- o
  ator nao faz I/O, entao estourar e' bug NOSSO, nao lentidao do peer.

  O comando e' REFCONTADO (duas referencias: o chamador e a caixa), e nao
  "propriedade do chamador": se um sincrono estourar o timeout, o dono liberar
  o objeto enquanto o ator ainda o tem na mao seria use-after-free empilhado
  em cima de um bug que ja' e' ruim. Custa quatro linhas e apaga a classe
  inteira de crash.

  Guard vivo: o ator grava o thread-id enquanto executa, e postar um comando
  SINCRONO a partir dessa thread levanta EAMQPQueueActor na hora, em vez de
  travar em silencio (o ator esperaria por si mesmo).

  Ordem de destruicao
  -------------------
  Stop marca a parada sob o lock (recusa posts novos e novo agendamento),
  espera o ator sair do laco (FScheduled=False) com deadline e SO' ENTAO drena
  o estoque inline. NUNCA liberar a fila sem o Stop ter voltado -- e' a mesma
  familia de erro que custou double-free na Fase 1. Stop NAO usa um "comando
  de parada": se o pool ja' estiver em shutdown (a finalizacao da unit
  AMQP.Threading libera itens enfileirados SEM executa-los), um comando
  postado ali nunca rodaria e o Stop so' voltaria no timeout.

  Escopo do WS3 -- o que NAO esta' aqui
  -------------------------------------
  A ENTREGA e' a WS4. Esta unit registra os consumidores (tag, canal, no-ack,
  alvo) e mantem o indice de rodizio, mas nao despacha Basic.Deliver, nao
  aplica prefetch e nao olha Channel.Flow. A costura e' IAMQPDeliveryTarget:
  a WS4 implementa (no canal/conexao) e passa no consumidor. Note tambem que
  o TAMQPFrameWriter.PostFrames de hoje BLOQUEIA quando a fila outbound enche
  -- incompativel com D2/D3; o TryPostFrames que a entrega precisa entra na
  WS4, nao aqui. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Server.Message,
  AMQP.Server.Header,
  AMQP.Server.Resources,
  AMQP.Server.Journal,
  AMQP.Server.Records;

const
  /// Quantos comandos o ator executa por rodada antes de reagendar. Sem teto,
  /// uma fila muito movimentada monopolizaria um worker do pool enquanto
  /// houvesse trabalho.
  AMQP_QUEUE_CMD_BATCH = 64;

  /// Espera de um comando sincrono. Generoso de proposito: o ator nunca faz
  /// I/O, entao estourar isto nao e' peer lento -- e' bug nosso (ou pool
  /// saturado, ver a mensagem de EAMQPQueueActor).
  AMQP_QUEUE_SYNC_TIMEOUT_MS = 15000;

  /// Quanto o Stop espera o ator sair do laco antes de desistir.
  AMQP_QUEUE_STOP_TIMEOUT_MS = 15000;

  /// Teto de saltos de dead-letter (decisao D14). Mesmo espirito do
  /// AMQP_MAX_EXCHANGE_HOP_DEPTH que o Route ja' usa: o grafo de DLX pode ter
  /// ciclo, e cortar por PROFUNDIDADE e' mais barato que carregar o conjunto
  /// de filas visitadas, com o mesmo efeito pratico. Ao estourar, a mensagem
  /// e' descartada e contada -- nao ha caminho em que ela circule para sempre.
  AMQP_MAX_DEADLETTER_HOPS = 16;

type
  { Falha de uso do ator: sincrono postado de dentro do proprio ator, comando
    numa fila ja parada, ou timeout de comando sincrono. Sempre bug de
    programacao ou saturacao do pool -- nunca erro de protocolo. }
  EAMQPQueueActor = class(Exception);

  { Resultado de um Queue.Delete com if-unused/if-empty. Quem mapeia para
    reply-code (405/406) e' a WS5 -- mesmo espirito do TAMQPTopologyResult. }
  TAMQPQueueDeleteResult = (amqqdOk, amqqdEmUso, amqqdNaoVazia);

  { Contagens que o cliente ve (Queue.Declare-Ok, Purge-Ok, Delete-Ok) mais as
    metricas internas. So' o ator as produz. }
  TAMQPQueueStats = record
    /// Mensagens prontas (nao conta as entregues e ainda nao confirmadas).
    MessageCount: Integer;
    ConsumerCount: Integer;
    /// Entregues e aguardando ack/nack/reject.
    UnackedCount: Integer;
    /// Descartadas por MaxLength (D7) ou por post numa fila ja parada.
    /// Metrica OBRIGATORIA: sem ela, mensagem some sem ninguem saber.
    DroppedCount: Int64;
    /// Entregues a consumidores desde a criacao (nao conta Basic.Get).
    DeliveredCount: Int64;
    /// Ha quanto tempo a fila esta' SEM USO (WS8 da Fase 3), em ms. Zero
    /// enquanto houver consumidor: fila com consumidor esta' em uso por
    /// definicao, e o relogio de x-expires so' comeca quando o ultimo sai.
    ///
    /// "Uso" e' consumidor, Basic.Get e redeclare -- PUBLICAR NAO CONTA. E' a
    /// regra do RabbitMQ, e a razao dela e' o proposito do argumento: uma fila
    /// que so' recebe e ninguem le e' exatamente a que se quer recolher.
    IdleMs: Int64;
    /// Mortas que foram republicadas num DLX. Nao se soma a ExpiredCount nem
    /// a DroppedCount: aquelas contam o MOTIVO, esta conta o DESTINO.
    DeadLetteredCount: Int64;
    /// Vencidas por TTL (x-message-ttl da fila ou 'expiration' da mensagem).
    /// Metrica OBRIGATORIA pela mesma razao de DroppedCount: sem ela, mensagem
    /// some sem ninguem saber. Na WS5 estas passam a ir para o DLX em vez de
    /// serem descartadas -- a contagem continua valendo.
    ExpiredCount: Int64;
    /// True se esta fila JA TEVE ao menos um consumidor. E' o que separa
    /// "auto-delete que perdeu o ultimo consumidor" (some) de "auto-delete
    /// que nunca teve nenhum" (fica) -- mesma regra do RabbitMQ.
    EverHadConsumer: Boolean;
  end;

  { Costura do dead-lettering (WS5). Quem implementa e' a engine, que e' a
    unica que enxerga a topologia; a fila so' guarda a referencia.

    Existe como interface pela mesma razao do IAMQPDeliveryTarget: sem ela,
    AMQP.Server.Queue teria de enxergar AMQP.Server.Engine, que ja' enxerga a
    fila -- ciclo de units.

    NAO PODE BLOQUEAR NEM FAZER I/O (D17): republicar e' ler a topologia sob
    RWLock e postar na caixa de outras filas, as duas coisas rapidas e sem
    espera. Se um dia precisar esperar alguma coisa, o dead-letter sai do ator. }
  IAMQPDeadLetterSink = interface
    ['{2B9F1C74-5E30-4A88-9D61-7C0E3F5A82B4}']
    /// Republica AMessage em ADlx com ARoutingKey. A referencia continua sendo
    /// do CHAMADOR (a implementacao da AddRef se quiser guardar).
    /// AContentId identifica o CORPO ja' gravado: a colocacao nova reusa o
    /// mesmo (D22), e por isso o corpo nunca e' reescrito num dead-letter.
    /// Zero = a mensagem nao estava persistida.
    procedure DeadLetter(const ADlx, ARoutingKey: string;
      AMessage: TAMQPMessage; APriority: Byte; AContentId: UInt64);
  end;

  { Costura da entrega (WS4). O canal/conexao implementa; a fila so' guarda a
    referencia no consumidor.

    TryDeliver NAO PODE BLOQUEAR (D2/D3): devolve False quando a fila outbound
    do writer esta cheia ou o canal morreu, e nesse caso o consumidor sai do
    rodizio ate drenar -- bloquear o ator seria head-of-line para todos os
    outros consumidores da mesma fila. }
  IAMQPDeliveryTarget = interface
    ['{7C4E1F82-3A65-4D19-B0E7-5F2A8C93D604}']
    /// Identidade do canal (NativeUInt e nao referencia, pela mesma razao do
    /// TAMQPQueueDef.OwnerId: nao criar ciclo com AMQP.Server.Connection).
    function ChannelId: NativeUInt;
    /// False se o canal/conexao ja' morreu -- o ator descarta o consumidor.
    function IsAlive: Boolean;
    /// Tenta escrever Basic.Deliver + content-header + body frames, como UM
    /// lote indivisivel. False = nao coube AGORA (fila outbound cheia, canal
    /// com Channel.Flow desligado, prefetch estourado ou canal morto) -- sem
    /// bloquear e sem levantar. A mensagem continua sendo do chamador.
    ///
    /// A delivery-tag e' MONOTONICA POR CANAL, logo quem a gera e' o alvo (o
    /// canal), nao a fila: uma fila entrega para N canais e um canal recebe de
    /// N filas. Sai em ADeliveryTag e so' e' consumida quando a entrega de
    /// fato entra na fila de escrita -- uma recusa nao abre buraco na
    /// sequencia.
    ///
    /// ANoAck vem do consumidor porque muda duas coisas no alvo: entrega em
    /// no-ack nao conta para o prefetch e nao gera nao-confirmada.
    function TryDeliver(const AConsumerTag, AQueueName: string;
      ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
      out ADeliveryTag: UInt64): Boolean;
    /// A fila sumiu debaixo do consumidor (Queue.Delete, ou auto-delete):
    /// manda Basic.Cancel para o cliente saber. Como TryDeliver, NAO PODE
    /// BLOQUEAR -- False = nao coube agora, e o consumidor morre em silencio
    /// (o cliente descobre no proximo uso). Nao ha retentativa: a fila esta
    /// sendo destruida.
    function TryCancel(const AConsumerTag: string): Boolean;
  end;

  { Um consumidor registrado (Basic.Consume). A fila e' dona destes objetos
    depois do PostAddConsumer. }
  TAMQPServerConsumer = class
  private
    FConsumerTag: string;
    FChannelId: NativeUInt;
    FNoAck: Boolean;
    FExclusive: Boolean;
    FTarget: IAMQPDeliveryTarget;
    FSuspended: Boolean;
  public
    /// ATarget NAO pode ser nil: um consumidor sem alvo nao teria como
    /// receber nada. O ChannelId vem DELE -- e' a mesma identidade com que a
    /// fila chaveia as nao-confirmadas e com que o teardown da conexao pede o
    /// PostRemoveChannel; guardar uma copia independente abriria espaco para
    /// as duas divergirem e o requeue de um canal morto errar o alvo.
    constructor Create(const AConsumerTag: string;
      ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget);
    property ConsumerTag: string read FConsumerTag;
    property ChannelId: NativeUInt read FChannelId;
    property NoAck: Boolean read FNoAck;
    property Exclusive: Boolean read FExclusive;
    property Target: IAMQPDeliveryTarget read FTarget;
    /// D3: recusou a entrega NESTA rodada (fila outbound cheia, prefetch
    /// estourado, flow desligado) e por isso sai do rodizio ate' o fim dela.
    /// Nao e' estado persistente: toda rodada de entrega recomeca com todos os
    /// consumidores elegiveis e simplesmente tenta de novo -- uma tentativa
    /// custa um lock e uma comparacao de contador, e assim nao ha nenhum
    /// estado de "suspenso" para ressincronizar quando o canal drena.
    property Suspended: Boolean read FSuspended write FSuspended;
  end;

  { Uma mensagem no estoque. Redelivered e' por FILA (a mesma TAMQPMessage
    pode estar em N filas com historicos diferentes), por isso vive na
    entrada e nao no objeto da mensagem. }
  TAMQPQueueEntry = record
    Msg: TAMQPMessage;
    Redelivered: Boolean;
    /// Nivel de prioridade JA' LIMITADO ao teto da fila. Quem le a propriedade
    /// 'priority' da mensagem e' a thread do publicador (invariante da Fase 2:
    /// a mensagem guarda o header CRU, decisao D1) -- o ator so' recebe o
    /// numero pronto.
    Priority: Byte;
    /// Instante absoluto (mesma base do NowTick) em que a mensagem vence.
    /// 0 = nunca vence.
    ExpiraEm: UInt64;
    /// Identidade desta COLOCACAO no journal (Fase 4, D22). 0 = a mensagem nao
    /// foi persistida -- fila nao duravel, mensagem nao persistente, ou broker
    /// sem DataDir. Quando e' 0, nenhum registro de aposentadoria e' escrito.
    JournalId: UInt64;
    /// Identidade do CORPO no journal. Varias colocacoes (e a derivada do
    /// dead-letter) compartilham o mesmo -- e' o que faz o corpo ser gravado
    /// uma vez so'.
    ContentId: UInt64;
  end;

  { Uma entrega aguardando confirmacao, chaveada por (canal, delivery-tag). }
  TAMQPUnackedEntry = record
    ChannelId: NativeUInt;
    DeliveryTag: UInt64;
    Msg: TAMQPMessage;
    /// Guardados porque o requeue tem de devolver a mensagem A' CABECA DO SEU
    /// PROPRIO BALDE, com o prazo original -- nao a' cabeca absoluta nem com o
    /// relogio zerado.
    Priority: Byte;
    ExpiraEm: UInt64;
    /// Os mesmos da entrada no estoque: a colocacao continua VIVA no journal
    /// enquanto estiver aqui (uma entrega nao confirmada nao e' aposentada --
    /// e' o que faz a D27 sair de graca no replay).
    JournalId: UInt64;
    ContentId: UInt64;
  end;

  TAMQPQueueCmdKind = (
    amqqcEnqueue, amqqcAddConsumer, amqqcRemoveConsumer, amqqcRemoveChannel,
    amqqcAck, amqqcNack, amqqcReject, amqqcDeliverTick,
    amqqcEnqueueSync, amqqcTouch, amqqcGet, amqqcPurge, amqqcDelete,
    amqqcStats);

  { Um comando na caixa. Campos publicos de proposito: e' um registro de
    passagem entre o postador e o ator, nao um objeto de dominio.

    Refcontado (chamador + caixa) -- ver o cabecalho da unit. }
  TAMQPQueueCommand = class
  private
    FRefCount: Integer;
    FKind: TAMQPQueueCmdKind;
    FEvent: TEvent; // nil nos assincronos
    function GetIsSync: Boolean;
  public
    // entrada
    Msg: TAMQPMessage;             // amqqcEnqueue (referencia PROPRIA da fila)
    Consumer: TAMQPServerConsumer; // amqqcAddConsumer (a fila passa a ser dona)
    ConsumerTag: string;
    ChannelId: NativeUInt;
    DeliveryTag: UInt64;
    NoAck: Boolean;
    Priority: Byte;        // amqqcEnqueue
    MessageTtlMs: Int64;   // amqqcEnqueue (-1 = sem 'expiration')
    JournalId: UInt64;     // amqqcEnqueue (0 = nao persistida)
    ContentId: UInt64;     // amqqcEnqueue
    /// amqqcEnqueue: entrada vinda da RECUPERACAO (WS6). A D27 manda toda
    /// entrada recuperada voltar com redelivered=True -- persistir o flag
    /// custaria uma escrita no caminho mais quente que existe, e seria
    /// precisao falsa: depois de uma queda o broker nao sabe se o consumidor
    /// chegou a ver a mensagem.
    Recuperada: Boolean;   // amqqcEnqueue
    Multiple: Boolean;
    Requeue: Boolean;
    IfUnused: Boolean;
    IfEmpty: Boolean;
    // saida (sincronos)
    ResOk: Boolean;
    ResCount: Integer;
    ResMsg: TAMQPMessage;
    ResRedelivered: Boolean;
    ResStats: TAMQPQueueStats;
    ResDelete: TAMQPQueueDeleteResult;

    constructor Create(AKind: TAMQPQueueCmdKind; ASync: Boolean);
    destructor Destroy; override;
    function AddRef: Integer;
    /// Ultimo Release libera o objeto (e, com ele, as referencias de mensagem
    /// que sobrarem em Msg/ResMsg -- caminho de descarte).
    function Release: Integer;
    procedure Signal;
    function WaitFor(ATimeoutMs: Cardinal): Boolean;

    property Kind: TAMQPQueueCmdKind read FKind;
    property IsSync: Boolean read GetIsSync;
  end;

  TAMQPServerQueue = class;

  { O work item que roda uma rodada do ator num worker do pool. }
  TAMQPQueueWork = class(TAMQPWorkItem)
  private
    FQueue: TAMQPServerQueue;
  public
    constructor Create(AQueue: TAMQPServerQueue);
    procedure Execute; override;
  end;

  { A fila. Toda interacao de fora e' por Post*/comando sincrono. }
  TAMQPServerQueue = class
  private
    FName: string;
    FMaxLength: Integer;      // teto EFETIVO de contagem (global + fila)
    FMaxBytes: Int64;         // teto EFETIVO de bytes (-1 = sem teto)
    FBytesProntos: Int64;     // soma dos corpos das PRONTAS (so' do ator)
    FPool: TAMQPThreadPool;
    // --- caixa (sob FMon) ---
    FMon: TAMQPMonitor;
    FMailbox: TQueue<TAMQPQueueCommand>;
    FScheduled: Boolean;
    FStopping: Boolean;
    FStopped: Boolean;
    FActorThread: TThreadID;
    FPolicy: TAMQPQueuePolicy;
    FMaxPriority: Integer; // 0 = fila sem prioridade (um balde so')
    // --- estado, SO' do ator ---
    // UM BALDE POR NIVEL de prioridade (decisao D12: baldes, nao heap). Fila
    // sem x-max-priority tem FMaxPriority=0, logo um balde so' e o mesmo
    // caminho de codigo de antes -- sem ramo condicional espalhado.
    FStock: array of TQueue<TAMQPQueueEntry>;
    FRequeued: array of TQueue<TAMQPQueueEntry>; // cabeca logica de cada balde
    FUnacked: TList<TAMQPUnackedEntry>;
    FConsumers: TObjectList<TAMQPServerConsumer>;
    FRoundRobin: Integer; // proximo consumidor a atender (rodizio)
    FEverHadConsumer: Boolean;
    FDropped: Int64;
    FDelivered: Int64;
    FExpired: Int64;
    FDeadLettered: Int64;
    FDeadLetterSink: IAMQPDeadLetterSink;
    FJournal: TAMQPJournal;
    FVHostName: string;
    FDurable: Boolean;
    /// Trava de mao unica: vira True no primeiro enqueue com prazo e nunca
    /// volta. Serve so' para a fila SEM nenhuma mensagem expiravel pular o
    /// laco de expiracao a cada rodada (D13: a varredura so' custa em fila que
    /// tem TTL). Superestimar e' inofensivo -- no maximo varre uma fila que
    /// ja' nao tem mais nada com prazo.
    FTemPrazo: Boolean;
    /// Instante do ultimo USO (ver Stats.IdleMs). So' do ator.
    FUltimoUso: UInt64;

    procedure ScheduleLocked;
    procedure Post(ACmd: TAMQPQueueCommand);
    procedure PostSync(ACmd: TAMQPQueueCommand);
    // --- helpers do ator (nunca chamados de fora dele) ---
    function ReadyCount: Integer;
    function TakeReady(out AEntry: TAMQPQueueEntry): Boolean;
    function PeekReady(out AEntry: TAMQPQueueEntry): Boolean;
    /// Vence o que passou do prazo, DA CABECA de cada balde para tras. Ver o
    /// comentario da implementacao para a limitacao (deliberada, e a mesma do
    /// RabbitMQ) de mensagem com prazo proprio no meio da fila.
    procedure ExpiraVencidas;
    /// Ponto unico por onde uma mensagem vencida sai. Na WS4 so' larga a
    /// referencia e conta; a WS5 troca o corpo disto por "manda para o DLX".
    procedure DescartaExpirada(AEntry: TAMQPQueueEntry);
    /// Manda a entrada para o DLX da fila, se houver um; senao so' larga a
    /// referencia. Devolve True se de fato republicou. SEMPRE consome a
    /// referencia da entrada -- quem chama nao larga de novo.
    function MorreCom(AEntry: TAMQPQueueEntry; const ARazao: string): Boolean;
    /// Prazo absoluto de uma mensagem, combinando o TTL da FILA com o TTL da
    /// MENSAGEM (o menor vence). 0 = nunca.
    function CalculaPrazo(AMessageTtlMs: Int64): UInt64;
    /// Limita a prioridade pedida ao teto da fila.
    function LimitaPrioridade(APriority: Byte): Byte;
    function NextEligible: TAMQPServerConsumer;
    procedure DeliverPending;
    procedure RequeueFront(AMsg: TAMQPMessage; APriority: Byte;
      AExpiraEm: UInt64; AJournalId: UInt64 = 0; AContentId: UInt64 = 0);
    procedure EnforceMaxLength;
    /// Soma/subtrai o corpo de uma entrada no acumulador de bytes prontos.
    /// Toda entrada que ENTRA ou SAI do estoque passa por aqui -- se um
    /// caminho esquecer, o acumulador vira lixo e o teto de bytes deixa de
    /// valer (sem nenhum sintoma imediato).
    /// UNICO ponto por onde uma colocacao e' aposentada no disco. Chamado de
    /// dentro do ator, com SubmitNoWait: a D2 proibe o ator de esperar I/O, e
    /// registro de retirada nunca pode ser recusado (D24, corolario c).
    ///
    /// Nao ha aposentadoria quando a fila INTEIRA e' apagada: o registro de
    /// delete da topologia ja' leva as colocacoes dela junto, do mesmo jeito
    /// que leva os bindings -- gravar um DEQ por entrada seria escrever o que
    /// ja' esta' implicito.
    procedure Aposenta(AJournalId: UInt64);
    procedure ContaBytes(const AEntry: TAMQPQueueEntry; ASinal: Integer);
    /// True se a fila esta' cheia AGORA (por contagem ou por bytes). So' faz
    /// sentido em fila com x-overflow reject-publish.
    function EstaCheia: Boolean;
    /// ARejeitada separa ACK de NACK/REJECT-sem-requeue: os dois chegam aqui
    /// com ARequeue=False, mas so' o segundo e' morte (razao 'rejected').
    /// Confundir os dois mandaria toda mensagem confirmada para o DLX.
    procedure ResolveUnacked(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple, ARequeue, ARejeitada: Boolean);
    function CurrentStats: TAMQPQueueStats;
    procedure DrainState;
  protected
    /// Ponto exato entre "o laco de comandos terminou" e "o lock do fim de
    /// rodada foi tomado" -- a JANELA que o desenho da caixa fecha. No-op em
    /// producao; existe porque essa janela e' inalcancavel por um teste de
    /// estresse comum (medido: uma versao com a checagem de caixa vazia FORA
    /// do lock passa nos 16.000 posts concorrentes, porque um comando perdido
    /// e' rescuscitado pelo post seguinte -- o bug so' e' fatal no ULTIMO post
    /// antes da ociosidade). O teste sobrescreve isto para postar de outra
    /// thread exatamente aqui. NAO PODE LEVANTAR.
    procedure ActorRoundEnding; virtual;
    /// Relogio do ator (decisao D13). Virtual porque teste de TTL nao pode
    /// depender de Sleep: a suite sobrescreve isto e faz o tempo andar na mao,
    /// o que torna a expiracao deterministica e instantanea. Em producao e'
    /// AmqpTickMs.
    function NowTick: UInt64; virtual;
    /// Uma rodada do ator. Publico so' para o work item; nunca chame de fora.
    procedure RunActor;
    /// Executa UM comando, sempre na thread do ator e fora do lock. Virtual
    /// so' para os testes conseguirem espiar de dentro do ator (e' assim que
    /// o guard de comando sincrono e' exercitado).
    procedure ExecuteCommand(ACmd: TAMQPQueueCommand); virtual;
  public
    /// AMaxLength = 0: sem limite (default da D7). > 0: ao estourar, descarta
    /// da CABECA e conta em Stats.DroppedCount.
    /// APool = nil: AmqpPool global (letra da D2). O parametro existe para o
    /// broker poder isolar seus atores num pool proprio sem mudanca de
    /// estrutura, se a inanicao aparecer (o AmqpPool tem teto e e'
    /// compartilhado com os callbacks de consumer do cliente).
    constructor Create(const AName: string; AMaxLength: Integer = 0;
      APool: TAMQPThreadPool = nil); overload;
    /// Com os x-arguments ja' lidos (WS2). E' por aqui que a engine cria a
    /// fila; a sobrecarga acima existe para quem nao tem politica nenhuma
    /// (testes de estado puro) e equivale a passar TAMQPQueuePolicy.Empty.
    constructor Create(const AName: string; const APolicy: TAMQPQueuePolicy;
      AMaxLength: Integer = 0; APool: TAMQPThreadPool = nil); overload;
    destructor Destroy; override;

    // --- comandos assincronos ---
    /// A fila tira a SUA PROPRIA referencia (AddRef): quem publica em N filas
    /// continua dono da referencia dele e da Release depois do fan-out.
    /// Numa fila ja parada, a mensagem e' descartada e contada em
    /// DroppedCount (nao levanta -- publish nao falha por corrida de
    /// teardown).
    /// APriority e AMessageTtlMs vem DECODIFICADOS pela thread do publicador
    /// (a mensagem guarda o header cru -- decisao D1 --, entao quem le as
    /// propriedades e' quem ainda tem a tabela viva do canal na mao). A fila
    /// limita a prioridade ao proprio teto e combina o TTL da mensagem com o
    /// da fila. AMessageTtlMs < 0 = a mensagem nao trouxe 'expiration'.
    /// AJournalId/AContentId vem de quem ESCREVEU o registro de colocacao --
    /// a thread do publicador (D24) --, e nao sao gerados aqui: a fila e' o
    /// ultimo elo, nao o primeiro. Zero = nada foi persistido.
    procedure PostMessage(AMessage: TAMQPMessage; APriority: Byte = 0;
      AMessageTtlMs: Int64 = -1; AJournalId: UInt64 = 0;
      AContentId: UInt64 = 0; ARecuperada: Boolean = False);
    /// A fila passa a ser dona de AConsumer.
    procedure PostAddConsumer(AConsumer: TAMQPServerConsumer);
    procedure PostRemoveConsumer(AChannelId: NativeUInt;
      const AConsumerTag: string);
    /// Canal (ou conexao) caiu: tira todos os consumidores dele e devolve as
    /// nao-confirmadas dele para a fila, com redelivered. WS7 usa.
    procedure PostRemoveChannel(AChannelId: NativeUInt);
    /// Reexamina a entrega sem nenhum outro evento. Existe para o caso
    /// degradado da D3: consumidores que recusaram (fila outbound cheia) e
    /// nenhum ack chegando -- tipico de consumidor em no-ack mais lento que o
    /// produtor. Nesse caso nada mais acordaria o ator, entao a thread
    /// monitora do broker cutuca as filas periodicamente. No caminho normal a
    /// entrega sai do proprio enqueue/ack, sem depender deste tick.
    procedure PostDeliverTick;
    /// Marca a fila como USADA agora (WS8). O declare/redeclare chama isto --
    /// e' um dos tres eventos que reiniciam o relogio do x-expires.
    procedure PostTouch;
    procedure PostAck(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple: Boolean);
    procedure PostNack(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple, ARequeue: Boolean);
    procedure PostReject(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      ARequeue: Boolean);

    // --- comandos sincronos (levantam EAMQPQueueActor se a fila ja parou,
    //     se forem postados de dentro do proprio ator, ou no timeout) ---
    /// Enqueue SINCRONO, para fila declarada com x-overflow reject-publish
    /// (decisao D15). Devolve False quando a fila esta' cheia e a mensagem foi
    /// RECUSADA -- e' o unico caminho em que uma fila diz nao a um publish.
    ///
    /// Por que sincrono: o publicador precisa da resposta ANTES do Basic.Ack, e
    /// o PostMessage e' assincrono e nao devolve nada. Bloquear o publicador
    /// aqui nao e' efeito colateral -- e' literalmente a semantica que
    /// reject-publish pede. O comando nao faz I/O, entao o teto de 15 s e' rede
    /// contra bug nosso, nao contra peer lento.
    ///
    /// E' o UNICO ponto da Fase 3 desenhado para ser substituido: quando a
    /// Fase 4 tiver o registro de confirms pendentes, isto vira assincrono.
    function TryPostMessage(AMessage: TAMQPMessage; APriority: Byte = 0;
      AMessageTtlMs: Int64 = -1; AJournalId: UInt64 = 0;
      AContentId: UInt64 = 0): Boolean;

    /// Basic.Get. ADeliveryTag e' a tag que o CANAL vai usar (a fila nao
    /// gera tags -- elas sao monotonicas por canal). Com ANoAck=False a
    /// mensagem vai para as nao-confirmadas. A referencia devolvida em
    /// AMessage e' do CHAMADOR (Release depois de usar). False = fila vazia.
    function Get(AChannelId: NativeUInt; ADeliveryTag: UInt64; ANoAck: Boolean;
      out AMessage: TAMQPMessage; out ARedelivered: Boolean): Boolean;
    /// Queue.Purge: descarta as prontas (nao mexe nas nao-confirmadas) e
    /// devolve quantas foram.
    function Purge: Integer;
    function Stats: TAMQPQueueStats;
    /// Queue.Delete. Em amqqdOk a fila fica parada e vazia -- o dono ainda
    /// precisa liberar o objeto.
    function Delete(AIfUnused, AIfEmpty: Boolean;
      out AMessageCount: Integer): TAMQPQueueDeleteResult;

    /// Para o ator e libera o estoque. Idempotente. Chamado pelo destrutor.
    /// Levanta EAMQPQueueActor se o ator nao sair do laco no prazo -- nesse
    /// caso NADA e' liberado (vazar e' melhor que liberar sob os pes de um
    /// ator ainda rodando).
    procedure Stop;

    property Name: string read FName;
    property MaxLength: Integer read FMaxLength;
    /// Teto de prioridade em vigor (0 = fila sem prioridade).
    property MaxPriority: Integer read FMaxPriority;

    /// Prioridade efetiva desta mensagem NESTA fila (limitada ao teto) e TTL
    /// efetivo em ms (o menor entre o da fila e o da mensagem; -1 = sem prazo).
    ///
    /// Sao leituras PURAS da politica, que e' imutavel depois da criacao, e por
    /// isso podem ser chamadas de FORA do ator -- e precisam ser: quem escreve
    /// o registro de colocacao e' a thread do publicador (D24), e ela tem de
    /// gravar os mesmos numeros que o ator vai guardar.
    function PrioridadeEfetiva(APriority: Byte): Byte;
    function TtlEfetivo(AMessageTtlMs: Int64): Int64;
    /// Quem republica as mortas. nil = sem dead-letter (a mensagem e'
    /// simplesmente descartada, como na WS4). A engine injeta na criacao.
    property DeadLetterSink: IAMQPDeadLetterSink read FDeadLetterSink
      write FDeadLetterSink;
    property Policy: TAMQPQueuePolicy read FPolicy;

    /// Journal de durabilidade (Fase 4). nil = nada desta fila vai ao disco.
    /// A fila NAO e' dona; quem injeta e' a engine, na criacao.
    property Journal: TAMQPJournal read FJournal write FJournal;
    /// vhost em que esta fila mora -- entra nos registros de colocacao, porque
    /// a fila sozinha nao e' identidade (dois vhosts podem ter 'q').
    property VHostName: string read FVHostName write FVHostName;
    /// D20: so' fila DURAVEL escreve. Fila transiente nao toca o journal, e o
    /// caminho quente dela fica identico ao da Fase 3.
    property Durable: Boolean read FDurable write FDurable;
  end;

implementation

{ TAMQPServerConsumer }

constructor TAMQPServerConsumer.Create(const AConsumerTag: string;
  ANoAck, AExclusive: Boolean; const ATarget: IAMQPDeliveryTarget);
begin
  inherited Create;
  if ATarget = nil then
    raise EAMQPQueueActor.CreateFmt(
      'consumidor "%s" sem alvo de entrega', [AConsumerTag]);
  FConsumerTag := AConsumerTag;
  FChannelId := ATarget.ChannelId;
  FNoAck := ANoAck;
  FExclusive := AExclusive;
  FTarget := ATarget;
end;

{ TAMQPQueueCommand }

constructor TAMQPQueueCommand.Create(AKind: TAMQPQueueCmdKind; ASync: Boolean);
begin
  inherited Create;
  FRefCount := 1;
  FKind := AKind;
  if ASync then
    FEvent := TEvent.Create(nil, True, False, ''); // manual-reset
end;

destructor TAMQPQueueCommand.Destroy;
begin
  // Caminho de descarte (comando que nunca rodou, ou sincrono que estourou o
  // timeout depois de o ator ter preenchido o resultado): as referencias de
  // mensagem que sobraram sao nossas e tem de sair daqui.
  if Msg <> nil then
    Msg.Release;
  if ResMsg <> nil then
    ResMsg.Release;
  Consumer.Free;
  FEvent.Free;
  inherited;
end;

function TAMQPQueueCommand.GetIsSync: Boolean;
begin
  Result := FEvent <> nil;
end;

function TAMQPQueueCommand.AddRef: Integer;
begin
  Result := AmqpAtomicInc(FRefCount);
end;

function TAMQPQueueCommand.Release: Integer;
begin
  Result := AmqpAtomicDec(FRefCount);
  if Result = 0 then
    Free;
end;

procedure TAMQPQueueCommand.Signal;
begin
  if FEvent <> nil then
    FEvent.SetEvent;
end;

function TAMQPQueueCommand.WaitFor(ATimeoutMs: Cardinal): Boolean;
begin
  Result := (FEvent <> nil) and (FEvent.WaitFor(ATimeoutMs) = wrSignaled);
end;

{ TAMQPQueueWork }

constructor TAMQPQueueWork.Create(AQueue: TAMQPServerQueue);
begin
  inherited Create;
  FQueue := AQueue;
end;

procedure TAMQPQueueWork.Execute;
begin
  FQueue.RunActor;
end;

{ TAMQPServerQueue }

constructor TAMQPServerQueue.Create(const AName: string; AMaxLength: Integer;
  APool: TAMQPThreadPool);
begin
  Create(AName, TAMQPQueuePolicy.Empty, AMaxLength, APool);
end;

constructor TAMQPServerQueue.Create(const AName: string;
  const APolicy: TAMQPQueuePolicy; AMaxLength: Integer;
  APool: TAMQPThreadPool);
var
  I: Integer;
begin
  inherited Create;
  FName := AName;
  FPolicy := APolicy;
  // MaxPriority ausente (-1) vira 0: um balde so', que e' a fila de sempre.
  if APolicy.MaxPriority > 0 then
    FMaxPriority := APolicy.MaxPriority
  else
    FMaxPriority := 0;
  // COMPOSICAO DOS TETOS (WS6): o teto global do broker
  // (TAMQPServer.MaxQueueLength, decisao D7) e o x-max-length da FILA sao os
  // dois um limite superior, entao o MENOR vence. A razao e' de papel, nao de
  // aritmetica: o global e' uma rede de seguranca do PROCESSO HOSPEDEIRO -- o
  // broker roda dentro da app do usuario --, e um argumento vindo do cliente
  // nao pode afrouxar uma rede de seguranca. 0/ausente = sem teto daquele
  // lado, e ai' vale o outro.
  if AMaxLength < 0 then
    AMaxLength := 0;
  FMaxLength := AMaxLength;
  if APolicy.MaxLength >= 0 then
    if (FMaxLength = 0) or (APolicy.MaxLength < FMaxLength) then
      FMaxLength := Integer(APolicy.MaxLength);
  // Nao ha teto global de BYTES: x-max-length-bytes so' vem da fila.
  FMaxBytes := APolicy.MaxLengthBytes;
  if APool <> nil then
    FPool := APool
  else
    FPool := AmqpPool;
  FMon := TAMQPMonitor.Create;
  FMailbox := TQueue<TAMQPQueueCommand>.Create;
  FUltimoUso := NowTick; // a fila nasce "usada agora"
  SetLength(FStock, FMaxPriority + 1);
  SetLength(FRequeued, FMaxPriority + 1);
  for I := 0 to FMaxPriority do
  begin
    FStock[I] := TQueue<TAMQPQueueEntry>.Create;
    FRequeued[I] := TQueue<TAMQPQueueEntry>.Create;
  end;
  FUnacked := TList<TAMQPUnackedEntry>.Create;
  FConsumers := TObjectList<TAMQPServerConsumer>.Create(True);
end;

destructor TAMQPServerQueue.Destroy;
var
  I: Integer;
begin
  try
    Stop;
  except
    // Ator preso: o Stop ja nao liberou nada, de proposito. Vazar e' o menos
    // pior -- liberar as estruturas sob os pes de um ator rodando seria
    // exatamente o double-free que a Fase 1 ja pagou uma vez. E excecao nao
    // escapa de destrutor.
    on E: Exception do
      ;
  end;
  FMailbox.Free;
  for I := 0 to High(FStock) do
  begin
    FStock[I].Free;
    FRequeued[I].Free;
  end;
  FUnacked.Free;
  FConsumers.Free;
  FMon.Free;
  inherited;
end;

procedure TAMQPServerQueue.ActorRoundEnding;
begin
  // no-op: ver o comentario da declaracao.
end;

{ --- caixa de comandos --- }

// Sob FMon. Agenda uma rodada se ainda nao ha uma agendada.
procedure TAMQPServerQueue.ScheduleLocked;
begin
  if FScheduled or FStopping then
    Exit;
  FScheduled := True;
  FPool.Queue(TAMQPQueueWork.Create(Self));
end;

// Assincrono: enfileira e devolve. A referencia do chamador vira a da caixa.
procedure TAMQPServerQueue.Post(ACmd: TAMQPQueueCommand);
var
  LDescartar: Boolean;
begin
  LDescartar := False;
  FMon.Enter;
  try
    if FStopping then
    begin
      // Fila parada/apagada: descarta em silencio, mas CONTA (nada some sem
      // metrica). O ator nao roda mais, entao ninguem concorre com esta
      // escrita feita sob o lock.
      if ACmd.Kind = amqqcEnqueue then
        Inc(FDropped);
      LDescartar := True;
    end
    else
    begin
      FMailbox.Enqueue(ACmd);
      ScheduleLocked;
    end;
  finally
    FMon.Leave;
  end;
  if LDescartar then
    ACmd.Release; // fora do lock: o destrutor solta mensagem/consumidor
end;

// Sincrono: enfileira, espera o evento e devolve com o resultado preenchido.
// A referencia do CHAMADOR continua com ele (a caixa ganha a sua).
procedure TAMQPServerQueue.PostSync(ACmd: TAMQPQueueCommand);
begin
  FMon.Enter;
  try
    if (FActorThread <> 0) and (FActorThread = TThread.CurrentThread.ThreadID) then
      raise EAMQPQueueActor.CreateFmt(
        'comando sincrono (%d) postado de dentro do proprio ator da fila "%s"'
        + ' -- o ator esperaria por si mesmo', [Ord(ACmd.Kind), FName]);
    if FStopping then
      raise EAMQPQueueActor.CreateFmt(
        'comando sincrono (%d) numa fila ja parada ("%s")',
        [Ord(ACmd.Kind), FName]);
    ACmd.AddRef; // a caixa tem a sua propria referencia
    FMailbox.Enqueue(ACmd);
    ScheduleLocked;
  finally
    FMon.Leave;
  end;

  if not ACmd.WaitFor(AMQP_QUEUE_SYNC_TIMEOUT_MS) then
    raise EAMQPQueueActor.CreateFmt(
      'timeout de %d ms no comando sincrono (%d) da fila "%s" -- o ator nao'
      + ' foi agendado; verifique saturacao do pool de threads',
      [AMQP_QUEUE_SYNC_TIMEOUT_MS, Ord(ACmd.Kind), FName]);
end;

// Uma rodada do ator, num worker do pool.
procedure TAMQPServerQueue.RunActor;
var
  LCmd: TAMQPQueueCommand;
  LCount: Integer;
begin
  FMon.Enter;
  FActorThread := TThread.CurrentThread.ThreadID;
  FMon.Leave;
  try
    LCount := 0;
    while True do
    begin
      LCmd := nil;
      FMon.Enter;
      if (not FStopping) and (LCount < AMQP_QUEUE_CMD_BATCH)
        and (FMailbox.Count > 0) then
        LCmd := FMailbox.Dequeue;
      FMon.Leave;
      if LCmd = nil then
        Break;
      try
        try
          ExecuteCommand(LCmd);
        except
          // Um comando nunca deveria levantar; se levantar, quem espera do
          // outro lado nao pode ficar parado para sempre -- sinalizamos assim
          // mesmo, com ResOk=False, e seguimos.
          on E: Exception do
            LCmd.ResOk := False;
        end;
      finally
        LCmd.Signal;
        LCmd.Release; // a referencia da caixa
      end;
      Inc(LCount);
    end;
  finally
    try
      ActorRoundEnding; // seam de teste (no-op em producao)
    except
      on E: Exception do
        ; // um hook que levante nao pode deixar FScheduled preso
    end;
    FMon.Enter;
    try
      FActorThread := 0;
      if (not FStopping) and (FMailbox.Count > 0) then
        // Teto da rodada estourado (ou comando chegou na janela): continua
        // agendado, so' troca de work item -- FScheduled segue True.
        FPool.Queue(TAMQPQueueWork.Create(Self))
      else
      begin
        FScheduled := False;
        FMon.PulseAll; // o Stop espera exatamente por isto
      end;
    finally
      FMon.Leave;
    end;
  end;
end;

procedure TAMQPServerQueue.Stop;
var
  LDeadline: UInt64;
begin
  FMon.Enter;
  try
    if FStopped then
      Exit;
    FStopping := True; // ninguem mais posta nem agenda
    LDeadline := AmqpTickMs + AMQP_QUEUE_STOP_TIMEOUT_MS;
    while FScheduled and (AmqpTickMs < LDeadline) do
      FMon.Wait(100);
    if FScheduled then
      raise EAMQPQueueActor.CreateFmt(
        'ator da fila "%s" nao terminou em %d ms -- estado NAO liberado',
        [FName, AMQP_QUEUE_STOP_TIMEOUT_MS]);
    // Daqui pra baixo somos o unico executor: o ator saiu do laco e nenhum
    // agendamento novo e' possivel.
    DrainState;
    FStopped := True;
  finally
    FMon.Leave;
  end;
end;

// Libera tudo. So' o Stop chama, com o ator ja parado.
procedure TAMQPServerQueue.DrainState;
var
  LCmd: TAMQPQueueCommand;
  LEntry: TAMQPQueueEntry;
  I: Integer;
begin
  while FMailbox.Count > 0 do
  begin
    LCmd := FMailbox.Dequeue;
    LCmd.Signal; // solta quem estiver esperando (vai ver ResOk=False)
    LCmd.Release;
  end;
  for I := 0 to High(FStock) do
  begin
    while FRequeued[I].Count > 0 do
    begin
      LEntry := FRequeued[I].Dequeue;
      LEntry.Msg.Release;
    end;
    while FStock[I].Count > 0 do
    begin
      LEntry := FStock[I].Dequeue;
      LEntry.Msg.Release;
    end;
  end;
  FBytesProntos := 0;
  for I := 0 to FUnacked.Count - 1 do
    FUnacked[I].Msg.Release;
  FUnacked.Clear;
  FConsumers.Clear; // OwnsObjects
end;

{ --- API assincrona --- }

procedure TAMQPServerQueue.PostMessage(AMessage: TAMQPMessage;
  APriority: Byte; AMessageTtlMs: Int64; AJournalId, AContentId: UInt64;
  ARecuperada: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcEnqueue, False);
  AMessage.AddRef; // a referencia da FILA; o publicador continua com a dele
  LCmd.Msg := AMessage;
  LCmd.Priority := APriority;
  LCmd.MessageTtlMs := AMessageTtlMs;
  LCmd.JournalId := AJournalId;
  LCmd.ContentId := AContentId;
  LCmd.Recuperada := ARecuperada;
  Post(LCmd);
end;

function TAMQPServerQueue.TryPostMessage(AMessage: TAMQPMessage;
  APriority: Byte; AMessageTtlMs: Int64;
  AJournalId, AContentId: UInt64): Boolean;
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcEnqueueSync, True);
  try
    AMessage.AddRef; // a referencia da FILA, como no PostMessage
    LCmd.Msg := AMessage;
    LCmd.Priority := APriority;
    LCmd.MessageTtlMs := AMessageTtlMs;
    LCmd.JournalId := AJournalId;
    LCmd.ContentId := AContentId;
    PostSync(LCmd);
    Result := LCmd.ResOk;
  finally
    // Se o ator recusou, a referencia que este comando carregava continua nele
    // e sai no destrutor -- por isso o Release do comando basta.
    LCmd.Release;
  end;
end;

procedure TAMQPServerQueue.PostAddConsumer(AConsumer: TAMQPServerConsumer);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcAddConsumer, False);
  LCmd.Consumer := AConsumer;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostRemoveConsumer(AChannelId: NativeUInt;
  const AConsumerTag: string);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcRemoveConsumer, False);
  LCmd.ChannelId := AChannelId;
  LCmd.ConsumerTag := AConsumerTag;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostRemoveChannel(AChannelId: NativeUInt);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcRemoveChannel, False);
  LCmd.ChannelId := AChannelId;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostTouch;
begin
  Post(TAMQPQueueCommand.Create(amqqcTouch, False));
end;

procedure TAMQPServerQueue.PostDeliverTick;
begin
  Post(TAMQPQueueCommand.Create(amqqcDeliverTick, False));
end;

procedure TAMQPServerQueue.PostAck(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; AMultiple: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcAck, False);
  LCmd.ChannelId := AChannelId;
  LCmd.DeliveryTag := ADeliveryTag;
  LCmd.Multiple := AMultiple;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostNack(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; AMultiple, ARequeue: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcNack, False);
  LCmd.ChannelId := AChannelId;
  LCmd.DeliveryTag := ADeliveryTag;
  LCmd.Multiple := AMultiple;
  LCmd.Requeue := ARequeue;
  Post(LCmd);
end;

procedure TAMQPServerQueue.PostReject(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; ARequeue: Boolean);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcReject, False);
  LCmd.ChannelId := AChannelId;
  LCmd.DeliveryTag := ADeliveryTag;
  LCmd.Requeue := ARequeue;
  Post(LCmd);
end;

{ --- API sincrona --- }

function TAMQPServerQueue.Get(AChannelId: NativeUInt; ADeliveryTag: UInt64;
  ANoAck: Boolean; out AMessage: TAMQPMessage;
  out ARedelivered: Boolean): Boolean;
var
  LCmd: TAMQPQueueCommand;
begin
  AMessage := nil;
  ARedelivered := False;
  LCmd := TAMQPQueueCommand.Create(amqqcGet, True);
  try
    LCmd.ChannelId := AChannelId;
    LCmd.DeliveryTag := ADeliveryTag;
    LCmd.NoAck := ANoAck;
    PostSync(LCmd);
    Result := LCmd.ResOk;
    AMessage := LCmd.ResMsg;
    LCmd.ResMsg := nil; // a referencia passa a ser do chamador
    ARedelivered := LCmd.ResRedelivered;
  finally
    LCmd.Release;
  end;
end;

function TAMQPServerQueue.Purge: Integer;
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcPurge, True);
  try
    PostSync(LCmd);
    Result := LCmd.ResCount;
  finally
    LCmd.Release;
  end;
end;

function TAMQPServerQueue.Stats: TAMQPQueueStats;
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcStats, True);
  try
    PostSync(LCmd);
    Result := LCmd.ResStats;
  finally
    LCmd.Release;
  end;
end;

function TAMQPServerQueue.Delete(AIfUnused, AIfEmpty: Boolean;
  out AMessageCount: Integer): TAMQPQueueDeleteResult;
var
  LCmd: TAMQPQueueCommand;
begin
  AMessageCount := 0;
  LCmd := TAMQPQueueCommand.Create(amqqcDelete, True);
  try
    LCmd.IfUnused := AIfUnused;
    LCmd.IfEmpty := AIfEmpty;
    PostSync(LCmd);
    Result := LCmd.ResDelete;
    AMessageCount := LCmd.ResCount;
  finally
    LCmd.Release;
  end;
  if Result = amqqdOk then
    Stop; // fila apagada nao volta a atender
end;

{ --- estado: helpers do ator --- }

function TAMQPServerQueue.ReadyCount: Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := 0 to High(FStock) do
    Inc(Result, FRequeued[I].Count + FStock[I].Count);
end;

// Tira a proxima pronta: do balde de MAIOR prioridade que tenha algo, e
// dentro dele as devolvidas (requeue) antes, na ordem original. FIFO dentro do
// nivel e' invariante testada -- prioridade reordena entre niveis, nunca
// dentro de um.
function TAMQPServerQueue.TakeReady(out AEntry: TAMQPQueueEntry): Boolean;
var
  I: Integer;
begin
  for I := High(FStock) downto 0 do
  begin
    if FRequeued[I].Count > 0 then
      AEntry := FRequeued[I].Dequeue
    else if FStock[I].Count > 0 then
      AEntry := FStock[I].Dequeue
    else
      Continue;
    ContaBytes(AEntry, -1); // TakeReady e' o caminho comum de saida
    Exit(True);
  end;
  Result := False;
end;

// Espia a proxima pronta SEM tirar do estoque -- a entrega so' tira depois
// de o alvo aceitar o lote de frames; se ele recusar, a mensagem tem de
// continuar exatamente onde estava. Mesma ordem do TakeReady.
function TAMQPServerQueue.PeekReady(out AEntry: TAMQPQueueEntry): Boolean;
var
  I: Integer;
begin
  for I := High(FStock) downto 0 do
  begin
    if FRequeued[I].Count > 0 then
      AEntry := FRequeued[I].Peek
    else if FStock[I].Count > 0 then
      AEntry := FStock[I].Peek
    else
      Continue;
    Exit(True);
  end;
  Result := False;
end;

function TAMQPServerQueue.NowTick: UInt64;
begin
  Result := AmqpTickMs;
end;

function TAMQPServerQueue.LimitaPrioridade(APriority: Byte): Byte;
begin
  if APriority > FMaxPriority then
    Result := Byte(FMaxPriority)
  else
    Result := APriority;
end;

// Combina o TTL da FILA (x-message-ttl) com o da MENSAGEM ('expiration'): o
// MENOR vence, que e' o que a spec de fato do RabbitMQ faz. Ausentes os dois,
// a mensagem nao vence nunca (0).
function TAMQPServerQueue.TtlEfetivo(AMessageTtlMs: Int64): Int64;
begin
  Result := -1;
  if FPolicy.MessageTtlMs >= 0 then
    Result := FPolicy.MessageTtlMs;
  if AMessageTtlMs >= 0 then
    if (Result < 0) or (AMessageTtlMs < Result) then
      Result := AMessageTtlMs;
end;

function TAMQPServerQueue.PrioridadeEfetiva(APriority: Byte): Byte;
begin
  Result := LimitaPrioridade(APriority);
end;

function TAMQPServerQueue.CalculaPrazo(AMessageTtlMs: Int64): UInt64;
var
  LTtl: Int64;
begin
  // Uma implementacao so' da regra do "menor vence": o registro que a thread do
  // publicador grava tem de dizer EXATAMENTE o que o ator vai guardar, e duas
  // copias da regra seriam duas chances de divergir.
  LTtl := TtlEfetivo(AMessageTtlMs);
  if LTtl < 0 then
    Exit(0);
  Result := NowTick + UInt64(LTtl);
end;

// Ponto UNICO de saida de uma mensagem vencida. Na WS4 so' larga a referencia
// e conta; a WS5 (dead-lettering) troca o corpo daqui por "reescreve o header
// com x-death e republica no DLX". Existe como metodo proprio justamente para
// essa troca ser local.
procedure TAMQPServerQueue.DescartaExpirada(AEntry: TAMQPQueueEntry);
begin
  Inc(FExpired);
  MorreCom(AEntry, AMQP_DEATH_EXPIRED);
end;

// Roda NA THREAD DO ATOR, e por isso e' so' CPU (reescrever o header) mais um
// post na caixa de outra fila (D17). Nada aqui espera I/O.
function TAMQPServerQueue.MorreCom(AEntry: TAMQPQueueEntry;
  const ARazao: string): Boolean;
var
  LEd: TAMQPHeaderEditor;
  LNova: TAMQPMessage;
  LDlx, LChave, LExpOriginal: string;
  LPayload: TBytes;
begin
  Result := False;
  // O try/FINALLY externo existe por uma razao especifica: `Exit` dentro de um
  // try/EXCEPT sai da FUNCAO e pula o codigo depois do bloco. A primeira
  // versao daqui liberava a referencia DEPOIS de um try/except que tinha Exit
  // no caminho "sem DLX" -- e nesse caminho o Release nunca rodava. Um teste
  // da WS3 pegou (Nack_SemRequeue_Descarta) e o heaptrc confirmou com 21
  // blocos vazados.
  try
    try
      if (FDeadLetterSink = nil) or (not FPolicy.HasDeadLetterExchange) then
        Exit;

      LEd := nil;
      LNova := nil;
      try
        LEd := TAMQPHeaderEditor.Create(AEntry.Msg.HeaderPayload);

        // Guarda de ciclo (D14) ANTES de mexer em qualquer coisa: se ja' deu
        // voltas demais, a mensagem morre aqui e nao vira trafego novo.
        if AmqpDeathHops(LEd.Headers) >= AMQP_MAX_DEADLETTER_HOPS then
          Exit;

        // A mensagem morta por TTL nao pode levar o MESMO prazo para a DLQ:
        // morreria no ato e o padrao retry-com-espera nao funcionaria. O valor
        // antigo fica registrado na entrada de x-death.
        LExpOriginal := LEd.TakeExpiration;
        AmqpAddDeath(LEd, FName, ARazao, AEntry.Msg.Exchange,
          AEntry.Msg.RoutingKey, LExpOriginal);
        LPayload := LEd.BuildPayload(AEntry.Msg.BodySize);

        LDlx := FPolicy.DeadLetterExchange;
        // Sem x-dead-letter-routing-key, a chave ORIGINAL e' mantida -- e' o
        // que faz um DLX topic continuar roteando pelo mesmo criterio.
        if FPolicy.HasDeadLetterRoutingKey then
          LChave := FPolicy.DeadLetterRoutingKey
        else
          LChave := AEntry.Msg.RoutingKey;

        LNova := AmqpDeriveMessage(AEntry.Msg, LPayload, LDlx, LChave);
        FDeadLetterSink.DeadLetter(LDlx, LChave, LNova, AEntry.Priority,
          AEntry.ContentId);
        Inc(FDeadLettered);
        Result := True;
      finally
        if LNova <> nil then
          LNova.Release; // a referencia do derivador; o sink tirou a dele
        LEd.Free;
      end;
    except
      // Header malformado, DLX que sumiu, o que for: uma morte que nao
      // consegue ser republicada vira descarte. NAO pode derrubar o ator --
      // levantar aqui mataria a rodada e deixaria a fila meio processada.
      on E: Exception do
        Result := False;
    end;
  finally
    // D23, ORDEM: a colocacao nova ja' foi escrita la' dentro (pelo sink, que
    // roda nesta mesma thread), e so' AGORA a velha e' aposentada. Falhar no
    // meio produz DUPLICATA, nunca perda -- e duplicata o at-least-once ja'
    // admite. Vale para os tres motivos de morte, porque todos passam por aqui.
    Aposenta(AEntry.JournalId);
    // A referencia da entrada e' SEMPRE consumida, tenha republicado ou nao.
    AEntry.Msg.Release;
  end;
end;

// Vence o que passou do prazo, DA CABECA de cada balde para tras.
//
// LIMITACAO DELIBERADA, a mesma do RabbitMQ: com 'expiration' POR MENSAGEM as
// entradas nao ficam em ordem de vencimento, entao uma mensagem de prazo curto
// atras de uma de prazo longo so' e' recolhida quando chega a' cabeca. Varrer
// a fila inteira a cada tick seria O(n) por tick para consertar um caso raro.
// Com x-message-ttl da FILA (o caso comum) nao ha problema nenhum: todas tem o
// mesmo TTL e a ordem FIFO ja' e' a ordem de vencimento.
procedure TAMQPServerQueue.ExpiraVencidas;
var
  I: Integer;
  LAgora: UInt64;
  LEntry: TAMQPQueueEntry;

  function VenceCabeca(AFila: TQueue<TAMQPQueueEntry>): Boolean;
  begin
    Result := (AFila.Count > 0) and (AFila.Peek.ExpiraEm > 0)
      and (AFila.Peek.ExpiraEm <= LAgora);
  end;

begin
  // D13: fila que nunca recebeu mensagem com prazo nao paga nada por isto.
  if not FTemPrazo then
    Exit;
  LAgora := NowTick;
  for I := 0 to High(FStock) do
  begin
    while VenceCabeca(FRequeued[I]) do
    begin
      LEntry := FRequeued[I].Dequeue;
      ContaBytes(LEntry, -1);
      DescartaExpirada(LEntry);
    end;
    while VenceCabeca(FStock[I]) do
    begin
      LEntry := FStock[I].Dequeue;
      ContaBytes(LEntry, -1);
      DescartaExpirada(LEntry);
    end;
  end;
end;

// Proximo consumidor do rodizio que ainda nao recusou nesta rodada.
// nil = todos recusaram (ou nao ha consumidor).
function TAMQPServerQueue.NextEligible: TAMQPServerConsumer;
var
  I, N, LIdx: Integer;
begin
  Result := nil;
  N := FConsumers.Count;
  if N = 0 then
    Exit;
  if (FRoundRobin < 0) or (FRoundRobin >= N) then
    FRoundRobin := 0;
  for I := 0 to N - 1 do
  begin
    LIdx := (FRoundRobin + I) mod N;
    if not FConsumers[LIdx].Suspended then
    begin
      Result := FConsumers[LIdx];
      // O ponteiro avanca mesmo quando este consumidor acaba recusando: quem
      // nao pode receber nao tem por que segurar a vez.
      FRoundRobin := (LIdx + 1) mod N;
      Exit;
    end;
  end;
end;

// Uma RODADA de entrega. Roda na thread do ator, fora do lock da caixa, e
// NUNCA bloqueia (D2): o alvo devolve False em vez de esperar, e o consumidor
// que recusou sai do rodizio ate' o fim desta rodada (D3) -- os outros
// continuam recebendo, que e' justamente o head-of-line que a D3 evita.
procedure TAMQPServerQueue.DeliverPending;
var
  I: Integer;
  LCons: TAMQPServerConsumer;
  LEntry: TAMQPQueueEntry;
  LUnacked: TAMQPUnackedEntry;
  LTag: UInt64;
begin
  // ANTES de qualquer coisa, e ANTES do early-exit por falta de consumidor:
  // uma fila sem consumidor nenhum tambem tem de vencer no prazo (D13), e e'
  // o tick da thread monitora que a acorda.
  ExpiraVencidas;

  // Canal/conexao que ja morreu nao volta: o consumidor sai aqui. As
  // nao-confirmadas dele sao devolvidas pelo amqqcRemoveChannel, que o
  // teardown da conexao posta (WS7).
  for I := FConsumers.Count - 1 downto 0 do
    if not FConsumers[I].Target.IsAlive then
      FConsumers.Delete(I);
  if FConsumers.Count = 0 then
    Exit;

  for I := 0 to FConsumers.Count - 1 do
    FConsumers[I].Suspended := False; // rodada nova: todos elegiveis de novo

  while ReadyCount > 0 do
  begin
    LCons := NextEligible;
    if LCons = nil then
      Break; // todos recusaram nesta rodada
    if not PeekReady(LEntry) then
      Break;

    if LCons.Target.TryDeliver(LCons.ConsumerTag, FName, LCons.NoAck,
      LEntry.Redelivered, LEntry.Msg, LTag) then
    begin
      TakeReady(LEntry); // so' sai do estoque depois de o lote ser aceito
      Inc(FDelivered);
      if LCons.NoAck then
      begin
        // Sem ack nao ha volta: a colocacao acaba aqui, no disco tambem.
        Aposenta(LEntry.JournalId);
        LEntry.Msg.Release; // a fila larga a referencia dela agora
      end
      else
      begin
        // A referencia do estoque passa para as nao-confirmadas.
        //
        // Priority e ExpiraEm TEM de vir junto: e' com eles que o requeue
        // devolve a mensagem ao balde certo e com o prazo original (D12/D13).
        // Faltavam aqui -- o caminho do Basic.Get ja' os copiava, este nao --,
        // e o registro local ficava com lixo de pilha. Efeito: toda mensagem
        // entregue a um CONSUMIDOR e depois devolvida (Nack/Reject com
        // requeue, ou queda do canal) voltava para um balde arbitrario e com um
        // prazo arbitrario. Ficou invisivel porque os testes de requeue usavam
        // Basic.Get, e porque fila sem prioridade tem um balde so' e fila sem
        // TTL nem varre prazo.
        LUnacked.ChannelId := LCons.Target.ChannelId;
        LUnacked.DeliveryTag := LTag;
        LUnacked.Msg := LEntry.Msg;
        LUnacked.Priority := LEntry.Priority;
        LUnacked.ExpiraEm := LEntry.ExpiraEm;
        LUnacked.JournalId := LEntry.JournalId;
        LUnacked.ContentId := LEntry.ContentId;
        FUnacked.Add(LUnacked);
      end;
    end
    else
      LCons.Suspended := True;
  end;
end;

// Devolve para a CABECA da fila, marcada como reentregue (spec: Basic.Nack/
// Reject com requeue devolvem a mensagem a' sua posicao original).
// Devolve a' CABECA DO PROPRIO BALDE, com o prazo original. Voltar para a
// cabeca absoluta faria uma mensagem de prioridade baixa furar a fila das
// altas; zerar o prazo daria sobrevida a quem ja' devia ter morrido.
procedure TAMQPServerQueue.RequeueFront(AMsg: TAMQPMessage; APriority: Byte;
  AExpiraEm: UInt64; AJournalId, AContentId: UInt64);
var
  LEntry: TAMQPQueueEntry;
begin
  LEntry.Msg := AMsg;
  LEntry.Redelivered := True;
  LEntry.Priority := LimitaPrioridade(APriority);
  LEntry.ExpiraEm := AExpiraEm;
  // A colocacao NAO e' aposentada num requeue: ela continua a mesma no disco,
  // so' voltou para o estoque. E' o que faz a D27 ("nao-confirmada volta
  // pronta") sair de graca no replay -- nao ha registro nenhum a escrever.
  LEntry.JournalId := AJournalId;
  LEntry.ContentId := AContentId;
  ContaBytes(LEntry, +1);
  FRequeued[LEntry.Priority].Enqueue(LEntry);
end;

// D7: teto global de memoria. Descarta da CABECA (a mais velha) -- o broker
// roda DENTRO da app do usuario e uma fila sem consumidor nao pode derrubar o
// processo hospedeiro. Conta so' as prontas (as nao-confirmadas ja estao com
// um consumidor).
// O acumulador de bytes existe porque somar os corpos a cada checagem seria
// O(n) por publish. O preco e' que TODA entrada e saida do estoque tem de
// passar por aqui -- um caminho esquecido nao da sintoma nenhum na hora, so'
// um teto que para de valer.
procedure TAMQPServerQueue.Aposenta(AJournalId: UInt64);
var
  LRecs: TAMQPJournalRecords;
  LDeq: TAMQPRecDequeue;
begin
  if (AJournalId = 0) or (FJournal = nil) or (not FDurable) then
    Exit;
  LDeq.VHost := FVHostName;
  LDeq.Queue := FName;
  LDeq.EntryId := AJournalId;
  SetLength(LRecs, 1);
  LRecs[0].Kind := AMQP_REC_DEQUEUE;
  LRecs[0].Payload := AmqpEncodeRecDequeue(LDeq);
  try
    // SubmitNoWait, nunca Submit: estamos NA thread do ator.
    FJournal.SubmitNoWait(LRecs);
  except
    // Journal em falha ja' avisou quem precisava (o sink da WS5). O ator nao
    // pode morrer por causa disso -- a fila em memoria continua correta.
    on E: Exception do
      ;
  end;
end;

procedure TAMQPServerQueue.ContaBytes(const AEntry: TAMQPQueueEntry;
  ASinal: Integer);
begin
  if AEntry.Msg <> nil then
    Inc(FBytesProntos, Int64(ASinal) * AEntry.Msg.BodySize);
end;

function TAMQPServerQueue.EstaCheia: Boolean;
begin
  Result := ((FMaxLength > 0) and (ReadyCount >= FMaxLength))
    or ((FMaxBytes >= 0) and (FBytesProntos >= FMaxBytes));
end;

procedure TAMQPServerQueue.EnforceMaxLength;
var
  LEntry: TAMQPQueueEntry;
  I: Integer;
  LTirou: Boolean;
begin
  if (FMaxLength <= 0) and (FMaxBytes < 0) then
    Exit;
  // Sai enquanto QUALQUER um dos dois tetos estiver estourado. O teste e' por
  // ">" e nao ">=": o teto e' o que a fila PODE guardar, entao ela so' descarta
  // depois de passar dele.
  while ((FMaxLength > 0) and (ReadyCount > FMaxLength))
    or ((FMaxBytes >= 0) and (FBytesProntos > FMaxBytes)) do
  begin
    // D12: comeca pelo balde de MENOR prioridade. Descartar da cabeca absoluta
    // (que e' a de MAIOR prioridade) jogaria fora justamente o que o cliente
    // marcou como mais importante. Dentro do balde, a cabeca -- a mais velha.
    LTirou := False;
    for I := 0 to High(FStock) do
    begin
      if FRequeued[I].Count > 0 then
        LEntry := FRequeued[I].Dequeue
      else if FStock[I].Count > 0 then
        LEntry := FStock[I].Dequeue
      else
        Continue;
      ContaBytes(LEntry, -1);
      Inc(FDropped);
      // 'maxlen': o descarte por teto tambem e' morte, e vai para o DLX como
      // as outras. MorreCom consome a referencia.
      MorreCom(LEntry, AMQP_DEATH_MAXLEN);
      LTirou := True;
      Break;
    end;
    if not LTirou then
      Break; // nao ha o que descartar (so' nao-confirmadas)
  end;
end;

// Ack (ARequeue=False), Nack/Reject com ou sem requeue. AMultiple: todas as
// tags <= ADeliveryTag DAQUELE canal.
procedure TAMQPServerQueue.ResolveUnacked(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; AMultiple, ARequeue, ARejeitada: Boolean);
var
  I: Integer;
  LEntry: TAMQPUnackedEntry;
  LQueueEntry: TAMQPQueueEntry;
  LCasou: Boolean;
begin
  I := 0;
  while I < FUnacked.Count do
  begin
    LEntry := FUnacked[I];
    if AMultiple then
      LCasou := (LEntry.ChannelId = AChannelId)
        and (LEntry.DeliveryTag <= ADeliveryTag)
    else
      LCasou := (LEntry.ChannelId = AChannelId)
        and (LEntry.DeliveryTag = ADeliveryTag);
    if LCasou then
    begin
      FUnacked.Delete(I);
      if ARequeue then
        RequeueFront(LEntry.Msg, LEntry.Priority, LEntry.ExpiraEm,
          LEntry.JournalId, LEntry.ContentId)
      else if ARejeitada then
      begin
        // 'rejected': nack/reject SEM requeue. O ack cai no else abaixo --
        // confundir os dois mandaria toda mensagem confirmada para o DLX.
        LQueueEntry.Msg := LEntry.Msg;
        LQueueEntry.Redelivered := True;
        LQueueEntry.Priority := LEntry.Priority;
        LQueueEntry.ExpiraEm := LEntry.ExpiraEm;
        LQueueEntry.JournalId := LEntry.JournalId;
        LQueueEntry.ContentId := LEntry.ContentId;
        MorreCom(LQueueEntry, AMQP_DEATH_REJECTED);
      end
      else
      begin
        // ACK: fim de linha. A colocacao sai do disco aqui -- e' o caso comum,
        // e o unico em que a mensagem some sem ter morrido.
        Aposenta(LEntry.JournalId);
        LEntry.Msg.Release;
      end;
    end
    else
      Inc(I);
  end;
  if ARequeue then
    EnforceMaxLength;
end;

function TAMQPServerQueue.CurrentStats: TAMQPQueueStats;
begin
  Result.MessageCount := ReadyCount;
  Result.ConsumerCount := FConsumers.Count;
  Result.UnackedCount := FUnacked.Count;
  Result.DroppedCount := FDropped;
  Result.DeliveredCount := FDelivered;
  Result.ExpiredCount := FExpired;
  Result.DeadLetteredCount := FDeadLettered;
  if FConsumers.Count > 0 then
    Result.IdleMs := 0
  else
    Result.IdleMs := Int64(NowTick - FUltimoUso);
  Result.EverHadConsumer := FEverHadConsumer;
end;

{ --- o despacho de comandos, na thread do ator --- }

procedure TAMQPServerQueue.ExecuteCommand(ACmd: TAMQPQueueCommand);
var
  LEntry: TAMQPQueueEntry;
  LUnacked: TAMQPUnackedEntry;
  I, LCount: Integer;
begin
  case ACmd.Kind of
    amqqcEnqueue:
      begin
        LEntry.Msg := ACmd.Msg;
        LEntry.Redelivered := ACmd.Recuperada; // D27
        LEntry.Priority := LimitaPrioridade(ACmd.Priority);
        LEntry.ExpiraEm := CalculaPrazo(ACmd.MessageTtlMs);
        LEntry.JournalId := ACmd.JournalId;
        LEntry.ContentId := ACmd.ContentId;
        if LEntry.ExpiraEm > 0 then
          FTemPrazo := True;
        ACmd.Msg := nil; // a referencia passou para o estoque
        ContaBytes(LEntry, +1);
        FStock[LEntry.Priority].Enqueue(LEntry);
        // A ORDEM importa: expira antes de aplicar o teto, senao uma mensagem
        // ja vencida ocuparia vaga e faria descartar uma viva.
        ExpiraVencidas;
        EnforceMaxLength;
      end;

    amqqcEnqueueSync:
      begin
        // Expira ANTES de decidir: uma vencida ocupando vaga faria recusar um
        // publish vivo -- mesmo motivo da ordem no enqueue assincrono.
        ExpiraVencidas;
        if EstaCheia then
        begin
          // A referencia fica no comando e sai no destrutor dele.
          ACmd.ResOk := False;
          // COMPENSACAO. O registro de colocacao ja' foi escrito pela thread do
          // publicador ANTES de chegar aqui (D24) -- e' ela quem conhece o LSN.
          // Se a fila recusa (x-overflow reject-publish, D15), a colocacao
          // precisa ser aposentada, senao a recuperacao ressuscitaria uma
          // mensagem que o broker RECUSOU. Escrever o DEQ e' mais simples e mais
          // seguro que adiar o ENQ: adiar abriria a janela de a mensagem ser
          // consumida e aposentada ANTES de existir no log.
          Aposenta(ACmd.JournalId);
        end
        else
        begin
          LEntry.Msg := ACmd.Msg;
          LEntry.Redelivered := False;
          LEntry.Priority := LimitaPrioridade(ACmd.Priority);
          LEntry.ExpiraEm := CalculaPrazo(ACmd.MessageTtlMs);
          LEntry.JournalId := ACmd.JournalId;
          LEntry.ContentId := ACmd.ContentId;
          if LEntry.ExpiraEm > 0 then
            FTemPrazo := True;
          ACmd.Msg := nil;
          ContaBytes(LEntry, +1);
          FStock[LEntry.Priority].Enqueue(LEntry);
          ACmd.ResOk := True;
        end;
      end;

    amqqcTouch:
      FUltimoUso := NowTick;

    amqqcAddConsumer:
      begin
        FUltimoUso := NowTick;
        FConsumers.Add(ACmd.Consumer);
        ACmd.Consumer := nil; // a fila e' dona agora
        FEverHadConsumer := True;
      end;

    amqqcRemoveConsumer:
      begin
        // Sair TAMBEM e' uso: e' daqui que o relogio do x-expires comeca a
        // contar, e nao do instante em que o consumidor entrou.
        FUltimoUso := NowTick;
        for I := FConsumers.Count - 1 downto 0 do
          if (FConsumers[I].ChannelId = ACmd.ChannelId)
            and (FConsumers[I].ConsumerTag = ACmd.ConsumerTag) then
            FConsumers.Delete(I);
      end;

    amqqcRemoveChannel:
      begin
        for I := FConsumers.Count - 1 downto 0 do
          if FConsumers[I].ChannelId = ACmd.ChannelId then
            FConsumers.Delete(I);
        // Nao-confirmadas de um canal morto voltam para a fila: o cliente
        // nunca as confirmou, entao a entrega tem de acontecer de novo.
        I := 0;
        while I < FUnacked.Count do
        begin
          LUnacked := FUnacked[I];
          if LUnacked.ChannelId = ACmd.ChannelId then
          begin
            FUnacked.Delete(I);
            RequeueFront(LUnacked.Msg, LUnacked.Priority, LUnacked.ExpiraEm,
              LUnacked.JournalId, LUnacked.ContentId);
          end
          else
            Inc(I);
        end;
        EnforceMaxLength;
      end;

    amqqcAck: // ack NAO e' morte: ARejeitada=False
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, ACmd.Multiple,
        False, False);

    amqqcNack:
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, ACmd.Multiple,
        ACmd.Requeue, not ACmd.Requeue);

    amqqcReject: // Basic.Reject nao tem 'multiple'
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, False, ACmd.Requeue,
        not ACmd.Requeue);

    amqqcGet:
      begin
      FUltimoUso := NowTick; // Basic.Get e' uso (spec de fato do RabbitMQ)
      // Um Basic.Get nao pode devolver mensagem ja vencida -- a expiracao
      // preguicosa roda no caminho de entrega, e o Get e' o outro caminho.
      ExpiraVencidas;
      if TakeReady(LEntry) then
      begin
        ACmd.ResOk := True;
        ACmd.ResRedelivered := LEntry.Redelivered;
        ACmd.ResMsg := LEntry.Msg;
        if not ACmd.NoAck then
        begin
          // O chamador leva UMA referencia e a fila guarda a dela nas
          // nao-confirmadas.
          LEntry.Msg.AddRef;
          LUnacked.ChannelId := ACmd.ChannelId;
          LUnacked.DeliveryTag := ACmd.DeliveryTag;
          LUnacked.Msg := LEntry.Msg;
          LUnacked.Priority := LEntry.Priority;
          LUnacked.ExpiraEm := LEntry.ExpiraEm;
          LUnacked.JournalId := LEntry.JournalId;
          LUnacked.ContentId := LEntry.ContentId;
          FUnacked.Add(LUnacked);
        end
        else
          // Get em no-ack: a colocacao acaba aqui, como na entrega sem ack.
          Aposenta(LEntry.JournalId);
        // Com no-ack a referencia do estoque simplesmente passa ao chamador.
      end
      else
        ACmd.ResOk := False;
      end;

    amqqcPurge:
      begin
        LCount := 0;
        while TakeReady(LEntry) do
        begin
          Aposenta(LEntry.JournalId);
          LEntry.Msg.Release; // purge nao e' descarte por limite: nao conta
          Inc(LCount);
        end;
        ACmd.ResOk := True;
        ACmd.ResCount := LCount;
      end;

    amqqcDelete:
      begin
        if ACmd.IfUnused and (FConsumers.Count > 0) then
          ACmd.ResDelete := amqqdEmUso
        else if ACmd.IfEmpty and (ReadyCount > 0) then
          ACmd.ResDelete := amqqdNaoVazia
        else
        begin
          ACmd.ResCount := ReadyCount;
          ACmd.ResDelete := amqqdOk;
          // Avisa quem estava consumindo ANTES de o estado ir embora: a fila
          // sumiu debaixo deles e o cliente precisa saber (spec 1.7.2.9 --
          // "the server MUST cancel all consumers"). Best-effort e sem
          // bloquear: a fila esta' sendo destruida, nao ha retentativa.
          for I := 0 to FConsumers.Count - 1 do
            try
              FConsumers[I].Target.TryCancel(FConsumers[I].ConsumerTag);
            except
              on E: Exception do
                ; // canal morrendo junto: nao atrapalha o delete
            end;
          // O estado sai no DrainState do Stop que o Delete dispara --
          // liberar aqui e depois de novo la' seria double-free.
        end;
        ACmd.ResOk := ACmd.ResDelete = amqqdOk;
      end;

    amqqcStats:
      begin
        ACmd.ResStats := CurrentStats;
        ACmd.ResOk := True;
      end;

    amqqcDeliverTick:
      ; // so' existe para disparar a rodada de entrega abaixo
  end;

  // Uma rodada de entrega depois de todo comando que pode ter DESTRAVADO
  // entrega: mensagem nova, consumidor novo, ack/nack/reject (que libera
  // prefetch ou devolve mensagem) e o tick do caso degradado da D3. Get,
  // Purge, Stats, Delete e a saida de um consumidor nunca criam oportunidade
  // de entrega, entao nem tentam.
  if ACmd.Kind in [amqqcEnqueue, amqqcEnqueueSync, amqqcAddConsumer, amqqcAck, amqqcNack,
    amqqcReject, amqqcRemoveChannel, amqqcDeliverTick] then
    DeliverPending;
end;

end.
