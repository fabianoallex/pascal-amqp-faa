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
  AMQP.Server.Message;

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
  end;

  { Uma entrega aguardando confirmacao, chaveada por (canal, delivery-tag). }
  TAMQPUnackedEntry = record
    ChannelId: NativeUInt;
    DeliveryTag: UInt64;
    Msg: TAMQPMessage;
  end;

  TAMQPQueueCmdKind = (
    amqqcEnqueue, amqqcAddConsumer, amqqcRemoveConsumer, amqqcRemoveChannel,
    amqqcAck, amqqcNack, amqqcReject, amqqcDeliverTick,
    amqqcGet, amqqcPurge, amqqcDelete, amqqcStats);

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
    FMaxLength: Integer;
    FPool: TAMQPThreadPool;
    // --- caixa (sob FMon) ---
    FMon: TAMQPMonitor;
    FMailbox: TQueue<TAMQPQueueCommand>;
    FScheduled: Boolean;
    FStopping: Boolean;
    FStopped: Boolean;
    FActorThread: TThreadID;
    // --- estado, SO' do ator ---
    FStock: TQueue<TAMQPQueueEntry>;
    FRequeued: TQueue<TAMQPQueueEntry>; // cabeca logica da fila
    FUnacked: TList<TAMQPUnackedEntry>;
    FConsumers: TObjectList<TAMQPServerConsumer>;
    FRoundRobin: Integer; // proximo consumidor a atender (rodizio)
    FDropped: Int64;
    FDelivered: Int64;

    procedure ScheduleLocked;
    procedure Post(ACmd: TAMQPQueueCommand);
    procedure PostSync(ACmd: TAMQPQueueCommand);
    // --- helpers do ator (nunca chamados de fora dele) ---
    function ReadyCount: Integer;
    function TakeReady(out AEntry: TAMQPQueueEntry): Boolean;
    function PeekReady(out AEntry: TAMQPQueueEntry): Boolean;
    function NextEligible: TAMQPServerConsumer;
    procedure DeliverPending;
    procedure RequeueFront(AMsg: TAMQPMessage);
    procedure EnforceMaxLength;
    procedure ResolveUnacked(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple, ARequeue: Boolean);
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
      APool: TAMQPThreadPool = nil);
    destructor Destroy; override;

    // --- comandos assincronos ---
    /// A fila tira a SUA PROPRIA referencia (AddRef): quem publica em N filas
    /// continua dono da referencia dele e da Release depois do fan-out.
    /// Numa fila ja parada, a mensagem e' descartada e contada em
    /// DroppedCount (nao levanta -- publish nao falha por corrida de
    /// teardown).
    procedure PostMessage(AMessage: TAMQPMessage);
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
    procedure PostAck(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple: Boolean);
    procedure PostNack(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      AMultiple, ARequeue: Boolean);
    procedure PostReject(AChannelId: NativeUInt; ADeliveryTag: UInt64;
      ARequeue: Boolean);

    // --- comandos sincronos (levantam EAMQPQueueActor se a fila ja parou,
    //     se forem postados de dentro do proprio ator, ou no timeout) ---
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
  inherited Create;
  FName := AName;
  if AMaxLength < 0 then
    AMaxLength := 0;
  FMaxLength := AMaxLength;
  if APool <> nil then
    FPool := APool
  else
    FPool := AmqpPool;
  FMon := TAMQPMonitor.Create;
  FMailbox := TQueue<TAMQPQueueCommand>.Create;
  FStock := TQueue<TAMQPQueueEntry>.Create;
  FRequeued := TQueue<TAMQPQueueEntry>.Create;
  FUnacked := TList<TAMQPUnackedEntry>.Create;
  FConsumers := TObjectList<TAMQPServerConsumer>.Create(True);
end;

destructor TAMQPServerQueue.Destroy;
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
  FStock.Free;
  FRequeued.Free;
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
  while FRequeued.Count > 0 do
  begin
    LEntry := FRequeued.Dequeue;
    LEntry.Msg.Release;
  end;
  while FStock.Count > 0 do
  begin
    LEntry := FStock.Dequeue;
    LEntry.Msg.Release;
  end;
  for I := 0 to FUnacked.Count - 1 do
    FUnacked[I].Msg.Release;
  FUnacked.Clear;
  FConsumers.Clear; // OwnsObjects
end;

{ --- API assincrona --- }

procedure TAMQPServerQueue.PostMessage(AMessage: TAMQPMessage);
var
  LCmd: TAMQPQueueCommand;
begin
  LCmd := TAMQPQueueCommand.Create(amqqcEnqueue, False);
  AMessage.AddRef; // a referencia da FILA; o publicador continua com a dele
  LCmd.Msg := AMessage;
  Post(LCmd);
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
begin
  Result := FRequeued.Count + FStock.Count;
end;

// Tira a proxima pronta: as devolvidas (requeue) vem antes, na ordem original.
function TAMQPServerQueue.TakeReady(out AEntry: TAMQPQueueEntry): Boolean;
begin
  if FRequeued.Count > 0 then
    AEntry := FRequeued.Dequeue
  else if FStock.Count > 0 then
    AEntry := FStock.Dequeue
  else
  begin
    Result := False;
    Exit;
  end;
  Result := True;
end;

// Espia a proxima pronta SEM tirar do estoque -- a entrega so' tira depois
// de o alvo aceitar o lote de frames; se ele recusar, a mensagem tem de
// continuar exatamente onde estava.
function TAMQPServerQueue.PeekReady(out AEntry: TAMQPQueueEntry): Boolean;
begin
  if FRequeued.Count > 0 then
    AEntry := FRequeued.Peek
  else if FStock.Count > 0 then
    AEntry := FStock.Peek
  else
  begin
    Result := False;
    Exit;
  end;
  Result := True;
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
        LEntry.Msg.Release // sem ack: a fila larga a referencia dela agora
      else
      begin
        // A referencia do estoque passa para as nao-confirmadas.
        LUnacked.ChannelId := LCons.Target.ChannelId;
        LUnacked.DeliveryTag := LTag;
        LUnacked.Msg := LEntry.Msg;
        FUnacked.Add(LUnacked);
      end;
    end
    else
      LCons.Suspended := True;
  end;
end;

// Devolve para a CABECA da fila, marcada como reentregue (spec: Basic.Nack/
// Reject com requeue devolvem a mensagem a' sua posicao original).
procedure TAMQPServerQueue.RequeueFront(AMsg: TAMQPMessage);
var
  LEntry: TAMQPQueueEntry;
begin
  LEntry.Msg := AMsg;
  LEntry.Redelivered := True;
  FRequeued.Enqueue(LEntry);
end;

// D7: teto global de memoria. Descarta da CABECA (a mais velha) -- o broker
// roda DENTRO da app do usuario e uma fila sem consumidor nao pode derrubar o
// processo hospedeiro. Conta so' as prontas (as nao-confirmadas ja estao com
// um consumidor).
procedure TAMQPServerQueue.EnforceMaxLength;
var
  LEntry: TAMQPQueueEntry;
begin
  if FMaxLength <= 0 then
    Exit;
  while (ReadyCount > FMaxLength) and TakeReady(LEntry) do
  begin
    LEntry.Msg.Release;
    Inc(FDropped);
  end;
end;

// Ack (ARequeue=False), Nack/Reject com ou sem requeue. AMultiple: todas as
// tags <= ADeliveryTag DAQUELE canal.
procedure TAMQPServerQueue.ResolveUnacked(AChannelId: NativeUInt;
  ADeliveryTag: UInt64; AMultiple, ARequeue: Boolean);
var
  I: Integer;
  LEntry: TAMQPUnackedEntry;
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
        RequeueFront(LEntry.Msg)
      else
        LEntry.Msg.Release; // ack, ou nack/reject sem requeue (DLX e' Fase 3)
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
        LEntry.Redelivered := False;
        ACmd.Msg := nil; // a referencia passou para o estoque
        FStock.Enqueue(LEntry);
        EnforceMaxLength;
      end;

    amqqcAddConsumer:
      begin
        FConsumers.Add(ACmd.Consumer);
        ACmd.Consumer := nil; // a fila e' dona agora
      end;

    amqqcRemoveConsumer:
      for I := FConsumers.Count - 1 downto 0 do
        if (FConsumers[I].ChannelId = ACmd.ChannelId)
          and (FConsumers[I].ConsumerTag = ACmd.ConsumerTag) then
          FConsumers.Delete(I);

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
            RequeueFront(LUnacked.Msg);
          end
          else
            Inc(I);
        end;
        EnforceMaxLength;
      end;

    amqqcAck:
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, ACmd.Multiple, False);

    amqqcNack:
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, ACmd.Multiple,
        ACmd.Requeue);

    amqqcReject: // Basic.Reject nao tem 'multiple'
      ResolveUnacked(ACmd.ChannelId, ACmd.DeliveryTag, False, ACmd.Requeue);

    amqqcGet:
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
          FUnacked.Add(LUnacked);
        end;
        // Com no-ack a referencia do estoque simplesmente passa ao chamador.
      end
      else
        ACmd.ResOk := False;

    amqqcPurge:
      begin
        LCount := 0;
        while TakeReady(LEntry) do
        begin
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
  if ACmd.Kind in [amqqcEnqueue, amqqcAddConsumer, amqqcAck, amqqcNack,
    amqqcReject, amqqcRemoveChannel, amqqcDeliverTick] then
    DeliverPending;
end;

end.
