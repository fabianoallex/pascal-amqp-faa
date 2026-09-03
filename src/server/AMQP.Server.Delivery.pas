unit AMQP.Server.Delivery;

{$I amqp.inc}

{ O alvo de entrega de um canal -- sub-modulo broker, WS4 da Fase 2.

  E' a peca entre o ator da fila e a escrita da conexao: "o ator entrega; a
  conexao escreve". A fila (AMQP.Server.Queue) so' conhece a interface
  IAMQPDeliveryTarget; quem sabe montar Basic.Deliver + content-header + body
  frames e enfia-los na fila de escrita e' esta unit.

  Por que existe uma CLASSE SEPARADA em vez de o proprio TAMQPServerChannel
  implementar a interface
  ------------------------------------------------------------------------
  Ciclo de vida. O canal e a conexao morrem quando o peer manda Channel.Close,
  quando o socket cai ou quando o reaper do broker recolhe a conexao -- e o
  ator da fila, que roda numa thread do pool, pode estar exatamente no meio de
  uma entrega naquele instante. Se a fila guardasse um ponteiro para o canal,
  isso seria use-after-free: e' o risco que o plano da Fase 2 marca como "o
  ponto onde mora o use-after-free da fase".

  A saida e' a dupla refcount + detach:
   - o alvo e' um TInterfacedObject, entao cada fila que guarda um consumidor
     segura uma referencia e o OBJETO nao morre enquanto alguem o tiver na
     mao, mesmo que canal e conexao ja tenham sido liberados;
   - Detach (chamado pelo canal/conexao ao morrer) zera a referencia ao
     writer SOB O LOCK do alvo e marca-o como morto. Depois disso todo
     TryDeliver devolve False sem tocar em nada liberado, e as filas descartam
     o consumidor na proxima rodada de entrega (IsAlive = False).
  Detach nunca espera I/O: TryDeliver so' faz trabalho nao-bloqueante, entao o
  teardown nao trava atras de um consumidor lento.

  Contabilidade das nao-confirmadas (e do prefetch)
  -------------------------------------------------
  A lista de entregas em aberto do canal vive AQUI, e nao no canal, por um
  motivo de quem-sabe-o-que: a entrega acontece na thread do ATOR DA FILA, que
  e' a unica que sabe o par (delivery-tag, fila de origem); a thread de leitura
  da conexao, que ve o Basic.Ack do cliente, sabe a tag mas nao a fila. Sem
  esse registro, um ack de consumidor nao chega a fila nenhuma e a mensagem
  fica pendurada para sempre -- foi exatamente o bug que o teste
  AckRemoveDeVez_NaoVoltaAposQueda pegou.

  Entao: quem REGISTRA e' a entrega (TryDeliver) e o Basic.Get da FSM
  (NoteGet); quem RESOLVE e' a thread de leitura ao processar
  Basic.Ack/Nack/Reject (NoteResolved), que devolve as entradas resolvidas
  para o chamador encaminhar a baixa a cada fila.

  Prefetch (Basic.Qos): so' as entradas marcadas Prefetch=True contam para o
  limite. Entrega em no-ack nao gera entrada nenhuma (nunca sera confirmada);
  Basic.Get gera entrada (precisa ser confirmado) mas NAO conta para o
  prefetch, que pela spec limita entregas a CONSUMIDOR. }

interface

uses
  SysUtils,
  SyncObjs,
  Generics.Collections,
  AMQP.Protocol,
  AMQP.Frame,
  AMQP.Basic.Methods,
  AMQP.Threading,
  AMQP.Server.FrameIO,
  AMQP.Server.Message,
  AMQP.Server.Queue;

type
  { Uma entrega em aberto neste canal: a tag, a fila que guarda a mensagem e
    se ela conta para o limite do Basic.Qos. }
  TAMQPOutstanding = record
    Tag: UInt64;
    Queue: string;
    Prefetch: Boolean;
  end;

  { Alvo de entrega de UM canal. Criado pelo canal, referenciado pelas filas
    onde ele tem consumidores. }
  TAMQPChannelDeliveryTarget = class(TInterfacedObject, IAMQPDeliveryTarget)
  private
    FLock: TCriticalSection;
    FWriter: TAMQPFrameWriter; // nil depois do Detach -- sempre sob FLock
    FChannelNo: Word;
    FMaxPayload: Cardinal;
    FIdentity: NativeUInt;
    FNextTag: UInt64;   // sob FLock
    FAttached: Integer; // atomico (1/0) -- espelha "FWriter <> nil"
    FActive: Integer;   // atomico (1/0) -- Channel.Flow
    FPrefetch: Integer; // atomico
    // Entregas em aberto NESTE canal, em ordem crescente de tag (a sequencia
    // e' monotonica, entao inserir no fim ja mantem ordenado). Lista e nao
    // contador porque o Basic.Ack/Nack com multiple resolve TODAS as tags <=
    // a informada -- e porque cada uma precisa lembrar de QUAL FILA veio.
    // Sob FLock.
    FOutstanding: TList<TAMQPOutstanding>;
    function BuildFrames(const AConsumerTag: string; ADeliveryTag: UInt64;
      ARedelivered: Boolean; AMessage: TAMQPMessage): TArray<TAMQPFrame>;
    function PrefetchInFlight: Integer;
  public
    /// AMaxPayload: quanto cabe no payload de UM frame para este peer (o
    /// frame-max negociado menos o overhead de frame) -- 0 = sem limite.
    /// O alvo NAO e' dono do writer.
    constructor Create(AWriter: TAMQPFrameWriter; AChannelNo: Word;
      AMaxPayload: Cardinal);
    destructor Destroy; override;

    // --- IAMQPDeliveryTarget ---
    function ChannelId: NativeUInt;
    function IsAlive: Boolean;
    function TryDeliver(const AConsumerTag, AQueueName: string;
      ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
      out ADeliveryTag: UInt64): Boolean;

    /// O canal/conexao morreu: solta o writer e marca o alvo como morto.
    /// Idempotente e chamavel de qualquer thread.
    procedure Detach;

    /// Proxima delivery-tag da MESMA sequencia por canal usada pela entrega.
    /// O Basic.Get precisa dela (a tag e' do canal, nao do consumidor).
    function NextDeliveryTag: UInt64;
    /// O cliente confirmou (Basic.Ack/Nack/Reject) uma entrega deste canal.
    /// AMultiple resolve todas as tags <= ADeliveryTag (e, com
    /// ADeliveryTag=0, todas as pendentes -- spec). Devolve as entradas
    /// resolvidas, COM A FILA DE ORIGEM de cada uma: e' o que permite ao
    /// chamador encaminhar a baixa para a fila certa. Chamada pela thread de
    /// leitura da conexao, que e' quem ve o metodo do cliente.
    function NoteResolved(ADeliveryTag: UInt64;
      AMultiple: Boolean): TArray<TAMQPOutstanding>;
    /// Registra uma entrega feita fora da entrega a consumidor (Basic.Get em
    /// modo ack). Nao conta para o prefetch.
    procedure NoteGet(ADeliveryTag: UInt64; const AQueue: string);
    /// Filas com entrega em aberto neste canal (sem repetir). O teardown usa
    /// para avisar cada uma.
    function PendingQueues: TArray<string>;

    /// Channel.Flow. False = o peer pediu para pararmos de entregar conteudo.
    procedure SetActive(AValue: Boolean);
    /// Basic.Qos prefetch-count (0 = ilimitado).
    procedure SetPrefetch(AValue: Word);
    /// Entregas ainda nao confirmadas neste canal.
    function UnackedCount: Integer;
    property ChannelNo: Word read FChannelNo;
  end;

/// Frames de CONTEUDO (content-header + N body) de uma mensagem, para o canal
/// e o frame-max dados. Usado pela entrega (Basic.Deliver) e pelo Basic.Get
/// da FSM -- os dois emitem method + header + body, mudando so' o method.
/// AMaxPayload = 0: corpo num unico frame.
///
/// O header sai do payload CRU guardado na mensagem (decisao D1): reemissao
/// byte a byte, sem redecodificar propriedades. So' quando nao ha payload cru
/// (mensagem criada pela propria engine) um header minimo e' montado.
function AmqpBuildContentFrames(AChannelNo: Word; AMessage: TAMQPMessage;
  AMaxPayload: Cardinal): TArray<TAMQPFrame>;

implementation

function AmqpBuildContentFrames(AChannelNo: Word; AMessage: TAMQPMessage;
  AMaxPayload: Cardinal): TArray<TAMQPFrame>;
var
  LHeader: TBytes;
  LChunks: TAMQPBodyChunks;
  I: Integer;
begin
  Result := nil;
  LHeader := AMessage.HeaderPayload;
  if Length(LHeader) = 0 then
    LHeader := BuildContentHeader(AMessage.BodySize,
      TAMQPBasicProperties.Empty);
  LChunks := AmqpSplitBody(AMessage.Body, AMaxPayload);
  SetLength(Result, 1 + Length(LChunks));
  Result[0] := TAMQPFrame.Create(AMQP_FRAME_HEADER, AChannelNo, LHeader);
  for I := 0 to High(LChunks) do
    Result[1 + I] := TAMQPFrame.Create(AMQP_FRAME_BODY, AChannelNo,
      LChunks[I]);
end;

var
  // Identidade de canal para a fila chavear as nao-confirmadas. Contador
  // global e monotonico em vez do endereco do objeto: endereco pode ser
  // reusado por um alvo novo depois que o antigo morre, e uma nao-confirmada
  // orfa casaria com o canal errado.
  GIdentitySeq: Integer = 0;

{ TAMQPChannelDeliveryTarget }

constructor TAMQPChannelDeliveryTarget.Create(AWriter: TAMQPFrameWriter;
  AChannelNo: Word; AMaxPayload: Cardinal);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FOutstanding := TList<TAMQPOutstanding>.Create;
  FWriter := AWriter;
  FChannelNo := AChannelNo;
  FMaxPayload := AMaxPayload;
  FIdentity := NativeUInt(Cardinal(AmqpAtomicInc(GIdentitySeq)));
  FNextTag := 1; // spec: a sequencia de delivery-tag de um canal comeca em 1
  FAttached := 1;
  FActive := 1;
end;

destructor TAMQPChannelDeliveryTarget.Destroy;
begin
  FOutstanding.Free;
  FLock.Free;
  inherited;
end;

function TAMQPChannelDeliveryTarget.ChannelId: NativeUInt;
begin
  Result := FIdentity;
end;

function TAMQPChannelDeliveryTarget.IsAlive: Boolean;
begin
  // Sem lock de proposito: e' consultado para cada consumidor em cada rodada
  // de entrega. O flag atomico basta -- quem chegar tarde cai no teste de
  // FWriter=nil dentro do TryDeliver, esse sim sob o lock.
  Result := AmqpAtomicGet(FAttached) = 1;
end;

procedure TAMQPChannelDeliveryTarget.Detach;
begin
  FLock.Enter;
  try
    FWriter := nil;
    AmqpAtomicSet(FAttached, 0);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPChannelDeliveryTarget.SetActive(AValue: Boolean);
begin
  if AValue then
    AmqpAtomicSet(FActive, 1)
  else
    AmqpAtomicSet(FActive, 0);
end;

procedure TAMQPChannelDeliveryTarget.SetPrefetch(AValue: Word);
begin
  AmqpAtomicSet(FPrefetch, AValue);
end;

function TAMQPChannelDeliveryTarget.UnackedCount: Integer;
begin
  FLock.Enter;
  try
    Result := FOutstanding.Count;
  finally
    FLock.Leave;
  end;
end;

// Quantas das entregas em aberto contam para o Basic.Qos. Chamar SOB FLock.
function TAMQPChannelDeliveryTarget.PrefetchInFlight: Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := 0 to FOutstanding.Count - 1 do
    if FOutstanding[I].Prefetch then
      Inc(Result);
end;

procedure TAMQPChannelDeliveryTarget.NoteGet(ADeliveryTag: UInt64;
  const AQueue: string);
var
  LEntry: TAMQPOutstanding;
begin
  LEntry.Tag := ADeliveryTag;
  LEntry.Queue := AQueue;
  LEntry.Prefetch := False; // Basic.Get nao entra no limite do Qos
  FLock.Enter;
  try
    FOutstanding.Add(LEntry);
  finally
    FLock.Leave;
  end;
end;

function TAMQPChannelDeliveryTarget.PendingQueues: TArray<string>;
var
  I, J: Integer;
  LNomes: TList<string>;
begin
  LNomes := TList<string>.Create;
  try
    FLock.Enter;
    try
      for I := 0 to FOutstanding.Count - 1 do
      begin
        J := LNomes.IndexOf(FOutstanding[I].Queue);
        if J < 0 then
          LNomes.Add(FOutstanding[I].Queue);
      end;
    finally
      FLock.Leave;
    end;
    Result := LNomes.ToArray;
  finally
    LNomes.Free;
  end;
end;

function TAMQPChannelDeliveryTarget.NoteResolved(ADeliveryTag: UInt64;
  AMultiple: Boolean): TArray<TAMQPOutstanding>;
var
  I: Integer;
  LRes: TList<TAMQPOutstanding>;
begin
  LRes := TList<TAMQPOutstanding>.Create;
  try
    FLock.Enter;
    try
      if AMultiple then
      begin
        // A lista esta em ordem crescente: as resolvidas sao um PREFIXO dela.
        // Tag 0 com multiple = todas as pendentes (spec 1.8.3.13).
        while (FOutstanding.Count > 0)
          and ((ADeliveryTag = 0) or (FOutstanding[0].Tag <= ADeliveryTag)) do
        begin
          LRes.Add(FOutstanding[0]);
          FOutstanding.Delete(0);
        end;
      end
      else
      begin
        for I := 0 to FOutstanding.Count - 1 do
          if FOutstanding[I].Tag = ADeliveryTag then
          begin
            LRes.Add(FOutstanding[I]);
            FOutstanding.Delete(I);
            Break;
          end;
        // Tag desconhecida nao e' erro AQUI: pode ser um ack duplicado. Quem
        // trata ack invalido como erro de canal e' a FSM.
      end;
    finally
      FLock.Leave;
    end;
    Result := LRes.ToArray;
  finally
    LRes.Free;
  end;
end;

function TAMQPChannelDeliveryTarget.NextDeliveryTag: UInt64;
begin
  FLock.Enter;
  try
    Result := FNextTag;
    Inc(FNextTag);
  finally
    FLock.Leave;
  end;
end;

// Monta o lote INDIVISIVEL de uma entrega: method + content-header + N body.
function TAMQPChannelDeliveryTarget.BuildFrames(const AConsumerTag: string;
  ADeliveryTag: UInt64; ARedelivered: Boolean;
  AMessage: TAMQPMessage): TArray<TAMQPFrame>;
var
  LDeliver: TAMQPBasicDeliver;
  LConteudo: TArray<TAMQPFrame>;
  I: Integer;
begin
  Result := nil;
  LDeliver.ConsumerTag := AConsumerTag;
  LDeliver.DeliveryTag := ADeliveryTag;
  LDeliver.Redelivered := ARedelivered;
  LDeliver.Exchange := AMessage.Exchange;
  LDeliver.RoutingKey := AMessage.RoutingKey;

  LConteudo := AmqpBuildContentFrames(FChannelNo, AMessage, FMaxPayload);
  SetLength(Result, 1 + Length(LConteudo));
  Result[0] := TAMQPFrame.Create(AMQP_FRAME_METHOD, FChannelNo,
    BuildBasicDeliver(LDeliver));
  for I := 0 to High(LConteudo) do
    Result[1 + I] := LConteudo[I];
end;

function TAMQPChannelDeliveryTarget.TryDeliver(
  const AConsumerTag, AQueueName: string; ANoAck, ARedelivered: Boolean;
  AMessage: TAMQPMessage; out ADeliveryTag: UInt64): Boolean;
var
  LTag: UInt64;
  LPrefetch: Integer;
  LEntry: TAMQPOutstanding;
begin
  ADeliveryTag := 0;
  Result := False;
  FLock.Enter;
  try
    if FWriter = nil then
      Exit; // canal/conexao ja morreu (Detach)
    if AmqpAtomicGet(FActive) = 0 then
      Exit; // Channel.Flow desligado pelo peer
    if not ANoAck then
    begin
      LPrefetch := AmqpAtomicGet(FPrefetch);
      if (LPrefetch > 0) and (PrefetchInFlight >= LPrefetch) then
        Exit; // prefetch estourado: o canal ja tem o que aguenta
    end;

    // A tag so' e' CONSUMIDA se o lote for aceito -- uma recusa nao pode
    // abrir buraco na sequencia do canal.
    LTag := FNextTag;
    if not FWriter.TryPostFrames(BuildFrames(AConsumerTag, LTag, ARedelivered,
      AMessage)) then
      Exit; // fila de escrita cheia: D3, o consumidor sai do rodizio da rodada

    FNextTag := LTag + 1;
    if not ANoAck then
    begin
      // A fila de origem fica registrada aqui porque so' ESTA chamada a
      // conhece -- ver o cabecalho da unit.
      LEntry.Tag := LTag;
      LEntry.Queue := AQueueName;
      LEntry.Prefetch := True;
      FOutstanding.Add(LEntry);
    end;
    ADeliveryTag := LTag;
    Result := True;
  finally
    FLock.Leave;
  end;
end;

end.
