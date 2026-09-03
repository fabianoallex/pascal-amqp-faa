unit AMQP.Server.Channel;

{$I amqp.inc}

{ Um canal de uma conexão do broker (sub-módulo server).

  Guarda o ciclo de vida do canal (WS4) e a montagem de conteúdo (WS5): no
  AMQP 0-9-1 uma mensagem publicada chega em três partes NESTE canal, nesta
  ordem exata (spec 2.3.5):

    1) o método Basic.Publish        -> BeginContent
    2) um content-header com o body-size e as propriedades  -> SetContentHeader
    3) N frames de body até somar o body-size               -> AppendBody

  Entre a parte 1 e a última parte 3 nenhum outro frame pode aparecer NESTE
  canal — é a "regra de interleave", e quem a aplica é a conexão (505
  UNEXPECTED_FRAME). Canais diferentes podem intercalar à vontade: por isso o
  estado de montagem vive aqui, por canal, e não na conexão.

  Só a thread de leitura da conexão dona toca estes objetos. }

interface

uses
  SysUtils,
  Generics.Collections,
  AMQP.Protocol,
  AMQP.Basic.Methods,
  AMQP.Server.Types,
  AMQP.Server.FrameIO,
  AMQP.Server.Queue,
  AMQP.Server.Delivery;

type
  { amqchOpen  — canal utilizável.
    amqchClosing — o broker mandou Channel.Close e espera o Channel.Close-Ok;
    todo frame que chegar no canal nesse meio-tempo é descartado (spec 1.4.2.2). }
  TAMQPServerChannelState = (amqchOpen, amqchClosing);

  { amqasIdle    — nenhum conteúdo em montagem.
    amqasHeader  — veio o Basic.Publish, falta o content-header.
    amqasBody    — veio o header, faltam body frames para fechar o body-size. }
  TAMQPChannelAsmState = (amqasIdle, amqasHeader, amqasBody);

  TAMQPServerChannel = class
  private
    FId: Word;
    FState: TAMQPServerChannelState;
    FActive: Boolean;
    FConfirmMode: Boolean;
    FPublishSeq: UInt64;
    FPrefetchCount: Word;
    // Alvo de entrega (WS4). O canal cria e DESTACA; as filas onde este canal
    // tem consumidores seguram a interface, o que mantem o objeto vivo mesmo
    // depois de o canal morrer -- ver AMQP.Server.Delivery.
    FTargetObj: TAMQPChannelDeliveryTarget;
    FTarget: IAMQPDeliveryTarget;
    // Identidade do alvo, guardada A PARTE porque ela precisa SOBREVIVER ao
    // DetachDelivery: o teardown da conexao destaca os alvos antes de varrer
    // os canais, e e' justamente nessa varredura que as filas precisam saber
    // de qual canal soltar consumidores e nao-confirmadas.
    FDeliveryId: NativeUInt;
    // WS5: o canal e' quem sabe amarrar o que o cliente diz ao que a engine
    // precisa. Duas amarracoes:
    //  - consumer-tag -> nome da fila, para o Basic.Cancel e para o teardown;
    //  - delivery-tag -> nome da fila, porque o Basic.Ack/Nack/Reject chega
    //    com uma tag do CANAL e quem tem a nao-confirmada e' a FILA.
    // Só a thread de leitura desta conexão mexe nestes mapas.
    FLastQueue: string;
    FConsumers: TDictionary<string, string>;
    FUnacked: TDictionary<UInt64, string>;
    // --- montagem de conteúdo ---
    FAsmState: TAMQPChannelAsmState;
    FPublish: TAMQPBasicPublish;
    FProps: TAMQPBasicProperties;
    FHeaderPayload: TBytes;
    FBodySize: UInt64;
    FBody: TBytes;
    FBodyLen: Integer;
    procedure FreeProps;
    procedure SetActive(AValue: Boolean);
    procedure SetPrefetchCount(AValue: Word);
    procedure SetConfirmMode(AValue: Boolean);
  public
    constructor Create(AId: Word);
    destructor Destroy; override;

    function IsOpen: Boolean;

    /// Cria o alvo de entrega deste canal sobre o writer da conexao.
    /// AMaxPayload = quanto cabe no payload de um frame (frame-max negociado
    /// menos o overhead). Chamar uma vez, ao abrir o canal.
    procedure AttachDelivery(AWriter: TAMQPFrameWriter; AMaxPayload: Cardinal);
    /// Solta o alvo: a partir daqui toda entrega pendente falha em vez de
    /// tocar no writer. OBRIGATORIO antes de liberar o canal ou o writer --
    /// uma fila pode estar entregando neste exato instante, numa thread do
    /// pool. Idempotente; o destrutor chama.
    procedure DetachDelivery;

    /// Chegou o Basic.Publish: começa a montagem, esperando o content-header.
    /// Descarta qualquer montagem anterior inacabada.
    procedure BeginContent(const APublish: TAMQPBasicPublish);
    /// Chegou o content-header, já decodificado (AHeader) E o payload cru do
    /// frame (AHeaderPayload — decisão D1 da Fase 2: a mensagem final carrega
    /// esse payload para reemissão fiel, ver AMQP.Server.Message). Result =
    /// conteúdo JÁ completo (body-size 0, mensagem sem corpo — caso legítimo
    /// e comum em RPC).
    function SetContentHeader(const AHeader: TAMQPContentHeader;
      const AHeaderPayload: TBytes): Boolean;
    /// Acrescenta um frame de body. Result = conteúdo completo.
    /// Levanta EAMQPConnectionError (505) se o corpo passar do body-size
    /// declarado — o stream ficou fora de sincronia com o que o peer prometeu.
    function AppendBody(const AData: TBytes): Boolean;
    /// Monta a mensagem pronta. Só faz sentido com a montagem completa; a
    /// tabela de Headers segue pertencendo a ESTE canal (ver IAMQPMessageSink).
    function CurrentMessage(const AUserId: string): TAMQPServerMessage;
    /// Volta a amqasIdle liberando o que foi acumulado. Idempotente.
    procedure ResetContent;

    // --- amarrações do WS5 ---
    procedure AddConsumerTag(const ATag, AQueue: string);
    function ConsumerQueue(const ATag: string; out AQueue: string): Boolean;
    procedure RemoveConsumerTag(const ATag: string);
    /// Filas em que este canal tem consumidor ou entrega pendente — o
    /// teardown precisa avisar cada uma.
    function TouchedQueues: TArray<string>;

    property Id: Word read FId;
    property State: TAMQPServerChannelState read FState write FState;
    /// Alvo de entrega para pendurar num consumidor de fila (nil antes do
    /// AttachDelivery). E' interface: quem guardar mantem o objeto vivo.
    property DeliveryTarget: IAMQPDeliveryTarget read FTarget;
    /// O mesmo alvo como objeto, para o que so' a conexao faz (delivery-tag
    /// do Basic.Get, baixa de prefetch no ack). nil antes do AttachDelivery.
    property Delivery: TAMQPChannelDeliveryTarget read FTargetObj;
    /// Identidade do alvo deste canal. Continua valida depois do
    /// DetachDelivery (ver o campo). 0 antes do AttachDelivery.
    property DeliveryId: NativeUInt read FDeliveryId;
    /// Channel.Flow: False = o peer pediu que paremos de entregar conteúdo
    /// neste canal. A Fase 2 respeita na entrega.
    property Active: Boolean read FActive write SetActive;
    /// Confirm.Select recebido (publisher confirms): cada publish passa a
    /// gerar um Basic.Ack. Ligar zera a sequência de seq-no (spec 1.3.1 da
    /// extensão: a contagem começa quando o modo é ligado).
    property ConfirmMode: Boolean read FConfirmMode write SetConfirmMode;
    /// Consome e devolve o próximo seq-no de publicação deste canal. A
    /// sequência começa em 1 e conta TODO publish enquanto o confirm mode
    /// estiver ligado — roteado ou não, porque é assim que o cliente
    /// correlaciona o ack com o publish que ele numerou.
    function NextPublishSeq: UInt64;
    /// Basic.Qos. A Fase 2 usa para limitar entregas não confirmadas.
    property PrefetchCount: Word read FPrefetchCount write SetPrefetchCount;

    /// Última fila declarada NESTE canal. A spec deixa o cliente omitir o
    /// nome da fila em Queue.Bind/Purge/Delete e Basic.Consume/Get, e nesse
    /// caso vale a última declarada no canal (spec 1.7.2.1).
    property LastQueue: string read FLastQueue write FLastQueue;
    property AsmState: TAMQPChannelAsmState read FAsmState;
    /// Body-size declarado no content-header (válido em amqasBody).
    property BodySize: UInt64 read FBodySize;
    /// Quanto do corpo já chegou.
    property BodyLen: Integer read FBodyLen;
  end;

implementation

constructor TAMQPServerChannel.Create(AId: Word);
begin
  inherited Create;
  FId := AId;
  FState := amqchOpen;
  FActive := True;
  FAsmState := amqasIdle;
  FProps := TAMQPBasicProperties.Empty;
  FConsumers := TDictionary<string, string>.Create;
end;

destructor TAMQPServerChannel.Destroy;
begin
  DetachDelivery; // nunca liberar o canal deixando entrega viva apontando aqui
  ResetContent; // um canal fechado no meio de um publish não pode vazar a tabela
  FConsumers.Free;
  inherited;
end;

procedure TAMQPServerChannel.AttachDelivery(AWriter: TAMQPFrameWriter;
  AMaxPayload: Cardinal);
begin
  if FTarget <> nil then
    Exit;
  FTargetObj := TAMQPChannelDeliveryTarget.Create(AWriter, FId, AMaxPayload);
  FTarget := FTargetObj; // a interface e' quem conta as referencias
  FDeliveryId := FTargetObj.ChannelId;
  FTargetObj.SetActive(FActive);
  FTargetObj.SetPrefetch(FPrefetchCount);
end;

procedure TAMQPServerChannel.DetachDelivery;
begin
  if FTargetObj <> nil then
    FTargetObj.Detach;
  // A REFERENCIA continua aqui de proposito (so' o Detach acontece): o
  // teardown da conexao destaca todos os alvos ANTES de varrer os canais, e
  // essa varredura ainda precisa perguntar ao alvo em que filas este canal
  // tem entrega em aberto. Quem libera o objeto e' o refcount da interface,
  // quando o canal e as filas soltarem a ultima referencia.
end;

procedure TAMQPServerChannel.AddConsumerTag(const ATag, AQueue: string);
begin
  FConsumers.AddOrSetValue(ATag, AQueue);
end;

function TAMQPServerChannel.ConsumerQueue(const ATag: string;
  out AQueue: string): Boolean;
begin
  Result := FConsumers.TryGetValue(ATag, AQueue);
end;

procedure TAMQPServerChannel.RemoveConsumerTag(const ATag: string);
begin
  FConsumers.Remove(ATag);
end;

function TAMQPServerChannel.TouchedQueues: TArray<string>;
var
  LNome: string;
  LSet: TDictionary<string, Boolean>;
begin
  LSet := TDictionary<string, Boolean>.Create;
  try
    for LNome in FConsumers.Values do
      LSet.AddOrSetValue(LNome, True);
    if FTargetObj <> nil then
      for LNome in FTargetObj.PendingQueues do
        LSet.AddOrSetValue(LNome, True);
    Result := LSet.Keys.ToArray;
  finally
    LSet.Free;
  end;
end;

procedure TAMQPServerChannel.SetConfirmMode(AValue: Boolean);
begin
  if AValue and (not FConfirmMode) then
    FPublishSeq := 0; // a sequência nasce com o modo
  FConfirmMode := AValue;
end;

function TAMQPServerChannel.NextPublishSeq: UInt64;
begin
  Inc(FPublishSeq);
  Result := FPublishSeq;
end;

procedure TAMQPServerChannel.SetActive(AValue: Boolean);
begin
  FActive := AValue;
  if FTargetObj <> nil then
    FTargetObj.SetActive(AValue);
end;

procedure TAMQPServerChannel.SetPrefetchCount(AValue: Word);
begin
  FPrefetchCount := AValue;
  if FTargetObj <> nil then
    FTargetObj.SetPrefetch(AValue);
end;

function TAMQPServerChannel.IsOpen: Boolean;
begin
  Result := FState = amqchOpen;
end;

procedure TAMQPServerChannel.FreeProps;
begin
  // DecodeContentHeader cria a tabela de headers quando a propriedade está
  // presente; o dono é quem decodificou, ou seja, este canal.
  if FProps.Has(bpHeaders) and Assigned(FProps.Headers) then
    FProps.Headers.Free;
  FProps := TAMQPBasicProperties.Empty;
end;

procedure TAMQPServerChannel.ResetContent;
begin
  FreeProps;
  FAsmState := amqasIdle;
  FHeaderPayload := nil;
  FBodySize := 0;
  FBody := nil;
  FBodyLen := 0;
end;

procedure TAMQPServerChannel.BeginContent(const APublish: TAMQPBasicPublish);
begin
  ResetContent;
  FPublish := APublish;
  FAsmState := amqasHeader;
end;

function TAMQPServerChannel.SetContentHeader(const AHeader: TAMQPContentHeader;
  const AHeaderPayload: TBytes): Boolean;
begin
  FreeProps;
  FProps := AHeader.Properties;
  FHeaderPayload := AHeaderPayload;
  FBodySize := AHeader.BodySize;
  FBody := nil;
  FBodyLen := 0;
  // Sem corpo: a mensagem já está completa e nenhum body frame virá.
  if FBodySize = 0 then
  begin
    FAsmState := amqasIdle;
    Exit(True);
  end;
  FAsmState := amqasBody;
  Result := False;
end;

function TAMQPServerChannel.AppendBody(const AData: TBytes): Boolean;
var
  LLen: Integer;
begin
  LLen := Length(AData);
  // Cresce conforme chega: nada de alocar o body-size declarado de uma vez,
  // senão um peer hostil derruba o broker só anunciando um corpo gigante.
  if UInt64(FBodyLen) + UInt64(LLen) > FBodySize then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('corpo excede o body-size declarado (%d): já %d + %d octetos',
        [Int64(FBodySize), FBodyLen, LLen]), 0, 0);

  if LLen > 0 then
  begin
    SetLength(FBody, FBodyLen + LLen);
    Move(AData[0], FBody[FBodyLen], LLen);
    Inc(FBodyLen, LLen);
  end;

  Result := UInt64(FBodyLen) = FBodySize;
  if Result then
    FAsmState := amqasIdle;
end;

function TAMQPServerChannel.CurrentMessage(
  const AUserId: string): TAMQPServerMessage;
begin
  Result.Exchange := FPublish.Exchange;
  Result.RoutingKey := FPublish.RoutingKey;
  Result.Mandatory := FPublish.Mandatory;
  Result.Immediate := FPublish.Immediate;
  Result.Properties := FProps;
  Result.Body := Copy(FBody, 0, FBodyLen); // FBody pode ter folga de alocação
  Result.UserId := AUserId;
  Result.HeaderPayload := FHeaderPayload;
end;

end.
