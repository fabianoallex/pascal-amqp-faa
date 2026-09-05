unit AMQP.Server.Recovery;

{$I amqp.inc}

{ Replay do WAL -- sub-modulo broker, WS6 da Fase 4 (durabilidade).

  Esta unit NAO fala com a engine. Ela le' os segmentos em ordem de LSN e
  responde uma pergunta so': DADO ESTE LOG, QUAL E' O ESTADO? Quem aplica esse
  estado a um broker vivo e' o TAMQPServer.Start, e a separacao e' deliberada
  -- assim a parte dificil (a semantica do replay) e' testavel sem subir socket,
  sem thread e sem broker, com o log montado a mao byte a byte.

  O QUE O REPLAY TEM DE ACERTAR, E QUE NAO E' OBVIO
  ------------------------------------------------

  1. O LOG E' DE OPERACOES, O ESTADO E' O FINAL. Um QUEUE_DECLARE seguido de
     QUEUE_DELETE nao deixa fila nenhuma -- e nao deixa BINDING nenhum, nem
     COLOCACAO nenhuma daquela fila. Apagar a fila e esquecer o que pendia nela
     ressuscitaria mensagem de fila que nao existe mais.

  2. A COLOCACAO VIVA E' A QUE TEM ENQ E NAO TEM DEQ. E' a D22 inteira: a
     entrega nao e' journalada, entao uma mensagem entregue e NAO confirmada
     continua sendo um ENQ vivo e volta pronta (D27). Nada a fazer para isso
     acontecer -- e' consequencia de nao escrever nada na entrega.

  3. O CORPO VEM UMA VEZ SO'. N colocacoes referenciam o mesmo ContentId
     (D22), e o dead-letter deriva colocacao nova com o MESMO conteudo. Por
     isso o conteudo vive num mapa a parte, e a colocacao so' guarda o id.

  4. A ORDEM DAS COLOCACOES E' A ORDEM DO LOG. A D27 se paga aqui: como o
     journal e' append-only e o balde de prioridade esta' dentro do ENQ,
     reenfileirar na ordem de leitura poe cada mensagem exatamente onde o
     RequeueFront da D12 a poria. Nao ha ordenacao a fazer.

  5. O MAIOR ID VISTO IMPORTA. EntryId e ContentId saem do mesmo contador da
     engine; se ele recomecasse do zero depois de um restart, uma colocacao
     nova colidiria com uma recuperada e o DEQ de uma aposentaria a outra.
     Por isso MaxId sai daqui.

  O QUE ESTA UNIT NAO FAZ: nao decide prazo. O ENQ carrega o instante de
  PAREDE e o TTL (D21), e converter isso para o relogio monotonico e' trabalho
  de quem tem a fila viva na mao -- aqui os dois numeros passam intactos. }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  AMQP.Wire,
  AMQP.Server.Wal,
  AMQP.Server.Records;

type
  { Um bind sobrevivente, com o tipo de destino que o distingue. }
  TAMQPRecoveredBinding = record
    Binding: TAMQPRecBinding;
    /// True = exchange->exchange (D5); False = exchange->fila.
    ParaExchange: Boolean;
  end;

  { Contagens do replay, para o log do broker e para os testes. }
  TAMQPRecoveryStats = record
    /// Segmentos abertos.
    Segmentos: Integer;
    /// Registros integros lidos.
    Registros: Integer;
    /// Por que a leitura do ULTIMO segmento parou. awsFim = log inteiro.
    Parada: TAMQPWalStop;
    /// Colocacoes que ficaram vivas.
    Vivas: Integer;
    /// Colocacoes aposentadas por um DEQ.
    Aposentadas: Integer;
    /// Colocacoes descartadas porque a fila delas foi apagada depois.
    OrfasDeFila: Integer;
    /// Colocacoes que apareceram DUAS vezes no log e foram unificadas. Nao e'
    /// anomalia: e' o preco -- e a prova -- de a compactacao ser segura a
    /// queda. Ela reescreve os registros vivos com o MESMO EntryId antes de
    /// apagar os segmentos velhos, entao uma queda no meio deixa as duas
    /// copias, e o replay tem de trata-las como UMA.
    Duplicadas: Integer;
    /// Colocacoes descartadas porque o conteudo nao esta' no log. Nao deveria
    /// acontecer (CONTENT e ENQ vao no MESMO lote indivisivel), e por isso e'
    /// contado em vez de ignorado: se um dia for diferente de zero, o defeito
    /// esta' em quem escreve, nao aqui.
    OrfasDeConteudo: Integer;
  end;

  { O estado que o log descreve. Dono de tudo que criou -- inclusive das
    TAMQPFieldTable de argumentos, que os decodificadores alocam. }
  TAMQPRecoveredState = class
  private
    FExchanges: TList<TAMQPRecExchange>;
    FQueues: TList<TAMQPRecQueue>;
    FBindings: TList<TAMQPRecoveredBinding>;
    // Colocacoes na ORDEM DO LOG. Uma aposentada vira LAPIDE (EntryId = 0, que
    // nenhuma colocacao real usa -- o contador da engine comeca no 1) em vez
    // de sair da lista: remover do meio custaria O(n) por DEQ, e o log de um
    // broker de vida longa tem um DEQ para cada ENQ. As lapides somem numa
    // varredura so', no fim do replay.
    FEntries: TList<TAMQPRecEnqueue>;
    /// EntryId -> posicao em FEntries, so' das VIVAS. E' o que faz unificar
    /// duplicada e achar o alvo de um DEQ custarem O(1) em vez de O(vivas).
    FVivas: TDictionary<UInt64, Integer>;
    FLapides: Integer;
    FContents: TDictionary<UInt64, TAMQPRecContent>;
    FStats: TAMQPRecoveryStats;
    FMaxId: UInt64;
    function AchaExchange(const AVHost, AName: string): Integer;
    function AchaFila(const AVHost, AName: string): Integer;
    function AchaBinding(const AVHost, ASource, ADestination,
      ARoutingKey: string; AParaExchange: Boolean): Integer;
    procedure TiraBindingsDe(const AVHost, ANome: string; AEhFila: Boolean);
    procedure Aplica(const ARec: TAMQPWalRecord);
    procedure NotaId(AId: UInt64);
    /// Marca a colocacao da posicao AIndex como morta, sem tirar da lista.
    procedure Lapida(AIndex: Integer);
    /// Tira as lapides, preservando a ordem. Uma passada, no fim do replay.
    procedure VarreLapides;
  public
    constructor Create;
    destructor Destroy; override;

    /// Exchanges duraveis sobreviventes, na ordem em que foram declaradas.
    property Exchanges: TList<TAMQPRecExchange> read FExchanges;
    /// Filas duraveis sobreviventes, na ordem em que foram declaradas.
    property Queues: TList<TAMQPRecQueue> read FQueues;
    /// Bindings sobreviventes (fila e exchange), na ordem de criacao.
    property Bindings: TList<TAMQPRecoveredBinding> read FBindings;
    /// Colocacoes vivas, NA ORDEM DO LOG -- que e' a ordem de enfileiramento.
    property Entries: TList<TAMQPRecEnqueue> read FEntries;
    /// ContentId -> corpo. So' os referenciados por Entries sobrevivem.
    property Contents: TDictionary<UInt64, TAMQPRecContent> read FContents;
    /// Maior EntryId/ContentId visto. O contador da engine tem de continuar
    /// DEPOIS deste numero.
    property MaxId: UInt64 read FMaxId;
    property Stats: TAMQPRecoveryStats read FStats;
  end;

/// Le todo o WAL de ADir e devolve o estado que ele descreve. Nunca levanta
/// por cauda torta -- e' o caso NORMAL depois de uma queda, e o prefixo valido
/// e' exatamente o que a D23 manda replicar. Diretorio vazio devolve estado
/// vazio.
///
/// O chamador e' dono do resultado.
function AmqpReplayWal(const ADir: string): TAMQPRecoveredState;

implementation

{ TAMQPRecoveredState }

constructor TAMQPRecoveredState.Create;
begin
  inherited Create;
  FExchanges := TList<TAMQPRecExchange>.Create;
  FQueues := TList<TAMQPRecQueue>.Create;
  FBindings := TList<TAMQPRecoveredBinding>.Create;
  FEntries := TList<TAMQPRecEnqueue>.Create;
  FVivas := TDictionary<UInt64, Integer>.Create;
  FContents := TDictionary<UInt64, TAMQPRecContent>.Create;
end;

destructor TAMQPRecoveredState.Destroy;
var
  I: Integer;
  LPar: TPair<UInt64, TAMQPRecContent>;
begin
  // As tabelas de argumentos sao criadas pelos decodificadores; a posse e' de
  // quem chamou, e aqui isso quer dizer nos.
  for I := 0 to FExchanges.Count - 1 do
    FExchanges[I].Arguments.Free;
  for I := 0 to FQueues.Count - 1 do
    FQueues[I].Arguments.Free;
  for I := 0 to FBindings.Count - 1 do
    FBindings[I].Binding.Arguments.Free;
  FExchanges.Free;
  FQueues.Free;
  FBindings.Free;
  FEntries.Free;
  FVivas.Free;
  FContents.Free;
  inherited;
end;

// EntryId = 0 e' a lapide: nenhuma colocacao real usa esse numero, porque o
// contador da engine faz Inc ANTES de devolver e portanto comeca no 1.
procedure TAMQPRecoveredState.Lapida(AIndex: Integer);
var
  LEnq: TAMQPRecEnqueue;
begin
  LEnq := FEntries[AIndex];
  FVivas.Remove(LEnq.EntryId);
  LEnq.EntryId := 0;
  FEntries[AIndex] := LEnq;
  Inc(FLapides);
end;

procedure TAMQPRecoveredState.VarreLapides;
var
  I, LDestino: Integer;
begin
  if FLapides = 0 then
    Exit;
  LDestino := 0;
  for I := 0 to FEntries.Count - 1 do
    if FEntries[I].EntryId <> 0 then
    begin
      if LDestino <> I then
        FEntries[LDestino] := FEntries[I];
      Inc(LDestino);
    end;
  FEntries.Count := LDestino;
  FVivas.Clear; // as posicoes mudaram; ninguem mais consulta o indice
  FLapides := 0;
end;

procedure TAMQPRecoveredState.NotaId(AId: UInt64);
begin
  if AId > FMaxId then
    FMaxId := AId;
end;

function TAMQPRecoveredState.AchaExchange(const AVHost, AName: string): Integer;
var
  I: Integer;
begin
  for I := 0 to FExchanges.Count - 1 do
    if (FExchanges[I].VHost = AVHost) and (FExchanges[I].Name = AName) then
      Exit(I);
  Result := -1;
end;

function TAMQPRecoveredState.AchaFila(const AVHost, AName: string): Integer;
var
  I: Integer;
begin
  for I := 0 to FQueues.Count - 1 do
    if (FQueues[I].VHost = AVHost) and (FQueues[I].Name = AName) then
      Exit(I);
  Result := -1;
end;

function TAMQPRecoveredState.AchaBinding(const AVHost, ASource, ADestination,
  ARoutingKey: string; AParaExchange: Boolean): Integer;
var
  I: Integer;
  LB: TAMQPRecoveredBinding;
begin
  for I := 0 to FBindings.Count - 1 do
  begin
    LB := FBindings[I];
    if (LB.ParaExchange = AParaExchange)
      and (LB.Binding.VHost = AVHost)
      and (LB.Binding.Source = ASource)
      and (LB.Binding.Destination = ADestination)
      and (LB.Binding.RoutingKey = ARoutingKey) then
      Exit(I);
  end;
  Result := -1;
end;

// Apagar um recurso apaga os bindings em que ele aparece -- dos DOIS lados,
// porque um exchange pode ser origem de uns e destino de outros (D5).
procedure TAMQPRecoveredState.TiraBindingsDe(const AVHost, ANome: string;
  AEhFila: Boolean);
var
  I: Integer;
  LB: TAMQPRecoveredBinding;
begin
  for I := FBindings.Count - 1 downto 0 do
  begin
    LB := FBindings[I];
    if LB.Binding.VHost <> AVHost then
      Continue;
    if AEhFila then
    begin
      if (not LB.ParaExchange) and (LB.Binding.Destination = ANome) then
      begin
        LB.Binding.Arguments.Free;
        FBindings.Delete(I);
      end;
    end
    else if (LB.Binding.Source = ANome)
      or (LB.ParaExchange and (LB.Binding.Destination = ANome)) then
    begin
      LB.Binding.Arguments.Free;
      FBindings.Delete(I);
    end;
  end;
end;

procedure TAMQPRecoveredState.Aplica(const ARec: TAMQPWalRecord);
var
  LEx: TAMQPRecExchange;
  LQ: TAMQPRecQueue;
  LB: TAMQPRecoveredBinding;
  LNome: TAMQPRecName;
  LCont: TAMQPRecContent;
  LEnq: TAMQPRecEnqueue;
  LDeq: TAMQPRecDequeue;
  I: Integer;
begin
  case ARec.Kind of
    AMQP_REC_EXCHANGE_DECLARE:
      begin
        LEx := AmqpDecodeRecExchange(ARec.Payload);
        I := AchaExchange(LEx.VHost, LEx.Name);
        if I >= 0 then
        begin
          // Redeclare: o ultimo vale. (A engine ja' recusou incompatibilidade
          // la' atras -- o log so' guarda o que foi aceito.)
          FExchanges[I].Arguments.Free;
          FExchanges[I] := LEx;
        end
        else
          FExchanges.Add(LEx);
      end;
    AMQP_REC_EXCHANGE_DELETE:
      begin
        LNome := AmqpDecodeRecName(ARec.Payload);
        I := AchaExchange(LNome.VHost, LNome.Name);
        if I >= 0 then
        begin
          FExchanges[I].Arguments.Free;
          FExchanges.Delete(I);
        end;
        TiraBindingsDe(LNome.VHost, LNome.Name, False);
      end;
    AMQP_REC_QUEUE_DECLARE:
      begin
        LQ := AmqpDecodeRecQueue(ARec.Payload);
        I := AchaFila(LQ.VHost, LQ.Name);
        if I >= 0 then
        begin
          FQueues[I].Arguments.Free;
          FQueues[I] := LQ;
        end
        else
          FQueues.Add(LQ);
      end;
    AMQP_REC_QUEUE_DELETE:
      begin
        LNome := AmqpDecodeRecName(ARec.Payload);
        I := AchaFila(LNome.VHost, LNome.Name);
        if I >= 0 then
        begin
          FQueues[I].Arguments.Free;
          FQueues.Delete(I);
        end;
        TiraBindingsDe(LNome.VHost, LNome.Name, True);
        // ...e o que pendia nela vai junto: ressuscitar mensagem de fila
        // apagada seria pior que perde-la.
        for I := FEntries.Count - 1 downto 0 do
          if (FEntries[I].EntryId <> 0)
            and (FEntries[I].VHost = LNome.VHost)
            and (FEntries[I].Queue = LNome.Name) then
          begin
            Lapida(I);
            Inc(FStats.OrfasDeFila);
          end;
      end;
    AMQP_REC_QUEUE_BIND, AMQP_REC_EXCHANGE_BIND:
      begin
        LB.Binding := AmqpDecodeRecBinding(ARec.Payload);
        LB.ParaExchange := ARec.Kind = AMQP_REC_EXCHANGE_BIND;
        I := AchaBinding(LB.Binding.VHost, LB.Binding.Source,
          LB.Binding.Destination, LB.Binding.RoutingKey, LB.ParaExchange);
        if I >= 0 then
        begin
          // Rebind do mesmo trio: idempotente, e o ultimo vale.
          FBindings[I].Binding.Arguments.Free;
          FBindings[I] := LB;
        end
        else
          FBindings.Add(LB);
      end;
    AMQP_REC_QUEUE_UNBIND, AMQP_REC_EXCHANGE_UNBIND:
      begin
        LB.Binding := AmqpDecodeRecBinding(ARec.Payload);
        try
          I := AchaBinding(LB.Binding.VHost, LB.Binding.Source,
            LB.Binding.Destination, LB.Binding.RoutingKey,
            ARec.Kind = AMQP_REC_EXCHANGE_UNBIND);
          if I >= 0 then
          begin
            FBindings[I].Binding.Arguments.Free;
            FBindings.Delete(I);
          end;
        finally
          LB.Binding.Arguments.Free; // o unbind so' precisava da chave
        end;
      end;
    AMQP_REC_CONTENT:
      begin
        LCont := AmqpDecodeRecContent(ARec.Payload);
        NotaId(LCont.ContentId);
        FContents.AddOrSetValue(LCont.ContentId, LCont);
      end;
    AMQP_REC_ENQUEUE:
      begin
        LEnq := AmqpDecodeRecEnqueue(ARec.Payload);
        NotaId(LEnq.EntryId);
        // MESMA COLOCACAO DUAS VEZES = UMA. E' o que torna a reescrita da
        // compactacao segura a queda: ela regrava o vivo com o MESMO EntryId
        // e so' DEPOIS apaga os segmentos velhos, entao uma queda no meio
        // deixa as duas copias no log. Sem isto, cada compactacao
        // interrompida duplicaria o estoque inteiro.
        //
        // Fica a PRIMEIRA ocorrencia: a ordem do log e' a ordem de
        // enfileiramento (D27), e a copia antiga e' a que esta' na posicao
        // certa da fila. As duas sao iguais campo a campo, entao a escolha so'
        // importa para a POSICAO.
        if FVivas.ContainsKey(LEnq.EntryId) then
          Inc(FStats.Duplicadas)
        else
        begin
          FVivas.Add(LEnq.EntryId, FEntries.Count);
          FEntries.Add(LEnq);
        end;
      end;
    AMQP_REC_DEQUEUE:
      begin
        LDeq := AmqpDecodeRecDequeue(ARec.Payload);
        // O EntryId e' unico no broker (sai de UM contador na engine), entao
        // ele sozinho identifica a colocacao -- vhost e fila vem de brinde.
        if FVivas.TryGetValue(LDeq.EntryId, I) then
        begin
          Lapida(I);
          Inc(FStats.Aposentadas);
        end;
      end;
  end;
end;

function AmqpReplayWal(const ADir: string): TAMQPRecoveredState;
var
  LSegs: TArray<Cardinal>;
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LRegs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LN, I, J: Integer;
  LDir: string;
  LUsados: TDictionary<UInt64, Byte>;
  LSoltar: TArray<UInt64>;
  LChave: UInt64;
begin
  Result := TAMQPRecoveredState.Create;
  Result.FStats.Parada := awsFim;
  if (ADir = '') or (not DirectoryExists(ADir)) then
    Exit;
  LDir := IncludeTrailingPathDelimiter(ADir);
  LSegs := AmqpWalListSegments(ADir);
  // AmqpWalListSegments devolve em ordem crescente de numero, e o numero cresce
  // com o tempo -- entao percorrer na ordem dada E' percorrer em ordem de LSN.
  for I := 0 to High(LSegs) do
  begin
    LArq := TAMQPWalOsFile.Create(LDir + AmqpWalSegmentName(LSegs[I]), False);
    LSeg := TAMQPWalSegment.OpenExisting(LArq, False);
    try
      LN := LSeg.ReadPrefix(LRegs, LStop);
      Inc(Result.FStats.Segmentos);
      Inc(Result.FStats.Registros, LN);
      Result.FStats.Parada := LStop;
      for J := 0 to LN - 1 do
        Result.Aplica(LRegs[J]);
    finally
      LSeg.Free;
      LArq := nil;
    end;
  end;

  // As lapides saem AQUI, numa passada so': durante o replay elas ficam para
  // as posicoes do indice continuarem valendo.
  Result.VarreLapides;

  // Conteudo que nenhuma colocacao viva referencia nao interessa a ninguem --
  // e' o corpo de uma mensagem ja' consumida, ainda no arquivo so' porque o
  // log e' append-only. Soltar aqui e' o que impede a recuperacao de carregar
  // na memoria o estoque inteiro da vida do broker.
  LUsados := TDictionary<UInt64, Byte>.Create;
  try
    for I := Result.FEntries.Count - 1 downto 0 do
      if Result.FContents.ContainsKey(Result.FEntries[I].ContentId) then
        LUsados.AddOrSetValue(Result.FEntries[I].ContentId, 0)
      else
      begin
        // CONTENT e ENQ vao no MESMO lote indivisivel, entao isto nao deveria
        // acontecer. Contado em vez de ignorado -- ver TAMQPRecoveryStats.
        Result.FEntries.Delete(I);
        Inc(Result.FStats.OrfasDeConteudo);
      end;
    // Chaves a soltar recolhidas ANTES de apagar: mexer num dicionario durante
    // a propria enumeracao e' comportamento indefinido nos dois compiladores.
    LSoltar := nil;
    for LChave in Result.FContents.Keys do
      if not LUsados.ContainsKey(LChave) then
      begin
        SetLength(LSoltar, Length(LSoltar) + 1);
        LSoltar[High(LSoltar)] := LChave;
      end;
    for I := 0 to High(LSoltar) do
      Result.FContents.Remove(LSoltar[I]);
  finally
    LUsados.Free;
  end;
  Result.FStats.Vivas := Result.FEntries.Count;
end;

end.
