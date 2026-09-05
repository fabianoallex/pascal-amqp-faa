unit AMQP.ServerRecoveryTests;

{ Replay do WAL -- WS6 da Fase 4 (durabilidade, ver CLAUDE.md, D19-D28).

  A pergunta que estes testes fazem e' uma so': DADO ESTE LOG, QUAL E' O
  ESTADO? Nenhum deles sobe broker, socket ou thread -- o log e' escrito aqui
  mesmo, registro a registro, e lido de volta. E' o que permite prender a
  semantica do replay em vez de so' constatar que "recuperou alguma coisa".

  O QUE PRECISA SER PROVADO, E POR QUE NAO E' OBVIO

  1. Apagar uma fila apaga o que PENDIA nela. Uma implementacao que so tirasse
     a fila da lista passaria em qualquer teste de topologia -- e ressuscitaria
     mensagem de fila que nao existe mais.

  2. Apagar um exchange leva os bindings dos DOIS lados. Um exchange pode ser
     origem de uns e destino de outros (D5): olhar so' o Source deixa binding
     orfao apontando para o que sumiu.

  3. A ordem das colocacoes e' a ordem do LOG. E' o que faz a D27 sair de
     graca: o balde de prioridade esta' dentro do ENQ, entao reenfileirar na
     ordem de leitura poe cada mensagem onde o RequeueFront da D12 a poria.

  4. UM conteudo para N colocacoes (D22), e conteudo que ninguem referencia e'
     SOLTADO. Sem isso a recuperacao carregaria na memoria o corpo de toda
     mensagem que o broker ja' viu na vida -- o log e' append-only.

  5. O maior id visto sobrevive. Se o contador da engine recomecasse do zero,
     uma colocacao nova colidiria com uma recuperada, e o DEQ de uma
     aposentaria a outra. Silencioso e fatal.

  6. Cauda torta replica o PREFIXO (D23). E' o caso NORMAL depois de uma queda,
     nao um erro.

  As tres funcoes Checa* existem para os corpos dos testes serem IDENTICOS nos
  dois dialetos: so' a implementacao delas muda de lado.

  Espelho de tests\Server\fpc\AMQP.ServerRecoveryTests.pas -- mantenha os dois em sincronia. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.IOUtils,
  AMQP.Wire,
  AMQP.Server.Wal,
  AMQP.Server.Journal,
  AMQP.Server.Records,
  AMQP.Server.Recovery,
  AMQP.ServerTestDoubles;

type
  { Escreve um WAL a mao e le de volta. }
  [TestFixture]
  TRecoveryReplayTests = class
  private
    FDir: string;
    FJournal: TAMQPJournal;
    /// Grava UM registro e devolve o LSN.
    function Grava(AKind: Byte; const APayload: TBytes): UInt64;
    procedure DeclaraExchange(const ANome, ATipo: string);
    procedure DeclaraFila(const ANome: string; AAutoDelete: Boolean = False);
    procedure Liga(const AExchange, AFila, ARk: string);
    procedure Desliga(const AExchange, AFila, ARk: string);
    procedure LigaExchanges(const AOrigem, ADestino, ARk: string);
    procedure ApagaFila(const ANome: string);
    procedure ApagaExchange(const ANome: string);
    procedure Conteudo(AId: UInt64; const ACorpo: string);
    procedure Coloca(AId, AConteudo: UInt64; const AFila: string;
      APrioridade: Byte = 0);
    procedure Aposenta(AId: UInt64; const AFila: string);
    /// Fecha o journal (o log fica no disco) e replica.
    function Replica: TAMQPRecoveredState;
  public
    [Setup]    procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure LogVazio_EstadoVazio;
    [Test] procedure TopologiaDuravel_Sobrevive;
    [Test] procedure Redeclare_OUltimoVale;
    [Test] procedure Unbind_TiraOBinding;
    [Test] procedure ApagarFila_LevaBindingsEColocacoes;
    [Test] procedure ApagarExchange_LevaBindingsDosDoisLados;
    [Test] procedure EnqSemDeq_FicaVivo;
    [Test] procedure EnqComDeq_Some;
    [Test] procedure OrdemDasColocacoes_EAOrdemDoLog;
    [Test] procedure UmConteudoDuasColocacoes;
    [Test] procedure ConteudoSemColocacaoViva_ESoltado;
    [Test] procedure MaxId_EOMaiorVisto;
    [Test] procedure PrazoEPrioridade_ChegamIntactos;
    [Test] procedure CaudaTorta_ReplicaOPrefixo;
    // --- WS7: rotacao de segmento ---
    [Test] procedure Rotaciona_QuandoOSegmentoEnche;
    [Test] procedure Rotaciona_NaoParteOLoteEntreDoisArquivos;
    [Test] procedure Rotaciona_LsnContinuaEntreSegmentos;
    [Test] procedure Rotaciona_ReplayLeTodosOsSegmentosComoUmLogSo;
    [Test] procedure Rotaciona_ReabrirContinuaNoUltimoSegmento;
    // --- WS7: compactacao ---
    [Test] procedure Compacta_EncolheOLogSemMudarOEstado;
    [Test] procedure Compacta_ApagaOsSegmentosVelhos;
    [Test] procedure Compacta_PreservaOsIdentificadores;
    [Test] procedure Compacta_NaoDuplicaSeOsVelhosSobrarem;
    [Test] procedure Compacta_PreservaAOrdemEOsPrazos;
    [Test] procedure Compacta_LogInteiroMorto_NaoSobraColocacao;
    [Test] procedure Compacta_NoStart_QuandoOLogPassaDoLimiar;
    [Test] procedure Compacta_FsyncReprovado_NaoApagaNada;
    [Test] procedure Compacta_FanOut_GravaOCorpoUmaVezSo;
    [Test] procedure Teto_ZeroEIlimitado;
    [Test] procedure Teto_Estourado_MarcaCheio;
    [Test] procedure Teto_RetiradaNuncaERecusada;
  end;

implementation

var
  GRecSeq: Integer = 0;

procedure LimpaDirRec(const ADir: string);
var
  LRec: TSearchRec;
begin
  if FindFirst(IncludeTrailingPathDelimiter(ADir) + '*', faAnyFile, LRec) = 0 then
  begin
    try
      repeat
        if (LRec.Attr and faDirectory) = 0 then
          System.SysUtils.DeleteFile(IncludeTrailingPathDelimiter(ADir) + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      System.SysUtils.FindClose(LRec);
    end;
  end;
end;

// As pontes para as assercoes do dialeto -- ver o cabecalho.
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  Assert.AreEqual(AEsperado, AObtido, AMsg);
end;

procedure ChecaStr(const AMsg, AEsperado, AObtido: string);
begin
  Assert.AreEqual(AEsperado, AObtido, AMsg);
end;

procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  Assert.IsTrue(ACond, AMsg);
end;

procedure ChecaNao(const AMsg: string; ACond: Boolean);
begin
  Assert.IsFalse(ACond, AMsg);
end;

/// Monta um registro de conteudo (as duas suites de rotacao precisam do mesmo).
type
  { Journal cujo fsync pode ser reprovado, sobre arquivos DE VERDADE. }
  TJournalComSyncFalho = class(TAMQPJournal)
  private
    FUltimo: TAMQPWalFileComFalha;
    FRefUltimo: IAMQPWalFile; // mantem FUltimo vivo enquanto o teste o usa
    FReprovando: Boolean;
  protected
    function CriaArquivo(const APath: string;
      ACriar: Boolean): IAMQPWalFile; override;
  public
    /// A partir daqui todo fsync reprova -- inclusive o da compactacao.
    procedure ReprovaOSync;
  end;

function TJournalComSyncFalho.CriaArquivo(const APath: string;
  ACriar: Boolean): IAMQPWalFile;
begin
  FUltimo := TAMQPWalFileComFalha.Create(TAMQPWalOsFile.Create(APath, ACriar));
  FUltimo.SetSyncOk(not FReprovando);
  FRefUltimo := FUltimo;
  Result := FRefUltimo;
end;

procedure TJournalComSyncFalho.ReprovaOSync;
begin
  FReprovando := True;
  if FUltimo <> nil then
    FUltimo.SetSyncOk(False);
end;

/// Um lote de UM registro (o SubmitNoWait recebe array).
function RegistroDe(AKind: Byte; const APayload: TBytes): TAMQPJournalRecords;
begin
  SetLength(Result, 1);
  Result[0].Kind := AKind;
  Result[0].Payload := APayload;
end;

function DequeueDe(AId: UInt64; const AFila: string): TAMQPRecDequeue;
begin
  Result.VHost := '/';
  Result.Queue := AFila;
  Result.EntryId := AId;
end;

function ConteudoDe(AId: UInt64; const ACorpo: string): TAMQPRecContent;
begin
  Result.ContentId := AId;
  Result.UserId := 'guest';
  Result.Body := AmqpUtf8Encode(ACorpo);
end;

function TRecoveryReplayTests.Grava(AKind: Byte;
  const APayload: TBytes): UInt64;
var
  LRecs: TAMQPJournalRecords;
begin
  SetLength(LRecs, 1);
  LRecs[0].Kind := AKind;
  LRecs[0].Payload := APayload;
  Result := FJournal.Submit(LRecs);
end;

procedure TRecoveryReplayTests.DeclaraExchange(const ANome, ATipo: string);
var
  LEx: TAMQPRecExchange;
begin
  LEx.VHost := '/';
  LEx.Name := ANome;
  LEx.ExchangeType := ATipo;
  LEx.Durable := True;
  LEx.AutoDelete := False;
  LEx.Internal := False;
  LEx.Arguments := nil;
  Grava(AMQP_REC_EXCHANGE_DECLARE, AmqpEncodeRecExchange(LEx));
end;

procedure TRecoveryReplayTests.DeclaraFila(const ANome: string;
  AAutoDelete: Boolean);
var
  LQ: TAMQPRecQueue;
begin
  LQ.VHost := '/';
  LQ.Name := ANome;
  LQ.Durable := True;
  LQ.AutoDelete := AAutoDelete;
  LQ.Arguments := nil;
  Grava(AMQP_REC_QUEUE_DECLARE, AmqpEncodeRecQueue(LQ));
end;

procedure TRecoveryReplayTests.Liga(const AExchange, AFila, ARk: string);
var
  LB: TAMQPRecBinding;
begin
  LB.VHost := '/';
  LB.Source := AExchange;
  LB.Destination := AFila;
  LB.RoutingKey := ARk;
  LB.Arguments := nil;
  Grava(AMQP_REC_QUEUE_BIND, AmqpEncodeRecBinding(LB));
end;

procedure TRecoveryReplayTests.Desliga(const AExchange, AFila, ARk: string);
var
  LB: TAMQPRecBinding;
begin
  LB.VHost := '/';
  LB.Source := AExchange;
  LB.Destination := AFila;
  LB.RoutingKey := ARk;
  LB.Arguments := nil;
  Grava(AMQP_REC_QUEUE_UNBIND, AmqpEncodeRecBinding(LB));
end;

procedure TRecoveryReplayTests.LigaExchanges(const AOrigem, ADestino,
  ARk: string);
var
  LB: TAMQPRecBinding;
begin
  LB.VHost := '/';
  LB.Source := AOrigem;
  LB.Destination := ADestino;
  LB.RoutingKey := ARk;
  LB.Arguments := nil;
  Grava(AMQP_REC_EXCHANGE_BIND, AmqpEncodeRecBinding(LB));
end;

procedure TRecoveryReplayTests.ApagaFila(const ANome: string);
var
  LN: TAMQPRecName;
begin
  LN.VHost := '/';
  LN.Name := ANome;
  Grava(AMQP_REC_QUEUE_DELETE, AmqpEncodeRecName(LN));
end;

procedure TRecoveryReplayTests.ApagaExchange(const ANome: string);
var
  LN: TAMQPRecName;
begin
  LN.VHost := '/';
  LN.Name := ANome;
  Grava(AMQP_REC_EXCHANGE_DELETE, AmqpEncodeRecName(LN));
end;

procedure TRecoveryReplayTests.Conteudo(AId: UInt64; const ACorpo: string);
var
  LC: TAMQPRecContent;
begin
  LC.ContentId := AId;
  LC.UserId := 'guest';
  LC.Body := AmqpUtf8Encode(ACorpo);
  Grava(AMQP_REC_CONTENT, AmqpEncodeRecContent(LC));
end;

procedure TRecoveryReplayTests.Coloca(AId, AConteudo: UInt64;
  const AFila: string; APrioridade: Byte);
var
  LE: TAMQPRecEnqueue;
begin
  LE.VHost := '/';
  LE.Queue := AFila;
  LE.EntryId := AId;
  LE.ContentId := AConteudo;
  LE.Exchange := '';
  LE.RoutingKey := AFila;
  LE.Priority := APrioridade;
  LE.EnqueuedAtWall := 1700000000000;
  LE.TtlMs := -1;
  LE.HeaderPayload := nil;
  Grava(AMQP_REC_ENQUEUE, AmqpEncodeRecEnqueue(LE));
end;

procedure TRecoveryReplayTests.Aposenta(AId: UInt64; const AFila: string);
var
  LD: TAMQPRecDequeue;
begin
  LD.VHost := '/';
  LD.Queue := AFila;
  LD.EntryId := AId;
  Grava(AMQP_REC_DEQUEUE, AmqpEncodeRecDequeue(LD));
end;

function TRecoveryReplayTests.Replica: TAMQPRecoveredState;
begin
  FJournal.Stop;
  FreeAndNil(FJournal);
  Result := AmqpReplayWal(FDir);
end;

procedure TRecoveryReplayTests.SetUp;
begin
  Inc(GRecSeq);
  FDir := IncludeTrailingPathDelimiter(TPath.GetTempPath) + 'amqprec-'
    + IntToStr(GRecSeq);
  ForceDirectories(FDir);
  LimpaDirRec(FDir);
  FJournal := TAMQPJournal.Create(FDir);
  FJournal.Start;
end;

procedure TRecoveryReplayTests.TearDown;
begin
  if FJournal <> nil then
  begin
    try
      FJournal.Stop;
    except
    end;
    FreeAndNil(FJournal);
  end;
  LimpaDirRec(FDir);
end;

procedure TRecoveryReplayTests.LogVazio_EstadoVazio;
var
  E: TAMQPRecoveredState;
begin
  E := Replica;
  try
    ChecaInt('sem exchange', 0, E.Exchanges.Count);
    ChecaInt('sem fila', 0, E.Queues.Count);
    ChecaInt('sem colocacao', 0, E.Entries.Count);
    ChecaInt('maior id', 0, Integer(E.MaxId));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.TopologiaDuravel_Sobrevive;
var
  E: TAMQPRecoveredState;
begin
  DeclaraExchange('ex.a', 'direct');
  DeclaraFila('q.a');
  Liga('ex.a', 'q.a', 'rk');
  E := Replica;
  try
    ChecaInt('um exchange', 1, E.Exchanges.Count);
    ChecaStr('nome do exchange', 'ex.a', E.Exchanges[0].Name);
    ChecaStr('tipo do exchange', 'direct', E.Exchanges[0].ExchangeType);
    ChecaInt('uma fila', 1, E.Queues.Count);
    ChecaStr('nome da fila', 'q.a', E.Queues[0].Name);
    ChecaInt('um binding', 1, E.Bindings.Count);
    ChecaStr('routing key', 'rk', E.Bindings[0].Binding.RoutingKey);
    ChecaNao('e o destino e uma FILA', E.Bindings[0].ParaExchange);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Redeclare_OUltimoVale;
var
  E: TAMQPRecoveredState;
begin
  // Redeclarar nao duplica: o log e' de operacoes, o estado e' o final.
  DeclaraFila('q.a');
  DeclaraFila('q.a', True);
  E := Replica;
  try
    ChecaInt('uma fila, nao duas', 1, E.Queues.Count);
    ChecaOk('e com o auto-delete do ULTIMO declare', E.Queues[0].AutoDelete);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Unbind_TiraOBinding;
var
  E: TAMQPRecoveredState;
begin
  DeclaraExchange('ex.a', 'direct');
  DeclaraFila('q.a');
  Liga('ex.a', 'q.a', 'rk');
  Desliga('ex.a', 'q.a', 'rk');
  E := Replica;
  try
    ChecaInt('nenhum binding', 0, E.Bindings.Count);
    ChecaInt('mas a fila fica', 1, E.Queues.Count);
    ChecaInt('e o exchange tambem', 1, E.Exchanges.Count);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.ApagarFila_LevaBindingsEColocacoes;
var
  E: TAMQPRecoveredState;
begin
  // O teste que uma implementacao ingenua NAO passa: tirar a fila da lista e
  // esquecer o resto ressuscitaria mensagem de fila que nao existe mais.
  DeclaraExchange('ex.a', 'direct');
  DeclaraFila('q.a');
  DeclaraFila('q.b');
  Liga('ex.a', 'q.a', 'rk');
  Liga('ex.a', 'q.b', 'rk');
  Conteudo(1, 'corpo');
  Coloca(2, 1, 'q.a');
  Coloca(3, 1, 'q.b');
  ApagaFila('q.a');
  E := Replica;
  try
    ChecaInt('sobrou uma fila', 1, E.Queues.Count);
    ChecaStr('a que nao foi apagada', 'q.b', E.Queues[0].Name);
    ChecaInt('sobrou um binding', 1, E.Bindings.Count);
    ChecaStr('o da fila viva', 'q.b', E.Bindings[0].Binding.Destination);
    ChecaInt('e uma colocacao so', 1, E.Entries.Count);
    ChecaStr('a da fila viva', 'q.b', E.Entries[0].Queue);
    ChecaInt('a outra contou como orfa de fila', 1, E.Stats.OrfasDeFila);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.ApagarExchange_LevaBindingsDosDoisLados;
var
  E: TAMQPRecoveredState;
begin
  // O exchange do meio e' DESTINO de um binding e ORIGEM de outro (D5).
  // Olhar so' o Source deixaria o primeiro orfao apontando para o que sumiu.
  DeclaraExchange('ex.orig', 'fanout');
  DeclaraExchange('ex.meio', 'fanout');
  DeclaraFila('q.a');
  LigaExchanges('ex.orig', 'ex.meio', '');
  Liga('ex.meio', 'q.a', '');
  ApagaExchange('ex.meio');
  E := Replica;
  try
    ChecaInt('sobrou um exchange', 1, E.Exchanges.Count);
    ChecaStr('o de origem', 'ex.orig', E.Exchanges[0].Name);
    ChecaInt('e NENHUM binding: os dois tocavam o que sumiu', 0,
      E.Bindings.Count);
    ChecaInt('a fila continua de pe', 1, E.Queues.Count);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.EnqSemDeq_FicaVivo;
var
  E: TAMQPRecoveredState;
begin
  // A D27 em uma linha: a ENTREGA nao e' journalada, entao uma mensagem
  // entregue e nao confirmada continua sendo um ENQ vivo e volta pronta.
  DeclaraFila('q.a');
  Conteudo(1, 'corpo');
  Coloca(2, 1, 'q.a');
  E := Replica;
  try
    ChecaInt('uma colocacao viva', 1, E.Entries.Count);
    ChecaInt('nas contagens tambem', 1, E.Stats.Vivas);
    ChecaInt('nada aposentado', 0, E.Stats.Aposentadas);
    ChecaStr('o corpo veio junto', 'corpo',
      AmqpUtf8Decode(E.Contents[E.Entries[0].ContentId].Body));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.EnqComDeq_Some;
var
  E: TAMQPRecoveredState;
begin
  DeclaraFila('q.a');
  Conteudo(1, 'corpo');
  Coloca(2, 1, 'q.a');
  Aposenta(2, 'q.a');
  E := Replica;
  try
    ChecaInt('nada vivo', 0, E.Entries.Count);
    ChecaInt('uma aposentadoria', 1, E.Stats.Aposentadas);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.OrdemDasColocacoes_EAOrdemDoLog;
var
  E: TAMQPRecoveredState;
begin
  // A ordem do log E' a ordem de enfileiramento, e o balde de prioridade esta'
  // dentro do ENQ -- e' o que faz a D27 nao precisar de registro nenhum extra.
  DeclaraFila('q.a');
  Conteudo(1, 'a');
  Coloca(2, 1, 'q.a');
  Conteudo(3, 'b');
  Coloca(4, 3, 'q.a');
  Conteudo(5, 'c');
  Coloca(6, 5, 'q.a');
  Aposenta(4, 'q.a'); // a do meio sai
  E := Replica;
  try
    ChecaInt('duas vivas', 2, E.Entries.Count);
    ChecaStr('1a: a mais antiga', 'a',
      AmqpUtf8Decode(E.Contents[E.Entries[0].ContentId].Body));
    ChecaStr('2a: a mais nova', 'c',
      AmqpUtf8Decode(E.Contents[E.Entries[1].ContentId].Body));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.UmConteudoDuasColocacoes;
var
  E: TAMQPRecoveredState;
begin
  // O pagamento da D22, do lado da leitura: um fan-out para N filas tem UM
  // corpo no arquivo e N colocacoes apontando para ele.
  DeclaraFila('q.a');
  DeclaraFila('q.b');
  Conteudo(1, 'corpo');
  Coloca(2, 1, 'q.a');
  Coloca(3, 1, 'q.b');
  E := Replica;
  try
    ChecaInt('duas colocacoes', 2, E.Entries.Count);
    ChecaInt('um conteudo so', 1, E.Contents.Count);
    ChecaOk('e as duas apontam para ele',
      (E.Entries[0].ContentId = 1) and (E.Entries[1].ContentId = 1));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.ConteudoSemColocacaoViva_ESoltado;
var
  E: TAMQPRecoveredState;
begin
  // Sem isto a recuperacao carregaria na memoria o corpo de TODA mensagem que
  // o broker ja' viu na vida -- o log e' append-only, os corpos ficam la'.
  DeclaraFila('q.a');
  Conteudo(1, 'consumida');
  Coloca(2, 1, 'q.a');
  Aposenta(2, 'q.a');
  Conteudo(3, 'viva');
  Coloca(4, 3, 'q.a');
  E := Replica;
  try
    ChecaInt('uma colocacao viva', 1, E.Entries.Count);
    ChecaInt('e UM conteudo, nao dois', 1, E.Contents.Count);
    ChecaOk('o que ficou e o da viva', E.Contents.ContainsKey(3));
    ChecaNao('o da consumida foi solto', E.Contents.ContainsKey(1));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.MaxId_EOMaiorVisto;
var
  E: TAMQPRecoveredState;
begin
  // Se o contador da engine recomecasse do zero, uma colocacao nova colidiria
  // com uma recuperada, e o DEQ de uma aposentaria a outra. Silencioso e fatal.
  DeclaraFila('q.a');
  Conteudo(7, 'x');
  Coloca(9, 7, 'q.a');
  Aposenta(9, 'q.a'); // aposentada, mas o id JA' FOI USADO
  // O maior id pode estar num CONTENT e nao num ENQ -- os dois saem do MESMO
  // contador da engine. Olhar so' as colocacoes deixaria este passar.
  Conteudo(20, 'y');
  Coloca(12, 20, 'q.a');
  E := Replica;
  try
    ChecaInt('uma viva', 1, E.Entries.Count);
    ChecaInt('e o maior id e o do CONTENT, nao o da colocacao', 20,
      Integer(E.MaxId));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.PrazoEPrioridade_ChegamIntactos;
var
  E: TAMQPRecoveredState;
begin
  // A D21: o replay NAO decide prazo. Ele entrega o instante de parede e o TTL
  // como estao, e quem tem a fila viva na mao recompoe o monotonico.
  DeclaraFila('q.a');
  Conteudo(1, 'x');
  Coloca(2, 1, 'q.a', 5);
  E := Replica;
  try
    ChecaInt('a prioridade veio', 5, Integer(E.Entries[0].Priority));
    ChecaOk('e o instante de parede tambem',
      E.Entries[0].EnqueuedAtWall = 1700000000000);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.CaudaTorta_ReplicaOPrefixo;
var
  E: TAMQPRecoveredState;
  LArq: string;
  LStream: TFileStream;
  LSegs: TArray<Cardinal>;
begin
  // O caso NORMAL depois de uma queda, nao um erro (D23). O prefixo valido
  // e' exatamente o que a recuperacao replica.
  DeclaraFila('q.a');
  Conteudo(1, 'a');
  Coloca(2, 1, 'q.a');
  Conteudo(3, 'b');
  Coloca(4, 3, 'q.a');
  FJournal.Stop;
  FreeAndNil(FJournal);

  // Corta os ultimos bytes: o registro final fica pela metade. O NUMERO do
  // segmento vem da listagem, e nao de um literal: o journal comeca no 1, e
  // adivinhar o nome faria o teste falhar por motivo errado.
  LSegs := AmqpWalListSegments(FDir);
  ChecaInt('um segmento', 1, Length(LSegs));
  LArq := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LSegs[0]);
  LStream := TFileStream.Create(LArq, fmOpenReadWrite or fmShareDenyNone);
  try
    LStream.Size := LStream.Size - 12;
  finally
    LStream.Free;
  end;

  E := AmqpReplayWal(FDir);
  try
    ChecaNao('a leitura NAO chegou ao fim inteira', E.Stats.Parada = awsFim);
    ChecaInt('a fila do prefixo sobreviveu', 1, E.Queues.Count);
    // A ultima colocacao caiu junto com a cauda; a primeira continua la'.
    ChecaInt('uma colocacao viva', 1, E.Entries.Count);
    ChecaStr('a do prefixo', 'a',
      AmqpUtf8Decode(E.Contents[E.Entries[0].ContentId].Body));
  finally
    E.Free;
  end;
end;

{ --- rotacao de segmento (WS7 da Fase 4, D26) --- }

procedure TRecoveryReplayTests.Rotaciona_QuandoOSegmentoEnche;
var
  I: Integer;
  LUltimo: UInt64;
begin
  // Teto minusculo para a rotacao acontecer com poucos registros: o valor real
  // (16 MB) so' torna o teste lento, nao mais verdadeiro.
  FJournal.MaxSegmentBytes := 600;
  DeclaraFila('q.a');
  for I := 1 to 40 do
    LUltimo := Grava(AMQP_REC_CONTENT,
      AmqpEncodeRecContent(ConteudoDe(I, 'corpo-' + IntToStr(I))));
  // BARREIRA: o Submit so ENFILEIRA -- quem escreve (e quem rotaciona) e a
  // thread do journal. Sem esperar a marca d agua, o teste olharia o segmento
  // antes de o primeiro byte ter saido.
  ChecaOk('o lote ficou duravel', FJournal.WaitDurable(LUltimo, 5000));
  ChecaOk('o segmento ativo passou do primeiro',
    FJournal.SegmentoAtivo > 1);
  ChecaOk('e as rotacoes foram contadas',
    FJournal.Stats.Rotacoes >= 1);
end;

procedure TRecoveryReplayTests.Rotaciona_NaoParteOLoteEntreDoisArquivos;
var
  LRecs: TAMQPJournalRecords;
  LSegs: TArray<Cardinal>;
  I, LAchou: Integer;
  LRegs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LN: Integer;
begin
  // A INVARIANTE QUE A ROTACAO NAO PODE QUEBRAR: o lote e' indivisivel (D24).
  // Parti-lo entre dois arquivos daria uma cauda torta ARTIFICIAL no primeiro,
  // e a recuperacao descartaria metade de um publish que ja' foi aceito.
  //
  // OS NUMEROS SAO ESCOLHIDOS PARA FORCAR A JANELA, e nao por acaso: uma
  // versao anterior deste teste usava dois registros pequenos e teto de 500, e
  // a MUTACAO "rotaciona no meio do lote" SOBREVIVIA -- com registros do mesmo
  // tamanho o ponto de corte cai sempre no mesmo lugar, e por sorte era par.
  // Aqui o PRIMEIRO registro do par ja' estoura o teto sozinho: se a rotacao
  // olhasse o tamanho a cada registro, ela partiria TODO par, e a contagem de
  // cada segmento ficaria impar.
  //
  // O WaitDurable entre submits mantem cada lote com exatamente dois registros
  // (sem ele o group commit juntaria tudo num lote so').
  FJournal.MaxSegmentBytes := 300;
  SetLength(LRecs, 2);
  for I := 1 to 8 do
  begin
    LRecs[0].Kind := AMQP_REC_CONTENT;
    LRecs[0].Payload := AmqpEncodeRecContent(
      ConteudoDe(I * 2 - 1, StringOfChar('a', 400)));
    LRecs[1].Kind := AMQP_REC_CONTENT;
    LRecs[1].Payload := AmqpEncodeRecContent(ConteudoDe(I * 2, 'b'));
    ChecaOk('o lote ficou duravel',
      FJournal.WaitDurable(FJournal.Submit(LRecs), 5000));
  end;
  FJournal.Stop;
  FreeAndNil(FJournal);

  LSegs := AmqpWalListSegments(FDir);
  ChecaOk('rotacionou mesmo', Length(LSegs) > 1);
  LAchou := 0;
  for I := 0 to High(LSegs) do
  begin
    LArq := TAMQPWalOsFile.Create(
      IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LSegs[I]), False);
    LSeg := TAMQPWalSegment.OpenExisting(LArq, False);
    try
      LN := LSeg.ReadPrefix(LRegs, LStop);
      Inc(LAchou, LN);
      ChecaInt('nenhum segmento termina com meio lote (segmento '
        + IntToStr(LSegs[I]) + ')', 0, LN mod 2);
    finally
      LSeg.Free;
      LArq := nil;
    end;
  end;
  ChecaInt('e nenhum registro se perdeu na troca', 16, LAchou);
end;

procedure TRecoveryReplayTests.Rotaciona_LsnContinuaEntreSegmentos;
var
  I: Integer;
  LUltimo, LDepois: UInt64;
begin
  // O LSN e' do JOURNAL, nao do arquivo. Se recomecasse a cada segmento, a
  // invariante de LSN crescente da WS1 recusaria o primeiro append do segmento
  // novo -- e, pior, se nao recusasse, a recuperacao leria um log em que o LSN
  // anda para tras no meio.
  //
  // O SUBMIT TEM DE VIR DEPOIS DA ROTACAO. Uma versao anterior deste teste
  // media o LSN de 40 submits feitos ANTES de a thread do journal escrever
  // qualquer coisa -- os LSNs ja' estavam todos atribuidos, e a mutacao que
  // zerava o contador na rotacao SOBREVIVIA. Aqui a rotacao acontece (com
  // barreira), e so' entao se pergunta qual e' o proximo numero.
  FJournal.MaxSegmentBytes := 600;
  LUltimo := 0;
  for I := 1 to 40 do
    LUltimo := Grava(AMQP_REC_CONTENT,
      AmqpEncodeRecContent(ConteudoDe(I, 'x')));
  ChecaOk('o lote ficou duravel', FJournal.WaitDurable(LUltimo, 5000));
  ChecaOk('rotacionou', FJournal.SegmentoAtivo > 1);

  LDepois := Grava(AMQP_REC_CONTENT, AmqpEncodeRecContent(ConteudoDe(99, 'y')));
  ChecaOk('o registro de depois da rotacao ficou duravel',
    FJournal.WaitDurable(LDepois, 5000));
  ChecaOk('o LSN CONTINUOU do outro lado da rotacao, em vez de reiniciar',
    LDepois > LUltimo);
end;

procedure TRecoveryReplayTests.Rotaciona_ReplayLeTodosOsSegmentosComoUmLogSo;
var
  E: TAMQPRecoveredState;
  I: Integer;
begin
  // A recuperacao ja' percorria a lista de segmentos (WS6); este teste e' o
  // que amarra as duas WS: com a rotacao ligada, o estado tem de sair
  // IDENTICO ao de um log de um arquivo so'.
  FJournal.MaxSegmentBytes := 700;
  DeclaraFila('q.a');
  for I := 1 to 20 do
  begin
    Conteudo(I * 2 - 1, 'corpo-' + IntToStr(I));
    Coloca(I * 2, I * 2 - 1, 'q.a');
  end;
  E := Replica;
  try
    ChecaOk('leu mais de um segmento', E.Stats.Segmentos > 1);
    ChecaInt('a fila veio', 1, E.Queues.Count);
    ChecaInt('e as 20 colocacoes tambem', 20, E.Entries.Count);
    ChecaStr('na ordem do log, da primeira', 'corpo-1',
      AmqpUtf8Decode(E.Contents[E.Entries[0].ContentId].Body));
    ChecaStr('a' + ' ultima', 'corpo-20',
      AmqpUtf8Decode(E.Contents[E.Entries[19].ContentId].Body));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Rotaciona_ReabrirContinuaNoUltimoSegmento;
var
  I: Integer;
  LSegDepois: Cardinal;
  LUltimo: UInt64;
begin
  // Um restart nao pode voltar a escrever no PRIMEIRO segmento: sobrescreveria
  // registros vivos. O journal reabre no ULTIMO, que e' o ativo.
  FJournal.MaxSegmentBytes := 600;
  for I := 1 to 40 do
    LUltimo := Grava(AMQP_REC_CONTENT,
      AmqpEncodeRecContent(ConteudoDe(I, 'x')));
  ChecaOk('o lote ficou duravel', FJournal.WaitDurable(LUltimo, 5000));
  LSegDepois := FJournal.SegmentoAtivo;
  ChecaOk('rotacionou antes de fechar', LSegDepois > 1);
  FJournal.Stop;
  FreeAndNil(FJournal);

  FJournal := TAMQPJournal.Create(FDir);
  FJournal.Start;
  ChecaInt('reabriu no ultimo segmento, nao no primeiro',
    Integer(LSegDepois), Integer(FJournal.SegmentoAtivo));
end;

{ --- compactacao (WS7 da Fase 4, D26) --- }

procedure TRecoveryReplayTests.Compacta_EncolheOLogSemMudarOEstado;
var
  E: TAMQPRecoveredState;
  I: Integer;
  LAntes, LDepois: Int64;
begin
  // O que a compactacao promete: MENOS BYTES, MESMO ESTADO. Publica 30 e
  // aposenta 28 -- e' o caso que a D26 descreve, log grande e conteudo vivo
  // pequeno.
  DeclaraFila('q.a');
  for I := 1 to 30 do
  begin
    Conteudo(I * 2 - 1, 'corpo-' + IntToStr(I));
    Coloca(I * 2, I * 2 - 1, 'q.a');
  end;
  for I := 1 to 28 do
    Aposenta(I * 2, 'q.a');
  ChecaOk('tudo duravel',
    FJournal.WaitDurable(FJournal.Stats.ProximoLsn - 1, 5000));
  LAntes := FJournal.TamanhoTotal;

  ChecaOk('compactou', FJournal.Compacta);
  LDepois := FJournal.TamanhoTotal;
  ChecaOk('o log encolheu', LDepois < LAntes);

  E := Replica;
  try
    ChecaInt('a fila continua la', 1, E.Queues.Count);
    ChecaInt('e as duas vivas tambem', 2, E.Entries.Count);
    ChecaStr('a primeira viva e a de numero 29', 'corpo-29',
      AmqpUtf8Decode(E.Contents[E.Entries[0].ContentId].Body));
    ChecaInt('e o conteudo das aposentadas foi embora', 2, E.Contents.Count);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Compacta_ApagaOsSegmentosVelhos;
var
  I: Integer;
  LLsn: UInt64;
  LSegsAntes, LSegsDepois: TArray<Cardinal>;
begin
  // UMA BARREIRA POR REGISTRO, e nao uma no fim. O group commit junta tudo o
  // que estiver na fila num lote so', e a rotacao acontece na FRONTEIRA DO
  // LOTE -- entao 60 submits em rajada podem virar UM lote e UMA rotacao, e o
  // numero de segmentos passa a depender de quem corre mais rapido, o laco de
  // teste ou a thread do journal. Foi o que aconteceu: verde no FPC, vermelho
  // no Delphi. Esperando cada registro ficar duravel, cada um e' o seu proprio
  // lote e a contagem de segmentos vira funcao do tamanho, nao do escalonador.
  FJournal.MaxSegmentBytes := 300;
  DeclaraFila('q.a');
  for I := 1 to 30 do
  begin
    LLsn := Grava(AMQP_REC_CONTENT,
      AmqpEncodeRecContent(ConteudoDe(I, 'corpo-' + IntToStr(I))));
    ChecaOk('registro duravel', FJournal.WaitDurable(LLsn, 5000));
  end;
  LSegsAntes := AmqpWalListSegments(FDir);
  ChecaOk('havia varios segmentos', Length(LSegsAntes) > 2);

  ChecaOk('compactou', FJournal.Compacta);
  LSegsDepois := AmqpWalListSegments(FDir);
  // Nenhuma colocacao viva (so' conteudo solto, que ninguem referencia): sobra
  // o segmento novo com a topologia e mais nada.
  ChecaInt('sobrou UM segmento', 1, Length(LSegsDepois));
  ChecaOk('e ele e mais novo que todos os apagados',
    LSegsDepois[0] > LSegsAntes[High(LSegsAntes)]);
  ChecaOk('os apagados foram contados',
    FJournal.Stats.SegmentosApagados >= Length(LSegsAntes));
end;

procedure TRecoveryReplayTests.Compacta_PreservaOsIdentificadores;
var
  E: TAMQPRecoveredState;
begin
  // OS IDENTIFICADORES SAO O QUE TORNA A REESCRITA IDEMPOTENTE. Se a
  // compactacao renumerasse, um log com as duas copias (queda entre escrever o
  // novo e apagar o velho) descreveria DUAS mensagens em vez de uma.
  DeclaraFila('q.a');
  Conteudo(7, 'x');
  Coloca(9, 7, 'q.a');
  ChecaOk('tudo duravel',
    FJournal.WaitDurable(FJournal.Stats.ProximoLsn - 1, 5000));
  ChecaOk('compactou', FJournal.Compacta);
  E := Replica;
  try
    ChecaInt('uma colocacao', 1, E.Entries.Count);
    ChecaInt('com o MESMO EntryId de antes', 9, Integer(E.Entries[0].EntryId));
    ChecaInt('e o MESMO ContentId', 7, Integer(E.Entries[0].ContentId));
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Compacta_NaoDuplicaSeOsVelhosSobrarem;
var
  E: TAMQPRecoveredState;
  LEnq: TAMQPRecEnqueue;
  LCont: TAMQPRecContent;
  LLsn: UInt64;
begin
  // A QUEDA ENTRE O PASSO 4 E O 5, simulada: o segmento novo ja' esta' no
  // disco e o velho ainda nao foi apagado. E' o unico ponto em que a
  // compactacao pode ser interrompida sem que o fsync tenha decidido, e o log
  // resultante tem de descrever UMA mensagem, nao duas.
  //
  // Em vez de derrubar o processo (que nao testa fsync, D28), monto a
  // situacao a mao: escrevo a MESMA colocacao duas vezes, que e' exatamente o
  // arquivo que uma compactacao interrompida deixaria.
  DeclaraFila('q.a');
  Conteudo(5, 'unica');
  Coloca(6, 5, 'q.a');
  LCont.ContentId := 5;
  LCont.UserId := 'guest';
  LCont.Body := AmqpUtf8Encode('unica');
  LEnq.VHost := '/';
  LEnq.Queue := 'q.a';
  LEnq.EntryId := 6;      // O MESMO id
  LEnq.ContentId := 5;
  LEnq.Exchange := '';
  LEnq.RoutingKey := 'q.a';
  LEnq.Priority := 0;
  LEnq.EnqueuedAtWall := 1700000000000;
  LEnq.TtlMs := -1;
  LEnq.HeaderPayload := nil;
  Grava(AMQP_REC_CONTENT, AmqpEncodeRecContent(LCont));
  LLsn := Grava(AMQP_REC_ENQUEUE, AmqpEncodeRecEnqueue(LEnq));
  ChecaOk('tudo duravel', FJournal.WaitDurable(LLsn, 5000));

  E := Replica;
  try
    ChecaInt('UMA colocacao, nao duas', 1, E.Entries.Count);
    ChecaInt('e a repetida foi contada', 1, E.Stats.Duplicadas);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Compacta_PreservaAOrdemEOsPrazos;
var
  E: TAMQPRecoveredState;
begin
  // O registro reescrito tem de sair campo a campo igual: se a compactacao
  // perdesse o instante de parede ou o balde, a mensagem voltaria do restart
  // com prazo renovado (D21) ou na prioridade errada (D12) -- e nenhum teste
  // de "a mensagem sobreviveu" perceberia.
  DeclaraFila('q.a');
  Conteudo(1, 'primeira');
  Coloca(2, 1, 'q.a', 3);
  Conteudo(3, 'segunda');
  Coloca(4, 3, 'q.a', 7);
  ChecaOk('tudo duravel',
    FJournal.WaitDurable(FJournal.Stats.ProximoLsn - 1, 5000));
  ChecaOk('compactou', FJournal.Compacta);
  E := Replica;
  try
    ChecaInt('as duas', 2, E.Entries.Count);
    ChecaStr('na ordem do log original', 'primeira',
      AmqpUtf8Decode(E.Contents[E.Entries[0].ContentId].Body));
    ChecaInt('com a prioridade da primeira', 3, Integer(E.Entries[0].Priority));
    ChecaInt('e a da segunda', 7, Integer(E.Entries[1].Priority));
    ChecaOk('e o instante de parede intacto',
      E.Entries[0].EnqueuedAtWall = 1700000000000);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Compacta_LogInteiroMorto_NaoSobraColocacao;
var
  E: TAMQPRecoveredState;
  I: Integer;
begin
  DeclaraFila('q.a');
  for I := 1 to 10 do
  begin
    Conteudo(I * 2 - 1, 'x');
    Coloca(I * 2, I * 2 - 1, 'q.a');
    Aposenta(I * 2, 'q.a');
  end;
  ChecaOk('tudo duravel',
    FJournal.WaitDurable(FJournal.Stats.ProximoLsn - 1, 5000));
  ChecaOk('compactou', FJournal.Compacta);
  E := Replica;
  try
    ChecaInt('nenhuma colocacao', 0, E.Entries.Count);
    ChecaInt('nenhum conteudo', 0, E.Contents.Count);
    ChecaInt('mas a TOPOLOGIA fica -- ela nao morre com as mensagens', 1,
      E.Queues.Count);
  finally
    E.Free;
  end;
end;

procedure TRecoveryReplayTests.Compacta_NoStart_QuandoOLogPassaDoLimiar;
var
  I: Integer;
  LLsn: UInt64;
  LAntes, LDepois: Int64;
  J: TAMQPJournal;
begin
  // A compactacao SO' roda com o journal quiescente -- hoje, dentro do Start.
  // Nao ha gatilho em voo, e a razao esta' no cabecalho de AMQP.Server.Journal:
  // o LSN e' atribuido no Submit, e a compactacao atribui LSNs NOVOS; um
  // publish que pegou o seu LSN antes e ainda esta' na fila seria escrito
  // DEPOIS, com LSN menor, e o Append recusaria. A versao anterior deste teste
  // disparava a compactacao na rotacao e foi ela que destapou o defeito.
  //
  // Aqui o equivalente do Start: escreve, fecha o journal, reabre e compacta
  // com ninguem mais submetendo.
  DeclaraFila('q.a');
  for I := 1 to 30 do
  begin
    Conteudo(I * 2 - 1, 'corpo-' + IntToStr(I));
    Coloca(I * 2, I * 2 - 1, 'q.a');
    LLsn := Grava(AMQP_REC_DEQUEUE,
      AmqpEncodeRecDequeue(DequeueDe(I * 2, 'q.a')));
  end;
  ChecaOk('tudo duravel', FJournal.WaitDurable(LLsn, 5000));
  LAntes := FJournal.TamanhoTotal;
  FJournal.Stop;
  FreeAndNil(FJournal);

  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    ChecaOk('o log esta acima do limiar', J.TamanhoTotal >= 1500);
    ChecaOk('compactou', J.Compacta);
    LDepois := J.TamanhoTotal;
    ChecaOk('e encolheu', LDepois < LAntes);
    ChecaOk('apagando segmento', J.Stats.SegmentosApagados >= 1);
  finally
    J.Stop;
    J.Free;
  end;
  FJournal := nil;
end;

procedure TRecoveryReplayTests.Compacta_FsyncReprovado_NaoApagaNada;
var
  J: TJournalComSyncFalho;
  LSegsAntes, LSegsDepois: TArray<Cardinal>;
  LRecs: TAMQPJournalRecords;
  I: Integer;
begin
  // A INVARIANTE DE SEGURANCA DA COMPACTACAO: enquanto o fsync do segmento
  // novo nao for aceito, os velhos NAO PODEM SER APAGADOS. Errar para o lado
  // de um log maior do que precisava e' o erro certo a cometer; errar para o
  // outro lado apaga o unico lugar onde as mensagens existem.
  //
  // Uma mutacao que tirasse a checagem do Sync passava em toda a suite ate'
  // este teste existir -- em processo, o fsync nunca falha sozinho.
  FJournal.Stop;
  FreeAndNil(FJournal);
  J := TJournalComSyncFalho.Create(FDir);
  try
    J.Start;
    J.MaxSegmentBytes := 300;
    SetLength(LRecs, 1);
    for I := 1 to 40 do
    begin
      LRecs[0].Kind := AMQP_REC_CONTENT;
      LRecs[0].Payload := AmqpEncodeRecContent(ConteudoDe(I, 'x'));
      J.Submit(LRecs);
    end;
    ChecaOk('tudo duravel', J.WaitDurable(J.Stats.ProximoLsn - 1, 5000));
    LSegsAntes := AmqpWalListSegments(FDir);
    ChecaOk('havia varios segmentos', Length(LSegsAntes) > 1);

    J.ReprovaOSync;
    ChecaNao('a compactacao se recusa a concluir', J.Compacta);
    LSegsDepois := AmqpWalListSegments(FDir);
    ChecaOk('e NENHUM segmento velho foi apagado',
      Length(LSegsDepois) >= Length(LSegsAntes));
  finally
    try
      J.Stop;
    except
    end;
    J.Free;
  end;
  // o TearDown espera FJournal <> nil ou nil; ja' foi liberado acima
  FJournal := nil;
end;

procedure TRecoveryReplayTests.Compacta_FanOut_GravaOCorpoUmaVezSo;
var
  LSegs: TArray<Cardinal>;
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LRegs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LN, I, J, LConteudos, LColocacoes: Integer;
begin
  // O PAGAMENTO DA D22, do lado da compactacao: um fan-out para tres filas tem
  // UM corpo e TRES colocacoes. Se a reescrita gravasse o corpo por colocacao,
  // o estado sairia igual -- e o log tres vezes maior, que e' justamente o que
  // a compactacao existe para evitar. Nenhum teste de estado percebe.
  DeclaraFila('q.a');
  DeclaraFila('q.b');
  DeclaraFila('q.c');
  Conteudo(1, 'compartilhado');
  Coloca(2, 1, 'q.a');
  Coloca(3, 1, 'q.b');
  Coloca(4, 1, 'q.c');
  ChecaOk('tudo duravel',
    FJournal.WaitDurable(FJournal.Stats.ProximoLsn - 1, 5000));
  ChecaOk('compactou', FJournal.Compacta);
  FJournal.Stop;
  FreeAndNil(FJournal);

  LConteudos := 0;
  LColocacoes := 0;
  LSegs := AmqpWalListSegments(FDir);
  for I := 0 to High(LSegs) do
  begin
    LArq := TAMQPWalOsFile.Create(
      IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LSegs[I]), False);
    LSeg := TAMQPWalSegment.OpenExisting(LArq, False);
    try
      LN := LSeg.ReadPrefix(LRegs, LStop);
      for J := 0 to LN - 1 do
        if LRegs[J].Kind = AMQP_REC_CONTENT then
          Inc(LConteudos)
        else if LRegs[J].Kind = AMQP_REC_ENQUEUE then
          Inc(LColocacoes);
    finally
      LSeg.Free;
      LArq := nil;
    end;
  end;
  ChecaInt('UM registro de conteudo no log compactado', 1, LConteudos);
  ChecaInt('e tres colocacoes', 3, LColocacoes);
end;

{ --- teto duro do log (WS7 da Fase 4, D26) --- }

procedure TRecoveryReplayTests.Teto_ZeroEIlimitado;
var
  I: Integer;
  LLsn: UInt64;
begin
  // 0 = ilimitado, a forma do MaxQueueLength da D7. E' o DEFAULT: ninguem
  // ganha um teto de disco por ter ligado a durabilidade.
  ChecaInt('o default e zero', 0, Integer(FJournal.MaxJournalBytes));
  for I := 1 to 40 do
    LLsn := Grava(AMQP_REC_CONTENT,
      AmqpEncodeRecContent(ConteudoDe(I, StringOfChar('x', 200))));
  ChecaOk('tudo duravel', FJournal.WaitDurable(LLsn, 5000));
  ChecaNao('e o journal nunca se declara cheio', FJournal.Cheio);
end;

procedure TRecoveryReplayTests.Teto_Estourado_MarcaCheio;
var
  I: Integer;
  LLsn: UInt64;
begin
  FJournal.MaxJournalBytes := 2000;
  ChecaNao('comeca vazio', FJournal.Cheio);
  for I := 1 to 40 do
    LLsn := Grava(AMQP_REC_CONTENT,
      AmqpEncodeRecContent(ConteudoDe(I, StringOfChar('x', 200))));
  ChecaOk('tudo duravel', FJournal.WaitDurable(LLsn, 5000));
  ChecaOk('passou do teto e se declara cheio', FJournal.Cheio);
  ChecaOk('e o tamanho relatado bate com o teto',
    FJournal.Stats.Bytes >= 2000);
end;

procedure TRecoveryReplayTests.Teto_RetiradaNuncaERecusada;
var
  I: Integer;
  LLsn: UInt64;
  E: TAMQPRecoveredState;
begin
  // A REGRA QUE SALVA O BROKER DE SE TRANCAR: o teto vale para o caminho do
  // PUBLICADOR. O registro de RETIRADA passa sempre -- alem de a D24 proibir
  // barrar o ator, recusar um DEQ ressuscitaria a mensagem no proximo boot, e
  // sao justamente os DEQs que deixam a compactacao encolher o log e SAIR
  // desta situacao. Recusa-los seria fechar a unica porta.
  DeclaraFila('q.a');
  Conteudo(1, 'x');
  Coloca(2, 1, 'q.a');
  FJournal.MaxJournalBytes := 1; // cheio desde ja
  ChecaOk('cheio', FJournal.Cheio);
  LLsn := FJournal.SubmitNoWait(RegistroDe(AMQP_REC_DEQUEUE,
    AmqpEncodeRecDequeue(DequeueDe(2, 'q.a'))));
  ChecaOk('o DEQ foi aceito mesmo com o log cheio', LLsn > 0);
  ChecaOk('e ficou duravel', FJournal.WaitDurable(LLsn, 5000));
  E := Replica;
  try
    ChecaInt('a colocacao foi mesmo aposentada', 0, E.Entries.Count);
  finally
    E.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRecoveryReplayTests);

end.
