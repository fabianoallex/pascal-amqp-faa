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

  Espelho de tests\Server\AMQP.ServerRecoveryTests.pas -- mantenha os dois em sincronia. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  AMQP.Wire,
  AMQP.Server.Wal,
  AMQP.Server.Journal,
  AMQP.Server.Records,
  AMQP.Server.Recovery;

type
  { Escreve um WAL a mao e le de volta. }
  TRecoveryReplayTests = class(TTestCase)
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
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
     procedure LogVazio_EstadoVazio;
     procedure TopologiaDuravel_Sobrevive;
     procedure Redeclare_OUltimoVale;
     procedure Unbind_TiraOBinding;
     procedure ApagarFila_LevaBindingsEColocacoes;
     procedure ApagarExchange_LevaBindingsDosDoisLados;
     procedure EnqSemDeq_FicaVivo;
     procedure EnqComDeq_Some;
     procedure OrdemDasColocacoes_EAOrdemDoLog;
     procedure UmConteudoDuasColocacoes;
     procedure ConteudoSemColocacaoViva_ESoltado;
     procedure MaxId_EOMaiorVisto;
     procedure PrazoEPrioridade_ChegamIntactos;
     procedure CaudaTorta_ReplicaOPrefixo;
    // --- WS7: rotacao de segmento ---
    procedure Rotaciona_QuandoOSegmentoEnche;
    procedure Rotaciona_NaoParteOLoteEntreDoisArquivos;
    procedure Rotaciona_LsnContinuaEntreSegmentos;
    procedure Rotaciona_ReplayLeTodosOsSegmentosComoUmLogSo;
    procedure Rotaciona_ReabrirContinuaNoUltimoSegmento;
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
          SysUtils.DeleteFile(IncludeTrailingPathDelimiter(ADir) + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      SysUtils.FindClose(LRec);
    end;
  end;
end;

// As pontes para as assercoes do dialeto -- ver o cabecalho.
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaStr(const AMsg, AEsperado, AObtido: string);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;

procedure ChecaNao(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertFalse(AMsg, ACond);
end;

/// Monta um registro de conteudo (as duas suites de rotacao precisam do mesmo).
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
  inherited SetUp;
  Inc(GRecSeq);
  FDir := IncludeTrailingPathDelimiter(GetTempDir) + 'amqprec-'
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
  inherited TearDown;
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

initialization
  RegisterTest(TRecoveryReplayTests);

end.
