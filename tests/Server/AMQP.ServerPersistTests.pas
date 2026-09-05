unit AMQP.ServerPersistTests;

{ Testes da mensagem persistente -- WS4 da Fase 4 (durabilidade, ver
  CLAUDE.md, decisoes D19-D28).

  Por ora, os registros de CONTEUDO e COLOCACAO (D22): o corpo escrito uma vez
  e compartilhado, e uma colocacao por (fila, mensagem) carregando o header
  proprio -- que e' o que faz o dead-letter deixar de ser caso especial.

  O TESTE QUE MAIS IMPORTA AQUI e' o do corpo binario. O blob e' escrito como
  comprimento + bytes crus justamente porque `WriteLongStr` trataria o valor
  como TEXTO e o passaria por `AmqpUtf8Encode`; um corpo com bytes fora de
  ASCII sairia transcodificado e ninguem perceberia ate' a recuperacao entregar
  lixo. O teste varre os 256 valores de byte, inclusive o zero.

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerPersistTests.pas -- mantenha
  os dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.Rtti,
  System.IOUtils,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Connection,
  AMQP.Server.Wal,
  AMQP.Server.Records,
  AMQP.Server.Broker;

type
  [TestFixture]
  TDataRecordTests = class
  public
    [Test] procedure Content_RoundTrip;
    [Test] procedure Content_CorpoBinarioSobreviveIntacto;
    [Test] procedure Content_CorpoBinario_NaoDependeDoCodepageDaApp;
    [Test] procedure Content_CorpoVazio;
    [Test] procedure Enqueue_RoundTrip;
    [Test] procedure Enqueue_TtlAusenteSobreviveComoMenosUm;
    [Test] procedure Enqueue_HeaderBinarioSobreviveIntacto;
    [Test] procedure Dequeue_RoundTrip;
    [Test] procedure KindsDeDados_SaoReconhecidos;
  end;


  { Sobe um broker com DataDir proprio, publica com o cliente de verdade e
    depois le' o WAL. E' o unico jeito honesto de testar a fiacao: os registros
    so' aparecem se o publish REAL atravessou a engine, a fila e o journal. }
  TPersistFixture = class(TObject)
  protected
    FBroker: TAMQPServer;
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FDir: string;
    [Setup]    procedure SetUp;
    [TearDown] procedure TearDown;
    function DeclaraFila(const ANome: string; ADurable: Boolean;
      AArgs: TAMQPFieldTable = nil): string;
    /// Fecha o cliente, para o broker e devolve um resumo do WAL.
    function FechaELeWal: TStringList;
    function Conta(L: TStringList; const APrefixo: string): Integer;
    /// Indice da primeira linha que comeca com APrefixo, ou -1.
    function Indice(L: TStringList; const APrefixo: string): Integer;
  end;

  [TestFixture]
  TPersistWiringTests = class(TPersistFixture)
  public
    [Test] procedure Persistente_EmFilaDuravel_GravaConteudoEColocacao;
    [Test] procedure Transiente_EmFilaDuravel_NaoGravaNada;
    [Test] procedure Persistente_EmFilaTransiente_NaoGravaNada;
    [Test] procedure DuasFilasDuraveis_UmConteudoDuasColocacoes;
    [Test] procedure Ack_AposentaAColocacao;
    [Test] procedure GetSemAck_AposentaNaHora;
    [Test] procedure DeadLetter_ColocacaoNovaAntesDeAposentarAVelha;
    [Test] procedure Confirm_ComDataDir_OCanalPassaAAdiar;
    [Test] procedure Confirm_PublishPersistente_ChegaComOJournalJaDuravel;
    [Test] procedure Confirm_LoteDePersistentes_TodosConfirmam;
    [Test] procedure Confirm_PersistenteETransienteMisturados_TodosConfirmam;
  end;

implementation

// Ponte para a assercao do dialeto (mesma ideia de AMQP.ServerConfirmTests).
procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  Assert.IsTrue(ACond, AMsg);
end;

function BytesIguais(const A, B: TBytes): Boolean;
var
  I: Integer;
begin
  Result := Length(A) = Length(B);
  if not Result then
    Exit;
  for I := 0 to High(A) do
    if A[I] <> B[I] then
      Exit(False);
end;

function TodosOsBytes: TBytes;
var
  I: Integer;
begin
  SetLength(Result, 256);
  for I := 0 to 255 do
    Result[I] := Byte(I);
end;

{ TDataRecordTests }

procedure TDataRecordTests.Content_RoundTrip;
var
  R, R2: TAMQPRecContent;
  LB: TBytes;
begin
  SetLength(LB, 3);
  LB[0] := 1;
  LB[1] := 2;
  LB[2] := 3;
  R.ContentId := $0102030405060708;
  R.UserId := 'guest';
  R.Body := LB;
  R2 := AmqpDecodeRecContent(AmqpEncodeRecContent(R));
  Assert.AreEqual(Int64($0102030405060708), Int64(R2.ContentId),
    'content-id de 64 bits');
  Assert.AreEqual('guest', R2.UserId, 'user-id');
  Assert.IsTrue(BytesIguais(LB, R2.Body), 'corpo igual');
end;

procedure TDataRecordTests.Content_CorpoBinarioSobreviveIntacto;
var
  R, R2: TAMQPRecContent;
  LB: TBytes;
begin
  // Os 256 valores de byte, inclusive o zero e tudo acima de 127. Se alguem
  // "simplificar" o blob para WriteLongStr, o corpo passa por AmqpUtf8Encode e
  // este teste cai -- que e' exatamente o ponto dele.
  LB := TodosOsBytes;
  R.ContentId := 1;
  R.UserId := '';
  R.Body := LB;
  R2 := AmqpDecodeRecContent(AmqpEncodeRecContent(R));
  Assert.AreEqual(256, Length(R2.Body), '256 bytes de volta');
  Assert.IsTrue(BytesIguais(LB, R2.Body),
    'todos os 256 valores de byte sobrevivem');
end;

procedure TDataRecordTests.Content_CorpoBinario_NaoDependeDoCodepageDaApp;
var
  R, R2: TAMQPRecContent;
  LB: TBytes;
  {$IFDEF FPC}
  LCpAntes: TSystemCodePage;
  {$ENDIF}
begin
  // ESTE teste existe por causa de uma mutacao que SOBREVIVEU.
  //
  // Trocar o blob por WriteLongStr (isto e', fazer o corpo passar por string)
  // nao derrubava nada -- e a razao, medida, e' que o runner de teste chama
  // SetMultiByteConversionCodePage(CP_UTF8), e com DefaultSystemCodePage=65001
  // o AmqpUtf8Encode vira passthrough: 256 bytes entram, 256 bytes saem.
  //
  // So' que o broker e' uma LIB, embutida na app de terceiros -- e uma app
  // console FPC pura tem codepage 1252 por padrao (esta' no CLAUDE.md). Nesse
  // caso, medido, os mesmos 256 bytes viram 401 e o corpo sai corrompido.
  // Entao o teste fixa o codepage do jeito que o mundo real pode entrega-lo,
  // em vez de confiar no que o runner arrumou para si.
  LB := TodosOsBytes;
  R.ContentId := 1;
  R.UserId := 'guest';
  R.Body := LB;
  {$IFDEF FPC}
  LCpAntes := DefaultSystemCodePage;
  DefaultSystemCodePage := 1252;
  try
  {$ENDIF}
    R2 := AmqpDecodeRecContent(AmqpEncodeRecContent(R));
  {$IFDEF FPC}
  finally
    DefaultSystemCodePage := LCpAntes;
  end;
  {$ENDIF}
  Assert.AreEqual(256, Length(R2.Body), '256 bytes de volta');
  Assert.IsTrue(BytesIguais(LB, R2.Body),
    'o corpo nao depende do codepage da app');
end;

procedure TDataRecordTests.Content_CorpoVazio;
var
  R, R2: TAMQPRecContent;
begin
  // Mensagem sem corpo e' legitima em AMQP (um evento pode ser so' o header).
  R.ContentId := 7;
  R.UserId := 'u';
  R.Body := nil;
  R2 := AmqpDecodeRecContent(AmqpEncodeRecContent(R));
  Assert.AreEqual(0, Length(R2.Body), 'corpo vazio continua vazio');
  Assert.AreEqual(Int64(7), Int64(R2.ContentId), 'e o resto sobrevive');
end;

procedure TDataRecordTests.Enqueue_RoundTrip;
var
  R, R2: TAMQPRecEnqueue;
  LH: TBytes;
begin
  SetLength(LH, 2);
  LH[0] := $60;
  LH[1] := $00;
  R.VHost := '/';
  R.Queue := 'q.pedidos';
  R.EntryId := 42;
  R.ContentId := 99;
  R.Exchange := 'ex.topico';
  R.RoutingKey := 'a.b';
  R.Priority := 5;
  R.EnqueuedAtWall := 1788533217385;
  R.TtlMs := 60000;
  R.HeaderPayload := LH;
  R2 := AmqpDecodeRecEnqueue(AmqpEncodeRecEnqueue(R));
  Assert.AreEqual('/', R2.VHost, 'vhost');
  Assert.AreEqual('q.pedidos', R2.Queue, 'fila');
  Assert.AreEqual(Int64(42), Int64(R2.EntryId), 'entry-id');
  Assert.AreEqual(Int64(99), Int64(R2.ContentId), 'content-id');
  Assert.AreEqual('ex.topico', R2.Exchange, 'exchange');
  Assert.AreEqual('a.b', R2.RoutingKey, 'routing-key');
  Assert.AreEqual(5, Integer(R2.Priority), 'prioridade');
  Assert.AreEqual(Int64(1788533217385), R2.EnqueuedAtWall,
    'instante de parede');
  Assert.AreEqual(Int64(60000), R2.TtlMs, 'ttl');
  Assert.IsTrue(BytesIguais(LH, R2.HeaderPayload), 'header igual');
end;

procedure TDataRecordTests.Enqueue_TtlAusenteSobreviveComoMenosUm;
var
  R, R2: TAMQPRecEnqueue;
begin
  // -1 e' "sem prazo" em toda a Fase 3, e ele atravessa o arquivo como UInt64.
  // Se o cast de ida e volta estiver errado, vira 18 quintilhoes de ms e a
  // mensagem nunca expira -- falha silenciosa e dificil de achar depois.
  R.VHost := '/';
  R.Queue := 'q';
  R.EntryId := 1;
  R.ContentId := 1;
  R.Exchange := '';
  R.RoutingKey := '';
  R.Priority := 0;
  R.EnqueuedAtWall := 0;
  R.TtlMs := -1;
  R.HeaderPayload := nil;
  R2 := AmqpDecodeRecEnqueue(AmqpEncodeRecEnqueue(R));
  Assert.AreEqual(Int64(-1), R2.TtlMs, 'ttl -1 continua -1');
end;

procedure TDataRecordTests.Enqueue_HeaderBinarioSobreviveIntacto;
var
  R, R2: TAMQPRecEnqueue;
  LH: TBytes;
begin
  // Mesma razao do corpo: o content-header cru (decisao D1) e' binario.
  LH := TodosOsBytes;
  R.VHost := '/';
  R.Queue := 'q';
  R.EntryId := 1;
  R.ContentId := 1;
  R.Exchange := '';
  R.RoutingKey := '';
  R.Priority := 0;
  R.EnqueuedAtWall := 0;
  R.TtlMs := -1;
  R.HeaderPayload := LH;
  R2 := AmqpDecodeRecEnqueue(AmqpEncodeRecEnqueue(R));
  Assert.IsTrue(BytesIguais(LH, R2.HeaderPayload), 'header binario intacto');
end;

procedure TDataRecordTests.Dequeue_RoundTrip;
var
  R, R2: TAMQPRecDequeue;
begin
  R.VHost := '/vh';
  R.Queue := 'q.morta';
  R.EntryId := $FFFFFFFFFFFFFFF;
  R2 := AmqpDecodeRecDequeue(AmqpEncodeRecDequeue(R));
  Assert.AreEqual('/vh', R2.VHost, 'vhost');
  Assert.AreEqual('q.morta', R2.Queue, 'fila');
  Assert.AreEqual(Int64($FFFFFFFFFFFFFFF), Int64(R2.EntryId),
    'entry-id grande');
end;

procedure TDataRecordTests.KindsDeDados_SaoReconhecidos;
begin
  Assert.IsTrue(AmqpIsDataRecord(AMQP_REC_CONTENT), 'conteudo');
  Assert.IsTrue(AmqpIsDataRecord(AMQP_REC_ENQUEUE), 'colocacao');
  Assert.IsTrue(AmqpIsDataRecord(AMQP_REC_DEQUEUE), 'aposentadoria');
  // Os dois conjuntos nao podem se cruzar: e' o que separa os passes da
  // recuperacao (WS6).
  Assert.IsFalse(AmqpIsDataRecord(AMQP_REC_QUEUE_DECLARE),
    'topologia nao e dado');
  Assert.IsFalse(AmqpIsTopoRecord(AMQP_REC_ENQUEUE), 'dado nao e topologia');
end;


var
  GSeq: Integer = 0;

procedure LimpaDir(const ADir: string);
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

// Uma linha por registro, legivel, para a falha dizer o que apareceu.
function LinhaDe(const ARec: TAMQPWalRecord): string;
var
  LC: TAMQPRecContent;
  LE: TAMQPRecEnqueue;
  LD: TAMQPRecDequeue;
begin
  case ARec.Kind of
    AMQP_REC_CONTENT:
      begin
        LC := AmqpDecodeRecContent(ARec.Payload);
        Result := 'content:' + IntToStr(LC.ContentId) + ':corpo='
          + IntToStr(Length(LC.Body));
      end;
    AMQP_REC_ENQUEUE:
      begin
        LE := AmqpDecodeRecEnqueue(ARec.Payload);
        Result := 'enqueue:' + LE.Queue + ':entry=' + IntToStr(LE.EntryId)
          + ':content=' + IntToStr(LE.ContentId)
          + ':prio=' + IntToStr(LE.Priority) + ':ttl=' + IntToStr(LE.TtlMs);
      end;
    AMQP_REC_DEQUEUE:
      begin
        LD := AmqpDecodeRecDequeue(ARec.Payload);
        Result := 'dequeue:' + LD.Queue + ':entry=' + IntToStr(LD.EntryId);
      end;
  else
    Result := AmqpRecordKindName(ARec.Kind);
  end;
end;

function LeWal(const ADir: string): TStringList;
var
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LRegs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LSegs: TArray<Cardinal>;
  I, LN: Integer;
begin
  Result := TStringList.Create;
  LSegs := AmqpWalListSegments(ADir);
  if Length(LSegs) = 0 then
    Exit;
  LArq := TAMQPWalOsFile.Create(
    IncludeTrailingPathDelimiter(ADir) + AmqpWalSegmentName(LSegs[0]), False);
  LSeg := TAMQPWalSegment.OpenExisting(LArq, False);
  try
    LN := LSeg.ReadPrefix(LRegs, LStop);
    for I := 0 to LN - 1 do
      Result.Add(LinhaDe(LRegs[I]));
  finally
    LSeg.Free;
    LArq := nil;
  end;
end;

{ TPersistFixture }

procedure TPersistFixture.SetUp;
var
  LParams: TAMQPConnectionParams;
begin
  
  Inc(GSeq);
  FDir := IncludeTrailingPathDelimiter(TPath.GetTempPath) + 'amqppersist-'
    + IntToStr(GSeq);
  ForceDirectories(FDir);
  LimpaDir(FDir);
  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '127.0.0.1';
  FBroker.Port := 0;
  FBroker.DataDir := FDir;
  FBroker.Start;
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := FBroker.Port;
  FConn := TAMQPConnection.Create(LParams);
  FConn.Open;
  FChan := FConn.CreateChannel;
end;

procedure TPersistFixture.TearDown;
begin
  if FConn <> nil then
  begin
    try
      FConn.Close;
    except
    end;
    FreeAndNil(FConn);
  end;
  if FBroker <> nil then
  begin
    try
      FBroker.Stop;
    except
    end;
    FreeAndNil(FBroker);
  end;
  LimpaDir(FDir);
  
end;

function TPersistFixture.DeclaraFila(const ANome: string; ADurable: Boolean;
  AArgs: TAMQPFieldTable): string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := TAMQPQueueDeclare.Create(ANome, ADurable);
  LDecl.Arguments := AArgs;
  Result := FChan.DeclareQueue(LDecl).QueueName;
end;

function TPersistFixture.FechaELeWal: TStringList;
begin
  FConn.Close;
  FreeAndNil(FConn);
  FBroker.Stop;
  FreeAndNil(FBroker);
  Result := LeWal(FDir);
end;

function TPersistFixture.Conta(L: TStringList;
  const APrefixo: string): Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := 0 to L.Count - 1 do
    if Pos(APrefixo, L[I]) = 1 then
      Inc(Result);
end;

function TPersistFixture.Indice(L: TStringList;
  const APrefixo: string): Integer;
var
  I: Integer;
begin
  Result := -1;
  for I := 0 to L.Count - 1 do
    if Pos(APrefixo, L[I]) = 1 then
      Exit(I);
end;

{ TPersistWiringTests }

procedure TPersistWiringTests.Persistente_EmFilaDuravel_GravaConteudoEColocacao;
var
  L: TStringList;
begin
  // A regra dos tres da D20, no caso em que ela diz SIM.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'carga', True);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'content:'), 'um conteudo');
    Assert.AreEqual(1, Conta(L, 'enqueue:q.dur:'), 'uma colocacao na fila');
    Assert.AreEqual(0, Conta(L, 'dequeue:'), 'e nada foi aposentado');
  finally
    L.Free;
  end;
end;

procedure TPersistWiringTests.Transiente_EmFilaDuravel_NaoGravaNada;
var
  L: TStringList;
begin
  // delivery-mode 1 numa fila DURAVEL: a fila e' duravel, a mensagem nao.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'carga', False);
  L := FechaELeWal;
  try
    Assert.AreEqual(0, Conta(L, 'content:'), 'nenhum conteudo');
    Assert.AreEqual(0, Conta(L, 'enqueue:'), 'nenhuma colocacao');
  finally
    L.Free;
  end;
end;

procedure TPersistWiringTests.Persistente_EmFilaTransiente_NaoGravaNada;
var
  L: TStringList;
begin
  // O contrario: a MENSAGEM e' persistente, a fila nao. Fila transiente nao
  // toca o journal, e o caminho quente dela fica identico ao da Fase 3.
  DeclaraFila('q.tra', False);
  FChan.PublishText('', 'q.tra', 'carga', True);
  L := FechaELeWal;
  try
    Assert.AreEqual(0, Conta(L, 'content:'), 'nenhum conteudo');
    Assert.AreEqual(0, Conta(L, 'enqueue:'), 'nenhuma colocacao');
  finally
    L.Free;
  end;
end;

procedure TPersistWiringTests.DuasFilasDuraveis_UmConteudoDuasColocacoes;
var
  L: TStringList;
  LBind: TAMQPQueueBind;
begin
  // O PAGAMENTO da D22: o corpo vai UMA vez, a colocacao vai por fila. E' o
  // que impede um fan-out de N filas de multiplicar o corpo por N no disco.
  DeclaraFila('q.a', True);
  DeclaraFila('q.b', True);
  FChan.DeclareExchange(TAMQPExchangeDeclare.Create('ex.fan', 'fanout', True));
  LBind.ExchangeName := 'ex.fan';
  LBind.RoutingKey := '';
  LBind.NoWait := False;
  LBind.Arguments := nil;
  LBind.QueueName := 'q.a';
  FChan.BindQueue(LBind);
  LBind.QueueName := 'q.b';
  FChan.BindQueue(LBind);

  FChan.PublishText('ex.fan', '', 'carga', True);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'content:'), 'UM conteudo para as duas filas');
    Assert.AreEqual(1, Conta(L, 'enqueue:q.a:'), 'colocacao na q.a');
    Assert.AreEqual(1, Conta(L, 'enqueue:q.b:'), 'colocacao na q.b');
  finally
    L.Free;
  end;
end;

procedure TPersistWiringTests.Ack_AposentaAColocacao;
var
  L: TStringList;
  LGet: TAMQPGetResult;
begin
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'carga', True);
  LGet := FChan.BasicGet('q.dur', False); // modo ack
  Assert.IsTrue(LGet.Found,
      'achou a mensagem');
  FChan.Ack(LGet.DeliveryTag);
  Sleep(300); // o ack viaja e o ator ainda tem de processa-lo
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'enqueue:q.dur:'), 'a colocacao existiu');
    Assert.AreEqual(1, Conta(L, 'dequeue:q.dur:'), 'e foi aposentada no ack');
  finally
    L.Free;
  end;
end;

procedure TPersistWiringTests.GetSemAck_AposentaNaHora;
var
  L: TStringList;
  LGet: TAMQPGetResult;
begin
  // Sem ack nao ha volta: a colocacao acaba no ato, no disco tambem.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'carga', True);
  LGet := FChan.BasicGet('q.dur', True); // no-ack
  Assert.IsTrue(LGet.Found,
      'achou a mensagem');
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'dequeue:q.dur:'), 'aposentada sem precisar de ack');
  finally
    L.Free;
  end;
end;

procedure TPersistWiringTests.DeadLetter_ColocacaoNovaAntesDeAposentarAVelha;
var
  L: TStringList;
  LArgs: TAMQPFieldTable;
  LIdxNova, LIdxVelha: Integer;
begin
  // A D23 VIRANDO TESTE. O dead-letter escreve a colocacao NOVA antes de
  // aposentar a velha, para que falhar no meio produza duplicata e nunca
  // perda. A ordem no arquivo e' a prova -- e e' verificavel exatamente
  // porque o log e' append-only.
  DeclaraFila('q.dlq', True);
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-message-ttl', TValue.From<Integer>(150));
    LArgs.Put('x-dead-letter-exchange', TValue.From<string>(''));
    LArgs.Put('x-dead-letter-routing-key', TValue.From<string>('q.dlq'));
    DeclaraFila('q.src', True, LArgs);
  finally
    LArgs.Free;
  end;

  FChan.PublishText('', 'q.src', 'carga', True);
  Sleep(1200); // TTL de 150 ms + a monitora passando (tick de 200 ms)

  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'enqueue:q.src:'), 'a colocacao original');
    Assert.AreEqual(1, Conta(L, 'enqueue:q.dlq:'), 'a colocacao na fila morta');
    Assert.AreEqual(1, Conta(L, 'content:'), 'o corpo NAO foi reescrito');
    Assert.AreEqual(1, Conta(L, 'dequeue:q.src:'), 'a original foi aposentada');

    LIdxNova := Indice(L, 'enqueue:q.dlq:');
    LIdxVelha := Indice(L, 'dequeue:q.src:');
    Assert.IsTrue((LIdxNova >= 0) and (LIdxVelha >= 0) and (LIdxNova < LIdxVelha),
      'a colocacao nova aparece ANTES da aposentadoria da velha '
      + '(D23: falhar no meio produz duplicata, nunca perda)');
  finally
    L.Free;
  end;
end;

// ---------------------------------------------------------------- WS5 -----
// De ponta a ponta, com cliente de verdade: o confirm passa a sair pela marca
// d'agua (D24), e nao mais inline. O que estes tres testes provam e' que a
// FIACAO esta viva -- publish -> engine -> journal -> marca -> rastreador ->
// wire -- e que ela nao PERDE nem PENDURA confirm nenhum.
//
// A ORDEM e o COLAPSO nao sao observaveis daqui: o WaitForConfirms espera por
// todos os seqs e ficaria verde com a ordem trocada ou com um frame por seq.
// Quem prende essas duas propriedades e' AMQP.ServerConfirmTests, que le os
// frames no wire com a marca entrando como parametro.

procedure TPersistWiringTests.Confirm_ComDataDir_OCanalPassaAAdiar;
var
  I: Integer;
begin
  // A mutacao que este teste existe para matar: NAO criar o rastreador e
  // seguir emitindo o confirm inline, como na Fase 3. Todo o resto da suite
  // continuaria verde -- o cliente receberia os acks do mesmo jeito, so' que
  // antes de o disco ter respondido. Aqui a fiacao e' observada direto.
  ChecaOk('o broker tem registro de confirms', FBroker.Confirms <> nil);
  ChecaOk('e nenhum canal adia nada antes do Confirm.Select',
    FBroker.Confirms.TrackerCount = 0);

  // O Confirm.Select e' RPC: quando ele volta, o Select-Ok ja' saiu, e o
  // rastreador foi criado ANTES dele. Nao ha o que esperar aqui.
  FChan.ConfirmSelect;
  ChecaOk('o Confirm.Select registrou o rastreador do canal',
    FBroker.Confirms.TrackerCount = 1);

  // E o rastreador SAI do registro quando o canal morre -- senao a thread do
  // journal percorreria canais mortos a cada fsync, para sempre. Aqui a
  // espera e' necessaria: o broker posta o Close-Ok ANTES de recolher o
  // canal, entao o Close do cliente volta antes do efeito.
  FChan.Close;
  for I := 1 to 200 do
  begin
    if FBroker.Confirms.TrackerCount = 0 then
      Break;
    Sleep(10);
  end;
  ChecaOk('o canal fechado saiu do registro',
    FBroker.Confirms.TrackerCount = 0);
  FreeAndNil(FChan); // Close desregistra: a posse volta para nos (ver CLAUDE.md)
end;

procedure TPersistWiringTests.Confirm_PublishPersistente_ChegaComOJournalJaDuravel;
var
  LMarca: UInt64;
begin
  DeclaraFila('q.dur', True);
  FChan.ConfirmSelect;
  FChan.PublishText('', 'q.dur', 'carga', True);
  ChecaOk('o confirm chegou', FChan.WaitForConfirms(10000));
  // Quando o ack chega, o que ele PROMETE ja' tem de estar cumprido: a marca
  // d'agua do journal cobre pelo menos os dois registros deste publish
  // (CONTENT + ENQ). Com o confirm saindo inline, este numero poderia ser 0.
  LMarca := FBroker.Journal.DurableLsn;
  ChecaOk('a marca d agua ja passou do lote deste publish', LMarca >= 2);
end;

procedure TPersistWiringTests.Confirm_LoteDePersistentes_TodosConfirmam;
var
  I: Integer;
begin
  // O caminho do group commit de ponta a ponta: 200 publishes persistentes
  // atravessam poucos fsyncs e voltam colapsados. Se a liberacao perdesse um
  // pendente -- ou o apagasse sem ter conseguido postar --, este teste NAO
  // FALHARIA: ele PENDURARIA ate' o timeout. E' o sintoma a reconhecer.
  DeclaraFila('q.dur', True);
  FChan.ConfirmSelect;
  for I := 1 to 200 do
    FChan.PublishText('', 'q.dur', 'm' + IntToStr(I), True);
  ChecaOk('os 200 confirmaram', FChan.WaitForConfirms(20000));
end;

procedure TPersistWiringTests.Confirm_PersistenteETransienteMisturados_TodosConfirmam;
var
  I: Integer;
begin
  // Metade nao toca o disco. Pelo corolario (a) da D24 os transientes ficam
  // ATRAS dos persistentes pendentes em vez de passar na frente -- e o que
  // este teste garante e' que ficar atras nao e' ficar preso.
  DeclaraFila('q.dur', True);
  FChan.ConfirmSelect;
  for I := 1 to 100 do
    FChan.PublishText('', 'q.dur', 'm' + IntToStr(I), (I mod 2) = 0);
  ChecaOk('os 100 confirmaram', FChan.WaitForConfirms(20000));
end;

initialization
  TDUnitX.RegisterTestFixture(TDataRecordTests);
  TDUnitX.RegisterTestFixture(TPersistWiringTests);

end.
