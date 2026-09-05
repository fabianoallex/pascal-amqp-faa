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
  AMQP.Basic.Methods,
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
    /// Teto do WAL que o AbreBroker aplica. 0 = ilimitado (o default),
    /// que e' o que todos os testes menos os do teto usam.
    FTetoWal: Int64;
    function DeclaraFila(const ANome: string; ADurable: Boolean;
      AArgs: TAMQPFieldTable = nil): string;
    /// Fecha o cliente, para o broker e devolve um resumo do WAL.
    function FechaELeWal: TStringList;
    function Conta(L: TStringList; const APrefixo: string): Integer;
    /// Indice da primeira linha que comeca com APrefixo, ou -1.
    function Indice(L: TStringList; const APrefixo: string): Integer;
    /// Sobe um broker novo apontando para FDir e reconecta o cliente.
    procedure AbreBroker;
    /// Derruba o broker e sobe outro no MESMO diretorio -- o que um restart e',
    /// do ponto de vista do disco.
    procedure Reabre;
    /// Soma o tamanho de todos os segmentos do WAL.
    function TamanhoDoWal: Int64;
    /// Publica persistente com a propriedade 'priority' preenchida.
    procedure PublicaComPrioridade(const AFila, ACorpo: string; APrio: Byte);
    /// True se a fila existe (passive declare num canal descartavel).
    function FilaExiste(const AFila: string): Boolean;
  public
    // PUBLIC, e nao protected: o DUnitX acha Setup/TearDown por RTTI, e o
    // RTTI padrao do Delphi so publica metodos public e published. Numa
    // secao protected os atributos existem no fonte, compilam, e simplesmente
    // NUNCA SAO ENCONTRADOS -- a fixture roda com todos os campos nil. Foi o
    // que aconteceu aqui: os [Test] (public) foram achados e executados, o
    // [Setup] (protected) nao, e as 11 asserções morreram em access violation
    // lendo nil+0x0C. Os helpers acima podem ficar protected: eles sao
    // chamados por codigo Pascal comum, que nao passa por RTTI.
    [Setup]    procedure SetUp;
    [TearDown] procedure TearDown;
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
    // --- WS6: o broker cai e sobe de novo no MESMO diretorio ---
    [Test] procedure Restart_MensagemPersistente_Sobrevive;
    [Test] procedure Restart_Transiente_NaoSobrevive;
    [Test] procedure Restart_TopologiaDuravel_Sobrevive;
    [Test] procedure Restart_MensagemVolta_ComRedelivered;
    [Test] procedure Restart_OrdemDePrioridade_Preservada;
    [Test] procedure Restart_Consumida_NaoRessuscita;
    [Test] procedure Restart_NaoReescreveOLog;
    [Test] procedure Restart_IdNovoNaoColideComRecuperado;
    [Test] procedure Restart_PrazoDescontaOTempoDecorrido;
    [Test] procedure Teto_PublishPersistenteAcimaDoTeto_LevaNack;
    [Test] procedure Teto_PublishTransiente_PassaMesmoComOLogCheio;
    [Test] procedure Teto_PublishRecusado_NaoEntraNaFila;
  end;

implementation

// Ponte para a assercao do dialeto (mesma ideia de AMQP.ServerConfirmTests).
procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  Assert.IsTrue(ACond, AMsg);
end;
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  Assert.AreEqual(AEsperado, AObtido, AMsg);
end;
procedure ChecaStr(const AMsg, AEsperado, AObtido: string);
begin
  Assert.AreEqual(AEsperado, AObtido, AMsg);
end;

procedure ChecaNao(const AMsg: string; ACond: Boolean);
begin
  Assert.IsFalse(ACond, AMsg);
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
begin
  
  Inc(GSeq);
  FDir := IncludeTrailingPathDelimiter(TPath.GetTempPath) + 'amqppersist-'
    + IntToStr(GSeq);
  ForceDirectories(FDir);
  LimpaDir(FDir);
  // Uma implementacao so de subir broker: o Reabre da WS6 chama a MESMA, e
  // duas copias seriam duas chances de o restart nao ser o que o SetUp faz.
  AbreBroker;
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

procedure TPersistFixture.AbreBroker;
var
  LParams: TAMQPConnectionParams;
begin
  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '127.0.0.1';
  FBroker.Port := 0;
  FBroker.DataDir := FDir;
  FBroker.MaxJournalBytes := FTetoWal;
  FBroker.Start;
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := FBroker.Port;
  FConn := TAMQPConnection.Create(LParams);
  FConn.Open;
  FChan := FConn.CreateChannel;
end;

procedure TPersistFixture.Reabre;
begin
  FChan := nil; // o destrutor da conexao recolhe os canais registrados
  if FConn <> nil then
  begin
    try
      FConn.Close;
    except
    end;
    FreeAndNil(FConn);
  end;
  FBroker.Stop;
  FreeAndNil(FBroker);
  AbreBroker;
end;

function TPersistFixture.TamanhoDoWal: Int64;
var
  LSegs: TArray<Cardinal>;
  I: Integer;
  LF: TFileStream;
begin
  Result := 0;
  LSegs := AmqpWalListSegments(FDir);
  for I := 0 to High(LSegs) do
  begin
    LF := TFileStream.Create(
      IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LSegs[I]),
      fmOpenRead or fmShareDenyNone);
    try
      Result := Result + LF.Size;
    finally
      LF.Free;
    end;
  end;
end;

procedure TPersistFixture.PublicaComPrioridade(const AFila, ACorpo: string;
  APrio: Byte);
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetDeliveryMode(2);
  LProps.SetPriority(APrio);
  FChan.Publish('', AFila, AmqpUtf8Encode(ACorpo), LProps);
end;

function TPersistFixture.FilaExiste(const AFila: string): Boolean;
var
  LCh: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
begin
  // Canal DESCARTAVEL: um passive que nao acha a fila fecha o canal com 404,
  // e um canal morto nao serve para mais nada.
  LCh := FConn.CreateChannel;
  try
    LDecl := TAMQPQueueDeclare.Create(AFila);
    LDecl.Passive := True;
    Result := True;
    try
      LCh.DeclareQueue(LDecl);
    except
      on E: EAMQPChannel do
        Result := False;
    end;
  finally
    LCh.Free;
  end;
end;

// ---------------------------------------------------------------- WS6 -----
// A prova que a Fase 4 inteira existe para dar: o broker cai, sobe de novo no
// MESMO DataDir, e a mensagem persistente esta la'.
//
// O QUE ESTES TESTES NAO SAO: teste de fsync. Matar (ou parar) o processo NAO
// testa fsync -- a cache do SO sobrevive a morte do processo, e so' a queda da
// maquina a perde. E' a admissao explicita da D28, e vai no README. O que eles
// provam e' a RECUPERACAO: que o log escrito e' suficiente para reconstruir o
// estado, e -- tao importante quanto -- que o que NAO devia estar la' nao volta.

procedure TPersistWiringTests.Restart_MensagemPersistente_Sobrevive;
var
  LGet: TAMQPGetResult;
begin
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'sobrevivi', True);
  Reabre;
  LGet := FChan.BasicGet('q.dur', True);
  ChecaOk('a mensagem voltou', LGet.Found);
  ChecaStr('com o corpo intacto', 'sobrevivi', LGet.BodyAsText);
end;

procedure TPersistWiringTests.Restart_Transiente_NaoSobrevive;
var
  LGet: TAMQPGetResult;
begin
  // A outra metade da regra dos tres (D20): o que nao foi ao disco nao volta.
  // Sem esta assercao, um broker que persistisse TUDO passaria no teste acima.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'efemera', False);
  Reabre;
  LGet := FChan.BasicGet('q.dur', True);
  ChecaNao('a transiente NAO voltou', LGet.Found);
end;

procedure TPersistWiringTests.Restart_TopologiaDuravel_Sobrevive;
var
  LGet: TAMQPGetResult;
  LBind: TAMQPQueueBind;
begin
  DeclaraFila('q.dur', True);
  FChan.DeclareExchange(TAMQPExchangeDeclare.Create('ex.dur', 'direct', True));
  LBind.QueueName := 'q.dur';
  LBind.ExchangeName := 'ex.dur';
  LBind.RoutingKey := 'rk';
  LBind.NoWait := False;
  LBind.Arguments := nil;
  FChan.BindQueue(LBind);
  Reabre;
  ChecaOk('a fila voltou', FilaExiste('q.dur'));
  // E o BINDING voltou junto -- a prova de verdade, porque uma recuperacao que
  // so' trouxesse fila e exchange passaria no FilaExiste acima.
  FChan.PublishText('ex.dur', 'rk', 'roteada', True);
  Sleep(300);
  LGet := FChan.BasicGet('q.dur', True);
  ChecaOk('o binding sobreviveu', LGet.Found);
end;

procedure TPersistWiringTests.Restart_MensagemVolta_ComRedelivered;
var
  LGet: TAMQPGetResult;
begin
  // D27: toda entrada recuperada volta redelivered=True. Persistir o flag
  // custaria uma escrita no caminho mais quente que existe, e seria precisao
  // FALSA -- depois de uma queda o broker nao sabe se o consumidor chegou a ver.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'x', True);
  Reabre;
  LGet := FChan.BasicGet('q.dur', True);
  ChecaOk('voltou', LGet.Found);
  ChecaOk('e marcada como reentregue', LGet.Redelivered);
end;

procedure TPersistWiringTests.Restart_OrdemDePrioridade_Preservada;
var
  LArgs: TAMQPFieldTable;
  LGet: TAMQPGetResult;
begin
  // O balde de prioridade esta' DENTRO do registro ENQ, e a ordem do log e' a
  // ordem de enfileiramento -- entao o replay poe cada mensagem exatamente
  // onde o RequeueFront da D12 a poria, sem ordenar nada. Publico a BAIXA
  // primeiro: se o replay ignorasse o balde e so' respeitasse a ordem do log,
  // ela sairia na frente.
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-max-priority', 5);
    DeclaraFila('q.pri', True, LArgs);
  finally
    LArgs.Free;
  end;
  PublicaComPrioridade('q.pri', 'baixa', 1);
  PublicaComPrioridade('q.pri', 'alta', 4);
  Sleep(300);
  Reabre;
  LGet := FChan.BasicGet('q.pri', True);
  ChecaOk('veio alguma', LGet.Found);
  ChecaStr('e a de MAIOR prioridade vem primeiro, como antes da queda',
    'alta', LGet.BodyAsText);
end;

procedure TPersistWiringTests.Restart_Consumida_NaoRessuscita;
var
  LGet: TAMQPGetResult;
begin
  // A colocacao viva e' a que tem ENQ e NAO tem DEQ. Sem o DEQ do ack, toda
  // mensagem ja' consumida voltaria a cada restart -- o pior modo de falha
  // possivel, porque cresce sem limite e ninguem percebe de imediato.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'consumida', True);
  LGet := FChan.BasicGet('q.dur', False); // modo ack
  ChecaOk('pegou antes da queda', LGet.Found);
  FChan.Ack(LGet.DeliveryTag);
  Sleep(300); // o ack viaja e o ator ainda tem de processa-lo
  Reabre;
  LGet := FChan.BasicGet('q.dur', True);
  ChecaNao('a consumida NAO ressuscitou', LGet.Found);
end;

procedure TPersistWiringTests.Restart_NaoReescreveOLog;
var
  LTamanho1, LTamanho2: Int64;
begin
  // O journal fica MUDO durante a recuperacao. Sem isso, cada boot reescreveria
  // a topologia e as colocacoes inteiras: o WAL cresceria uma copia por
  // restart, PARA SEMPRE, e a compactacao da WS7 nunca alcancaria. Nenhum outro
  // teste pega isso -- todos passariam com o log inchando.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'x', True);
  Sleep(300);
  Reabre;
  LTamanho1 := TamanhoDoWal;
  Reabre;
  LTamanho2 := TamanhoDoWal;
  ChecaOk('o segundo restart nao acrescentou nada ao log',
    LTamanho2 = LTamanho1);
end;

procedure TPersistWiringTests.Restart_IdNovoNaoColideComRecuperado;
var
  LGet: TAMQPGetResult;
  I, LVivas: Integer;
begin
  // O CONTADOR DE IDS TEM DE CONTINUAR DEPOIS DO MAIOR ID DO LOG. Se ele
  // recomecasse do zero, a colocacao do publish NOVO abaixo receberia um
  // EntryId que uma RECUPERADA ja' usa -- e o DEQ do ack dele aposentaria as
  // duas. A mensagem antiga sumiria sem ninguem a ter consumido.
  //
  // Nenhum outro teste desta suite pega isso: todos passam com o contador
  // zerado, porque so' o SEGUNDO restart revela o estrago.
  DeclaraFila('q.dur', True);
  FChan.PublishText('', 'q.dur', 'velha-1', True);
  FChan.PublishText('', 'q.dur', 'velha-2', True);
  Sleep(300);
  Reabre;

  // Publica DEPOIS da recuperacao, consome e confirma so' esta.
  FChan.PublishText('', 'q.dur', 'nova', True);
  Sleep(300);
  LGet := FChan.BasicGet('q.dur', False); // modo ack
  ChecaOk('pegou alguma', LGet.Found);
  FChan.Ack(LGet.DeliveryTag);
  Sleep(300);
  Reabre;

  // Sobrou UMA a menos que as tres publicadas -- e as duas restantes tem de
  // estar la'. Com o contador zerado, o ack acima teria levado duas.
  LVivas := 0;
  for I := 1 to 3 do
  begin
    LGet := FChan.BasicGet('q.dur', True);
    if not LGet.Found then
      Break;
    Inc(LVivas);
  end;
  ChecaInt('duas mensagens sobreviveram ao ack de UMA', 2, LVivas);
end;

procedure TPersistWiringTests.Restart_PrazoDescontaOTempoDecorrido;
var
  LArgs: TAMQPFieldTable;
  LGet: TAMQPGetResult;
begin
  // O CORACAO DA D21, e o unico teste que o alcanca: no disco vive PAREDE
  // (instante do enfileiramento + TTL efetivo); na memoria, monotonico. A
  // recuperacao tem de DESCONTAR o tempo que passou -- senao cada restart
  // renovaria o prazo, e uma mensagem com TTL de um minuto viveria para sempre
  // num broker que reinicia a cada cinquenta segundos.
  //
  // Os numeros sao escolhidos para separar as duas hipoteses: com o desconto,
  // sobram ~300 ms depois do restart e a mensagem morre dentro da espera; sem
  // ele, o prazo volta a 1200 ms e ela sobrevive a ela.
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-message-ttl', Int64(1200));
    DeclaraFila('q.ttl', True, LArgs);
  finally
    LArgs.Free;
  end;
  FChan.PublishText('', 'q.ttl', 'vence', True);
  Sleep(900); // ainda VIVA quando o broker cai -- o log a leva consigo
  Reabre;
  Sleep(600); // o que sobrava do prazo (~300 ms) acaba aqui
  LGet := FChan.BasicGet('q.ttl', True);
  ChecaNao('o prazo continuou correndo por cima do restart', LGet.Found);
end;

// ---------------------------------------------------------- WS7: teto duro ---

procedure TPersistWiringTests.Teto_PublishPersistenteAcimaDoTeto_LevaNack;
var
  I: Integer;
  LTodosOk: Boolean;
begin
  // A diferenca que a D26 marca em relacao a D7: teto de MEMORIA descarta da
  // cabeca, teto de DISCO RECUSA. Descartar aqui seria apagar dado que o
  // broker ja' confirmou como duravel -- a promessa que a Fase 4 existe para
  // cumprir. Recusar devolve a decisao a quem publica, que ainda tem a
  // mensagem na mao.
  FTetoWal := 40000; // pequeno de proposito: o teste nao mede desempenho
  Reabre;
  DeclaraFila('q.dur', True);
  FChan.ConfirmSelect;
  LTodosOk := True;
  for I := 1 to 300 do
  begin
    FChan.PublishText('', 'q.dur', StringOfChar('x', 500), True);
    if (I mod 50) = 0 then
      if not FChan.WaitForConfirms(10000) then
      begin
        LTodosOk := False;
        Break;
      end;
  end;
  if LTodosOk then
    LTodosOk := FChan.WaitForConfirms(10000);
  ChecaNao('algum publish foi NACK-ado depois de o log encher', LTodosOk);
  ChecaOk('e a recusa foi contada', FBroker.Journal.Stats.Recusados > 0);
end;

procedure TPersistWiringTests.Teto_PublishTransiente_PassaMesmoComOLogCheio;
var
  I: Integer;
  LGet: TAMQPGetResult;
begin
  // O teto e' de DISCO: quem nao escreve no disco nao paga por ele. Sem esta
  // assercao, uma implementacao que recusasse TODO publish com o log cheio
  // passaria no teste acima -- e derrubaria o broker inteiro por causa de uma
  // fila duravel cheia.
  FTetoWal := 40000;
  Reabre;
  DeclaraFila('q.dur', True);
  DeclaraFila('q.tra', False);
  for I := 1 to 300 do
    FChan.PublishText('', 'q.dur', StringOfChar('x', 500), True);
  Sleep(500);
  ChecaOk('o log encheu', FBroker.Journal.Cheio);

  FChan.PublishText('', 'q.tra', 'transiente', False);
  Sleep(300);
  LGet := FChan.BasicGet('q.tra', True);
  ChecaOk('o publish transiente passou', LGet.Found);
  ChecaStr('com o corpo certo', 'transiente', LGet.BodyAsText);
end;

procedure TPersistWiringTests.Teto_PublishRecusado_NaoEntraNaFila;
var
  I, LAntes, LDepois: Integer;
  LDecl: TAMQPQueueDeclare;
begin
  // NAO PODE HAVER PUBLISH PELA METADE. Recusar com Nack e ao mesmo tempo
  // enfileirar seria o pior dos dois mundos: o publicador acha que falhou e
  // reenvia, enquanto um consumidor recebe a copia que "falhou". A recusa
  // acontece ANTES de escrever ou postar coisa nenhuma, e e' isto que este
  // teste prende -- a mutacao "recusa mas posta" passava em todos os outros.
  FTetoWal := 40000;
  Reabre;
  DeclaraFila('q.dur', True);
  FChan.ConfirmSelect;
  for I := 1 to 300 do
    FChan.PublishText('', 'q.dur', StringOfChar('x', 500), True);
  FChan.WaitForConfirms(10000); // parte vai levar Nack: o log encheu
  Sleep(500);                   // o ator termina de enfileirar o que passou
  ChecaOk('o log encheu mesmo', FBroker.Journal.Cheio);

  LDecl := TAMQPQueueDeclare.Create('q.dur');
  LDecl.Passive := True;
  LAntes := Integer(FChan.DeclareQueue(LDecl).MessageCount);

  // Este ja' nasce recusado.
  FChan.PublishText('', 'q.dur', StringOfChar('y', 500), True);
  FChan.WaitForConfirms(5000);
  Sleep(500);
  LDepois := Integer(FChan.DeclareQueue(LDecl).MessageCount);

  ChecaInt('o publish recusado NAO entrou na fila', LAntes, LDepois);
end;

initialization
  TDUnitX.RegisterTestFixture(TDataRecordTests);
  TDUnitX.RegisterTestFixture(TPersistWiringTests);

end.
