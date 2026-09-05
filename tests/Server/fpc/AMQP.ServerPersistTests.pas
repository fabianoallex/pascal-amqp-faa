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

  Espelho DUnitX de tests\Server\AMQP.ServerPersistTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes, Rtti,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Basic.Methods,
  AMQP.Queue.Methods,
  AMQP.Connection,
  AMQP.Server.Wal,
  AMQP.Server.Records,
  AMQP.Server.Broker;

type
  TDataRecordTests = class(TTestCase)
  published
    procedure Content_RoundTrip;
    procedure Content_CorpoBinarioSobreviveIntacto;
    procedure Content_CorpoBinario_NaoDependeDoCodepageDaApp;
    procedure Content_CorpoVazio;
    procedure Enqueue_RoundTrip;
    procedure Enqueue_TtlAusenteSobreviveComoMenosUm;
    procedure Enqueue_HeaderBinarioSobreviveIntacto;
    procedure Dequeue_RoundTrip;
    procedure KindsDeDados_SaoReconhecidos;
  end;


  { Sobe um broker com DataDir proprio, publica com o cliente de verdade e
    depois le' o WAL. E' o unico jeito honesto de testar a fiacao: os registros
    so' aparecem se o publish REAL atravessou a engine, a fila e o journal. }
  TPersistFixture = class(TTestCase)
  protected
    FBroker: TAMQPServer;
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FDir: string;
    procedure SetUp; override;
    procedure TearDown; override;
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
  end;

  
  TPersistWiringTests = class(TPersistFixture)
  published
    procedure Persistente_EmFilaDuravel_GravaConteudoEColocacao;
    procedure Transiente_EmFilaDuravel_NaoGravaNada;
    procedure Persistente_EmFilaTransiente_NaoGravaNada;
    procedure DuasFilasDuraveis_UmConteudoDuasColocacoes;
    procedure Ack_AposentaAColocacao;
    procedure GetSemAck_AposentaNaHora;
    procedure DeadLetter_ColocacaoNovaAntesDeAposentarAVelha;
    procedure Confirm_ComDataDir_OCanalPassaAAdiar;
    procedure Confirm_PublishPersistente_ChegaComOJournalJaDuravel;
    procedure Confirm_LoteDePersistentes_TodosConfirmam;
    procedure Confirm_PersistenteETransienteMisturados_TodosConfirmam;
    // --- WS6: o broker cai e sobe de novo no MESMO diretorio ---
    procedure Restart_MensagemPersistente_Sobrevive;
    procedure Restart_Transiente_NaoSobrevive;
    procedure Restart_TopologiaDuravel_Sobrevive;
    procedure Restart_MensagemVolta_ComRedelivered;
    procedure Restart_OrdemDePrioridade_Preservada;
    procedure Restart_Consumida_NaoRessuscita;
    procedure Restart_NaoReescreveOLog;
    procedure Restart_IdNovoNaoColideComRecuperado;
    procedure Restart_PrazoDescontaOTempoDecorrido;
  end;

implementation

// Ponte para a assercao do dialeto (mesma ideia de AMQP.ServerConfirmTests).
procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;
procedure ChecaStr(const AMsg, AEsperado, AObtido: string);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaNao(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertFalse(AMsg, ACond);
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
  AssertEquals('content-id de 64 bits', Int64($0102030405060708),
    Int64(R2.ContentId));
  AssertEquals('user-id', 'guest', R2.UserId);
  AssertTrue('corpo igual', BytesIguais(LB, R2.Body));
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
  AssertEquals('256 bytes de volta', 256, Length(R2.Body));
  AssertTrue('todos os 256 valores de byte sobrevivem',
    BytesIguais(LB, R2.Body));
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
  AssertEquals('256 bytes de volta', 256, Length(R2.Body));
  AssertTrue('o corpo nao depende do codepage da app',
    BytesIguais(LB, R2.Body));
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
  AssertEquals('corpo vazio continua vazio', 0, Length(R2.Body));
  AssertEquals('e o resto sobrevive', Int64(7), Int64(R2.ContentId));
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
  AssertEquals('vhost', '/', R2.VHost);
  AssertEquals('fila', 'q.pedidos', R2.Queue);
  AssertEquals('entry-id', Int64(42), Int64(R2.EntryId));
  AssertEquals('content-id', Int64(99), Int64(R2.ContentId));
  AssertEquals('exchange', 'ex.topico', R2.Exchange);
  AssertEquals('routing-key', 'a.b', R2.RoutingKey);
  AssertEquals('prioridade', 5, Integer(R2.Priority));
  AssertEquals('instante de parede', Int64(1788533217385), R2.EnqueuedAtWall);
  AssertEquals('ttl', Int64(60000), R2.TtlMs);
  AssertTrue('header igual', BytesIguais(LH, R2.HeaderPayload));
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
  AssertEquals('ttl -1 continua -1', Int64(-1), R2.TtlMs);
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
  AssertTrue('header binario intacto', BytesIguais(LH, R2.HeaderPayload));
end;

procedure TDataRecordTests.Dequeue_RoundTrip;
var
  R, R2: TAMQPRecDequeue;
begin
  R.VHost := '/vh';
  R.Queue := 'q.morta';
  R.EntryId := $FFFFFFFFFFFFFFF;
  R2 := AmqpDecodeRecDequeue(AmqpEncodeRecDequeue(R));
  AssertEquals('vhost', '/vh', R2.VHost);
  AssertEquals('fila', 'q.morta', R2.Queue);
  AssertEquals('entry-id grande', Int64($FFFFFFFFFFFFFFF),
    Int64(R2.EntryId));
end;

procedure TDataRecordTests.KindsDeDados_SaoReconhecidos;
begin
  AssertTrue('conteudo', AmqpIsDataRecord(AMQP_REC_CONTENT));
  AssertTrue('colocacao', AmqpIsDataRecord(AMQP_REC_ENQUEUE));
  AssertTrue('aposentadoria', AmqpIsDataRecord(AMQP_REC_DEQUEUE));
  // Os dois conjuntos nao podem se cruzar: e' o que separa os passes da
  // recuperacao (WS6).
  AssertFalse('topologia nao e dado', AmqpIsDataRecord(AMQP_REC_QUEUE_DECLARE));
  AssertFalse('dado nao e topologia', AmqpIsTopoRecord(AMQP_REC_ENQUEUE));
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
          SysUtils.DeleteFile(IncludeTrailingPathDelimiter(ADir) + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      SysUtils.FindClose(LRec);
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
  inherited SetUp;
  Inc(GSeq);
  FDir := IncludeTrailingPathDelimiter(GetTempDir) + 'amqppersist-'
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
  inherited TearDown;
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
    AssertEquals('um conteudo', 1, Conta(L, 'content:'));
    AssertEquals('uma colocacao na fila', 1, Conta(L, 'enqueue:q.dur:'));
    AssertEquals('e nada foi aposentado', 0, Conta(L, 'dequeue:'));
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
    AssertEquals('nenhum conteudo', 0, Conta(L, 'content:'));
    AssertEquals('nenhuma colocacao', 0, Conta(L, 'enqueue:'));
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
    AssertEquals('nenhum conteudo', 0, Conta(L, 'content:'));
    AssertEquals('nenhuma colocacao', 0, Conta(L, 'enqueue:'));
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
    AssertEquals('UM conteudo para as duas filas', 1, Conta(L, 'content:'));
    AssertEquals('colocacao na q.a', 1, Conta(L, 'enqueue:q.a:'));
    AssertEquals('colocacao na q.b', 1, Conta(L, 'enqueue:q.b:'));
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
  AssertTrue('achou a mensagem', LGet.Found);
  FChan.Ack(LGet.DeliveryTag);
  Sleep(300); // o ack viaja e o ator ainda tem de processa-lo
  L := FechaELeWal;
  try
    AssertEquals('a colocacao existiu', 1, Conta(L, 'enqueue:q.dur:'));
    AssertEquals('e foi aposentada no ack', 1, Conta(L, 'dequeue:q.dur:'));
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
  AssertTrue('achou a mensagem', LGet.Found);
  L := FechaELeWal;
  try
    AssertEquals('aposentada sem precisar de ack', 1, Conta(L, 'dequeue:q.dur:'));
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
    AssertEquals('a colocacao original', 1, Conta(L, 'enqueue:q.src:'));
    AssertEquals('a colocacao na fila morta', 1, Conta(L, 'enqueue:q.dlq:'));
    AssertEquals('o corpo NAO foi reescrito', 1, Conta(L, 'content:'));
    AssertEquals('a original foi aposentada', 1, Conta(L, 'dequeue:q.src:'));

    LIdxNova := Indice(L, 'enqueue:q.dlq:');
    LIdxVelha := Indice(L, 'dequeue:q.src:');
    AssertTrue('a colocacao nova aparece ANTES da aposentadoria da velha '
      + '(D23: falhar no meio produz duplicata, nunca perda)',
      (LIdxNova >= 0) and (LIdxVelha >= 0) and (LIdxNova < LIdxVelha));
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

initialization
  RegisterTest(TDataRecordTests);
  RegisterTest(TPersistWiringTests);

end.
