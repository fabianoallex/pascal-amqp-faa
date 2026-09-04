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
  AMQP.Server.Records;

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

implementation

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

initialization
  TDUnitX.RegisterTestFixture(TDataRecordTests);

end.
