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
  fpcunit, testregistry, SysUtils,
  AMQP.Server.Records;

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

initialization
  RegisterTest(TDataRecordTests);

end.
