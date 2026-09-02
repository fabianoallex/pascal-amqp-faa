unit AMQP.ServerEngineTests;

{ Testes de TAMQPMessage / AmqpSplitBody (AMQP.Server.Message) e dos
  descritores de recurso / AmqpArgsEqual (AMQP.Server.Resources) -- WS1 da
  Fase 2 (engine de roteamento em memoria, ver CLAUDE.md).

  Sao testes unitarios puros: nenhum dos dois sobe um broker (ao contrario das
  fixtures de AMQP.ServerHandshakeTests).

  Espelho DUnitX de tests\Server\AMQP.ServerEngineTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Rtti,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Server.Types,
  AMQP.Server.Message,
  AMQP.Server.Resources;

type
  TMessageTests = class(TTestCase)
  published
    procedure NasceComRefCount1;
    procedure AddRefSobeReleaseDesce;
    procedure UltimoReleaseLibera;
    procedure CorpoCompartilhadoSemCopia;
    procedure HeaderPayloadVoltaIgual;
    procedure FromServerMessage_CopiaOsCampos;
    procedure Split_CorpoVazio;
    procedure Split_CorpoMenorQueOMaximo;
    procedure Split_CorpoGrandeEmVariosPedacos;
    procedure Split_MaximoZero_UmPedacoSo;
  end;

  TResourceTests = class(TTestCase)
  published
    procedure ArgsEqual_Iguais;
    procedure ArgsEqual_ValorDiferente;
    procedure ArgsEqual_ChaveAMais;
    procedure ArgsEqual_NilXVazia;
    procedure ArgsEqual_TabelaAninhada;
    procedure ArgsEqual_ArrayDeValores;
    procedure ReservedName;
    procedure ValidExchangeType;
    procedure ExchangeDef_EquivalentTo;
    procedure QueueDef_EquivalentTo;
  end;

implementation

// Monta { 'x-lista': array de field-values ['S':ATexto, 'l':AInt] } pelo
// caminho do WIRE (encode + decode). E' o unico jeito de obter um array 'A'
// do jeito que o broker vai receber -- e e' o que expoe o embrulho de TValue
// do FPC no GetArrayElement, que o AmqpUnwrapValue desfaz dentro do
// AmqpArgsEqual (ver o gotcha no CLAUDE.md).
function TabelaComArray(const ATexto: string; AInt: Int64): TAMQPFieldTable;
var
  W: TAMQPWriter;
  R: TAMQPReader;
  LElems, LBody, LAll: TBytes;
begin
  W := TAMQPWriter.Create;
  try
    W.WriteOctet(Ord('S'));
    W.WriteLongStr(ATexto);
    W.WriteOctet(Ord('l'));
    W.WriteLongLongUInt(UInt64(AInt));
    LElems := W.ToBytes;
  finally
    W.Free;
  end;

  W := TAMQPWriter.Create;
  try
    W.WriteShortStr('x-lista');
    W.WriteOctet(Ord('A'));
    W.WriteLongUInt(Cardinal(Length(LElems)));
    W.WriteRaw(LElems);
    LBody := W.ToBytes;
  finally
    W.Free;
  end;

  W := TAMQPWriter.Create;
  try
    W.WriteLongUInt(Cardinal(Length(LBody)));
    W.WriteRaw(LBody);
    LAll := W.ToBytes;
  finally
    W.Free;
  end;

  R := TAMQPReader.Create(LAll);
  try
    Result := R.ReadFieldTable;
  finally
    R.Free;
  end;
end;

{ TMessageTests }

procedure TMessageTests.NasceComRefCount1;
var
  LMsg: TAMQPMessage;
begin
  LMsg := TAMQPMessage.Create('ex', 'rk', 'guest', nil, nil);
  try
    AssertEquals('refcount inicial', 1, LMsg.RefCount);
  finally
    LMsg.Release; // unico Release: chega a 0 e libera de fato
  end;
end;

procedure TMessageTests.AddRefSobeReleaseDesce;
var
  LMsg: TAMQPMessage;
begin
  LMsg := TAMQPMessage.Create('ex', 'rk', 'guest', nil, nil);
  AssertEquals('AddRef devolve o novo valor', 2, LMsg.AddRef);
  AssertEquals('refcount apos AddRef', 2, LMsg.RefCount);
  AssertEquals('Release devolve o novo valor', 1, LMsg.Release);
  AssertEquals('refcount apos o primeiro Release', 1, LMsg.RefCount);
  LMsg.Release; // segundo Release: chega a 0 e libera de fato
end;

procedure TMessageTests.UltimoReleaseLibera;
var
  LMsg: TAMQPMessage;
begin
  LMsg := TAMQPMessage.Create('ex', 'rk', 'guest', nil, nil);
  // O ultimo Release devolve 0 e libera o objeto -- nao ha' mais nada pra
  // asserir sobre LMsg depois disso (o teste em si prova que nao vazou nem
  // deu duplo-free: o runner FPCUnit nao acusa leak, mas o mirror DUnitX
  // acusa se este Release nao liberar de fato).
  AssertEquals('ultimo release chega a 0', 0, LMsg.Release);
end;

procedure TMessageTests.CorpoCompartilhadoSemCopia;
var
  LOriginal: TBytes;
  LMsg: TAMQPMessage;
begin
  SetLength(LOriginal, 5);
  LOriginal[0] := 1;
  LMsg := TAMQPMessage.Create('ex', 'rk', 'guest', nil, LOriginal);
  try
    AssertTrue('corpo compartilhado -- mesmo buffer, sem copia',
      Pointer(LMsg.Body) = Pointer(LOriginal));
  finally
    LMsg.Release;
  end;
end;

procedure TMessageTests.HeaderPayloadVoltaIgual;
var
  LHdr, LGot: TBytes;
  LMsg: TAMQPMessage;
  I: Integer;
begin
  SetLength(LHdr, 10);
  for I := 0 to 9 do
    LHdr[I] := Byte(I * 3);
  LMsg := TAMQPMessage.Create('ex', 'rk', 'guest', LHdr, nil);
  try
    LGot := LMsg.HeaderPayload;
    AssertEquals('tamanho do header payload', Length(LHdr), Length(LGot));
    for I := 0 to High(LHdr) do
      AssertEquals(Format('octeto %d do header payload', [I]),
        Integer(LHdr[I]), Integer(LGot[I]));
  finally
    LMsg.Release;
  end;
end;

procedure TMessageTests.FromServerMessage_CopiaOsCampos;
var
  LSrv: TAMQPServerMessage;
  LMsg: TAMQPMessage;
begin
  LSrv.Exchange := 'x.ws1';
  LSrv.RoutingKey := 'rk.ws1';
  LSrv.UserId := 'guest';
  LSrv.HeaderPayload := AmqpUtf8Encode('hdr');
  LSrv.Body := AmqpUtf8Encode('corpo');

  LMsg := TAMQPMessage.FromServerMessage(LSrv);
  try
    AssertEquals('refcount inicial', 1, LMsg.RefCount);
    AssertEquals('exchange', 'x.ws1', LMsg.Exchange);
    AssertEquals('routing key', 'rk.ws1', LMsg.RoutingKey);
    AssertEquals('user id', 'guest', LMsg.UserId);
    AssertEquals('header payload', 'hdr', AmqpUtf8Decode(LMsg.HeaderPayload));
    AssertEquals('body', 'corpo', AmqpUtf8Decode(LMsg.Body));
    AssertEquals('body size', 5, LMsg.BodySize);
  finally
    LMsg.Release;
  end;
end;

procedure TMessageTests.Split_CorpoVazio;
var
  LChunks: TAMQPBodyChunks;
begin
  LChunks := AmqpSplitBody(nil, 4096);
  AssertEquals('corpo vazio -> 0 pedacos', 0, Length(LChunks));
end;

procedure TMessageTests.Split_CorpoMenorQueOMaximo;
var
  LBody: TBytes;
  LChunks: TAMQPBodyChunks;
begin
  LBody := AmqpUtf8Encode('um corpo pequeno');
  LChunks := AmqpSplitBody(LBody, 4096);
  AssertEquals('um pedaco so', 1, Length(LChunks));
  AssertEquals('tamanho do pedaco unico', Length(LBody), Length(LChunks[0]));
end;

procedure TMessageTests.Split_CorpoGrandeEmVariosPedacos;
var
  LBody: TBytes;
  LChunks: TAMQPBodyChunks;
  LRebuilt: TBytes;
  LTotal, LOffset, I: Integer;
begin
  // 300 KB / 4096 = 75 pedacos exatos.
  SetLength(LBody, 300 * 1024);
  for I := 0 to High(LBody) do
    LBody[I] := Byte(I and $FF);

  LChunks := AmqpSplitBody(LBody, 4096);
  AssertEquals('numero de pedacos', 75, Length(LChunks));

  LTotal := 0;
  for I := 0 to High(LChunks) do
  begin
    AssertTrue(Format('pedaco %d nao excede o maximo', [I]),
      Length(LChunks[I]) <= 4096);
    Inc(LTotal, Length(LChunks[I]));
  end;
  AssertEquals('soma dos pedacos bate com o corpo', Length(LBody), LTotal);

  // A concatenacao dos pedacos reproduz o corpo original.
  SetLength(LRebuilt, LTotal);
  LOffset := 0;
  for I := 0 to High(LChunks) do
  begin
    if Length(LChunks[I]) > 0 then
      Move(LChunks[I][0], LRebuilt[LOffset], Length(LChunks[I]));
    Inc(LOffset, Length(LChunks[I]));
  end;
  for I := 0 to High(LBody) do
    if LRebuilt[I] <> LBody[I] then
      Fail(Format('corpo remontado diverge no octeto %d', [I]));
end;

procedure TMessageTests.Split_MaximoZero_UmPedacoSo;
var
  LBody: TBytes;
  LChunks: TAMQPBodyChunks;
begin
  SetLength(LBody, 10000);
  LChunks := AmqpSplitBody(LBody, 0);
  AssertEquals('maximo 0 -> um pedaco unico', 1, Length(LChunks));
  AssertEquals('pedaco unico tem o corpo inteiro',
    Length(LBody), Length(LChunks[0]));
end;

{ TResourceTests }

procedure TResourceTests.ArgsEqual_Iguais;
var
  A, B: TAMQPFieldTable;
begin
  A := TAMQPFieldTable.Create;
  B := TAMQPFieldTable.Create;
  try
    A.Put('x-match', 'all');
    A.Put('n', 42);
    B.Put('x-match', 'all');
    B.Put('n', 42);
    AssertTrue('tabelas com as mesmas chaves/valores', AmqpArgsEqual(A, B));
  finally
    A.Free;
    B.Free;
  end;
end;

procedure TResourceTests.ArgsEqual_ValorDiferente;
var
  A, B: TAMQPFieldTable;
begin
  A := TAMQPFieldTable.Create;
  B := TAMQPFieldTable.Create;
  try
    A.Put('n', 1);
    B.Put('n', 2);
    AssertFalse('mesma chave, valor diferente', AmqpArgsEqual(A, B));
  finally
    A.Free;
    B.Free;
  end;
end;

procedure TResourceTests.ArgsEqual_ChaveAMais;
var
  A, B: TAMQPFieldTable;
begin
  A := TAMQPFieldTable.Create;
  B := TAMQPFieldTable.Create;
  try
    A.Put('n', 1);
    B.Put('n', 1);
    B.Put('extra', 2);
    AssertFalse('B tem uma chave a mais', AmqpArgsEqual(A, B));
  finally
    A.Free;
    B.Free;
  end;
end;

procedure TResourceTests.ArgsEqual_NilXVazia;
var
  LVazia, LComItem: TAMQPFieldTable;
begin
  LVazia := TAMQPFieldTable.Create;
  try
    AssertTrue('nil == tabela vazia', AmqpArgsEqual(nil, LVazia));
    AssertTrue('tabela vazia == nil', AmqpArgsEqual(LVazia, nil));
    AssertTrue('nil == nil', AmqpArgsEqual(nil, nil));
  finally
    LVazia.Free;
  end;

  LComItem := TAMQPFieldTable.Create;
  try
    LComItem.Put('a', 1);
    AssertFalse('nil != tabela com item', AmqpArgsEqual(nil, LComItem));
    AssertFalse('tabela com item != nil', AmqpArgsEqual(LComItem, nil));
  finally
    LComItem.Free;
  end;
end;

procedure TResourceTests.ArgsEqual_TabelaAninhada;
var
  A, B, C: TAMQPFieldTable;
  LInnerA, LInnerB, LInnerC: TAMQPFieldTable;
begin
  LInnerA := TAMQPFieldTable.Create;
  LInnerA.Put('y', 1);
  LInnerB := TAMQPFieldTable.Create;
  LInnerB.Put('y', 1);
  LInnerC := TAMQPFieldTable.Create;
  LInnerC.Put('y', 2); // diverge

  A := TAMQPFieldTable.Create;
  B := TAMQPFieldTable.Create;
  C := TAMQPFieldTable.Create;
  try
    A.Put('nested', TValue.From<TAMQPFieldTable>(LInnerA));
    B.Put('nested', TValue.From<TAMQPFieldTable>(LInnerB));
    C.Put('nested', TValue.From<TAMQPFieldTable>(LInnerC));
    AssertTrue('tabelas aninhadas iguais', AmqpArgsEqual(A, B));
    AssertFalse('tabela aninhada diverge', AmqpArgsEqual(A, C));
  finally
    A.Free; // libera tambem a tabela aninhada (dono e' a tabela pai)
    B.Free;
    C.Free;
  end;
end;

procedure TResourceTests.ArgsEqual_ArrayDeValores;
var
  A, B, C, D, E: TAMQPFieldTable;
begin
  A := TabelaComArray('nfe', 7);
  B := TabelaComArray('nfe', 7);
  C := TabelaComArray('cte', 7);  // elemento string diverge
  D := TabelaComArray('nfe', 8);  // elemento inteiro diverge
  E := TAMQPFieldTable.Create;
  try
    E.Put('x-lista', 7); // escalar onde o outro tem array
    AssertTrue('arrays iguais elemento a elemento', AmqpArgsEqual(A, B));
    AssertFalse('elemento string diverge', AmqpArgsEqual(A, C));
    AssertFalse('elemento inteiro diverge', AmqpArgsEqual(A, D));
    AssertFalse('escalar nao e igual a array', AmqpArgsEqual(A, E));
  finally
    A.Free;
    B.Free;
    C.Free;
    D.Free;
    E.Free;
  end;
end;

procedure TResourceTests.ReservedName;
begin
  AssertTrue('prefixo amq.', AmqpIsReservedName('amq.direct'));
  AssertTrue('so o prefixo', AmqpIsReservedName('amq.'));
  AssertFalse('sem o prefixo', AmqpIsReservedName('minha.fila'));
  AssertFalse('prefixo parcial nao conta', AmqpIsReservedName('am.direct'));
  AssertFalse('vazio', AmqpIsReservedName(''));
end;

procedure TResourceTests.ValidExchangeType;
begin
  AssertTrue('direct', AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_DIRECT));
  AssertTrue('fanout', AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_FANOUT));
  AssertTrue('topic', AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_TOPIC));
  AssertTrue('headers', AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_HEADERS));
  AssertFalse('tipo desconhecido', AmqpIsValidExchangeType('custom'));
  AssertFalse('case-sensitive: maiusculas nao contam',
    AmqpIsValidExchangeType('Direct'));
end;

procedure TResourceTests.ExchangeDef_EquivalentTo;
var
  A, B, C, D: TAMQPExchangeDef;
  LArgsA, LArgsB, LArgsC, LArgsD: TAMQPFieldTable;
begin
  LArgsA := TAMQPFieldTable.Create;
  LArgsA.Put('x-foo', 1);
  LArgsB := TAMQPFieldTable.Create;
  LArgsB.Put('x-foo', 1);
  LArgsC := TAMQPFieldTable.Create;
  LArgsC.Put('x-foo', 2); // argumento diverge
  LArgsD := TAMQPFieldTable.Create; // flag diverge (Durable=False)

  A := TAMQPExchangeDef.Create('ex', AMQP_EXCHANGE_TYPE_TOPIC, True, False,
    False, LArgsA);
  B := TAMQPExchangeDef.Create('ex', AMQP_EXCHANGE_TYPE_TOPIC, True, False,
    False, LArgsB);
  C := TAMQPExchangeDef.Create('ex', AMQP_EXCHANGE_TYPE_TOPIC, True, False,
    False, LArgsC);
  D := TAMQPExchangeDef.Create('ex', AMQP_EXCHANGE_TYPE_TOPIC, False, False,
    False, LArgsD);
  try
    AssertTrue('mesmo tipo/flags/argumentos', A.EquivalentTo(B));
    AssertFalse('argumentos divergem', A.EquivalentTo(C));
    AssertFalse('durable diverge', A.EquivalentTo(D));
  finally
    A.Free;
    B.Free;
    C.Free;
    D.Free;
  end;
end;

procedure TResourceTests.QueueDef_EquivalentTo;
var
  A, B, C, D: TAMQPQueueDef;
  LArgsA, LArgsB, LArgsC, LArgsD: TAMQPFieldTable;
begin
  LArgsA := TAMQPFieldTable.Create;
  LArgsA.Put('x-max-priority', 10);
  LArgsB := TAMQPFieldTable.Create;
  LArgsB.Put('x-max-priority', 10);
  LArgsC := TAMQPFieldTable.Create;
  LArgsC.Put('x-max-priority', 5); // argumento diverge
  LArgsD := TAMQPFieldTable.Create; // flag diverge (Exclusive=True)

  A := TAMQPQueueDef.Create('q', True, False, False, LArgsA);
  B := TAMQPQueueDef.Create('q', True, False, False, LArgsB);
  C := TAMQPQueueDef.Create('q', True, False, False, LArgsC);
  D := TAMQPQueueDef.Create('q', True, True, False, LArgsD);
  try
    AssertTrue('mesmos flags/argumentos', A.EquivalentTo(B));
    AssertFalse('argumentos divergem', A.EquivalentTo(C));
    AssertFalse('exclusive diverge', A.EquivalentTo(D));

    // OwnerId nao entra na comparacao -- dono nao e' parte do redeclare.
    A.OwnerId := 123;
    B.OwnerId := 456;
    AssertTrue('OwnerId nao afeta EquivalentTo', A.EquivalentTo(B));
  finally
    A.Free;
    B.Free;
    C.Free;
    D.Free;
  end;
end;

initialization
  RegisterTest(TMessageTests);
  RegisterTest(TResourceTests);

end.
