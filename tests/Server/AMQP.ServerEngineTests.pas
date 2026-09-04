unit AMQP.ServerEngineTests;

{ Testes de TAMQPMessage / AmqpSplitBody (AMQP.Server.Message) e dos
  descritores de recurso / AmqpArgsEqual (AMQP.Server.Resources) -- WS1 da
  Fase 2 (engine de roteamento em memoria, ver CLAUDE.md).

  Sao testes unitarios puros: nenhum dos dois sobe um broker (ao contrario das
  fixtures de AMQP.ServerHandshakeTests).

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerEngineTests.pas -- mantenha
  os dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Rtti,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Basic.Methods,
  AMQP.Server.Types,
  AMQP.Server.Message,
  AMQP.Server.Resources,
  AMQP.Server.Header;

type
  [TestFixture]
  TMessageTests = class
  public
    [Test] procedure NasceComRefCount1;
    [Test] procedure AddRefSobeReleaseDesce;
    [Test] procedure UltimoReleaseLibera;
    [Test] procedure CorpoCompartilhadoSemCopia;
    [Test] procedure HeaderPayloadVoltaIgual;
    [Test] procedure FromServerMessage_CopiaOsCampos;
    [Test] procedure Split_CorpoVazio;
    [Test] procedure Split_CorpoMenorQueOMaximo;
    [Test] procedure Split_CorpoGrandeEmVariosPedacos;
    [Test] procedure Split_MaximoZero_UmPedacoSo;
  end;

  [TestFixture]
  TResourceTests = class
  public
    [Test] procedure ArgsEqual_Iguais;
    [Test] procedure ArgsEqual_ValorDiferente;
    [Test] procedure ArgsEqual_ChaveAMais;
    [Test] procedure ArgsEqual_NilXVazia;
    [Test] procedure ArgsEqual_TabelaAninhada;
    [Test] procedure ArgsEqual_ArrayDeValores;
    [Test] procedure ReservedName;
    [Test] procedure ValidExchangeType;
    [Test] procedure ExchangeDef_EquivalentTo;
    [Test] procedure QueueDef_EquivalentTo;
  end;

  [TestFixture]
  TQueuePolicyTests = class
  public
    [Test] procedure Ausente_TudoNeutro;
    [Test] procedure Ttl_AceitaIntegerEInt64;
    [Test] procedure Ttl_NegativoRecusa;
    [Test] procedure Ttl_TipoErradoRecusa;
    [Test] procedure Expires_ZeroRecusa;
    [Test] procedure MaxLength_ZeroEValidoENaoAusente;
    [Test] procedure MaxPriority_FaixaFechada;
    [Test] procedure Overflow_DuasPoliticas;
    [Test] procedure DeadLetter_VazioEPresente;
    [Test] procedure ArgumentoDesconhecido_Ignorado;
    [Test] procedure AlternateExchange_LeEValida;
    [Test] procedure QueueDef_DerivaPolitica;
    [Test] procedure ExchangeDef_DerivaAlternate;
  end;

  [TestFixture]
  THeaderRewriteTests = class
  public
    [Test] procedure RoundTripSemMexer_BytesIdenticos;
    [Test] procedure TodasAsCatorzePropriedades_Preservadas;
    [Test] procedure AcrescentaHeader_RestoIntacto;
    [Test] procedure SemHeaders_GanhaTabelaSobDemanda;
    [Test] procedure ArrayDeTabelas_AtravessaAReescrita;
    [Test] procedure Derive_CompartilhaOCorpoSemCopiar;
    [Test] procedure Derive_NaoTocaNaOrigem;
    [Test] procedure Derive_BodySizeBateComOCorpo;
    [Test] procedure PayloadMalformado_Levanta;
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
    Assert.AreEqual(1, LMsg.RefCount, 'refcount inicial');
  finally
    LMsg.Release; // unico Release: chega a 0 e libera de fato
  end;
end;

procedure TMessageTests.AddRefSobeReleaseDesce;
var
  LMsg: TAMQPMessage;
begin
  LMsg := TAMQPMessage.Create('ex', 'rk', 'guest', nil, nil);
  Assert.AreEqual(2, LMsg.AddRef, 'AddRef devolve o novo valor');
  Assert.AreEqual(2, LMsg.RefCount, 'refcount apos AddRef');
  Assert.AreEqual(1, LMsg.Release, 'Release devolve o novo valor');
  Assert.AreEqual(1, LMsg.RefCount, 'refcount apos o primeiro Release');
  LMsg.Release; // segundo Release: chega a 0 e libera de fato
end;

procedure TMessageTests.UltimoReleaseLibera;
var
  LMsg: TAMQPMessage;
begin
  LMsg := TAMQPMessage.Create('ex', 'rk', 'guest', nil, nil);
  // O ultimo Release devolve 0 e libera o objeto de fato -- se nao liberasse,
  // o ReportMemoryLeaksOnShutdown do runner acusaria o vazamento.
  Assert.AreEqual(0, LMsg.Release, 'ultimo release chega a 0');
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
    Assert.IsTrue(Pointer(LMsg.Body) = Pointer(LOriginal),
      'corpo compartilhado -- mesmo buffer, sem copia');
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
    Assert.AreEqual(Length(LHdr), Length(LGot), 'tamanho do header payload');
    for I := 0 to High(LHdr) do
      Assert.AreEqual(Integer(LHdr[I]), Integer(LGot[I]),
        Format('octeto %d do header payload', [I]));
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
    Assert.AreEqual(1, LMsg.RefCount, 'refcount inicial');
    Assert.AreEqual('x.ws1', LMsg.Exchange, 'exchange');
    Assert.AreEqual('rk.ws1', LMsg.RoutingKey, 'routing key');
    Assert.AreEqual('guest', LMsg.UserId, 'user id');
    Assert.AreEqual('hdr', AmqpUtf8Decode(LMsg.HeaderPayload), 'header payload');
    Assert.AreEqual('corpo', AmqpUtf8Decode(LMsg.Body), 'body');
    Assert.AreEqual(5, LMsg.BodySize, 'body size');
  finally
    LMsg.Release;
  end;
end;

procedure TMessageTests.Split_CorpoVazio;
var
  LChunks: TAMQPBodyChunks;
begin
  LChunks := AmqpSplitBody(nil, 4096);
  Assert.AreEqual(0, Length(LChunks), 'corpo vazio -> 0 pedacos');
end;

procedure TMessageTests.Split_CorpoMenorQueOMaximo;
var
  LBody: TBytes;
  LChunks: TAMQPBodyChunks;
begin
  LBody := AmqpUtf8Encode('um corpo pequeno');
  LChunks := AmqpSplitBody(LBody, 4096);
  Assert.AreEqual(1, Length(LChunks), 'um pedaco so');
  Assert.AreEqual(Length(LBody), Length(LChunks[0]), 'tamanho do pedaco unico');
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
  Assert.AreEqual(75, Length(LChunks), 'numero de pedacos');

  LTotal := 0;
  for I := 0 to High(LChunks) do
  begin
    Assert.IsTrue(Length(LChunks[I]) <= 4096,
      Format('pedaco %d nao excede o maximo', [I]));
    Inc(LTotal, Length(LChunks[I]));
  end;
  Assert.AreEqual(Length(LBody), LTotal, 'soma dos pedacos bate com o corpo');

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
      Assert.Fail(Format('corpo remontado diverge no octeto %d', [I]));
end;

procedure TMessageTests.Split_MaximoZero_UmPedacoSo;
var
  LBody: TBytes;
  LChunks: TAMQPBodyChunks;
begin
  SetLength(LBody, 10000);
  LChunks := AmqpSplitBody(LBody, 0);
  Assert.AreEqual(1, Length(LChunks), 'maximo 0 -> um pedaco unico');
  Assert.AreEqual(Length(LBody), Length(LChunks[0]),
    'pedaco unico tem o corpo inteiro');
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
    Assert.IsTrue(AmqpArgsEqual(A, B), 'tabelas com as mesmas chaves/valores');
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
    Assert.IsFalse(AmqpArgsEqual(A, B), 'mesma chave, valor diferente');
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
    Assert.IsFalse(AmqpArgsEqual(A, B), 'B tem uma chave a mais');
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
    Assert.IsTrue(AmqpArgsEqual(nil, LVazia), 'nil == tabela vazia');
    Assert.IsTrue(AmqpArgsEqual(LVazia, nil), 'tabela vazia == nil');
    Assert.IsTrue(AmqpArgsEqual(nil, nil), 'nil == nil');
  finally
    LVazia.Free;
  end;

  LComItem := TAMQPFieldTable.Create;
  try
    LComItem.Put('a', 1);
    Assert.IsFalse(AmqpArgsEqual(nil, LComItem), 'nil != tabela com item');
    Assert.IsFalse(AmqpArgsEqual(LComItem, nil), 'tabela com item != nil');
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
    Assert.IsTrue(AmqpArgsEqual(A, B), 'tabelas aninhadas iguais');
    Assert.IsFalse(AmqpArgsEqual(A, C), 'tabela aninhada diverge');
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
    Assert.IsTrue(AmqpArgsEqual(A, B), 'arrays iguais elemento a elemento');
    Assert.IsFalse(AmqpArgsEqual(A, C), 'elemento string diverge');
    Assert.IsFalse(AmqpArgsEqual(A, D), 'elemento inteiro diverge');
    Assert.IsFalse(AmqpArgsEqual(A, E), 'escalar nao e igual a array');
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
  Assert.IsTrue(AmqpIsReservedName('amq.direct'), 'prefixo amq.');
  Assert.IsTrue(AmqpIsReservedName('amq.'), 'so o prefixo');
  Assert.IsFalse(AmqpIsReservedName('minha.fila'), 'sem o prefixo');
  Assert.IsFalse(AmqpIsReservedName('am.direct'), 'prefixo parcial nao conta');
  Assert.IsFalse(AmqpIsReservedName(''), 'vazio');
end;

procedure TResourceTests.ValidExchangeType;
begin
  Assert.IsTrue(AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_DIRECT), 'direct');
  Assert.IsTrue(AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_FANOUT), 'fanout');
  Assert.IsTrue(AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_TOPIC), 'topic');
  Assert.IsTrue(AmqpIsValidExchangeType(AMQP_EXCHANGE_TYPE_HEADERS), 'headers');
  Assert.IsFalse(AmqpIsValidExchangeType('custom'), 'tipo desconhecido');
  Assert.IsFalse(AmqpIsValidExchangeType('Direct'),
    'case-sensitive: maiusculas nao contam');
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
    Assert.IsTrue(A.EquivalentTo(B), 'mesmo tipo/flags/argumentos');
    Assert.IsFalse(A.EquivalentTo(C), 'argumentos divergem');
    Assert.IsFalse(A.EquivalentTo(D), 'durable diverge');
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
    Assert.IsTrue(A.EquivalentTo(B), 'mesmos flags/argumentos');
    Assert.IsFalse(A.EquivalentTo(C), 'argumentos divergem');
    Assert.IsFalse(A.EquivalentTo(D), 'exclusive diverge');

    // OwnerId nao entra na comparacao -- dono nao e' parte do redeclare.
    A.OwnerId := 123;
    B.OwnerId := 456;
    Assert.IsTrue(A.EquivalentTo(B), 'OwnerId nao afeta EquivalentTo');
  finally
    A.Free;
    B.Free;
    C.Free;
    D.Free;
  end;
end;


{ Wrappers de assercao usados so' pela TQueuePolicyTests. Existem para o CORPO
  da fixture ficar LITERALMENTE identico nas duas suites -- a divergencia de
  espelho ja' mordeu antes (o helper ExpectChannelClose que so' existia do lado
  FPCUnit). Aqui a unica coisa que difere entre os dois arquivos sao estas
  quatro linhas. }
procedure ChecaOk(AOk: Boolean; const AErro: string);
begin
  Assert.IsTrue(AOk, 'parse deveria passar, erro: ' + AErro);
end;

procedure ChecaInt(const AMsg: string; AEsperado, AReal: Int64);
begin
  Assert.AreEqual(AEsperado, AReal, AMsg);
end;

procedure ChecaBool(const AMsg: string; AEsperado, AReal: Boolean);
begin
  Assert.IsTrue(AReal = AEsperado, AMsg);
end;

procedure ChecaStr(const AMsg, AEsperado, AReal: string);
begin
  Assert.AreEqual(AEsperado, AReal, AMsg);
end;

{ TQueuePolicyTests }

// Tabela com uma unica chave numerica, montada com o valor ja' num TValue
// local (encadear TValue.From<T> inline num Put trava o FPC 3.2).
function TabInt(const AChave: string; AValor: Int64): TAMQPFieldTable;
var
  LVal: TValue;
begin
  Result := TAMQPFieldTable.Create;
  LVal := TValue.From<Int64>(AValor);
  Result.Put(AChave, LVal);
end;

function TabStr(const AChave, AValor: string): TAMQPFieldTable;
var
  LVal: TValue;
begin
  Result := TAMQPFieldTable.Create;
  LVal := TValue.From<string>(AValor);
  Result.Put(AChave, LVal);
end;

procedure TQueuePolicyTests.Ausente_TudoNeutro;
var
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  // nil e' o caso comum: a maioria dos Queue.Declare nao manda argumento.
  ChecaOk(AmqpParseQueuePolicy(nil, LPol, LErro), LErro);
  ChecaInt('ttl ausente', -1, LPol.MessageTtlMs);
  ChecaInt('expires ausente', -1, LPol.ExpiresMs);
  ChecaInt('max-length ausente', -1, LPol.MaxLength);
  ChecaInt('max-priority ausente', -1, LPol.MaxPriority);
  ChecaBool('sem dead-letter', False, LPol.HasDeadLetterExchange);
  ChecaBool('HasAny False', False, LPol.HasAny);
end;

procedure TQueuePolicyTests.Ttl_AceitaIntegerEInt64;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
  LVal: TValue;
begin
  // O wire nao tem um tipo unico para numero: 'b'/'s'/'I' viram tkInteger e
  // 'i'/'l'/'T' viram tkInt64. Um cliente que manda ttl como Integer e outro
  // como Int64 estao os dois certos.
  LTab := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<Integer>(60000);
    LTab.Put('x-message-ttl', LVal);
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaInt('ttl como Integer', 60000, LPol.MessageTtlMs);
  finally
    LTab.Free;
  end;

  LTab := TabInt('x-message-ttl', 60000);
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaInt('ttl como Int64', 60000, LPol.MessageTtlMs);
    ChecaBool('HasAny True', True, LPol.HasAny);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.Ttl_NegativoRecusa;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  LTab := TabInt('x-message-ttl', -1);
  try
    ChecaBool('deve recusar', False, AmqpParseQueuePolicy(LTab, LPol, LErro));
    // A mensagem tem de NOMEAR o argumento -- e' o que separa um erro
    // acionavel de um "precondition failed" que o usuario adivinha.
    ChecaBool('erro cita o argumento', True,
      Pos('x-message-ttl', LErro) > 0);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.Ttl_TipoErradoRecusa;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  LTab := TabStr('x-message-ttl', '60000'); // string, nao numero
  try
    ChecaBool('deve recusar', False, AmqpParseQueuePolicy(LTab, LPol, LErro));
    ChecaBool('erro diz integer', True, Pos('integer', LErro) > 0);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.Expires_ZeroRecusa;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  // Uma fila que expira em 0 ms sumiria no instante do proprio declare.
  LTab := TabInt('x-expires', 0);
  try
    ChecaBool('deve recusar', False, AmqpParseQueuePolicy(LTab, LPol, LErro));
    ChecaBool('erro cita o argumento', True, Pos('x-expires', LErro) > 0);
  finally
    LTab.Free;
  end;
  LTab := TabInt('x-expires', 1);
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaInt('1 ms e valido', 1, LPol.ExpiresMs);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.MaxLength_ZeroEValidoENaoAusente;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  // O motivo de ausencia ser -1 e nao 0: x-max-length=0 e' um valor legitimo
  // ("fila que nao guarda nada"), diferente de nao ter sido informado.
  LTab := TabInt('x-max-length', 0);
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaInt('zero preservado', 0, LPol.MaxLength);
    ChecaBool('HasAny True com zero', True, LPol.HasAny);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.MaxPriority_FaixaFechada;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  LTab := TabInt('x-max-priority', AMQP_MAX_PRIORITY_LEVEL);
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaInt('teto aceito', AMQP_MAX_PRIORITY_LEVEL, LPol.MaxPriority);
  finally
    LTab.Free;
  end;

  // Desvio deliberado do RabbitMQ (D12): la' vai ate' 255, aqui para em 9 --
  // e recusa em vez de fazer clamp silencioso.
  LTab := TabInt('x-max-priority', AMQP_MAX_PRIORITY_LEVEL + 1);
  try
    ChecaBool('acima do teto recusa', False,
      AmqpParseQueuePolicy(LTab, LPol, LErro));
    ChecaBool('erro cita o argumento', True,
      Pos('x-max-priority', LErro) > 0);
  finally
    LTab.Free;
  end;

  LTab := TabInt('x-max-priority', -1);
  try
    ChecaBool('negativo recusa', False,
      AmqpParseQueuePolicy(LTab, LPol, LErro));
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.Overflow_DuasPoliticas;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  LTab := TabStr('x-overflow', 'drop-head');
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaBool('drop-head', True, LPol.Overflow = amqovDropHead);
  finally
    LTab.Free;
  end;

  LTab := TabStr('x-overflow', 'reject-publish');
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaBool('reject-publish', True, LPol.Overflow = amqovRejectPublish);
    ChecaBool('HasAny True', True, LPol.HasAny);
  finally
    LTab.Free;
  end;

  // 'reject-publish-dlx' do RabbitMQ nao entra nesta implementacao.
  LTab := TabStr('x-overflow', 'reject-publish-dlx');
  try
    ChecaBool('politica desconhecida recusa', False,
      AmqpParseQueuePolicy(LTab, LPol, LErro));
    ChecaBool('erro cita o argumento', True, Pos('x-overflow', LErro) > 0);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.DeadLetter_VazioEPresente;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  // x-dead-letter-exchange='' e' o exchange DEFAULT, nao "sem DLX" -- por isso
  // o par Has*, e nao string vazia como sentinela.
  LTab := TabStr('x-dead-letter-exchange', '');
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaBool('presente', True, LPol.HasDeadLetterExchange);
    ChecaStr('valor vazio', '', LPol.DeadLetterExchange);
    ChecaBool('HasAny True', True, LPol.HasAny);
  finally
    LTab.Free;
  end;

  LTab := TabStr('x-dead-letter-routing-key', 'falhou');
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaBool('dlrk presente', True, LPol.HasDeadLetterRoutingKey);
    ChecaStr('dlrk valor', 'falhou', LPol.DeadLetterRoutingKey);
    ChecaBool('dlx continua ausente', False, LPol.HasDeadLetterExchange);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.ArgumentoDesconhecido_Ignorado;
var
  LTab: TAMQPFieldTable;
  LPol: TAMQPQueuePolicy;
  LErro: string;
begin
  // Recusar 'x-' desconhecido quebraria cliente que manda argumento de uma
  // extensao que este broker nao tem. Mesmo comportamento do RabbitMQ.
  LTab := TabInt('x-invencao-do-cliente', 7);
  try
    ChecaOk(AmqpParseQueuePolicy(LTab, LPol, LErro), LErro);
    ChecaBool('nao vira politica', False, LPol.HasAny);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.AlternateExchange_LeEValida;
var
  LTab: TAMQPFieldTable;
  LNome, LErro: string;
  LTem: Boolean;
begin
  ChecaOk(AmqpParseAlternateExchange(nil, LNome, LTem, LErro), LErro);
  ChecaBool('ausente', False, LTem);

  LTab := TabStr('alternate-exchange', 'ae');
  try
    ChecaOk(AmqpParseAlternateExchange(LTab, LNome, LTem, LErro), LErro);
    ChecaBool('presente', True, LTem);
    ChecaStr('nome', 'ae', LNome);
  finally
    LTab.Free;
  end;

  LTab := TabInt('alternate-exchange', 42); // numero onde se espera string
  try
    ChecaBool('tipo errado recusa', False,
      AmqpParseAlternateExchange(LTab, LNome, LTem, LErro));
    ChecaBool('erro cita o argumento', True,
      Pos('alternate-exchange', LErro) > 0);
  finally
    LTab.Free;
  end;
end;

procedure TQueuePolicyTests.QueueDef_DerivaPolitica;
var
  LTab: TAMQPFieldTable;
  LDef: TAMQPQueueDef;
  LVal: TValue;
begin
  // O descritor deriva a politica na construcao; a tabela continua sendo a
  // fonte da verdade (e' ela que o EquivalentTo compara).
  LTab := TAMQPFieldTable.Create;
  LVal := TValue.From<Int64>(30000);
  LTab.Put('x-message-ttl', LVal);
  LVal := TValue.From<string>('dlx');
  LTab.Put('x-dead-letter-exchange', LVal);

  LDef := TAMQPQueueDef.Create('q', True, False, False, LTab);
  try
    ChecaInt('ttl derivado', 30000, LDef.Policy.MessageTtlMs);
    ChecaBool('dlx derivado', True, LDef.Policy.HasDeadLetterExchange);
    ChecaStr('dlx nome', 'dlx', LDef.Policy.DeadLetterExchange);
  finally
    LDef.Free; // dono da tabela
  end;
end;

procedure TQueuePolicyTests.ExchangeDef_DerivaAlternate;
var
  LTab: TAMQPFieldTable;
  LDef: TAMQPExchangeDef;
  LVal: TValue;
begin
  LTab := TAMQPFieldTable.Create;
  LVal := TValue.From<string>('ae');
  LTab.Put('alternate-exchange', LVal);

  LDef := TAMQPExchangeDef.Create('x', 'direct', True, False, False, LTab);
  try
    ChecaBool('tem AE', True, LDef.HasAlternateExchange);
    ChecaStr('nome do AE', 'ae', LDef.AlternateExchange);
  finally
    LDef.Free;
  end;
end;

{ THeaderRewriteTests }

// Hex de um TBytes: comparar buffers inteiros numa assercao so', com os dois
// lados visiveis na mensagem de falha.
function HexDe(const ABytes: TBytes): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(ABytes) do
    Result := Result + IntToHex(ABytes[I], 2);
end;

// Preenche as CATORZE propriedades da classe Basic. O teste de fidelidade tem
// de cobrir todas, nao uma amostra: DecodeContentHeader descarta as palavras
// de continuacao do property-flags, e so' exercitando o conjunto inteiro da
// para afirmar que a reescrita e' fiel ao que a classe define.
function PropsCompletas: TAMQPBasicProperties;
var
  LTab: TAMQPFieldTable;
  LVal: TValue;
begin
  Result := TAMQPBasicProperties.Empty;
  Result.SetContentType('application/json');       //  1
  Result.SetContentEncoding('gzip');               //  2
  LTab := TAMQPFieldTable.Create;                  //  3
  LVal := TValue.From<string>('sul');
  LTab.Put('regiao', LVal);
  LVal := TValue.From<Int64>(7);
  LTab.Put('tentativa', LVal);
  Result.SetHeaders(LTab);
  Result.SetDeliveryMode(2);                       //  4
  Result.SetPriority(5);                           //  5
  Result.SetCorrelationId('corr-123');             //  6
  Result.SetReplyTo('fila.resposta');              //  7
  Result.SetExpiration('60000');                   //  8
  Result.SetMessageId('msg-1');                    //  9
  Result.SetTimestamp(1756900000);                 // 10
  Result.SetMsgType('nota.emitida');               // 11
  Result.SetUserId('guest');                       // 12
  Result.SetAppId('retaguarda');                   // 13
  Result.SetClusterId('cluster-a');                // 14
end;

// A propriedade central desta frente: reescrever SEM MEXER devolve os MESMOS
// bytes. E' o que garante que o dead-lettering so' muda o que mudou de fato,
// e o que sustenta a decisao D10 (a D1 continua valendo porque a reescrita
// deriva, nao clona no caminho quente).
procedure THeaderRewriteTests.RoundTripSemMexer_BytesIdenticos;
var
  LProps: TAMQPBasicProperties;
  LEd: TAMQPHeaderEditor;
  B1, B2: TBytes;
begin
  LProps := PropsCompletas;
  try
    B1 := BuildContentHeader(4321, LProps);
  finally
    LProps.Headers.Free;
  end;

  LEd := TAMQPHeaderEditor.Create(B1);
  try
    ChecaInt('body-size lido do payload', 4321, Int64(LEd.BodySizeOriginal));
    B2 := LEd.BuildPayload(LEd.BodySizeOriginal);
  finally
    LEd.Free;
  end;

  ChecaStr('bytes identicos apos ida e volta', HexDe(B1), HexDe(B2));
end;

// Complementa o teste de bytes: se algum dia ele falhar, este diz QUAL
// propriedade se perdeu, em vez de mostrar dois hexdumps.
procedure THeaderRewriteTests.TodasAsCatorzePropriedades_Preservadas;
var
  LProps: TAMQPBasicProperties;
  LEd: TAMQPHeaderEditor;
  LP: TAMQPBasicProperties;
  B: TBytes;
  LVal: TValue;
begin
  LProps := PropsCompletas;
  try
    B := BuildContentHeader(10, LProps);
  finally
    LProps.Headers.Free;
  end;

  // ATENCAO: a assercao tem de rodar sobre o payload RE-ENCODADO, nao sobre o
  // decode do original. Descoberto por mutacao: com a versao anterior deste
  // teste (que lia LEd.Properties direto), remover uma propriedade do
  // BuildPayload passava incolume aqui e so' o teste de bytes acusava -- ou
  // seja, o teste que existe para dizer QUAL propriedade se perdeu nao
  // exercitava o caminho onde a perda acontece.
  LEd := TAMQPHeaderEditor.Create(B);
  try
    B := LEd.BuildPayload(10);
  finally
    LEd.Free;
  end;

  LEd := TAMQPHeaderEditor.Create(B);
  try
    LP := LEd.Properties;
    ChecaStr('content-type', 'application/json', LP.ContentType);
    ChecaStr('content-encoding', 'gzip', LP.ContentEncoding);
    ChecaBool('headers presente', True, LP.Has(bpHeaders));
    ChecaBool('headers tem regiao', True,
      LEd.Headers.TryGetValue('regiao', LVal));
    ChecaStr('headers.regiao', 'sul', LVal.AsString);
    ChecaInt('delivery-mode', 2, LP.DeliveryMode);
    ChecaInt('priority', 5, LP.Priority);
    ChecaStr('correlation-id', 'corr-123', LP.CorrelationId);
    ChecaStr('reply-to', 'fila.resposta', LP.ReplyTo);
    ChecaStr('expiration', '60000', LP.Expiration);
    ChecaStr('message-id', 'msg-1', LP.MessageId);
    ChecaInt('timestamp', 1756900000, Int64(LP.Timestamp));
    ChecaStr('type', 'nota.emitida', LP.MsgType);
    ChecaStr('user-id', 'guest', LP.UserId);
    ChecaStr('app-id', 'retaguarda', LP.AppId);
    ChecaStr('cluster-id', 'cluster-a', LP.ClusterId);
    ChecaInt('body-size', 10, Int64(LEd.BodySizeOriginal));
  finally
    LEd.Free;
  end;
end;

// Acrescentar um header nao pode danificar o resto -- e' exatamente o que o
// dead-lettering faz com o x-death.
procedure THeaderRewriteTests.AcrescentaHeader_RestoIntacto;
var
  LProps: TAMQPBasicProperties;
  LEd: TAMQPHeaderEditor;
  B, B2: TBytes;
  LVal: TValue;
begin
  LProps := PropsCompletas;
  try
    B := BuildContentHeader(10, LProps);
  finally
    LProps.Headers.Free;
  end;

  LEd := TAMQPHeaderEditor.Create(B);
  try
    LVal := TValue.From<string>('expired');
    LEd.Headers.Put('x-motivo', LVal);
    B2 := LEd.BuildPayload(10);
  finally
    LEd.Free;
  end;

  ChecaBool('o header mudou', True, HexDe(B) <> HexDe(B2));

  LEd := TAMQPHeaderEditor.Create(B2);
  try
    ChecaBool('chave nova presente', True,
      LEd.Headers.TryGetValue('x-motivo', LVal));
    ChecaStr('valor da chave nova', 'expired', LVal.AsString);
    ChecaBool('chave antiga sobreviveu', True,
      LEd.Headers.TryGetValue('regiao', LVal));
    ChecaStr('valor antigo', 'sul', LVal.AsString);
    ChecaStr('correlation-id intacto', 'corr-123', LEd.Properties.CorrelationId);
    ChecaInt('priority intacta', 5, LEd.Properties.Priority);
  finally
    LEd.Free;
  end;
end;

// Mensagem publicada sem headers: o editor cria a tabela sob demanda, senao
// nao haveria como acrescentar x-death nela.
procedure THeaderRewriteTests.SemHeaders_GanhaTabelaSobDemanda;
var
  LProps: TAMQPBasicProperties;
  LEd: TAMQPHeaderEditor;
  B, B2: TBytes;
  LVal: TValue;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  B := BuildContentHeader(3, LProps);

  LEd := TAMQPHeaderEditor.Create(B);
  try
    ChecaBool('original nao tinha headers', False, LEd.TinhaHeaders);
    LVal := TValue.From<Int64>(1);
    LEd.Headers.Put('x-death-count', LVal);
    B2 := LEd.BuildPayload(3);
  finally
    LEd.Free;
  end;

  LEd := TAMQPHeaderEditor.Create(B2);
  try
    ChecaBool('agora tem headers', True, LEd.TinhaHeaders);
    ChecaBool('chave presente', True,
      LEd.Headers.TryGetValue('x-death-count', LVal));
    ChecaInt('valor', 1, LVal.AsInt64);
    ChecaStr('content-type intacto', 'text/plain', LEd.Properties.ContentType);
  finally
    LEd.Free;
  end;
end;

// A forma exata do x-death (array de tabelas) atravessando a reescrita. Este
// teste cruza a WS1 (escrita de array no field-table) com a WS3: sem aquela,
// o BuildPayload aqui levantaria EAMQPWire.
procedure THeaderRewriteTests.ArrayDeTabelas_AtravessaAReescrita;
var
  LEd: TAMQPHeaderEditor;
  LProps: TAMQPBasicProperties;
  LEntrada: TAMQPFieldTable;
  LArr: TAMQPValueArray;
  LVal, LElem, LCampo: TValue;
  B, B2: TBytes;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  B := BuildContentHeader(0, LProps);

  LEd := TAMQPHeaderEditor.Create(B);
  try
    LEntrada := TAMQPFieldTable.Create;
    LEntrada.Put('queue', 'fila-origem');
    LEntrada.Put('reason', 'expired');
    LEntrada.Put('count', Int64(1));
    SetLength(LArr, 1);
    LArr[0] := TValue.From<TObject>(LEntrada);
    LVal := TValue.From<TAMQPValueArray>(LArr);
    LEd.Headers.Put('x-death', LVal);
    B2 := LEd.BuildPayload(0);
  finally
    LEd.Free; // dono da tabela, e ela e' dona da entrada dentro do array
  end;

  LEd := TAMQPHeaderEditor.Create(B2);
  try
    ChecaBool('x-death presente', True,
      LEd.Headers.TryGetValue('x-death', LVal));
    LVal := AmqpUnwrapValue(LVal);
    ChecaBool('e array', True, LVal.IsArray);
    ChecaInt('uma entrada', 1, LVal.GetArrayLength);
    LElem := AmqpUnwrapValue(LVal.GetArrayElement(0));
    ChecaBool('entrada e tabela', True,
      LElem.IsObject and (LElem.AsObject is TAMQPFieldTable));
    TAMQPFieldTable(LElem.AsObject).TryGetValue('reason', LCampo);
    ChecaStr('reason', 'expired', LCampo.AsString);
  finally
    LEd.Free;
  end;
end;

// D10: o corpo e' COMPARTILHADO, nunca copiado. E' o que torna a reescrita
// barata mesmo com payload grande.
procedure THeaderRewriteTests.Derive_CompartilhaOCorpoSemCopiar;
var
  LOrig, LNova: TAMQPMessage;
  LCorpo, LHdr: TBytes;
  LProps: TAMQPBasicProperties;
begin
  SetLength(LCorpo, 64);
  LCorpo[0] := 42;
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LHdr := BuildContentHeader(64, LProps);

  LOrig := TAMQPMessage.Create('ex', 'rk', 'guest', LHdr, LCorpo);
  try
    LNova := AmqpDeriveMessage(LOrig, LHdr, 'dlx', 'morta');
    try
      ChecaBool('mesmo buffer de corpo, sem copia', True,
        Pointer(LNova.Body) = Pointer(LOrig.Body));
      ChecaInt('tamanho do corpo', 64, Length(LNova.Body));
    finally
      LNova.Release;
    end;
  finally
    LOrig.Release;
  end;
end;

// A origem nao pode ser tocada: outras filas podem estar segurando a MESMA
// TAMQPMessage, e so' uma delas esta dead-letterando.
procedure THeaderRewriteTests.Derive_NaoTocaNaOrigem;
var
  LOrig, LNova: TAMQPMessage;
  LCorpo, LHdr, LHdr2: TBytes;
  LProps: TAMQPBasicProperties;
  LEd: TAMQPHeaderEditor;
  LVal: TValue;
begin
  SetLength(LCorpo, 4);
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LHdr := BuildContentHeader(4, LProps);
  LOrig := TAMQPMessage.Create('ex', 'rk', 'guest', LHdr, LCorpo);
  try
    LEd := TAMQPHeaderEditor.Create(LOrig.HeaderPayload);
    try
      LVal := TValue.From<Int64>(1);
      LEd.Headers.Put('x-novo', LVal);
      LHdr2 := LEd.BuildPayload(4);
    finally
      LEd.Free;
    end;

    LNova := AmqpDeriveMessage(LOrig, LHdr2, 'dlx', 'morta');
    try
      ChecaStr('header da origem intacto', HexDe(LHdr),
        HexDe(LOrig.HeaderPayload));
      ChecaBool('header da nova e diferente', True,
        HexDe(LNova.HeaderPayload) <> HexDe(LOrig.HeaderPayload));
      ChecaInt('refcount da origem inalterado', 1, LOrig.RefCount);
      ChecaStr('exchange novo', 'dlx', LNova.Exchange);
      ChecaStr('routing key nova', 'morta', LNova.RoutingKey);
      ChecaStr('user-id herdado da origem', 'guest', LNova.UserId);
    finally
      LNova.Release;
    end;
  finally
    LOrig.Release;
  end;
end;

// Body-size divergente faria o consumidor esperar body frames que nunca
// chegam -- por isso AmqpDeriveMessage o preenche a partir do corpo real.
procedure THeaderRewriteTests.Derive_BodySizeBateComOCorpo;
var
  LOrig, LNova: TAMQPMessage;
  LCorpo, LHdr: TBytes;
  LProps: TAMQPBasicProperties;
  LEd: TAMQPHeaderEditor;
begin
  SetLength(LCorpo, 300);
  LProps := TAMQPBasicProperties.Empty;
  LHdr := BuildContentHeader(300, LProps);
  LOrig := TAMQPMessage.Create('ex', 'rk', '', LHdr, LCorpo);
  try
    LEd := TAMQPHeaderEditor.Create(LOrig.HeaderPayload);
    try
      LNova := AmqpDeriveMessage(LOrig, LEd.BuildPayload(LOrig.BodySize),
        'dlx', 'morta');
    finally
      LEd.Free;
    end;
    try
      LEd := TAMQPHeaderEditor.Create(LNova.HeaderPayload);
      try
        ChecaInt('body-size do header bate com o corpo',
          Int64(Length(LNova.Body)), Int64(LEd.BodySizeOriginal));
      finally
        LEd.Free;
      end;
    finally
      LNova.Release;
    end;
  finally
    LOrig.Release;
  end;
end;

procedure THeaderRewriteTests.PayloadMalformado_Levanta;
var
  B: TBytes;
  LOk: Boolean;
begin
  SetLength(B, 3); // curto demais para um content-header
  LOk := False;
  try
    TAMQPHeaderEditor.Create(B).Free;
  except
    on E: Exception do
      LOk := True;
  end;
  ChecaBool('payload truncado tem de levantar', True, LOk);
end;

initialization
  TDUnitX.RegisterTestFixture(TMessageTests);
  TDUnitX.RegisterTestFixture(TResourceTests);
  TDUnitX.RegisterTestFixture(TQueuePolicyTests);
  RegisterTestFixture(THeaderRewriteTests);

end.
