unit AMQP.ServerRoutingTests;

{ Testes dos matchers (AMQP.Server.Routing) e da topologia por vhost
  (AMQP.Server.VHost) -- WS2 da Fase 2 (engine de roteamento em memoria, ver
  CLAUDE.md).

  Os vetores dos matchers foram escritos ANTES do codigo, num arquivo
  normativo do plano do WS2 -- os numeros dos vetores nos comentarios abaixo
  se referem a essa tabela (secoes "Matcher topic" e "Matcher headers").

  Sao testes unitarios puros: nenhum sobe broker nenhum (a exemplo de
  AMQP.ServerEngineTests, e ao contrario das fixtures de handshake).

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerRoutingTests.pas -- mantenha
  os dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Threading,
  AMQP.Server.Resources,
  AMQP.Server.Routing,
  AMQP.Server.VHost;

type
  [TestFixture]
  TTopicMatchTests = class
  public
    [Test] procedure Vetores;
    [Test] procedure DezMilCasamentos_SemEstourarTempo;
    [Test] procedure MuitosHash_NaoExplode;
  end;

  [TestFixture]
  THeadersMatchTests = class
  public
    // Vetores 1-12 da tabela "Matcher headers".
    [Test] procedure V1_Ausente_DefaultAll_TodosBatem;
    [Test] procedure V2_All_ExtrasNaoEstorvam;
    [Test] procedure V3_All_CriterioFaltando;
    [Test] procedure V4_All_CaseSensitive;
    [Test] procedure V5_All_TipoConta;
    [Test] procedure V6_Any_UmCriterioBasta;
    [Test] procedure V7_Any_NenhumBate;
    [Test] procedure V8_All_SemCriterios_VerdadeVacua;
    [Test] procedure V9_Any_SemCriterios_FalsoVacuo;
    [Test] procedure V10_All_ChaveXIgnorada;
    [Test] procedure V11_Any_SemHeadersNaMensagem;
    [Test] procedure V12_All_Booleano_TkBool;
    [Test] procedure ParseHeadersMatch_AusenteAllAnyInvalido;
  end;

  [TestFixture]
  TVHostTests = class
  public
    [Test] procedure Predefinidos_Existem;
    [Test] procedure DeclareExchange_Idempotente;
    [Test] procedure DeclareExchange_Divergente;
    [Test] procedure DeclareExchange_TipoInvalido;
    [Test] procedure DeclareQueue_Idempotente;
    [Test] procedure DeclareQueue_Divergente;
    [Test] procedure BindQueue_ExchangeInexistente;
    [Test] procedure BindQueue_FilaInexistente;
    [Test] procedure BindQueue_HeadersXMatchInvalido;
    [Test] procedure BindQueue_Idempotente;
    [Test] procedure UnbindQueue_RemoveOBinding;
    [Test] procedure UnbindQueue_InexistenteNaoEncontrado;
    [Test] procedure Route_Direct;
    [Test] procedure Route_Fanout;
    [Test] procedure Route_Topic;
    [Test] procedure Route_Headers;
    [Test] procedure Route_DefaultExchange_EntregaPelaFilaDeMesmoNome;
    [Test] procedure Route_FilaEmNBindings_RecebeUmaVezSo;
    [Test] procedure Route_SemRota_ArrayVazio;
    [Test] procedure Route_ExchangeParaExchange_Entrega;
    [Test] procedure Route_Ciclo_NaoTravaNaoDuplica;
    [Test] procedure DeleteQueue_RemoveOsBindings;
    [Test] procedure DeleteExchange_RemoveBindingsPendurados;
  end;

implementation

{ TTopicMatchTests }

type
  TTopicVector = record
    Binding, Routing: string;
    Expected: Boolean;
  end;

const
  TopicVectors: array[1..35] of TTopicVector = (
    (Binding: 'a.b.c';     Routing: 'a.b.c';                 Expected: True),  // 1
    (Binding: 'a.b.c';     Routing: 'a.b.d';                 Expected: False), // 2
    (Binding: 'a.b';       Routing: 'a.b.c';                 Expected: False), // 3
    (Binding: 'a.b.c';     Routing: 'a.b';                   Expected: False), // 4
    (Binding: 'a.*.c';     Routing: 'a.b.c';                 Expected: True),  // 5
    (Binding: 'a.*.c';     Routing: 'a.c';                   Expected: False), // 6
    (Binding: 'a.*';       Routing: 'a.b.c';                 Expected: False), // 7
    (Binding: '*.*';       Routing: 'a.b';                   Expected: True),  // 8
    (Binding: '*.*';       Routing: 'a';                     Expected: False), // 9
    (Binding: 'a.*.b';     Routing: 'a..b';                  Expected: True),  // 10
    (Binding: 'a.#';       Routing: 'a';                     Expected: True),  // 11
    (Binding: 'a.#';       Routing: 'a.b.c.d';                Expected: True),  // 12
    (Binding: 'a.#';       Routing: 'b.a';                    Expected: False), // 13
    (Binding: '#';         Routing: '';                       Expected: True),  // 14
    (Binding: '#';         Routing: 'a.b';                    Expected: True),  // 15
    (Binding: '#';         Routing: 'a..b';                   Expected: True),  // 16
    (Binding: '#.b';       Routing: 'b';                      Expected: True),  // 17
    (Binding: '#.b';       Routing: 'a.x.b';                  Expected: True),  // 18
    (Binding: '#.b';       Routing: 'a.b.c';                  Expected: False), // 19
    (Binding: 'a.#.c';     Routing: 'a.c';                    Expected: True),  // 20
    (Binding: 'a.#.c';     Routing: 'a.b.c';                  Expected: True),  // 21
    (Binding: 'a.#.c';     Routing: 'a.b.x.c';                Expected: True),  // 22
    (Binding: 'a.#.c';     Routing: 'a.b.c.d';                Expected: False), // 23
    (Binding: '#.#';       Routing: 'a.b';                    Expected: True),  // 24
    (Binding: 'a.#.#.b';   Routing: 'a.b';                    Expected: True),  // 25
    (Binding: '*.#';       Routing: 'a';                      Expected: True),  // 26
    (Binding: '*.#';       Routing: '';                       Expected: False), // 27
    (Binding: '#.*';       Routing: 'a';                      Expected: True),  // 28
    (Binding: '#.*';       Routing: '';                       Expected: False), // 29
    (Binding: 'A.b';       Routing: 'a.b';                    Expected: False), // 30
    (Binding: '';          Routing: '';                       Expected: True),  // 31
    (Binding: '';          Routing: 'a';                      Expected: False), // 32
    (Binding: 'a.b';       Routing: 'a.b.';                   Expected: False), // 33
    (Binding: 'nota.*';    Routing: 'nota.aprovada';          Expected: True),  // 34
    (Binding: 'nota.#';    Routing: 'nota.aprovada.fiscal';   Expected: True)   // 35
  );

procedure TTopicMatchTests.Vetores;
var
  I: Integer;
  LGot: Boolean;
begin
  for I := Low(TopicVectors) to High(TopicVectors) do
  begin
    LGot := AmqpTopicMatches(TopicVectors[I].Binding, TopicVectors[I].Routing);
    Assert.AreEqual(TopicVectors[I].Expected, LGot,
      Format('vetor #%d: binding=%s routing=%s',
      [I, TopicVectors[I].Binding, TopicVectors[I].Routing]));
  end;
end;

procedure TTopicMatchTests.DezMilCasamentos_SemEstourarTempo;
var
  I: Integer;
  LStart, LElapsedMs: Int64;
begin
  LStart := AmqpTickMs;
  for I := 1 to 10000 do
    AmqpTopicMatches('a.#.c.*.e', 'a.b.c.d.e');
  LElapsedMs := AmqpTickMs - LStart;
  Assert.IsTrue(LElapsedMs < 2000,
    Format('10000 casamentos em %d ms (esperado < 2000ms -- guarda barata ' +
    'contra uma implementacao que aloque por chamada)', [LElapsedMs]));
end;

// Regressao de negacao de servico: a implementacao recursiva anterior
// ("para cada '#', tente 0, 1, 2... palavras") era exponencial no numero de
// '#'. Medido antes da troca, com 24 palavras de routing: 6 '#' = 15 ms,
// 7 = 110 ms, 8 = 453 ms -- e uma binding key cabe mais de 100 '#'. Como o
// match roda na thread do publicador e com o RWLock de leitura na mao, um
// unico declare derrubava o broker. Com o casamento iterativo (backtracking
// so' do ultimo '#') isto e' instantaneo.
procedure TTopicMatchTests.MuitosHash_NaoExplode;
var
  LBind, LRoute: string;
  I: Integer;
  LStart, LElapsedMs: Int64;
begin
  LBind := '';
  for I := 1 to 8 do
    LBind := LBind + '#.';
  LBind := LBind + 'zz'; // nunca casa: o routing nao termina em 'zz'
  LRoute := 'a';
  for I := 2 to 24 do
    LRoute := LRoute + '.a';

  LStart := AmqpTickMs;
  Assert.IsFalse(AmqpTopicMatches(LBind, LRoute), '8 hashes contra 24 palavras nao casa');
  LElapsedMs := AmqpTickMs - LStart;
  Assert.IsTrue(LElapsedMs < 1000,
    Format('casamento patologico levou %d ms (esperado < 1000ms -- ' + 'guarda contra a volta do matcher exponencial)', [LElapsedMs]));
end;

{ THeadersMatchTests }

procedure THeadersMatchTests.V1_Ausente_DefaultAll_TodosBatem;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    LBind.Put('regiao', 'sul');
    LMsg.Put('tipo', 'nfe');
    LMsg.Put('regiao', 'sul');
    Assert.IsTrue(AmqpHeadersMatch(LBind, LMsg, amqhmAll),
      'x-match ausente -> default all, todos os criterios batem');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V2_All_ExtrasNaoEstorvam;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    LBind.Put('regiao', 'sul');
    LMsg.Put('tipo', 'nfe');
    LMsg.Put('regiao', 'sul');
    LMsg.Put('lote', 7);
    Assert.IsTrue(AmqpHeadersMatch(LBind, LMsg, amqhmAll),
      'mensagem com campo extra ainda casa em all');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V3_All_CriterioFaltando;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    LBind.Put('regiao', 'sul');
    LMsg.Put('tipo', 'nfe');
    Assert.IsFalse(AmqpHeadersMatch(LBind, LMsg, amqhmAll),
      'falta o criterio regiao na mensagem');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V4_All_CaseSensitive;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    LMsg.Put('tipo', 'NFE');
    Assert.IsFalse(AmqpHeadersMatch(LBind, LMsg, amqhmAll),
      'comparacao de string e case-sensitive');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V5_All_TipoConta;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('lote', 7);   // inteiro
    LMsg.Put('lote', '7');  // string
    Assert.IsFalse(AmqpHeadersMatch(LBind, LMsg, amqhmAll),
      'inteiro e string com o mesmo texto NAO sao iguais');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V6_Any_UmCriterioBasta;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    LBind.Put('regiao', 'sul');
    LMsg.Put('regiao', 'sul');
    Assert.IsTrue(AmqpHeadersMatch(LBind, LMsg, amqhmAny),
      'any -- um criterio ja basta');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V7_Any_NenhumBate;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    LBind.Put('regiao', 'sul');
    LMsg.Put('tipo', 'cte');
    LMsg.Put('regiao', 'norte');
    Assert.IsFalse(AmqpHeadersMatch(LBind, LMsg, amqhmAny),
      'any -- nenhum criterio bate');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V8_All_SemCriterios_VerdadeVacua;
begin
  Assert.IsTrue(AmqpHeadersMatch(nil, nil, amqhmAll),
    'all sem nenhum criterio -- verdade vacua');
end;

procedure THeadersMatchTests.V9_Any_SemCriterios_FalsoVacuo;
var
  LMsg: TAMQPFieldTable;
begin
  LMsg := TAMQPFieldTable.Create;
  try
    LMsg.Put('tipo', 'nfe');
    Assert.IsFalse(AmqpHeadersMatch(nil, LMsg, amqhmAny),
      'any sem nenhum criterio -- falso vacuo');
  finally
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V10_All_ChaveXIgnorada;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    LBind.Put('x-lixo', 1);
    LMsg.Put('tipo', 'nfe');
    Assert.IsTrue(AmqpHeadersMatch(LBind, LMsg, amqhmAll),
      'chave x- do binding e ignorada no casamento');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V11_Any_SemHeadersNaMensagem;
var
  LBind: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  try
    LBind.Put('tipo', 'nfe');
    Assert.IsFalse(AmqpHeadersMatch(LBind, nil, amqhmAny),
      'any com mensagem sem headers nenhum');
  finally
    LBind.Free;
  end;
end;

procedure THeadersMatchTests.V12_All_Booleano_TkBool;
var
  LBind, LMsg: TAMQPFieldTable;
begin
  LBind := TAMQPFieldTable.Create;
  LMsg := TAMQPFieldTable.Create;
  try
    LBind.Put('ativo', True);
    LMsg.Put('ativo', True);
    Assert.IsTrue(AmqpHeadersMatch(LBind, LMsg, amqhmAll),
      'booleano (tkBool no FPC) compara certo');
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.ParseHeadersMatch_AusenteAllAnyInvalido;
var
  LArgs: TAMQPFieldTable;
  LMode: TAMQPHeadersMatch;
begin
  Assert.IsTrue(AmqpParseHeadersMatch(nil, LMode),
    'x-match ausente -> valido, default all');
  Assert.IsTrue(LMode = amqhmAll, 'default e amqhmAll');

  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-match', 'all');
    Assert.IsTrue(AmqpParseHeadersMatch(LArgs, LMode), 'x-match=all -> valido');
    Assert.IsTrue(LMode = amqhmAll, 'modo amqhmAll');
  finally
    LArgs.Free;
  end;

  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-match', 'any');
    Assert.IsTrue(AmqpParseHeadersMatch(LArgs, LMode), 'x-match=any -> valido');
    Assert.IsTrue(LMode = amqhmAny, 'modo amqhmAny');
  finally
    LArgs.Free;
  end;

  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-match', 'xyz');
    Assert.IsFalse(AmqpParseHeadersMatch(LArgs, LMode),
      'x-match invalido -> False (bind e 406 na WS5)');
  finally
    LArgs.Free;
  end;
end;

{ TVHostTests }

procedure TVHostTests.Predefinidos_Existem;
var
  LVHost: TAMQPVHost;
  LDef: TAMQPExchangeDef;
begin
  LVHost := TAMQPVHost.Create;
  try
    Assert.IsTrue(LVHost.ExchangeExists(''), 'exchange default existe');
    Assert.IsTrue(LVHost.ExchangeExists('amq.direct'), 'amq.direct existe');
    Assert.IsTrue(LVHost.ExchangeExists('amq.fanout'), 'amq.fanout existe');
    Assert.IsTrue(LVHost.ExchangeExists('amq.topic'), 'amq.topic existe');
    Assert.IsTrue(LVHost.ExchangeExists('amq.headers'), 'amq.headers existe');
    Assert.IsTrue(LVHost.ExchangeExists('amq.match'), 'amq.match existe');

    LDef := LVHost.ExchangeDef('');
    Assert.AreEqual(AMQP_EXCHANGE_TYPE_DIRECT, LDef.ExchangeType,
      'exchange default e direct');
    LDef := LVHost.ExchangeDef('amq.match');
    Assert.AreEqual(AMQP_EXCHANGE_TYPE_HEADERS, LDef.ExchangeType,
      'amq.match e um alias de headers');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.DeclareExchange_Idempotente;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    Assert.IsTrue(LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_TOPIC,
      True, False, False, nil) = amqtrOk, 'primeira declaracao');
    Assert.IsTrue(LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_TOPIC,
      True, False, False, nil) = amqtrEquivalente, 'redeclare identico -> equivalente');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.DeclareExchange_Divergente;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    Assert.IsTrue(LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_TOPIC,
      True, False, False, nil) = amqtrOk, 'primeira declaracao');
    Assert.IsTrue(LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_TOPIC,
      False, False, False, nil) = amqtrDivergente,
      'redeclare com durable diferente -> divergente');
    Assert.IsTrue(LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_FANOUT,
      True, False, False, nil) = amqtrDivergente,
      'redeclare com tipo diferente -> divergente');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.DeclareExchange_TipoInvalido;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    Assert.IsTrue(LVHost.DeclareExchange('ex1', 'custom', True, False, False,
      nil) = amqtrTipoInvalido, 'tipo desconhecido -> amqtrTipoInvalido');
    Assert.IsFalse(LVHost.ExchangeExists('ex1'), 'nao ficou registrado');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.DeclareQueue_Idempotente;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    Assert.IsTrue(LVHost.DeclareQueue('q1', True, False, False, nil) =
      amqtrOk, 'primeira declaracao');
    Assert.IsTrue(LVHost.DeclareQueue('q1', True, False, False, nil) =
      amqtrEquivalente, 'redeclare identica -> equivalente');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.DeclareQueue_Divergente;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    Assert.IsTrue(LVHost.DeclareQueue('q1', True, False, False, nil) =
      amqtrOk, 'primeira declaracao');
    Assert.IsTrue(LVHost.DeclareQueue('q1', True, True, False, nil) =
      amqtrDivergente, 'redeclare com exclusive diferente -> divergente');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.BindQueue_ExchangeInexistente;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareQueue('q1', True, False, False, nil);
    Assert.IsTrue(LVHost.BindQueue('nao-existe', 'q1', 'rk', nil) =
      amqtrNaoEncontrado, 'exchange nao existe -> naoEncontrado');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.BindQueue_FilaInexistente;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, nil);
    Assert.IsTrue(LVHost.BindQueue('ex1', 'nao-existe', 'rk', nil) =
      amqtrNaoEncontrado, 'fila nao existe -> naoEncontrado');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.BindQueue_HeadersXMatchInvalido;
var
  LVHost: TAMQPVHost;
  LArgs: TAMQPFieldTable;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('exh', AMQP_EXCHANGE_TYPE_HEADERS, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LArgs := TAMQPFieldTable.Create;
    LArgs.Put('x-match', 'xyz');
    Assert.IsTrue(LVHost.BindQueue('exh', 'q1', '', LArgs) =
      amqtrTipoInvalido, 'x-match invalido -> amqtrTipoInvalido');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.BindQueue_Idempotente;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    Assert.IsTrue(LVHost.BindQueue('ex1', 'q1', 'rk', nil) = amqtrOk,
      'primeiro bind');
    Assert.IsTrue(LVHost.BindQueue('ex1', 'q1', 'rk', nil) = amqtrOk,
      'bind identico de novo -> ok (idempotente, nao duplica)');

    LResult := LVHost.Route('ex1', 'rk', nil);
    Assert.AreEqual(1, Length(LResult), 'bind repetido nao duplica a entrega');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.UnbindQueue_RemoveOBinding;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LVHost.BindQueue('ex1', 'q1', 'rk', nil);
    Assert.IsTrue(LVHost.UnbindQueue('ex1', 'q1', 'rk', nil) = amqtrOk,
      'unbind');
    LResult := LVHost.Route('ex1', 'rk', nil);
    Assert.AreEqual(0, Length(LResult), 'nao entrega mais depois do unbind');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.UnbindQueue_InexistenteNaoEncontrado;
var
  LVHost: TAMQPVHost;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    Assert.IsTrue(LVHost.UnbindQueue('ex1', 'q1', 'rk-nunca-ligada', nil) =
      amqtrNaoEncontrado, 'unbind de algo que nunca foi ligado -> naoEncontrado');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_Direct;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('exd', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LVHost.BindQueue('exd', 'q1', 'rk', nil);

    LResult := LVHost.Route('exd', 'rk', nil);
    Assert.AreEqual(1, Length(LResult), 'routing key igual -> entrega');
    Assert.AreEqual('q1', LResult[0]);

    LResult := LVHost.Route('exd', 'rk-diferente', nil);
    Assert.AreEqual(0, Length(LResult), 'routing key diferente -> sem entrega');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_Fanout;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('exf', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LVHost.DeclareQueue('q2', True, False, False, nil);
    LVHost.BindQueue('exf', 'q1', '', nil);
    LVHost.BindQueue('exf', 'q2', 'ignorada-mesmo-assim', nil);

    LResult := LVHost.Route('exf', 'qualquer-coisa', nil);
    Assert.AreEqual(2, Length(LResult),
      'fanout entrega pras duas filas, binding key ignorada');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_Topic;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ext', AMQP_EXCHANGE_TYPE_TOPIC, True, False,
      False, nil);
    LVHost.DeclareQueue('qAprovada', True, False, False, nil);
    LVHost.DeclareQueue('qTodas', True, False, False, nil);
    LVHost.BindQueue('ext', 'qAprovada', 'nota.aprovada', nil);
    LVHost.BindQueue('ext', 'qTodas', 'nota.#', nil);

    LResult := LVHost.Route('ext', 'nota.aprovada', nil);
    Assert.AreEqual(2, Length(LResult), 'as duas filas casam em nota.aprovada');

    LResult := LVHost.Route('ext', 'nota.rejeitada', nil);
    Assert.AreEqual(1, Length(LResult), 'so a fila # casa em nota.rejeitada');
    Assert.AreEqual('qTodas', LResult[0]);
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_Headers;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
  LArgs, LHeaders: TAMQPFieldTable;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('exh', AMQP_EXCHANGE_TYPE_HEADERS, True, False,
      False, nil);
    LVHost.DeclareQueue('qAll', True, False, False, nil);

    LArgs := TAMQPFieldTable.Create;
    LArgs.Put('x-match', 'all');
    LArgs.Put('tipo', 'nfe');
    LVHost.BindQueue('exh', 'qAll', '', LArgs); // vhost fica dono de LArgs

    LHeaders := TAMQPFieldTable.Create;
    try
      LHeaders.Put('tipo', 'nfe');
      LResult := LVHost.Route('exh', '', LHeaders);
      Assert.AreEqual(1, Length(LResult), 'headers casam -> entrega');
    finally
      LHeaders.Free;
    end;

    LHeaders := TAMQPFieldTable.Create;
    try
      LHeaders.Put('tipo', 'cte');
      LResult := LVHost.Route('exh', '', LHeaders);
      Assert.AreEqual(0, Length(LResult), 'headers nao casam -> sem entrega');
    finally
      LHeaders.Free;
    end;
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_DefaultExchange_EntregaPelaFilaDeMesmoNome;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareQueue('minhaFila', True, False, False, nil);
    LResult := LVHost.Route('', 'minhaFila', nil);
    Assert.AreEqual(1, Length(LResult),
      'publish direto na fila via exchange default');
    Assert.AreEqual('minhaFila', LResult[0]);

    LResult := LVHost.Route('', 'fila-que-nao-existe', nil);
    Assert.AreEqual(0, Length(LResult), 'fila inexistente -> sem rota');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_FilaEmNBindings_RecebeUmaVezSo;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ext', AMQP_EXCHANGE_TYPE_TOPIC, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LVHost.BindQueue('ext', 'q1', 'a.#', nil);
    LVHost.BindQueue('ext', 'q1', '#.b', nil);
    LVHost.BindQueue('ext', 'q1', '*.*', nil);

    // 'a.b' casa nos TRES bindings acima -- so' pode entregar uma vez.
    LResult := LVHost.Route('ext', 'a.b', nil);
    Assert.AreEqual(1, Length(LResult),
      'fila casada por 3 bindings entrega uma unica vez');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_SemRota_ArrayVazio;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ext', AMQP_EXCHANGE_TYPE_TOPIC, True, False,
      False, nil);
    LResult := LVHost.Route('ext', 'nada.casa', nil);
    Assert.AreEqual(0, Length(LResult), 'sem binding nenhum -> array vazio');

    LResult := LVHost.Route('exchange-inexistente', 'rk', nil);
    Assert.AreEqual(0, Length(LResult),
      'exchange inexistente -> array vazio (nao erro)');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_ExchangeParaExchange_Entrega;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('exA', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareExchange('exB', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LVHost.BindQueue('exB', 'q1', 'rk', nil);
    // exA -> exB (Exchange.Bind: destino=exB, origem=exA)
    LVHost.BindExchange('exB', 'exA', 'ignorada-no-fanout', nil);

    LResult := LVHost.Route('exA', 'rk', nil);
    Assert.AreEqual(1, Length(LResult), 'publish em exA chega em q1 via exB');
    Assert.AreEqual('q1', LResult[0]);
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.Route_Ciclo_NaoTravaNaoDuplica;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('exA', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareExchange('exB', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LVHost.BindQueue('exA', 'q1', '', nil);
    // ciclo: exA -> exB -> exA
    LVHost.BindExchange('exB', 'exA', '', nil);
    LVHost.BindExchange('exA', 'exB', '', nil);

    // Se nao terminar, o runner de testes trava/estoura -- o proprio teste
    // rodar ate o fim ja e' parte da asserção.
    LResult := LVHost.Route('exA', 'qualquer', nil);
    Assert.AreEqual(1, Length(LResult), 'ciclo nao duplica a entrega em q1');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.DeleteQueue_RemoveOsBindings;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ext', AMQP_EXCHANGE_TYPE_TOPIC, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil); // auto-bind no default
    LVHost.BindQueue('ext', 'q1', 'a.#', nil);

    Assert.IsTrue(LVHost.DeleteQueue('q1') = amqtrOk, 'delete');
    Assert.IsFalse(LVHost.QueueExists('q1'), 'fila sumiu');

    LResult := LVHost.Route('ext', 'a.b', nil);
    Assert.AreEqual(0, Length(LResult), 'binding no topic sumiu junto');

    LResult := LVHost.Route('', 'q1', nil);
    Assert.AreEqual(0, Length(LResult),
      'auto-bind no exchange default tambem sumiu');

    Assert.IsTrue(LVHost.DeleteQueue('q1') = amqtrNaoEncontrado,
      'delete de novo -> naoEncontrado');
  finally
    LVHost.Free;
  end;
end;

procedure TVHostTests.DeleteExchange_RemoveBindingsPendurados;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('exA', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareExchange('exB', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, nil);
    LVHost.DeclareQueue('q1', True, False, False, nil);
    LVHost.BindQueue('exB', 'q1', 'rk', nil);
    LVHost.BindExchange('exB', 'exA', 'rk', nil); // exA -> exB

    Assert.IsTrue(LVHost.DeleteExchange('exB', False) = amqtrOk, 'delete exB');

    // O binding exA->exB nao pode ter sobrado apontando pro vazio: publicar
    // em exA nao pode dar erro nem "achar" q1 mais.
    LResult := LVHost.Route('exA', 'rk', nil);
    Assert.AreEqual(0, Length(LResult), 'binding pendurado para exB sumiu');
  finally
    LVHost.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TTopicMatchTests);
  TDUnitX.RegisterTestFixture(THeadersMatchTests);
  TDUnitX.RegisterTestFixture(TVHostTests);

end.
