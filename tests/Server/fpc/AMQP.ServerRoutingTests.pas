unit AMQP.ServerRoutingTests;

{ Testes dos matchers (AMQP.Server.Routing) e da topologia por vhost
  (AMQP.Server.VHost) -- WS2 da Fase 2 (engine de roteamento em memoria, ver
  CLAUDE.md).

  Os vetores dos matchers foram escritos ANTES do codigo, num arquivo
  normativo do plano do WS2 -- os numeros dos vetores nos comentarios abaixo
  se referem a essa tabela (secoes "Matcher topic" e "Matcher headers").

  Sao testes unitarios puros: nenhum sobe broker nenhum (a exemplo de
  AMQP.ServerEngineTests, e ao contrario das fixtures de handshake).

  Espelho DUnitX de tests\Server\AMQP.ServerRoutingTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Rtti,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Threading,
  AMQP.Server.Resources,
  AMQP.Server.Routing,
  AMQP.Server.VHost;

type
  TTopicMatchTests = class(TTestCase)
  published
    procedure Vetores;
    procedure DezMilCasamentos_SemEstourarTempo;
    procedure MuitosHash_NaoExplode;
  end;

  THeadersMatchTests = class(TTestCase)
  published
    // Vetores 1-12 da tabela "Matcher headers".
    procedure V1_Ausente_DefaultAll_TodosBatem;
    procedure V2_All_ExtrasNaoEstorvam;
    procedure V3_All_CriterioFaltando;
    procedure V4_All_CaseSensitive;
    procedure V5_All_TipoConta;
    procedure V6_Any_UmCriterioBasta;
    procedure V7_Any_NenhumBate;
    procedure V8_All_SemCriterios_VerdadeVacua;
    procedure V9_Any_SemCriterios_FalsoVacuo;
    procedure V10_All_ChaveXIgnorada;
    procedure V11_Any_SemHeadersNaMensagem;
    procedure V12_All_Booleano_TkBool;
    procedure ParseHeadersMatch_AusenteAllAnyInvalido;
  end;

  TVHostTests = class(TTestCase)
  published
    procedure Predefinidos_Existem;
    procedure DeclareExchange_Idempotente;
    procedure DeclareExchange_Divergente;
    procedure DeclareExchange_TipoInvalido;
    procedure DeclareQueue_Idempotente;
    procedure DeclareQueue_Divergente;
    procedure BindQueue_ExchangeInexistente;
    procedure BindQueue_FilaInexistente;
    procedure BindQueue_HeadersXMatchInvalido;
    procedure BindQueue_Idempotente;
    procedure UnbindQueue_RemoveOBinding;
    procedure UnbindQueue_InexistenteNaoEncontrado;
    procedure Route_Direct;
    procedure Route_Fanout;
    procedure Route_Topic;
    procedure Route_Headers;
    procedure Route_DefaultExchange_EntregaPelaFilaDeMesmoNome;
    procedure Route_FilaEmNBindings_RecebeUmaVezSo;
    procedure Route_SemRota_ArrayVazio;
    procedure Route_ExchangeParaExchange_Entrega;
    procedure Ae_RecebeOQueNaoCasou;
    procedure Ae_NaoDisparaSeUmBindingCasou;
    procedure Ae_EmCadeia;
    procedure Ae_CicloNaoTrava;
    procedure Ae_Inexistente_SemRota;
    procedure Ae_PodeSerDeOutroTipo;
    procedure Route_GrafoDenso_NaoExplode;
    procedure Route_Ciclo_NaoTravaNaoDuplica;
    procedure DeleteQueue_RemoveOsBindings;
    procedure DeleteExchange_RemoveBindingsPendurados;
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
    AssertEquals(Format('vetor #%d: binding=%s routing=%s',
      [I, TopicVectors[I].Binding, TopicVectors[I].Routing]),
      TopicVectors[I].Expected, LGot);
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
  AssertTrue(Format('10000 casamentos em %d ms (esperado < 2000ms -- guarda ' +
    'barata contra uma implementacao que aloque por chamada)', [LElapsedMs]),
    LElapsedMs < 2000);
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
  AssertFalse('8 hashes contra 24 palavras nao casa', AmqpTopicMatches(LBind, LRoute));
  LElapsedMs := AmqpTickMs - LStart;
  AssertTrue(Format('casamento patologico levou %d ms (esperado < 1000ms -- ' + 'guarda contra a volta do matcher exponencial)', [LElapsedMs]),
    LElapsedMs < 1000);
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
    AssertTrue('x-match ausente -> default all, todos os criterios batem',
      AmqpHeadersMatch(LBind, LMsg, amqhmAll));
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
    AssertTrue('mensagem com campo extra ainda casa em all',
      AmqpHeadersMatch(LBind, LMsg, amqhmAll));
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
    AssertFalse('falta o criterio regiao na mensagem',
      AmqpHeadersMatch(LBind, LMsg, amqhmAll));
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
    AssertFalse('comparacao de string e case-sensitive',
      AmqpHeadersMatch(LBind, LMsg, amqhmAll));
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
    AssertFalse('inteiro e string com o mesmo texto NAO sao iguais',
      AmqpHeadersMatch(LBind, LMsg, amqhmAll));
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
    AssertTrue('any -- um criterio ja basta',
      AmqpHeadersMatch(LBind, LMsg, amqhmAny));
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
    AssertFalse('any -- nenhum criterio bate',
      AmqpHeadersMatch(LBind, LMsg, amqhmAny));
  finally
    LBind.Free;
    LMsg.Free;
  end;
end;

procedure THeadersMatchTests.V8_All_SemCriterios_VerdadeVacua;
begin
  AssertTrue('all sem nenhum criterio -- verdade vacua',
    AmqpHeadersMatch(nil, nil, amqhmAll));
end;

procedure THeadersMatchTests.V9_Any_SemCriterios_FalsoVacuo;
var
  LMsg: TAMQPFieldTable;
begin
  LMsg := TAMQPFieldTable.Create;
  try
    LMsg.Put('tipo', 'nfe');
    AssertFalse('any sem nenhum criterio -- falso vacuo',
      AmqpHeadersMatch(nil, LMsg, amqhmAny));
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
    AssertTrue('chave x- do binding e ignorada no casamento',
      AmqpHeadersMatch(LBind, LMsg, amqhmAll));
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
    AssertFalse('any com mensagem sem headers nenhum',
      AmqpHeadersMatch(LBind, nil, amqhmAny));
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
    AssertTrue('booleano (tkBool no FPC) compara certo',
      AmqpHeadersMatch(LBind, LMsg, amqhmAll));
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
  AssertTrue('x-match ausente -> valido, default all',
    AmqpParseHeadersMatch(nil, LMode));
  AssertTrue('default e amqhmAll', LMode = amqhmAll);

  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-match', 'all');
    AssertTrue('x-match=all -> valido', AmqpParseHeadersMatch(LArgs, LMode));
    AssertTrue('modo amqhmAll', LMode = amqhmAll);
  finally
    LArgs.Free;
  end;

  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-match', 'any');
    AssertTrue('x-match=any -> valido', AmqpParseHeadersMatch(LArgs, LMode));
    AssertTrue('modo amqhmAny', LMode = amqhmAny);
  finally
    LArgs.Free;
  end;

  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-match', 'xyz');
    AssertFalse('x-match invalido -> False (bind e 406 na WS5)',
      AmqpParseHeadersMatch(LArgs, LMode));
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
    AssertTrue('exchange default existe', LVHost.ExchangeExists(''));
    AssertTrue('amq.direct existe', LVHost.ExchangeExists('amq.direct'));
    AssertTrue('amq.fanout existe', LVHost.ExchangeExists('amq.fanout'));
    AssertTrue('amq.topic existe', LVHost.ExchangeExists('amq.topic'));
    AssertTrue('amq.headers existe', LVHost.ExchangeExists('amq.headers'));
    AssertTrue('amq.match existe', LVHost.ExchangeExists('amq.match'));

    LDef := LVHost.ExchangeDef('');
    AssertEquals('exchange default e direct', AMQP_EXCHANGE_TYPE_DIRECT,
      LDef.ExchangeType);
    LDef := LVHost.ExchangeDef('amq.match');
    AssertEquals('amq.match e um alias de headers', AMQP_EXCHANGE_TYPE_HEADERS,
      LDef.ExchangeType);
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
    AssertTrue('primeira declaracao', LVHost.DeclareExchange('ex1',
      AMQP_EXCHANGE_TYPE_TOPIC, True, False, False, nil) = amqtrOk);
    AssertTrue('redeclare identico -> equivalente', LVHost.DeclareExchange('ex1',
      AMQP_EXCHANGE_TYPE_TOPIC, True, False, False, nil) = amqtrEquivalente);
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
    AssertTrue('primeira declaracao', LVHost.DeclareExchange('ex1',
      AMQP_EXCHANGE_TYPE_TOPIC, True, False, False, nil) = amqtrOk);
    AssertTrue('redeclare com durable diferente -> divergente',
      LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_TOPIC, False, False,
      False, nil) = amqtrDivergente);
    AssertTrue('redeclare com tipo diferente -> divergente',
      LVHost.DeclareExchange('ex1', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil) = amqtrDivergente);
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
    AssertTrue('tipo desconhecido -> amqtrTipoInvalido',
      LVHost.DeclareExchange('ex1', 'custom', True, False, False, nil) =
      amqtrTipoInvalido);
    AssertFalse('nao ficou registrado', LVHost.ExchangeExists('ex1'));
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
    AssertTrue('primeira declaracao',
      LVHost.DeclareQueue('q1', True, False, False, nil) = amqtrOk);
    AssertTrue('redeclare identica -> equivalente',
      LVHost.DeclareQueue('q1', True, False, False, nil) = amqtrEquivalente);
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
    AssertTrue('primeira declaracao',
      LVHost.DeclareQueue('q1', True, False, False, nil) = amqtrOk);
    AssertTrue('redeclare com exclusive diferente -> divergente',
      LVHost.DeclareQueue('q1', True, True, False, nil) = amqtrDivergente);
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
    AssertTrue('exchange nao existe -> naoEncontrado',
      LVHost.BindQueue('nao-existe', 'q1', 'rk', nil) = amqtrNaoEncontrado);
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
    AssertTrue('fila nao existe -> naoEncontrado',
      LVHost.BindQueue('ex1', 'nao-existe', 'rk', nil) = amqtrNaoEncontrado);
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
    AssertTrue('x-match invalido -> amqtrTipoInvalido',
      LVHost.BindQueue('exh', 'q1', '', LArgs) = amqtrTipoInvalido);
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
    AssertTrue('primeiro bind', LVHost.BindQueue('ex1', 'q1', 'rk', nil) = amqtrOk);
    AssertTrue('bind identico de novo -> ok (idempotente, nao duplica)',
      LVHost.BindQueue('ex1', 'q1', 'rk', nil) = amqtrOk);

    LResult := LVHost.Route('ex1', 'rk', nil);
    AssertEquals('bind repetido nao duplica a entrega', 1, Length(LResult));
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
    AssertTrue('unbind', LVHost.UnbindQueue('ex1', 'q1', 'rk', nil) = amqtrOk);
    LResult := LVHost.Route('ex1', 'rk', nil);
    AssertEquals('nao entrega mais depois do unbind', 0, Length(LResult));
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
    AssertTrue('unbind de algo que nunca foi ligado -> naoEncontrado',
      LVHost.UnbindQueue('ex1', 'q1', 'rk-nunca-ligada', nil) = amqtrNaoEncontrado);
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
    AssertEquals('routing key igual -> entrega', 1, Length(LResult));
    AssertEquals('q1', LResult[0]);

    LResult := LVHost.Route('exd', 'rk-diferente', nil);
    AssertEquals('routing key diferente -> sem entrega', 0, Length(LResult));
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
    AssertEquals('fanout entrega pras duas filas, binding key ignorada',
      2, Length(LResult));
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
    AssertEquals('as duas filas casam em nota.aprovada', 2, Length(LResult));

    LResult := LVHost.Route('ext', 'nota.rejeitada', nil);
    AssertEquals('so a fila # casa em nota.rejeitada', 1, Length(LResult));
    AssertEquals('qTodas', LResult[0]);
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
      AssertEquals('headers casam -> entrega', 1, Length(LResult));
    finally
      LHeaders.Free;
    end;

    LHeaders := TAMQPFieldTable.Create;
    try
      LHeaders.Put('tipo', 'cte');
      LResult := LVHost.Route('exh', '', LHeaders);
      AssertEquals('headers nao casam -> sem entrega', 0, Length(LResult));
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
    AssertEquals('publish direto na fila via exchange default', 1,
      Length(LResult));
    AssertEquals('minhaFila', LResult[0]);

    LResult := LVHost.Route('', 'fila-que-nao-existe', nil);
    AssertEquals('fila inexistente -> sem rota', 0, Length(LResult));
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
    AssertEquals('fila casada por 3 bindings entrega uma unica vez', 1,
      Length(LResult));
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
    AssertEquals('sem binding nenhum -> array vazio', 0, Length(LResult));

    LResult := LVHost.Route('exchange-inexistente', 'rk', nil);
    AssertEquals('exchange inexistente -> array vazio (nao erro)', 0,
      Length(LResult));
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
    AssertEquals('publish em exA chega em q1 via exB', 1, Length(LResult));
    AssertEquals('q1', LResult[0]);
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
    AssertEquals('ciclo nao duplica a entrega em q1', 1, Length(LResult));
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

    AssertTrue('delete', LVHost.DeleteQueue('q1') = amqtrOk);
    AssertFalse('fila sumiu', LVHost.QueueExists('q1'));

    LResult := LVHost.Route('ext', 'a.b', nil);
    AssertEquals('binding no topic sumiu junto', 0, Length(LResult));

    LResult := LVHost.Route('', 'q1', nil);
    AssertEquals('auto-bind no exchange default tambem sumiu', 0,
      Length(LResult));

    AssertTrue('delete de novo -> naoEncontrado',
      LVHost.DeleteQueue('q1') = amqtrNaoEncontrado);
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

    AssertTrue('delete exB', LVHost.DeleteExchange('exB', False) = amqtrOk);

    // O binding exA->exB nao pode ter sobrado apontando pro vazio: publicar
    // em exA nao pode dar erro nem "achar" q1 mais.
    LResult := LVHost.Route('exA', 'rk', nil);
    AssertEquals('binding pendurado para exB sumiu', 0, Length(LResult));
  finally
    LVHost.Free;
  end;
end;

// --- alternate exchange (WS7 da Fase 3) ------------------------------------

// Monta os argumentos de declare com alternate-exchange.
function ArgsAe(const ANome: string): TAMQPFieldTable;
var
  LVal: TValue;
begin
  Result := TAMQPFieldTable.Create;
  LVal := TValue.From<string>(ANome);
  Result.Put('alternate-exchange', LVal);
end;

procedure TVHostTests.Ae_RecebeOQueNaoCasou;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ae', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareExchange('ex', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, ArgsAe('ae'));
    LVHost.DeclareQueue('q.normal', True, False, False, nil);
    LVHost.DeclareQueue('q.ae', True, False, False, nil);
    LVHost.BindQueue('ex', 'q.normal', 'certa', nil);
    LVHost.BindQueue('ae', 'q.ae', '', nil);

    LResult := LVHost.Route('ex', 'certa', nil);
    AssertEquals('casou: vai pela rota normal', 1, Length(LResult));
    AssertEquals('q.normal', LResult[0]);

    LResult := LVHost.Route('ex', 'nao-casa-nada', nil);
    AssertEquals('nao casou: vai para o AE', 1, Length(LResult));
    AssertEquals('q.ae', LResult[0]);
  finally
    LVHost.Free;
  end;
end;

// O AE dispara quando NENHUM BINDING DESTE exchange casou -- nao quando o
// resultado final ficou vazio. Aqui um binding de 'ex' CASA e leva a 'meio',
// que nao tem binding nenhum: o resultado e' vazio, mas o AE de 'ex' NAO pode
// disparar, porque 'ex' roteou. Quem deixou de rotear foi 'meio'.
procedure TVHostTests.Ae_NaoDisparaSeUmBindingCasou;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ae', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareExchange('meio', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareExchange('ex', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, ArgsAe('ae'));
    LVHost.DeclareQueue('q.ae', True, False, False, nil);
    LVHost.BindQueue('ae', 'q.ae', '', nil);
    // Exchange.Bind e' (DESTINO, ORIGEM): mensagens de 'ex' vao para 'meio'.
    LVHost.BindExchange('meio', 'ex', '', nil);

    LResult := LVHost.Route('ex', 'qualquer', nil);
    AssertEquals('binding casou, entao o AE de ex nao entra', 0,
      Length(LResult));
  finally
    LVHost.Free;
  end;
end;

// AE de AE: a cadeia desce enquanto ninguem casar.
procedure TVHostTests.Ae_EmCadeia;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ae2', AMQP_EXCHANGE_TYPE_FANOUT, True, False,
      False, nil);
    LVHost.DeclareExchange('ae1', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, ArgsAe('ae2'));
    LVHost.DeclareExchange('ex', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, ArgsAe('ae1'));
    LVHost.DeclareQueue('q.fim', True, False, False, nil);
    LVHost.BindQueue('ae2', 'q.fim', '', nil);

    LResult := LVHost.Route('ex', 'nada-casa', nil);
    AssertEquals('desceu dois niveis de AE', 1, Length(LResult));
    AssertEquals('q.fim', LResult[0]);
  finally
    LVHost.Free;
  end;
end;

// Ciclo por AE (ex -> ae -> ex) nao pode travar nem duplicar: o conjunto de
// visitados e o mesmo do caminho exchange->exchange.
procedure TVHostTests.Ae_CicloNaoTrava;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('a', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, ArgsAe('b'));
    LVHost.DeclareExchange('b', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, ArgsAe('a'));

    LResult := LVHost.Route('a', 'nada', nil);
    AssertEquals('ciclo de AE termina, sem rota', 0, Length(LResult));
  finally
    LVHost.Free;
  end;
end;

// AE apontando para exchange inexistente nao e' erro: declarar o AE antes de
// ele existir e' legitimo, e no publish a mensagem simplesmente nao tem rota.
procedure TVHostTests.Ae_Inexistente_SemRota;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ex', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, ArgsAe('nao-existe'));
    LResult := LVHost.Route('ex', 'nada', nil);
    AssertEquals('sem rota, e sem travar', 0, Length(LResult));
  finally
    LVHost.Free;
  end;
end;

// O AE pode ser de qualquer tipo -- inclusive headers, que casa pela tabela.
procedure TVHostTests.Ae_PodeSerDeOutroTipo;
var
  LVHost: TAMQPVHost;
  LArgs, LHeaders: TAMQPFieldTable;
  LResult: TArray<string>;
  LVal: TValue;
begin
  LVHost := TAMQPVHost.Create;
  try
    LVHost.DeclareExchange('ae', AMQP_EXCHANGE_TYPE_HEADERS, True, False,
      False, nil);
    LVHost.DeclareExchange('ex', AMQP_EXCHANGE_TYPE_DIRECT, True, False,
      False, ArgsAe('ae'));
    LVHost.DeclareQueue('q.ae', True, False, False, nil);
    LArgs := TAMQPFieldTable.Create;
    LVal := TValue.From<string>('sul');
    LArgs.Put('regiao', LVal);
    LVHost.BindQueue('ae', 'q.ae', '', LArgs); // o vhost toma posse

    LHeaders := TAMQPFieldTable.Create;
    try
      LVal := TValue.From<string>('sul');
      LHeaders.Put('regiao', LVal);
      LResult := LVHost.Route('ex', 'nada-casa', LHeaders);
      AssertEquals('o AE headers casou', 1, Length(LResult));
      AssertEquals('q.ae', LResult[0]);
    finally
      LHeaders.Free;
    end;
  finally
    LVHost.Free;
  end;
end;


// O conjunto de visitados NAO existe so' para cortar ciclo -- o teto de
// profundidade sozinho ja' faria o ciclo terminar. Ele existe para o grafo
// DENSO nao explodir: com N caminhos distintos por nivel e profundidade D, uma
// travessia sem memoria de visitados custa N^D. Medido: sem o visitados este
// teste leva segundos; com ele, milissegundos.
//
// E' a mesma familia do MuitosHash_NaoExplode do matcher de topic: o match
// roda na thread do PUBLICADOR e com o RWLock de leitura na mao, entao um
// declare hostil viraria travamento do broker inteiro.
procedure TVHostTests.Route_GrafoDenso_NaoExplode;
var
  LVHost: TAMQPVHost;
  LResult: TArray<string>;
  I: Integer;
  LIni: UInt64;
begin
  LVHost := TAMQPVHost.Create;
  try
    for I := 0 to 15 do
      LVHost.DeclareExchange('e' + IntToStr(I), AMQP_EXCHANGE_TYPE_FANOUT,
        True, False, False, nil);
    // Tres caminhos distintos de cada nivel para o seguinte: 3^15 travessias
    // se nao houver memoria de por onde ja' se passou.
    for I := 0 to 14 do
    begin
      LVHost.BindExchange('e' + IntToStr(I + 1), 'e' + IntToStr(I), 'a', nil);
      LVHost.BindExchange('e' + IntToStr(I + 1), 'e' + IntToStr(I), 'b', nil);
      LVHost.BindExchange('e' + IntToStr(I + 1), 'e' + IntToStr(I), 'c', nil);
    end;
    LVHost.DeclareQueue('q.fim', True, False, False, nil);
    LVHost.BindQueue('e15', 'q.fim', '', nil);

    LIni := AmqpTickMs;
    LResult := LVHost.Route('e0', 'qualquer', nil);
    AssertEquals('chegou ao fim', 1, Length(LResult));
    AssertTrue('travessia tem de ser linear, nao exponencial (levou '
      + IntToStr(AmqpTickMs - LIni) + ' ms)', (AmqpTickMs - LIni) < 500);
  finally
    LVHost.Free;
  end;
end;

initialization
  RegisterTest(TTopicMatchTests);
  RegisterTest(THeadersMatchTests);
  RegisterTest(TVHostTests);

end.
