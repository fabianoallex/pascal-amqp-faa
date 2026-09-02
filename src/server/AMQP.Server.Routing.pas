unit AMQP.Server.Routing;

{$I amqp.inc}

{ Matchers puros de roteamento -- sub-modulo broker, WS2 da Fase 2 (engine de
  roteamento em memoria, ver CLAUDE.md).

  Nenhuma funcao aqui toca em topologia (isso e' AMQP.Server.VHost, que usa
  estas): so' decide, dados um binding e uma mensagem, se casam. Os vetores de
  aceitacao que guiam esta unit foram escritos ANTES do codigo e sao
  normativos -- ver o arquivo de vetores citado no plano do WS2. Onde este
  codigo divergir dos vetores, os vetores vencem.

  Requisito de desempenho: o casamento roda no caminho quente do publish, na
  thread do PUBLICADOR (ver "Invariante nova" do CLAUDE.md) -- por isso
  AmqpTopicMatches caminha por INDICES sobre as strings (sem Copy, sem montar
  lista de palavras, sem regex): cada "palavra" e' so' um par (inicio,
  tamanho) dentro da string original. }

interface

uses
  SysUtils,
  Rtti,
  AMQP.Wire,
  AMQP.Server.Resources; // AmqpValuesEqual (comparador escalar exportado do WS1)

type
  { Modo de casamento de um binding de headers exchange (spec: propriedade
    'x-match' dos argumentos do bind). Default (ausente) e' amqhmAll. }
  TAMQPHeadersMatch = (amqhmAll, amqhmAny);

/// True se ARoutingKey casa com ABindingKey pelas regras do exchange topic
/// (spec 3.1.3.3): '*' casa EXATAMENTE uma palavra (inclusive a palavra
/// vazia); '#' casa ZERO ou mais palavras, em qualquer posicao, quantas vezes
/// aparecer; qualquer outra palavra precisa bater literalmente, byte a byte
/// (case-sensitive). Palavras sao separadas por '.'; uma chave vazia ("") tem
/// ZERO palavras, nao uma palavra vazia -- e' a definicao travada no vetor de
/// aceitacao (regra especifica deste projeto onde a spec e' omissa).
///
/// Implementacao: backtracking recursivo sobre INDICES nas duas strings (ver
/// TryGetWord na implementation) -- nenhuma alocacao por chamada (sem Copy,
/// sem TStringList, sem regex). O teste de 10k casamentos na suite cobre isso.
function AmqpTopicMatches(const ABindingKey, ARoutingKey: string): Boolean;

/// True se ARoutingKey e' EXATAMENTE igual a ABindingKey (exchange direct,
/// spec 3.1.3.1) -- byte a byte, case-sensitive. Existe como funcao propria
/// (em vez de so' usar "=" direto no chamador) para o dispatch por tipo de
/// exchange em AMQP.Server.VHost.Route ficar uniforme entre os quatro tipos.
function AmqpDirectMatches(const ABindingKey, ARoutingKey: string): Boolean;

/// Le a propriedade 'x-match' de AArgs (argumentos de um Queue.Bind ou
/// Exchange.Bind num headers exchange) e devolve o modo em AMode.
/// - AArgs = nil OU sem a chave 'x-match': AMode = amqhmAll (default da spec).
/// - 'x-match' = 'all' ou 'any': AMode preenchido, devolve True.
/// - qualquer outro valor: devolve False -- o BIND e' invalido (a WS5 mapeia
///   isso para 406 PRECONDITION_FAILED no proprio Exchange.Bind/Queue.Bind,
///   NAO faz fallback silencioso para 'all').
function AmqpParseHeadersMatch(AArgs: TAMQPFieldTable;
  out AMode: TAMQPHeadersMatch): Boolean;

/// True se AMessageHeaders casa com os criterios de ABindingArgs (headers
/// exchange, spec 3.1.3.4), no modo AMode:
/// - amqhmAll: TODO criterio (chave que NAO comeca com 'x-') tem de existir
///   em AMessageHeaders com valor IGUAL (AmqpValuesEqual: tipo e valor).
///   Zero criterios => True (verdade vacua -- "todos de um conjunto vazio").
/// - amqhmAny: BASTA um criterio bater. Zero criterios => False (falso vacuo
///   -- "existe algum de um conjunto vazio" e' sempre falso).
/// Chaves do binding que comecam com 'x-' (inclusive 'x-match', que e'
/// justamente o modo, nao um criterio) sao ignoradas na comparacao.
/// ABindingArgs = nil equivale a "nenhum criterio". AMessageHeaders = nil
/// equivale a "mensagem sem headers" (tabela vazia).
function AmqpHeadersMatch(ABindingArgs, AMessageHeaders: TAMQPFieldTable;
  AMode: TAMQPHeadersMatch): Boolean;

implementation

{ ---- Matcher topic: caminhamento por indices, sem alocar ---- }

// Extrai a PROXIMA palavra da string S a partir do cursor APos (1-based),
// avancando APos ate o inicio da palavra seguinte (ou para um estado
// "esgotado" depois da ultima). Convencao do cursor, ver o comentario de
// AmqpTopicMatches: uma string de tamanho ALen=0 tem ZERO palavras (regra
// travada no vetor de aceitacao) -- TryGetWord devolve False de cara nesse
// caso, mesmo na primeira chamada. Para ALen>0, toda palavra -- inclusive a
// vazia apos um '.' final ou entre dois '.' consecutivos -- e' emitida.
//
// Nao aloca nada: so' anda o indice AStart/AWordLen sobre a string original.
function TryGetWord(const S: string; var APos: Integer; ALen: Integer;
  out AStart, AWordLen: Integer): Boolean;
begin
  if (ALen = 0) or (APos > ALen + 1) then
    Exit(False); // string original vazia (zero palavras) OU cursor esgotado

  AStart := APos;
  AWordLen := 0;
  while (APos <= ALen) and (S[APos] <> '.') do
  begin
    Inc(APos);
    Inc(AWordLen);
  end;

  if APos <= ALen then
    Inc(APos) // pula o '.' -- proxima chamada comeca na palavra seguinte
  else
    Inc(APos); // ALen+1 -> ALen+2: sentinela de "acabou" pra proxima chamada

  Result := True;
end;

// Compara duas faixas de caracteres (uma "palavra" de cada string) byte a
// byte, sem materializar substring nenhuma.
function WordsEqual(const S1: string; AStart1, ALen1: Integer;
  const S2: string; AStart2, ALen2: Integer): Boolean;
var
  I: Integer;
begin
  if ALen1 <> ALen2 then
    Exit(False);
  for I := 0 to ALen1 - 1 do
    if S1[AStart1 + I] <> S2[AStart2 + I] then
      Exit(False);
  Result := True;
end;

// Nucleo do casamento de topic. Iterativo de proposito, com backtracking
// APENAS para o ultimo '#' visto -- o algoritmo classico de casamento de
// curinga (o mesmo de glob, onde '#' faz o papel de '*' e '*' o de '?').
//
// POR QUE NAO RECURSAO: a versao recursiva "tenta 0, 1, 2... palavras para
// cada '#'" e' exponencial no numero de '#'. Medido nesta maquina, com um
// routing key de 24 palavras: 5 '#' = 0 ms, 6 = 15 ms, 7 = 110 ms, 8 = 453 ms.
// Uma binding key cabe 255 octetos, ou seja, mais de 100 '#' -- um cliente
// podia declarar 'hash.hash....zz' e fazer CADA publish naquele exchange
// custar segundos, na thread do publicador e com o RWLock de leitura na mao.
// Negacao de servico com uma linha de declare. Aqui o custo e' O(n*m) no pior
// caso e O(n) no caso comum, sem recursao e sem alocar nada.
//
// A chave do algoritmo: basta lembrar o ULTIMO '#'. Se o resto do padrao
// falhar mais adiante, a unica coisa util a tentar e' deixar esse '#' engolir
// mais uma palavra -- retomar de um '#' anterior nunca abre possibilidade que
// esta retomada ja nao cubra.
function AmqpTopicMatches(const ABindingKey, ARoutingKey: string): Boolean;
var
  LBLen, LRLen: Integer;
  LBPos, LRPos: Integer;           // cursores correntes (binding e routing)
  LStarBPos, LStarRPos: Integer;   // retomada do ultimo '#'; -1 = nenhum ainda
  LNextB, LNextR: Integer;         // cursores de tentativa (TryGetWord avanca)
  LBStart, LBWordLen: Integer;
  LRStart, LRWordLen: Integer;
  LTemB: Boolean;
begin
  LBLen := Length(ABindingKey);
  LRLen := Length(ARoutingKey);
  LBPos := 1;
  LRPos := 1;
  LStarBPos := -1;
  LStarRPos := -1;

  // Enquanto houver palavra de routing para casar.
  while True do
  begin
    LNextR := LRPos;
    if not TryGetWord(ARoutingKey, LNextR, LRLen, LRStart, LRWordLen) then
      Break; // routing esgotado -- decide no rabo, depois do laco

    LNextB := LBPos;
    LTemB := TryGetWord(ABindingKey, LNextB, LBLen, LBStart, LBWordLen);

    // '#': por ora consome ZERO palavras; guarda de onde retomar se falhar.
    if LTemB and (LBWordLen = 1) and (ABindingKey[LBStart] = '#') then
    begin
      LStarBPos := LNextB;  // binding logo depois do '#'
      LStarRPos := LRPos;   // routing onde o '#' comecou
      LBPos := LNextB;
      Continue;
    end;

    // '*' casa exatamente uma palavra (inclusive vazia); literal casa byte a
    // byte. Nos dois casos, consome uma palavra de cada lado.
    if LTemB and (((LBWordLen = 1) and (ABindingKey[LBStart] = '*')) or
       WordsEqual(ABindingKey, LBStart, LBWordLen,
                  ARoutingKey, LRStart, LRWordLen)) then
    begin
      LBPos := LNextB;
      LRPos := LNextR;
      Continue;
    end;

    // Nao casou aqui. Se ha um '#' pendente, deixa ele engolir mais uma
    // palavra e retoma dali; senao, acabou.
    if LStarBPos < 0 then
      Exit(False);

    LNextR := LStarRPos;
    if not TryGetWord(ARoutingKey, LNextR, LRLen, LRStart, LRWordLen) then
      Exit(False); // nao ha mais palavra para o '#' engolir
    LStarRPos := LNextR;
    LRPos := LNextR;
    LBPos := LStarBPos;
  end;

  // Routing esgotado: o que sobrou do binding so' pode ser '#' (que casa zero
  // palavras). Qualquer literal ou '*' pendente significa palavra faltando.
  while True do
  begin
    LNextB := LBPos;
    if not TryGetWord(ABindingKey, LNextB, LBLen, LBStart, LBWordLen) then
      Exit(True);
    if (LBWordLen <> 1) or (ABindingKey[LBStart] <> '#') then
      Exit(False);
    LBPos := LNextB;
  end;
end;

{ ---- Matcher direct ---- }

function AmqpDirectMatches(const ABindingKey, ARoutingKey: string): Boolean;
begin
  Result := ABindingKey = ARoutingKey;
end;

{ ---- Matcher headers ---- }

function AmqpIsXKey(const AKey: string): Boolean;
begin
  Result := (Length(AKey) >= 2) and (AKey[1] = 'x') and (AKey[2] = '-');
end;

function AmqpParseHeadersMatch(AArgs: TAMQPFieldTable;
  out AMode: TAMQPHeadersMatch): Boolean;
var
  LValue: TValue;
  LMatch: string;
begin
  AMode := amqhmAll; // default da spec quando 'x-match' esta ausente
  if (AArgs = nil) or (not AArgs.TryGetValue('x-match', LValue)) then
    Exit(True);

  LMatch := LValue.AsString;
  if LMatch = 'all' then
  begin
    AMode := amqhmAll;
    Exit(True);
  end;
  if LMatch = 'any' then
  begin
    AMode := amqhmAny;
    Exit(True);
  end;

  Result := False; // valor de 'x-match' desconhecido -- bind invalido (406)
end;

function AmqpHeadersMatch(ABindingArgs, AMessageHeaders: TAMQPFieldTable;
  AMode: TAMQPHeadersMatch): Boolean;
var
  LKey: string;
  LBindVal, LMsgVal: TValue;
  LMatched: Boolean;
begin
  if ABindingArgs <> nil then
    for LKey in ABindingArgs.Keys do
    begin
      if AmqpIsXKey(LKey) then
        Continue; // 'x-match' e qualquer outra chave 'x-' nao sao criterio

      LBindVal := ABindingArgs[LKey]; // indexador -> local antes do uso (gotcha FPC)
      LMatched := (AMessageHeaders <> nil) and
        AMessageHeaders.TryGetValue(LKey, LMsgVal) and
        AmqpValuesEqual(LBindVal, LMsgVal);

      case AMode of
        amqhmAll:
          if not LMatched then
            Exit(False); // um criterio falhou -> "todos" ja nao bate mais
        amqhmAny:
          if LMatched then
            Exit(True); // um criterio bateu -> "algum" ja e' suficiente
      end;
    end;

  // Terminou o loop sem decidir por short-circuit:
  // - amqhmAll: nenhum criterio falhou (inclusive o caso de zero criterios --
  //   verdade vacua, vetor #8);
  // - amqhmAny: nenhum criterio bateu (inclusive zero criterios -- falso
  //   vacuo, vetor #9).
  case AMode of
    amqhmAll: Result := True;
    amqhmAny: Result := False;
  else
    Result := False;
  end;
end;

end.
