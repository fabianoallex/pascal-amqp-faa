unit AMQP.Server.Header;

{$I amqp.inc}

{ Reescrita do content-header -- sub-modulo broker, WS3 da Fase 3 (ciclo de
  vida da mensagem, ver CLAUDE.md).

  POR QUE ISTO EXISTE, E POR QUE NAO FERE A D1
  --------------------------------------------
  A decisao D1 (Fase 2) fez a TAMQPMessage carregar o payload CRU do
  content-header em vez de um clone da TAMQPFieldTable: reemissao fiel byte a
  byte ao consumidor, e nada do problema de TValue-em-TValue do FPC 3.2 ao
  clonar tabelas/arrays aninhados. O dead-lettering (WS5) precisa do oposto --
  mexer no header, para acrescentar ou estender o x-death.

  O ponto que desfaz o no: a D1 proibe clonar no caminho QUENTE (todo publish,
  toda entrega), nao no frio. Uma mensagem MORRE UMA VEZ. E a TAMQPMessage ja
  e' imutavel por invariante da Fase 2 ("ninguem muta a mensagem depois de
  criada"), entao a reescrita nao muta nada: DERIVA uma mensagem nova, com
  HeaderPayload reconstruido e O MESMO Body por referencia. O caro (o corpo)
  nunca e' copiado; o barato (o header) e' refeito so' quando muda de fato.

  FIDELIDADE, MEDIDA E NAO PRESUMIDA
  ----------------------------------
  O round-trip DecodeContentHeader -> BuildContentHeader devolve os MESMOS
  bytes: conferido antes de este codigo existir e coberto pelos testes desta
  frente sobre as CATORZE propriedades da classe Basic, nao sobre uma amostra.

  LIMITACAO CONHECIDA, deliberada: DecodeContentHeader consome e DESCARTA as
  palavras de continuacao do property-flags (o bit 0 de cada palavra encadeia a
  proxima). Nenhuma propriedade da classe Basic vive alem da primeira palavra,
  entao na pratica isto e' teorico -- mas se um peer mandar continuacao, a
  reescrita a apaga em silencio. Uma mensagem que NUNCA passa por aqui segue
  intacta, porque a entrega normal reemite o payload cru sem tocar nele.

  SEM CALLBACK, DE PROPOSITO
  --------------------------
  O jeito obvio seria "reescreva este payload aplicando esta funcao a' tabela",
  mas o FPC 3.2 nao tem metodo anonimo e um `procedure of object` obrigaria
  quem chama a criar uma classe so' para carregar o estado da mutacao (ver a
  tabela de proibidos do CLAUDE.md). Em vez disso, um EDITOR explicito:
  construir (decodifica) -> mexer na tabela viva -> BuildPayload (re-encoda). }

interface

uses
  SysUtils,
  DateUtils,
  Rtti,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Server.Message;

const
  /// Razoes de dead-letter que este broker produz (spec de fato do RabbitMQ).
  /// 'delivery_limit' fica de fora: e' de quorum queue, que nao existe aqui.
  AMQP_DEATH_REJECTED = 'rejected'; // Basic.Nack/Reject com requeue=false
  AMQP_DEATH_EXPIRED  = 'expired';  // TTL (da fila ou da mensagem)
  AMQP_DEATH_MAXLEN   = 'maxlen';   // estourou o teto de mensagens

type
  { Editor de um content-header ja' serializado.

    Ciclo de uso:
      LEd := TAMQPHeaderEditor.Create(AMsg.HeaderPayload);
      try
        LEd.Headers.Put('x-death', ...);      // tabela VIVA, mexa a' vontade
        LNovo := LEd.BuildPayload(AMsg.BodySize);
      finally
        LEd.Free;
      end;

    Posse: o editor e' dono da tabela de headers decodificada e a libera no
    destrutor. O TBytes devolvido por BuildPayload e' do chamador. O payload de
    entrada NAO e' modificado -- e' so' lido. }
  TAMQPHeaderEditor = class
  private
    FProps: TAMQPBasicProperties;
    FBodySize: UInt64;
    FTinhaHeaders: Boolean;
  public
    /// Decodifica APayload (o content-header cru, como veio no frame).
    /// Levanta EAMQPWire se o payload for malformado.
    constructor Create(const APayload: TBytes);
    destructor Destroy; override;

    /// Tabela de headers VIVA, para ler e mexer. Criada vazia sob demanda se a
    /// mensagem nao tinha a propriedade -- pedir a tabela ja e' declarar
    /// intencao de mexer nela, e sem isso nao haveria como acrescentar x-death
    /// numa mensagem publicada sem headers. Continua sendo do EDITOR: nao
    /// libere, nao guarde depois do destrutor.
    function Headers: TAMQPFieldTable;
    /// True se a mensagem ORIGINAL trazia a propriedade headers -- util para
    /// quem quiser evitar acrescentar uma tabela vazia sem necessidade.
    function TinhaHeaders: Boolean;

    /// Re-encoda o header com as mudancas. ABodySize e' EXPLICITO de proposito:
    /// derivar uma mensagem com corpo diferente e esquecer de ajustar o
    /// body-size faria o consumidor esperar body frames que nunca chegam. Quem
    /// so' compartilha o corpo deve usar AmqpDeriveMessage abaixo, que
    /// preenche isto sozinho e nao tem como errar.
    function BuildPayload(ABodySize: UInt64): TBytes;

    /// Remove a propriedade 'expiration' e devolve o valor que ela tinha
    /// ('' se nao havia). Existe para o dead-lettering: uma mensagem que
    /// morreu POR TTL nao pode levar o mesmo prazo para a DLQ, senao morre de
    /// novo no ato e o padrao retry-com-espera nao funciona. O valor antigo
    /// vira 'original-expiration' na entrada de x-death.
    function TakeExpiration: string;

    /// Body-size lido do payload original.
    property BodySizeOriginal: UInt64 read FBodySize;
    /// Propriedades decodificadas (leitura). A tabela de headers dentro delas
    /// e' a mesma que Headers devolve.
    property Properties: TAMQPBasicProperties read FProps;
  end;

/// Soma dos campos 'count' de TODAS as entradas de x-death -- ou seja,
/// quantos saltos de dead-letter a mensagem ja' deu. E' o contador da guarda
/// de ciclo (decisao D14): o grafo de DLX pode ter ciclo, e o teto por
/// PROFUNDIDADE e' mais barato que o conjunto de filas visitadas do RabbitMQ,
/// com o mesmo efeito pratico numa engine em memoria.
/// Tabela nil, ausencia de x-death ou x-death malformado devolvem 0.
function AmqpDeathHops(AHeaders: TAMQPFieldTable): Int64;

/// Acrescenta uma morte ao x-death da tabela VIVA do editor.
///
/// COLAPSA por (fila, razao): se ja' existe entrada com a mesma dupla, apenas
/// incrementa o 'count' e atualiza o 'time'. Sem isso um laco de retry infla o
/// header a cada volta ate' estourar o frame-max -- e' para isso que a spec
/// tem o campo count. Entrada nova vai para a FRENTE do array (mais recente
/// primeiro), como o RabbitMQ faz.
///
/// AOriginalExpiration: quando a mensagem trazia 'expiration', o valor entra
/// na entrada e a PROPRIEDADE E' REMOVIDA do header (o chamador ja' a tirou
/// com TakeExpiration). Sem isso a mensagem morreria de novo no ato ao chegar
/// na DLQ, e o padrao retry-com-espera nao funcionaria. Passe '' se nao havia.
procedure AmqpAddDeath(AEditor: TAMQPHeaderEditor;
  const AQueue, AReason, AExchange, ARoutingKey, AOriginalExpiration: string);

/// Deriva uma mensagem NOVA a partir de AOrigem, trocando so' o header.
///
/// O Body e' COMPARTILHADO por referencia (semantica normal de dynamic array):
/// nada e' copiado, e como a TAMQPMessage e' imutavel, compartilhar e' seguro.
/// O body-size do header novo vem do corpo real, entao nao ha como divergir.
///
/// A mensagem devolvida nasce com RefCount=1 e e' do chamador (Release, nunca
/// Free). AOrigem NAO e' tocada nem liberada.
///
/// AExchange/ARoutingKey trocam o destino -- e' o que o dead-lettering precisa
/// (a mensagem morta e' republicada no DLX com outra routing key).
function AmqpDeriveMessage(AOrigem: TAMQPMessage;
  const ANovoHeaderPayload: TBytes;
  const AExchange, ARoutingKey: string): TAMQPMessage;

implementation

{ TAMQPHeaderEditor }

constructor TAMQPHeaderEditor.Create(const APayload: TBytes);
var
  LReader: TAMQPReader;
  LHeader: TAMQPContentHeader;
begin
  inherited Create;
  LReader := TAMQPReader.Create(APayload);
  try
    LHeader := DecodeContentHeader(LReader);
  finally
    LReader.Free;
  end;
  FProps := LHeader.Properties;
  FBodySize := LHeader.BodySize;
  FTinhaHeaders := FProps.Has(bpHeaders) and (FProps.Headers <> nil);
end;

destructor TAMQPHeaderEditor.Destroy;
begin
  // O decode entrega a tabela para quem chamou; aqui o dono e' o editor.
  if FProps.Has(bpHeaders) then
    FProps.Headers.Free;
  inherited;
end;

function TAMQPHeaderEditor.Headers: TAMQPFieldTable;
begin
  if not (FProps.Has(bpHeaders) and (FProps.Headers <> nil)) then
    // SetHeaders preenche o valor e marca o flag juntos.
    FProps.SetHeaders(TAMQPFieldTable.Create);
  Result := FProps.Headers;
end;

function TAMQPHeaderEditor.TinhaHeaders: Boolean;
begin
  Result := FTinhaHeaders;
end;

function TAMQPHeaderEditor.BuildPayload(ABodySize: UInt64): TBytes;
begin
  Result := BuildContentHeader(ABodySize, FProps);
end;

function TAMQPHeaderEditor.TakeExpiration: string;
begin
  Result := '';
  if not FProps.Has(bpExpiration) then
    Exit;
  Result := FProps.Expiration;
  Exclude(FProps.Flags, bpExpiration);
  FProps.Expiration := '';
end;

{ --- x-death --------------------------------------------------------------

  Forma do header, tal como o RabbitMQ define e os clientes esperam:

    x-death = array de TABELAS, cada uma com os campos
       count               long
       reason              longstr   (rejected | expired | maxlen)
       queue               longstr
       time                timestamp
       exchange            longstr
       routing-keys        array de longstr
       original-expiration longstr, so' quando havia 'expiration'

  ATENCAO ao escrever esta prosa: chave de fechar dentro de comentario de bloco
  FECHA O COMENTARIO -- por isso os campos vao em lista, e nao na notacao de
  chaves. E' o mesmo gotcha que o CLAUDE.md ja registra para diretivas citadas
  em comentario, e mordeu aqui na primeira escrita.

  Array de TABELAS -- exatamente a forma que o codec so' passou a saber
  ESCREVER na WS1 desta fase. Ate' la' isto aqui levantaria EAMQPWire. }

// Le o x-death como array, ja' desembrulhado. nil se ausente/malformado.
// AmqpUnwrapValue e' obrigatorio em TODO GetArrayElement (gotcha do FPC 3.2:
// elemento de TArray<TValue> volta re-embrulhado).
function LeDeaths(AHeaders: TAMQPFieldTable; out AArr: TValue): Boolean;
begin
  AArr := TValue.Empty;
  Result := (AHeaders <> nil) and AHeaders.TryGetValue('x-death', AArr);
  if not Result then
    Exit;
  AArr := AmqpUnwrapValue(AArr);
  Result := AArr.IsArray;
end;

function EntradaDe(const AArr: TValue; AIdx: Integer): TAMQPFieldTable;
var
  LElem: TValue;
begin
  Result := nil;
  LElem := AmqpUnwrapValue(AArr.GetArrayElement(AIdx));
  if LElem.IsObject and (LElem.AsObject is TAMQPFieldTable) then
    Result := TAMQPFieldTable(LElem.AsObject);
end;

function CampoStr(ATab: TAMQPFieldTable; const AKey: string): string;
var
  LVal: TValue;
begin
  Result := '';
  if (ATab <> nil) and ATab.TryGetValue(AKey, LVal) then
  begin
    LVal := AmqpUnwrapValue(LVal);
    if not LVal.IsObject then
      Result := LVal.AsString;
  end;
end;

function CampoInt(ATab: TAMQPFieldTable; const AKey: string): Int64;
var
  LVal: TValue;
begin
  Result := 0;
  if (ATab <> nil) and ATab.TryGetValue(AKey, LVal) then
  begin
    LVal := AmqpUnwrapValue(LVal);
    case LVal.Kind of
      tkInteger: Result := LVal.AsInteger;
      tkInt64:   Result := LVal.AsInt64;
    end;
  end;
end;

function AmqpDeathHops(AHeaders: TAMQPFieldTable): Int64;
var
  LArr: TValue;
  I: Integer;
begin
  Result := 0;
  if not LeDeaths(AHeaders, LArr) then
    Exit;
  for I := 0 to LArr.GetArrayLength - 1 do
    Inc(Result, CampoInt(EntradaDe(LArr, I), 'count'));
end;

procedure AmqpAddDeath(AEditor: TAMQPHeaderEditor;
  const AQueue, AReason, AExchange, ARoutingKey, AOriginalExpiration: string);
var
  LHeaders, LEntrada, LNova: TAMQPFieldTable;
  LArr, LVal: TValue;
  LNovoArr, LChaves: TAMQPValueArray;
  I, N: Integer;
  LAgora: Int64;
begin
  LHeaders := AEditor.Headers; // cria sob demanda se a mensagem nao tinha
  LAgora := DateTimeToUnix(Now, False); // False = a entrada e' hora LOCAL
  LEntrada := nil;

  // Colapso por (fila, razao).
  if LeDeaths(LHeaders, LArr) then
    for I := 0 to LArr.GetArrayLength - 1 do
    begin
      LNova := EntradaDe(LArr, I);
      if (LNova <> nil) and (CampoStr(LNova, 'queue') = AQueue)
        and (CampoStr(LNova, 'reason') = AReason) then
      begin
        LEntrada := LNova;
        Break;
      end;
    end;

  if LEntrada <> nil then
  begin
    LVal := TValue.From<Int64>(CampoInt(LEntrada, 'count') + 1);
    LEntrada.Put('count', LVal);
    LVal := TValue.From<Int64>(LAgora);
    LEntrada.Put('time', LVal);
    Exit; // o array nao muda: a entrada ja' esta' nele
  end;

  // Entrada nova.
  LEntrada := TAMQPFieldTable.Create;
  LVal := TValue.From<Int64>(1);
  LEntrada.Put('count', LVal);
  LVal := TValue.From<string>(AReason);
  LEntrada.Put('reason', LVal);
  LVal := TValue.From<string>(AQueue);
  LEntrada.Put('queue', LVal);
  LVal := TValue.From<Int64>(LAgora);
  LEntrada.Put('time', LVal);
  LVal := TValue.From<string>(AExchange);
  LEntrada.Put('exchange', LVal);
  SetLength(LChaves, 1);
  LChaves[0] := TValue.From<string>(ARoutingKey);
  LVal := TValue.From<TAMQPValueArray>(LChaves);
  LEntrada.Put('routing-keys', LVal);
  if AOriginalExpiration <> '' then
  begin
    LVal := TValue.From<string>(AOriginalExpiration);
    LEntrada.Put('original-expiration', LVal);
  end;

  // Mais recente na FRENTE (ordem do RabbitMQ).
  N := 0;
  if LArr.IsArray then
    N := LArr.GetArrayLength;
  SetLength(LNovoArr, N + 1);
  LNovoArr[0] := TValue.From<TObject>(LEntrada);
  for I := 0 to N - 1 do
    LNovoArr[I + 1] := TValue.From<TObject>(EntradaDe(LArr, I));
  LVal := TValue.From<TAMQPValueArray>(LNovoArr);
  LHeaders.Put('x-death', LVal);

  // x-first-death-*: so' na PRIMEIRA morte, nunca sobrescritos depois.
  if not LHeaders.ContainsKey('x-first-death-queue') then
  begin
    LVal := TValue.From<string>(AQueue);
    LHeaders.Put('x-first-death-queue', LVal);
    LVal := TValue.From<string>(AReason);
    LHeaders.Put('x-first-death-reason', LVal);
    LVal := TValue.From<string>(AExchange);
    LHeaders.Put('x-first-death-exchange', LVal);
  end;
end;

{ AmqpDeriveMessage }

function AmqpDeriveMessage(AOrigem: TAMQPMessage;
  const ANovoHeaderPayload: TBytes;
  const AExchange, ARoutingKey: string): TAMQPMessage;
begin
  if AOrigem = nil then
    raise EAMQPWire.Create('AmqpDeriveMessage: mensagem de origem nil');
  // UserId vem da origem: quem publicou continua sendo quem publicou, mesmo
  // depois de a mensagem ser republicada num dead-letter exchange.
  Result := TAMQPMessage.Create(AExchange, ARoutingKey, AOrigem.UserId,
    ANovoHeaderPayload, AOrigem.Body);
end;

end.
