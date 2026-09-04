unit AMQP.Server.Resources;

{$I amqp.inc}

{ Descritores dos recursos declarados (exchange/fila) -- sub-modulo broker,
  WS1 da Fase 2.

  So' a estrutura de dados e as funcoes de comparacao/validacao entram aqui;
  quem monta o registro de exchanges/filas por vhost em cima disso e' a WS2
  (topologia). O uso mais direto de AmqpArgsEqual/EquivalentTo e' a base do
  redeclare divergente (406 PRECONDITION_FAILED, spec): declarar de novo um
  recurso existente com flags/argumentos diferentes e' erro; com os MESMOS
  flags/argumentos e' idempotente.

  Posse de Arguments: os descritores TOMAM POSSE da TAMQPFieldTable vinda do
  decode de Exchange.Declare/Queue.Declare e a liberam no destrutor -- mesmo
  espirito da decisao D1 (a mensagem carrega o payload cru em vez de clonar
  TValue-em-TValue): clonar a tabela teria o mesmo problema de tabelas/arrays
  aninhados que o AmqpUnwrapValue (AMQP.Wire) existe pra contornar, entao o
  descritor vira o dono direto em vez de copiar. }

interface

uses
  SysUtils,
  Rtti,
  TypInfo,
  AMQP.Wire,
  AMQP.Exchange.Methods;

const
  /// Teto de x-max-priority desta implementacao (decisao D12 da Fase 3). O
  /// RabbitMQ aceita ate' 255 e recomenda <= 5; acima de 9 o custo de memoria
  /// por fila (um balde de prontas por nivel) deixa de valer o que entrega.
  /// Declarar acima disto e' 406, nao um clamp silencioso -- desvio deliberado,
  /// documentado no CLAUDE.md.
  AMQP_MAX_PRIORITY_LEVEL = 9;

type
  { Politica de x-overflow: o que fazer quando a fila bate o teto.
    - amqovDropHead: descarta da cabeca (default da spec de fato do RabbitMQ);
    - amqovRejectPublish: recusa o publish, que em confirm mode vira Basic.Nack.
    'reject-publish-dlx' do RabbitMQ nao entra: dead-letter no overflow e' a
    razao 'maxlen' da WS5, que ja' cobre o caso pelo caminho normal. }
  TAMQPOverflow = (amqovDropHead, amqovRejectPublish);

  { Os x-arguments de uma fila, ja' decodificados e validados.

    Escopo da WS2 (Fase 3): esta unit apenas LE e VALIDA. Nenhum campo daqui
    tem efeito ainda -- TTL e prioridade sao a WS4, dead-letter a WS5, os tetos
    a WS6, x-expires a WS8. Guardar a politica no descritor desde ja' evita que
    cada frente reparseie a tabela do seu jeito.

    Ausencia e' -1 nos numericos (e nao 0, que e' um valor VALIDO e diferente:
    x-max-length=0 significa "fila que nao guarda nada"). Nas strings, o par
    Has* separa "ausente" de "presente e vazia" -- x-dead-letter-exchange=''
    e' o exchange DEFAULT, nao a ausencia de DLX. }
  TAMQPQueuePolicy = record
    MessageTtlMs: Int64;             // x-message-ttl        (-1 = ausente)
    ExpiresMs: Int64;                // x-expires            (-1 = ausente)
    MaxLength: Int64;                // x-max-length         (-1 = ausente)
    MaxLengthBytes: Int64;           // x-max-length-bytes   (-1 = ausente)
    MaxPriority: Integer;            // x-max-priority       (-1 = sem prioridade)
    Overflow: TAMQPOverflow;         // x-overflow           (default drop-head)
    DeadLetterExchange: string;      // x-dead-letter-exchange
    HasDeadLetterExchange: Boolean;
    DeadLetterRoutingKey: string;    // x-dead-letter-routing-key
    HasDeadLetterRoutingKey: Boolean;

    class function Empty: TAMQPQueuePolicy; static;
    /// True se a fila tem ao menos um argumento com efeito -- atalho para as
    /// frentes seguintes pularem o caminho caro numa fila comum.
    function HasAny: Boolean;
  end;

  { Descreve um exchange declarado. Dono de Arguments. }
  TAMQPExchangeDef = class
  private
    FName: string;
    FExchangeType: string;
    FDurable: Boolean;
    FAutoDelete: Boolean;
    FInternal: Boolean;
    FArguments: TAMQPFieldTable;
    FAlternateExchange: string;
    FHasAlternateExchange: Boolean;
  public
    /// AArguments: o descritor passa a ser dono (libera no destrutor); pode
    /// ser nil.
    constructor Create(const AName, AExchangeType: string;
      ADurable, AAutoDelete, AInternal: Boolean; AArguments: TAMQPFieldTable);
    destructor Destroy; override;

    /// True se AOther descreve um exchange EQUIVALENTE (mesmo tipo, mesmos
    /// flags, mesmos argumentos por AmqpArgsEqual) -- nao compara o Name, so'
    /// o resto: quem chama ja sabe que o nome bate (e' o redeclare do mesmo
    /// nome que esta' sendo validado).
    function EquivalentTo(AOther: TAMQPExchangeDef): Boolean;

    property Name: string read FName;
    property ExchangeType: string read FExchangeType;
    property Durable: Boolean read FDurable;
    property AutoDelete: Boolean read FAutoDelete;
    property Internal: Boolean read FInternal;
    property Arguments: TAMQPFieldTable read FArguments;
    /// 'alternate-exchange', derivado de Arguments na construcao. A WS7 da
    /// Fase 3 roteia para ca' o que nao casou binding nenhum; ate' la' o
    /// campo so' existe. Has* separa ausente de presente-e-vazio.
    property AlternateExchange: string read FAlternateExchange;
    property HasAlternateExchange: Boolean read FHasAlternateExchange;
  end;

  { Descreve uma fila declarada. Dono de Arguments. }
  TAMQPQueueDef = class
  private
    FName: string;
    FDurable: Boolean;
    FExclusive: Boolean;
    FAutoDelete: Boolean;
    FArguments: TAMQPFieldTable;
    FOwnerId: NativeUInt;
    FPolicy: TAMQPQueuePolicy;
  public
    /// AArguments: o descritor passa a ser dono (libera no destrutor); pode
    /// ser nil. AOwnerId: identidade da conexao dona (0 = fila nao-exclusiva,
    /// ou dono ainda nao atribuido) -- ver o comentario da property OwnerId.
    constructor Create(const AName: string;
      ADurable, AExclusive, AAutoDelete: Boolean; AArguments: TAMQPFieldTable;
      AOwnerId: NativeUInt = 0);
    destructor Destroy; override;

    /// True se AOther descreve uma fila EQUIVALENTE (mesmos flags, mesmos
    /// argumentos por AmqpArgsEqual). OwnerId NAO entra na comparacao: dono
    /// nao e' parte do que o redeclare valida (mesma semantica do RabbitMQ).
    function EquivalentTo(AOther: TAMQPQueueDef): Boolean;

    property Name: string read FName;
    property Durable: Boolean read FDurable;
    property Exclusive: Boolean read FExclusive;
    property AutoDelete: Boolean read FAutoDelete;
    property Arguments: TAMQPFieldTable read FArguments;
    /// Dono da fila exclusiva. Escolha de tipo: NativeUInt guardando a
    /// identidade da TAMQPServerConnection dona (ex.: NativeUInt(Connection)
    /// ou um id sequencial que ela exponha) -- nao uma referencia direta ao
    /// objeto, pra este unit nao precisar enxergar AMQP.Server.Connection (e
    /// evitar um ciclo de units). 0 = nenhum dono. A WS3 (fila-ator) e a WS7
    /// (ciclo de vida cruzado / teardown) usam isso pra negar acesso de outra
    /// conexao e pra liberar a fila quando a dona cai.
    property OwnerId: NativeUInt read FOwnerId write FOwnerId;
    /// Os x-arguments ja' decodificados (WS2 da Fase 3). Derivada de Arguments
    /// na construcao -- a tabela continua sendo a fonte da verdade (e' ela que
    /// o EquivalentTo compara), a politica e' so' a leitura tipada dela.
    /// Argumento invalido nao chega aqui: a FSM valida ANTES de declarar.
    property Policy: TAMQPQueuePolicy read FPolicy;
  end;

/// Compara duas field-tables SEMANTICAMENTE (nao por referencia):
/// - nil e tabela vazia sao consideradas iguais;
/// - precisam ter a mesma quantidade de chaves;
/// - cada chave de A precisa existir em B com o MESMO valor;
/// - tabela aninhada ('F') compara recursivamente por AmqpArgsEqual;
/// - array ('A' e afins) compara elemento a elemento, com AmqpUnwrapValue
///   (AMQP.Wire) em TODO GetArrayElement -- gotcha conhecido do FPC 3.2
///   (GetArrayElement sobre um array de field-values devolve o elemento
///   re-embrulhado; ver CLAUDE.md).
/// E' a base do 406 PRECONDITION_FAILED de um redeclare divergente.
function AmqpArgsEqual(A, B: TAMQPFieldTable): Boolean;

/// Prefixo reservado 'amq.' (spec 1.4: nomes de exchange/fila comecando com
/// isso sao reservados ao servidor). A WS5 usa para negar declare/bind de um
/// nome reservado vindo do cliente (403 ACCESS_REFUSED), exceto os exchanges
/// predefinidos que o proprio broker semeia.
function AmqpIsReservedName(const AName: string): Boolean;

/// True se AType e' um dos quatro tipos de exchange desta implementacao
/// (direct/fanout/topic/headers). Comparacao EXATA, case-sensitive: o AMQP
/// e' case-sensitive aqui (ao contrario, por exemplo, do SASL mechanism), e
/// 'Direct'/'DIRECT' nao contam.
function AmqpIsValidExchangeType(const AType: string): Boolean;

/// Compara dois field-VALUES (um par chave/valor ja' desembrulhado de uma
/// TAMQPFieldTable) semanticamente: mesmo Kind e' pre-requisito, tabela
/// aninhada ('F') recorre por AmqpArgsEqual, array ('A') compara elemento a
/// elemento com AmqpUnwrapValue (gotcha do FPC 3.2, ver AmqpArgsEqual acima),
/// escalares comparam por AsBoolean/AsInteger/.../AsString conforme o Kind
/// (TValue.AsVariant nao existe no FPC 3.2 -- nao ha atalho generico).
/// Exportada (nao so' uso interno de AmqpArgsEqual) para a WS2 (AMQP.Server.
/// Routing) reusar no casamento de headers exchange em vez de duplicar a
/// logica de comparacao de TValue.
function AmqpValuesEqual(const A, B: TValue): Boolean;

/// Le e VALIDA os x-arguments de fila de AArgs (pode ser nil = tabela vazia).
///
/// False = algum argumento e' invalido, e AErro traz a mensagem ja' no
/// formato que vai para o reply-text do 406 PRECONDITION_FAILED, NOMEANDO o
/// argumento ofensor (ex.: "invalid arg 'x-max-priority' for queue: must be
/// an integer between 0 and 9"). Nomear e' o que separa um erro acionavel de
/// um "precondition failed" que o usuario tem de adivinhar -- o RabbitMQ faz
/// igual.
///
/// Quem chama e' a FSM (AMQP.Server.Connection), ANTES de entregar a tabela
/// a' engine: validacao produz reply-code, e a engine nao conhece reply-code
/// nenhum (invariante da Fase 2). Chaves 'x-' desconhecidas sao IGNORADAS,
/// nao recusadas -- e' o que o RabbitMQ faz, e recusar quebraria cliente que
/// manda argumento de uma extensao que este broker nao tem.
function AmqpParseQueuePolicy(AArgs: TAMQPFieldTable;
  out APolicy: TAMQPQueuePolicy; out AErro: string): Boolean;

/// Le e valida o argumento 'alternate-exchange' de um Exchange.Declare.
/// ANome sai vazio e Result=True quando o argumento esta ausente (use
/// AHas para distinguir de "presente e vazio"). Mesmo contrato de AErro.
function AmqpParseAlternateExchange(AArgs: TAMQPFieldTable;
  out AName: string; out AHas: Boolean; out AErro: string): Boolean;

/// Le a propriedade 'expiration' do Basic: TTL da MENSAGEM em milissegundos,
/// que a spec define como STRING decimal e nao como numero (por isso o
/// TAMQPBasicProperties.Expiration e' string).
///
/// AValue vazio = ausente: devolve True com AMs = -1 (sem TTL proprio).
/// Malformada (nao-digito, negativa, estouro) devolve False -- e a FSM
/// transforma isso em 406, do mesmo jeito que faz com x-argument invalido.
/// Ignorar em silencio seria pior que recusar: a mensagem que o cliente
/// mandou expirar viveria para sempre, e ele nao teria como saber.
function AmqpParseExpirationMs(const AValue: string; out AMs: Int64): Boolean;

implementation

{ TAMQPQueuePolicy }

class function TAMQPQueuePolicy.Empty: TAMQPQueuePolicy;
begin
  Result.MessageTtlMs := -1;
  Result.ExpiresMs := -1;
  Result.MaxLength := -1;
  Result.MaxLengthBytes := -1;
  Result.MaxPriority := -1;
  Result.Overflow := amqovDropHead;
  Result.DeadLetterExchange := '';
  Result.HasDeadLetterExchange := False;
  Result.DeadLetterRoutingKey := '';
  Result.HasDeadLetterRoutingKey := False;
end;

function TAMQPQueuePolicy.HasAny: Boolean;
begin
  Result := (MessageTtlMs >= 0) or (ExpiresMs >= 0) or (MaxLength >= 0)
    or (MaxLengthBytes >= 0) or (MaxPriority >= 0)
    or (Overflow <> amqovDropHead)
    or HasDeadLetterExchange or HasDeadLetterRoutingKey;
end;

{ --- leitura tipada de um x-argument --------------------------------------
  O decoder do wire nao produz um tipo unico para numero: FV_INT8/16/32 viram
  tkInteger e FV_UINT32/INT64/TIMESTAMP viram tkInt64 (ver TAMQPReader.
  ReadFieldValue). Um cliente que manda x-message-ttl como 'b' e outro que
  manda como 'l' estao os dois certos, entao aceitar so' um Kind recusaria
  cliente valido. Boolean fica de fora de proposito: no FPC ele tem Kind
  proprio (tkBool) e no Delphi cai em tkEnumeration -- em nenhum dos dois e'
  um numero, e aceitar True como 1 mascararia erro de tipo do cliente. }

// True se a chave existe. AOk sai False quando existe mas nao e' numero.
function TryArgInt64(AArgs: TAMQPFieldTable; const AKey: string;
  out AValue: Int64; out AOk: Boolean): Boolean;
var
  LVal: TValue;
begin
  AValue := 0;
  AOk := False;
  // O indexador encadeado (Tabela['k'].AsInt64) trava o FPC 3.2 com erro
  // interno -- por isso o TValue vai para uma variavel local antes.
  Result := (AArgs <> nil) and AArgs.TryGetValue(AKey, LVal);
  if not Result then
    Exit;
  LVal := AmqpUnwrapValue(LVal);
  case LVal.Kind of
    tkInteger: begin AValue := LVal.AsInteger; AOk := True; end;
    tkInt64:   begin AValue := LVal.AsInt64;   AOk := True; end;
  end;
end;

// True se a chave existe. AOk sai False quando existe mas nao e' string.
function TryArgString(AArgs: TAMQPFieldTable; const AKey: string;
  out AValue: string; out AOk: Boolean): Boolean;
var
  LVal: TValue;
begin
  AValue := '';
  AOk := False;
  Result := (AArgs <> nil) and AArgs.TryGetValue(AKey, LVal);
  if not Result then
    Exit;
  LVal := AmqpUnwrapValue(LVal);
  case LVal.Kind of
    tkString, tkUString, tkLString, tkWString, tkChar, tkWChar
    {$IFDEF FPC}, tkAString{$ENDIF}:
      begin
        AValue := LVal.AsString;
        AOk := True;
      end;
  end;
end;

function ErroArg(const AKey, ADetalhe: string): string;
begin
  Result := Format('invalid arg ''%s'' for queue: %s', [AKey, ADetalhe]);
end;

// Le um numerico com piso. AMin e' o menor valor ACEITO (0 para os tetos e
// para o TTL, 1 para x-expires -- uma fila que expira em 0 ms sumiria no
// instante do declare, o que o RabbitMQ tambem recusa).
function LerNumerico(AArgs: TAMQPFieldTable; const AKey: string; AMin: Int64;
  var ADestino: Int64; out AErro: string): Boolean;
var
  LVal: Int64;
  LOk: Boolean;
begin
  AErro := '';
  Result := True;
  if not TryArgInt64(AArgs, AKey, LVal, LOk) then
    Exit; // ausente
  if not LOk then
  begin
    AErro := ErroArg(AKey, 'must be an integer');
    Exit(False);
  end;
  if LVal < AMin then
  begin
    AErro := ErroArg(AKey, Format('must be >= %d', [AMin]));
    Exit(False);
  end;
  ADestino := LVal;
end;

function AmqpParseQueuePolicy(AArgs: TAMQPFieldTable;
  out APolicy: TAMQPQueuePolicy; out AErro: string): Boolean;
var
  LVal: Int64;
  LTexto: string;
  LOk: Boolean;
begin
  APolicy := TAMQPQueuePolicy.Empty;
  AErro := '';

  if not LerNumerico(AArgs, 'x-message-ttl', 0, APolicy.MessageTtlMs, AErro) then
    Exit(False);
  if not LerNumerico(AArgs, 'x-expires', 1, APolicy.ExpiresMs, AErro) then
    Exit(False);
  if not LerNumerico(AArgs, 'x-max-length', 0, APolicy.MaxLength, AErro) then
    Exit(False);
  if not LerNumerico(AArgs, 'x-max-length-bytes', 0, APolicy.MaxLengthBytes,
    AErro) then
    Exit(False);

  // x-max-priority: faixa fechada da D12, nao so' "nao-negativo".
  if TryArgInt64(AArgs, 'x-max-priority', LVal, LOk) then
  begin
    if (not LOk) or (LVal < 0) or (LVal > AMQP_MAX_PRIORITY_LEVEL) then
    begin
      AErro := ErroArg('x-max-priority', Format(
        'must be an integer between 0 and %d', [AMQP_MAX_PRIORITY_LEVEL]));
      Exit(False);
    end;
    APolicy.MaxPriority := Integer(LVal);
  end;

  // x-overflow: so' as duas politicas desta implementacao.
  if TryArgString(AArgs, 'x-overflow', LTexto, LOk) then
  begin
    if not LOk then
    begin
      AErro := ErroArg('x-overflow', 'must be a string');
      Exit(False);
    end;
    if LTexto = 'drop-head' then
      APolicy.Overflow := amqovDropHead
    else if LTexto = 'reject-publish' then
      APolicy.Overflow := amqovRejectPublish
    else
    begin
      AErro := ErroArg('x-overflow',
        'must be ''drop-head'' or ''reject-publish''');
      Exit(False);
    end;
  end;

  if TryArgString(AArgs, 'x-dead-letter-exchange', LTexto, LOk) then
  begin
    if not LOk then
    begin
      AErro := ErroArg('x-dead-letter-exchange', 'must be a string');
      Exit(False);
    end;
    APolicy.DeadLetterExchange := LTexto;
    APolicy.HasDeadLetterExchange := True;
  end;

  if TryArgString(AArgs, 'x-dead-letter-routing-key', LTexto, LOk) then
  begin
    if not LOk then
    begin
      AErro := ErroArg('x-dead-letter-routing-key', 'must be a string');
      Exit(False);
    end;
    APolicy.DeadLetterRoutingKey := LTexto;
    APolicy.HasDeadLetterRoutingKey := True;
  end;

  Result := True;
end;

function AmqpParseAlternateExchange(AArgs: TAMQPFieldTable;
  out AName: string; out AHas: Boolean; out AErro: string): Boolean;
var
  LOk: Boolean;
begin
  AName := '';
  AHas := False;
  AErro := '';
  Result := True;
  if not TryArgString(AArgs, 'alternate-exchange', AName, LOk) then
    Exit; // ausente
  if not LOk then
  begin
    AName := '';
    AErro := 'invalid arg ''alternate-exchange'' for exchange: '
      + 'must be a string';
    Exit(False);
  end;
  AHas := True;
end;

function AmqpParseExpirationMs(const AValue: string; out AMs: Int64): Boolean;
var
  I: Integer;
  LAcc: Int64;
begin
  AMs := -1;
  if AValue = '' then
    Exit(True); // ausente
  LAcc := 0;
  for I := 1 to Length(AValue) do
  begin
    if (AValue[I] < '0') or (AValue[I] > '9') then
      Exit(False);
    // Teto generoso mas finito: 'expiration' com 19 digitos e' erro do
    // cliente, e deixar estourar o Int64 daria um prazo negativo silencioso.
    if LAcc > (High(Int64) - 9) div 10 then
      Exit(False);
    LAcc := LAcc * 10 + (Ord(AValue[I]) - Ord('0'));
  end;
  AMs := LAcc;
  Result := True;
end;

function AmqpArgsEqual(A, B: TAMQPFieldTable): Boolean;
var
  LKey: string;
  LValA, LValB: TValue;
begin
  // nil e tabela vazia sao equivalentes para fins de comparacao semantica.
  if (A = nil) or (A.Count = 0) then
    Exit((B = nil) or (B.Count = 0));
  if (B = nil) or (B.Count = 0) then
    Exit(False); // A nao-vazia (early exit acima), B vazia/nil: divergem

  if A.Count <> B.Count then
    Exit(False);

  Result := True;
  for LKey in A.Keys do
  begin
    if not B.TryGetValue(LKey, LValB) then
      Exit(False);
    LValA := A[LKey]; // indexador -> variavel local antes de usar (gotcha FPC)
    if not AmqpValuesEqual(LValA, LValB) then
      Exit(False);
  end;
end;

function AmqpValuesEqual(const A, B: TValue): Boolean;
var
  LObjA, LObjB: TObject;
  LLen, I: Integer;
  LElemA, LElemB: TValue;
begin
  if A.Kind <> B.Kind then
    Exit(False);

  // 'V' (void): TValue.Empty dos dois lados -- kind ja bateu, nada mais a
  // comparar.
  if A.IsEmpty then
    Exit(True);

  if A.IsObject then
  begin
    LObjA := A.AsObject;
    LObjB := B.AsObject;
    // 'F' (tabela aninhada): unico objeto que um field-value carrega hoje.
    if (LObjA is TAMQPFieldTable) and (LObjB is TAMQPFieldTable) then
      Exit(AmqpArgsEqual(TAMQPFieldTable(LObjA), TAMQPFieldTable(LObjB)));
    Exit(LObjA = LObjB); // fallback: mesma instancia
  end;

  if A.IsArray then
  begin
    if not B.IsArray then
      Exit(False);
    LLen := A.GetArrayLength;
    if LLen <> B.GetArrayLength then
      Exit(False);
    // 'A' (array de field-values): GetArrayElement devolve o elemento
    // re-embrulhado num TValue no FPC -- AmqpUnwrapValue desfaz. Aplicamos o
    // unwrap em TODO elemento, de qualquer array: ele e' idempotente e no-op
    // no Delphi, e num array que nao seja de field-values (ex.: 'x' = TBytes)
    // o elemento nao e' um TValue embrulhado, entao tambem e' no-op.
    //
    // NAO testar aqui "TypeInfo = TypeInfo(TArray<TValue>)" para decidir se
    // desembrulha: no FPC 3.2 essa especializacao ESCRITA INLINE nesta unit e'
    // um tipo DIFERENTE do TAMQPValueArray que o AMQP.Wire produz ao decodar
    // (medido: a comparacao da False), e o efeito seria comparar os elementos
    // ainda embrulhados -- tabelas iguais dando "diferente".
    for I := 0 to LLen - 1 do
    begin
      LElemA := AmqpUnwrapValue(A.GetArrayElement(I));
      LElemB := AmqpUnwrapValue(B.GetArrayElement(I));
      if not AmqpValuesEqual(LElemA, LElemB) then
        Exit(False);
    end;
    Exit(True);
  end;

  // Escalares: os unicos Kinds que sobram sao os que WriteFieldValue sabe
  // escrever (ver AMQP.Wire) -- TValue.AsVariant NAO existe no FPC 3.2, entao
  // o dispatch e' por Kind, espelhando exatamente o case de WriteFieldValue.
  case A.Kind of
    {$IFDEF FPC}
    tkBool:
      Result := A.AsBoolean = B.AsBoolean;
    {$ENDIF}
    tkEnumeration:
      if A.TypeInfo = TypeInfo(Boolean) then
        Result := A.AsBoolean = B.AsBoolean
      else
        Result := False; // enum nao-Boolean: nao esperado num field-value
    tkInteger:
      Result := A.AsInteger = B.AsInteger;
    tkInt64:
      Result := A.AsInt64 = B.AsInt64;
    tkFloat:
      Result := A.AsExtended = B.AsExtended;
    tkString, tkUString, tkLString, tkWString, tkChar, tkWChar
    {$IFDEF FPC}, tkAString{$ENDIF}:
      Result := A.AsString = B.AsString;
  else
    Result := False; // Kind nao suportado num field-value
  end;
end;

function AmqpIsReservedName(const AName: string): Boolean;
begin
  Result := (Length(AName) >= 4) and (Copy(AName, 1, 4) = 'amq.');
end;

function AmqpIsValidExchangeType(const AType: string): Boolean;
begin
  Result := (AType = AMQP_EXCHANGE_TYPE_DIRECT) or
            (AType = AMQP_EXCHANGE_TYPE_FANOUT) or
            (AType = AMQP_EXCHANGE_TYPE_TOPIC) or
            (AType = AMQP_EXCHANGE_TYPE_HEADERS);
end;

{ TAMQPExchangeDef }

constructor TAMQPExchangeDef.Create(const AName, AExchangeType: string;
  ADurable, AAutoDelete, AInternal: Boolean; AArguments: TAMQPFieldTable);
var
  LErro: string;
begin
  inherited Create;
  FName := AName;
  FExchangeType := AExchangeType;
  FDurable := ADurable;
  FAutoDelete := AAutoDelete;
  FInternal := AInternal;
  FArguments := AArguments;
  // Mesma politica do TAMQPQueueDef: nao levanta -- a FSM ja' validou.
  if not AmqpParseAlternateExchange(FArguments, FAlternateExchange,
    FHasAlternateExchange, LErro) then
  begin
    FAlternateExchange := '';
    FHasAlternateExchange := False;
  end;
end;

destructor TAMQPExchangeDef.Destroy;
begin
  FArguments.Free;
  inherited;
end;

function TAMQPExchangeDef.EquivalentTo(AOther: TAMQPExchangeDef): Boolean;
begin
  Result := Assigned(AOther) and
    (FExchangeType = AOther.FExchangeType) and
    (FDurable = AOther.FDurable) and
    (FAutoDelete = AOther.FAutoDelete) and
    (FInternal = AOther.FInternal) and
    AmqpArgsEqual(FArguments, AOther.FArguments);
end;

{ TAMQPQueueDef }

constructor TAMQPQueueDef.Create(const AName: string;
  ADurable, AExclusive, AAutoDelete: Boolean; AArguments: TAMQPFieldTable;
  AOwnerId: NativeUInt);
var
  LErro: string;
begin
  inherited Create;
  FName := AName;
  FDurable := ADurable;
  FExclusive := AExclusive;
  FAutoDelete := AAutoDelete;
  FArguments := AArguments;
  FOwnerId := AOwnerId;
  // Nao levanta: a FSM ja' validou antes de chegar aqui, e um descritor que
  // falha ao construir deixaria a topologia num estado pior que uma politica
  // vazia. Se um dia falhar, e' bug nosso e aparece como argumento sem efeito.
  if not AmqpParseQueuePolicy(FArguments, FPolicy, LErro) then
    FPolicy := TAMQPQueuePolicy.Empty;
end;

destructor TAMQPQueueDef.Destroy;
begin
  FArguments.Free;
  inherited;
end;

function TAMQPQueueDef.EquivalentTo(AOther: TAMQPQueueDef): Boolean;
begin
  Result := Assigned(AOther) and
    (FDurable = AOther.FDurable) and
    (FExclusive = AOther.FExclusive) and
    (FAutoDelete = AOther.FAutoDelete) and
    AmqpArgsEqual(FArguments, AOther.FArguments);
end;

end.
