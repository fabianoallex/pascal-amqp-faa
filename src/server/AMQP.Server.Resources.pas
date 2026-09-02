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

type
  { Descreve um exchange declarado. Dono de Arguments. }
  TAMQPExchangeDef = class
  private
    FName: string;
    FExchangeType: string;
    FDurable: Boolean;
    FAutoDelete: Boolean;
    FInternal: Boolean;
    FArguments: TAMQPFieldTable;
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

implementation

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
begin
  inherited Create;
  FName := AName;
  FExchangeType := AExchangeType;
  FDurable := ADurable;
  FAutoDelete := AAutoDelete;
  FInternal := AInternal;
  FArguments := AArguments;
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
begin
  inherited Create;
  FName := AName;
  FDurable := ADurable;
  FExclusive := AExclusive;
  FAutoDelete := AAutoDelete;
  FArguments := AArguments;
  FOwnerId := AOwnerId;
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
