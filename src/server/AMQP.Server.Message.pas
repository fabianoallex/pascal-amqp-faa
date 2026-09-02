unit AMQP.Server.Message;

{$I amqp.inc}

{ A mensagem publicada, do jeito que a engine (Fase 2) vai guardar nas filas
  (sub-modulo broker, WS1 da Fase 2).

  Decisao D1 (CLAUDE.md, "Sub-modulo broker" / Fase 2): a mensagem carrega o
  payload CRU do content-header (HeaderPayload), nao um clone da
  TAMQPFieldTable de Properties -- reemissao fiel byte a byte ao consumidor
  (Basic.Deliver) e nada do problema de TValue-em-TValue do FPC 3.2 ao clonar
  tabelas/arrays aninhados. Quem precisa das propriedades decodificadas (ex.:
  o match de bindings de um headers exchange) decodifica a partir da tabela
  VIVA do canal, antes do enqueue -- e' o que a "Invariante nova" do CLAUDE.md
  descreve. TAMQPMessage, por isso, NAO guarda TAMQPBasicProperties nenhuma.

  Refcount (decisao de arquitetura travada no CLAUDE.md, secao "Mensagem"):
  TAMQPMessage nasce com RefCount=1 e e' compartilhada por N filas sem copiar
  o corpo (COW) -- cada fila que segura a mensagem da' AddRef; Release quando
  larga. O ultimo Release libera o objeto. NINGUEM muta a mensagem depois de
  criada: Body e HeaderPayload sao expostos so' para leitura.

  Atomics via AmqpAtomicInc/Dec (AMQP.Threading) -- TInterlocked nao existe no
  FPC (ver tabela de proibidos do CLAUDE.md). }

interface

uses
  SysUtils,
  AMQP.Threading,
  AMQP.Server.Types;

type
  { Um pedaco do corpo, para reparticao em N body frames pelo frame-max do
    DESTINATARIO da entrega. Tipo nomeado por causa do FPC: um parametro
    tipado como "array of T" e' open array (semantica de valor, copia os
    elementos) -- aqui e' so' o tipo de retorno de AmqpSplitBody, mas fica
    nomeado por documentacao e simetria com o resto da lib (ver gotcha do
    array nomeado vs. open array no CLAUDE.md). }
  TAMQPBodyChunks = array of TBytes;

  { Mensagem publicada, guardada pela engine sob refcount atomico. Imutavel
    apos construida. }
  TAMQPMessage = class
  private
    FRefCount: Integer; // atomico -- ver AddRef/Release
    FExchange: string;
    FRoutingKey: string;
    FUserId: string;
    FHeaderPayload: TBytes;
    FBody: TBytes;
    function GetBodySize: Integer;
    function GetRefCount: Integer;
  public
    /// Body e HeaderPayload sao guardados por REFERENCIA (semantica normal de
    /// dynamic array do Pascal): o chamador que quiser um buffer independente
    /// copia ANTES de construir. RefCount nasce em 1 -- quem cria e' dono
    /// dessa primeira referencia e tem de dar Release (nunca Free direto).
    constructor Create(const AExchange, ARoutingKey, AUserId: string;
      const AHeaderPayload, ABody: TBytes);

    /// Incrementa o refcount; devolve o valor novo.
    function AddRef: Integer;
    /// Decrementa o refcount; quando chega a 0, libera o objeto (Self.Free)
    /// e devolve 0 -- nesse caso, Self nao pode mais ser usado.
    function Release: Integer;

    /// Constroi a partir de uma TAMQPServerMessage ja remontada pelo canal
    /// (BeginContent -> SetContentHeader -> AppendBody -> CurrentMessage).
    /// RefCount nasce em 1, como em Create.
    class function FromServerMessage(
      const AMsg: TAMQPServerMessage): TAMQPMessage;

    property Exchange: string read FExchange;
    property RoutingKey: string read FRoutingKey;
    /// Usuario autenticado da conexao que publicou (auditoria / validacao
    /// futura da propriedade user-id).
    property UserId: string read FUserId;
    /// Payload cru do content-header (class-id/weight/body-size/flags e as
    /// propriedades presentes), exatamente como veio no frame -- decisao D1.
    /// Reemitir para um consumidor e' escrever este payload direto no frame
    /// de header da entrega, sem redecodificar/reencodar.
    property HeaderPayload: TBytes read FHeaderPayload;
    /// Corpo da mensagem. Compartilhado por N filas sem copia -- ninguem muta.
    property Body: TBytes read FBody;
    property BodySize: Integer read GetBodySize;
    property RefCount: Integer read GetRefCount;
  end;

/// Fatia ABody em pedacos de no maximo AMaxPayload bytes cada. AMaxPayload e'
/// o tamanho maximo de payload de um body frame para o DESTINATARIO da
/// entrega -- o chamador ja' desconta o overhead do frame do frame-max
/// negociado (cabecalho de frame + terminador). Corpo vazio -> 0 pedacos.
/// AMaxPayload = 0 -> um unico pedaco com o corpo inteiro (sem limite).
function AmqpSplitBody(const ABody: TBytes;
  AMaxPayload: Cardinal): TAMQPBodyChunks;

implementation

{ TAMQPMessage }

constructor TAMQPMessage.Create(const AExchange, ARoutingKey, AUserId: string;
  const AHeaderPayload, ABody: TBytes);
begin
  inherited Create;
  FRefCount := 1;
  FExchange := AExchange;
  FRoutingKey := ARoutingKey;
  FUserId := AUserId;
  FHeaderPayload := AHeaderPayload;
  FBody := ABody;
end;

function TAMQPMessage.GetBodySize: Integer;
begin
  Result := Length(FBody);
end;

function TAMQPMessage.GetRefCount: Integer;
begin
  Result := AmqpAtomicGet(FRefCount);
end;

function TAMQPMessage.AddRef: Integer;
begin
  Result := AmqpAtomicInc(FRefCount);
end;

function TAMQPMessage.Release: Integer;
begin
  Result := AmqpAtomicDec(FRefCount);
  if Result = 0 then
    Free;
end;

class function TAMQPMessage.FromServerMessage(
  const AMsg: TAMQPServerMessage): TAMQPMessage;
begin
  Result := TAMQPMessage.Create(AMsg.Exchange, AMsg.RoutingKey, AMsg.UserId,
    AMsg.HeaderPayload, AMsg.Body);
end;

{ AmqpSplitBody }

function AmqpSplitBody(const ABody: TBytes;
  AMaxPayload: Cardinal): TAMQPBodyChunks;
var
  LTotal, LMax, LOffset, LChunkLen, LCount, I: Integer;
begin
  Result := nil;
  LTotal := Length(ABody);
  if LTotal = 0 then
    Exit; // corpo vazio -> 0 pedacos

  if AMaxPayload = 0 then
  begin
    SetLength(Result, 1);
    Result[0] := Copy(ABody, 0, LTotal);
    Exit;
  end;

  LMax := Integer(AMaxPayload);
  LCount := (LTotal + LMax - 1) div LMax; // teto da divisao
  SetLength(Result, LCount);
  LOffset := 0;
  for I := 0 to LCount - 1 do
  begin
    LChunkLen := LMax;
    if LOffset + LChunkLen > LTotal then
      LChunkLen := LTotal - LOffset;
    Result[I] := Copy(ABody, LOffset, LChunkLen);
    Inc(LOffset, LChunkLen);
  end;
end;

end.
