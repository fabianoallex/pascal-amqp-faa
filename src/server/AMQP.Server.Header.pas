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
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Server.Message;

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

    /// Body-size lido do payload original.
    property BodySizeOriginal: UInt64 read FBodySize;
    /// Propriedades decodificadas (leitura). A tabela de headers dentro delas
    /// e' a mesma que Headers devolve.
    property Properties: TAMQPBasicProperties read FProps;
  end;

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
