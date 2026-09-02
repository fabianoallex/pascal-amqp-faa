unit AMQP.Server.Channel;

{$I amqp.inc}

{ Um canal de uma conexão do broker (sub-módulo server).

  Guarda o ciclo de vida do canal (WS4) e a montagem de conteúdo (WS5): no
  AMQP 0-9-1 uma mensagem publicada chega em três partes NESTE canal, nesta
  ordem exata (spec 2.3.5):

    1) o método Basic.Publish        -> BeginContent
    2) um content-header com o body-size e as propriedades  -> SetContentHeader
    3) N frames de body até somar o body-size               -> AppendBody

  Entre a parte 1 e a última parte 3 nenhum outro frame pode aparecer NESTE
  canal — é a "regra de interleave", e quem a aplica é a conexão (505
  UNEXPECTED_FRAME). Canais diferentes podem intercalar à vontade: por isso o
  estado de montagem vive aqui, por canal, e não na conexão.

  Só a thread de leitura da conexão dona toca estes objetos. }

interface

uses
  SysUtils,
  AMQP.Protocol,
  AMQP.Basic.Methods,
  AMQP.Server.Types;

type
  { amqchOpen  — canal utilizável.
    amqchClosing — o broker mandou Channel.Close e espera o Channel.Close-Ok;
    todo frame que chegar no canal nesse meio-tempo é descartado (spec 1.4.2.2). }
  TAMQPServerChannelState = (amqchOpen, amqchClosing);

  { amqasIdle    — nenhum conteúdo em montagem.
    amqasHeader  — veio o Basic.Publish, falta o content-header.
    amqasBody    — veio o header, faltam body frames para fechar o body-size. }
  TAMQPChannelAsmState = (amqasIdle, amqasHeader, amqasBody);

  TAMQPServerChannel = class
  private
    FId: Word;
    FState: TAMQPServerChannelState;
    FActive: Boolean;
    FConfirmMode: Boolean;
    FPrefetchCount: Word;
    // --- montagem de conteúdo ---
    FAsmState: TAMQPChannelAsmState;
    FPublish: TAMQPBasicPublish;
    FProps: TAMQPBasicProperties;
    FBodySize: UInt64;
    FBody: TBytes;
    FBodyLen: Integer;
    procedure FreeProps;
  public
    constructor Create(AId: Word);
    destructor Destroy; override;

    function IsOpen: Boolean;

    /// Chegou o Basic.Publish: começa a montagem, esperando o content-header.
    /// Descarta qualquer montagem anterior inacabada.
    procedure BeginContent(const APublish: TAMQPBasicPublish);
    /// Chegou o content-header. Result = conteúdo JÁ completo (body-size 0,
    /// mensagem sem corpo — caso legítimo e comum em RPC).
    function SetContentHeader(const AHeader: TAMQPContentHeader): Boolean;
    /// Acrescenta um frame de body. Result = conteúdo completo.
    /// Levanta EAMQPConnectionError (505) se o corpo passar do body-size
    /// declarado — o stream ficou fora de sincronia com o que o peer prometeu.
    function AppendBody(const AData: TBytes): Boolean;
    /// Monta a mensagem pronta. Só faz sentido com a montagem completa; a
    /// tabela de Headers segue pertencendo a ESTE canal (ver IAMQPMessageSink).
    function CurrentMessage(const AUserId: string): TAMQPServerMessage;
    /// Volta a amqasIdle liberando o que foi acumulado. Idempotente.
    procedure ResetContent;

    property Id: Word read FId;
    property State: TAMQPServerChannelState read FState write FState;
    /// Channel.Flow: False = o peer pediu que paremos de entregar conteúdo
    /// neste canal. A Fase 2 respeita na entrega.
    property Active: Boolean read FActive write FActive;
    /// Confirm.Select recebido (publisher confirms). A Fase 2 usa para decidir
    /// se cada publish gera Basic.Ack.
    property ConfirmMode: Boolean read FConfirmMode write FConfirmMode;
    /// Basic.Qos. A Fase 2 usa para limitar entregas não confirmadas.
    property PrefetchCount: Word read FPrefetchCount write FPrefetchCount;

    property AsmState: TAMQPChannelAsmState read FAsmState;
    /// Body-size declarado no content-header (válido em amqasBody).
    property BodySize: UInt64 read FBodySize;
    /// Quanto do corpo já chegou.
    property BodyLen: Integer read FBodyLen;
  end;

implementation

constructor TAMQPServerChannel.Create(AId: Word);
begin
  inherited Create;
  FId := AId;
  FState := amqchOpen;
  FActive := True;
  FAsmState := amqasIdle;
  FProps := TAMQPBasicProperties.Empty;
end;

destructor TAMQPServerChannel.Destroy;
begin
  ResetContent; // um canal fechado no meio de um publish não pode vazar a tabela
  inherited;
end;

function TAMQPServerChannel.IsOpen: Boolean;
begin
  Result := FState = amqchOpen;
end;

procedure TAMQPServerChannel.FreeProps;
begin
  // DecodeContentHeader cria a tabela de headers quando a propriedade está
  // presente; o dono é quem decodificou, ou seja, este canal.
  if FProps.Has(bpHeaders) and Assigned(FProps.Headers) then
    FProps.Headers.Free;
  FProps := TAMQPBasicProperties.Empty;
end;

procedure TAMQPServerChannel.ResetContent;
begin
  FreeProps;
  FAsmState := amqasIdle;
  FBodySize := 0;
  FBody := nil;
  FBodyLen := 0;
end;

procedure TAMQPServerChannel.BeginContent(const APublish: TAMQPBasicPublish);
begin
  ResetContent;
  FPublish := APublish;
  FAsmState := amqasHeader;
end;

function TAMQPServerChannel.SetContentHeader(
  const AHeader: TAMQPContentHeader): Boolean;
begin
  FreeProps;
  FProps := AHeader.Properties;
  FBodySize := AHeader.BodySize;
  FBody := nil;
  FBodyLen := 0;
  // Sem corpo: a mensagem já está completa e nenhum body frame virá.
  if FBodySize = 0 then
  begin
    FAsmState := amqasIdle;
    Exit(True);
  end;
  FAsmState := amqasBody;
  Result := False;
end;

function TAMQPServerChannel.AppendBody(const AData: TBytes): Boolean;
var
  LLen: Integer;
begin
  LLen := Length(AData);
  // Cresce conforme chega: nada de alocar o body-size declarado de uma vez,
  // senão um peer hostil derruba o broker só anunciando um corpo gigante.
  if UInt64(FBodyLen) + UInt64(LLen) > FBodySize then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('corpo excede o body-size declarado (%d): já %d + %d octetos',
        [Int64(FBodySize), FBodyLen, LLen]), 0, 0);

  if LLen > 0 then
  begin
    SetLength(FBody, FBodyLen + LLen);
    Move(AData[0], FBody[FBodyLen], LLen);
    Inc(FBodyLen, LLen);
  end;

  Result := UInt64(FBodyLen) = FBodySize;
  if Result then
    FAsmState := amqasIdle;
end;

function TAMQPServerChannel.CurrentMessage(
  const AUserId: string): TAMQPServerMessage;
begin
  Result.Exchange := FPublish.Exchange;
  Result.RoutingKey := FPublish.RoutingKey;
  Result.Mandatory := FPublish.Mandatory;
  Result.Immediate := FPublish.Immediate;
  Result.Properties := FProps;
  Result.Body := Copy(FBody, 0, FBodyLen); // FBody pode ter folga de alocação
  Result.UserId := AUserId;
end;

end.
