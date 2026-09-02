unit AMQP.Server.Channel;

{$I amqp.inc}

{ Um canal de uma conexão do broker (sub-módulo server, WS4).

  No WS4 o canal é só ciclo de vida: existe entre o Channel.Open e o
  Channel.Close(-Ok), e carrega o flag de Channel.Flow. WS5 pendura aqui a
  montagem de conteúdo (method Basic.Publish + content-header + N body frames)
  e a Fase 2 acrescenta consumidores, prefetch e delivery tags.

  Só a thread de leitura da conexão dona toca estes objetos. }

interface

type
  { amqchOpen  — canal utilizável.
    amqchClosing — o broker mandou Channel.Close e espera o Channel.Close-Ok;
    todo frame que chegar no canal nesse meio-tempo é descartado (spec 1.4.2.2). }
  TAMQPServerChannelState = (amqchOpen, amqchClosing);

  TAMQPServerChannel = class
  private
    FId: Word;
    FState: TAMQPServerChannelState;
    FActive: Boolean;
  public
    constructor Create(AId: Word);

    function IsOpen: Boolean;

    property Id: Word read FId;
    property State: TAMQPServerChannelState read FState write FState;
    /// Channel.Flow: False = o peer pediu que paremos de entregar conteúdo
    /// neste canal. WS4 só guarda o flag; a Fase 2 respeita na entrega.
    property Active: Boolean read FActive write FActive;
  end;

implementation

constructor TAMQPServerChannel.Create(AId: Word);
begin
  inherited Create;
  FId := AId;
  FState := amqchOpen;
  FActive := True;
end;

function TAMQPServerChannel.IsOpen: Boolean;
begin
  Result := FState = amqchOpen;
end;

end.
