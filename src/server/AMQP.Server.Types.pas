unit AMQP.Server.Types;

{$I amqp.inc}

{ Tipos compartilhados do sub-módulo broker (WS4).

  Vive abaixo de AMQP.Server.Connection e de AMQP.Server.Broker para os dois
  enxergarem as mesmas peças sem ciclo de units:

  - TAMQPVirtualHostRegistry — os vhosts que o broker aceita no Connection.Open
    (estava em AMQP.Server.Broker no WS6; a FSM de handshake precisa consultá-lo,
    e o broker não pode ser usado pela conexão, que ele próprio usa).
  - TAMQPServerConnConfig — o que o broker entrega a cada conexão aceita
    (autenticador, autorizador, vhosts e os limites propostos no Tune).
  - As exceções que a FSM usa como controle de fluxo do modelo de erro do AMQP:
    EAMQPChannelError (fecha só o canal), EAMQPConnectionError (fecha a conexão
    com Connection.Close) e EAMQPServerAbort (derruba o socket sem Close). }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  AMQP.Threading,     // atomics
  AMQP.Basic.Methods, // TAMQPBasicProperties
  AMQP.Server.Auth;

const
  /// Limites que o broker propõe no Connection.Tune. Iguais aos do RabbitMQ,
  /// para os defaults do cliente (TAMQPConnectionParams.Localhost) casarem sem
  /// negociação para baixo.
  AMQP_SERVER_DEFAULT_CHANNEL_MAX = 2047;
  AMQP_SERVER_DEFAULT_FRAME_MAX   = 131072;
  AMQP_SERVER_DEFAULT_HEARTBEAT   = 60;

  /// Quanto esperamos pelo Connection.Close-Ok depois de mandarmos um
  /// Connection.Close antes de simplesmente derrubar o socket. Aplicado pela
  /// thread monitora do broker via TAMQPServerConnection.EnforceCloseDeadline.
  AMQP_SERVER_CLOSE_TIMEOUT_MS = 10000;

  /// Período da thread monitora do broker (heartbeats, detecção de peer morto,
  /// prazo do Close-Ok e reap das conexões mortas). Tem de ser bem menor que
  /// metade do menor heartbeat útil — com 200 ms, um heartbeat negociado de
  /// 1 s (usado nos testes) ainda é atendido com folga.
  AMQP_SERVER_MONITOR_TICK_MS = 200;

type
  { Conjunto de virtual-hosts que o broker aceita no Connection.Open.
    Fase 1: só a existência importa (sem isolamento de recursos, sem
    permissões — ver desvios no CLAUDE.md). Default: só a raiz "/".
    Thread-safe (várias conexões handshakeiam em paralelo). }
  TAMQPVirtualHostRegistry = class
  private
    FLock: TCriticalSection;
    FNames: TStringList; // CaseSensitive, Sorted, Duplicates ignorados
  public
    constructor Create;
    destructor Destroy; override;
    procedure Add(const AName: string);
    procedure Remove(const AName: string);
    function Contains(const AName: string): Boolean;
    function ToArray: TArray<string>;
  end;

  { Uma mensagem publicada, já com o conteúdo remontado (método Basic.Publish +
    content-header + N body frames). }
  TAMQPServerMessage = record
    Exchange: string;
    RoutingKey: string;
    Mandatory: Boolean;   // sem rota => devolver com Basic.Return (Fase 2)
    Immediate: Boolean;   // obsoleto no RabbitMQ; decodificado e ignorado
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    /// Usuário autenticado da conexão que publicou (para validar a propriedade
    /// user-id e para auditoria na Fase 2).
    UserId: string;
  end;

  { Destino de uma mensagem publicada. A Fase 1 usa TAMQPNullMessageSink (que
    descarta); a engine de roteamento da Fase 2 entra aqui sem mudar a
    assinatura de nenhum chamador.

    IMPORTANTE: a mensagem e a tabela de Headers dentro de Properties pertencem
    ao CANAL, que as libera assim que RouteMessage retorna. Uma implementação
    que precise guardar a mensagem tem de copiar o que interessa. }
  IAMQPMessageSink = interface
    ['{2A0F6E31-1C5D-4E8B-9F27-6D3B0A5C4E19}']
    /// True se a mensagem foi roteada para ao menos uma fila. False com
    /// Mandatory=True é o que dispara o Basic.Return na Fase 2.
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage): Boolean;
  end;

  { Descarta tudo (broker "nulo" da Fase 1). Conta o que passou, para os testes
    e para diagnóstico. }
  TAMQPNullMessageSink = class(TInterfacedObject, IAMQPMessageSink)
  private
    FCount: Integer; // atômico
  public
    function RouteMessage(const AVHost: string;
      const AMessage: TAMQPServerMessage): Boolean;
    /// Quantas mensagens foram descartadas desde a criação.
    function Count: Integer;
  end;

  { Erro de canal (spec 1.4.2.2): o broker manda Channel.Close no canal
    ofensor, o canal morre e a conexão segue viva. }
  EAMQPChannelError = class(Exception)
  private
    FChannel: Word;
    FCode: Word;
    FClassId: Word;
    FMethodId: Word;
  public
    constructor Create(AChannel, ACode: Word; const AText: string;
      AClassId: Word = 0; AMethodId: Word = 0); reintroduce;
    /// Canal onde o erro ocorreu (nunca 0).
    property Channel: Word read FChannel;
    /// reply-code (ver AMQP.Protocol: 403/404/405/406/311...).
    property Code: Word read FCode;
    /// Método que causou o erro (0/0 quando não se aplica).
    property ClassId: Word read FClassId;
    property MethodId: Word read FMethodId;
  end;

  { Erro de conexão (spec 1.4.2.3): o broker manda Connection.Close no canal 0,
    espera o Close-Ok e encerra. }
  EAMQPConnectionError = class(Exception)
  private
    FCode: Word;
    FClassId: Word;
    FMethodId: Word;
  public
    constructor Create(ACode: Word; const AText: string;
      AClassId: Word = 0; AMethodId: Word = 0); reintroduce;
    property Code: Word read FCode;
    property ClassId: Word read FClassId;
    property MethodId: Word read FMethodId;
  end;

  { Encerra a conexão SEM Connection.Close: ou o transporte já morreu, ou a
    spec manda fechar o socket direto (auth negada a um cliente que não
    anunciou a capability 'authentication_failure_close'). }
  EAMQPServerAbort = class(Exception);

  { O que o broker entrega a cada conexão aceita. A conexão NÃO é dona de
    nada aqui (o broker vive mais que ela). }
  TAMQPServerConnConfig = record
    Auth: IAMQPAuthenticator;
    Authorizer: IAMQPAuthorizer;
    VHosts: TAMQPVirtualHostRegistry;
    /// Limites propostos no Connection.Tune (0 = sem limite).
    ChannelMax: Word;
    FrameMax: Cardinal;
    Heartbeat: Word;
    /// True se o socket já está cifrado (repassado ao autenticador).
    Tls: Boolean;
    /// Quanto esperar pelo Connection.Close-Ok antes de derrubar o socket.
    CloseTimeoutMs: Cardinal;
    /// Para onde vão as mensagens publicadas. Nunca nil nas conexões que o
    /// broker cria (ele semeia com TAMQPNullMessageSink).
    Sink: IAMQPMessageSink;
    /// Config sem autenticador/vhosts (o broker preenche esses dois).
    class function Defaults: TAMQPServerConnConfig; static;
  end;

implementation

{ TAMQPVirtualHostRegistry }

constructor TAMQPVirtualHostRegistry.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FNames := TStringList.Create;
  FNames.CaseSensitive := True;
  FNames.Sorted := True;
  FNames.Duplicates := dupIgnore;
  FNames.Add('/');
end;

destructor TAMQPVirtualHostRegistry.Destroy;
begin
  FNames.Free;
  FLock.Free;
  inherited;
end;

procedure TAMQPVirtualHostRegistry.Add(const AName: string);
begin
  FLock.Enter;
  try
    FNames.Add(AName);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPVirtualHostRegistry.Remove(const AName: string);
var
  I: Integer;
begin
  FLock.Enter;
  try
    I := FNames.IndexOf(AName);
    if I >= 0 then
      FNames.Delete(I);
  finally
    FLock.Leave;
  end;
end;

function TAMQPVirtualHostRegistry.Contains(const AName: string): Boolean;
begin
  FLock.Enter;
  try
    Result := FNames.IndexOf(AName) >= 0;
  finally
    FLock.Leave;
  end;
end;

function TAMQPVirtualHostRegistry.ToArray: TArray<string>;
var
  I: Integer;
begin
  Result := nil;
  FLock.Enter;
  try
    SetLength(Result, FNames.Count);
    for I := 0 to FNames.Count - 1 do
      Result[I] := FNames[I];
  finally
    FLock.Leave;
  end;
end;

{ EAMQPChannelError }

constructor EAMQPChannelError.Create(AChannel, ACode: Word; const AText: string;
  AClassId: Word; AMethodId: Word);
begin
  inherited Create(AText);
  FChannel := AChannel;
  FCode := ACode;
  FClassId := AClassId;
  FMethodId := AMethodId;
end;

{ EAMQPConnectionError }

constructor EAMQPConnectionError.Create(ACode: Word; const AText: string;
  AClassId: Word; AMethodId: Word);
begin
  inherited Create(AText);
  FCode := ACode;
  FClassId := AClassId;
  FMethodId := AMethodId;
end;

{ TAMQPServerConnConfig }

class function TAMQPServerConnConfig.Defaults: TAMQPServerConnConfig;
begin
  Result.Auth := nil;
  Result.Authorizer := nil;
  Result.VHosts := nil;
  Result.ChannelMax := AMQP_SERVER_DEFAULT_CHANNEL_MAX;
  Result.FrameMax := AMQP_SERVER_DEFAULT_FRAME_MAX;
  Result.Heartbeat := AMQP_SERVER_DEFAULT_HEARTBEAT;
  Result.Tls := False;
  Result.CloseTimeoutMs := AMQP_SERVER_CLOSE_TIMEOUT_MS;
  Result.Sink := nil;
end;

{ TAMQPNullMessageSink }

function TAMQPNullMessageSink.RouteMessage(const AVHost: string;
  const AMessage: TAMQPServerMessage): Boolean;
begin
  AmqpAtomicInc(FCount);
  // Fase 1: nenhuma fila existe, então nada foi roteado. Devolver False é o
  // que a Fase 2 usará para disparar o Basic.Return de um publish mandatory.
  Result := False;
end;

function TAMQPNullMessageSink.Count: Integer;
begin
  Result := AmqpAtomicGet(FCount);
end;

end.
