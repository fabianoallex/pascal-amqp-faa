unit Posto.PDV.Cliente;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Conexao do PDV com o servidor de automacao e o RPC sincrono
  (ARQUITETURA.md §5, §6.2).

  Chamar() bloqueia ate' a resposta ou o timeout -- e' feito para rodar numa
  thread de trabalho (AmqpPool), NUNCA na thread da UI. A fila de respostas
  e' exclusiva, auto-delete, de nome fixo (sobrevive ao replay da reconexao,
  ao contrario de um nome gerado pelo broker -- ver ConsultaStatusVcl).

  A reconexao automatica da lib recria fila de eventos, binds e consumers; o
  que o PDV faz DEPOIS da reconexao (re-snapshot, reconciliacao) e' com a
  Sincronia e o Modelo. }

interface

uses
  SysUtils, Classes, SyncObjs, Generics.Collections,
  AMQP.Wire, AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods, AMQP.Basic.Methods,
  Posto.Json, Posto.Contratos;

type
  TRespostaRpc = record
    Chegou: Boolean;     // resposta chegou dentro do prazo
    Timeout: Boolean;
    Json: TJsonValue;    // dono: o chamador; nil se Timeout
  end;

  TPostoPdvCliente = class
  private
    FHost: string;
    FPorta: Word;
    FPdv: string;
    FSenha: string;
    FConn: TAMQPConnection;
    FCanal: TAMQPChannel;      // publish de comandos + consumo das respostas
    FCanalEventos: TAMQPChannel;
    FFilaResp: string;
    FFilaEventos: string;
    FPubLock: TCriticalSection;
    FPendLock: TCriticalSection;
    FPend: TObjectDictionary<string, TEvent>;
    FResp: TDictionary<string, TBytes>;
    procedure OnResposta(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    constructor Create(const AHost: string; APorta: Word;
      const APdv, ASenha: string);
    destructor Destroy; override;

    procedure Conectar(AOnDisconnect, AOnReconnect,
      AOnReconnectFailed: TAMQPConnectionEvent);
    procedure Desconectar;
    function Conectado: Boolean;

    { Assina os eventos do posto. O callback roda numa thread do pool. }
    procedure AssinarEventos(const ACallback: TAMQPConsumerCallback);

    { RPC sincrono. Bloqueia ate' a resposta ou ATimeoutMs. }
    function Chamar(const ABytes: TBytes; ATimeoutMs: Integer): TRespostaRpc;

    property Pdv: string read FPdv;
  end;

implementation

function NovoCorr: string;
var
  LG: TGUID;
begin
  CreateGUID(LG);
  Result := GUIDToString(LG);
end;

constructor TPostoPdvCliente.Create(const AHost: string; APorta: Word;
  const APdv, ASenha: string);
begin
  inherited Create;
  FHost := AHost;
  FPorta := APorta;
  FPdv := APdv;
  FSenha := ASenha;
  FPubLock := TCriticalSection.Create;
  FPendLock := TCriticalSection.Create;
  FPend := TObjectDictionary<string, TEvent>.Create([doOwnsValues]);
  FResp := TDictionary<string, TBytes>.Create;
  FFilaResp := 'pdv-resp-' + APdv + '-' + Copy(NovoCorr, 2, 8);
  FFilaEventos := 'pdv-eventos-' + APdv + '-' + Copy(NovoCorr, 2, 8);
end;

destructor TPostoPdvCliente.Destroy;
begin
  Desconectar;
  FResp.Free;
  FPend.Free;
  FPendLock.Free;
  FPubLock.Free;
  inherited Destroy;
end;

procedure TPostoPdvCliente.Conectar(AOnDisconnect, AOnReconnect,
  AOnReconnectFailed: TAMQPConnectionEvent);
var
  LParams: TAMQPConnectionParams;
  LDecl: TAMQPQueueDeclare;
begin
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := FHost;
  LParams.Port := FPorta;
  LParams.User := FPdv;
  LParams.Password := FSenha;
  LParams.ConnectionName := FPdv;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 2000;
  LParams.MaxReconnectAttempts := 0;

  FConn := TAMQPConnection.Create(LParams);
  FConn.OnDisconnect := AOnDisconnect;
  FConn.OnReconnect := AOnReconnect;
  FConn.OnReconnectFailed := AOnReconnectFailed;
  FConn.Open;

  FCanal := FConn.CreateChannel;
  LDecl := TAMQPQueueDeclare.Create(FFilaResp, False);
  LDecl.Exclusive := True;
  LDecl.AutoDelete := True;
  FCanal.DeclareQueue(LDecl);
  FCanal.Consume(FFilaResp, OnResposta, True);   // NoAck
end;

procedure TPostoPdvCliente.Desconectar;
begin
  FreeAndNil(FCanalEventos);
  FreeAndNil(FCanal);
  FreeAndNil(FConn);
end;

function TPostoPdvCliente.Conectado: Boolean;
begin
  Result := Assigned(FConn) and FConn.IsOpen;
end;

procedure TPostoPdvCliente.AssinarEventos(const ACallback: TAMQPConsumerCallback);
var
  LDecl: TAMQPQueueDeclare;
  LBind: TAMQPQueueBind;
begin
  FCanalEventos := FConn.CreateChannel;
  FCanalEventos.DeclareExchange(TAMQPExchangeDeclare.Create(EXCHANGE_EVENTOS,
    AMQP_EXCHANGE_TYPE_TOPIC, False));
  LDecl := TAMQPQueueDeclare.Create(FFilaEventos, False);
  LDecl.Exclusive := True;
  LDecl.AutoDelete := True;
  FCanalEventos.DeclareQueue(LDecl);
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := FFilaEventos;
  LBind.ExchangeName := EXCHANGE_EVENTOS;
  LBind.RoutingKey := BIND_EVENTOS;
  FCanalEventos.BindQueue(LBind);
  FCanalEventos.Consume(FFilaEventos, ACallback, True);   // NoAck
end;

procedure TPostoPdvCliente.OnResposta(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LCorr: string;
  LEvt: TEvent;
begin
  LCorr := ADelivery.Properties.CorrelationId;
  FPendLock.Enter;
  try
    if FPend.TryGetValue(LCorr, LEvt) then
    begin
      FResp.AddOrSetValue(LCorr, ADelivery.Body);
      LEvt.SetEvent;
    end;
  finally
    FPendLock.Leave;
  end;
end;

function TPostoPdvCliente.Chamar(const ABytes: TBytes;
  ATimeoutMs: Integer): TRespostaRpc;
var
  LCorr: string;
  LEvt: TEvent;
  LProps: TAMQPBasicProperties;
  LBody: TBytes;
begin
  Result := Default(TRespostaRpc);
  if not Conectado then
  begin
    Result.Timeout := True;
    Exit;
  end;

  LCorr := NovoCorr;
  LEvt := TEvent.Create(nil, True, False, '');
  FPendLock.Enter;
  try
    FPend.Add(LCorr, LEvt);
  finally
    FPendLock.Leave;
  end;

  try
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetContentType('application/json');
    LProps.SetReplyTo(FFilaResp);
    LProps.SetCorrelationId(LCorr);
    LProps.SetExpiration(IntToStr(ATimeoutMs));

    FPubLock.Enter;
    try
      FCanal.Publish('', FILA_COMANDOS, ABytes, LProps);
    finally
      FPubLock.Leave;
    end;

    if LEvt.WaitFor(ATimeoutMs) = wrSignaled then
    begin
      FPendLock.Enter;
      try
        FResp.TryGetValue(LCorr, LBody);
        FResp.Remove(LCorr);
      finally
        FPendLock.Leave;
      end;
      Result.Chegou := True;
      try
        Result.Json := TJsonValue.Parse(AmqpUtf8Decode(LBody));
      except
        Result.Chegou := False;
        Result.Timeout := True;
      end;
    end
    else
      Result.Timeout := True;
  except
    on E: Exception do
    begin
      Result := Default(TRespostaRpc);
      Result.Timeout := True;
    end;
  end;

  FPendLock.Enter;
  try
    FPend.Remove(LCorr);   // TObjectDictionary libera o TEvent
    FResp.Remove(LCorr);
  finally
    FPendLock.Leave;
  end;
end;

end.
