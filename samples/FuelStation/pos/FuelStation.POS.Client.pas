unit FuelStation.POS.Client;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The POS connection to the automation server and the synchronous RPC
  (ARCHITECTURE.md §5, §6.2).

  Call() blocks until the reply or the timeout -- it is meant to run on a
  worker thread (PcPool), NEVER on the UI thread. The reply queue
  is exclusive, auto-delete, with a fixed name (it survives the reconnection
  replay, unlike a broker-generated name -- see StatusQueryVcl).

  The lib's automatic reconnection recreates the event queue, binds and
  consumers; what the POS does AFTER the reconnection (re-snapshot,
  reconciliation) is up to Sincronia and Modelo. }

interface

uses
  SysUtils, Classes, SyncObjs, Generics.Collections,
  AMQP.Wire, AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods, AMQP.Basic.Methods,
  FuelStation.Json, FuelStation.Contracts;

type
  TRpcReply = record
    Arrived: Boolean;     // reply arrived within the deadline
    Timeout: Boolean;
    Json: TJsonValue;    // owner: the caller; nil on Timeout
  end;

  TFuelStationPosClient = class
  private
    FHost: string;
    FPort: Word;
    FPosName: string;
    FPassword: string;
    FConn: TAMQPConnection;
    FPubChannel: TAMQPChannel;      // command publish + reply consumption
    FEventChannel: TAMQPChannel;
    FReplyQueue: string;
    FEventQueue: string;
    FPubLock: TCriticalSection;
    FPendLock: TCriticalSection;
    FPend: TObjectDictionary<string, TEvent>;
    FResp: TDictionary<string, TBytes>;
    procedure OnReply(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    constructor Create(const AHost: string; APort: Word;
      const APosName, APassword: string);
    destructor Destroy; override;

    procedure Connect(AOnDisconnect, AOnReconnect,
      AOnReconnectFailed: TAMQPConnectionEvent);
    procedure Disconnect;
    function Connected: Boolean;

    { Subscribes to the station's events. The callback runs on a pool thread. }
    procedure SubscribeEvents(const ACallback: TAMQPConsumerCallback);

    { Synchronous RPC. Blocks until the reply or ATimeoutMs. }
    function Call(const ABytes: TBytes; ATimeoutMs: Integer): TRpcReply;

    property PosName: string read FPosName;
  end;

implementation

function NewCorrId: string;
var
  LG: TGUID;
begin
  CreateGUID(LG);
  Result := GUIDToString(LG);
end;

constructor TFuelStationPosClient.Create(const AHost: string; APort: Word;
  const APosName, APassword: string);
begin
  inherited Create;
  FHost := AHost;
  FPort := APort;
  FPosName := APosName;
  FPassword := APassword;
  FPubLock := TCriticalSection.Create;
  FPendLock := TCriticalSection.Create;
  FPend := TObjectDictionary<string, TEvent>.Create([doOwnsValues]);
  FResp := TDictionary<string, TBytes>.Create;
  FReplyQueue := 'pdv-resp-' + APosName + '-' + Copy(NewCorrId, 2, 8);
  FEventQueue := 'pdv-eventos-' + APosName + '-' + Copy(NewCorrId, 2, 8);
end;

destructor TFuelStationPosClient.Destroy;
begin
  Disconnect;
  FResp.Free;
  FPend.Free;
  FPendLock.Free;
  FPubLock.Free;
  inherited Destroy;
end;

procedure TFuelStationPosClient.Connect(AOnDisconnect, AOnReconnect,
  AOnReconnectFailed: TAMQPConnectionEvent);
var
  LParams: TAMQPConnectionParams;
  LDecl: TAMQPQueueDeclare;
begin
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := FHost;
  LParams.Port := FPort;
  LParams.User := FPosName;
  LParams.Password := FPassword;
  LParams.ConnectionName := FPosName;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 2000;
  LParams.MaxReconnectAttempts := 0;

  FConn := TAMQPConnection.Create(LParams);
  FConn.OnDisconnect := AOnDisconnect;
  FConn.OnReconnect := AOnReconnect;
  FConn.OnReconnectFailed := AOnReconnectFailed;
  FConn.Open;

  FPubChannel := FConn.CreateChannel;
  LDecl := TAMQPQueueDeclare.Create(FReplyQueue, False);
  LDecl.Exclusive := True;
  LDecl.AutoDelete := True;
  FPubChannel.DeclareQueue(LDecl);
  FPubChannel.Consume(FReplyQueue, OnReply, True);   // NoAck
end;

procedure TFuelStationPosClient.Disconnect;
begin
  FreeAndNil(FEventChannel);
  FreeAndNil(FPubChannel);
  FreeAndNil(FConn);
end;

function TFuelStationPosClient.Connected: Boolean;
begin
  Result := Assigned(FConn) and FConn.IsOpen;
end;

procedure TFuelStationPosClient.SubscribeEvents(const ACallback: TAMQPConsumerCallback);
var
  LDecl: TAMQPQueueDeclare;
  LBind: TAMQPQueueBind;
begin
  FEventChannel := FConn.CreateChannel;
  FEventChannel.DeclareExchange(TAMQPExchangeDeclare.Create(EVENTS_EXCHANGE,
    AMQP_EXCHANGE_TYPE_TOPIC, False));
  LDecl := TAMQPQueueDeclare.Create(FEventQueue, False);
  LDecl.Exclusive := True;
  LDecl.AutoDelete := True;
  FEventChannel.DeclareQueue(LDecl);
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := FEventQueue;
  LBind.ExchangeName := EVENTS_EXCHANGE;
  LBind.RoutingKey := EVENTS_BINDING;
  FEventChannel.BindQueue(LBind);
  FEventChannel.Consume(FEventQueue, ACallback, True);   // NoAck
end;

procedure TFuelStationPosClient.OnReply(AChannel: TAMQPChannel;
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

function TFuelStationPosClient.Call(const ABytes: TBytes;
  ATimeoutMs: Integer): TRpcReply;
var
  LCorr: string;
  LEvt: TEvent;
  LProps: TAMQPBasicProperties;
  LBody: TBytes;
begin
  Result := Default(TRpcReply);
  if not Connected then
  begin
    Result.Timeout := True;
    Exit;
  end;

  LCorr := NewCorrId;
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
    LProps.SetReplyTo(FReplyQueue);
    LProps.SetCorrelationId(LCorr);
    LProps.SetExpiration(IntToStr(ATimeoutMs));

    FPubLock.Enter;
    try
      FPubChannel.Publish('', COMMANDS_QUEUE, ABytes, LProps);
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
      Result.Arrived := True;
      try
        Result.Json := TJsonValue.Parse(AmqpUtf8Decode(LBody));
      except
        Result.Arrived := False;
        Result.Timeout := True;
      end;
    end
    else
      Result.Timeout := True;
  except
    on E: Exception do
    begin
      Result := Default(TRpcReply);
      Result.Timeout := True;
    end;
  end;

  FPendLock.Enter;
  try
    FPend.Remove(LCorr);   // TObjectDictionary frees the TEvent
    FResp.Remove(LCorr);
  finally
    FPendLock.Leave;
  end;
end;

end.
