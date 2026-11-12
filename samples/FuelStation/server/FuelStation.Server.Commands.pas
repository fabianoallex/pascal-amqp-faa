unit FuelStation.Server.Commands;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Consumer of the posto.comandos queue (ARCHITECTURE.md §6.2). For each
  RPC request: parses the body, applies the transition to the authoritative
  registry, publishes the change's event(s) and replies through reply-to echoing
  the correlation-id. Manual ack only after replying.

  prefetch 1 + single consumer => commands are applied serially, which is
  what guarantees "a fueling in one sale only". }

interface

uses
  SysUtils,
  AMQP.Connection, AMQP.Basic.Methods,
  FuelStation.Json, FuelStation.Fueling, FuelStation.Contracts,
  FuelStation.Server.Registry, FuelStation.Server.Events;

type
  TOnServerLog = procedure(const AMsg: string) of object;
  TOnSomethingChanged = procedure of object;

  TFuelStationCommands = class
  private
    FPubChannel: TAMQPChannel;
    FRegistry: TFuelStationRegistry;
    FEvents: TFuelStationEvents;
    FTag: string;
    FOnLog: TOnServerLog;
    FOnChanged: TOnSomethingChanged;
    procedure Log(const AMsg: string);
    procedure OnCommand(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure Reply(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery;
      const ABytes: TBytes);
    function DispatchCommand(J: TJsonValue): TBytes;
    function HandlePost(J: TJsonValue): TBytes;
    function HandleUnpost(J: TJsonValue): TBytes;
    function HandleFinish(J: TJsonValue; ACancellation: Boolean): TBytes;
    function HandleForceRelease(J: TJsonValue): TBytes;
    function HandleDiscard(J: TJsonValue): TBytes;
  public
    constructor Create(AChan: TAMQPChannel; ARegistry: TFuelStationRegistry;
      AEvents: TFuelStationEvents);
    procedure Startup;
    procedure Shutdown;
    property OnLog: TOnServerLog read FOnLog write FOnLog;
    property OnChanged: TOnSomethingChanged read FOnChanged write FOnChanged;
  end;

implementation

constructor TFuelStationCommands.Create(AChan: TAMQPChannel;
  ARegistry: TFuelStationRegistry; AEvents: TFuelStationEvents);
begin
  inherited Create;
  FPubChannel := AChan;
  FRegistry := ARegistry;
  FEvents := AEvents;
end;

procedure TFuelStationCommands.Startup;
begin
  FPubChannel.Qos(1);
  FTag := FPubChannel.Consume(COMMANDS_QUEUE, OnCommand);   // manual ack
end;

procedure TFuelStationCommands.Shutdown;
begin
  if FTag <> '' then
  begin
    try
      FPubChannel.Cancel(FTag);
    except
    end;
    FTag := '';
  end;
end;

procedure TFuelStationCommands.Log(const AMsg: string);
begin
  if Assigned(FOnLog) then
    FOnLog(AMsg);
end;

procedure TFuelStationCommands.Reply(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery; const ABytes: TBytes);
var
  LProps: TAMQPBasicProperties;
begin
  if ADelivery.Properties.ReplyTo = '' then
    Exit;
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  if ADelivery.Properties.CorrelationId <> '' then
    LProps.SetCorrelationId(ADelivery.Properties.CorrelationId);
  AChannel.Publish('', ADelivery.Properties.ReplyTo, ABytes, LProps);
end;

procedure TFuelStationCommands.OnCommand(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  J: TJsonValue;
  LResp: TBytes;
begin
  J := nil;
  try
    try
      J := TJsonValue.Parse(ADelivery.BodyAsText);
      LResp := DispatchCommand(J);
    except
      on E: Exception do
      begin
        Log('Comando invalido: ' + E.Message);
        LResp := EncRespRefusal('ERRO_FORMATO', '', '');
      end;
    end;
    Reply(AChannel, ADelivery, LResp);
  finally
    J.Free;
    AChannel.Ack(ADelivery.DeliveryTag);
  end;
  if Assigned(FOnChanged) then
    FOnChanged();
end;

function TFuelStationCommands.DispatchCommand(J: TJsonValue): TBytes;
var
  LCmd: string;
  LVersion: Int64;
  LAvailable: TArray<TFueling>;
  LLocked: TArray<TTrackedFueling>;
begin
  LCmd := J.AsStr('cmd');
  if LCmd = CMD_SNAPSHOT then
  begin
    LVersion := FRegistry.Snapshot(LAvailable, LLocked);
    Result := EncRespSnapshot(LVersion, LAvailable, LLocked);
  end
  else if LCmd = CMD_POST then
    Result := HandlePost(J)
  else if LCmd = CMD_UNPOST then
    Result := HandleUnpost(J)
  else if LCmd = CMD_FINISH then
    Result := HandleFinish(J, False)
  else if LCmd = CMD_CANCEL_FINISHED then
    Result := HandleFinish(J, True)
  else if LCmd = CMD_FORCE_RELEASE then
    Result := HandleForceRelease(J)
  else if LCmd = CMD_DISCARD then
    Result := HandleDiscard(J)
  else
  begin
    Log('Comando desconhecido: "' + LCmd + '"');
    Result := EncRespRefusal('CMD_DESCONHECIDO', '', '');
  end;
end;

function TFuelStationCommands.HandlePost(J: TJsonValue): TBytes;
var
  LId, LPosName, LSale: string;
  LR: TTransitionResult;
begin
  LId := J.AsStr('id');
  LPosName := J.AsStr('pdv');
  LSale := J.AsStr('venda');
  LR := FRegistry.Post(LId, LPosName, LSale);
  if LR.Ok then
  begin
    if LR.Changed then
    begin
      FEvents.Posting(LId, LPosName, LSale, LR.Version);
      Log(Format('lancar: %s -> %s (venda %s), versao %d',
        [LId, LPosName, LSale, LR.Version]));
    end;
    Result := EncRespOk(LR.Version);
  end
  else
  begin
    Log(Format('lancar RECUSADO: %s por %s (motivo %s, dono %s)',
      [LId, LPosName, LR.Reason, LR.CurrentPos]));
    Result := EncRespRefusal(LR.Reason, LR.CurrentPos, LR.CurrentSale);
  end;
end;

function TFuelStationCommands.HandleUnpost(J: TJsonValue): TBytes;
var
  LId, LPosName, LSale, LReason: string;
  LR: TTransitionResult;
begin
  LId := J.AsStr('id');
  LPosName := J.AsStr('pdv');
  LSale := J.AsStr('venda');
  LReason := J.AsStr('motivo', RSN_ITEM_UNPOST);
  LR := FRegistry.Unpost(LId, LPosName, LSale);
  if LR.Ok then
  begin
    if LR.Changed then
    begin
      FEvents.Available(LId, LReason, LR.Version);
      Log(Format('estornar: %s liberada por %s (%s), versao %d',
        [LId, LPosName, LReason, LR.Version]));
    end;
    Result := EncRespOk(LR.Version);
  end
  else
    Result := EncRespRefusal(LR.Reason, LR.CurrentPos, LR.CurrentSale);
end;

function TFuelStationCommands.HandleFinish(J: TJsonValue;
  ACancellation: Boolean): TBytes;
var
  LPosName, LSale, LId: string;
  LIds: TJsonValue;
  I: Integer;
  LR: TTransitionResult;
  LItems: array of TItemResult;
  LReason: string;
begin
  LPosName := J.AsStr('pdv');
  LSale := J.AsStr('venda');
  LIds := J.Get('ids');
  if ACancellation then
    LReason := RSN_CANCEL_INVOICE
  else
    LReason := '';
  SetLength(LItems, 0);
  if (LIds <> nil) and (LIds.Kind = jkArray) then
    for I := 0 to LIds.Count - 1 do
    begin
      LId := LIds.Item(I).StrVal;
      if ACancellation then
        LR := FRegistry.CancelFinished(LId, LPosName, LSale)
      else
        LR := FRegistry.Finish(LId, LPosName, LSale);

      if LR.Ok and LR.Changed then
      begin
        if ACancellation then
          FEvents.Available(LId, LReason, LR.Version)
        else
          FEvents.Posted(LId, LPosName, LSale, LR.Version);
      end;

      SetLength(LItems, Length(LItems) + 1);
      LItems[High(LItems)].Id := LId;
      LItems[High(LItems)].Ok := LR.Ok;
      if not LR.Ok then
        LItems[High(LItems)].Reason := LR.Reason;
    end;

  if ACancellation then
    Log(Format('cancelar_finalizada: venda %s (%d itens)', [LSale, Length(LItems)]))
  else
    Log(Format('finalizar: venda %s (%d itens)', [LSale, Length(LItems)]));
  Result := EncRespItems(FRegistry.Version, LItems);
end;

function TFuelStationCommands.HandleForceRelease(J: TJsonValue): TBytes;
var
  LId, LOperator, LReason: string;
  LR: TTransitionResult;
begin
  LId := J.AsStr('id');
  LOperator := J.AsStr('operador');
  LReason := J.AsStr('motivo');
  LR := FRegistry.ForceRelease(LId);
  if not LR.Ok then
    Exit(EncRespRefusal(LR.Reason, '', ''));
  if LR.Changed then
  begin
    FEvents.Available(LId, RSN_MANUAL_RELEASE, LR.Version);
    Log(Format('LIBERACAO MANUAL: %s (era %s de %s) por "%s": %s',
      [LId, StateToStr(LR.PreviousState), LR.PreviousPos, LOperator,
       LReason]));
    Result := EncRespForceRelease(LR.Version,
      StateToStr(LR.PreviousState), LR.PreviousPos);
  end
  else
    Result := EncRespForceRelease(LR.Version, 'disponivel', '');
end;

function TFuelStationCommands.HandleDiscard(J: TJsonValue): TBytes;
var
  LId, LOperator, LReason: string;
  LR: TTransitionResult;
begin
  LId := J.AsStr('id');
  LOperator := J.AsStr('operador');
  LReason := J.AsStr('motivo');
  LR := FRegistry.Discard(LId);
  if not LR.Ok then
    Exit(EncRespRefusal(LR.Reason, '', ''));
  if LR.Changed then
  begin
    FEvents.Discarded(LId, LR.Version);
    Log(Format('DESCARTE: %s por "%s": %s', [LId, LOperator, LReason]));
  end;
  Result := EncRespOk(LR.Version);
end;

end.
