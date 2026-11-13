unit FuelStation.Server.App;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The assembly of the automation server WITHOUT UI: embedded broker + loopback
  'automation' connection + registry + events + command consumer +
  Watcher + (optional) pump simulator.

  The GUI panel and the smoke test use this class -- the UI only wires callbacks
  and reads Registry/Watcher. }

interface

uses
  SysUtils,
  AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods,
  AMQP.Server.Broker,
  FuelStation.Fueling, FuelStation.Contracts,
  FuelStation.Server.Registry, FuelStation.Server.Events, FuelStation.Server.Commands,
  FuelStation.Server.Pumps, FuelStation.Server.Watcher, FuelStation.Server.Auth;

type
  TFuelStationLogEvent = procedure(const AMsg: string) of object;
  TFuelStationNotifyEvent = procedure of object;

  TFuelStationServerApp = class
  private
    FBroker: TAMQPServer;
    FConn: TAMQPConnection;
    FEventChannel: TAMQPChannel;
    FCommandChannel: TAMQPChannel;
    FRegistry: TFuelStationRegistry;
    FEvents: TFuelStationEvents;
    FCommands: TFuelStationCommands;
    FWatcher: TFuelStationWatcher;
    FPumps: TFuelStationPumps;
    FRunning: Boolean;
    FOnLog: TFuelStationLogEvent;
    FOnChanged: TFuelStationNotifyEvent;
    procedure Log(const AMsg: string);
    procedure OnFueling(const A: TFueling);
  public
    constructor Create;
    destructor Destroy; override;

    procedure Startup(APort: Word; const AAutomationPassword, APosPassword: string;
      AWithPumps: Boolean);
    procedure Shutdown;

    function Running: Boolean;
    function EffectivePort: Word;

    property Broker: TAMQPServer read FBroker;
    property Registry: TFuelStationRegistry read FRegistry;
    property Events: TFuelStationEvents read FEvents;
    property Watcher: TFuelStationWatcher read FWatcher;
    property Pumps: TFuelStationPumps read FPumps;

    property OnLog: TFuelStationLogEvent read FOnLog write FOnLog;
    { Called (from various threads) when something changes -- the UI marks itself dirty. }
    property OnChanged: TFuelStationNotifyEvent read FOnChanged write FOnChanged;
  end;

implementation

const
  DEFAULT_AUTOMATION_PASSWORD = 'automation';

constructor TFuelStationServerApp.Create;
begin
  inherited Create;
end;

destructor TFuelStationServerApp.Destroy;
begin
  Shutdown;
  inherited Destroy;
end;

procedure TFuelStationServerApp.Log(const AMsg: string);
begin
  if Assigned(FOnLog) then
    FOnLog(AMsg);
end;

function TFuelStationServerApp.Running: Boolean;
begin
  Result := FRunning;
end;

function TFuelStationServerApp.EffectivePort: Word;
begin
  if FBroker <> nil then
    Result := FBroker.Port
  else
    Result := 0;
end;

// Runs on the pump simulator thread.
procedure TFuelStationServerApp.OnFueling(const A: TFueling);
var
  LVersion: Int64;
begin
  if FRegistry.RegisterFueling(A, LVersion) then
  begin
    FEvents.NewFueling(A, LVersion);
    Log(Format('PUMP %d/%d %s  %.3f L  R$ %.2f  (%s)  version %d',
      [A.Pump, A.Nozzle, A.Product, A.Liters, A.TotalAmount,
       FuelingTypeToStr(A.Kind), LVersion]));
    if Assigned(FOnChanged) then
      FOnChanged();
  end;
end;

procedure TFuelStationServerApp.Startup(APort: Word; const AAutomationPassword,
  APosPassword: string; AWithPumps: Boolean);
var
  LParams: TAMQPConnectionParams;
  LAutomationPassword: string;
begin
  if FRunning then
    Exit;
  LAutomationPassword := AAutomationPassword;
  if LAutomationPassword = '' then
    LAutomationPassword := DEFAULT_AUTOMATION_PASSWORD;

  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '0.0.0.0';
  FBroker.Port := APort;
  FBroker.Authenticator := TFuelStationAuthenticator.Create(LAutomationPassword, APosPassword);
  FBroker.Start;
  Log(Format('Broker up on 0.0.0.0:%d.', [FBroker.Port]));

  FRegistry := TFuelStationRegistry.Create;

  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := FBroker.Port;
  LParams.User := AUTOMATION_USER;
  LParams.Password := LAutomationPassword;
  LParams.ConnectionName := 'automation';
  LParams.AutoReconnect := False;
  FConn := TAMQPConnection.Create(LParams);
  FConn.Open;

  FEventChannel := FConn.CreateChannel;
  FEventChannel.DeclareExchange(TAMQPExchangeDeclare.Create(EVENTS_EXCHANGE,
    AMQP_EXCHANGE_TYPE_TOPIC, False));
  FEvents := TFuelStationEvents.Create(FEventChannel);

  FCommandChannel := FConn.CreateChannel;
  FCommandChannel.DeclareQueue(TAMQPQueueDeclare.Create(COMMANDS_QUEUE, False));
  FCommands := TFuelStationCommands.Create(FCommandChannel, FRegistry, FEvents);
  FCommands.OnLog := FOnLog;
  FCommands.OnChanged := FOnChanged;
  FCommands.Startup;

  FWatcher := TFuelStationWatcher.Create(FBroker);
  FWatcher.OnChanged := FOnChanged;
  FWatcher.Startup;

  // The simulator thread always starts; it begins generating or paused depending
  // on AWithPumps. The panel turns generation on/off through FPumps.SetPaused.
  FPumps := TFuelStationPumps.Create(OnFueling);
  FPumps.SetPaused(not AWithPumps);
  FPumps.Start;

  FRunning := True;
  Log('Automation ready.');
end;

procedure TFuelStationServerApp.Shutdown;
begin
  if FPumps <> nil then
  begin
    FPumps.Shutdown;
    FreeAndNil(FPumps);
  end;
  if FCommands <> nil then
  begin
    FCommands.Shutdown;
    FreeAndNil(FCommands);
  end;
  if FWatcher <> nil then
  begin
    FWatcher.Shutdown;
    FreeAndNil(FWatcher);
  end;
  FreeAndNil(FEvents);
  FreeAndNil(FCommandChannel);
  FreeAndNil(FEventChannel);
  FreeAndNil(FConn);
  FreeAndNil(FRegistry);
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
  if FRunning then
    Log('Automation stopped.');
  FRunning := False;
end;

end.
