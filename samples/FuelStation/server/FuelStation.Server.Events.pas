unit FuelStation.Server.Events;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Publishing of the fueling events to the posto.eventos exchange
  (ARCHITECTURE.md §6.3). One dedicated channel; an internal lock because the
  pump thread and the command consumer thread both publish. }

interface

uses
  SysUtils, SyncObjs,
  AMQP.Connection, AMQP.Basic.Methods,
  FuelStation.Fueling, FuelStation.Contracts;

type
  TFuelStationEvents = class
  private
    FPubChannel: TAMQPChannel;
    FLock: TCriticalSection;
    procedure PublishTo(const ARoute: string; const ABytes: TBytes);
  public
    constructor Create(AChan: TAMQPChannel);
    destructor Destroy; override;
    procedure NewFueling(const A: TFueling; AVersion: Int64);
    procedure Posting(const AId, APosName, ASale: string; AVersion: Int64);
    procedure Posted(const AId, APosName, ASale: string; AVersion: Int64);
    procedure Available(const AId, AReason: string; AVersion: Int64);
    procedure Discarded(const AId: string; AVersion: Int64);
  end;

implementation

constructor TFuelStationEvents.Create(AChan: TAMQPChannel);
begin
  inherited Create;
  FPubChannel := AChan;
  FLock := TCriticalSection.Create;
end;

destructor TFuelStationEvents.Destroy;
begin
  FLock.Free;
  inherited Destroy;
end;

procedure TFuelStationEvents.PublishTo(const ARoute: string; const ABytes: TBytes);
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  FLock.Enter;
  try
    FPubChannel.Publish(EVENTS_EXCHANGE, ARoute, ABytes, LProps);
  finally
    FLock.Leave;
  end;
end;

procedure TFuelStationEvents.NewFueling(const A: TFueling; AVersion: Int64);
begin
  PublishTo(EVT_NEW, EncEventNew(A, AVersion));
end;

procedure TFuelStationEvents.Posting(const AId, APosName, ASale: string; AVersion: Int64);
begin
  PublishTo(EVT_POSTING, EncEventPosting(AId, APosName, ASale, AVersion));
end;

procedure TFuelStationEvents.Posted(const AId, APosName, ASale: string; AVersion: Int64);
begin
  PublishTo(EVT_POSTED, EncEventPosted(AId, APosName, ASale, AVersion));
end;

procedure TFuelStationEvents.Available(const AId, AReason: string; AVersion: Int64);
begin
  PublishTo(EVT_AVAILABLE, EncEventAvailable(AId, AReason, AVersion));
end;

procedure TFuelStationEvents.Discarded(const AId: string; AVersion: Int64);
begin
  PublishTo(EVT_DISCARDED, EncEventDiscarded(AId, AVersion));
end;

end.
