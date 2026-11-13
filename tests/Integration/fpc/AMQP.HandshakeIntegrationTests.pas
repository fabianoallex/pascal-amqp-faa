unit AMQP.HandshakeIntegrationTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Integration/AMQP.HandshakeIntegrationTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }

{ Handshake integration tests — they need a real RabbitMQ on
  localhost:5672 (guest/guest). Start it with: docker compose -f docker/docker-compose.yml up -d
  Without a broker, the tests that open a connection will ERROR (not a library failure). }


interface

uses
  fpcunit, testregistry,
  AMQP.TestSupport,
  SysUtils, Classes,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Connection.Methods,
  AMQP.Queue.Methods;

type
  TAMQPHandshakeIntegrationTests = class(TTestCase)
  private
    FParams: TAMQPConnectionParams;
    procedure DoOpenWithParams;
  published
    procedure Connects_DoesHandshake_And_Closes;
    procedure NegotiatedTune_HasReasonableLimits;
    procedure InvalidCredentials_Raises;
    procedure NonexistentVirtualHost_Raises;
    procedure Heartbeat_KeepsConnectionAlive;
  end;

implementation

{ TAMQPHandshakeIntegrationTests }

procedure TAMQPHandshakeIntegrationTests.DoOpenWithParams;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(FParams);
  try
    LConn.Open;
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.Connects_DoesHandshake_And_Closes;
var
  LConn: TAMQPConnection;
begin
  LConn := TAMQPConnection.Create(IntegrationParams);
  try
    LConn.Open;
    TAssert.AssertTrue('should be open after the handshake', LConn.IsOpen);
    LConn.Close;
    TAssert.AssertFalse('should be closed after Close', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.NegotiatedTune_HasReasonableLimits;
var
  LConn: TAMQPConnection;
  LTune: TAMQPConnectionTune;
begin
  LConn := TAMQPConnection.Create(IntegrationParams);
  try
    LConn.Open;
    LTune := LConn.NegotiatedTune;
    // The negotiated channel-max must never exceed the client's proposal
    // (2047) nor be 0 (RabbitMQ 3.13 proposes 2047 by default).
    TAssert.AssertTrue('channel-max should be > 0', LTune.ChannelMax > 0);
    TAssert.AssertTrue('channel-max should not exceed 2047', LTune.ChannelMax <= 2047);
    // frame-max must respect the protocol minimum (4096).
    TAssert.AssertTrue('frame-max should be >= 4096', LTune.FrameMax >= 4096);
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

procedure TAMQPHandshakeIntegrationTests.InvalidCredentials_Raises;
var
  LRaised: Boolean;
begin
  FParams := IntegrationParams;
  FParams.Password := 'obviously-wrong-password';
  // With the authentication_failure_close capability, the server replies
  // Connection.Close (403) — but we accept any exception (in old versions the
  // broker may just drop the socket). CheckRaises requires the EXACT class,
  // so "any exception" needs a manual try/except (Fail() inside the try would
  // be caught by the except itself, since an assertion failure is also an
  // Exception).
  LRaised := False;
  try
    DoOpenWithParams;
  except
    on E: Exception do
      LRaised := True;
  end;
  TAssert.AssertTrue('should have raised an exception for invalid credentials', LRaised);
end;

procedure TAMQPHandshakeIntegrationTests.NonexistentVirtualHost_Raises;
begin
  FParams := IntegrationParams;
  FParams.VirtualHost := '/vhost-that-does-not-exist';
  CheckRaises(EAMQPConnection, DoOpenWithParams);
end;

procedure TAMQPHandshakeIntegrationTests.Heartbeat_KeepsConnectionAlive;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
begin
  LParams := IntegrationParams;
  LParams.Heartbeat := 2; // negotiates 2s; the client sends a heartbeat every 1s
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.Open;
    TAssert.AssertTrue('heartbeat should be > 0', LConn.NegotiatedTune.Heartbeat > 0);
    TAssert.AssertTrue('negotiated heartbeat <= 2s', LConn.NegotiatedTune.Heartbeat <= 2);

    // Stays idle for more than 2x the interval. Without heartbeat, the broker
    // would drop the connection; with heartbeat, it stays alive.
    TThread.Sleep(5000);

    TAssert.AssertTrue('connection should stay open after idling', LConn.IsOpen);

    // And it is still usable:
    LChan := LConn.CreateChannel;
    try
      LDecl := Default(TAMQPQueueDeclare);
      LDecl.Exclusive := True;
      LDecl.AutoDelete := True;
      TAssert.AssertTrue('channel should work after the idle period',
        LChan.DeclareQueue(LDecl).QueueName <> '');
    finally
      LChan.Free;
    end;
  finally
    LConn.Free;
  end;
end;

initialization
  RegisterTest(TAMQPHandshakeIntegrationTests);

end.
