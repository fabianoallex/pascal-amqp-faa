unit AMQP.IntegrationConfig;

{ Where the integration suite points -- WS9 of Phase 2.

  Until then every test called TAMQPConnectionParams.Localhost directly, which
  tied the suite to the RabbitMQ on localhost:5672. WS9 needs to run
  EXACTLY THE SAME tests against this lib's embedded broker (decision D8
  in CLAUDE.md), so the address became a one-line indirection.

  With nobody configuring anything, the behaviour is identical to before:
  localhost:5672 (and 5671 for TLS). The acceptance runner
  (tests\Acceptance\fpc) starts a TAMQPServer on an ephemeral port and calls
  AmqpUseEmbeddedBroker before running the tests.

  A single copy, compiled by the Delphi and FPC runners (integration and
  acceptance). }

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

interface

uses
  SysUtils,
  AMQP.Connection;

/// The suite's PLAIN connection parameters. Default: TAMQPConnectionParams.
/// Localhost (dev RabbitMQ on localhost:5672). The environment variables
/// AMQP_TEST_HOST and AMQP_TEST_PORT change the default (the acceptance
/// embedded broker beats both): on a machine where 5672 belongs to another
/// broker, point at a RabbitMQ of your own on another port.
function IntegrationParams: TAMQPConnectionParams;

/// The suite's TLS connection parameters. Default: TAMQPConnectionParams.
/// LocalhostTls (port 5671, verify-peer off -- self-signed dev cert).
function IntegrationTlsParams: TAMQPConnectionParams;

/// Points the suite at a broker on 127.0.0.1 on the given ports (0 = the suite
/// keeps that protocol's default). Call BEFORE running the tests.
procedure AmqpUseEmbeddedBroker(APlainPort, ATlsPort: Word);

/// True if the suite points at the embedded broker -- some tests
/// need to know (see the use in AMQP.HandshakeIntegrationTests).
function AmqpUsingEmbeddedBroker: Boolean;

implementation

var
  GPlainPort: Word = 0;
  GTlsPort: Word = 0;
  GEmbedded: Boolean = False;

function AmqpUsingEmbeddedBroker: Boolean;
begin
  Result := GEmbedded;
end;

procedure AmqpUseEmbeddedBroker(APlainPort, ATlsPort: Word);
begin
  GPlainPort := APlainPort;
  GTlsPort := ATlsPort;
  GEmbedded := True;
end;

function IntegrationParams: TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  if GetEnvironmentVariable('AMQP_TEST_HOST') <> '' then
    Result.Host := GetEnvironmentVariable('AMQP_TEST_HOST');
  Result.Port := StrToIntDef(GetEnvironmentVariable('AMQP_TEST_PORT'), Result.Port);
  if GPlainPort <> 0 then
  begin
    Result.Host := '127.0.0.1';
    Result.Port := GPlainPort;
  end;
end;

function IntegrationTlsParams: TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.LocalhostTls;
  if GTlsPort <> 0 then
  begin
    Result.Host := '127.0.0.1';
    Result.Port := GTlsPort;
  end
  else if GEmbedded then
    // Embedded mode WITHOUT a TLS port (Default build: the broker has no TLS).
    // Pointing at a port where nothing listens is deliberate: otherwise the suite
    // would fall back to RabbitMQ's 5671 and report an acceptance PASS for
    // tests that never touched the embedded broker -- a false green. This way
    // the TLS tests' Setup fails to connect and they ignore themselves, which is
    // the same path as "TLS broker down".
    Result.Port := 1;
end;

end.
