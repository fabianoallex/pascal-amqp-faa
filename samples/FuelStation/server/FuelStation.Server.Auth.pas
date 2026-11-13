unit FuelStation.Server.Auth;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The station's authenticator (ARCHITECTURE.md §6.1): SASL PLAIN, accepts
  'automation' (the server's own connection) and any 'pos-*' with the
  configured password. No register pre-enrollment -- a new POS joins the network
  and connects. The authenticated UserId is the POS name, and that is how Watcher
  maps connection -> POS.

  Descends from TInterfacedObject: _AddRef/_Release inherited (their calling
  convention varies by platform on FPC -- reimplementing them would require the
  calling-convention IFDEF; we do not reimplement them). }

interface

uses
  SysUtils,
  AMQP.Server.Auth,
  FuelStation.Contracts;

type
  TFuelStationAuthenticator = class(TInterfacedObject, IAMQPAuthenticator)
  private
    FAutomationPassword: string;
    FPosPassword: string;
  public
    constructor Create(const AAutomationPassword, APosPassword: string);
    function Mechanisms: TArray<string>;
    function Authenticate(const AMechanism: string; const AResponse: TBytes;
      const APeerAddress: string; ATls: Boolean): TAMQPAuthResult;
  end;

implementation

constructor TFuelStationAuthenticator.Create(const AAutomationPassword,
  APosPassword: string);
begin
  inherited Create;
  FAutomationPassword := AAutomationPassword;
  FPosPassword := APosPassword;
end;

function TFuelStationAuthenticator.Mechanisms: TArray<string>;
begin
  Result := TArray<string>.Create('PLAIN');
end;

function TFuelStationAuthenticator.Authenticate(const AMechanism: string;
  const AResponse: TBytes; const APeerAddress: string;
  ATls: Boolean): TAMQPAuthResult;
var
  LUser, LPass: string;
begin
  if not SameText(AMechanism, 'PLAIN') then
    Exit(TAMQPAuthResult.Deny('unsupported mechanism'));
  if not AmqpParsePlainResponse(AResponse, LUser, LPass) then
    Exit(TAMQPAuthResult.Deny('malformed PLAIN response'));

  if LUser = AUTOMATION_USER then
  begin
    if LPass = FAutomationPassword then
      Exit(TAMQPAuthResult.Allow(LUser));
    Exit(TAMQPAuthResult.Deny('invalid password'));
  end;

  if Copy(LUser, 1, Length(POS_USER_PREFIX)) = POS_USER_PREFIX then
  begin
    if LPass = FPosPassword then
      Exit(TAMQPAuthResult.Allow(LUser));
    Exit(TAMQPAuthResult.Deny('invalid password'));
  end;

  Result := TAMQPAuthResult.Deny('unknown user');
end;

end.
