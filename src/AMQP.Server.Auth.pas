unit AMQP.Server.Auth;

{$I amqp.inc}

{ Autenticacao e autorizacao do broker (sub-modulo server, Fase 1).

  Fluxo no handshake (ver AMQP.Server.Connection, WS4):
    S: Connection.Start   mechanisms = IAMQPAuthenticator.Mechanisms
    C: Connection.Start-Ok  mechanism, response (SASL)
       -> IAMQPAuthenticator.Authenticate(mechanism, response, peer, tls)
          Deny  + capability 'authentication_failure_close' anunciada
                -> Connection.Close 403 ACCESS_REFUSED (FailReason no texto)
          Deny  sem a capability -> fecha o socket TCP direto
          Allow -> segue para Connection.Tune; UserId fica anexado a conexao

  Fase 1: so o mecanismo PLAIN (#0 user #0 pass). Sem Connection.Secure/Secure-Ok
  (SASL multi-step) e sem AMQPLAIN (mecanismo proprietario RabbitMQ). Ver o
  CLAUDE.md ("Sub-modulo broker") para os desvios deliberados do RabbitMQ:
  guest aceito fora de loopback, sem permissoes por vhost, sem delay pos-falha,
  sem validacao da propriedade user-id do Basic.Publish. }

interface

uses
  SysUtils,
  SyncObjs,
  Generics.Collections,
  AMQP.Wire; // AmqpUtf8Decode

const
  AMQP_MECH_PLAIN = 'PLAIN';

type
  { Resultado de uma tentativa de autenticacao. }
  TAMQPAuthResult = record
    Granted: Boolean;
    UserId: string;      // identidade canonica anexada a conexao quando Granted
    FailReason: string;  // texto do 403 ACCESS_REFUSED quando negado
    class function Allow(const AUserId: string): TAMQPAuthResult; static;
    class function Deny(const AReason: string): TAMQPAuthResult; static;
  end;

  { Valida credenciais SASL. O broker segura uma referencia; a implementacao
    e' consultada uma vez por conexao, durante o handshake. Deve ser
    thread-safe (varias conexoes handshakeiam em paralelo). }
  IAMQPAuthenticator = interface
    ['{44FC93B3-FD5A-40EB-8910-3AFBA9FD1054}']
    /// Mecanismos SASL suportados, em ordem de preferencia. Vira a lista de
    /// 'mechanisms' do Connection.Start (separada por espacos). Fase 1: ['PLAIN'].
    function Mechanisms: TArray<string>;
    /// Valida a resposta SASL crua. Para PLAIN: #0 user #0 pass.
    /// APeerAddress: 'host:porta' do cliente, para log / rate-limit futuro.
    /// ATls: True se a conexao ja esta cifrada no momento do handshake.
    function Authenticate(const AMechanism: string; const AResponse: TBytes;
      const APeerAddress: string; ATls: Boolean): TAMQPAuthResult;
  end;

  /// Permissao sobre um recurso, no modelo do RabbitMQ (configure/write/read).
  TAMQPPerm = (ampConfigure, ampWrite, ampRead);

  { Autoriza operacoes sobre recursos (exchange/queue) por usuario e vhost.
    Stub allow-all na Fase 1 (TAMQPAllowAllAuthorizer); a interface existe para
    a engine da Fase 2 encaixar sem mudar as assinaturas dos chamadores. }
  IAMQPAuthorizer = interface
    ['{06CDFA25-22D2-4B6B-B719-1B854837C4BD}']
    function CheckResource(const AUserId, AVHost, AResource: string;
      APerm: TAMQPPerm): Boolean;
  end;

  { Concede qualquer credencial. Para testes e uso local sem controle de acesso.
    O UserId vira o usuario informado no PLAIN (ou 'anonymous' se vazio). }
  TAMQPAllowAllAuthenticator = class(TInterfacedObject, IAMQPAuthenticator)
  public
    function Mechanisms: TArray<string>;
    function Authenticate(const AMechanism: string; const AResponse: TBytes;
      const APeerAddress: string; ATls: Boolean): TAMQPAuthResult;
  end;

  { Valida contra um dicionario usuario -> senha em memoria (senha em claro).
    Default do broker: uma unica entrada guest/guest -- assim o
    TAMQPConnectionParams.Localhost do cliente conecta sem mudanca. }
  TAMQPStaticAuthenticator = class(TInterfacedObject, IAMQPAuthenticator)
  private
    FUsers: TDictionary<string, string>;
    FLock: TCriticalSection; // leituras/escritas concorrentes (varias conexoes)
  public
    /// Semeia guest/guest.
    constructor Create; overload;
    /// Pares planos: ['user1','pass1','user2','pass2', ...]. Numero impar
    /// de itens levanta EArgumentException.
    constructor Create(const APairs: array of string); overload;
    destructor Destroy; override;
    procedure SetUser(const AUser, APassword: string);
    procedure RemoveUser(const AUser: string);
    function Mechanisms: TArray<string>;
    function Authenticate(const AMechanism: string; const AResponse: TBytes;
      const APeerAddress: string; ATls: Boolean): TAMQPAuthResult;
  end;

  { Autorizador que libera tudo (Fase 1). }
  TAMQPAllowAllAuthorizer = class(TInterfacedObject, IAMQPAuthorizer)
  public
    function CheckResource(const AUserId, AVHost, AResource: string;
      APerm: TAMQPPerm): Boolean;
  end;

/// Faz o parse da resposta do mecanismo PLAIN: [authzid] #0 authcid #0 passwd.
/// authzid (primeiro campo) e' ignorado. Retorna False se a estrutura estiver
/// malformada (numero de separadores != 2). user/passwd saem decodificados de
/// UTF-8.
function AmqpParsePlainResponse(const AResponse: TBytes;
  out AUser, APassword: string): Boolean;

implementation

{ TAMQPAuthResult }

class function TAMQPAuthResult.Allow(const AUserId: string): TAMQPAuthResult;
begin
  Result.Granted := True;
  Result.UserId := AUserId;
  Result.FailReason := '';
end;

class function TAMQPAuthResult.Deny(const AReason: string): TAMQPAuthResult;
begin
  Result.Granted := False;
  Result.UserId := '';
  Result.FailReason := AReason;
end;

{ Parse PLAIN }

function AmqpParsePlainResponse(const AResponse: TBytes;
  out AUser, APassword: string): Boolean;
var
  LFirstNul, LSecondNul, I: Integer;
  LUserBytes, LPassBytes: TBytes;
begin
  AUser := '';
  APassword := '';
  LFirstNul := -1;
  LSecondNul := -1;
  for I := 0 to Length(AResponse) - 1 do
    if AResponse[I] = 0 then
    begin
      if LFirstNul < 0 then
        LFirstNul := I
      else if LSecondNul < 0 then
      begin
        LSecondNul := I;
        Break;
      end;
    end;
  if (LFirstNul < 0) or (LSecondNul < 0) then
    Exit(False);

  LUserBytes := Copy(AResponse, LFirstNul + 1, LSecondNul - LFirstNul - 1);
  LPassBytes := Copy(AResponse, LSecondNul + 1, Length(AResponse) - LSecondNul - 1);
  AUser := AmqpUtf8Decode(LUserBytes);
  APassword := AmqpUtf8Decode(LPassBytes);
  Result := True;
end;

{ TAMQPAllowAllAuthenticator }

function TAMQPAllowAllAuthenticator.Mechanisms: TArray<string>;
begin
  Result := nil;
  SetLength(Result, 1);
  Result[0] := AMQP_MECH_PLAIN;
end;

function TAMQPAllowAllAuthenticator.Authenticate(const AMechanism: string;
  const AResponse: TBytes; const APeerAddress: string;
  ATls: Boolean): TAMQPAuthResult;
var
  LUser, LPass: string;
begin
  if not SameText(AMechanism, AMQP_MECH_PLAIN) then
    Exit(TAMQPAuthResult.Deny('mecanismo nao suportado: ' + AMechanism));
  if not AmqpParsePlainResponse(AResponse, LUser, LPass) then
    Exit(TAMQPAuthResult.Deny('resposta PLAIN malformada'));
  if LUser = '' then
    LUser := 'anonymous';
  Result := TAMQPAuthResult.Allow(LUser);
end;

{ TAMQPStaticAuthenticator }

constructor TAMQPStaticAuthenticator.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FUsers := TDictionary<string, string>.Create;
  FUsers.Add('guest', 'guest');
end;

constructor TAMQPStaticAuthenticator.Create(const APairs: array of string);
var
  I: Integer;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FUsers := TDictionary<string, string>.Create;
  if Odd(Length(APairs)) then
    raise EArgumentException.Create(
      'TAMQPStaticAuthenticator: numero impar de itens (esperado user,pass,...)');
  I := 0;
  while I < Length(APairs) do
  begin
    FUsers.AddOrSetValue(APairs[I], APairs[I + 1]);
    Inc(I, 2);
  end;
end;

destructor TAMQPStaticAuthenticator.Destroy;
begin
  FUsers.Free;
  FLock.Free;
  inherited;
end;

procedure TAMQPStaticAuthenticator.SetUser(const AUser, APassword: string);
begin
  FLock.Enter;
  try
    FUsers.AddOrSetValue(AUser, APassword);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPStaticAuthenticator.RemoveUser(const AUser: string);
begin
  FLock.Enter;
  try
    FUsers.Remove(AUser);
  finally
    FLock.Leave;
  end;
end;

function TAMQPStaticAuthenticator.Mechanisms: TArray<string>;
begin
  Result := nil;
  SetLength(Result, 1);
  Result[0] := AMQP_MECH_PLAIN;
end;

function TAMQPStaticAuthenticator.Authenticate(const AMechanism: string;
  const AResponse: TBytes; const APeerAddress: string;
  ATls: Boolean): TAMQPAuthResult;
var
  LUser, LPass, LStored: string;
  LFound: Boolean;
begin
  if not SameText(AMechanism, AMQP_MECH_PLAIN) then
    Exit(TAMQPAuthResult.Deny('mecanismo nao suportado: ' + AMechanism));
  if not AmqpParsePlainResponse(AResponse, LUser, LPass) then
    Exit(TAMQPAuthResult.Deny('resposta PLAIN malformada'));

  FLock.Enter;
  try
    LFound := FUsers.TryGetValue(LUser, LStored);
  finally
    FLock.Leave;
  end;

  // Mensagem generica de proposito: nao revela se o usuario existe.
  if (not LFound) or (LStored <> LPass) then
    Exit(TAMQPAuthResult.Deny(
      'Login was refused using authentication mechanism PLAIN.'));
  Result := TAMQPAuthResult.Allow(LUser);
end;

{ TAMQPAllowAllAuthorizer }

function TAMQPAllowAllAuthorizer.CheckResource(const AUserId, AVHost,
  AResource: string; APerm: TAMQPPerm): Boolean;
begin
  Result := True;
end;

end.
