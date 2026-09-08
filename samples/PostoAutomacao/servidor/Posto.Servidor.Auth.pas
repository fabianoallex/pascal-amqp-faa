unit Posto.Servidor.Auth;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Autenticador do posto (ARQUITETURA.md §6.1): SASL PLAIN, aceita
  'automacao' (a conexao do proprio servidor) e qualquer 'pdv-*' com a senha
  configurada. Sem pre-cadastro de caixa -- um PDV novo entra na rede e
  conecta. O UserId autenticado e' o nome do PDV, e e' assim que o Vigia
  mapeia conexao -> PDV.

  Desce de TInterfacedObject: _AddRef/_Release herdados (a convencao de
  chamada deles varia por plataforma no FPC -- reimplementar exigiria o
  IFDEF de convencao; nao reimplementamos). }

interface

uses
  SysUtils,
  AMQP.Server.Auth,
  Posto.Contratos;

type
  TPostoAuthenticator = class(TInterfacedObject, IAMQPAuthenticator)
  private
    FSenhaAutomacao: string;
    FSenhaPdv: string;
  public
    constructor Create(const ASenhaAutomacao, ASenhaPdv: string);
    function Mechanisms: TArray<string>;
    function Authenticate(const AMechanism: string; const AResponse: TBytes;
      const APeerAddress: string; ATls: Boolean): TAMQPAuthResult;
  end;

implementation

constructor TPostoAuthenticator.Create(const ASenhaAutomacao,
  ASenhaPdv: string);
begin
  inherited Create;
  FSenhaAutomacao := ASenhaAutomacao;
  FSenhaPdv := ASenhaPdv;
end;

function TPostoAuthenticator.Mechanisms: TArray<string>;
begin
  Result := TArray<string>.Create('PLAIN');
end;

function TPostoAuthenticator.Authenticate(const AMechanism: string;
  const AResponse: TBytes; const APeerAddress: string;
  ATls: Boolean): TAMQPAuthResult;
var
  LUser, LPass: string;
begin
  if not SameText(AMechanism, 'PLAIN') then
    Exit(TAMQPAuthResult.Deny('mecanismo nao suportado'));
  if not AmqpParsePlainResponse(AResponse, LUser, LPass) then
    Exit(TAMQPAuthResult.Deny('resposta PLAIN malformada'));

  if LUser = USER_AUTOMACAO then
  begin
    if LPass = FSenhaAutomacao then
      Exit(TAMQPAuthResult.Allow(LUser));
    Exit(TAMQPAuthResult.Deny('senha invalida'));
  end;

  if Copy(LUser, 1, Length(PREFIXO_USER_PDV)) = PREFIXO_USER_PDV then
  begin
    if LPass = FSenhaPdv then
      Exit(TAMQPAuthResult.Allow(LUser));
    Exit(TAMQPAuthResult.Deny('senha invalida'));
  end;

  Result := TAMQPAuthResult.Deny('usuario nao reconhecido');
end;

end.
