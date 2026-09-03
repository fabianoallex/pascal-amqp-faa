unit AMQP.IntegrationConfig;

{ Para onde a suite de integracao aponta -- WS9 da Fase 2.

  Ate' aqui cada teste chamava TAMQPConnectionParams.Localhost direto, o que
  amarrava a suite ao RabbitMQ de localhost:5672. A WS9 precisa rodar
  EXATAMENTE OS MESMOS testes contra o broker embutido desta lib (decisao D8
  do CLAUDE.md), entao o endereco virou uma indirecao de uma linha.

  Sem ninguem configurar nada, o comportamento e' identico ao de antes:
  localhost:5672 (e 5671 para TLS). O runner de aceitacao
  (tests\Acceptance\fpc) sobe um TAMQPServer em porta efemera e chama
  AmqpUseEmbeddedBroker antes de rodar os testes.

  Espelho FPCUnit de tests\Integration\fpc\AMQP.IntegrationConfig.pas -- mantenha
  os dois em sincronia. }

interface

uses
  System.SysUtils,
  AMQP.Connection;

/// Parametros de conexao PLAIN da suite. Default: TAMQPConnectionParams.
/// Localhost (RabbitMQ de dev em localhost:5672).
function IntegrationParams: TAMQPConnectionParams;

/// Parametros de conexao TLS da suite. Default: TAMQPConnectionParams.
/// LocalhostTls (porta 5671, verify-peer desligado -- cert self-signed de dev).
function IntegrationTlsParams: TAMQPConnectionParams;

/// Aponta a suite para um broker em 127.0.0.1 nas portas dadas (0 = a suite
/// segue no default daquele protocolo). Chamar ANTES de rodar os testes.
procedure AmqpUseEmbeddedBroker(APlainPort, ATlsPort: Word);

/// True se a suite esta apontada para o broker embutido -- alguns testes
/// precisam saber (ver o uso em AMQP.HandshakeIntegrationTests).
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
    // Modo embutido SEM porta TLS (build Default: o broker nao tem TLS).
    // Apontar para uma porta onde nada escuta e' deliberado: sem isso a suite
    // cairia de volta na 5671 do RabbitMQ e reportaria PASS de aceitacao para
    // testes que nem tocaram no broker embutido -- um verde falso. Assim o
    // Setup dos testes de TLS falha em conectar e eles se auto-ignoram, que e'
    // o mesmo caminho de "broker TLS fora do ar".
    Result.Port := 1;
end;

end.
