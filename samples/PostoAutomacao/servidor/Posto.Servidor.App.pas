unit Posto.Servidor.App;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ A montagem do servidor de automacao SEM UI: broker embutido + conexao
  'automacao' de loopback + registro + eventos + consumidor de comandos +
  Vigia + (opcional) simulador de bombas.

  O painel GUI e o smoke test usam esta classe -- a UI so' liga callbacks e
  le' Registro/Vigia. }

interface

uses
  SysUtils,
  AMQP.Connection, AMQP.Transport,
  AMQP.Exchange.Methods, AMQP.Queue.Methods,
  AMQP.Server.Broker,
  Posto.Abastecida, Posto.Contratos,
  Posto.Servidor.Registro, Posto.Servidor.Eventos, Posto.Servidor.Comandos,
  Posto.Servidor.Bombas, Posto.Servidor.Vigia, Posto.Servidor.Auth;

type
  TPostoLog = procedure(const AMsg: string) of object;
  TPostoAviso = procedure of object;

  TPostoServidorApp = class
  private
    FBroker: TAMQPServer;
    FConn: TAMQPConnection;
    FCanalEventos: TAMQPChannel;
    FCanalComandos: TAMQPChannel;
    FRegistro: TPostoRegistro;
    FEventos: TPostoEventos;
    FComandos: TPostoComandos;
    FVigia: TPostoVigia;
    FBombas: TPostoBombas;
    FRodando: Boolean;
    FOnLog: TPostoLog;
    FOnMudou: TPostoAviso;
    procedure Log(const AMsg: string);
    procedure OnAbastecida(const A: TAbastecida);
  public
    constructor Create;
    destructor Destroy; override;

    procedure Iniciar(APorta: Word; const ASenhaAutomacao, ASenhaPdv: string;
      AComBombas: Boolean);
    procedure Parar;

    function Rodando: Boolean;
    function PortaEfetiva: Word;

    property Broker: TAMQPServer read FBroker;
    property Registro: TPostoRegistro read FRegistro;
    property Eventos: TPostoEventos read FEventos;
    property Vigia: TPostoVigia read FVigia;
    property Bombas: TPostoBombas read FBombas;

    property OnLog: TPostoLog read FOnLog write FOnLog;
    { Chamado (de threads variadas) quando algo muda -- a UI marca-se suja. }
    property OnMudou: TPostoAviso read FOnMudou write FOnMudou;
  end;

implementation

const
  SENHA_AUTOMACAO_PADRAO = 'automacao';

constructor TPostoServidorApp.Create;
begin
  inherited Create;
end;

destructor TPostoServidorApp.Destroy;
begin
  Parar;
  inherited Destroy;
end;

procedure TPostoServidorApp.Log(const AMsg: string);
begin
  if Assigned(FOnLog) then
    FOnLog(AMsg);
end;

function TPostoServidorApp.Rodando: Boolean;
begin
  Result := FRodando;
end;

function TPostoServidorApp.PortaEfetiva: Word;
begin
  if FBroker <> nil then
    Result := FBroker.Port
  else
    Result := 0;
end;

// Roda na thread do simulador de bombas.
procedure TPostoServidorApp.OnAbastecida(const A: TAbastecida);
var
  LVersao: Int64;
begin
  if FRegistro.Registrar(A, LVersao) then
  begin
    FEventos.Nova(A, LVersao);
    Log(Format('BOMBA %d/%d %s  %.3f L  R$ %.2f  (%s)  versao %d',
      [A.Bomba, A.Bico, A.Produto, A.Litros, A.ValorTotal,
       TipoParaStr(A.Tipo), LVersao]));
    if Assigned(FOnMudou) then
      FOnMudou();
  end;
end;

procedure TPostoServidorApp.Iniciar(APorta: Word; const ASenhaAutomacao,
  ASenhaPdv: string; AComBombas: Boolean);
var
  LParams: TAMQPConnectionParams;
  LSenhaAut: string;
begin
  if FRodando then
    Exit;
  LSenhaAut := ASenhaAutomacao;
  if LSenhaAut = '' then
    LSenhaAut := SENHA_AUTOMACAO_PADRAO;

  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '0.0.0.0';
  FBroker.Port := APorta;
  FBroker.Authenticator := TPostoAuthenticator.Create(LSenhaAut, ASenhaPdv);
  FBroker.Start;
  Log(Format('Broker no ar em 0.0.0.0:%d.', [FBroker.Port]));

  FRegistro := TPostoRegistro.Create;

  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := FBroker.Port;
  LParams.User := USER_AUTOMACAO;
  LParams.Password := LSenhaAut;
  LParams.ConnectionName := 'automacao';
  LParams.AutoReconnect := False;
  FConn := TAMQPConnection.Create(LParams);
  FConn.Open;

  FCanalEventos := FConn.CreateChannel;
  FCanalEventos.DeclareExchange(TAMQPExchangeDeclare.Create(EXCHANGE_EVENTOS,
    AMQP_EXCHANGE_TYPE_TOPIC, False));
  FEventos := TPostoEventos.Create(FCanalEventos);

  FCanalComandos := FConn.CreateChannel;
  FCanalComandos.DeclareQueue(TAMQPQueueDeclare.Create(FILA_COMANDOS, False));
  FComandos := TPostoComandos.Create(FCanalComandos, FRegistro, FEventos);
  FComandos.OnLog := FOnLog;
  FComandos.OnMudou := FOnMudou;
  FComandos.Iniciar;

  FVigia := TPostoVigia.Create(FBroker);
  FVigia.OnMudou := FOnMudou;
  FVigia.Iniciar;

  // A thread do simulador sobe sempre; comeca gerando ou pausada conforme
  // AComBombas. O painel liga/desliga a geracao pelo FBombas.DefinePausado.
  FBombas := TPostoBombas.Create(OnAbastecida);
  FBombas.DefinePausado(not AComBombas);
  FBombas.Start;

  FRodando := True;
  Log('Automacao pronta.');
end;

procedure TPostoServidorApp.Parar;
begin
  if FBombas <> nil then
  begin
    FBombas.Parar;
    FreeAndNil(FBombas);
  end;
  if FComandos <> nil then
  begin
    FComandos.Parar;
    FreeAndNil(FComandos);
  end;
  if FVigia <> nil then
  begin
    FVigia.Parar;
    FreeAndNil(FVigia);
  end;
  FreeAndNil(FEventos);
  FreeAndNil(FCanalComandos);
  FreeAndNil(FCanalEventos);
  FreeAndNil(FConn);
  FreeAndNil(FRegistro);
  if FBroker <> nil then
  begin
    FBroker.Stop;
    FreeAndNil(FBroker);
  end;
  if FRodando then
    Log('Automacao parada.');
  FRodando := False;
end;

end.
