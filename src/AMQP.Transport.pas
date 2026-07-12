unit AMQP.Transport;

{$I amqp.inc}

{ Socket TCP cliente com a mesma superficie nos dois compiladores.

  TAMQPTcpSocket esconde a diferenca de RTL:
  - Delphi: System.Net.Socket (TSocket), como na lib delphi-amqp-faa original.
  - FPC: ssockets (TInetSocket), que resolve DNS e conecta no construtor.

  Contrato usado por AMQP.Connection:
  - Receive/Send bloqueantes; Receive devolve <= 0 em fim de stream/erro (o
    framing trata como conexao encerrada).
  - Close e' thread-safe no sentido que importa aqui: chamado por OUTRA thread
    (heartbeat/teardown) para desbloquear um Receive pendente da thread de
    leitura. No FPC usamos shutdown() — o handle so e' fechado no destrutor,
    evitando corrida de reuso de FD; no Delphi TSocket.Close ja tem esse papel
    (comportamento identico ao da lib original). Close pode ser chamado mais
    de uma vez. }

interface

uses
  SysUtils,
  Classes
  {$IFDEF FPC}
  , ssockets
  {$ELSE}
  , System.Net.Socket
  {$ENDIF}
  ;

type
  EAMQPTransport = class(Exception);

  { Erros da camada TLS (handshake, cifra, validação de cert). Declarada aqui —
    e não nas units de transporte TLS — para existir em TODA plataforma/build:
    AMQP.Transport.Tls só compila sob AMQP_WINDOWS e AMQP.Transport.OpenSSL só
    sob AMQP_OPENSSL, mas testes e chamadores precisam capturar EAMQPTls sem
    depender dessas diretivas. }
  EAMQPTls = class(Exception);

  TAMQPTcpSocket = class
  private
    {$IFDEF FPC}
    FSock: TInetSocket;
    FShutdown: Boolean;
    {$ELSE}
    FSock: TSocket;
    {$ENDIF}
  public
    constructor Create;
    destructor Destroy; override;
    /// Resolve o host e conecta (bloqueante). Levanta excecao em falha.
    procedure Connect(const AHost: string; APort: Word);
    /// Devolve os bytes lidos; 0 (ou negativo) = conexao encerrada.
    function Receive(var Buffer; ACount: Integer): Integer;
    function Send(const Buffer; ACount: Integer): Integer;
    /// Encerra a conexao, desbloqueando um Receive pendente em outra thread.
    procedure Close;
  end;

implementation

{$IFDEF FPC}
uses
  sockets;

const
  AMQP_SHUT_RDWR = 2;
{$ENDIF}

constructor TAMQPTcpSocket.Create;
begin
  inherited Create;
  {$IFNDEF FPC}
  FSock := TSocket.Create(TSocketType.TCP);
  {$ENDIF}
end;

destructor TAMQPTcpSocket.Destroy;
begin
  try
    Close;
  except
  end;
  FSock.Free;
  inherited;
end;

procedure TAMQPTcpSocket.Connect(const AHost: string; APort: Word);
begin
  {$IFDEF FPC}
  FSock := TInetSocket.Create(AHost, APort); // conecta no construtor
  {$ELSE}
  FSock.Connect(AHost, '', '', APort);
  {$ENDIF}
end;

function TAMQPTcpSocket.Receive(var Buffer; ACount: Integer): Integer;
begin
  if FSock = nil then
    Exit(0);
  {$IFDEF FPC}
  Result := FSock.Read(Buffer, ACount);
  {$ELSE}
  Result := FSock.Receive(Buffer, ACount);
  {$ENDIF}
end;

function TAMQPTcpSocket.Send(const Buffer; ACount: Integer): Integer;
begin
  if FSock = nil then
    raise EAMQPTransport.Create('socket nao conectado');
  {$IF Defined(FPC) and Defined(UNIX) and Declared(MSG_NOSIGNAL)}
  // MSG_NOSIGNAL: send() num socket ja encerrado devolve erro (EPIPE) em vez
  // de matar o processo com SIGPIPE. Essencial pro TLS: o destrutor do stream
  // manda close_notify best-effort mesmo quando o socket ja foi derrubado
  // (teardown/reconexao) — no Windows isso e' so um erro de send engolido;
  // no Linux, sem esta flag, era SIGPIPE fatal (visto no smoke test --tls).
  // Vale pra qualquer Unix cuja unit sockets declare a flag (Linux, BSDs);
  // no Darwin (sem MSG_NOSIGNAL) fica o caminho comum, sujeito a SIGPIPE.
  Result := fpsend(FSock.Handle, @Buffer, ACount, MSG_NOSIGNAL);
  {$ELSE}
    {$IFDEF FPC}
  Result := FSock.Write(Buffer, ACount);
    {$ELSE}
  Result := FSock.Send(Buffer, ACount);
    {$ENDIF}
  {$ENDIF}
end;

procedure TAMQPTcpSocket.Close;
begin
  if FSock = nil then
    Exit;
  {$IFDEF FPC}
  if not FShutdown then
  begin
    FShutdown := True;
    fpshutdown(FSock.Handle, AMQP_SHUT_RDWR);
  end;
  {$ELSE}
  if TSocketState.Connected in FSock.State then
    FSock.Close;
  {$ENDIF}
end;

end.
