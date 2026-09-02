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
  /// Erro de socket plain (conectar, enviar, receber) fora do TLS —
  /// tipicamente host/porta inalcançável ou conexão derrubada pelo peer.
  EAMQPTransport = class(Exception);

  { Erros da camada TLS (handshake, cifra, validação de cert). Declarada aqui —
    e não nas units de transporte TLS — para existir em TODA plataforma/build:
    AMQP.Transport.Tls só compila sob AMQP_WINDOWS e AMQP.Transport.OpenSSL só
    sob AMQP_OPENSSL, mas testes e chamadores precisam capturar EAMQPTls sem
    depender dessas diretivas. Também é a exceção levantada por UseTls quando
    nenhum backend TLS foi compilado (fora do Windows, sem -dAMQP_OPENSSL) —
    a mensagem explica como habilitar. Nos backends reais: falha de
    handshake, cert rejeitado (TlsVerifyPeer=True) ou libssl/libcrypto não
    encontrada em runtime. }
  EAMQPTls = class(Exception);

  TAMQPTcpSocket = class
  private
    FPeerAddress: string;
    {$IFDEF FPC}
    FSock: TInetSocket;    // caminho cliente (Connect)
    FRawFd: LongInt;       // caminho aceito (>= 0); -1 quando usa FSock
    FShutdown: Boolean;
    {$ELSE}
    FSock: TSocket;
    {$ENDIF}
  public
    constructor Create;
    {$IFDEF FPC}
    /// Envolve um descritor JÁ conectado (vindo de TAMQPTcpListener.Accept).
    constructor CreateFromAccepted(AHandle: LongInt; const APeerAddr: string = '');
    {$ELSE}
    constructor CreateFromAccepted(ASock: TSocket; const APeerAddr: string = '');
    {$ENDIF}
    destructor Destroy; override;
    /// 'ip:porta' do outro lado quando o socket veio de um Accept; '' senão.
    property PeerAddress: string read FPeerAddress;
    /// Resolve o host e conecta (bloqueante). Levanta excecao em falha.
    procedure Connect(const AHost: string; APort: Word);
    /// Devolve os bytes lidos; 0 (ou negativo) = conexao encerrada.
    function Receive(var Buffer; ACount: Integer): Integer;
    function Send(const Buffer; ACount: Integer): Integer;
    /// Encerra a conexao, desbloqueando um Receive pendente em outra thread.
    procedure Close;
  end;

  { Socket de escuta TCP (lado servidor do sub-módulo broker). Mesma superfície
    nos dois compiladores:
    - FPC: sockets crus (fpsocket/fpbind/fplisten/fpaccept) — evita o modelo de
      callback do ssockets.TInetServer.
    - Delphi: System.Net.Socket (TSocket.Listen/Accept).

    Contrato:
    - Listen liga e começa a escutar; levanta EAMQPTransport em falha.
    - Accept bloqueia até uma conexão chegar e devolve um TAMQPTcpSocket novo
      (dono do descritor aceito), ou nil se o listener foi fechado por outra
      thread (shutdown do broker).
    - Close pode ser chamado de OUTRA thread para desbloquear um Accept
      pendente; pode ser chamado mais de uma vez.
    - Port devolve a porta efetiva (útil ao ligar na porta 0 nos testes). }
  TAMQPTcpListener = class
  private
    FClosing: Boolean;
    FPort: Word;
    {$IFDEF FPC}
    FListenFd: LongInt;
    {$ELSE}
    FListen: TSocket;
    {$ENDIF}
  public
    constructor Create;
    destructor Destroy; override;
    /// ABindAddr: '' ou '0.0.0.0' => todas as interfaces. APort 0 => o SO
    /// escolhe (ver Port depois de Listen).
    procedure Listen(const ABindAddr: string; APort: Word; ABacklog: Integer = 64);
    function Accept: TAMQPTcpSocket;
    procedure Close;
    property Port: Word read FPort;
  end;

  { Adapta um TAMQPTcpSocket como TStream, para os frames trafegarem por
    TAMQPFrame.ReadFrom/WriteTo. Nao e' dono do socket. Compartilhado pelo
    cliente (AMQP.Connection) e pelo broker (AMQP.Server.*); os backends TLS
    (AMQP.Transport.Tls/.OpenSSL) envolvem uma instancia destas. }
  TAMQPSocketStream = class(TStream)
  private
    FSocket: TAMQPTcpSocket;
  public
    constructor Create(ASocket: TAMQPTcpSocket);
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  end;

/// Backend TLS deste build (decidido em compilacao): 'OpenSSL', 'SChannel'
/// ou 'nenhum'. Util para exibir em UI/log qual motor um build usa.
function AmqpTlsBackendName: string;

/// Como AmqpTlsBackendName, mas com o detalhe de runtime quando o backend ja
/// carregou — o OpenSSL publica versao e biblioteca na 1ª conexao TLS (ex.:
/// 'OpenSSL 3.5.2 ... (libssl-3.dll)'). Antes disso, devolve so o nome.
function AmqpTlsBackendInfo: string;

/// Uso interno dos backends TLS: publica o detalhe de AmqpTlsBackendInfo.
procedure AmqpSetTlsBackendDetail(const ADetail: string);

implementation

{$IFDEF FPC}
uses
  sockets;

const
  AMQP_SHUT_RDWR = 2;
{$ENDIF}

var
  // Escrito uma vez pelo backend ao carregar (antes de qualquer leitura util:
  // so ha detalhe DEPOIS de uma conexao TLS abrir).
  GTlsBackendDetail: string = '';

function AmqpTlsBackendName: string;
begin
  {$IFDEF AMQP_OPENSSL}
  Result := 'OpenSSL';
  {$ELSE}
    {$IFDEF AMQP_WINDOWS}
  Result := 'SChannel';
    {$ELSE}
  Result := 'nenhum';
    {$ENDIF}
  {$ENDIF}
end;

function AmqpTlsBackendInfo: string;
begin
  Result := GTlsBackendDetail;
  if Result = '' then
    Result := AmqpTlsBackendName;
end;

procedure AmqpSetTlsBackendDetail(const ADetail: string);
begin
  GTlsBackendDetail := ADetail;
end;

constructor TAMQPTcpSocket.Create;
begin
  inherited Create;
  {$IFDEF FPC}
  FRawFd := -1;
  {$ELSE}
  FSock := TSocket.Create(TSocketType.TCP);
  {$ENDIF}
end;

{$IFDEF FPC}
constructor TAMQPTcpSocket.CreateFromAccepted(AHandle: LongInt; const APeerAddr: string);
begin
  inherited Create;
  FPeerAddress := APeerAddr;
  // Descritor cru: controlamos recv/send/close diretamente. (TInetSocket.Create
  // sobre um handle não roteia o Read por onde o shutdown/close desbloqueia.)
  FRawFd := AHandle;
end;
{$ELSE}
constructor TAMQPTcpSocket.CreateFromAccepted(ASock: TSocket; const APeerAddr: string);
begin
  inherited Create;
  FPeerAddress := APeerAddr;
  FSock := ASock;
end;
{$ENDIF}

destructor TAMQPTcpSocket.Destroy;
begin
  try
    Close;
  except
  end;
  {$IFDEF FPC}
  if FRawFd >= 0 then
  begin
    CloseSocket(FRawFd); // no-op se Close já fechou no Windows
    FRawFd := -1;
  end;
  {$ENDIF}
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
  {$IFDEF FPC}
  if FRawFd >= 0 then
    Exit(fprecv(FRawFd, @Buffer, ACount, 0));
  if FSock = nil then
    Exit(0);
  Result := FSock.Read(Buffer, ACount);
  {$ELSE}
  if FSock = nil then
    Exit(0);
  Result := FSock.Receive(Buffer, ACount);
  {$ENDIF}
end;

function TAMQPTcpSocket.Send(const Buffer; ACount: Integer): Integer;
{$IFDEF FPC}
var
  LFd: LongInt;
begin
  if FRawFd >= 0 then
    LFd := FRawFd
  else if FSock <> nil then
    LFd := FSock.Handle
  else
    raise EAMQPTransport.Create('socket nao conectado');
  {$IF Defined(UNIX) and Declared(MSG_NOSIGNAL)}
  // MSG_NOSIGNAL: send() num socket ja encerrado devolve erro (EPIPE) em vez
  // de matar o processo com SIGPIPE (ver smoke test --tls no Linux).
  Result := fpsend(LFd, @Buffer, ACount, MSG_NOSIGNAL);
  {$ELSE}
  Result := fpsend(LFd, @Buffer, ACount, 0);
  {$ENDIF}
end;
{$ELSE}
begin
  if FSock = nil then
    raise EAMQPTransport.Create('socket nao conectado');
  Result := FSock.Send(Buffer, ACount);
end;
{$ENDIF}

procedure TAMQPTcpSocket.Close;
begin
  {$IFDEF FPC}
  if FRawFd >= 0 then
  begin
    // Socket aceito: no Windows só o closesocket() desbloqueia um recv()
    // pendente (shutdown num socket destes NÃO desbloqueia — medido). No Unix
    // shutdown() basta e evita a corrida de reuso de FD; o handle fecha no
    // destrutor.
    {$IFDEF UNIX}
    if not FShutdown then
    begin
      FShutdown := True;
      fpshutdown(FRawFd, AMQP_SHUT_RDWR);
    end;
    {$ELSE}
    if not FShutdown then
    begin
      FShutdown := True;
      CloseSocket(FRawFd);
      FRawFd := -1; // destrutor não re-fecha
    end;
    {$ENDIF}
    Exit;
  end;
  if FSock = nil then
    Exit;
  if not FShutdown then
  begin
    FShutdown := True;
    fpshutdown(FSock.Handle, AMQP_SHUT_RDWR);
  end;
  {$ELSE}
  if FSock = nil then
    Exit;
  if TSocketState.Connected in FSock.State then
    FSock.Close;
  {$ENDIF}
end;

{ TAMQPTcpListener }

constructor TAMQPTcpListener.Create;
begin
  inherited Create;
  {$IFDEF FPC}
  FListenFd := -1;
  {$ENDIF}
end;

destructor TAMQPTcpListener.Destroy;
begin
  try
    Close;
  except
  end;
  {$IFDEF FPC}
  if FListenFd >= 0 then
  begin
    CloseSocket(FListenFd);
    FListenFd := -1;
  end;
  {$ELSE}
  FListen.Free;
  {$ENDIF}
  inherited;
end;

procedure TAMQPTcpListener.Listen(const ABindAddr: string; APort: Word;
  ABacklog: Integer);
{$IFDEF FPC}
var
  LAddr: TInetSockAddr;
  LLen: {$IF Declared(TSockLen)}TSockLen{$ELSE}LongInt{$ENDIF};
  LOpt: LongInt;
begin
  FListenFd := fpSocket(AF_INET, SOCK_STREAM, 0);
  if FListenFd < 0 then
    raise EAMQPTransport.CreateFmt('fpSocket falhou (%d)', [SocketError]);

  LOpt := 1;
  fpSetSockOpt(FListenFd, SOL_SOCKET, SO_REUSEADDR, @LOpt, SizeOf(LOpt));

  FillChar(LAddr, SizeOf(LAddr), 0);
  LAddr.sin_family := AF_INET;
  LAddr.sin_port := htons(APort);
  if (ABindAddr = '') or (ABindAddr = '0.0.0.0') then
    LAddr.sin_addr.s_addr := 0 // INADDR_ANY
  else
    LAddr.sin_addr := StrToNetAddr(ABindAddr); // já em ordem de rede

  if fpBind(FListenFd, @LAddr, SizeOf(LAddr)) <> 0 then
    raise EAMQPTransport.CreateFmt('bind em %s:%d falhou (%d)',
      [ABindAddr, APort, SocketError]);

  if fpListen(FListenFd, ABacklog) <> 0 then
    raise EAMQPTransport.CreateFmt('listen falhou (%d)', [SocketError]);

  // Porta efetiva (relevante quando APort = 0).
  LLen := SizeOf(LAddr);
  if fpGetSockName(FListenFd, @LAddr, @LLen) = 0 then
    FPort := ntohs(LAddr.sin_port)
  else
    FPort := APort;
end;
{$ELSE}
begin
  if FListen = nil then
    FListen := TSocket.Create(TSocketType.TCP);
  FListen.Listen(ABindAddr, '', '', APort, ABacklog);
  FPort := APort;
  try
    if APort = 0 then
      FPort := Word(FListen.LocalPort);
  except
  end;
end;
{$ENDIF}

function TAMQPTcpListener.Accept: TAMQPTcpSocket;
{$IFDEF FPC}
var
  LClient: TInetSockAddr;
  LLen: {$IF Declared(TSockLen)}TSockLen{$ELSE}LongInt{$ENDIF};
  LFd: LongInt;
begin
  Result := nil;
  if FClosing or (FListenFd < 0) then
    Exit;
  LLen := SizeOf(LClient);
  LFd := fpAccept(FListenFd, @LClient, @LLen);
  if LFd < 0 then
    Exit; // listener fechado / interrompido
  Result := TAMQPTcpSocket.CreateFromAccepted(LFd,
    NetAddrToStr(LClient.sin_addr) + ':' + IntToStr(ntohs(LClient.sin_port)));
end;
{$ELSE}
var
  LSock: TSocket;
  LPeer: string;
begin
  Result := nil;
  if FClosing or (FListen = nil) then
    Exit;
  try
    LSock := FListen.Accept;
  except
    Exit; // listener fechado por outra thread
  end;
  if LSock = nil then
    Exit;
  LPeer := '';
  try
    LPeer := LSock.RemoteEndpoint.Address.Address + ':' +
      IntToStr(LSock.RemoteEndpoint.Port);
  except
  end;
  Result := TAMQPTcpSocket.CreateFromAccepted(LSock, LPeer);
end;
{$ENDIF}

procedure TAMQPTcpListener.Close;
begin
  if FClosing then
    Exit;
  FClosing := True;
  {$IFDEF FPC}
    {$IFDEF UNIX}
  // No Linux/BSD, shutdown() num socket de escuta desbloqueia o accept()
  // pendente; o descritor só é fechado no destrutor (evita corrida de FD).
  if FListenFd >= 0 then
    fpShutdown(FListenFd, AMQP_SHUT_RDWR);
    {$ELSE}
  // No Windows, é o closesocket() que desbloqueia um accept() pendente
  // (shutdown num listen socket devolve WSAEINVAL).
  if FListenFd >= 0 then
  begin
    CloseSocket(FListenFd);
    FListenFd := -1;
  end;
    {$ENDIF}
  {$ELSE}
  if FListen <> nil then
    FListen.Close;
  {$ENDIF}
end;

{ TAMQPSocketStream }

constructor TAMQPSocketStream.Create(ASocket: TAMQPTcpSocket);
begin
  inherited Create;
  FSocket := ASocket;
end;

function TAMQPSocketStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Receive(Buffer, Count);
end;

function TAMQPSocketStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Send(Buffer, Count);
end;

function TAMQPSocketStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  raise EAMQPTransport.Create('TAMQPSocketStream nao suporta Seek');
end;

end.
