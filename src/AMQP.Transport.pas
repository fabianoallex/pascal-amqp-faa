unit AMQP.Transport;

{$I amqp.inc}

{ The library's transport: what is left here since TCP and TLS come from
  pascal-common-faa 1.27.0 (0.6.0; step 5 of its docs/transport-design.md).

  Up to 0.5.x this unit had the library's own sockets (TAMQPTcpSocket and
  TAMQPTcpListener over ssockets on FPC and System.Net.Socket on Delphi) and
  the two TLS units had their own SChannel and OpenSSL streams. Today:

  - The client connects with PcTcpConnect/PcTlsConnect and the broker listens
    with PcTcpListen/PcTlsListen. Both read and write through a
    TAMQPEndpointStream, declared below.
  - TAMQPEndpointStream is the EXCEPTION BOUNDARY (decision T6 there): no
    EPcTransport* leaves the library. A transport failure becomes
    EAMQPTransport and a TLS failure EAMQPTls, with the common library's
    message (in English). The peer closing the connection is not an error
    for a frame reader: Read returns 0, which AMQP.Frame turns into
    EAMQPFrameEOF, as the old socket stream did.
  - The TLS backend is chosen at RUN TIME (decision T3): both backends are in
    every Windows build, OpenSSL is the only one elsewhere. AMQP_OPENSSL only
    changes the DEFAULT to OpenSSL (see AmqpDefaultTlsBackend).
  - No socket deadline: AMQP has heartbeats, so reads and writes wait with no
    timeout (SetIoDeadline and SetRead/WriteDeadline are never called and
    start at 0). The only deadlines are the connect's (ConnectTimeoutMs) and
    the TLS handshake's (the common's default, 15 s), both over before the
    first AMQP byte.
  - TCP_NODELAY on every socket, client and accepted: a zeroed TPcTcpOptions
    already asks for it (see AmqpTcpOptions). }

interface

uses
  SysUtils,
  Classes,
  PascalCommon.Transport;

type
  /// Plain socket failure (connect, send, receive) outside TLS: typically an
  /// unreachable host or port, or a connection dropped by the peer.
  EAMQPTransport = class(Exception);

  { TLS failure: handshake, certificate refused (TlsVerifyPeer, TlsCaFile,
    mTLS), libssl/libcrypto not found at run time, SChannel asked for off
    Windows, a server certificate that doesn't load. Deliberately NOT a
    subclass of EAMQPTransport: an "except on EAMQPTransport" written before
    0.6.0 must not start catching TLS errors. }
  EAMQPTls = class(Exception);

  { TPcEndpointStream that translates the common's exceptions into the
    library's (see the header). Doesn't own the endpoint. Shared by the
    client (AMQP.Connection) and the broker (AMQP.Server.*). }
  TAMQPEndpointStream = class(TPcEndpointStream)
  public
    /// Returns 0 when the peer closed the connection or the endpoint was
    /// closed by CloseAbort; raises EAMQPTransport/EAMQPTls otherwise.
    function Read(var Buffer; Count: Longint): Longint; override;
    /// Writes everything or raises EAMQPTransport/EAMQPTls.
    function Write(const Buffer; Count: Longint): Longint; override;
  end;

/// Raises the library's exception for a common transport exception: EPcTls
/// becomes EAMQPTls, every other EPcTransport becomes EAMQPTransport, with
/// the same message.
procedure AmqpRaiseTransportError(E: EPcTransport);

/// The TCP options the client and the broker use: TCP_NODELAY (the zeroed
/// record's default; AMQP is request-reply with small frames, and Nagle's
/// delay costs ~40 ms per round trip on Linux), no keep-alive (AMQP has
/// heartbeats), and the connect budget (0 = the common's default, 10 s).
function AmqpTcpOptions(AConnectTimeoutMs: Cardinal = 0): TPcTcpOptions;

/// What tbDefault means in THIS build: tbOpenSsl with -dAMQP_OPENSSL,
/// otherwise the platform's default (SChannel on Windows, OpenSSL elsewhere).
function AmqpDefaultTlsBackend: TPcTlsBackend;

/// tbDefault resolved as AmqpDefaultTlsBackend; any other value as is.
function AmqpResolveTlsBackend(ABackend: TPcTlsBackend): TPcTlsBackend;

/// Name of this build's default TLS backend: 'SChannel' or 'OpenSSL'. Useful
/// to show in a UI or a log which engine a build uses.
function AmqpTlsBackendName: string; overload;
/// Name of a given backend (tbDefault resolved as above).
function AmqpTlsBackendName(ABackend: TPcTlsBackend): string; overload;

/// Like AmqpTlsBackendName, with the run-time detail: on OpenSSL the
/// version and library loaded (after the first TLS connection), on SChannel
/// its fixed description. Before OpenSSL loads, just the name.
function AmqpTlsBackendInfo: string; overload;
function AmqpTlsBackendInfo(ABackend: TPcTlsBackend): string; overload;

implementation

uses
  PascalCommon.Transport.Tls;

procedure AmqpRaiseTransportError(E: EPcTransport);
begin
  if E is EPcTls then
    raise EAMQPTls.Create(E.Message);
  raise EAMQPTransport.Create(E.Message);
end;

{ TAMQPEndpointStream }

function TAMQPEndpointStream.Read(var Buffer; Count: Longint): Longint;
begin
  try
    Result := inherited Read(Buffer, Count);
  except
    on E: EPcTransportClosed do
      Result := 0; // end of stream: the frame reader raises EAMQPFrameEOF
    on E: EPcTransport do
    begin
      AmqpRaiseTransportError(E);
      Result := 0; // not reached
    end;
  end;
end;

function TAMQPEndpointStream.Write(const Buffer; Count: Longint): Longint;
begin
  try
    Result := inherited Write(Buffer, Count);
  except
    on E: EPcTransport do
    begin
      AmqpRaiseTransportError(E);
      Result := 0; // not reached
    end;
  end;
end;

function AmqpTcpOptions(AConnectTimeoutMs: Cardinal): TPcTcpOptions;
begin
  // A zeroed record is the common's recommended one: TCP_NODELAY (Nagle =
  // False), no keep-alive. Only the connect budget comes from the caller.
  FillChar(Result, SizeOf(Result), 0);
  Result.ConnectTimeoutMs := AConnectTimeoutMs;
end;

function AmqpDefaultTlsBackend: TPcTlsBackend;
begin
  {$IFDEF AMQP_OPENSSL}
  Result := tbOpenSsl;
  {$ELSE}
    {$IFDEF AMQP_WINDOWS}
  Result := tbSChannel;
    {$ELSE}
  Result := tbOpenSsl;
    {$ENDIF}
  {$ENDIF}
end;

function AmqpResolveTlsBackend(ABackend: TPcTlsBackend): TPcTlsBackend;
begin
  if ABackend = tbDefault then
    Result := AmqpDefaultTlsBackend
  else
    Result := ABackend;
end;

function AmqpTlsBackendName: string;
begin
  Result := AmqpTlsBackendName(tbDefault);
end;

function AmqpTlsBackendName(ABackend: TPcTlsBackend): string;
begin
  Result := PcTlsBackendName(AmqpResolveTlsBackend(ABackend));
end;

function AmqpTlsBackendInfo: string;
begin
  Result := AmqpTlsBackendInfo(tbDefault);
end;

function AmqpTlsBackendInfo(ABackend: TPcTlsBackend): string;
begin
  Result := PcTlsBackendInfo(AmqpResolveTlsBackend(ABackend));
  if Result = '' then
    Result := AmqpTlsBackendName(ABackend);
end;

end.
