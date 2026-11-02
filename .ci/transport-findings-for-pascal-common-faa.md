# pascal-amqp-faa 0.6.0 (transport) — findings for pascal-common-faa

Found on 2026-11-02 while moving amqp's client and broker onto the 1.27.0 transport (step 5 of
`docs/transport-design.md` there). Nothing in pascal-common-faa was changed; by the rule of the
major, anything here becomes a new version there, not a fix on a branch. Most important first.
The earlier findings are in `f8-findings-for-pascal-common-faa.md` and
`findings-for-pascal-common-faa.md`.

## 1. SChannel server: a client that isn't speaking TLS waits out the whole handshake deadline

**What happened.** amqp's broker test `ClientePlainNaPortaTls_Falha` connects a plain AMQP client
to the broker's TLS port. The client sends the 8-byte protocol-header (`AMQP\0\0\9\1`) and waits
for `Connection.Start`. With an **OpenSSL** server the handshake fails at once and the connection
drops (0.04 s). With an **SChannel** server it takes **15.08 s**, the default handshake deadline:
`AcceptSecurityContext` answers `SEC_E_INCOMPLETE_MESSAGE` for 8 bytes that can't be a whole
record, and the server keeps reading until the deadline. Measured on FPC 3.2.2 Win64, both the
server-SChannel fixtures (`tbDefault`, and OpenSSL client against SChannel server).

**Why it matters.** It isn't wrong (the deadline is what bounds it), but a plaintext client on a
TLS port is the most common TLS misconfiguration there is, and on one backend the answer comes in
15 s and on the other at once.

**Possible fix (a new version there).** The first byte of a TLS connection from a client is a
handshake record, `0x16`. The SChannel server session could check it before calling
`AcceptSecurityContext` and raise `EPcTls` ("not a TLS client hello") at once, as OpenSSL does.
Worth a loopback test on both backends: plain bytes to a TLS listener fail in well under the
deadline.

## 2. No way to read TCP_NODELAY back from an endpoint (no action needed)

amqp had a test reading `TCP_NODELAY` back from its client and accepted sockets
(`Sockets_ClienteEAceito_TemNoDelayLigado`), the guard for its Linux gotcha (the server suite
went from 50 s to 13 min without it). A `TPcEndpoint` has no handle and no getter, and the common
has no test of its own that the option is on. amqp now has two tests instead: the options it
passes (`AmqpTcpOptions.Nagle = False`) and a round-trip latency test over `PcTcpListen`/
`PcTcpConnect` (20 rounds of write-write-read each way must take under 400 ms; with Nagle on
Linux they take ~40 ms each). The second is the one that would catch a regression in the common;
it might belong there too.

## 3. The listen backlog isn't configurable (no action unless someone asks)

amqp's `TAMQPServer.Backlog` (default 64) has no counterpart in `TPcTcpOptions`; the listener uses
`PC_LISTEN_BACKLOG`. amqp keeps the property, documented as ignored. No consumer has asked for it.

## 4. Things that worked as documented (no action)

- `TPcEndpointStream` subclassed as the exception boundary (T6): `TAMQPEndpointStream` turns
  `EPcTransportClosed` on read into a 0-byte read (amqp's frame reader raises its own EOF from
  that, exactly as with the old socket stream), and every other `EPcTransport` into the library's
  classes. A connection reset is `EPcTransportClosed` on both platforms, so the broker still sees
  a peer that drops as a normal close.
- Server TLS in `Handshake`, on the connection's thread; `CloseAbort` from the broker's `Stop`
  unblocks a handshake in progress.
- Port 0 and `TPcListener.Port` (T1): every amqp suite starts its brokers that way.
- The `tests/pki` through the submodule: amqp's test runners look it up under
  `external/pascal-common-faa/tests/pki` instead of keeping a copy.
