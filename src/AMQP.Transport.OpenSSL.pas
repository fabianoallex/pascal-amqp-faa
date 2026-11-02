unit AMQP.Transport.OpenSSL;

{$I amqp.inc}

{ OBSOLETE since 0.6.0; removed in the next minor version.

  The OpenSSL backend moved to pascal-common-faa
  (PascalCommon.Transport.OpenSSL) with the rest of TLS (step 5 of its
  docs/transport-design.md). This unit stays only so that a uses clause or a
  .dproj that lists it keeps compiling (it just pulls the new unit; Pascal
  doesn't re-export a used unit's symbols). TAMQPOpenSslStream has no public
  successor: the client uses PcTlsConnect and the broker PcTlsListen. The
  backend is chosen at run time (TAMQPConnectionParams.TlsBackend,
  TAMQPServer.TlsBackend); -dAMQP_OPENSSL now only makes OpenSSL the
  default. }

interface

uses
  PascalCommon.Transport.OpenSSL;

implementation

end.
