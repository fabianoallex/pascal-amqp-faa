unit AMQP.Transport.Tls;

{$I amqp.inc}

{ OBSOLETE since 0.6.0; removed in the next minor version.

  The SChannel backend moved to pascal-common-faa
  (PascalCommon.Transport.SChannel) with the rest of TLS (step 5 of its
  docs/transport-design.md). This unit stays only so that a uses clause or a
  .dproj that lists it keeps compiling (it just pulls the new unit; Pascal
  doesn't re-export a used unit's symbols). TAMQPSchannelStream has no public
  successor: AMQP.Connection uses PcTlsConnect. To pick the backend, set
  TAMQPConnectionParams.TlsBackend := tbSChannel. }

interface

{$IFDEF AMQP_WINDOWS}
uses
  PascalCommon.Transport.SChannel;
{$ENDIF}

implementation

end.
