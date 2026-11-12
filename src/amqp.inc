{ Shared compiler directives (dual Delphi / Free Pascal).

  Every unit of the lib includes this file right after 'unit ...;'. On Delphi it
  has no effect; on FPC it enables Delphi mode (long strings, advanced records,
  Delphi-style generics, type helpers).

  AMQP_WINDOWS: defined when the target is Windows, on either compiler
  (Delphi defines MSWINDOWS; FPC defines WINDOWS).

  AMQP_OPENSSL: opt-in (-dAMQP_OPENSSL). Since 0.6.0 it only makes OpenSSL
  the DEFAULT TLS backend (tbDefault); both backends come from
  pascal-common-faa and are chosen at run time. See AmqpDefaultTlsBackend
  in AMQP.Transport. }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ENDIF}

{$IFDEF MSWINDOWS}
  {$DEFINE AMQP_WINDOWS}
{$ENDIF}
{$IFDEF WINDOWS}
  {$IFNDEF AMQP_WINDOWS}
    {$DEFINE AMQP_WINDOWS}
  {$ENDIF}
{$ENDIF}
