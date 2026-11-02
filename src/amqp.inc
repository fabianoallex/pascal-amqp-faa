{ Diretivas de compilacao compartilhadas (dual Delphi / Free Pascal).

  Cada unit da lib inclui este arquivo logo apos o 'unit ...;'. No Delphi ele
  nao tem efeito; no FPC ativa o mode Delphi (strings longas, advanced records,
  generics estilo Delphi, type helpers).

  AMQP_WINDOWS: definido quando o alvo e' Windows, em qualquer compilador
  (Delphi define MSWINDOWS; FPC define WINDOWS).

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
