{ Diretivas de compilacao compartilhadas (dual Delphi / Free Pascal).

  Cada unit da lib inclui este arquivo logo apos o 'unit ...;'. No Delphi ele
  nao tem efeito; no FPC ativa o mode Delphi (strings longas, advanced records,
  generics estilo Delphi, type helpers).

  AMQP_WINDOWS: definido quando o alvo e' Windows, em qualquer compilador
  (Delphi define MSWINDOWS; FPC define WINDOWS). Usado para o transporte TLS
  via SChannel, que e' exclusivo de Windows. }

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
