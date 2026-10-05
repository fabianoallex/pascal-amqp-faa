unit AMQP.Threading;

{$I amqp.inc}

{ O que sobrou da camada de concorrencia propria da lib: o relogio de parede
  (AmqpWallMs) e a checagem da versao minima da pascal-common-faa.

  Atomics, PcTickMs, o monitor (TPcMonitor) e o pool de threads (TPcThreadPool,
  PcPool) vivem na pascal-common-faa desde a migracao F8 (PascalCommon.Threading
  e PascalCommon.ThreadPool). Eram copia identica, linha a linha, da mesma
  origem que pascal-named-pipes-faa e pascal-redis-faa carregavam.

  Esta unit continua no uses de AMQP.Connection e de AMQP.Server.Engine mesmo
  onde AmqpWallMs nao e' usado: e' por ela que todo usuario do cliente ou do
  broker compila a checagem de versao abaixo. Uma pascal-common-faa mais velha
  que a minima para o build com a mensagem do $MESSAGE, em vez de falhar adiante
  com identificador nao encontrado.

  O PcPool e' UM pool para o processo inteiro, dividido com qualquer outra lib
  que o use, e e' liberado DEPOIS de todas as units da amqp (na finalizacao de
  PascalCommon.ThreadPool). O broker NAO roda seus atores nele: ver
  TAMQPServer.Create em AMQP.Server.Broker. }

interface

uses
  SysUtils,
  DateUtils,
  PascalCommon.Version;

// 1.1.3: antes dela o PcPool nao crescia numa rajada de itens (com 1 worker
// ocioso, 17 callbacks que bloqueiam rodavam um de cada vez) -- achado aqui,
// ver .ci/findings-for-pascal-common-faa.md. Os callbacks do cliente rodam
// no PcPool, entao uma copia mais velha e' defeito em producao, nao so' teste.
{$IF PASCALCOMMON_VERSION < 10103}
  {$MESSAGE FATAL 'pascal-amqp-faa precisa da pascal-common-faa 1.1.3 ou mais nova'}
{$IFEND}

/// Milissegundos de RELOGIO DE PAREDE desde a epoch Unix, em UTC.
///
/// Existe para a UNICA fronteira em que um instante precisa sobreviver ao
/// processo: o journal de durabilidade (decisao D21 da Fase 4 do broker).
/// Monotonico na memoria, parede no disco -- gravar PcTickMs seria gravar
/// coordenada de um referencial que nao existe depois de um restart, e ler o
/// relogio de parede em regime normal exporia todo prazo vivo a um acerto de
/// NTP.
///
/// A FORMA IMPORTA, e foi medida (WS0 da Fase 4). O caminho obvio --
/// DateTimeToUnix(LocalTimeToUniversal(Now), False) -- converte DUAS vezes: o
/// False significa "a entrada e' local, converta", e a entrada ja' tinha sido
/// convertida na mao. Erra em uma hora inteira por fuso... e some numa maquina
/// com fuso UTC, que e' todo container e a maioria dos servidores Linux, entao
/// o defeito nao aparece justamente onde a suite roda.
///
/// Daqui sai a escolha desta expressao: ela nao chama DateTimeToUnix, entao
/// nao depende da semantica do flag (que difere entre os compiladores) nem de
/// a RTL zerar os milissegundos antes do Round interno (o FPC zera, via
/// RecodeMillisecond; o Delphi nao foi verificado). UnixDateDelta vale 25569
/// nos dois.
function AmqpWallMs: Int64;

implementation

function AmqpWallMs: Int64;
var
  LUtc: TDateTime;
begin
  {$IFDEF FPC}
  LUtc := LocalTimeToUniversal(Now);
  {$ELSE}
  LUtc := TTimeZone.Local.ToUniversalTime(Now);
  {$ENDIF}
  Result := Round((LUtc - UnixDateDelta) * Int64(86400000));
end;

end.
