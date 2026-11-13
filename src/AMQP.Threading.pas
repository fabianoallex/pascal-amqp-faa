unit AMQP.Threading;

{$I amqp.inc}

{ What is left of the lib's own concurrency layer: the wall clock
  (AmqpWallMs) and the pascal-common-faa minimum-version check.

  Atomics, PcTickMs, the monitor (TPcMonitor) and the thread pool (TPcThreadPool,
  PcPool) live in pascal-common-faa since the F8 migration (PascalCommon.Threading
  and PascalCommon.ThreadPool). They were an identical copy, line by line, of the
  same source that pascal-named-pipes-faa and pascal-redis-faa carried.

  This unit stays in the uses of AMQP.Connection and AMQP.Server.Engine even
  where AmqpWallMs is not used: it is through it that every user of the client or
  the broker compiles the version check below. A pascal-common-faa older than
  the minimum stops the build with the $MESSAGE text, instead of failing later
  with an identifier not found.

  PcPool is ONE pool for the whole process, shared with any other lib
  that uses it, and it is freed AFTER every amqp unit (in the finalization of
  PascalCommon.ThreadPool). The broker does NOT run its actors on it: see
  TAMQPServer.Create in AMQP.Server.Broker. }

interface

uses
  SysUtils,
  DateUtils,
  PascalCommon.Version;

// 1.1.3: before it PcPool did not grow on a burst of items (with 1 idle
// worker, 17 blocking callbacks ran one at a time) -- found here,
// see .ci/findings-for-pascal-common-faa.md. The client's callbacks run
// on PcPool, so an older copy is a production defect, not just a test one.
// 1.8.0: PascalCommon.Tracing (publish and consume spans, AMQP.Tracing). 1.7
// already had what is used here, but 1.8 is what the sibling libs require -- an
// application does not end up with two different requirements for the same package.
// 1.27.0: the TCP and TLS transport (PascalCommon.Transport*), which the
// client and the broker use since 0.6.0.
{$IF PASCALCOMMON_VERSION < 12700}
  {$MESSAGE FATAL 'pascal-amqp-faa needs pascal-common-faa 1.27.0 or newer'}
{$IFEND}

/// WALL-CLOCK milliseconds since the Unix epoch, in UTC.
///
/// Exists for the ONLY boundary where an instant must outlive the
/// process: the durability journal (decision D21 of the broker's Phase 4).
/// Monotonic in memory, wall clock on disk -- writing PcTickMs would write a
/// coordinate of a reference frame that does not exist after a restart, and
/// reading the wall clock in normal operation would expose every live deadline
/// to an NTP adjustment.
///
/// THE FORM MATTERS, and it was measured (WS0 of Phase 4). The obvious path --
/// DateTimeToUnix(LocalTimeToUniversal(Now), False) -- converts TWICE: the
/// False means "the input is local, convert it", and the input had already been
/// converted by hand. It is off by a whole hour per time zone... and vanishes on a
/// machine in the UTC zone, which is every container and most Linux servers, so
/// the defect does not show up precisely where the suite runs.
///
/// Hence the choice of this expression: it does not call DateTimeToUnix, so
/// it depends neither on the flag's semantics (which differ between the compilers)
/// nor on the RTL zeroing the milliseconds before the internal Round (FPC does, via
/// RecodeMillisecond; Delphi was not checked). UnixDateDelta is 25569
/// on both.
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
