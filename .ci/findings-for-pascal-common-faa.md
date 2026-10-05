# pascal-amqp-faa (after v0.1.0) — findings for pascal-common-faa

Found on 2026-10-05 while closing the items that pascal-dfe-broker's F10 raised for amqp
(`external/pascal-common-faa` at v1.1.2). Nothing in pascal-common-faa was changed; these are
the points to bring back. Most important first. The F8 findings are in
`f8-findings-for-pascal-common-faa.md`.

## 1. `TPcThreadPool.Queue` does not grow the pool during a burst: queued items wait behind blocking ones

**What happens.** `Queue` chooses between starting a worker and signalling `FWork` by looking at
`FIdle`:

```pascal
FQueue.Enqueue(AItem);
if (FIdle = 0) and (FWorkers.Count < FMaxWorkers) then
  FWorkers.Add(TWorker.Create(Self))
else
  FWork.SetEvent;
```

`FIdle` only goes down when a woken worker takes the lock again in `Fetch`. A worker that has
already been signalled but has not taken its item yet still counts as idle. So in a burst of
`Queue` calls against N idle workers, every call sees `FIdle = N` and only signals; no worker is
started. If the first N items block (a callback doing I/O, a wait on another item), the rest of
the burst stays in the queue until somebody calls `Queue` again with `FIdle = 0`. (The
auto-reset `FWork` also merges the signals, but the baton passing in `Fetch` makes up for that;
the missing workers are the problem.)

**Measured** (FPC 3.2.2, Windows 11 x64 and Debian 12 container; repro below): a
`TPcThreadPool.Create(16)` with N idle workers, then 17 items queued in a row that block on an
event; after 2 s:

| idle workers before the burst | started | still queued |
|---|---|---|
| 0 | 16 | 1 |
| 1 | **1** | **16** |
| 6 | 6 (once 12) | 11 (once 5) |

With one idle worker, a burst of 17 blocking items runs one at a time on a pool whose ceiling
is 16.

**Why it matters.** For `PcPool` this is the normal case: after the first callbacks the pool
has idle workers, and amqp's client queues one item per delivery as frames arrive. A burst of
deliveries whose callbacks block on I/O runs on as many workers as were idle when the burst
started, not up to the ceiling. If a callback waits for something that another queued item
would produce (a reply consumed on the same pool, for instance), it waits until an unrelated
`Queue` call happens to start a worker. It is a liveness problem, not only throughput.

**It already showed up in amqp's client, unexplained.** The F8 worklog recorded an intermittent
failure of the integration test `ConsomeTodas_ComAck_E_Concorrencia` on Linux under load ("peak
concurrency 1: no two callbacks overlapped", cause not found). The test publishes 8 messages,
consumes them with callbacks that take a while and asserts a peak above 1. This is the burst:
the 8 deliveries are queued on `PcPool` in a row, `PcPool` has one idle worker left by earlier
tests, and all 8 callbacks run on it one after another (17.7 s instead of a fraction of it). It
came back once more here, in the first Linux round after these changes, with nothing in the
client changed.

**How amqp found it.** A test that saturates `PcPool` with `MaxWorkers + 1` blocking items
(amqp's `TPoolIsolationTests`) queued them in a row. In the Linux container, `PcPool` had idle
workers left by earlier tests, so only those started; the test waited 10 s for saturation, then
(through a bug of the test itself, now fixed) left its items blocked, and a later test's queue
actor (a standalone queue on `PcPool`) timed out after 15 s. `gdb` showed 5 pool threads idle
on the work event while the queue held 18 items; a dump of the pool showed `workers=6 idle=0
queue=11` with a ceiling of 16. The amqp test now queues one item at a time and waits for each
to start.

**Suggestion.** Start a worker when there are more queued items than idle workers to take them:

```pascal
FQueue.Enqueue(AItem);
if (FQueue.Count > FIdle) and (FWorkers.Count < FMaxWorkers) then
  FWorkers.Add(TWorker.Create(Self))
else
  FWork.SetEvent;
```

Both counts are read under `FLock`, and a woken worker decrements `FIdle` and dequeues in the
same critical section, so `FQueue.Count - FIdle` is the number of items no idle worker will
take. Not tried here (amqp does not change pascal-common-faa).

**Repro** (`Rajada.lpr`, kept in amqp's ignored `build/v011/rajada/`; first argument is the
number of idle workers):

```pascal
program Rajada;
{$mode delphi}
uses
  {$IFDEF UNIX}cthreads,{$ENDIF}
  SysUtils, Classes, SyncObjs, PascalCommon.Threading, PascalCommon.ThreadPool;
type
  TRapido = class(TPcWorkItem) procedure Execute; override; end;
  TBloqueia = class(TPcWorkItem) procedure Execute; override; end;
var
  GPortao: TEvent;
  GPartiram, GRapidos: Integer;
procedure TRapido.Execute; begin PcAtomicInc(GRapidos); end;
procedure TBloqueia.Execute; begin PcAtomicInc(GPartiram); GPortao.WaitFor(10000); end;
var
  LPool: TPcThreadPool;
  I, LOciosos: Integer;
begin
  GPortao := TEvent.Create(nil, True, False, '');
  LOciosos := StrToIntDef(ParamStr(1), 6);
  LPool := TPcThreadPool.Create(16);
  try
    for I := 1 to LOciosos do LPool.Queue(TRapido.Create);
    while PcAtomicGet(GRapidos) < LOciosos do Sleep(5);
    Sleep(200);
    for I := 1 to 17 do LPool.Queue(TBloqueia.Create);
    Sleep(2000);
    WriteLn(Format('idle before=%d ceiling=%d queued=17 -> started=%d, still queued=%d',
      [LOciosos, LPool.MaxWorkers, PcAtomicGet(GPartiram), LPool.QueueDepth]));
  finally
    GPortao.SetEvent;
    LPool.Free;
    GPortao.Free;
  end;
end.
```

## 2. On FPC/Linux, `TThread.ProcessorCount` is 1, so `PcPool`'s ceiling is always 16

**Measured** (FPC 3.2.2, Debian 12 container, `nproc` = 12, with and without `--cpus=1`):
`TThread.ProcessorCount = 1` and `PcPool.MaxWorkers = 16`. The default ceiling documented as
`max(16, 4 × cores)` is `max(16, 4 × 1)` there. On Windows (FPC and Delphi) it reads the real
count.

**Why it matters.** Less than finding 1, but it makes finding 1 easier to hit on Linux (a
smaller ceiling, reached sooner), and the documented ceiling is wrong on that platform. Either
document it or read the count another way on Unix (`sysconf(_SC_NPROCESSORS_ONLN)`). Not
verified on other Unixes or on newer FPC.
