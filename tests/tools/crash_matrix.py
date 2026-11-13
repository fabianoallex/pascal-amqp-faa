#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Broker process-death matrix -- WS8 of Phase 4 (D28, layer 3).

WHAT THIS PROVES -- AND, FIRST, WHAT IT DOES NOT
------------------------------------------------
KILLING THE PROCESS DOES NOT TEST FSYNC. The operating system's cache survives
the death of the process; only a crash of the MACHINE loses it. This sentence
is in D28 and goes in the README, and it is the reason this matrix exists with
a declared scope instead of a grandiose name.

What it does prove, and is worth having on its own:

  1. WHAT WAS CONFIRMED COMES BACK. The broker dies without shutting anything
     down -- no destructor, no Stop, no drain -- and what it had already
     confirmed to the publisher is there on the next boot.

  2. WHAT WAS CONSUMED DOES NOT COME BACK TO LIFE. Without the ack's DEQ on
     disk, every message already consumed would return on each restart: the
     failure mode that grows without bound and nobody notices right away.

  3. A TORN TAIL DOES NOT PREVENT BOOT. Killed IN THE MIDDLE of a write, the
     log ends with a half-written record -- which is the NORMAL case after a
     crash, not an error (D23). The broker has to start, and must not LOSE what
     it already confirmed, nor return anything corrupted or out of order.

The other two layers of D28 live in the suite, not here: the exhaustive
truncation sweep (WS1) and the file test double that counts fsyncs and fails
at the chosen moment (WS1/WS2/WS7).

USAGE
    python tests/tools/crash_matrix.py [path-to-CrashHostFpc]

Exits with code 1 if any scenario fails.
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

DEFAULT_HOST = os.path.join(ROOT, 'tests', 'Acceptance', 'fpc',
                           'CrashHostFpc.exe' if os.name == 'nt'
                           else 'CrashHostFpc')

MARK_TIMEOUT = 60  # seconds waiting for the marker to appear in the output


class Failure(Exception):
    pass


def kill_proc(proc):
    """Death WITHOUT WARNING -- the point of the exercise.

    No polite terminate(): on Windows, TerminateProcess; on Unix, SIGKILL.
    A SIGTERM would give the process a chance to run finalization, and then the
    test would measure a shutdown, not a crash.
    """
    if proc.poll() is not None:
        return
    if os.name == 'nt':
        subprocess.run(['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                       capture_output=True)
    else:
        os.kill(proc.pid, signal.SIGKILL)
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        pass


def run_until_mark(host, args, mark, timeout=MARK_TIMEOUT):
    """Starts the host, waits for the line starting with `marca` and returns
    (proc, linha).

    The process stays ALIVE: the caller is the one who kills it.
    """
    proc = subprocess.Popen([host] + args, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                raise Failure('the host died before the marker %r' % mark)
            continue
        line = line.strip()
        if line.startswith('ERROR') or line.startswith('NACK'):
            kill_proc(proc)
            raise Failure('the host complained: ' + line)
        if line.startswith(mark):
            return proc, line
    kill_proc(proc)
    raise Failure('the marker %r did not appear within %ds' % (mark, timeout))


def verify(host, directory):
    """Starts the host in verify mode (it exits by itself) and returns the count."""
    r = subprocess.run([host, 'verify', directory], capture_output=True,
                       text=True, timeout=120)
    output = r.stdout + r.stderr
    if 'OUT-OF-ORDER' in output:
        raise Failure('the messages came back out of order:\n' + output)
    m = re.search(r'RECOVERED (\d+)', output)
    if not m:
        raise Failure('verify did not answer (exit code %d):\n%s'
                    % (r.returncode, output))
    return int(m.group(1))


# -------------------------------------------------------------- scenarios ---

def scenario_confirmed_returns(host, d):
    """(1) Killed after the confirm: everything that was confirmed must come back."""
    proc, _ = run_until_mark(host, ['publish', d, '200'], 'PUBLISHED')
    kill_proc(proc)
    n = verify(host, d)
    if n != 200:
        raise Failure('expected 200 back, got %d' % n)
    return 'the 200 confirmed came back'


def scenario_consumed_does_not_return(host, d):
    """(2) Killed after the ack: what was consumed must not come back to life."""
    proc, _ = run_until_mark(host, ['publish', d, '100'], 'PUBLISHED')
    kill_proc(proc)
    proc, _ = run_until_mark(host, ['consume', d, '40'], 'CONSUMED')
    kill_proc(proc)
    n = verify(host, d)
    if n != 60:
        raise Failure('expected 60 (100 - 40), got %d' % n)
    return '40 consumed did not come back to life'


def scenario_death_mid_write(host, d):
    """(3) Killed IN THE MIDDLE of a write: the boot survives the torn tail.

    The criterion is not "N came back": it is that the broker STARTS, that it
    does NOT LOSE what it had already confirmed, and that nothing comes back
    corrupted or out of order.

    THERE IS NO CHECKABLE UPPER BOUND, and the first version of this scenario
    was wrong for trying: the driver reads the CONFIRMED markers through a
    pipe, and the reading lags behind the writing. When it decides to kill, the
    host has already confirmed much more than the driver read -- "407 came
    back for 150 read" is no violation at all, it is the lagging pipe. What can
    be asserted from outside is the floor: everything the driver SAW confirmed
    must be there.
    """
    proc = subprocess.Popen([host, 'dump', d], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    last = 0
    deadline = time.time() + MARK_TIMEOUT
    try:
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    raise Failure('the host died on its own during the dump')
                continue
            m = re.match(r'CONFIRMED (\d+)', line.strip())
            if m:
                last = int(m.group(1))
                if last >= 150:
                    break
        if last < 150:
            raise Failure('the dump did not reach 150 confirmed within %ds'
                        % MARK_TIMEOUT)
    finally:
        # Kill IN THE MIDDLE: the next write is in flight at this instant.
        kill_proc(proc)

    n = verify(host, d)
    if n < last:
        raise Failure('%d came back, FEWER than the %d the driver saw confirmed '
                    '-- confirmed data was lost' % (n, last))
    return ('killed in flight: >=%d confirmed, %d recovered, in order, '
            'clean boot' % (last, n))


def scenario_repeated_restart(host, d):
    """(4) Several crashes in a row on the SAME directory.

    Each boot recovers, publishes more and dies. Pins down two things that a
    single crash does not catch: that recovery does not DUPLICATE (the id
    counter continues after the largest one in the log) and that the log does
    not grow by one copy per boot (the journal stays silent during the replay).
    """
    total = 0
    for _ in range(4):
        # `start` keeps the numbering GLOBALLY increasing across the rounds:
        # without it, each crash would restart from 1 and verify -- which
        # checks ORDER, not just count -- would report out-of-order for a
        # reason that is not the broker's.
        proc, _ = run_until_mark(
            host, ['publish', d, '50', str(total + 1)], 'PUBLISHED')
        kill_proc(proc)
        total += 50
        n = verify(host, d)
        if n != total:
            raise Failure('after %d published across successive crashes, '
                        '%d came back' % (total, n))
    return '4 crashes in a row, 200 accumulated without duplicates'


SCENARIOS = [
    ('what was confirmed comes back', scenario_confirmed_returns),
    ('what was consumed does not come back to life', scenario_consumed_does_not_return),
    ('death in the middle of a write', scenario_death_mid_write),
    ('successive crashes on the same directory', scenario_repeated_restart),
]


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_HOST
    if not os.path.exists(host):
        print('CrashHostFpc not found at %s' % host)
        print('build it with: lazbuild tests/Acceptance/fpc/CrashHostFpc.lpi')
        return 1

    print('PROCESS-DEATH MATRIX')
    print('  host: %s' % host)
    print('  (reminder: killing the process does NOT test fsync -- see D28)')
    print('')

    failed = False
    for name_, fn in SCENARIOS:
        d = tempfile.mkdtemp(prefix='amqpcrash-')
        print('[%s]' % name_)
        try:
            print('    ok  ' + fn(host, d))
        except Failure as e:
            failed = True
            print('    FAIL  %s' % e)
        except Exception as e:  # noqa: BLE001 -- the driver must not bring everything down
            failed = True
            print('    ERROR  %s: %s' % (type(e).__name__, e))
        finally:
            shutil.rmtree(d, ignore_errors=True)

    print('')
    if failed:
        print('MATRIX HAS FAILURES')
        return 1
    print('matrix clean')
    return 0


if __name__ == '__main__':
    sys.exit(main())
