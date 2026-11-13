#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SmokeTest restart step -- WS10 of Phase 4.

The ONLY place in the repository where durability is observed by a PURE
client, over a socket, against a broker process that really died. The suites
run broker and client in the same process -- great for coverage, useless for
this question.

    BrokerHostFpc --datadir D  ->  SmokeTest --durable-publish N
                               ->  KILLS the host (SIGKILL / taskkill /F)
                               ->  BrokerHostFpc --datadir D (the SAME one)
                               ->  SmokeTest --durable-verify N

The verify checks count, ORDER, body integrity and the `redelivered` flag
(D27), and requires that nothing extra comes back.

REMINDER, the same as the crash matrix's: killing the process does NOT test
fsync -- the OS cache survives the death of the process. What this step proves
is that what the broker CONFIRMED to a real client is still there after the
process dies and another one starts on the same directory.

USAGE
    python tests/tools/smoke_restart.py [--port 5674] [--n 50]
                                       [--smoke <path>]

--smoke points to ANOTHER SmokeTest -- that is how the Delphi side is
validated: the host is still BrokerHostFpc (the broker only has a
command-line FPC build), and the client becomes the executable the IDE built.
"""

import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
EXT = '.exe' if os.name == 'nt' else ''
HOST = os.path.join(ROOT, 'tests', 'Acceptance', 'fpc', 'BrokerHostFpc' + EXT)
SMOKE = os.path.join(ROOT, 'samples', 'SmokeTest', 'SmokeTest' + EXT)


def kill_proc(proc):
    """Death without warning -- a SIGTERM would give it a chance to finalize,
    and then the step would measure a shutdown instead of a crash."""
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


def wait_for_port(port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(0.5)
            try:
                s.connect(('127.0.0.1', port))
                return True
            except OSError:
                time.sleep(0.1)
    return False


def start_host(port, datadir):
    proc = subprocess.Popen(
        [HOST, str(port), '--datadir', datadir, '--seconds', '600'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL)
    if not wait_for_port(port):
        kill_proc(proc)
        raise RuntimeError('BrokerHostFpc did not open port %d' % port)
    return proc


def run_smoke(smoke, port, option, n):
    r = subprocess.run(
        [smoke, '--host', '127.0.0.1', '--port', str(port), option, str(n)],
        capture_output=True, text=True, timeout=180)
    return r.returncode, r.stdout + r.stderr


def main():
    port = 5674
    n = 50
    smoke = SMOKE
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == '--port' and i + 1 < len(args):
            port = int(args[i + 1])
        elif a == '--n' and i + 1 < len(args):
            n = int(args[i + 1])
        elif a == '--smoke' and i + 1 < len(args):
            smoke = os.path.abspath(args[i + 1])

    for path_, name_ in ((HOST, 'BrokerHostFpc'), (smoke, 'SmokeTest')):
        if not os.path.exists(path_):
            print('%s not found at %s' % (name_, path_))
            return 1

    datadir = tempfile.mkdtemp(prefix='amqpsmokerestart-')
    print('SMOKETEST RESTART STEP')
    print('  port: %d   messages: %d' % (port, n))
    print('  smoke: %s' % smoke)
    print('  datadir: %s' % datadir)
    print('  (reminder: killing the process does NOT test fsync -- see D28)')
    print('')

    proc = None
    try:
        print('[1] start the broker with DataDir and publish %d persistent' % n)
        proc = start_host(port, datadir)
        rc, output = run_smoke(smoke, port, '--durable-publish', n)
        if rc != 0 or 'DURABLE PUBLISH: OK' not in output:
            print(output)
            print('FAIL in publish')
            return 1
        print('    ok  published and confirmed')

        print('[2] KILL the broker without warning')
        kill_proc(proc)
        proc = None
        print('    ok  process killed')

        print('[3] start ANOTHER broker on the same directory')
        proc = start_host(port, datadir)
        print('    ok  up again')

        print('[4] the client checks what came back')
        rc, output = run_smoke(smoke, port, '--durable-verify', n)
        if rc != 0 or 'DURABLE VERIFY: OK' not in output:
            print(output)
            print('FAIL in verify')
            return 1
        print('    ok  %d back, in order, intact and redelivered' % n)
    finally:
        if proc is not None:
            kill_proc(proc)
        shutil.rmtree(datadir, ignore_errors=True)

    print('')
    print('SMOKETEST RESTART: PASS')
    return 0


if __name__ == '__main__':
    sys.exit(main())
