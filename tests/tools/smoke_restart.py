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

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
EXT = '.exe' if os.name == 'nt' else ''
HOST = os.path.join(RAIZ, 'tests', 'Acceptance', 'fpc', 'BrokerHostFpc' + EXT)
SMOKE = os.path.join(RAIZ, 'samples', 'SmokeTest', 'SmokeTest' + EXT)


def mata(proc):
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


def espera_porta(porta, timeout=30):
    limite = time.time() + timeout
    while time.time() < limite:
        with socket.socket() as s:
            s.settimeout(0.5)
            try:
                s.connect(('127.0.0.1', porta))
                return True
            except OSError:
                time.sleep(0.1)
    return False


def sobe_host(porta, datadir):
    proc = subprocess.Popen(
        [HOST, str(porta), '--datadir', datadir, '--seconds', '600'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL)
    if not espera_porta(porta):
        mata(proc)
        raise RuntimeError('o BrokerHostFpc nao abriu a porta %d' % porta)
    return proc


def roda_smoke(smoke, porta, opcao, n):
    r = subprocess.run(
        [smoke, '--host', '127.0.0.1', '--port', str(porta), opcao, str(n)],
        capture_output=True, text=True, timeout=180)
    return r.returncode, r.stdout + r.stderr


def main():
    porta = 5674
    n = 50
    smoke = SMOKE
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == '--port' and i + 1 < len(args):
            porta = int(args[i + 1])
        elif a == '--n' and i + 1 < len(args):
            n = int(args[i + 1])
        elif a == '--smoke' and i + 1 < len(args):
            smoke = os.path.abspath(args[i + 1])

    for caminho, nome in ((HOST, 'BrokerHostFpc'), (smoke, 'SmokeTest')):
        if not os.path.exists(caminho):
            print('nao achei o %s em %s' % (nome, caminho))
            return 1

    datadir = tempfile.mkdtemp(prefix='amqpsmokerestart-')
    print('PASSO DE RESTART DO SMOKETEST')
    print('  porta: %d   mensagens: %d' % (porta, n))
    print('  smoke: %s' % smoke)
    print('  datadir: %s' % datadir)
    print('  (lembrete: matar o processo NAO testa fsync -- ver D28)')
    print('')

    proc = None
    try:
        print('[1] sobe o broker com DataDir e publica %d persistentes' % n)
        proc = sobe_host(porta, datadir)
        rc, saida = roda_smoke(smoke, porta, '--durable-publish', n)
        if rc != 0 or 'DURABLE PUBLISH: OK' not in saida:
            print(saida)
            print('FALHA no publish')
            return 1
        print('    ok  publicadas e confirmadas')

        print('[2] MATA o broker sem aviso')
        mata(proc)
        proc = None
        print('    ok  processo morto')

        print('[3] sobe OUTRO broker no mesmo diretorio')
        proc = sobe_host(porta, datadir)
        print('    ok  no ar de novo')

        print('[4] o cliente confere o que voltou')
        rc, saida = roda_smoke(smoke, porta, '--durable-verify', n)
        if rc != 0 or 'DURABLE VERIFY: OK' not in saida:
            print(saida)
            print('FALHA no verify')
            return 1
        print('    ok  %d de volta, na ordem, integras e redelivered' % n)
    finally:
        if proc is not None:
            mata(proc)
        shutil.rmtree(datadir, ignore_errors=True)

    print('')
    print('RESTART DO SMOKETEST: PASS')
    return 0


if __name__ == '__main__':
    sys.exit(main())
