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
    python tests/tools/matriz_de_queda.py [path-to-CrashHostFpc]

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

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

HOST_PADRAO = os.path.join(RAIZ, 'tests', 'Acceptance', 'fpc',
                           'CrashHostFpc.exe' if os.name == 'nt'
                           else 'CrashHostFpc')

TIMEOUT_MARCA = 60  # seconds waiting for the marker to appear in the output


class Falha(Exception):
    pass


def mata(proc):
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


def roda_ate_marca(host, args, marca, timeout=TIMEOUT_MARCA):
    """Starts the host, waits for the line starting with `marca` and returns
    (proc, linha).

    The process stays ALIVE: the caller is the one who kills it.
    """
    proc = subprocess.Popen([host] + args, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    limite = time.time() + timeout
    while time.time() < limite:
        linha = proc.stdout.readline()
        if not linha:
            if proc.poll() is not None:
                raise Falha('o host morreu antes da marca %r' % marca)
            continue
        linha = linha.strip()
        if linha.startswith('ERRO') or linha.startswith('NACK'):
            mata(proc)
            raise Falha('o host reclamou: ' + linha)
        if linha.startswith(marca):
            return proc, linha
    mata(proc)
    raise Falha('a marca %r nao apareceu em %ds' % (marca, timeout))


def confere(host, diretorio):
    """Starts the host in confere mode (it exits by itself) and returns the count."""
    r = subprocess.run([host, 'confere', diretorio], capture_output=True,
                       text=True, timeout=120)
    saida = r.stdout + r.stderr
    if 'FORA-DE-ORDEM' in saida:
        raise Falha('as mensagens voltaram fora de ordem:\n' + saida)
    m = re.search(r'RECUPERADAS (\d+)', saida)
    if not m:
        raise Falha('o confere nao respondeu (codigo %d):\n%s'
                    % (r.returncode, saida))
    return int(m.group(1))


# -------------------------------------------------------------- scenarios ---

def cenario_confirmado_volta(host, d):
    """(1) Killed after the confirm: everything that was confirmed must come back."""
    proc, _ = roda_ate_marca(host, ['publica', d, '200'], 'PUBLICADAS')
    mata(proc)
    n = confere(host, d)
    if n != 200:
        raise Falha('esperava 200 de volta, vieram %d' % n)
    return 'as 200 confirmadas voltaram'


def cenario_consumido_nao_volta(host, d):
    """(2) Killed after the ack: what was consumed must not come back to life."""
    proc, _ = roda_ate_marca(host, ['publica', d, '100'], 'PUBLICADAS')
    mata(proc)
    proc, _ = roda_ate_marca(host, ['consome', d, '40'], 'CONSUMIDAS')
    mata(proc)
    n = confere(host, d)
    if n != 60:
        raise Falha('esperava 60 (100 - 40), vieram %d' % n)
    return '40 consumidas nao ressuscitaram'


def cenario_morte_no_meio_da_escrita(host, d):
    """(3) Killed IN THE MIDDLE of a write: the boot survives the torn tail.

    The criterion is not "N came back": it is that the broker STARTS, that it
    does NOT LOSE what it had already confirmed, and that nothing comes back
    corrupted or out of order.

    THERE IS NO CHECKABLE UPPER BOUND, and the first version of this scenario
    was wrong for trying: the driver reads the CONFIRMADA markers through a
    pipe, and the reading lags behind the writing. When it decides to kill, the
    host has already confirmed much more than the driver read -- "407 came
    back for 150 read" is no violation at all, it is the lagging pipe. What can
    be asserted from outside is the floor: everything the driver SAW confirmed
    must be there.
    """
    proc = subprocess.Popen([host, 'despeja', d], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    ultimo = 0
    limite = time.time() + TIMEOUT_MARCA
    try:
        while time.time() < limite:
            linha = proc.stdout.readline()
            if not linha:
                if proc.poll() is not None:
                    raise Falha('o host morreu sozinho no despejo')
                continue
            m = re.match(r'CONFIRMADA (\d+)', linha.strip())
            if m:
                ultimo = int(m.group(1))
                if ultimo >= 150:
                    break
        if ultimo < 150:
            raise Falha('o despejo nao alcancou 150 confirmadas em %ds'
                        % TIMEOUT_MARCA)
    finally:
        # Kill IN THE MIDDLE: the next write is in flight at this instant.
        mata(proc)

    n = confere(host, d)
    if n < ultimo:
        raise Falha('voltaram %d, MENOS que as %d que o driver viu confirmadas '
                    '-- perda de dado confirmado' % (n, ultimo))
    return ('morto em voo: >=%d confirmadas, %d recuperadas, em ordem, '
            'boot limpo' % (ultimo, n))


def cenario_restart_repetido(host, d):
    """(4) Several crashes in a row on the SAME directory.

    Each boot recovers, publishes more and dies. Pins down two things that a
    single crash does not catch: that recovery does not DUPLICATE (the id
    counter continues after the largest one in the log) and that the log does
    not grow by one copy per boot (the journal stays silent during the replay).
    """
    total = 0
    for _ in range(4):
        # `inicio` keeps the numbering GLOBALLY increasing across the rounds:
        # without it, each crash would restart from 1 and confere -- which
        # checks ORDER, not just count -- would report out-of-order for a
        # reason that is not the broker's.
        proc, _ = roda_ate_marca(
            host, ['publica', d, '50', str(total + 1)], 'PUBLICADAS')
        mata(proc)
        total += 50
        n = confere(host, d)
        if n != total:
            raise Falha('depois de %d publicadas em quedas sucessivas, '
                        'voltaram %d' % (total, n))
    return '4 quedas seguidas, 200 acumuladas sem duplicar'


CENARIOS = [
    ('o que foi confirmado volta', cenario_confirmado_volta),
    ('o que foi consumido nao ressuscita', cenario_consumido_nao_volta),
    ('morte no meio da escrita', cenario_morte_no_meio_da_escrita),
    ('quedas sucessivas no mesmo diretorio', cenario_restart_repetido),
]


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else HOST_PADRAO
    if not os.path.exists(host):
        print('nao achei o CrashHostFpc em %s' % host)
        print('compile com: lazbuild tests/Acceptance/fpc/CrashHostFpc.lpi')
        return 1

    print('MATRIZ DE MORTE DE PROCESSO')
    print('  host: %s' % host)
    print('  (lembrete: matar o processo NAO testa fsync -- ver D28)')
    print('')

    falhou = False
    for nome, fn in CENARIOS:
        d = tempfile.mkdtemp(prefix='amqpcrash-')
        print('[%s]' % nome)
        try:
            print('    ok  ' + fn(host, d))
        except Falha as e:
            falhou = True
            print('    FALHA  %s' % e)
        except Exception as e:  # noqa: BLE001 -- the driver must not bring everything down
            falhou = True
            print('    ERRO  %s: %s' % (type(e).__name__, e))
        finally:
            shutil.rmtree(d, ignore_errors=True)

    print('')
    if falhou:
        print('MATRIZ COM FALHAS')
        return 1
    print('matriz limpa')
    return 0


if __name__ == '__main__':
    sys.exit(main())
