#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Passo de restart do SmokeTest -- WS10 da Fase 4.

O UNICO lugar do repositorio onde a durabilidade e' observada por um cliente
PURO, por cima de um socket, contra um processo de broker que morreu de
verdade. As suites rodam broker e cliente no mesmo processo -- otimo para
cobertura, inutil para esta pergunta.

    BrokerHostFpc --datadir D  ->  SmokeTest --durable-publish N
                               ->  MATA o host (SIGKILL / taskkill /F)
                               ->  BrokerHostFpc --datadir D (o MESMO)
                               ->  SmokeTest --durable-verify N

O verify checa contagem, ORDEM, integridade do corpo e o flag `redelivered`
(D27), e exige que nada a mais volte.

LEMBRETE, o mesmo da matriz de queda: matar o processo NAO testa fsync -- a
cache do SO sobrevive a morte do processo. O que este passo prova e' que o que
o broker CONFIRMOU a um cliente de verdade continua la' depois de o processo
morrer e outro subir no mesmo diretorio.

USO
    python tests/tools/smoke_restart.py [--porta 5674] [--n 50]
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
    """Morte sem aviso -- um SIGTERM daria chance de finalizar, e ai' o passo
    mediria um shutdown em vez de uma queda."""
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


def roda_smoke(porta, opcao, n):
    r = subprocess.run(
        [SMOKE, '--host', '127.0.0.1', '--port', str(porta), opcao, str(n)],
        capture_output=True, text=True, timeout=180)
    return r.returncode, r.stdout + r.stderr


def main():
    porta = 5674
    n = 50
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == '--porta' and i + 1 < len(args):
            porta = int(args[i + 1])
        elif a == '--n' and i + 1 < len(args):
            n = int(args[i + 1])

    for caminho, nome in ((HOST, 'BrokerHostFpc'), (SMOKE, 'SmokeTest')):
        if not os.path.exists(caminho):
            print('nao achei o %s em %s' % (nome, caminho))
            return 1

    datadir = tempfile.mkdtemp(prefix='amqpsmokerestart-')
    print('PASSO DE RESTART DO SMOKETEST')
    print('  porta: %d   mensagens: %d' % (porta, n))
    print('  datadir: %s' % datadir)
    print('  (lembrete: matar o processo NAO testa fsync -- ver D28)')
    print('')

    proc = None
    try:
        print('[1] sobe o broker com DataDir e publica %d persistentes' % n)
        proc = sobe_host(porta, datadir)
        rc, saida = roda_smoke(porta, '--durable-publish', n)
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
        rc, saida = roda_smoke(porta, '--durable-verify', n)
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
