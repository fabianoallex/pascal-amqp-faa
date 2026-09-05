#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Matriz de morte de processo do broker -- WS8 da Fase 4 (D28, camada 3).

O QUE ISTO PROVA -- E, ANTES, O QUE NAO PROVA
---------------------------------------------
MATAR O PROCESSO NAO TESTA FSYNC. A cache do sistema operacional sobrevive a
morte do processo; so' a queda da MAQUINA a perde. Esta frase esta na D28 e vai
no README, e ela e o motivo de esta matriz existir com escopo declarado em vez
de com um nome grandioso.

O que ela prova, e que vale por si:

  1. O QUE FOI CONFIRMADO VOLTA. O broker morre sem encerrar nada -- nenhum
     destrutor, nenhum Stop, nenhum drain -- e o que ele ja' tinha confirmado
     ao publicador esta' la' no boot seguinte.

  2. O QUE FOI CONSUMIDO NAO RESSUSCITA. Sem o DEQ do ack no disco, toda
     mensagem ja' consumida voltaria a cada restart: o modo de falha que cresce
     sem limite e ninguem percebe de imediato.

  3. A CAUDA TORTA NAO IMPEDE O BOOT. Morto NO MEIO da escrita, o log fica com
     um registro pela metade -- que e' o caso NORMAL depois de uma queda, nao um
     erro (D23). O broker tem de subir, e nao pode PERDER o que ja confirmou,
     nem devolver nada corrompido ou fora de ordem.

As outras duas camadas da D28 vivem na suite e nao aqui: a varredura exaustiva
de truncamento (WS1) e o duble de arquivo que conta fsyncs e falha na hora
escolhida (WS1/WS2/WS7).

USO
    python tests/tools/matriz_de_queda.py [caminho-do-CrashHostFpc]

Sai com codigo 1 se algum cenario falhar.
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

TIMEOUT_MARCA = 60  # segundos esperando a marca aparecer na saida


class Falha(Exception):
    pass


def mata(proc):
    """Morte SEM AVISO -- o ponto do exercicio.

    Nada de terminate() cortes: no Windows, TerminateProcess; no Unix, SIGKILL.
    Um SIGTERM daria ao processo a chance de rodar finalizacao, e ai' o teste
    mediria um shutdown, nao uma queda.
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
    """Sobe o host, espera a linha que comeca com `marca` e devolve (proc, linha).

    O processo fica VIVO: quem o mata e' o chamador.
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
    """Sobe o host em modo confere (ele encerra sozinho) e devolve a contagem."""
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


# --------------------------------------------------------------- cenarios ---

def cenario_confirmado_volta(host, d):
    """(1) Morto depois do confirm: tudo o que foi confirmado tem de voltar."""
    proc, _ = roda_ate_marca(host, ['publica', d, '200'], 'PUBLICADAS')
    mata(proc)
    n = confere(host, d)
    if n != 200:
        raise Falha('esperava 200 de volta, vieram %d' % n)
    return 'as 200 confirmadas voltaram'


def cenario_consumido_nao_volta(host, d):
    """(2) Morto depois do ack: o consumido nao pode ressuscitar."""
    proc, _ = roda_ate_marca(host, ['publica', d, '100'], 'PUBLICADAS')
    mata(proc)
    proc, _ = roda_ate_marca(host, ['consome', d, '40'], 'CONSUMIDAS')
    mata(proc)
    n = confere(host, d)
    if n != 60:
        raise Falha('esperava 60 (100 - 40), vieram %d' % n)
    return '40 consumidas nao ressuscitaram'


def cenario_morte_no_meio_da_escrita(host, d):
    """(3) Morto NO MEIO da escrita: o boot sobrevive a cauda torta.

    O criterio nao e' "voltaram N": e' que o broker SOBE, que NAO PERDE o que
    ja' tinha confirmado, e que nada volta corrompido ou fora de ordem.

    NAO HA LIMITE SUPERIOR CHECAVEL, e a primeira versao deste cenario errava
    por tentar: o driver le' as marcas CONFIRMADA por um cano, e a leitura fica
    para tras da escrita. Quando ele decide matar, o host ja' confirmou muito
    mais do que o driver leu -- "voltaram 407 para 150 lidas" nao e' violacao
    nenhuma, e' o cano atrasado. O que se pode afirmar de fora e' o piso: tudo
    o que o driver VIU confirmado tem de estar la'.
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
        # Mata NO MEIO: a proxima escrita esta' em voo neste instante.
        mata(proc)

    n = confere(host, d)
    if n < ultimo:
        raise Falha('voltaram %d, MENOS que as %d que o driver viu confirmadas '
                    '-- perda de dado confirmado' % (n, ultimo))
    return ('morto em voo: >=%d confirmadas, %d recuperadas, em ordem, '
            'boot limpo' % (ultimo, n))


def cenario_restart_repetido(host, d):
    """(4) Varias quedas seguidas no MESMO diretorio.

    Cada boot recupera, publica mais e morre. Prende duas coisas que uma queda
    so' nao pega: que a recuperacao nao DUPLICA (o contador de ids continua
    depois do maior do log) e que o log nao cresce uma copia por boot (o
    journal fica mudo durante o replay).
    """
    total = 0
    for _ in range(4):
        # O `inicio` mantem a numeracao GLOBALMENTE crescente entre as rodadas:
        # sem ele, cada queda recomecaria do 1 e o confere -- que checa ORDEM,
        # nao so' contagem -- acusaria fora-de-ordem por um motivo que nao e' o
        # do broker.
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
        except Exception as e:  # noqa: BLE001 -- o driver nao pode derrubar tudo
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
