#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verificador estrutural das suites espelhadas DUnitX <-> FPCUnit.

POR QUE ISTO EXISTE
-------------------
Metade da validacao deste projeto depende do IDE Delphi (a Community Edition
nao compila por linha de comando), entao um defeito que so aparece no espelho
Delphi custa um round-trip inteiro -- ou pior, passa despercebido. Os dois
defeitos que motivaram este script apareceram na WS2 da Fase 3:

1. Implementacoes de teste inseridas DEPOIS do `initialization`. Em Pascal,
   `procedure` ali e parseado como diretiva (E2070 no dcc32). O FPC pegou; o
   arquivo Delphi, gerado pelo mesmo caminho, tinha o mesmo defeito e nao foi
   reconferido. Verde num compilador nao e evidencia sobre o outro.

2. Fixture nova NAO registrada no `initialization` do DUnitX. Este e o pior:
   nao da erro de compilacao nenhum -- da uma suite verde com 13 testes a
   menos, que e exatamente o verde falso que o CLAUDE.md ja documenta da WS9
   da Fase 2.

Nenhum dos dois precisa de compilador para ser detectado. Rode isto antes de
mandar a suite para o IDE.

USO
---
    python tests/tools/verifica_espelhos.py

Sai com codigo 1 se achar divergencia real, 0 se estiver limpo -- da para
amarrar num hook ou num passo de CI.

NAO E um parser de Pascal: e casamento de padroes deliberadamente simples
sobre a secao de tipos e o bloco de registro. Serve para as duas classes de
erro acima e nao pretende mais que isso.
"""

import io
import os
import re
import sys

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Diretorios varridos pelo lint de "codigo depois do initialization".
DIRS_LINT = ['tests', 'samples']

# Pastas de artefato de build que nao interessam.
IGNORAR = ('/lib/', '\\lib\\', '/backup/', '\\backup\\', '__recovery')

# NAO ha lista de excecoes, e isso e proposital. A primeira versao deste
# script comparava TODA `procedure` da secao de tipos e exigia registro de TODA
# classe -- e acusou 7 divergencias, das quais 7 eram falso-positivo: metodos
# `Do*` que so existem do lado FPC (o AssertException do FPCUnit exige um
# METODO onde o Assert.WillRaise do DUnitX aceita closure), o `Execute` de um
# TAMQPWorkItem, e classes auxiliares como TAlvoFalso, que nao sao fixtures.
#
# Uma lista de excecoes teria escondido os falso-positivos sem consertar a
# heuristica -- e cada teste novo com nome parecido voltaria a acusar. Os
# discriminadores EXATOS existem nos dois dialetos e sao usados abaixo:
#   DUnitX  : fixture = classe com [TestFixture]; teste = metodo com [Test].
#   FPCUnit : fixture = class(TTestCase); teste = metodo na secao `published`.
# Com eles, helper em private/protected e classe auxiliar simplesmente nao
# entram na conta.


def le(caminho):
    with io.open(caminho, encoding='utf-8-sig', errors='replace') as f:
        return f.read()


def arquivos_pascal(dirs):
    for d in dirs:
        base = os.path.join(RAIZ, d)
        if not os.path.isdir(base):
            continue
        for pasta, _, nomes in os.walk(base):
            for n in nomes:
                if not n.lower().endswith(('.pas', '.dpr', '.lpr')):
                    continue
                p = os.path.join(pasta, n)
                if any(x in p for x in IGNORAR):
                    continue
                yield p


def rel(p):
    return os.path.relpath(p, RAIZ).replace('\\', '/')


def lint_apos_initialization():
    """Codigo executavel declarado depois do `initialization`."""
    achados = []
    for p in arquivos_pascal(DIRS_LINT):
        s = le(p)
        m = re.search(r'^initialization\s*$', s, re.M)
        if not m:
            continue
        depois = s[m.end():]
        for d in re.finditer(r'^(procedure|function)\s+(\w+)', depois, re.M):
            achados.append((rel(p), d.group(2)))
            break  # um por arquivo basta para acusar
    return achados


def pares_espelhados():
    """Casa tests/X/fpc/Nome.pas com tests/X/Nome.pas."""
    for p in arquivos_pascal(['tests']):
        pasta, nome = os.path.split(p)
        if os.path.basename(pasta).lower() != 'fpc':
            continue
        if not nome.lower().endswith('.pas'):
            continue
        delphi = os.path.join(os.path.dirname(pasta), nome)
        if os.path.exists(delphi):
            yield delphi, p


def blocos_de_classe(decl):
    """Gera (nome, pai, texto_antes, corpo) de cada `TXxx = class...end;`.

    O corpo vai ate' a primeira linha que seja so' `end;` -- basta para o
    formato consistente desta codebase e evita ter de parsear Pascal de
    verdade. `texto_antes` carrega os atributos que precedem a classe, que e'
    onde o [TestFixture] do DUnitX aparece. `pai` e' vazio em `= class` sem
    ancestral explicito.
    """
    for m in re.finditer(r'^[ \t]*(T\w+)\s*=\s*class\b[ \t]*(?:\(\s*(\w+))?',
                         decl, re.M):
        nome = m.group(1)
        pai = m.group(2) or ''
        antes = decl[max(0, m.start() - 120):m.start()]
        resto = decl[m.end():]
        fim = re.search(r'^[ \t]*end;\s*$', resto, re.M)
        corpo = resto[:fim.start()] if fim else resto
        yield nome, pai, antes, corpo


def testes_publicados(corpo):
    """Metodos da secao `published` (idioma de fixture do FPCUnit)."""
    nomes = set()
    # divide o corpo por palavras de visibilidade, mantendo qual abriu cada
    # trecho
    partes = re.split(r'^[ \t]*(private|protected|public|published)\b',
                      corpo, flags=re.M)
    atual = None
    for i, parte in enumerate(partes):
        if parte in ('private', 'protected', 'public', 'published'):
            atual = parte
            continue
        if atual == 'published':
            nomes.update(re.findall(r'procedure\s+(\w+)\s*;', parte))
    return nomes


def analisa(caminho):
    """(testes declarados, fixtures COM testes, fixtures registradas).

    Usa os discriminadores exatos de cada dialeto -- ver o comentario no topo:
    helper em private/protected e classe auxiliar nao entram na conta.

    "Fixture com testes" e' a unidade certa para exigir registro: uma classe
    BASE compartilhada (o TServerFixture das suites do server) e fixture, mas
    nao declara teste nenhum e nao precisa estar registrada. Exigir registro
    dela seria falso-positivo; nao exigir de quem tem teste deixaria passar o
    defeito silencioso que motivou este script.
    """
    s = le(caminho)
    i = s.find('implementation')
    decl = s[:i] if i > 0 else s

    dunitx = '[TestFixture]' in decl or 'DUnitX.TestFramework' in s
    classes = list(blocos_de_classe(decl))

    # Uma fixture do FPCUnit pode descender de outra fixture, nao so de
    # TTestCase direto -- as suites do server usam uma base comum. Fecho
    # transitivo por ponto fixo; sem isso, toda fixture derivada some da
    # analise e o arquivo inteiro parece nao ter teste algum.
    ehfixture = {}
    for nome, pai, antes, _c in classes:
        if dunitx:
            ehfixture[nome] = '[TestFixture]' in antes
        else:
            ehfixture[nome] = (pai == 'TTestCase')
    if not dunitx:
        mudou = True
        while mudou:
            mudou = False
            for nome, pai, _a, _c in classes:
                if not ehfixture.get(nome) and ehfixture.get(pai):
                    ehfixture[nome] = True
                    mudou = True

    testes = set()
    com_testes = set()
    for nome, _pai, _antes, corpo in classes:
        if not ehfixture.get(nome):
            continue
        if dunitx:
            meus = set(re.findall(r'\[Test\]\s*procedure\s+(\w+)\s*;', corpo))
        else:
            meus = testes_publicados(corpo)
        testes.update(meus)
        if meus:
            com_testes.add(nome)

    # A FORMA da chamada importa, nao so' o nome da fixture: no DUnitX o
    # registro e' um metodo de classe e precisa do `TDUnitX.` na frente --
    # `RegisterTestFixture(X)` solto nao compila (E2003, identificador nao
    # declarado). A primeira versao deste script casava os dois jeitos e por
    # isso deu a fixture como registrada num arquivo que nao compilava; foi
    # falso-negativo dele, achado quando o dcc32 recusou o arquivo.
    if dunitx:
        padrao = r'TDUnitX\s*\.\s*RegisterTestFixture\s*\(\s*(\w+)\s*\)'
    else:
        padrao = r'\bRegisterTest\s*\(\s*(\w+)\s*\)'
    registradas = set(re.findall(padrao, s))
    return testes, com_testes, registradas


def verifica_paridade():
    problemas = []
    for delphi, fpc in pares_espelhados():
        td, fd, rd = analisa(delphi)
        tf, ff, rf = analisa(fpc)

        so_delphi = td - tf
        so_fpc = tf - td
        if so_delphi:
            problemas.append((rel(delphi),
                              'teste so existe no DELPHI: ' + ', '.join(sorted(so_delphi))))
        if so_fpc:
            problemas.append((rel(fpc),
                              'teste so existe no FPC: ' + ', '.join(sorted(so_fpc))))

        # Fixture declarada e nao registrada = testes que nunca rodam, sem
        # nenhum erro de compilacao. E o defeito silencioso.
        for caminho, fx, reg in ((delphi, fd, rd), (fpc, ff, rf)):
            faltando = fx - reg
            if faltando:
                problemas.append((rel(caminho),
                                  'fixture NAO REGISTRADA: ' + ', '.join(sorted(faltando))))
    return problemas


def main():
    falhou = False

    apos = lint_apos_initialization()
    print('[1] codigo depois do initialization')
    if apos:
        falhou = True
        for arq, nome in apos:
            print('    FALHA  %s: %s declarado apos o initialization' % (arq, nome))
    else:
        print('    ok')

    print('[2] paridade dos espelhos DUnitX <-> FPCUnit')
    probs = verifica_paridade()
    if probs:
        falhou = True
        for arq, msg in probs:
            print('    FALHA  %s: %s' % (arq, msg))
    else:
        print('    ok')

    if falhou:
        print('\nDIVERGENCIAS ENCONTRADAS')
        return 1
    print('\nlimpo')
    return 0


if __name__ == '__main__':
    sys.exit(main())
