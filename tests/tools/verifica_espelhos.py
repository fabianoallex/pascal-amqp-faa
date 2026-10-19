#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""O que o gerador de espelhos NAO confere, nos mestres DUnitX.

DE ONDE VEIO
------------
Ate' 2026-10-19 os dois lados das suites eram escritos a mao, e este script
nasceu para pegar, sem compilador, divergencias entre eles que custavam um
round-trip pela IDE Delphi (a Community Edition nao compila por linha de
comando). Tinha seis checagens; o historico de cada uma esta' em
docs/broker-worklog.md.

Desde entao os testes seguem o kit da skill dual-compiler-delphi-lazarus: o
mestre DUnitX (tests/<Suite>/X.pas) e' escrito no dialeto do FPCUnit
(TAssert.*, via AMQP.DUnitXCompat) e o espelho tests/<Suite>/fpc/X.pas e'
GERADO por tools/gen_fpc_mirror.py, que no --check acusa espelho
desatualizado e recusa API do DUnitX no corpo. Com um corpo so', o build FPC
compila o mesmo texto que o Delphi. Isso aposentou tres checagens:
  - codigo depois do `initialization` (o FPC recusa o mesmo texto);
  - paridade de testes entre os lados (o espelho e' derivado do mestre);
  - Assert.* do DUnitX no lado FPC (o gerador recusa).

FICARAM as que o gerador e o FPC nao enxergam, porque o defeito so' aparece
no Delphi, ou em lugar nenhum:

[1] Fixture com [Test] e sem TDUnitX.RegisterTestFixture. O gerador copia o
    registro, entao a fixture some DOS DOIS lados sem erro nenhum -- a suite
    fica verde com menos testes.
[2] Toda unit de src/server listada em todo .dpr/.dproj que compila o
    submodulo, e no pascal_amqp_faa_server.lpk (ver o docstring da funcao:
    unit fora do .lpk compila e morre em runtime).
[3] Assercao do FPCUnit SEM o `TAssert.` na frente. No espelho gerado ela e'
    metodo herdado de TTestCase e compila; no mestre, que e' uma classe
    simples, so' o dcc32 recusa (E2003). Vale tambem para Fail e Ignore.
[4] Atributo do DUnitX ([Setup], [Test]...) fora de `public`: o RTTI padrao
    do Delphi nao o enxerga e ele nunca roda. O gerador nao olha a secao, e
    no FPCUnit a fixture funciona.
[5] RTL que so' existe no FPC (GetTempDir, GetLastOSError, o qualificador
    `SysUtils.`): compila no FPC, falha so' no dcc32. O lugar dessas
    diferencas e' tests/AMQP.TestSupport.pas.

USO
---
    python tests/tools/verifica_espelhos.py

Sai com codigo 1 se achar problema. Rode junto com
`python tools/gen_fpc_mirror.py --check --suite tests/Unit --suite
tests/Integration --suite tests/Server`, antes de mandar a suite para a IDE.

NAO E um parser de Pascal: e casamento de padroes deliberadamente simples.
"""

import io
import os
import re
import sys

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
SUITES = ['tests/Unit', 'tests/Integration', 'tests/Server']
IGNORAR = ('/lib/', '\\lib\\', '/backup/', '\\backup\\', '__recovery')


def le(caminho):
    with io.open(caminho, encoding='utf-8-sig', errors='replace') as f:
        return f.read()


def rel(p):
    return os.path.relpath(p, RAIZ).replace('\\', '/')


def mestres():
    """Os mestres DUnitX: tests/<Suite>/*Tests.pas (o que o gerador le')."""
    for s in SUITES:
        base = os.path.join(RAIZ, s)
        if not os.path.isdir(base):
            continue
        for n in sorted(os.listdir(base)):
            if n.endswith('Tests.pas'):
                yield os.path.join(base, n)


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


def sem_comentarios(texto):
    """Troca comentarios e literais por espacos, preservando os \\n.

    Sem isto as checagens acusam a PROSA (um comentario que explica por que
    um nome do outro dialeto nao serve). Preservar as quebras de linha mantem
    os numeros de linha do relatorio apontando para o arquivo de verdade.
    """
    saida = []
    i, n = 0, len(texto)
    while i < n:
        c = texto[i]
        if c == "'":
            j = i + 1
            while j < n and texto[j] != "'":
                j += 1
            saida.append(' ' * (min(j, n - 1) - i + 1))
            i = j + 1
        elif c == '/' and texto[i:i + 2] == '//':
            j = texto.find('\n', i)
            if j < 0:
                j = n
            saida.append(' ' * (j - i))
            i = j
        elif c == '{':
            j = texto.find('}', i)
            if j < 0:
                j = n - 1
            saida.append(''.join(ch if ch == '\n' else ' '
                                 for ch in texto[i:j + 1]))
            i = j + 1
        elif texto[i:i + 2] == '(*':
            j = texto.find('*)', i)
            j = n - 2 if j < 0 else j
            saida.append(''.join(ch if ch == '\n' else ' '
                                 for ch in texto[i:j + 2]))
            i = j + 2
        else:
            saida.append(c)
            i += 1
    return ''.join(saida)


def linha_de(texto, pos):
    return texto.count('\n', 0, pos) + 1


# [1] -------------------------------------------------------------------------

def verifica_registro():
    """Fixture com [Test] e sem registro.

    "Fixture com testes" e' a unidade certa: classe auxiliar sem [Test] nao
    precisa de registro. A FORMA da chamada importa: `RegisterTestFixture(X)`
    sem o `TDUnitX.` nao compila no Delphi.
    """
    problemas = []
    for p in mestres():
        s = le(p)
        decl = s[:s.find('implementation')]
        com_testes = set()
        for m in re.finditer(r'\[TestFixture\]\s*(T\w+)\s*=\s*class\b(.*?)^\s*end;',
                             decl, re.S | re.M):
            if re.search(r'\[Test\]', m.group(2)):
                com_testes.add(m.group(1))
        reg = set(re.findall(r'TDUnitX\s*\.\s*RegisterTestFixture\s*\(\s*(\w+)\s*\)', s))
        faltando = com_testes - reg
        if faltando:
            problemas.append((rel(p), 'fixture NAO REGISTRADA: ' + ', '.join(sorted(faltando))))
    return problemas


# [2] -------------------------------------------------------------------------

def projetos_delphi_com_server():
    """.dpr que compilam src/server (listam as units uma a uma)."""
    for p in arquivos_pascal(['tests', 'samples']):
        if not p.lower().endswith('.dpr'):
            continue
        s = le(p)
        if ('src' + os.sep + 'server') in s or 'src/server/' in s:
            yield p, s


def verifica_units_do_server():
    r"""Toda unit de src/server tem de estar em TODO .dpr que usa o submodulo
    E no pacote pascal_amqp_faa_server.lpk.

    No Delphi cada projeto lista as units uma a uma, e a que faltar aparece
    como "F2613 Unit not found" na IDE.

    No FPC o sintoma e' pior: uma unit de src/server fora do .lpk ainda
    compila (o OtherUnitFiles do pacote aponta para ..\src\server), mas e'
    compilada DUAS VEZES, com .ppu distintos. No dia em que ela declara uma
    interface usada pelo pacote, os dois lados discordam do layout e o
    programa morre de access violation em runtime, com tudo compilando limpo
    (AMQP.Server.Events, Inc. 2 da Fase 4.1).
    """
    problemas = []
    srv = os.path.join(RAIZ, 'src', 'server')
    if not os.path.isdir(srv):
        return problemas
    units = sorted(n[:-4] for n in os.listdir(srv) if n.lower().endswith('.pas'))
    for caminho, texto in projetos_delphi_com_server():
        faltando = [u for u in units if u not in texto]
        if faltando:
            problemas.append((rel(caminho),
                              'unit de src/server nao listada: ' + ', '.join(faltando)))
        dproj = caminho[:-4] + '.dproj'
        if os.path.exists(dproj):
            td = le(dproj)
            faltam2 = [u for u in units if u in texto and u not in td]
            if faltam2:
                problemas.append((rel(dproj),
                                  'unit no .dpr mas nao no .dproj: ' + ', '.join(faltam2)))
    lpk = os.path.join(RAIZ, 'packages', 'pascal_amqp_faa_server.lpk')
    if os.path.exists(lpk):
        tl = le(lpk)
        faltam3 = [u for u in units if ('<UnitName Value="%s"/>' % u) not in tl]
        if faltam3:
            problemas.append((rel(lpk),
                              'unit de src/server nao listada no pacote: ' + ', '.join(faltam3)))
    return problemas


# [3] -------------------------------------------------------------------------

# Metodos de TAssert/TTestCase do FPCUnit. No mestre so' valem qualificados
# (`TAssert.AssertTrue(...)`); soltos, compilam so' no espelho gerado.
SO_NO_TTESTCASE = (
    'AssertEquals', 'AssertTrue', 'AssertFalse', 'AssertNull', 'AssertNotNull',
    'AssertSame', 'AssertNotSame', 'AssertException', 'Fail', 'Ignore',
)


def verifica_assercoes_qualificadas():
    problemas = []
    for p in mestres():
        corpo = sem_comentarios(le(p))
        for nome in SO_NO_TTESTCASE:
            for m in re.finditer(r'(?<![\w.])' + nome + r'\s*\(', corpo, re.IGNORECASE):
                problemas.append((rel(p), 'linha %d: %s sem o TAssert. na frente -- '
                                  'no mestre so o dcc32 recusa' % (linha_de(corpo, m.start()), nome)))
    return problemas


# [4] -------------------------------------------------------------------------

VIS_SEM_RTTI = ('private', 'protected', 'strict private', 'strict protected')
RE_SECAO = re.compile(r'^\s*(strict\s+private|strict\s+protected|private|'
                      r'protected|public|published)\s*$', re.IGNORECASE)
RE_ATRIBUTO_DUNITX = re.compile(
    r'\[\s*(Setup|TearDown|SetupFixture|TearDownFixture|Test|TestCase)\b', re.IGNORECASE)


def verifica_visibilidade_dos_atributos():
    """Varre linha a linha guardando a ultima secao vista; comeca em `public`,
    o default de uma classe Delphi sem especificador."""
    problemas = []
    for p in mestres():
        corpo = sem_comentarios(le(p))
        vis = 'public'
        for n, linha in enumerate(corpo.splitlines(), 1):
            m = RE_SECAO.match(linha)
            if m:
                vis = ' '.join(m.group(1).lower().split())
                continue
            if RE_ATRIBUTO_DUNITX.search(linha) and vis in VIS_SEM_RTTI:
                problemas.append((rel(p), 'linha %d: atributo do DUnitX em secao %s -- o RTTI '
                                  'padrao nao o enxerga, e ele nunca sera executado' % (n, vis)))
    return problemas


# [5] -------------------------------------------------------------------------

# Lista deliberadamente CURTA: cada entrada custou um round-trip pela IDE.
SO_NO_FPC = (
    'GetTempDir',        # use DirTemporario (AMQP.TestSupport)
    'GetLastOSError',    # use AmqpLastOsError (o RaiseLastOSError do Delphi LEVANTA)
)
# No Delphi a unit e' System.SysUtils: qualificar como `SysUtils.Xxx` nao
# compila (E2003); `System.SysUtils.Xxx` nao compila no FPC.
RE_QUALIF = re.compile(r'(?<![\w.])(?:System\s*\.\s*)?SysUtils\s*\.\s*[A-Za-z]')


def verifica_rtl():
    problemas = []
    for p in mestres():
        corpo = sem_comentarios(le(p))
        impl = corpo.find('implementation')
        for nome in SO_NO_FPC:
            for m in re.finditer(r'(?<![\w.])' + nome + r'(?![\w])', corpo, re.IGNORECASE):
                problemas.append((rel(p), 'linha %d: %s so existe no FPC'
                                  % (linha_de(corpo, m.start()), nome)))
        for m in RE_QUALIF.finditer(corpo, impl if impl > 0 else 0):
            problemas.append((rel(p), 'linha %d: unit qualificada (SysUtils.) -- o nome '
                              'difere entre os compiladores; mova para AMQP.TestSupport'
                              % linha_de(corpo, m.start())))
    return problemas


CHECAGENS = [
    ('[1] fixture com testes registrada no DUnitX', verifica_registro),
    ('[2] units de src/server listadas em todo projeto Delphi e no .lpk', verifica_units_do_server),
    ('[3] assercoes qualificadas com TAssert. nos mestres', verifica_assercoes_qualificadas),
    ('[4] atributos do DUnitX em secao visivel ao RTTI', verifica_visibilidade_dos_atributos),
    ('[5] RTL que so existe no FPC fora dos mestres', verifica_rtl),
]


def main():
    falhou = False
    for titulo, fn in CHECAGENS:
        print(titulo)
        probs = fn()
        if probs:
            falhou = True
            for arq, msg in probs:
                print('    FALHA  %s: %s' % (arq, msg))
        else:
            print('    ok')
    if falhou:
        print('\nPROBLEMAS ENCONTRADOS')
        return 1
    print('\nlimpo')
    return 0


if __name__ == '__main__':
    sys.exit(main())
