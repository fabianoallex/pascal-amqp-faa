"""Generates the FPCUnit mirrors (tests/<Suite>/fpc/X.pas) from the DUnitX
masters (tests/<Suite>/X.pas). The canonical copy of the *-faa family's test
kit (the dual-compiler-delphi-lazarus skill, scripts/gen_fpc_mirror.py):
copy it UNCHANGED into a project's tools/ folder; scripts/check_test_kit.py
tells a copy that drifted. Until 2026-10-19 eight repositories carried their
own copy, identical but for three names, which this one finds by itself:

  - the project root: the folder above tools/ when the script sits in tools/,
    else the current folder (or --root DIR);
  - the suites: tests/Unit and tests/Integration, whichever exist (or one or
    more --suite DIR, relative to the root);
  - the compat unit: any "<Prefix>.DUnitXCompat" right after
    DUnitX.TestFramework in the master's uses clause (the skill's
    assets/DUnitXCompat.pas.template).

Why generate instead of maintaining both by hand: the test bodies are
written in FPCUnit's assertion dialect (TAssert.AssertEquals/AssertTrue...)
on both sides, on Delphi through the compat unit. That leaves the fixture
declarations and the registration as the only real difference between the
two suites. This script swaps just that; the body comes out byte-for-byte
identical, and "forgot to port the new test to the other side" stops being
possible.

The master is ALWAYS the DUnitX file. Never edit tests/*/fpc/*.pas by hand:
edit the DUnitX file and run this again.

Transformations, only inside classes marked with [TestFixture]:
  [TestFixture] TX = class        -> TX = class(TTestCase)
  [Test] procedure Foo;           -> published: procedure Foo;
  [Setup] procedure Setup;        -> protected: procedure SetUp; override;
  [TearDown] procedure TearDown;  -> protected: procedure TearDown; override;
  other public members            -> stay public (in FPCUnit, EVERY
                                     published method becomes a test)
Outside the fixtures:
  uses DUnitX.TestFramework, X.DUnitXCompat -> fpcunit, testregistry
  TDUnitX.RegisterTestFixture(TX)            -> RegisterTest(TX)
A DUnitX API left in a body (Assert.AreEqual, TDUnitX...) is an error.

Usage: python tools/gen_fpc_mirror.py            (every suite's *Tests.pas)
       python tools/gen_fpc_mirror.py --check    (fails if any mirror is out
                                                  of date; writes nothing)
       [--root DIR] [--suite DIR ...]
"""
import re
import sys
from pathlib import Path

GENERATED_NOTE = (
    '{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from\n'
    '  {rel} (DUnitX). Do not edit by hand: edit the DUnitX\n'
    '  master and run the script again. }\n\n'
)

SECTION_RE = re.compile(r'^\s*(private|protected|public|published|strict private|strict protected)\s*$')
USES_RE = re.compile(r'\bDUnitX\.TestFramework,\s*\n\s*[\w.]+\.DUnitXCompat,')


class MirrorError(Exception):
    pass


def convert_fixture(lines):
    """lines: the class body lines (between 'TX = class' and 'end;')."""
    sections = {'private': [], 'protected': [], 'public': [], 'published': []}
    current = 'public'  # default visibility of a class with no section
    pending_attr = None
    trivia = []  # comments/blank lines: they go with the next member

    def emit(section, text):
        sections[section].extend(trivia)
        trivia.clear()
        sections[section].append(text)

    for line in lines:
        m = SECTION_RE.match(line)
        if m:
            current = m.group(1).replace('strict ', '')
            if current == 'published':
                raise MirrorError('DUnitX fixture with a published section: use public + [Test]')
            continue
        stripped = line.strip()
        if not pending_attr and (not stripped or stripped.startswith(('{', '//', '(*'))):
            trivia.append(line)
            continue
        attr = re.match(r'^\[(Test|Setup|TearDown)\]\s*(.*)$', stripped)
        if attr:
            kind, rest = attr.group(1), attr.group(2)
            if not rest:
                pending_attr = kind
                continue
            stripped, pending_attr = rest, kind
        if pending_attr:
            indent = '    '
            if pending_attr == 'Test':
                emit('published', indent + stripped)
            elif pending_attr == 'Setup':
                if not re.match(r'procedure\s+Setup\s*;', stripped, re.I):
                    raise MirrorError(f'[Setup] must be named SetUp for FPCUnit: {stripped}')
                emit('protected', indent + 'procedure SetUp; override;')
            else:
                if not re.match(r'procedure\s+TearDown\s*;', stripped, re.I):
                    raise MirrorError(f'[TearDown] must be named TearDown for FPCUnit: {stripped}')
                emit('protected', indent + 'procedure TearDown; override;')
            pending_attr = None
            continue
        emit(current, line)
    sections[current].extend(trivia)
    out = []
    for name in ('private', 'protected', 'public', 'published'):
        body = list(sections[name])
        if any(l.strip() for l in body):
            out.append(f'  {name}')
            out.extend(body)
    return out


def convert(text, name, rel):
    lines = text.split('\n')
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip() == '[TestFixture]':
            i += 1
            decl = lines[i]
            m = re.match(r'^(\s*)(\w+)\s*=\s*class\s*$', decl)
            if not m:
                raise MirrorError(f'{name}: fixture with inheritance/interfaces is not supported: {decl.strip()}')
            out.append(f'{m.group(1)}{m.group(2)} = class(TTestCase)')
            i += 1
            body = []
            while lines[i].strip() != 'end;':
                body.append(lines[i])
                i += 1
            out.extend(convert_fixture(body))
            out.append(lines[i])  # end;
            i += 1
            continue
        out.append(line)
        i += 1
    text = '\n'.join(out)

    # uses: swap the framework and the compat unit, whatever its prefix
    text, n = USES_RE.subn('fpcunit, testregistry,', text, count=1)
    if n != 1:
        raise MirrorError(f'{name}: uses clause lacks "DUnitX.TestFramework, <Prefix>.DUnitXCompat," in sequence')
    text = re.sub(r'TDUnitX\.RegisterTestFixture\((\w+)\);', r'RegisterTest(\1);', text)
    # Any DUnitX API left over doesn't exist in FPCUnit: the master must use
    # only the TAssert.* dialect.
    leftover = re.search(r'\b(TDUnitX|Assert\.[A-Z]\w*)\b', text)
    if leftover:
        raise MirrorError(f'{name}: DUnitX API in the body ({leftover.group(0)}); use TAssert.*')

    # Right after "unit X;": the FPC mode and the generated-file note. The
    # master's header follows, untouched.
    note = GENERATED_NOTE.replace('{rel}', rel)
    text, n = re.subn(r'^(unit [\w.]+;\n\n)',
                      lambda m: m.group(1) + '{$mode delphi}{$H+}\n\n' + note,
                      text, count=1, flags=re.M)
    if n != 1:
        raise MirrorError(f'{name}: "unit X;" followed by a blank line not found')
    return text


def parse_args(argv):
    here = Path(__file__).resolve().parent
    root = here.parent if here.name == 'tools' else Path.cwd()
    suites, check = [], False
    i = 0
    while i < len(argv):
        if argv[i] == '--check':
            check = True
        elif argv[i] == '--root' and i + 1 < len(argv):
            root = Path(argv[i + 1]).resolve()
            i += 1
        elif argv[i] == '--suite' and i + 1 < len(argv):
            suites.append(argv[i + 1])
            i += 1
        else:
            raise SystemExit(f'unknown argument: {argv[i]}')
        i += 1
    dirs = [root / s for s in suites] if suites else [root / 'tests' / 'Unit', root / 'tests' / 'Integration']
    return root, [d for d in dirs if d.is_dir()], check


def main():
    root, suite_dirs, check = parse_args(sys.argv[1:])
    stale = []
    for src in sorted(p for d in suite_dirs for p in d.glob('*Tests.pas')):
        dst_dir = src.parent / 'fpc'
        dst_dir.mkdir(parents=True, exist_ok=True)
        text = src.read_text(encoding='utf-8-sig')
        rel = src.relative_to(root).as_posix()
        mirror = convert(text, src.stem, rel)
        dst = dst_dir / src.name
        current = dst.read_text(encoding='utf-8-sig') if dst.exists() else None
        if current != mirror:
            stale.append(dst.name)
            if not check:
                dst.write_text(mirror, encoding='utf-8-sig', newline='\n')
    if check and stale:
        print('Out-of-date FPCUnit mirrors:', ', '.join(stale))
        print('Run: python tools/gen_fpc_mirror.py')
        sys.exit(1)
    print(('Out of date: ' if check else 'Generated/updated: ') + (', '.join(stale) or 'none'))


if __name__ == '__main__':
    main()
