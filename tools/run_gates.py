"""
tools/run_gates.py - the local gate for hyperliquid-nike-rocket-api (Wave 1: spec C47, T19, AC1, AC3, AC4).
There is no GitHub Actions: this command is the gate.

    pip install -r requirements-gate.txt      # CPython 3.11-3.13; the gate-only set, NOT production
    python tools/run_gates.py

1. import-check the gate dependencies (FAIL with the install hint when one is missing);
2. py_compile every Wave 1 module, tools/*.py and tests/**/*.py;
3. no 'importorskip' anywhere under tests/;
4. pytest over the explicit DB-free list with a junit report; FAIL on any failure, error, skip or xfail, or a
   listed module that ran 0 tests (tests/test_billing_integration.py stays out: asyncpg + TEST_DATABASE_URL);
5. newline-normalised sha256 of the shared files: bracket_guard.py + tests/test_bracket_guard.py against
   ../massive-rocket-algos and ../kraken-nike-rocket-api; follower_guard.py, signal_guard.py and
   tests/test_follower_common.py against ../kraken-nike-rocket-api. A missing sibling repo is a FAIL.
The pytest child runs with gate_env(): production variables removed (QC security-6); tests/conftest.py blocks
every non-loopback connection.
Writes gate_logs/junit_<UTC>.xml and gate_logs/gates_<UTC>.log; prints GATES PASS|FAIL; exits 0|1.
"""
import glob
import hashlib
import importlib
import os
import py_compile
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARENT = os.path.dirname(ROOT)
GATE_DEPS = ('pytest', 'pytest_asyncio', 'pytest_mock', 'fastapi', 'pydantic', 'email_validator', 'sqlalchemy',
             'httpx', 'cryptography', 'aiohttp', 'requests')
MODULES = ('hosted_trading_loop.py', 'order_utils.py', 'position_guardian.py', 'position_monitor.py',
           'follower_endpoints.py', 'main.py', 'bracket_guard.py', 'follower_guard.py', 'signal_guard.py',
           'hl_rate.py')
TESTS = ('test_bracket_guard', 'test_follower_common', 'test_emergency_close_and_bracket', 'test_position_guardian',
         'test_hl_rate', 'test_fail_closed_and_bounds', 'test_endpoint_static', 'test_post_entry_recovery')
SHARED = (('bracket_guard.py', ('massive-rocket-algos', 'kraken-nike-rocket-api')),
          ('tests/test_bracket_guard.py', ('massive-rocket-algos', 'kraken-nike-rocket-api')),
          ('follower_guard.py', ('kraken-nike-rocket-api',)),
          ('signal_guard.py', ('kraken-nike-rocket-api',)),
          ('tests/test_follower_common.py', ('kraken-nike-rocket-api',)))


# QC security-6: the pytest child never sees production variables (tests/conftest.py also blocks the network)
SCRUB_ENV = ('DATABASE_URL', 'RESEND_API_KEY', 'MASTER_API_KEY', 'ADMIN_PASSWORD', 'CREDENTIALS_ENCRYPTION_KEY',
             'GUARDIAN_MODE', 'HL_WEIGHT_CEILING', 'HL_GUARDIAN_WEIGHT_PER_MIN', 'USE_TESTNET', 'ADMIN_EMAIL',
             'COINBASE_COMMERCE_API_KEY', 'COINBASE_COMMERCE_WEBHOOK_SECRET')
SCRUB_PREFIXES = ('CHAOS_',)


def gate_env(environ):
    env = {k: v for k, v in environ.items() if k not in SCRUB_ENV and not k.startswith(SCRUB_PREFIXES)}
    env['PYTHONUTF8'] = '1'
    return env


def sha256_normalised(path):
    with open(path, 'rb') as f:
        return hashlib.sha256(f.read().replace(b'\r\n', b'\n')).hexdigest()


def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(errors='backslashreplace')      # Windows consoles: never crash on log text
    stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    os.makedirs(os.path.join(ROOT, 'gate_logs'), exist_ok=True)
    junit = os.path.join(ROOT, 'gate_logs', 'junit_%s.xml' % stamp)
    lines, problems = [], []

    def out(msg):
        print(msg)
        lines.append(msg)

    # 1. gate dependencies
    missing = []
    for dep in GATE_DEPS:
        try:
            importlib.import_module(dep)
        except ImportError:
            missing.append(dep)
    if missing:
        problems.append('missing gate deps %s: run  pip install -r requirements-gate.txt' % ', '.join(missing))

    # 2. py_compile
    files = [os.path.join(ROOT, m) for m in MODULES]
    files += sorted(glob.glob(os.path.join(ROOT, 'tools', '*.py')))
    files += sorted(glob.glob(os.path.join(ROOT, 'tests', '**', '*.py'), recursive=True))
    for path in files:
        try:
            py_compile.compile(path, doraise=True)
            out('py_compile OK   %s' % os.path.relpath(path, ROOT))
        except (py_compile.PyCompileError, OSError) as e:
            problems.append('py_compile FAILED %s: %s' % (os.path.relpath(path, ROOT), e))

    # 3. no importorskip
    for path in glob.glob(os.path.join(ROOT, 'tests', '**', '*.py'), recursive=True):
        with open(path, encoding='utf-8') as f:
            if 'importorskip' in f.read():
                problems.append('importorskip found in %s' % os.path.relpath(path, ROOT))

    # 4. pytest + junit
    n_tests = failures = errors = skipped = 0
    if not missing:
        cmd = [sys.executable, '-m', 'pytest', '-q', '-rA', '-p', 'no:cacheprovider', '--junitxml', junit]
        cmd += ['tests/%s.py' % t for t in TESTS]
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding='utf-8', errors='replace',
                              env=gate_env(os.environ))
        out(proc.stdout[-4000:])
        if proc.returncode != 0:
            problems.append('pytest exit code %d' % proc.returncode)
        if not os.path.exists(junit):
            problems.append('no junit report written')
        else:
            per_module = dict.fromkeys(TESTS, 0)
            for case in ET.parse(junit).iter('testcase'):
                n_tests += 1
                module = next((part for part in case.get('classname', '').split('.') if part in per_module), None)
                if module:
                    per_module[module] += 1
                for child in case:
                    if child.tag == 'failure':
                        failures += 1
                    elif child.tag == 'error':
                        errors += 1
                    elif child.tag == 'skipped':
                        skipped += 1
            for module, count in per_module.items():
                out('tests %-36s %d' % (module, count))
                if count == 0:
                    problems.append('listed module %s ran 0 tests' % module)
            if failures or errors or skipped:
                problems.append('junit: %d failures, %d errors, %d skipped/xfail' % (failures, errors, skipped))

    # 5. shared files byte-identical (newline-normalised)
    for rel, repos in SHARED:
        here = os.path.join(ROOT, rel)
        if not os.path.exists(here):
            problems.append('shared file missing here: %s' % rel)
            continue
        mine = sha256_normalised(here)
        for repo in repos:
            there = os.path.join(PARENT, repo, rel)
            if not os.path.exists(there):
                problems.append('sibling copy missing: %s/%s' % (repo, rel))
                continue
            theirs = sha256_normalised(there)
            out('sha256 %-32s %s %s %s' % (rel, mine[:16], '==' if mine == theirs else '!=', repo))
            if mine != theirs:
                problems.append('shared file differs: %s vs %s' % (rel, repo))

    verdict = 'GATES PASS' if not problems else 'GATES FAIL'
    summary = '%s n_tests=%d failures=%d errors=%d skipped=%d' % (verdict, n_tests, failures, errors, skipped)
    for p in problems:
        out('FAIL: ' + p)
    out(summary)
    with open(os.path.join(ROOT, 'gate_logs', 'gates_%s.log' % stamp), 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    return 0 if not problems else 1


if __name__ == '__main__':
    sys.exit(main())
