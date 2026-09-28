"""
T28 hl_rate meter (Wave 1, spec C45 / I18 / A1): exact sliding-window sums, acquire_background waits on
either limit and gives up after max_wait_s, charge never blocks, and a trading-burst + monitor + guardian
simulation never lets guardian reads push the window above HL_WEIGHT_CEILING. The AST half checks that every
Hyperliquid call site in position_monitor.py, hosted_trading_loop.py and order_utils.py is charged.
"""
import ast
import asyncio
import hashlib
import os

import pytest

import hl_rate

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture
def meter(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(hl_rate, '_clock', clock)
    with hl_rate._lock:
        hl_rate._events.clear()
    yield clock
    with hl_rate._lock:
        hl_rate._events.clear()


def test_window_sums_are_exact_across_the_60s_boundary(meter):
    hl_rate.charge(5, 'trading')
    meter.t += 30
    hl_rate.charge(7, 'guardian')
    hl_rate.charge(3, 'monitor')
    assert hl_rate.window() == (15, 7, {'trading': 5, 'guardian': 7, 'monitor': 3})
    meter.t += 29.999
    assert hl_rate.window()[0] == 15
    meter.t += 0.001                      # the first charge is now exactly 60 s old: out of the window
    assert hl_rate.window() == (10, 7, {'guardian': 7, 'monitor': 3})
    meter.t += 30
    assert hl_rate.window() == (0, 0, {})


def test_charge_never_blocks_even_far_above_the_ceiling(meter):
    for _ in range(100):
        hl_rate.charge(50, 'trading')     # 5000 weight: charge-only callers never wait
    assert hl_rate.window()[0] == 5000


def test_extra_fill_weight_is_one_per_20_rows():
    assert hl_rate.extra_fill_weight(None) == 0
    assert hl_rate.extra_fill_weight([{}] * 19) == 0
    assert hl_rate.extra_fill_weight([{}] * 45) == 2


async def test_acquire_background_charges_immediately_when_there_is_room(meter):
    assert await hl_rate.acquire_background(20, 'guardian') is True
    assert hl_rate.window() == (20, 20, {'guardian': 20})


async def test_acquire_background_waits_on_the_total_ceiling_then_proceeds_when_drained(meter, monkeypatch):
    hl_rate.charge(hl_rate.CEILING - 10, 'monitor')
    real_sleep = asyncio.sleep
    waits = []

    async def advancing_sleep(delay, result=None):
        waits.append(delay)
        meter.t += delay
        await real_sleep(0)

    monkeypatch.setattr(asyncio, 'sleep', advancing_sleep)
    assert await hl_rate.acquire_background(20, 'guardian', max_wait_s=240.0) is True
    assert len(waits) == 60 and set(waits) == {1.0}          # waited until the monitor charge aged out
    assert hl_rate.window() == (20, 20, {'guardian': 20})


async def test_acquire_background_waits_on_the_guardian_share(meter, monkeypatch):
    hl_rate.charge(hl_rate.GUARDIAN_PER_MIN - 5, 'guardian')
    real_sleep = asyncio.sleep

    async def advancing_sleep(delay, result=None):
        meter.t += delay
        await real_sleep(0)

    monkeypatch.setattr(asyncio, 'sleep', advancing_sleep)
    t0 = meter.t
    assert await hl_rate.acquire_background(20, 'guardian') is True
    assert meter.t - t0 == 60.0
    assert hl_rate.window()[1] == 20


async def test_acquire_background_gives_up_after_max_wait(meter, monkeypatch):
    real_sleep = asyncio.sleep

    async def advancing_sleep(delay, result=None):
        meter.t += delay
        hl_rate.charge(hl_rate.CEILING, 'trading')       # the window never drains
        await real_sleep(0)

    monkeypatch.setattr(asyncio, 'sleep', advancing_sleep)
    hl_rate.charge(hl_rate.CEILING, 'trading')
    assert await hl_rate.acquire_background(20, 'guardian', max_wait_s=5.0) is False
    assert hl_rate.window()[1] == 0


async def test_simulated_burst_monitor_and_guardian_never_exceed_the_ceiling_through_guardian_reads(meter, monkeypatch):
    schedule = []
    for minute in range(10):
        for u in range(30):                                  # position_monitor: 30 credentialed users x 20 / min
            schedule.append((1000.0 + minute * 60 + u * 2.0, 20, 'monitor'))
    for burst in (1125.0, 1305.0):                           # two signal bursts: 25 active users x ~30 weight
        for u in range(25):
            schedule.append((burst + u * 0.1, 30, 'trading'))
    schedule.sort()
    real_sleep = asyncio.sleep

    def apply_due():
        while schedule and schedule[0][0] <= meter.t:
            _, w, src = schedule.pop(0)
            hl_rate.charge(w, src)

    async def advancing_sleep(delay, result=None):
        meter.t += delay
        apply_due()
        await real_sleep(0)

    monkeypatch.setattr(asyncio, 'sleep', advancing_sleep)
    apply_due()
    seen = []
    while meter.t < 1000.0 + 600:
        assert await hl_rate.acquire_background(22, 'guardian', max_wait_s=600.0) is True
        total, guardian, _ = hl_rate.window()
        seen.append((total, guardian))
        await asyncio.sleep(1.0)
    assert seen
    assert all(total <= hl_rate.CEILING for total, _ in seen)
    assert all(guardian <= hl_rate.GUARDIAN_PER_MIN for _, guardian in seen)


# ---------------------------------------------------------------- AST: every HL call site is charged (C45)
HL_INFO_METHODS = {'user_state', 'open_orders', 'frontend_open_orders', 'all_mids', 'meta', 'spot_meta',
                   'meta_and_asset_ctxs', 'user_fills', 'user_fills_by_time', 'candles_snapshot', 'l2_snapshot'}


HL_EXCHANGE_METHODS = {'order', 'cancel', 'update_leverage', 'market_close', 'bulk_orders', 'modify_order'}


POSITION_MONITOR_BASE_SHA256 = '43c742a869ef396ed4236c91c2890d2556736d1a6be1ae9d91a72edf436ca2e6'


def _is_counting_charge(node):
    """hl_rate.charge(...) whose weight is not the post-call extra_fill_weight(...) top-up."""
    if not (isinstance(node, ast.Call) and ast.unparse(node.func) == 'hl_rate.charge'):
        return False
    return not (node.args and 'extra_fill_weight' in ast.unparse(node.args[0]))


def _hl_site(node):
    """A Hyperliquid request: info.<read>, exchange.<action> (called or passed to an executor), or an
    Info(/Exchange( construction that does not pass cached meta=."""
    if isinstance(node, ast.Attribute):
        owner = ast.unparse(node.value).split('.')[-1]
        return (owner == 'info' and node.attr in HL_INFO_METHODS) or \
               (owner == 'exchange' and node.attr in HL_EXCHANGE_METHODS)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ('Info', 'Exchange'):
        return not any(k.arg == 'meta' for k in node.keywords)
    return False


def uncharged_sites(source):
    """'function:line' of every HL site not preceded, since the previous HL site in the same function,
    by a counting hl_rate.charge( call."""
    bad = []
    for fn in ast.walk(ast.parse(source)):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        events = sorted((n.lineno, n.col_offset, 'charge' if _is_counting_charge(n) else 'hl')
                        for n in ast.walk(fn) if _is_counting_charge(n) or _hl_site(n))
        charged = False
        for line, _, kind in events:
            if kind == 'charge':
                charged = True
            elif not charged:
                bad.append('%s:%d' % (fn.name, line))
            else:
                charged = False
    return sorted(set(bad))


def test_the_checker_flags_an_uncharged_call():
    src = ('def f(info, exchange):\n'
           '    hl_rate.charge(2, "t")\n'
           '    info.user_state("w")\n'
           '    info.open_orders("w")\n')
    assert uncharged_sites(src) == ['f:4']


@pytest.mark.parametrize('name', ['position_monitor.py', 'hosted_trading_loop.py', 'order_utils.py'])
def test_every_hyperliquid_call_site_is_charged(name):
    with open(os.path.join(ROOT, name), encoding='utf-8') as f:
        assert uncharged_sites(f.read()) == []


def test_position_monitor_differs_from_base_only_by_charge_lines_and_the_import():
    with open(os.path.join(ROOT, 'position_monitor.py'), encoding='utf-8') as f:
        text = f.read()
    kept = '\n'.join(line for line in text.split('\n')
                     if not (line.strip().startswith('hl_rate.charge(') or line.strip() == 'import hl_rate'))
    assert hashlib.sha256(kept.encode('utf-8')).hexdigest() == POSITION_MONITOR_BASE_SHA256
    assert 'import hl_rate' in text.split('\n')
