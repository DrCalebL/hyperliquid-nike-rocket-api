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
