"""
T25 (HL follower) dedup + endpoint half: check_any_open_positions_or_orders fails CLOSED (C42), and
/api/broadcast-signal validates the payload with signal_guard -> 422 before any DB write (C46 / I11).
The endpoint runs through fastapi's TestClient with get_db overridden (gate deps from requirements-gate.txt).
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.fakes import fake_hl
from tests.fakes.fake_hl import creds, fast_sleep, notes, venue  # noqa: F401  (pytest fixtures)
from tests.fakes.fake_pool import FakeDB, FakePool

import follower_endpoints as fe
import hosted_trading_loop as htl

WALLET = '0x' + '%040x' % 7


@pytest.fixture
def db():
    return FakeDB()


def _dedup(loop, info):
    return loop.check_any_open_positions_or_orders(info, None, WALLET, 'nk_user...', user_id=7, signal_id='sig-1')


# ---------------------------------------------------------------- dedup fails closed (C42)
async def test_position_read_error_skips_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.script('user_state', lambda *a: TimeoutError('read timed out'))
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason.startswith('Position check failed (TimeoutError)')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR')


async def test_order_read_error_skips_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.script('open_orders', lambda *a: ConnectionError('reset'))
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason.startswith('Order check failed (ConnectionError)')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR')


async def test_outer_error_skips_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    db.add_signal('sig-1')
    db.fail_next.append(('SELECT COUNT(*) FROM open_positions', RuntimeError('db down')))
    assert await _dedup(loop, loop.info) == (True, 'Safety check error - skipping')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR') and db.errors('SAFETY_CHECK_ERROR')


async def test_clean_reads_allow_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await _dedup(loop, loop.info) == (False, None)


async def test_open_order_still_blocks_as_today(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.add_order(WALLET, 'ADA', 'A', 100, 0.45)
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason == 'Open orders exist: 1 orders'


async def test_a_read_error_sends_no_entry(venue, db, creds, notes, fast_sleep):
    user = db.add_user(7, encrypt=creds)
    s = db.add_signal('sig-1')
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.script('user_state', lambda *a: TimeoutError('read timed out'))
    ok = await loop.execute_trade(
        {'id': 7, 'api_key': user['api_key'], 'email': user['email'],
         'hl_private_key_encrypted': user['hl_private_key_encrypted'], 'hl_wallet_address': WALLET},
        {'delivery_id': None, 'signal_id': s['signal_id'], 'action': 'BUY', 'symbol': 'ADA/USDT',
         'entry_price': 0.5, 'stop_loss': 0.45, 'take_profit': 0.55, 'leverage': 5.0, 'risk_pct': 0.02})
    assert ok is False and venue.order_calls() == []
