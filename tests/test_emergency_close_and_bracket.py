"""
T25 (HL follower): verified emergency close (C41), SL-first bracket sized to the confirmed fill (C40), the IOC
double-entry guard and unexpected-status handling (C43), in-flight token first (I12), trading-path metering and
cached constructor meta (C45 / R3-M3). Offline: fake_hl stubs the SDK, FakePool stands in for asyncpg.
"""
import ast
import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.fakes import fake_hl
from tests.fakes.fake_hl import creds, fast_sleep, notes, venue  # noqa: F401  (pytest fixtures)
from tests.fakes.fake_pool import FakeDB, FakePool

import follower_guard
import hl_rate
import hosted_trading_loop as htl
import order_utils

WALLET = '0x' + '%040x' % 7


@pytest.fixture
def db():
    return FakeDB()


def make_user(db, encrypt, uid=7):
    u = db.add_user(uid, encrypt=encrypt)
    return {'id': uid, 'api_key': u['api_key'], 'email': u['email'],
            'hl_private_key_encrypted': u['hl_private_key_encrypted'], 'hl_wallet_address': u['hl_wallet_address']}


def make_signal(db, action='BUY', entry=0.5, sl=0.45, tp=0.55, risk=0.02, leverage=5.0, delivery=True):
    s = db.add_signal('sig-%d' % (len(db.signals) + 1))
    d = db.add_delivery(7, s['id']) if delivery else None
    return {'delivery_id': d['id'] if d else None, 'signal_id': s['signal_id'], 'action': action,
            'symbol': 'ADA/USDT', 'entry_price': entry, 'stop_loss': sl, 'take_profit': tp, 'leverage': leverage,
            'risk_pct': risk}


def entry_orders(venue):
    return [a for a in venue.order_calls() if 'limit' in a[5] and not a[6]]


def trigger_orders(venue):
    return [a for a in venue.order_calls() if 'trigger' in a[5]]


# ---------------------------------------------------------------- order_utils (C43, A3)
def test_cancel_result_ok_trusts_only_the_inner_status():
    ok = order_utils.cancel_result_ok
    envelope = lambda st: {'status': 'ok', 'response': {'type': 'cancel', 'data': {'statuses': [st]}}}
    assert ok(envelope('success')) is True
    assert ok(envelope({'error': 'Order was never placed, already canceled, or filled. asset=0'})) is True
    assert ok(envelope({'error': 'Rate limited'})) is False
    assert ok({'status': 'err', 'response': 'boom'}) is False
    assert ok(None) is False


async def test_cancel_order_verified_retries_an_inner_error_three_times(venue, fast_sleep):
    venue.script('cancel', lambda *a: {'status': 'ok', 'response': {'data': {'statuses': [{'error': 'busy'}]}}})
    ex = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET)
    assert await order_utils.cancel_order_verified(ex, 'ADA', '1234', 'SL') is False
    assert [a[2][2] for a in venue.calls if a[1] == 'cancel'] == [1234, 1234, 1234]     # int oid, 3 attempts


async def test_cancel_order_verified_success_and_unusable_oid(venue, fast_sleep):
    oid = venue.add_order(WALLET, 'ADA', 'A', 100, 0.45)
    ex = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET)
    assert await order_utils.cancel_order_verified(ex, 'ADA', str(oid), 'SL') is True
    assert await order_utils.cancel_order_verified(ex, 'ADA', 'unknown', 'SL') is False
    assert len([c for c in venue.calls if c[1] == 'cancel']) == 1


async def test_unexpected_ok_statuses_stay_success_and_alarm(venue, notes, fast_sleep):
    venue.script('order', lambda *a: fake_hl.ok_status('waitingForTrigger'))
    ex = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET)
    res = await order_utils.place_sl_order_with_retry(ex, 'ADA', False, 100.0, 0.45)
    assert res['id'] == 'unknown'
    assert any('HL_UNEXPECTED_STATUS' in t for t, _, _ in notes)
