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


# ---------------------------------------------------------------- emergency close (C41)
async def test_close_envelope_ok_with_inner_error_is_a_failure(venue, notes, fast_sleep):
    venue.set_position(WALLET, 'ADA', 100.0, 0.5)
    venue.script('order', lambda *a: fake_hl.err_status('Order could not immediately match against any resting orders.'))
    ex, info = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET), fake_hl.Info(skip_ws=True)
    ok, oid = await htl.hl_close_with_verification(ex, info, WALLET, 'ADA', False, 100.0, round)
    assert (ok, oid) == (False, None)
    assert len(venue.order_calls()) == 3                           # the whole ladder was tried
    assert venue.szi(WALLET, 'ADA') == 100.0
    assert any('EMERGENCY_CLOSE_FAILED' in t for t, _, _ in notes)


async def test_close_filled_and_flat_reread_is_success(venue, notes, fast_sleep):
    venue.set_position(WALLET, 'ADA', -100.0, 0.5)
    ex, info = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET), fake_hl.Info(skip_ws=True)
    ok, oid = await htl.hl_close_with_verification(ex, info, WALLET, 'ADA', True, 100.0, round)
    assert ok is True and isinstance(oid, int)
    assert venue.szi(WALLET, 'ADA') == 0.0
    assert venue.names('info')[-1] == 'user_state'                 # verified by a re-read after the fill
    assert not notes


async def test_close_ladder_is_2_5_10_percent_off_a_fresh_mid_each_attempt(venue, notes, fast_sleep):
    venue.set_position(WALLET, 'ADA', 100.0, 0.5)
    mids = iter([0.50, 0.40, 0.30])
    venue.script('all_mids', lambda *a: {'ADA': str(next(mids))})
    venue.script('order', lambda *a: fake_hl.err_status('no match'), times=2)
    ex, info = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET), fake_hl.Info(skip_ws=True)
    ok, _ = await htl.hl_close_with_verification(ex, info, WALLET, 'ADA', False, 100.0, lambda p: round(p, 6))
    assert ok is True
    assert [a[4] for a in venue.order_calls()] == [0.49, 0.38, 0.27]   # sell: mid * (1 - 2% / 5% / 10%)
    assert all(a[6] is True and a[5] == {'limit': {'tif': 'Ioc'}} for a in venue.order_calls())
    names = [n for t, n, _ in venue.calls if t in ('info', 'exchange')]
    assert names[:6] == ['all_mids', 'order', 'all_mids', 'order', 'all_mids', 'order']
    assert fast_sleep == [1, 2]


async def test_close_filled_but_still_open_is_a_failure(venue, notes, fast_sleep):
    venue.set_position(WALLET, 'ADA', 100.0, 0.5)
    venue.script('order', lambda *a: fake_hl.ok_status({'filled': {'totalSz': '40', 'avgPx': '0.49', 'oid': 9}}))
    ex, info = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET), fake_hl.Info(skip_ws=True)
    ok, oid = await htl.hl_close_with_verification(ex, info, WALLET, 'ADA', False, 100.0, round)
    assert ok is False and oid == 9
    assert any('EMERGENCY_CLOSE_FAILED' in t for t, _, _ in notes)


async def test_close_honours_source_and_executor(venue, notes, fast_sleep):
    venue.set_position(WALLET, 'ADA', 100.0, 0.5)
    threads = []
    venue.script('order', lambda *a: threads.append(threading.current_thread().name) or NotImplemented)
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='guardian-test')
    ex, info = fake_hl.Exchange(fake_hl.Account.from_key('k'), account_address=WALLET), fake_hl.Info(skip_ws=True)
    before = hl_rate.window()[2]
    ok, _ = await htl.hl_close_with_verification(ex, info, WALLET, 'ADA', False, 100.0, round,
                                                 source='guardian', executor=pool)
    pool.shutdown()
    after = hl_rate.window()[2]
    assert ok is True
    assert threads and all(t.startswith('guardian-test') for t in threads)
    assert after.get('guardian', 0) - before.get('guardian', 0) == 2 + 1 + 2   # all_mids + order + user_state
    assert after.get('trading', 0) == before.get('trading', 0)
