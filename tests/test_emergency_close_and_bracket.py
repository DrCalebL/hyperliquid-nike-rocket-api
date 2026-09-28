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


async def test_constructor_uses_cached_meta_and_charges_startup_reads(venue, db, creds, notes, caplog):
    caplog.set_level(logging.INFO, logger='HOSTED_TRADING')
    loop = htl.HostedTradingLoop(FakePool(db))
    assert hl_rate.window()[2] == {'trading': 40 + 20 + 20}               # Info ctor + meta() + spot_meta()
    assert 'HL_SDK ctor_meta=True' in caplog.text
    before = venue.ctor_requests
    loop.get_or_create_exchange(make_user(db, creds))
    kind, rec = venue.ctors[-1]
    assert kind == 'Exchange' and rec['meta'] and rec['spot_meta'] and rec['account_address'] == WALLET
    assert venue.ctor_requests == before                                  # zero constructor requests
    assert hl_rate.window()[2] == {'trading': 80}


async def test_constructor_falls_back_with_a_40_charge_on_an_old_sdk(venue, db, creds, notes, monkeypatch, caplog):
    monkeypatch.setattr(htl, 'Exchange', fake_hl.ExchangeNoMeta)
    caplog.set_level(logging.INFO, logger='HOSTED_TRADING')
    loop = htl.HostedTradingLoop(FakePool(db))
    assert 'HL_SDK ctor_meta=False' in caplog.text
    before = venue.ctor_requests
    loop.get_or_create_exchange(make_user(db, creds))
    assert venue.ctors[-1][1]['meta'] is False
    assert venue.ctor_requests == before + 2
    assert hl_rate.window()[2] == {'trading': 80 + 40}


# ---------------------------------------------------------------- execute path (C40)
async def test_sl_first_sized_to_totalsz_without_any_sleep(venue, db, creds, notes, fast_sleep):
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    trig = trigger_orders(venue)
    assert [a[5]['trigger']['tpsl'] for a in trig] == ['sl', 'tp']          # SL before TP
    assert trig[0][5]['trigger']['isMarket'] is True and trig[1][5]['trigger']['isMarket'] is False
    assert [a[3] for a in trig] == [4000.0, 4000.0] and all(a[6] is True for a in trig)
    assert fast_sleep == []                                               # no blind asyncio.sleep(2)
    row = next(iter(db.positions.values()))
    assert (row['status'], row['quantity'], row['target_sl'], row['target_tp']) == ('open', 4000.0, 0.45, 0.55)


async def test_partial_totalsz_sizes_bracket_and_insert(venue, db, creds, notes, fast_sleep):
    def partial(account, coin, is_buy, sz, px, otype, ro):
        if 'limit' in otype and not ro:
            venue.set_position(account, coin, 2500.0, px)
            return fake_hl.ok_status({'filled': {'totalSz': '2500', 'avgPx': str(px), 'oid': 77}})
        return NotImplemented
    venue.script('order', partial)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    assert [a[3] for a in trigger_orders(venue)] == [2500.0, 2500.0]
    assert next(iter(db.positions.values()))['quantity'] == 2500.0


async def test_user_state_is_polled_when_the_ioc_reports_no_totalsz(venue, db, creds, notes, fast_sleep):
    def resting(account, coin, is_buy, sz, px, otype, ro):
        if 'limit' in otype and not ro:
            venue.set_position(account, coin, 3000.0, 0.501)
            return fake_hl.ok_status({'resting': {'oid': 78}})
        return NotImplemented
    venue.script('order', resting)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    names = venue.names()
    first_order = names.index('order')
    assert 'user_state' in names[first_order:names.index('order', first_order + 1)]
    assert [a[3] for a in trigger_orders(venue)] == [3000.0, 3000.0]
    assert next(iter(db.positions.values()))['entry_fill_price'] == 0.501


async def test_enforce_tp_failure_keeps_position_and_inserts_tp_null(venue, db, creds, notes, fast_sleep, monkeypatch):
    monkeypatch.setenv('GUARDIAN_MODE', 'enforce')
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('rejected')
                 if 'trigger' in ot and ot['trigger']['tpsl'] == 'tp' else NotImplemented)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    assert venue.szi(WALLET, 'ADA') == 4000.0                              # no close
    assert not [a for a in venue.order_calls() if 'limit' in a[5] and a[6]]
    row = next(iter(db.positions.values()))
    assert row['status'] == 'open' and row['tp_order_id'] is None and row['sl_order_id']
    assert db.errors('TP_PLACE_FAILED')


async def test_shadow_tp_failure_closes_before_cancelling_the_sl(venue, db, creds, notes, fast_sleep, monkeypatch):
    monkeypatch.setenv('GUARDIAN_MODE', 'shadow')
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('rejected')
                 if 'trigger' in ot and ot['trigger']['tpsl'] == 'tp' else NotImplemented)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    seq = [(n, a) for t, n, a in venue.calls if n in ('order', 'cancel')]
    close_i = next(i for i, (n, a) in enumerate(seq) if n == 'order' and 'limit' in a[5] and a[6])
    cancel_i = next(i for i, (n, a) in enumerate(seq) if n == 'cancel')
    assert close_i < cancel_i                                              # R3-M1: close first, SL kept live
    assert venue.szi(WALLET, 'ADA') == 0.0 and venue.orders(WALLET) == []  # verified flat, SL cancelled
    assert db.positions == {}                                              # nothing left to recover


async def test_sl_failure_closes_once_and_never_places_a_tp(venue, db, creds, notes, fast_sleep):
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('rejected')
                 if 'trigger' in ot and ot['trigger']['tpsl'] == 'sl' else NotImplemented)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    assert [a[5]['trigger']['tpsl'] for a in trigger_orders(venue)] == ['sl', 'sl', 'sl']
    assert len([a for a in venue.order_calls() if 'limit' in a[5] and a[6]]) == 1
    assert venue.szi(WALLET, 'ADA') == 0.0 and db.positions == {}


async def test_ioc_exception_with_a_position_is_not_resent(venue, db, creds, notes, fast_sleep):
    def fill_then_timeout(account, coin, is_buy, sz, px, otype, ro):
        if 'limit' in otype and not ro:
            venue.set_position(account, coin, 4000.0, px)
            return TimeoutError('read timed out')
        return NotImplemented
    venue.script('order', fill_then_timeout, times=1)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    assert len(entry_orders(venue)) == 1                                   # double-entry guard
    assert [a[3] for a in trigger_orders(venue)] == [4000.0, 4000.0]


async def test_ioc_error_status_is_retried_as_today(venue, db, creds, notes, fast_sleep):
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('Order could not immediately match')
                 if 'limit' in ot and not ro else NotImplemented, times=1)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    assert len(entry_orders(venue)) == 2


async def test_token_is_taken_before_acknowledgment_and_dedup(venue, db, creds, notes, fast_sleep, monkeypatch):
    seq = []
    real_mark, real_ack = follower_guard.mark, htl.HostedTradingLoop.acknowledge_signal
    real_dedup = htl.HostedTradingLoop.check_any_open_positions_or_orders
    monkeypatch.setattr(follower_guard, 'mark', lambda uid: seq.append('mark') or real_mark(uid))

    async def ack(self, delivery_id):
        seq.append(('ack', follower_guard.is_inflight(7)))
        return await real_ack(self, delivery_id)

    async def dedup(self, *a, **k):
        seq.append(('dedup', follower_guard.is_inflight(7)))
        return await real_dedup(self, *a, **k)
    monkeypatch.setattr(htl.HostedTradingLoop, 'acknowledge_signal', ack)
    monkeypatch.setattr(htl.HostedTradingLoop, 'check_any_open_positions_or_orders', dedup)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    assert seq == ['mark', ('ack', True), ('dedup', True)]
    assert follower_guard.is_inflight(7) is False                          # cleared on return


async def test_a_second_concurrent_execution_keeps_the_user_in_flight(venue, db, creds, notes, fast_sleep):
    other = follower_guard.mark(7)                                         # a concurrent execute_trade's token
    loop = htl.HostedTradingLoop(FakePool(db))
    await loop.execute_trade(make_user(db, creds), make_signal(db))
    assert follower_guard.is_inflight(7) is True
    follower_guard.clear(7, other)
    assert follower_guard.is_inflight(7) is False


async def test_token_is_cleared_when_the_body_raises(venue, db, creds, notes, fast_sleep, monkeypatch):
    async def boom(self, delivery_id):
        raise RuntimeError('db down')
    monkeypatch.setattr(htl.HostedTradingLoop, 'acknowledge_signal', boom)
    loop = htl.HostedTradingLoop(FakePool(db))
    with pytest.raises(RuntimeError):
        await loop.execute_trade(make_user(db, creds), make_signal(db))
    assert follower_guard.is_inflight(7) is False


async def test_every_trading_request_is_charged_to_the_meter(venue, db, creds, notes, fast_sleep):
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    total, guardian, by_source = hl_rate.window()
    assert by_source == {'trading': venue.weight} and guardian == 0


async def test_risk_log_prints_two_decimals(venue, db, creds, notes, fast_sleep, caplog):
    caplog.set_level(logging.INFO, logger='HOSTED_TRADING')
    loop = htl.HostedTradingLoop(FakePool(db))
    await loop.execute_trade(make_user(db, creds), make_signal(db, risk=0.0125))
    assert 'Risk: $125.00 (1.25%)' in caplog.text


def test_execute_trade_has_no_blind_sleep_2():
    tree = ast.parse(open(htl.__file__, encoding='utf-8').read())
    bodies = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
              and n.name in ('execute_trade', '_execute_trade_body')]
    assert len(bodies) == 2
    for fn in bodies:
        for node in ast.walk(fn):
            if isinstance(node, ast.Call) and ast.unparse(node.func) == 'asyncio.sleep':
                assert not (node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == 2)
