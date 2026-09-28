"""
T26 (R3-H1, I19, AC23): every execute_trade exit reached after the entry may have filled ends with an SL + an
'open' row, a verified-flat account with no service orders, or a needs_recovery row carrying the signal levels,
the qty and every known service leg id - plus the A3 cancel check and the A6 in-flight age alarm.
"""
import pytest

from tests.fakes import fake_hl
from tests.fakes.fake_hl import creds, fast_sleep, notes, venue  # noqa: F401  (pytest fixtures)
from tests.fakes.fake_pool import FakeDB, FakePool

import follower_guard
import hosted_trading_loop as htl

WALLET = '0x' + '%040x' % 7


@pytest.fixture
def db():
    return FakeDB()


def make_user(db, encrypt, uid=7):
    u = db.add_user(uid, encrypt=encrypt)
    return {'id': uid, 'api_key': u['api_key'], 'email': u['email'],
            'hl_private_key_encrypted': u['hl_private_key_encrypted'], 'hl_wallet_address': u['hl_wallet_address']}


def make_signal(db, action='BUY', entry=0.5, sl=0.45, tp=0.55, risk=0.02):
    s = db.add_signal('sig-%d' % (len(db.signals) + 1))
    d = db.add_delivery(7, s['id'])
    return {'delivery_id': d['id'], 'signal_id': s['signal_id'], 'action': action, 'symbol': 'ADA/USDT',
            'entry_price': entry, 'stop_loss': sl, 'take_profit': tp, 'leverage': 5.0, 'risk_pct': risk}


def recovery_rows(db):
    return [r for r in db.positions.values() if r['status'] == 'needs_recovery']


def reject(tpsl):
    return lambda a, c, b, sz, px, ot, ro: (fake_hl.err_status('rejected')
                                            if 'trigger' in ot and ot['trigger']['tpsl'] == tpsl else NotImplemented)


def close_fails(a, c, b, sz, px, ot, ro):
    return fake_hl.err_status('could not immediately match') if 'limit' in ot and ro else NotImplemented


def unconfirmed_entry(venue):
    """The IOC answers 'resting' (no totalSz) and every user_state read after it times out."""
    sent = []

    def order(a, c, b, sz, px, ot, ro):
        if 'limit' in ot and not ro:
            sent.append(px)
            return fake_hl.ok_status({'resting': {'oid': 555}})
        return NotImplemented
    venue.script('order', order)
    venue.script('user_state', lambda *a: TimeoutError('HL read timeout') if sent else NotImplemented)


# ---------------------------------------------------------------- trading path (execute_trade)
async def test_unconfirmed_ioc_records_needs_recovery_and_sends_no_bracket(venue, db, creds, notes, fast_sleep):
    unconfirmed_entry(venue)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    [row] = recovery_rows(db)
    assert (row['entry_order_id'], row['quantity'], row['side']) == ('555', 4000.0, 'BUY')
    assert (row['target_sl'], row['target_tp'], row['sl_order_id'], row['tp_order_id']) == (0.45, 0.55, None, None)
    assert [a for a in venue.order_calls() if 'trigger' in a[5]] == []          # zero bracket orders
    assert any(t == 'ENTRY_UNCONFIRMED (HL)' and lvl == 'error' for t, lvl, _ in notes)
    assert db.errors('ENTRY_UNCONFIRMED') and db.errors('NEEDS_RECOVERY_RECORDED')
    assert fast_sleep.count(1.0) == 7                                          # 8 polls, 1 s apart


async def test_sl_failure_with_a_failed_close_records_needs_recovery_without_ids(venue, db, creds, notes, fast_sleep):
    venue.script('order', reject('sl'))
    venue.script('order', close_fails)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    [row] = recovery_rows(db)
    assert (row['sl_order_id'], row['tp_order_id'], row['quantity']) == (None, None, 4000.0)
    assert db.errors('NEEDS_RECOVERY_RECORDED')[0]['error_message'] == 'SL_FAILED_CLOSE_FAILED'


async def test_shadow_tp_failure_with_a_failed_close_keeps_the_sl_and_records_its_id(venue, db, creds, notes, fast_sleep, monkeypatch):
    monkeypatch.setenv('GUARDIAN_MODE', 'shadow')
    venue.script('order', reject('tp'))
    venue.script('order', close_fails)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    assert not [c for c in venue.calls if c[1] == 'cancel']                    # the SL is never cancelled
    [sl] = venue.orders(WALLET, 'ADA')
    [row] = recovery_rows(db)
    assert row['sl_order_id'] == str(sl['oid'])
    assert db.errors('NEEDS_RECOVERY_RECORDED')[0]['error_message'] == 'TP_FAILED_CLOSE_FAILED'


async def test_shadow_tp_failure_leftover_sl_cancel_inner_error_records_needs_recovery(venue, db, creds, notes, fast_sleep, monkeypatch):
    """A3: an envelope-'ok' cancel whose statuses[0] is an error is a failure -> row with sl_order_id."""
    monkeypatch.setenv('GUARDIAN_MODE', 'shadow')
    venue.script('order', reject('tp'))
    venue.script('cancel', lambda *a: {'status': 'ok', 'response': {'data': {'statuses': [{'error': 'Rate limited'}]}}})
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    assert venue.szi(WALLET, 'ADA') == 0.0                                     # closed first
    [sl] = venue.orders(WALLET, 'ADA')                                         # the SL survived the cancel attempts
    [row] = recovery_rows(db)
    assert row['sl_order_id'] == str(sl['oid'])
    assert db.errors('NEEDS_RECOVERY_RECORDED')[0]['error_message'] == 'LEFTOVER_SL_CANCEL_FAILED'


async def test_post_send_exception_with_the_position_open_records_needs_recovery(venue, db, creds, notes, fast_sleep, monkeypatch):
    async def boom(**kw):
        raise RuntimeError('SDK exploded')
    monkeypatch.setattr(htl, 'place_sl_order_with_retry', boom)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    [row] = recovery_rows(db)
    assert row['quantity'] == 4000.0 and row['sl_order_id'] is None
    assert db.errors('NEEDS_RECOVERY_RECORDED')[0]['error_message'] == 'POST_ENTRY_EXCEPTION:RuntimeError'
    assert db.errors('POST_ENTRY_EXCEPTION')


async def test_post_send_exception_when_verified_flat_cancels_the_known_legs(venue, db, creds, notes, fast_sleep, monkeypatch):
    async def stopped_out_then_boom(**kw):
        venue.set_position(WALLET, 'ADA', 0)                                  # the position is gone
        raise RuntimeError('TP call exploded')
    monkeypatch.setattr(htl, 'place_tp_order_with_retry', stopped_out_then_boom)
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    assert venue.orders(WALLET) == [] and recovery_rows(db) == []              # SL cancelled, nothing to recover


async def test_post_send_exception_flat_but_cancel_fails_records_the_leftover_id(venue, db, creds, notes, fast_sleep, monkeypatch):
    async def stopped_out_then_boom(**kw):
        venue.set_position(WALLET, 'ADA', 0)
        raise RuntimeError('TP call exploded')
    monkeypatch.setattr(htl, 'place_tp_order_with_retry', stopped_out_then_boom)
    venue.script('cancel', lambda *a: ConnectionError('reset'))
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    [sl] = venue.orders(WALLET, 'ADA')
    [row] = recovery_rows(db)
    assert row['sl_order_id'] == str(sl['oid']) and row['tp_order_id'] is None
    assert db.errors('NEEDS_RECOVERY_RECORDED')[0]['error_message'] == 'POST_ENTRY_EXCEPTION_LEFTOVER'


async def test_open_row_insert_failure_alarms_position_record_failed(venue, db, creds, notes, fast_sleep):
    db.fail_next.append(('INSERT INTO open_positions', RuntimeError('db down')))
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is True
    assert any(t == 'POSITION_RECORD_FAILED (HL)' and lvl == 'error' for t, lvl, _ in notes)
    assert len(venue.orders(WALLET, 'ADA')) == 2                              # the position keeps SL + TP


async def test_needs_recovery_insert_failure_notifies(venue, db, creds, notes, fast_sleep):
    unconfirmed_entry(venue)
    loop = htl.HostedTradingLoop(FakePool(db))
    db.fail_next.append(('INSERT INTO open_positions', RuntimeError('db down')))
    assert await loop.execute_trade(make_user(db, creds), make_signal(db)) is False
    assert any(t == 'NEEDS_RECOVERY_RECORD_FAILED (HL)' for t, _, _ in notes)
