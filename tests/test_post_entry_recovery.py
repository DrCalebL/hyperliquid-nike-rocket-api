"""
T26 (R3-H1, I19, AC23): every execute_trade exit reached after the entry may have filled ends with an SL + an
'open' row, a verified-flat account with no service orders, or a needs_recovery row carrying the signal levels,
the qty and every known service leg id - plus the A3 cancel check and the A6 in-flight age alarm.
"""
import asyncio
from datetime import timedelta

import pytest

from tests.fakes import fake_hl
from tests.fakes.fake_hl import creds, fast_sleep, notes, venue  # noqa: F401  (pytest fixtures)
from tests.fakes.fake_pool import FakeDB, FakePool

import follower_guard
import hl_rate
import hosted_trading_loop as htl
import order_utils
import position_guardian as pg

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


# ---------------------------------------------------------------- recovery lane (C44 + A1, A2, A7)
class _Stop(BaseException):
    """Ends an otherwise endless guardian loop inside a test."""


@pytest.fixture
def g(venue, db, creds, notes, fast_sleep):
    guardian = pg.HLPositionGuardian(FakePool(db))
    guardian.world = fake_hl.World(guardian, db, venue)
    db.add_user(7, encrypt=creds)
    yield guardian
    guardian._pool.shutdown(wait=False)


def nr_row(g, db, **kw):
    """An ENTRY_UNCONFIRMED-style needs_recovery row written 8 s after its IOC (entry oid 555)."""
    fields = dict(user_id=7, status='needs_recovery', side='BUY', quantity=4000.0, target_sl=0.45,
                  target_tp=0.55, entry_order_id='555', entry_fill_price=None, opened_at=g.world.t)
    fields.update(kw)
    return db.add_position(**fields)


def lane_mutations(venue):
    return [c for c in venue.calls if c[1] in ('order', 'cancel')]


async def tick(g, mode, seconds=pg.LANE_S):
    await g._recovery_lane(mode)
    g.world.advance(seconds)


async def test_enforce_lane_places_sl_then_tp_then_restores_the_row(g, db, venue):
    row = nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5012)
    await tick(g, 'enforce')
    assert [a[5] for a in venue.order_calls()] == [order_utils.sl_order_type(0.45), order_utils.tp_order_type(0.55)]
    assert db.positions[row['id']]['status'] == 'needs_recovery'
    await tick(g, 'enforce')
    r = db.positions[row['id']]
    assert (r['status'], r['quantity'], r['entry_fill_price']) == ('open', 4000.0, 0.5012)
    assert any(s.startswith('/* hlg:row_open */') and "status = 'needs_recovery'" in s for _, s, _ in db.log)


async def test_shadow_lane_closes_and_marks_closed(g, db, venue, notes):
    row = nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'shadow')
    assert venue.szi(WALLET, 'ADA') == 0.0
    assert db.positions[row['id']]['status'] == 'closed'
    assert any('GUARDIAN_RECOVERY_CLOSE' in t for t, _, _ in notes)


async def test_off_lane_acts_on_a_row_and_is_silent_without_one(g, db, venue):
    await tick(g, 'off')
    assert venue.calls == [] and venue.ctors == []                           # AC12: zero Info/Exchange calls
    row = nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'off')
    assert db.positions[row['id']]['status'] == 'closed'


async def test_never_filled_row_is_closed_manual_only_after_240s_confirmed_flat(g, db, venue):
    row = nr_row(g, db)
    for _ in range(7):                                                       # flat for 0, 35, ..., 210 s
        await tick(g, 'enforce')
        assert db.positions[row['id']]['status'] == 'needs_recovery'
    await tick(g, 'enforce')                                                 # 245 s of confirmed flat
    assert db.positions[row['id']]['status'] == 'closed_manual'
    assert lane_mutations(venue) == []


async def test_shadow_leftover_sl_row_is_cancelled_then_closed_manual(g, db, venue):
    sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.45, 'Stop Market')        # flat account, leftover service SL
    row = nr_row(g, db, sl_order_id=str(sl))
    for _ in range(10):
        await tick(g, 'shadow')
    assert db.positions[row['id']]['status'] == 'closed_manual'
    assert venue.orders(WALLET) == []
    assert [a[2] for t, n, a in venue.calls if n == 'cancel'] == [sl]


async def test_shadow_close_failed_row_is_closed_and_its_sl_cancelled(g, db, venue):
    sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.45, 'Stop Market')
    row = nr_row(g, db, sl_order_id=str(sl))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'shadow')
    assert venue.szi(WALLET, 'ADA') == 0.0 and venue.orders(WALLET) == []
    assert db.positions[row['id']]['status'] == 'closed'


async def test_lane_honours_the_token_but_not_an_acknowledgment_older_than_the_row(g, db, venue):
    sig = db.add_signal('sig-5', created_at=g.world.t - timedelta(seconds=30))
    db.add_delivery(7, sig['id'], acknowledged_at=g.world.t - timedelta(seconds=20))   # the row's own delivery
    row = nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    tok = follower_guard.mark(7)
    await tick(g, 'shadow', 0)
    assert lane_mutations(venue) == []                                       # in-process token: yield
    follower_guard.clear(7, tok)
    await tick(g, 'shadow', 0)
    assert db.positions[row['id']]['status'] == 'closed'                     # acknowledged_at before the row: act


async def test_busy_turning_true_before_decide_yields_and_never_restores_a_naked_row(g, db, venue, monkeypatch):
    """I12/I17/I19: decide(in_flight=True) returns [], which must never read as an empty plan. _busy is False at
    _lane_user's two checks and True at _invariant's (a new execute_trade, or an acknowledgment after the row)."""
    row = nr_row(g, db)                                                      # ENTRY_UNCONFIRMED: no SL, no TP
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    answers = [False, False, True]

    async def busy(user_id, since=None):
        return answers.pop(0) if answers else False
    monkeypatch.setattr(g, '_busy', busy)
    await tick(g, 'enforce')
    assert answers == []                                                     # the 3rd check precedes decide()
    assert db.positions[row['id']]['status'] == 'needs_recovery'
    assert lane_mutations(venue) == [] and db.writes() == []
    assert db.errors('GUARDIAN_YIELD_INFLIGHT') and not db.errors('GUARDIAN_RECOVERY_RESTORED')
    await tick(g, 'enforce')                                                 # no longer busy: SL first, then TP
    assert [a[5]['trigger']['tpsl'] for a in venue.order_calls()] == ['sl', 'tp']
    assert db.positions[row['id']]['status'] == 'needs_recovery'


async def test_open_and_needs_recovery_rows_on_one_coin_are_ambiguous(g, db, venue):
    db.add_position(user_id=7, status='open', opened_at=g.world.t - timedelta(minutes=5))
    nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'shadow')
    await tick(g, 'enforce')
    assert lane_mutations(venue) == [] and db.errors('GUARDIAN_ROW_AMBIGUOUS')


async def test_invalid_levels_use_the_legacy_close_even_in_enforce(g, db, venue):
    row = nr_row(g, db, target_sl=0.60)                                        # inverted for a long
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'enforce')
    assert db.errors('GUARDIAN_ROW_LEVELS_INVALID')
    assert db.positions[row['id']]['status'] == 'closed' and venue.szi(WALLET, 'ADA') == 0.0


async def test_a7_intact_service_bracket_restores_the_row_instead_of_closing(g, db, venue):
    sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.45, 'Stop Market')
    tp = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.55, 'Take Profit Limit')
    row = nr_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))             # POST_ENTRY_EXCEPTION after the TP
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'shadow')
    assert lane_mutations(venue) == []
    assert db.positions[row['id']]['status'] == 'open'
    assert db.errors('GUARDIAN_RECOVERY_RESTORED')


async def test_a1_saturated_meter_shadow_lane_still_closes_within_one_tick(g, db, venue, monkeypatch):
    hl_rate.charge(hl_rate.CEILING + 100, 'monitor')                          # window pinned above the ceiling

    async def never(*a, **k):
        raise AssertionError('the recovery lane must never wait on the meter')
    monkeypatch.setattr(hl_rate, 'acquire_background', never)
    row = nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'shadow')
    assert db.positions[row['id']]['status'] == 'closed'


async def test_a1_saturated_meter_enforce_lane_brackets_sl_first_within_one_tick(g, db, venue, monkeypatch):
    hl_rate.charge(hl_rate.CEILING + 100, 'monitor')

    async def never(*a, **k):
        raise AssertionError('the recovery lane must never wait on the meter')
    monkeypatch.setattr(hl_rate, 'acquire_background', never)
    nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await tick(g, 'enforce')
    assert [a[5]['trigger']['tpsl'] for a in venue.order_calls()] == ['sl', 'tp']


async def test_a1_every_needs_recovery_row_is_acted_on_within_60s_of_insert(g, db, venue):
    hl_rate.charge(hl_rate.CEILING + 100, 'monitor')
    seen = {}

    def insert_then_watch():
        if 'at' not in seen:                                                 # row written right after tick 1
            seen['at'] = g.world.epoch()
            seen['row'] = nr_row(g, db)
            venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
        elif db.positions[seen['row']['id']]['status'] == 'closed':
            seen['done'] = g.world.epoch()
            raise _Stop()
    g.world.on_sleep.append(insert_then_watch)
    with pytest.raises(_Stop):
        await g._lane_loop()
    assert seen['done'] - seen['at'] <= 60
    assert all(s <= pg.LANE_S for s in g.world.slept)


async def test_a2_own_entry_fill_inside_the_pad_does_not_make_the_row_ambiguous(g, db, venue):
    ioc_ms = int((g.world.epoch() - 8) * 1000)                                 # the IOC filled 8 s before the row
    row = nr_row(g, db)
    await tick(g, 'shadow')                                                  # confirmed flat #1 (fill not visible)
    await tick(g, 'shadow')                                                  # confirmed flat #2
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)                           # the position appears late
    venue.add_fill(WALLET, 'ADA', 'B', 4000, 0.5, 'Open Long', ioc_ms, 555)  # only its own entry fill ...
    venue.add_fill(WALLET, 'ADA', 'B', 4000, 0.5, 'Open Long', int(g.world.epoch() * 1000) - 1000, 555)  # ... late-stamped
    await tick(g, 'shadow')
    assert not db.errors('GUARDIAN_ROW_AMBIGUOUS')
    assert db.errors('GUARDIAN_FALSE_FLAT_CLEARED')
    assert db.positions[row['id']]['status'] == 'closed' and venue.szi(WALLET, 'ADA') == 0.0


async def test_expired_inflight_tokens_are_reported_as_critical_alarms(g, db, venue, notes):
    """A6: a token that aged out while its task still ran is recorded by follower_guard; every lane tick reports it."""
    follower_guard.drain_expired()                                           # drop events left by other tests
    tok = follower_guard.mark(7)                                             # this test's task is still running
    assert follower_guard.is_inflight(7, max_age_s=0.0) is False             # aged out -> recorded as expired
    follower_guard.clear(7, tok)

    def stop_after_one_tick():
        raise _Stop()
    g.world.on_sleep.append(stop_after_one_tick)
    with pytest.raises(_Stop):
        await g._lane_loop()                                                 # one lane tick, no needs_recovery row
    [row] = db.errors('ALARM_INFLIGHT_TOKEN_EXPIRED')
    assert row['context']['user_id'] == '7'
    assert any(t == 'INFLIGHT_TOKEN_EXPIRED (HL)' and lvl == 'error' for t, lvl, _ in notes)
    await g._report_expired_tokens()
    assert len(db.errors('ALARM_INFLIGHT_TOKEN_EXPIRED')) == 1                # drained: reported once


async def test_a2_enforce_lane_brackets_after_a_false_flat(g, db, venue):
    """The IOC raised, so the row never learned its entry oid: only since_ms(row)'s opened_at clamp excludes the fill."""
    ioc_ms = int((g.world.epoch() - 8) * 1000)
    nr_row(g, db, entry_order_id='unknown-after-exception')
    await tick(g, 'enforce')
    await tick(g, 'enforce')
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.add_fill(WALLET, 'ADA', 'B', 4000, 0.5, 'Open Long', ioc_ms, 555)
    await tick(g, 'enforce')
    assert [a[5]['trigger']['tpsl'] for a in venue.order_calls()] == ['sl', 'tp']
    assert not db.errors('GUARDIAN_ROW_AMBIGUOUS')


async def test_a1_a_full_pass_blocked_on_the_meter_does_not_delay_the_lane(g, db, venue, creds):
    hl_rate.charge(hl_rate.CEILING + 100, 'monitor')                          # the pass will wait on the meter
    db.add_user(8, encrypt=creds)
    db.add_position(user_id=8, status='open', opened_at=g.world.t - timedelta(minutes=5))
    pass_task = asyncio.ensure_future(g._pass(dry=False, start=0, deadline=g.world.mono + pg.PASS_S))
    for _ in range(5):
        await asyncio.sleep(0)
    assert not pass_task.done()                                              # blocked in acquire_background
    row = nr_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await g._recovery_lane('shadow')                                         # runs while the pass is blocked
    assert db.positions[row['id']]['status'] == 'closed'
    assert not pass_task.done()
    pass_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pass_task
