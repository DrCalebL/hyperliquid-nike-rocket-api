"""
T27 (HL follower guardian, C44 / I1-I18): reads orders FIRST then position; mark-basis decisions; str oids with
int() on cancel; repairs with the order_utils trigger dicts requiring 'resting'; status-guarded id UPDATEs; the
follower row policy (ROW_AMBIGUOUS, flat 'open' rows left to position_monitor, rowless alarm-only); R3-M6 fill
evidence; in-flight yields; shadow = zero mutations; R3-M3 construction; budget / deferral / round-robin;
expired wallets; rounding parity; main.py registration.
"""
import ast
import logging
import os
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

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WALLET = '0x' + '%040x' % 7


class _Stop(BaseException):
    """Ends an otherwise endless guardian loop inside a test."""


@pytest.fixture
def db():
    return FakeDB()


@pytest.fixture
def g(venue, db, creds, notes, fast_sleep):
    guardian = pg.HLPositionGuardian(FakePool(db))
    guardian.world = fake_hl.World(guardian, db, venue)
    db.add_user(7, encrypt=creds)
    yield guardian
    guardian._pool.shutdown(wait=False)


def open_row(g, db, **kw):
    fields = dict(user_id=7, status='open', side='BUY', quantity=4000.0, target_sl=0.45, target_tp=0.55,
                  entry_order_id='900', opened_at=g.world.t - timedelta(minutes=10))
    fields.update(kw)
    return db.add_position(**fields)


def bracket(venue, sl=0.45, tp=0.55, qty=4000):
    return (venue.add_order(WALLET, 'ADA', 'A', qty, sl, 'Stop Market'),
            venue.add_order(WALLET, 'ADA', 'A', qty, tp, 'Take Profit Limit'))


async def run_pass(g, dry=False, start=0):
    return await g._pass(dry=dry, start=start, deadline=g.world.mono + pg.PASS_S)


def mutations(venue):
    return [c for c in venue.calls if c[1] in ('order', 'cancel')]


# ---------------------------------------------------------------- reads, repairs, closes (enforce)
async def test_enforce_repairs_a_missing_sl_after_reading_orders_first(g, db, venue):
    _, tp = bracket(venue)
    venue.external_cancel(WALLET, _)                                        # the SL is gone
    row = open_row(g, db, sl_order_id='111', tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    info = [n for t, n, _ in venue.calls if t == 'info']
    i = info.index('frontend_open_orders')
    assert info[i - 1] == 'user_state' and info[i + 1] == 'user_state'     # screen, orders FIRST, position SECOND
    [place] = venue.order_calls()
    assert place[5] == order_utils.sl_order_type(0.45) and place[2] is False and place[3] == 4000.0 and place[6]
    new_sl = next(o for o in venue.orders(WALLET, 'ADA') if o['orderType'] == 'Stop Market')
    assert db.positions[row['id']]['sl_order_id'] == str(new_sl['oid'])
    assert any(s.startswith('/* hlg:row_sl */') and 'status IN' in s for _, s, _ in db.log)
    assert db.errors('GUARDIAN_SL_REPAIRED')


async def test_enforce_closes_when_the_mark_is_beyond_the_sl(g, db, venue, notes):
    sl, tp = bracket(venue)
    venue.external_cancel(WALLET, sl)
    open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.marks['ADA'] = 0.44                                                # long: mark at/below the SL
    await run_pass(g)
    closes = [a for a in venue.order_calls() if 'limit' in a[5] and a[6]]
    assert len(closes) == 1 and venue.szi(WALLET, 'ADA') == 0.0
    assert not [a for a in venue.order_calls() if 'trigger' in a[5]]
    assert db.errors('GUARDIAN_CLOSE') and any('GUARDIAN_CLOSE' in t for t, _, _ in notes)
    assert all(r['status'] == 'open' for r in db.positions.values())         # position_monitor records the close


async def test_enforce_replaces_a_missing_tp_and_never_closes(g, db, venue):
    sl, tp = bracket(venue)
    venue.external_cancel(WALLET, tp)
    row = open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    [place] = venue.order_calls()
    assert place[5] == order_utils.tp_order_type(0.55)
    assert venue.szi(WALLET, 'ADA') == 4000.0
    new_tp = next(o for o in venue.orders(WALLET, 'ADA') if o['orderType'] == 'Take Profit Limit')
    assert db.positions[row['id']]['tp_order_id'] == str(new_tp['oid'])


async def test_service_leg_cancelled_by_int_oid_and_customer_leg_kept(g, db, venue):
    sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.45, 'Stop Market')
    wrong_tp = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.60, 'Take Profit Limit')
    customer = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.58, 'Limit')          # customer's own reduce-only limit
    open_row(g, db, sl_order_id=str(sl), tp_order_id=str(wrong_tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    cancels = [a[2] for t, n, a in venue.calls if n == 'cancel']
    assert cancels == [wrong_tp] and isinstance(cancels[0], int)
    assert customer in [o['oid'] for o in venue.orders(WALLET, 'ADA')]
    assert db.errors('GUARDIAN_FOREIGN_LEG_KEPT')
    assert [a[5] for a in venue.order_calls()] == [order_utils.tp_order_type(0.55)]


# ---------------------------------------------------------------- row policy (I17)
async def test_two_active_rows_are_ambiguous_with_zero_mutations(g, db, venue, notes):
    bracket(venue)
    open_row(g, db)
    open_row(g, db, target_sl=0.46, target_tp=0.49, opened_at=g.world.t - timedelta(minutes=2))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    assert mutations(venue) == [] and db.writes() == []
    assert db.errors('GUARDIAN_ROW_AMBIGUOUS') and any('ROW_AMBIGUOUS' in t for t, _, _ in notes)


async def test_open_row_on_a_flat_coin_gets_no_cancel_and_alarms_after_600s_hourly(g, db, venue):
    sl, tp = bracket(venue)
    venue.external_cancel(WALLET, sl)                                          # SL filled: position flat, TP rests
    open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    await run_pass(g)
    g.world.advance(700)
    await run_pass(g)
    g.world.advance(300)
    await run_pass(g)
    assert mutations(venue) == [] and db.writes() == []
    assert len(db.errors('GUARDIAN_FLAT_ROW_UNRECORDED')) == 1


@pytest.mark.parametrize('legs,code', [
    ([('Take Profit Limit', 0.55, False)], 'UNTRACKED_NAKED_POSITION'),         # TP-only: no exit-side stop
    ([('Stop Limit', 0.45, False)], 'UNTRACKED_POSITION_NO_ROW'),
    ([('Stop Market', 0.45, False), ('Limit', 0.52, False), ('Limit', 0.55, False)], 'UNTRACKED_POSITION_NO_ROW'),
    ([('Stop Market', 0.45, True)], 'UNTRACKED_POSITION_NO_ROW'),               # positionTpsl row, sz '0.0'
    ([], 'UNTRACKED_NAKED_POSITION'),
])
async def test_rowless_positions_only_alarm_once_per_hour(g, db, venue, notes, legs, code):
    for kind, level, whole in legs:
        venue.add_order(WALLET, 'ADA', 'A', 4000, level, kind, position_tpsl=whole)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    g.world.advance(600)
    await run_pass(g)
    assert mutations(venue) == [] and db.writes() == []
    assert len(db.errors('GUARDIAN_' + code)) == 1
    assert (len([t for t, _, _ in notes if code in t]) == 1) == (code == 'UNTRACKED_NAKED_POSITION')


async def test_customer_position_tpsl_at_the_recorded_level_is_the_sl(g, db, venue):
    customer_sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.45, 'Stop Market', position_tpsl=True)
    tp = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.55, 'Take Profit Limit')
    open_row(g, db, sl_order_id=None, tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    assert mutations(venue) == []
    assert customer_sl in [o['oid'] for o in venue.orders(WALLET, 'ADA')]


# ---------------------------------------------------------------- R3-M6 evidence (fills decide on HL)
def _false_flat(venue, reads):
    left = [reads]
    venue.script('user_state', lambda *a: (left.__setitem__(0, left[0] - 1) or
                                           {'assetPositions': [], 'marginSummary': {'accountValue': '1'}})
                 if left[0] > 0 else NotImplemented)


async def _two_confirmed_flats(g, db, venue):
    sl, tp = bracket(venue)
    row = open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.add_fill(WALLET, 'ADA', 'B', 4000, 0.5, 'Open Long', int((g.world.epoch() - 600) * 1000), 900)
    _false_flat(venue, 6)                                    # screen + book + CONFIRM read, two passes
    await run_pass(g)
    g.world.advance(240)
    await run_pass(g)
    venue.external_cancel(WALLET, sl)                         # meanwhile the SL was removed
    g.world.advance(240)
    return row


async def test_same_position_after_confirmed_false_flats_is_cleared_and_repaired(g, db, venue):
    row = await _two_confirmed_flats(g, db, venue)
    await run_pass(g)
    assert db.errors('GUARDIAN_FALSE_FLAT_CLEARED')
    assert [a[5] for a in venue.order_calls()] == [order_utils.sl_order_type(0.45)]
    assert 'user_fills_by_time' in venue.names('info')
    assert db.positions[row['id']]['status'] == 'open'


async def test_an_open_long_fill_after_the_flat_is_new_entry_evidence(g, db, venue):
    await _two_confirmed_flats(g, db, venue)
    venue.add_fill(WALLET, 'ADA', 'B', 4000, 0.5, 'Open Long', int(g.world.epoch() * 1000), 999)   # same entryPx
    await run_pass(g)
    assert db.errors('GUARDIAN_ROW_AMBIGUOUS') and mutations(venue) == []


# ---------------------------------------------------------------- in-flight (I12), shadow, construction (R3-M3)
async def _leftover_service_sl(g, db, venue):
    sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.45, 'Stop Market')
    db.add_position(user_id=7, status='closed', sl_order_id=str(sl), opened_at=g.world.t - timedelta(hours=1),
                    last_fill_at=g.world.t - timedelta(minutes=10))
    return sl


async def test_rowless_flat_leftover_service_leg_is_cancelled_after_confirm(g, db, venue):
    sl = await _leftover_service_sl(g, db, venue)
    await run_pass(g)
    assert [a[2] for t, n, a in venue.calls if n == 'cancel'] == [sl]


async def test_execute_trade_starting_during_confirm_flat_means_zero_mutations(g, db, venue):
    await _leftover_service_sl(g, db, venue)
    tokens = []
    g.world.on_sleep.append(lambda: tokens.append(follower_guard.mark(7)))
    await run_pass(g)
    for tok in tokens:
        follower_guard.clear(7, tok)
    assert mutations(venue) == [] and db.errors('GUARDIAN_YIELD_INFLIGHT')


async def test_recent_acknowledgment_skips_the_user(g, db, venue):
    sig = db.add_signal('sig-9', created_at=g.world.t - timedelta(minutes=5))
    db.add_delivery(7, sig['id'], acknowledged_at=g.world.t - timedelta(seconds=60))
    open_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    assert [n for t, n, _ in venue.calls if t == 'info'] == ['meta_and_asset_ctxs']
    assert mutations(venue) == []


async def test_shadow_pass_makes_zero_mutations_and_logs_would(g, db, venue):
    _, tp = bracket(venue)
    venue.external_cancel(WALLET, _)
    open_row(g, db, sl_order_id='111', tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g, dry=True)
    assert mutations(venue) == [] and db.writes() == []
    assert db.errors('GUARDIAN_WOULD_PLACE_SL')
    assert not [c for c in venue.ctors if c[0] == 'Exchange']


async def test_qc_voice1_shadow_pass_raises_a_critical_alarm_for_a_naked_rowed_position(g, db, venue, notes):
    """QC voice-1: in shadow a service-opened, rowed position with no SL was only an info GUARDIAN_WOULD_PLACE_SL
    row, quieter than a rowless one. It now also raises a critical, notified GUARDIAN_SHADOW_SL_MISSING
    (deduped hourly), with zero mutations."""
    sl, tp = bracket(venue)
    venue.external_cancel(WALLET, sl)                                        # the customer cancelled the SL
    open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g, dry=True)
    g.world.advance(pg.PASS_S)
    await run_pass(g, dry=True)
    assert mutations(venue) == []
    assert db.errors('GUARDIAN_SHADOW_SL_MISSING')
    assert [t for t, lvl, _ in notes if t == 'GUARDIAN_SHADOW_SL_MISSING (HL)' and lvl == 'error'] == [
        'GUARDIAN_SHADOW_SL_MISSING (HL)']


async def test_qc_voice1_a_shadow_sl_at_a_moved_level_is_not_reported_missing(g, db, venue, notes):
    venue.add_order(WALLET, 'ADA', 'A', 4000, 0.44, 'Stop Market')          # an SL rests, away from the row level
    tp = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.55, 'Take Profit Limit')
    open_row(g, db, sl_order_id='111', tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g, dry=True)
    assert db.errors('GUARDIAN_WOULD_PLACE_SL') and not db.errors('GUARDIAN_SHADOW_SL_MISSING')


async def test_qc_voice15_every_full_pass_logs_a_guardian_pass_heartbeat(g, db, venue, creds, caplog):
    """QC voice-15: the 48 h shadow review needs proof that each pass ran and evaluated users."""
    caplog.set_level(logging.INFO, logger='POSITION_GUARDIAN')
    _, tp = bracket(venue)
    venue.external_cancel(WALLET, _)
    open_row(g, db, sl_order_id='111', tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    db.add_user(8, encrypt=creds)                                            # active, flat, no rows: screen only
    await run_pass(g, dry=True)
    [line] = [r.getMessage() for r in caplog.records if r.getMessage().startswith('GUARDIAN_PASS ')]
    assert 'dry=True users=2/2 books_read=1 would=' in line and 'read_failures=0 elapsed=' in line
    assert 'would=0' not in line


async def test_enforce_builds_one_exchange_per_user_with_cached_meta_and_meters_everything(g, db, venue):
    sl, tp = bracket(venue)
    venue.external_cancel(WALLET, sl)
    venue.external_cancel(WALLET, tp)
    open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    exchanges = [c for c in venue.ctors if c[0] == 'Exchange']
    assert len(exchanges) == 1 and exchanges[0][1]['meta'] and exchanges[0][1]['spot_meta']
    assert venue.ctor_requests == 2                                            # only the guardian's own Info
    total, guardian, by_source = hl_rate.window()
    assert by_source == {'guardian': venue.weight}                             # every request metered
    assert len(venue.order_calls()) == 2                                       # SL then TP


async def test_off_mode_pass_loop_makes_no_hyperliquid_call(g, db, venue, monkeypatch):
    monkeypatch.setenv('GUARDIAN_MODE', 'off')
    open_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)

    def stop():
        raise _Stop()
    g.world.on_sleep.append(stop)
    with pytest.raises(_Stop):
        await g._pass_loop()
    assert venue.calls == [] and venue.ctors == []


# ---------------------------------------------------------------- budget, deferral, round-robin, wallets
async def test_no_full_pass_reads_within_180s_of_a_signal(g, db, venue, caplog):
    caplog.set_level(logging.INFO, logger='POSITION_GUARDIAN')
    db.add_signal('sig-new', created_at=g.world.t - timedelta(seconds=60))
    open_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)

    def stop():
        raise _Stop()
    g.world.on_sleep.append(stop)
    with pytest.raises(_Stop):
        await g._pass_loop()
    assert venue.calls == [] and 'GUARDIAN_PASS_DEFERRED_SIGNAL' in caplog.text


async def test_meter_block_raises_guardian_budget_and_the_next_pass_resumes_round_robin(g, db, venue, creds, monkeypatch):
    for uid in (8, 9):
        db.add_user(uid, encrypt=creds)
    for uid in (7, 8, 9):                                      # healthy bracketed positions: 3 reads per user
        wallet = '0x' + '%040x' % uid
        sl = venue.add_order(wallet, 'ADA', 'A', 4000, 0.45, 'Stop Market')
        tp = venue.add_order(wallet, 'ADA', 'A', 4000, 0.55, 'Take Profit Limit')
        venue.set_position(wallet, 'ADA', 4000.0, 0.5)
        db.add_position(user_id=uid, status='open', sl_order_id=str(sl), tp_order_id=str(tp),
                        opened_at=g.world.t - timedelta(minutes=10))
    answers = iter([True] * 5 + [False] * 50)                  # Info ctor, marks, user 7 (3 reads), then blocked

    async def fake_acquire(weight, source='guardian', max_wait_s=240.0):
        return next(answers)
    monkeypatch.setattr(hl_rate, 'acquire_background', fake_acquire)
    assert await run_pass(g) == 1
    assert db.errors('GUARDIAN_BUDGET')
    monkeypatch.setattr(hl_rate, 'acquire_background', lambda *a, **k: _true())
    venue.calls.clear()
    assert await run_pass(g, start=1) == 0
    screens = [a[0] for t, n, a in venue.calls if n == 'user_state']
    assert screens[0] == '0x' + '%040x' % 8                    # resumed with user 8


async def _true():
    return True


async def test_open_row_users_come_first(g, db, venue, creds):
    db.add_user(3, encrypt=creds)                              # active user without rows
    open_row(g, db)                                            # user 7 has an 'open' row
    users = await g._fetch(pg.SQL_PASS_USERS)
    rows = await g._fetch(pg.SQL_PASS_ROWS)
    order = g._pass_order(users, {7: rows})
    assert [u['user_id'] for u in order] == [7, 3]


async def test_expired_wallet_alarms_once_per_hour_without_mutations(g, db, venue, notes):
    db.users[7]['api_wallet_expires_at'] = g.world.t - timedelta(days=1)
    _, tp = bracket(venue)
    venue.external_cancel(WALLET, _)
    open_row(g, db, sl_order_id='111', tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    await run_pass(g)
    g.world.advance(600)
    await run_pass(g)
    assert mutations(venue) == []
    assert len(db.errors('GUARDIAN_CANNOT_TRADE')) == 1
    assert len([t for t, _, _ in notes if 'GUARDIAN_CANNOT_TRADE' in t]) == 1


# ---------------------------------------------------------------- parity, wiring
def test_round_size_and_round_price_equal_the_trading_loops(venue, db):
    loop = htl.HostedTradingLoop(FakePool(db))
    guardian = pg.HLPositionGuardian(FakePool(db))
    guardian._szd = {a['name']: a['szDecimals'] for a in venue.meta['universe']}
    for coin, size in (('ADA', 4000.987), ('ADA', 0.9), ('BTC', 0.0123456), ('XYZ', 1.23456)):
        assert guardian._round_size(coin, size) == loop.round_size(coin, size)
    for price in (0.45123456, 0.5, 61234.567, 1.0000049, 0.000123456):
        assert guardian._round_price(price) == loop.round_price(price)
    guardian._pool.shutdown(wait=False)


def test_qc_round_size_never_shaves_a_lot_off_an_exact_lot_multiple(venue, db):
    """QC correctness-followers-5 / money-followers-7: math.floor(0.29 * 100) / 100 == 0.28 (binary float), so an
    exact fill size (totalSz / szi, already on the lot grid) was bracketed one lot short. Every exact k-lot size
    must round to itself in both the trading loop and the guardian, and sizes between lots still floor."""
    venue.meta['universe'].append({'name': 'SOL', 'szDecimals': 2})
    venue.meta['universe'].append({'name': 'ETH', 'szDecimals': 4})
    loop = htl.HostedTradingLoop(FakePool(db))
    guardian = pg.HLPositionGuardian(FakePool(db))
    guardian._szd = {a['name']: a['szDecimals'] for a in venue.meta['universe']}
    for coin, d in (('SOL', 2), ('ETH', 4), ('BTC', 5)):
        for k in range(1, 20001):
            size = float('%.*f' % (d, k / 10 ** d))
            assert loop.round_size(coin, size) == size, (coin, size)
            assert guardian._round_size(coin, size) == size, (coin, size)
    assert loop.round_size('SOL', 1.139) == 1.13 and guardian._round_size('SOL', 1.139) == 1.13
    assert loop.round_size('SOL', 0.2899999) == 0.28                             # truly below 0.29: floors
    assert loop.round_size('ADA', 4000.987) == 4000.0
    guardian._pool.shutdown(wait=False)


async def test_run_starts_the_lane_and_the_pass_as_separate_loops(g, monkeypatch):
    started = []

    async def lane():
        started.append('lane')

    async def full():
        started.append('pass')
    monkeypatch.setattr(g, '_lane_loop', lane)
    monkeypatch.setattr(g, '_pass_loop', full)
    await g.run()
    assert sorted(started) == ['lane', 'pass'] and g.world.slept == [pg.START_DELAY_S]


async def test_qc_cov4_the_full_pass_starts_while_the_lane_loop_never_returns(g, monkeypatch):
    """QC coverage-followers-4: run() must run the two loops concurrently; awaiting them one after the other would
    never start the full pass (the lane loop never returns)."""
    import asyncio
    started = asyncio.Event()

    async def lane():
        await asyncio.Event().wait()                                         # never returns, like the real loop

    async def full():
        started.set()
    monkeypatch.setattr(g, '_lane_loop', lane)
    monkeypatch.setattr(g, '_pass_loop', full)
    task = asyncio.ensure_future(g.run())
    try:
        await asyncio.wait_for(started.wait(), 2.0)
    finally:
        task.cancel()


# ---------------------------------------------------------------- churn, crossed TP, blind reads, retired oids
async def test_sl_removed_after_every_pass_is_replaced_3_times_then_closed(g, db, venue, notes):
    sl, tp = bracket(venue)
    row = open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    for _ in range(4):
        for o in [o for o in venue.orders(WALLET, 'ADA') if o['orderType'] == 'Stop Market']:
            venue.external_cancel(WALLET, o['oid'])                        # an external canceller
        await run_pass(g)
        g.world.advance(pg.PASS_S)
    assert len([a for a in venue.order_calls() if 'trigger' in a[5]]) == 3
    assert venue.szi(WALLET, 'ADA') == 0.0                                 # 4th repair -> SL_UNMAINTAINABLE close
    assert db.errors('GUARDIAN_CHURN') and db.errors('GUARDIAN_CLOSE')
    assert db.positions[row['id']]['status'] == 'open'                     # position_monitor records it


async def test_tp_rejected_every_pass_never_gates_the_sl(g, db, venue):
    sl, tp = bracket(venue)
    venue.external_cancel(WALLET, tp)
    open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('PERCENT_PRICE')
                 if 'trigger' in ot and ot['trigger']['tpsl'] == 'tp' else NotImplemented)
    await run_pass(g)                                                      # TP rejected -> backoff
    venue.external_cancel(WALLET, sl)
    g.world.advance(pg.PASS_S)
    await run_pass(g)
    assert [a[5]['trigger']['tpsl'] for a in venue.order_calls()] == ['tp', 'sl']
    assert db.errors('GUARDIAN_TP_PLACE_FAILED') and db.errors('GUARDIAN_SL_REPAIRED')


async def test_a_crossed_tp_is_never_placed(g, db, venue):
    sl, tp = bracket(venue)
    venue.external_cancel(WALLET, tp)
    open_row(g, db, sl_order_id=str(sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.marks['ADA'] = 0.56                                              # long: mark already above the TP
    await run_pass(g)
    assert mutations(venue) == [] and db.errors('GUARDIAN_TP_LEVEL_CROSSED')


async def test_three_failed_reads_raise_guardian_blind_and_never_act(g, db, venue, notes):
    open_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('frontend_open_orders', lambda *a: ConnectionError('reset'))
    for _ in range(3):
        await run_pass(g)
        g.world.advance(pg.PASS_S)
    assert mutations(venue) == []
    assert db.errors('GUARDIAN_READ_FAILED') and db.errors('GUARDIAN_BLIND')


async def test_an_unrecognised_orders_shape_is_read_unknown_with_zero_mutations(g, db, venue):
    open_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('frontend_open_orders', lambda *a: {'error': 'unexpected'})
    await run_pass(g)
    assert mutations(venue) == [] and db.errors('GUARDIAN_READ_UNKNOWN')


async def test_a_replaced_sl_stays_service_owned_after_the_row_moves_on(g, db, venue):
    old_sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.44, 'Stop Market')    # service SL at a wrong level
    tp = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.55, 'Take Profit Limit')
    row = open_row(g, db, sl_order_id=str(old_sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('cancel', lambda *a: ConnectionError('reset'), times=3)       # the first cancel attempt fails
    await run_pass(g)
    assert db.positions[row['id']]['sl_order_id'] != str(old_sl)               # the row now names the new SL
    assert old_sl in [o['oid'] for o in venue.orders(WALLET, 'ADA')]
    g.world.advance(pg.PASS_S)
    await run_pass(g)
    assert old_sl not in [o['oid'] for o in venue.orders(WALLET, 'ADA')]       # retired oid: still ours


async def test_a_slow_pass_raises_guardian_pass_slow(g, db, venue, monkeypatch):
    open_row(g, db)
    real = g._pass_user

    async def slow(*a, **k):
        g.world.advance(0.9 * pg.PASS_S)
        return await real(*a, **k)
    monkeypatch.setattr(g, '_pass_user', slow)
    await g._pass(dry=True, start=0, deadline=g.world.mono + 10 * pg.PASS_S)
    assert db.errors('GUARDIAN_PASS_SLOW')


def test_hl_weight_line_every_300s(g, caplog):
    caplog.set_level(logging.INFO, logger='POSITION_GUARDIAN')
    g._log_weight_every(pg.WEIGHT_LOG_S)
    g.world.advance(100)
    g._log_weight_every(pg.WEIGHT_LOG_S)
    g.world.advance(200)
    g._log_weight_every(pg.WEIGHT_LOG_S)
    assert caplog.text.count('HL_WEIGHT total=') == 2


async def test_the_recovery_lane_runs_inside_the_signal_window(g, db, venue):
    db.add_signal('sig-new', created_at=g.world.t - timedelta(seconds=30))
    row = db.add_position(user_id=7, status='needs_recovery', quantity=4000.0, opened_at=g.world.t,
                          entry_order_id='555')
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)

    def stop():
        raise _Stop()
    g.world.on_sleep.append(stop)
    with pytest.raises(_Stop):
        await g._lane_loop()
    assert db.positions[row['id']]['status'] == 'closed'


async def test_a_failed_recovery_close_alarms_and_is_retried_next_tick(g, db, venue, notes):
    row = db.add_position(user_id=7, status='needs_recovery', quantity=4000.0, opened_at=g.world.t,
                          entry_order_id='555')
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('no liquidity')
                 if 'limit' in ot and ro else NotImplemented, times=3)
    await g._recovery_lane('shadow')
    assert db.positions[row['id']]['status'] == 'needs_recovery'
    assert db.errors('GUARDIAN_RECOVERY_CLOSE_FAILED')
    g.world.advance(pg.LANE_S)
    await g._recovery_lane('shadow')
    assert db.positions[row['id']]['status'] == 'closed'


def test_main_registers_the_guardian_right_after_the_expiry_checker():
    with open(os.path.join(ROOT, 'main.py'), encoding='utf-8') as f:
        tree = ast.parse(f.read())
    assert any(isinstance(n, ast.ImportFrom) and n.module == 'position_guardian'
               and [a.name for a in n.names] == ['start_position_guardian'] for n in tree.body)
    startup = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'startup_event')
    calls = sorted((c for c in ast.walk(startup) if isinstance(c, ast.Call)
                    and ast.unparse(c.func) == 'asyncio.create_task' and isinstance(c.args[0], ast.Call)),
                   key=lambda c: c.lineno)
    started = [ast.unparse(c.args[0]) for c in calls]
    assert started.index('start_position_guardian(db_pool)') == started.index('start_expiry_checker(db_pool)') + 1


# ---------------------------------------------------------------- coordinator amendments (CA-B8-*)
async def test_ca_b8_1_a_yield_between_sl_attempts_stops_the_plan_before_any_cancel(g, db, venue, monkeypatch):
    """CA-B8-1 (I2, I16): the replacement SL is rejected once, the user goes in flight before the retry and is
    free again right after: the plan's CANCEL of the old SL must never run while no replacement SL rests."""
    old_sl = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.44, 'Stop Market')    # service SL at a wrong level
    tp = venue.add_order(WALLET, 'ADA', 'A', 4000, 0.55, 'Take Profit Limit')
    open_row(g, db, sl_order_id=str(old_sl), tp_order_id=str(tp))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('rejected')
                 if 'trigger' in ot and ot['trigger']['tpsl'] == 'sl' else NotImplemented, times=1)
    answers = [False, False, False, False, True]      # _pass_user x2, _invariant, _execute(PLACE_SL), SL retry

    async def busy(user_id, since=None):
        return answers.pop(0) if answers else False
    monkeypatch.setattr(g, '_busy', busy)
    await run_pass(g)
    assert answers == []
    assert not [c for c in venue.calls if c[1] == 'cancel']                    # the old SL was never cancelled
    assert old_sl in [o['oid'] for o in venue.orders(WALLET, 'ADA')]
    assert len([a for a in venue.order_calls() if 'trigger' in a[5]]) == 1    # no SL retry after the yield
    assert db.errors('GUARDIAN_YIELD_INFLIGHT') and not db.errors('GUARDIAN_SL_REPAIRED')


async def test_ca_b8_2_a_flat_user_with_only_old_closed_rows_gets_the_screen_read_only(g, db, venue):
    """CA-B8-2 (capacity): no live position, no active row and no unverified row closed within 60 min -> no book."""
    db.add_position(user_id=7, status='closed', sl_order_id='111', opened_at=g.world.t - timedelta(days=2),
                    last_fill_at=g.world.t - timedelta(days=2))
    await run_pass(g)
    assert [n for t, n, _ in venue.calls if t == 'info'] == ['meta_and_asset_ctxs', 'user_state']


async def test_ca_b8_2_a_recently_closed_row_gets_the_book_until_verified_clean(g, db, venue):
    db.add_position(user_id=7, status='closed', sl_order_id='111', opened_at=g.world.t - timedelta(hours=1),
                    last_fill_at=g.world.t - timedelta(minutes=10))
    await run_pass(g)
    assert 'frontend_open_orders' in venue.names('info')                       # closed 10 min ago, not yet verified
    venue.calls.clear()
    g.world.advance(pg.PASS_S)
    await run_pass(g)                                                          # verified clean by the first book
    assert [n for t, n, _ in venue.calls if t == 'info'] == ['meta_and_asset_ctxs', 'user_state']


async def test_ca_b8_4_unrecognised_order_reads_count_toward_guardian_blind(g, db, venue, notes):
    """CA-B8-4 (I1): a read that answers with an unrecognised shape counts toward GUARDIAN_BLIND like a raise."""
    open_row(g, db)
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('frontend_open_orders', lambda *a: {'error': 'unexpected'})   # an answer, but not a list
    for _ in range(3):
        await run_pass(g)
        g.world.advance(pg.PASS_S)
    assert mutations(venue) == []
    assert db.errors('GUARDIAN_READ_UNKNOWN') and db.errors('GUARDIAN_BLIND')
    assert any(t == 'GUARDIAN_BLIND (HL)' for t, _, _ in notes)


async def test_ca_b8_4_unrecognised_screens_count_and_a_recognised_screen_resets(g, db, venue):
    garbage = lambda *a: {'unexpected': True}                                  # noqa: E731
    venue.script('user_state', garbage, times=2)          # user 7: agent_active, flat, no rows -> screen only
    for _ in range(2):
        await run_pass(g)
        g.world.advance(pg.PASS_S)
    await run_pass(g)                                     # a recognised screen was the user's whole read: reset
    g.world.advance(pg.PASS_S)
    venue.script('user_state', garbage, times=2)
    for _ in range(2):
        await run_pass(g)
        g.world.advance(pg.PASS_S)
    assert db.errors('GUARDIAN_READ_UNKNOWN') and not db.errors('GUARDIAN_BLIND')
    venue.script('user_state', garbage, times=1)
    await run_pass(g)                                     # the third unrecognised screen in a row
    assert db.errors('GUARDIAN_BLIND')


async def test_ca_b8_6_alarm_log_lines_carry_the_error_type_once(g, caplog):
    """CA-B8-6: codes already named GUARDIAN_* are logged as-is (never GUARDIAN_GUARDIAN_*)."""
    caplog.set_level(logging.INFO, logger='POSITION_GUARDIAN')
    await g._alarm(None, '*', 'GUARDIAN_BUDGET', 'critical', done=0, users=1)
    await g._alarm(None, 'ADA', 'GUARDIAN_WOULD_PLACE_SL', 'info', code='SL_REPAIRED')
    await g._alarm(None, 'ADA', 'ROW_AMBIGUOUS', 'critical')
    assert 'GUARDIAN_GUARDIAN_' not in caplog.text
    for line in ('GUARDIAN_BUDGET sev=critical', 'GUARDIAN_WOULD_PLACE_SL sev=info', 'GUARDIAN_ROW_AMBIGUOUS sev=critical'):
        assert line in caplog.text
