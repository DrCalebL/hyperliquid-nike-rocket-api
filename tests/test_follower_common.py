"""T02 test_follower_common (REQUIRED in both follower gates).

Byte-identical in kraken-nike-rocket-api/tests and hyperliquid-nike-rocket-api/tests (builder B1). Pure: imports
bracket_guard, follower_guard and signal_guard only (no DB, no exchange, no fastapi). Covers the row policy
(select_row, rec_from_row, filter_actions), the Kraken legacy rowless predicate, guardian_mode, the in-flight tokens
(I12 + A6), FlatTracker / fills evidence (R3-M6 + A2) and signal_guard (I11).
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bracket_guard as bg  # noqa: E402
import follower_guard as fg  # noqa: E402
import signal_guard as sg  # noqa: E402

LOT, TICK = 1.0, 0.0001
T0 = datetime(2026, 9, 28, 10, 0)                     # naive UTC, as stored in open_positions.opened_at
OLD = {'id': 1, 'side': 'BUY', 'quantity': 800, 'target_sl': 0.49, 'target_tp': 0.52, 'status': 'open',
       'opened_at': T0, 'sl_order_id': 'o-sl', 'tp_order_id': 'o-tp', 'entry_order_id': 'o-e'}
NEW = {'id': 2, 'side': 'BUY', 'quantity': 800, 'target_sl': 0.46, 'target_tp': 0.49, 'status': 'open',
       'opened_at': T0 + timedelta(hours=1), 'sl_order_id': 'n-sl', 'tp_order_id': 'n-tp', 'entry_order_id': 'n-e'}


def ts(dt):
    return dt.replace(tzinfo=timezone.utc).timestamp()


def kinds(acts):
    return ['%s:%s' % (a.kind, a.code) if a.code else a.kind for a in acts]


def pos(side=1, qty=800.0, opened_ts=None):
    return bg.Pos(side, qty, 0.47, opened_ts)


def leg(oid, kind=bg.STOP_MARKET, level=0.46, side='sell', qty=800.0):
    return bg.Leg(oid, side, kind, qty, trigger=level, reduce_only=True)


# ---- select_row / row_ts ----

def test_select_row_two_active_rows_reviewer_numbers_are_ambiguous():
    assert fg.select_row([NEW, OLD], pos(), LOT) == (None, 'ROW_AMBIGUOUS')


def test_select_row_consistent_and_ambiguous_cases():
    opened = ts(NEW['opened_at'])
    assert fg.select_row([], pos(), LOT) == (None, None)
    assert fg.select_row([NEW], pos(), LOT) == (NEW, None)
    assert fg.select_row([NEW], None, LOT) == (NEW, None)
    assert fg.select_row([NEW], bg.UNKNOWN, LOT) == (NEW, None)
    assert fg.select_row([NEW], pos(side=-1), LOT) == (None, 'ROW_AMBIGUOUS')
    assert fg.select_row([NEW], pos(qty=900.0), LOT) == (None, 'ROW_AMBIGUOUS')
    assert fg.select_row([NEW], pos(qty=800.4), LOT) == (NEW, None)
    assert fg.select_row([NEW], pos(), LOT, frozenset({2})) == (None, 'ROW_AMBIGUOUS')
    assert fg.select_row([NEW], pos(opened_ts=opened + 301), LOT) == (None, 'ROW_AMBIGUOUS')
    assert fg.select_row([NEW], pos(opened_ts=opened + 300), LOT) == (NEW, None)
    assert fg.select_row([NEW], pos(qty=400.0, opened_ts=opened + 5000), LOT) == (NEW, None)   # partial reduction


def test_row_ts_naive_is_utc():
    assert fg.row_ts(T0) == ts(T0)
    assert fg.row_ts(datetime(2026, 9, 28, 6, 0, tzinfo=timezone(timedelta(hours=-4)))) == ts(T0)
    assert fg.row_ts(None) is None


# ---- rec_from_row / real_oid / service_oids ----

def test_rec_from_row_geometry_and_trust():
    rec = fg.rec_from_row(NEW, 'mark', bg.TP_TRIGGER_LIMIT)
    assert rec == bg.Rec(1, 0.46, 0.49, 'mark', bg.TP_TRIGGER_LIMIT, status='active', sl_oid='n-sl',
                         sl_oid_trusted=True, tp_oid='n-tp', created_ts=ts(NEW['opened_at']))
    short = fg.rec_from_row(dict(NEW, side='SELL', target_sl=0.49, target_tp=0.45), 'last', bg.TP_LIMIT)
    assert (short.side, short.sl, short.tp) == (-1, 0.49, 0.45)
    for bad in (dict(NEW, target_sl=0.5), dict(NEW, side='SELL'), dict(NEW, target_sl=0), dict(NEW, target_tp=None),
                dict(NEW, target_sl=float('nan')), dict(NEW, target_tp='abc'), dict(NEW, side='HOLD'),
                {k: v for k, v in NEW.items() if k != 'target_tp'}):
        assert fg.rec_from_row(bad, 'last', bg.TP_LIMIT) is None, bad


@pytest.mark.parametrize('placeholder', [None, '', 'None', 'none', 'unknown', 'UNKNOWN', 'unknown-after-exception',
                                         '  '])
def test_placeholder_oids_are_never_trusted(placeholder):
    rec = fg.rec_from_row(dict(NEW, sl_order_id=placeholder, tp_order_id=placeholder), 'last', bg.TP_LIMIT)
    assert (rec.sl_oid, rec.sl_oid_trusted, rec.tp_oid) == (None, False, None)
    assert fg.real_oid(placeholder) is None


def test_real_oid_and_service_oids():
    assert fg.real_oid(123) == '123'
    assert fg.real_oid(' abc ') == 'abc'
    assert fg.service_oids([NEW, dict(OLD, sl_order_id='unknown', tp_order_id=None)]) == {'n-sl', 'n-tp'}


# ---- filter_actions ----

def test_non_owned_cancels_are_dropped_owned_kept():
    plan = [bg.Action('CANCEL', 'SL_EXTRA_OR_INVALID', leg=leg('cust')),
            bg.Action('CANCEL', 'SL_EXTRA_OR_INVALID', leg=leg('n-sl'))]
    kept, alarms = fg.filter_actions(plan, NEW, {'n-sl'})
    assert [a.leg.oid for a in kept] == ['n-sl']
    assert alarms == [('FOREIGN_LEG_KEPT', 'info', {'oid': 'cust', 'code': 'SL_EXTRA_OR_INVALID'})]


def test_flat_open_row_gets_no_cancel_and_no_clear_needs_recovery_is_marked():
    plan = [bg.Action('CANCEL', 'ORPHAN_LEG', leg=leg('n-sl')), bg.Action('CLEAR_RECORD', 'FLAT')]
    assert fg.filter_actions(plan, NEW, {'n-sl'}) == ([], [])
    kept, _ = fg.filter_actions(plan, dict(NEW, status='needs_recovery'), {'n-sl'})
    assert kinds(kept) == ['CANCEL:ORPHAN_LEG', 'MARK_ROW_CLOSED_MANUAL:FLAT_CONFIRMED']


@pytest.mark.parametrize('bad', [bg.Action('ADOPT', 'UNTRACKED_POSITION_ADOPTED'),
                                 bg.Action('ALARM', 'RECORD_SIDE_MISMATCH', 'critical'),
                                 bg.Action('CLOSE', 'NAKED_UNKNOWN_LEVEL', 'critical')])
def test_untracked_plans_are_dropped_whole(bad):
    plan = [bad, bg.Action('PLACE_TP', 'TP_REPAIRED', level=0.49)]
    kept, alarms = fg.filter_actions(plan, NEW, {'n-sl'})
    assert kept == []
    assert alarms == [('FOLLOWER_UNTRACKED_PLAN_DROPPED', 'critical',
                       {'plan': ['%s:%s' % (bad.kind, bad.code), 'PLACE_TP:TP_REPAIRED']})]


def test_tp_through_a_crossed_level_is_never_placed():
    plan = [bg.Action('PLACE_SL', 'SL_REPAIRED', level=0.46),
            bg.Action('CANCEL', 'TP_EXTRA_OR_INVALID', leg=leg('n-tp', bg.TP_LIMIT, 0.49)),
            bg.Action('PLACE_TP', 'TP_REPAIRED', level=0.49, qty=800.0)]
    kept, alarms = fg.filter_actions(plan, NEW, {'n-tp'}, pos=pos(), ref_px=0.495)
    assert kinds(kept) == ['PLACE_SL:SL_REPAIRED']
    assert alarms == [('TP_LEVEL_CROSSED', 'critical', {'tp': 0.49, 'px': 0.495})]
    kept, alarms = fg.filter_actions(plan, NEW, {'n-tp'}, pos=pos(), ref_px=0.47)
    assert (kinds(kept), alarms) == (['PLACE_SL:SL_REPAIRED', 'CANCEL:TP_EXTRA_OR_INVALID', 'PLACE_TP:TP_REPAIRED'], [])
    short_plan = [bg.Action('PLACE_TP', 'TP_REPAIRED', level=0.45)]
    assert fg.filter_actions(short_plan, NEW, set(), pos=pos(side=-1), ref_px=0.449)[0] == []
    kept, _ = fg.filter_actions(short_plan, NEW, set(), pos=pos(side=-1), ref_px=0.46)
    assert kinds(kept) == ['PLACE_TP:TP_REPAIRED']


def test_followers_replace_service_legs_and_keep_customer_legs():
    assert fg.DECIDE_LEVEL_POLICY == bg.LEVEL_REPLACE == 'replace'
    rec = fg.rec_from_row(NEW, 'last', bg.TP_LIMIT)
    legs = [leg('cust', level=0.44), leg('n-tp', bg.TP_LIMIT, 0.49)]
    acts = bg.decide(pos(), rec, legs, {'last': 0.47}, TICK, LOT, 0.0, level_policy=fg.DECIDE_LEVEL_POLICY)
    kept, alarms = fg.filter_actions(acts, NEW, fg.service_oids([NEW]), pos=pos(), ref_px=0.47)
    assert kinds(kept) == ['PLACE_SL:SL_REPAIRED']
    assert alarms == [('FOREIGN_LEG_KEPT', 'info', {'oid': 'cust', 'code': 'SL_EXTRA_OR_INVALID'})]
    moved = [leg('n-sl', level=0.455), leg('n-tp', bg.TP_LIMIT, 0.49)]
    acts = bg.decide(pos(), rec, moved, {'last': 0.47}, TICK, LOT, 0.0, level_policy=fg.DECIDE_LEVEL_POLICY)
    kept, _ = fg.filter_actions(acts, NEW, fg.service_oids([NEW]), pos=pos(), ref_px=0.47)
    assert kinds(kept) == ['PLACE_SL:SL_REPAIRED', 'CANCEL:SL_EXTRA_OR_INVALID']


# ---- legacy_rowless_should_close: today's inline expression, copied verbatim ----

def todays_inline_should_close(symbol_orders, db_position):
    has_tp = any(
        o.get('type') in ('limit',) and o.get('reduceOnly', False)
        for o in symbol_orders
    )
    has_sl = any(
        o.get('type') in ('stop', 'stop_market', 'stopMarket') and o.get('reduceOnly', False)
        for o in symbol_orders
    )
    if has_tp or has_sl or (db_position and db_position > 0):
        return False
    return True


ORDERS = {'none': [], 'limit_ro': [{'type': 'limit', 'reduceOnly': True}],
          'limit_non_ro': [{'type': 'limit', 'reduceOnly': False}], 'stop_ro': [{'type': 'stop', 'reduceOnly': True}],
          'stop_limit_ro': [{'type': 'stop', 'reduceOnly': True, 'price': 0.46}],
          'take_profit_ro': [{'type': 'take_profit', 'reduceOnly': True}],
          'stop_market_ro': [{'type': 'stop_market', 'reduceOnly': True}]}


@pytest.mark.parametrize('tracked', [0, 1])
@pytest.mark.parametrize('name', sorted(ORDERS))
def test_legacy_rowless_predicate_equals_todays_expression(name, tracked):
    assert fg.legacy_rowless_should_close(ORDERS[name], tracked) == todays_inline_should_close(ORDERS[name], tracked)


def test_legacy_take_profit_only_untracked_closes_as_today():
    assert fg.legacy_rowless_should_close(ORDERS['take_profit_ro'], 0) is True
    assert fg.legacy_rowless_should_close(ORDERS['take_profit_ro'], 1) is False


# ---- guardian_mode ----

def test_guardian_mode(monkeypatch):
    kraken, hl = ('legacy', 'shadow', 'enforce'), ('off', 'shadow', 'enforce')
    monkeypatch.delenv('GUARDIAN_MODE', raising=False)
    assert (fg.guardian_mode(kraken), fg.guardian_mode(hl)) == ('shadow', 'shadow')
    for value, k, h in ((' Enforce ', 'enforce', 'enforce'), ('off', 'shadow', 'off'), ('legacy', 'legacy', 'shadow'),
                        ('bogus', 'shadow', 'shadow')):
        monkeypatch.setenv('GUARDIAN_MODE', value)
        assert (fg.guardian_mode(kraken), fg.guardian_mode(hl)) == (k, h), value


# ---- in-flight tokens (I12 + A6) ----

class FakeMonotonic:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def clock(monkeypatch):
    c = FakeMonotonic()
    monkeypatch.setattr(fg, '_clock', c)
    fg.drain_expired()
    return c


def test_inflight_defaults():
    assert fg.INFLIGHT_MAX_AGE_S == 600.0


def test_two_marks_one_clear_still_in_flight(clock):
    a, b = fg.mark(42), fg.mark(42)
    assert a != b
    fg.clear(42, a)
    assert fg.is_inflight(42)
    fg.clear(42, b)
    assert not fg.is_inflight(42)
    fg.clear(42, b)                                   # idempotent
    assert not fg.is_inflight(43)


def test_token_outside_a_task_expires_at_600s_silently(clock):
    tok = fg.mark(51)
    clock.t += 599
    assert fg.is_inflight(51)
    clock.t += 1
    assert not fg.is_inflight(51)
    assert fg.drain_expired() == []
    fg.clear(51, tok)


def test_token_of_a_finished_task_is_dropped(clock):
    async def execute_trade_without_clear():
        fg.mark(52)

    asyncio.run(execute_trade_without_clear())
    assert not fg.is_inflight(52)
    assert fg.drain_expired() == []


def test_live_task_stays_in_flight_past_180s_and_alarms_when_it_ages_out(clock, caplog):
    seen = {}

    async def main():
        release = asyncio.Event()

        async def execute_trade():
            tok = fg.mark(53)
            await release.wait()
            fg.clear(53, tok)

        task = asyncio.create_task(execute_trade())
        await asyncio.sleep(0)
        clock.t += 300
        seen['at_300s'] = fg.is_inflight(53)
        clock.t += 301
        with caplog.at_level(logging.CRITICAL, logger='follower_guard'):
            seen['at_601s'] = fg.is_inflight(53)
        seen['expired'] = fg.drain_expired()
        release.set()
        await task

    asyncio.run(main())
    assert seen['at_300s'] is True
    assert seen['at_601s'] is False
    assert [(e['user_id'], e['age_s']) for e in seen['expired']] == [(53, 601.0)]
    assert 'INFLIGHT_TOKEN_EXPIRED user_id=53' in caplog.text
    assert fg.drain_expired() == []


# ---- FlatTracker and fills evidence (R3-M6 + A2) ----

def test_false_flat_with_the_same_fingerprint_clears_and_decide_repairs_a_removed_sl():
    ft, t0, fp = fg.FlatTracker(), ts(NEW['opened_at']) + 3600, ('long', 1790590000000, 0.47)
    ft.note_live(2, fp)
    ft.note_confirmed_flat(2, t0)
    assert ft.note_confirmed_flat(2, t0 + 240) == 240
    assert ft.pending_check(2)
    ft.note_live(2, ('other',))                       # ignored while a flat is pending
    assert ft.fp[2] == fp
    ft.resolve(2, new_position=False, fingerprint=fp)
    assert not ft.pending_check(2) and 2 not in ft.evidence
    row, code = fg.select_row([NEW], pos(), LOT, ft.evidence)
    assert (row, code) == (NEW, None)
    acts = bg.decide(pos(), fg.rec_from_row(NEW, 'last', bg.TP_LIMIT), [leg('n-tp', bg.TP_LIMIT, 0.49)],
                     {'last': 0.47}, TICK, LOT, t0 + 500, level_policy=fg.DECIDE_LEVEL_POLICY)
    assert kinds(acts) == ['PLACE_SL:SL_REPAIRED']


def test_manual_reopen_after_the_flat_is_evidence_and_the_row_becomes_ambiguous():
    ft, t0 = fg.FlatTracker(), ts(NEW['opened_at']) + 3600
    ft.note_confirmed_flat(2, t0)
    fills = [{'side': 'buy', 'ts_ms': int((t0 + 30) * 1000), 'opening': True, 'order_id': 'manual-1'}]
    assert fg.fills_show_new_entry(fills, 1, ft.since_ms(NEW), NEW['entry_order_id'])
    ft.resolve(2, new_position=True)
    assert not ft.pending_check(2)
    assert fg.select_row([NEW], pos(), LOT, ft.evidence) == (None, 'ROW_AMBIGUOUS')


def test_fills_that_are_not_new_entry_evidence():
    since = int(ts(T0) * 1000)
    assert not fg.fills_show_new_entry([{'side': 'sell', 'ts_ms': since + 1, 'opening': None}], 1, since)
    assert not fg.fills_show_new_entry([{'side': 'buy', 'ts_ms': since - 1, 'opening': True}], 1, since)
    assert not fg.fills_show_new_entry([{'side': 'buy', 'ts_ms': since + 1, 'opening': False}], 1, since)
    assert not fg.fills_show_new_entry([{'side': 'buy', 'ts_ms': None, 'opening': True}], 1, since)
    assert not fg.fills_show_new_entry([{'side': 'buy', 'ts_ms': since + 1, 'opening': True, 'order_id': 'e-9'}],
                                       1, since, entry_order_id='e-9')
    assert fg.fills_show_new_entry([{'side': 'buy', 'ts_ms': since + 1, 'opening': True, 'order_id': 'e-9'}],
                                   1, since, entry_order_id='unknown')
    assert fg.fills_show_new_entry([{'side': 'sell', 'ts_ms': since, 'opening': None}], -1, since)
    assert not fg.fills_show_new_entry(None, 1, since)


def test_since_ms_is_clamped_to_the_rows_opened_at():
    ft = fg.FlatTracker()
    opened = ts(NEW['opened_at'])
    ft.note_confirmed_flat(2, opened + 20)
    assert ft.since_ms(NEW) == int(opened * 1000)                       # flat - 60 s would reach before the row
    ft.note_confirmed_flat(1, ts(OLD['opened_at']) + 3600)
    assert ft.since_ms(OLD) == int((ts(OLD['opened_at']) + 3600 - 60) * 1000)
    ft.note_confirmed_flat(9, 5000.0)
    assert ft.since_ms({'id': 9, 'opened_at': None}) == 4_940_000
    with pytest.raises(TypeError):
        ft.since_ms(2)


def test_entry_unconfirmed_row_own_entry_fill_inside_the_pad_keeps_the_row_consistent():
    """A2: a needs_recovery row written ~8 s after its IOC; two confirmed flats; the position then appears and the
    only entry-side fills are the row's own (before opened_at, and a late report carrying its order id)."""
    written = T0 + timedelta(hours=2)
    row = dict(NEW, id=77, status='needs_recovery', opened_at=written, entry_order_id='e-77', sl_order_id=None,
               tp_order_id=None)
    t = ts(written)
    ft = fg.FlatTracker()
    ft.note_confirmed_flat(77, t + 20)
    ft.note_confirmed_flat(77, t + 50)
    fills = [{'side': 'buy', 'ts_ms': int((t - 8) * 1000), 'opening': True, 'order_id': 'e-77'},
             {'side': 'buy', 'ts_ms': int((t + 2) * 1000), 'opening': True, 'order_id': 'e-77'}]
    new = fg.fills_show_new_entry(fills, fg.row_side(row), ft.since_ms(row), row['entry_order_id'])
    assert new is False
    ft.resolve(77, new_position=new)
    live = pos(opened_ts=None)
    assert fg.select_row([row], live, LOT, ft.evidence) == (row, None)
    acts = bg.decide(live, fg.rec_from_row(row, 'mark', bg.TP_TRIGGER_LIMIT), [], {'mark': 0.47}, TICK, LOT, t + 60,
                     level_policy=fg.DECIDE_LEVEL_POLICY)
    assert kinds(acts) == ['PLACE_SL:SL_REPAIRED', 'PLACE_TP:TP_REPAIRED']


def test_gc_drops_inactive_rows():
    ft = fg.FlatTracker()
    for rid in (1, 2, 3):
        ft.note_live(rid, (rid,))
        ft.note_confirmed_flat(rid, 100.0)
        ft.resolve(rid, new_position=True)
    ft.gc([2])
    assert (set(ft.flat), set(ft.fp), ft.evidence) == ({2}, {2}, {2})


# ---- signal_guard (I11: 422 before any DB write) ----

@pytest.mark.parametrize('risk', [0.01, 0.0125, 0.02, 0.025, 0.03])
def test_signal_guard_accepts_the_configured_fractions(risk, monkeypatch):
    monkeypatch.delenv('MAX_BROADCAST_RISK_PCT', raising=False)
    assert sg.validate_signal_payload('BUY', 0.47, 0.46, 0.49, 5, risk) is None
    assert sg.validate_signal_payload('SELL', 0.47, 0.48, 0.45, 5, risk) is None
    assert sg.validate_signal_payload('buy', '0.47', '0.46', '0.49', '1', str(risk)) is None


@pytest.mark.parametrize('kwargs, reason', [
    (dict(risk_pct=0), 'risk_pct must be in (0, 0.03]'),
    (dict(risk_pct=0.05), 'risk_pct must be in (0, 0.03]'),
    (dict(risk_pct=-0.01), 'risk_pct must be in (0, 0.03]'),
    (dict(risk_pct=float('nan')), 'must be finite numbers'),
    (dict(risk_pct=None), 'must be finite numbers'),
    (dict(risk_pct='abc'), 'must be finite numbers'),
    (dict(stop_loss=0.48), 'BUY requires 0 < stop_loss < entry_price < take_profit'),
    (dict(take_profit=0.465), 'BUY requires 0 < stop_loss < entry_price < take_profit'),
    (dict(stop_loss=0), 'BUY requires 0 < stop_loss < entry_price < take_profit'),
    (dict(action='SELL'), 'SELL requires 0 < take_profit < entry_price < stop_loss'),
    (dict(leverage=0), 'leverage must be in [1, 50]'),
    (dict(leverage=100), 'leverage must be in [1, 50]'),
    (dict(leverage=0.5), 'leverage must be in [1, 50]'),
    (dict(leverage=51), 'leverage must be in [1, 50]'),
    (dict(action='HOLD'), 'action must be BUY or SELL'),
    (dict(action=None), 'action must be BUY or SELL'),
])
def test_signal_guard_rejects(kwargs, reason, monkeypatch):
    monkeypatch.delenv('MAX_BROADCAST_RISK_PCT', raising=False)
    payload = dict(action='BUY', entry_price=0.47, stop_loss=0.46, take_profit=0.49, leverage=5, risk_pct=0.02)
    payload.update(kwargs)
    got = sg.validate_signal_payload(**payload)
    assert got is not None and reason in got


def test_signal_guard_max_risk_from_env_or_argument(monkeypatch):
    monkeypatch.setenv('MAX_BROADCAST_RISK_PCT', '0.02')
    assert sg.validate_signal_payload('BUY', 0.47, 0.46, 0.49, 5, 0.025) == 'risk_pct must be in (0, 0.02]'
    assert sg.validate_signal_payload('BUY', 0.47, 0.46, 0.49, 5, 0.025, max_risk=0.03) is None
    monkeypatch.setenv('MAX_BROADCAST_RISK_PCT', 'junk')
    assert sg.validate_signal_payload('BUY', 0.47, 0.46, 0.49, 5, 0.03) is None
