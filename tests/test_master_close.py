"""
T07 (HL follower CLOSE, spec massive-rocket-algos docs/superpowers/specs/2026-10-06-t1-live-design.md 19.3-19.9, 21):
POST /api/broadcast-close (auth, 422 before any write, idempotent receipt, master_trade_id-only matching), the v2 entry
fields on /api/broadcast-signal, execute_close's money-safety order (TP cancel -> exit_hint -> reduce-only close ->
verify flat -> SL cancel), process_close_jobs (kill switch, inactive-user policy, in-flight skip, 24 h expiry, runs
before the signal poll), the position_monitor TIME label, the guardian busy rule, the section-21 close events and the
unchanged shared files / dashboard f-string / additive migration.
"""
import ast
import hashlib
import json
import os
from datetime import datetime, timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from tests.fakes import fake_hl
from tests.fakes.fake_hl import creds, fast_sleep, notes, venue  # noqa: F401  (pytest fixtures)
from tests.fakes.fake_close import CloseDB
from tests.fakes.fake_pool import FakePool

import bracket_guard as bg
import close_guard as cg
import follower_endpoints as fe
import follower_guard
import follower_models as fm
import hosted_trading_loop as htl
import position_guardian as pg
import position_monitor as pm

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MASTER_KEY = 'test-master-key-7f3a9c'
FIXTURE = os.path.join(ROOT, 'tests', 'fixtures', 'close_v2.json')


def wallet(uid):
    return '0x' + '%040x' % uid


def private_key(uid):
    return '0x%064x' % uid


# ================================================================ shared fixtures
@pytest.fixture
def clog(tmp_path, monkeypatch):
    """The process close-event log, redirected to a temp file; .events() parses it."""
    log = cg.CloseEventLog(str(tmp_path / 'close_events.jsonl'), venue='hyperliquid',
                           service='hyperliquid-nike-rocket-api', config=htl.close_flags(), host='test-host',
                           code_sha='testsha', free_bytes_fn=lambda d: 10 ** 12)
    monkeypatch.setattr(htl, '_CLOSE_LOG', log)

    def events(name=None):
        if not os.path.exists(log.path):
            return []
        with open(log.path, encoding='utf-8') as f:
            recs = [json.loads(line) for line in f if line.strip()]
        return [r for r in recs if name is None or r['event'] == name]
    log.events = events
    return log


@pytest.fixture
def db():
    return CloseDB()


@pytest.fixture
def ro_guard(venue):
    """I1: records every order the close path sends that is not reduce-only, or that is a trigger order."""
    bad = []

    def check(a, c, b, sz, px, ot, ro):
        if not ro or 'trigger' in ot:
            bad.append((c, b, sz, ot, ro))
        return NotImplemented
    venue.script('order', check)
    return bad


@pytest.fixture
def loop(venue, db, creds, notes, fast_sleep, clog, ro_guard):
    return htl.HostedTradingLoop(FakePool(db))


def arm(db, venue, encrypt, uid=7, side='BUY', qty=100.0, venue_qty=None, status='open', job_status='pending',
        payload_side=None, job_age_s=0, **user_kw):
    """One follower row opened from a v2 entry, its live bracket on the venue, the master's CLOSE and its job."""
    if uid not in db.users:
        db.add_user(uid, encrypt=encrypt, **user_kw)
    w = wallet(uid)
    sgn = 1 if side == 'BUY' else -1
    vq = qty if venue_qty is None else venue_qty
    if vq:
        venue.set_position(w, 'ADA', sgn * vq, 0.5)
    exit_side = 'A' if sgn > 0 else 'B'
    sl_px, tp_px = (0.45, 0.55) if sgn > 0 else (0.55, 0.45)
    sl = venue.add_order(w, 'ADA', exit_side, qty, sl_px, 'Stop Market')
    tp = venue.add_order(w, 'ADA', exit_side, qty, tp_px, 'Take Profit Limit')
    row = db.add_position(user_id=uid, signal_id=1, side=side, quantity=qty, sl_order_id=str(sl),
                          tp_order_id=str(tp), target_sl=sl_px, target_tp=tp_px, status=status, exit_hint=None,
                          close_fill_qty=None, entry_order_id='E%d' % uid)
    mid = '%032x' % (uid * 1000 + row['id'])
    db.add_master_close(mid, side=payload_side or side)
    db.add_job(row['id'], uid, mid, status=job_status,
               created_at=db.now() - timedelta(seconds=job_age_s) if job_age_s else None)
    return row, sl, tp, mid


def mutations(venue):
    return [(n, a) for t, n, a in venue.calls if t == 'exchange' and n in ('order', 'cancel')]


def close_orders(venue):
    return [a for t, n, a in venue.calls if t == 'exchange' and n == 'order' and 'limit' in a[5]]


def steps(clog):
    return [e['step'] for e in clog.events(cg.EV_JOB_STEP)]


# ================================================================ execute_close: the money-safety order (19.7, I3)
async def test_long_time_exit_cancels_tp_then_closes_then_verifies_flat_then_cancels_sl(loop, db, venue, creds,
                                                                                       clog, ro_guard):
    row, sl, tp, mid = arm(db, venue, creds)
    assert await loop.process_close_jobs() == 1
    muts = mutations(venue)
    assert [n for n, _ in muts] == ['cancel', 'order', 'cancel']               # I3: TP -> close -> SL
    assert muts[0][1][2] == tp and muts[2][1][2] == sl                         # by recorded id only
    _, coin, is_buy, sz, px, ot, ro = muts[1][1]
    assert (coin, is_buy, sz, ro, ot) == ('ADA', False, 100.0, True, {'limit': {'tif': 'Ioc'}})
    assert venue.szi(wallet(7), 'ADA') == 0.0 and venue.orders(wallet(7)) == []
    r = db.positions[row['id']]
    assert (r['exit_hint'], r['close_fill_qty'], r['status']) == ('TIME', 100.0, 'open')   # I5: monitor closes
    assert db.job(row['id'])['status'] == 'done'
    assert ro_guard == []
    assert steps(clog) == ['read', 'cancel_tp', 'reread', 'write_hint', 'close', 'record_fill', 'verify_flat',
                           'cancel_sl']
    [out] = clog.events(cg.EV_JOB_OUTCOME)
    assert out['status'] == 'done' and out['master_trade_id'] == mid and out['position_id'] == row['id']


async def test_exit_hint_is_written_before_the_close_order(loop, db, venue, creds):
    row, _, _, _ = arm(db, venue, creds)
    hint_at = []
    venue.script('order', lambda a, c, b, sz, px, ot, ro: hint_at.append(db.positions[row['id']].get('exit_hint'))
                 or NotImplemented)
    await loop.process_close_jobs()
    assert hint_at == ['TIME']


async def test_short_time_exit_buys_back_reduce_only(loop, db, venue, creds, ro_guard):
    row, sl, tp, _ = arm(db, venue, creds, side='SELL', qty=80.0)
    await loop.process_close_jobs()
    [close] = close_orders(venue)
    assert close[2] is True and close[3] == 80.0 and close[6] is True
    assert venue.szi(wallet(7), 'ADA') == 0.0 and db.job(row['id'])['status'] == 'done'
    assert ro_guard == []


async def test_a_read_error_never_acts(loop, db, venue, creds, clog):
    row, _, _, _ = arm(db, venue, creds)
    venue.script('user_state', lambda *a: TimeoutError('HL read timeout'))
    await loop.process_close_jobs()
    assert mutations(venue) == []                                              # I8
    job = db.job(row['id'])
    assert (job['status'], job['attempts']) == ('pending', 1) and 'read' in job['last_error']
    assert db.positions[row['id']]['exit_hint'] is None


async def test_an_unrecognised_user_state_shape_is_a_read_error(loop, db, venue, creds):
    row, _, _, _ = arm(db, venue, creds)
    venue.script('user_state', lambda *a: {'error': 'Could not parse JSON'})
    await loop.process_close_jobs()
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'pending'


async def test_already_flat_is_done_without_any_order(loop, db, venue, creds):
    row, _, _, _ = arm(db, venue, creds, venue_qty=0)
    await loop.process_close_jobs()
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'done'


async def test_venue_side_mismatch_fails_the_job_with_no_order(loop, db, venue, creds, notes, clog):
    row, _, _, _ = arm(db, venue, creds)
    venue.set_position(wallet(7), 'ADA', -100.0, 0.5)                          # venue short, row long
    await loop.process_close_jobs()
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'failed'
    assert clog.events(cg.EV_SIDE_MISMATCH)[0]['severity'] == 'CRITICAL'
    assert any('CLOSE_SIDE_MISMATCH' in t for t, _, _ in notes)               # admin e-mail
    assert db.errors('CLOSE_SIDE_MISMATCH')


async def test_payload_side_mismatch_fails_the_job_with_no_order(loop, db, venue, creds, clog):
    row, _, _, _ = arm(db, venue, creds, payload_side='SELL')
    await loop.process_close_jobs()
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'failed'
    assert clog.events(cg.EV_SIDE_MISMATCH)


async def test_needs_recovery_row_is_deferred_then_runs_when_the_lane_reopens_it(loop, db, venue, creds, clog):
    row, _, _, _ = arm(db, venue, creds, status='needs_recovery')
    await loop.process_close_jobs()
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'deferred'
    await loop.process_close_jobs()                                            # still deferred: no churn
    assert db.job(row['id'])['status'] == 'deferred'
    db.positions[row['id']]['status'] = 'open'                                 # the recovery lane restored it
    await loop.process_close_jobs()
    assert db.job(row['id'])['status'] == 'done' and venue.szi(wallet(7), 'ADA') == 0.0


async def test_needs_recovery_row_closed_by_the_lane_ends_the_job_done(loop, db, venue, creds):
    row, _, _, _ = arm(db, venue, creds, status='needs_recovery')
    await loop.process_close_jobs()
    db.positions[row['id']]['status'] = 'closed_manual'
    await loop.process_close_jobs()
    assert db.job(row['id'])['status'] == 'done' and mutations(venue) == []


async def test_an_inactive_row_is_done_without_a_venue_call(loop, db, venue, creds):
    row, _, _, _ = arm(db, venue, creds, status='closed')
    before = len(venue.calls)
    await loop.process_close_jobs()
    assert db.job(row['id'])['status'] == 'done'
    assert [n for _, n, _ in venue.calls[before:]] == []


async def test_a_failed_tp_cancel_stops_the_attempt_before_any_order(loop, db, venue, creds):
    row, sl, tp, _ = arm(db, venue, creds)
    venue.script('cancel', lambda *a: {'status': 'ok', 'response': {'data': {'statuses': [{'error': 'Rate limited'}]}}})
    await loop.process_close_jobs()
    assert close_orders(venue) == []
    job = db.job(row['id'])
    assert (job['status'], job['attempts']) == ('pending', 1)
    assert db.positions[row['id']]['exit_hint'] is None
    assert {o['oid'] for o in venue.orders(wallet(7))} == {sl, tp}             # the SL is untouched


async def test_a_tp_partial_fill_before_the_cancel_closes_only_what_is_left(loop, db, venue, creds):
    row, _, _, _ = arm(db, venue, creds, venue_qty=60.0)                       # 40 of 100 already filled by the TP
    await loop.process_close_jobs()
    [close] = close_orders(venue)
    assert close[3] == 60.0
    assert db.positions[row['id']]['close_fill_qty'] == 60.0 and db.job(row['id'])['status'] == 'done'


async def test_tp_fills_between_the_read_and_the_cancel_rereads_the_size(loop, db, venue, creds):
    row, _, tp, _ = arm(db, venue, creds)

    def tp_fills_now(a, c, oid):
        if oid == tp:
            venue.set_position(wallet(7), 'ADA', 30.0, 0.5)                     # 70 filled at the TP
        return NotImplemented
    venue.script('cancel', tp_fills_now, times=1)
    await loop.process_close_jobs()
    [close] = close_orders(venue)
    assert close[3] == 30.0 and db.job(row['id'])['status'] == 'done'


async def test_tp_fills_the_whole_position_before_the_cancel(loop, db, venue, creds):
    row, sl, tp, _ = arm(db, venue, creds)

    def tp_fills_all(a, c, oid):
        if oid == tp:
            venue.set_position(wallet(7), 'ADA', 0, 0.5)
        return NotImplemented
    venue.script('cancel', tp_fills_all, times=1)
    await loop.process_close_jobs()
    assert close_orders(venue) == [] and db.job(row['id'])['status'] == 'done'
    assert sl not in {o['oid'] for o in venue.orders(wallet(7))}               # 19.7 step 5 -> 8: SL cancelled
    assert db.positions[row['id']]['exit_hint'] is None                        # the TP label stays


async def test_venue_bigger_than_the_row_closes_only_the_row_share_and_keeps_the_sl(loop, db, venue, creds, clog,
                                                                                   ro_guard):
    row, sl, _, _ = arm(db, venue, creds, venue_qty=150.0)                     # the user added 50 by hand
    await loop.process_close_jobs()
    assert sum(a[3] for a in close_orders(venue)) == 100.0                     # I1: never more than the row
    assert venue.szi(wallet(7), 'ADA') == 50.0
    assert sl in {o['oid'] for o in venue.orders(wallet(7))}                   # SL kept while not flat
    assert db.positions[row['id']]['close_fill_qty'] == 100.0
    assert db.job(row['id'])['status'] == 'pending'
    assert clog.events(cg.EV_RESIDUAL)[0]['severity'] == 'CRITICAL'
    await loop.process_close_jobs()                                            # next cycle: the share is closed
    assert sum(a[3] for a in close_orders(venue)) == 100.0                     # no second close order
    assert db.job(row['id'])['status'] == 'done'
    assert sl not in {o['oid'] for o in venue.orders(wallet(7))}
    assert venue.szi(wallet(7), 'ADA') == 50.0 and clog.events(cg.EV_RESIDUAL_NOT_OURS)
    assert ro_guard == []


async def test_residual_ladder_never_exceeds_the_row_share_on_partial_fills(loop, db, venue, creds):
    row, _, _, _ = arm(db, venue, creds, venue_qty=150.0)
    partial = []

    def half(a, c, b, sz, px, ot, ro):
        if 'limit' in ot and ro and not partial:
            partial.append(sz)
            venue.set_position(a, c, 150.0 - 40.0, 0.5)
            return fake_hl.ok_status({'filled': {'totalSz': '40.0', 'avgPx': str(px), 'oid': 4242}})
        return NotImplemented
    venue.script('order', half)
    await loop.process_close_jobs()
    sizes = [a[3] for a in close_orders(venue)]
    assert sizes[0] == 100.0 and sum(sizes[1:]) == 60.0                        # 40 + 60 = the row's 100
    assert venue.szi(wallet(7), 'ADA') == 50.0 and db.positions[row['id']]['close_fill_qty'] == 100.0


async def test_sl_fills_between_the_read_and_the_close_records_no_time_fill(loop, db, venue, creds):
    row, sl, _, _ = arm(db, venue, creds)

    def sl_fires(a, c, b, sz, px, ot, ro):
        if 'limit' in ot and ro and venue.szi(a, c) != 0:
            venue.set_position(a, c, 0, 0.5)
            venue.external_cancel(a, sl)
            venue.add_fill(a, c, 'A', 100.0, 0.45, 'Close Long', venue.now_ms, sl, 100.0)
        return NotImplemented
    venue.script('order', sl_fires)
    await loop.process_close_jobs()
    r = db.positions[row['id']]
    assert not r['close_fill_qty'] and db.job(row['id'])['status'] == 'done'


async def test_a_failed_close_keeps_the_sl_and_retries_then_fails_after_max_attempts(loop, db, venue, creds, notes,
                                                                                    clog):
    row, sl, _, _ = arm(db, venue, creds)
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('could not immediately match')
                 if 'limit' in ot else NotImplemented)
    for _ in range(cg.CLOSE_JOB_MAX_ATTEMPTS):
        await loop.process_close_jobs()
    assert sl in {o['oid'] for o in venue.orders(wallet(7))}
    job = db.job(row['id'])
    assert (job['status'], job['attempts']) == ('failed', cg.CLOSE_JOB_MAX_ATTEMPTS)
    assert clog.events(cg.EV_RESIDUAL) and clog.events(cg.EV_JOB_FAILED)
    assert any('CLOSE_JOB_FAILED' in t for t, _, _ in notes)


async def test_restart_mid_job_sends_exactly_one_filled_close(venue, db, creds, notes, fast_sleep, clog, ro_guard):
    row, sl, _, _ = arm(db, venue, creds)
    venue.script('order', lambda a, c, b, sz, px, ot, ro: fake_hl.err_status('could not immediately match')
                 if 'limit' in ot else NotImplemented, times=3)
    await htl.HostedTradingLoop(FakePool(db)).process_close_jobs()             # TP cancelled, hint written, no fill
    assert db.job(row['id'])['status'] == 'pending' and db.positions[row['id']]['exit_hint'] == 'TIME'
    restarted = htl.HostedTradingLoop(FakePool(db))                            # a new process
    await restarted.process_close_jobs()
    await restarted.process_close_jobs()                                       # job done: not selected again
    filled = [a for a in close_orders(venue)][3:]
    assert len(filled) == 1 and filled[0][3] == 100.0
    assert db.job(row['id'])['status'] == 'done' and db.positions[row['id']]['close_fill_qty'] == 100.0


async def test_a_sl_cancel_failure_after_flat_is_a_leg_left_warning_and_the_job_is_done(loop, db, venue, creds,
                                                                                       clog):
    row, sl, tp, _ = arm(db, venue, creds)
    venue.script('cancel', lambda a, c, oid: {'status': 'ok', 'response': {'data': {'statuses': [
        {'error': 'Rate limited'}]}}} if oid == sl else NotImplemented)
    await loop.process_close_jobs()
    assert db.job(row['id'])['status'] == 'done' and venue.szi(wallet(7), 'ADA') == 0.0
    assert clog.events(cg.EV_LEG_LEFT)[0]['severity'] == 'WARN'


async def test_undecryptable_credentials_fail_the_job_without_a_venue_call(loop, db, venue, creds, clog, notes):
    row, _, _, _ = arm(db, venue, creds)
    db.users[7]['hl_private_key_encrypted'] = 'not-a-fernet-token'
    await loop.process_close_jobs()
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'failed'
    assert clog.events(cg.EV_JOB_FAILED)


# ================================================================ process_close_jobs policy
async def test_kill_switch_records_nothing_executed_and_then_runs_the_backlog(loop, db, venue, creds, monkeypatch):
    row, _, _, _ = arm(db, venue, creds)
    monkeypatch.setenv('CLOSE_JOBS_ENABLED', '0')
    assert await loop.process_close_jobs() == 0
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'pending'
    monkeypatch.setenv('CLOSE_JOBS_ENABLED', '1')
    assert await loop.process_close_jobs() == 1
    assert db.job(row['id'])['status'] == 'done'


@pytest.mark.parametrize('user_kw', [{'agent_active': False}, {'access_granted': False},
                                     {'pending_invoice_id': 'inv_1',
                                      'invoice_due_date': datetime(2020, 1, 1)}])
async def test_inactive_users_are_closed_by_default(loop, db, venue, creds, user_kw):
    row, _, _, _ = arm(db, venue, creds, **user_kw)
    await loop.process_close_jobs()
    assert db.job(row['id'])['status'] == 'done' and venue.szi(wallet(7), 'ADA') == 0.0


async def test_skip_inactive_users_flag_skips_with_an_alarm(loop, db, venue, creds, monkeypatch, clog):
    monkeypatch.setenv('CLOSE_SKIP_INACTIVE_USERS', '1')
    row, _, _, _ = arm(db, venue, creds, agent_active=False)
    active, _, _, _ = arm(db, venue, creds, uid=8)
    await loop.process_close_jobs()
    assert db.job(row['id'])['status'] == 'skipped' and venue.szi(wallet(7), 'ADA') == 100.0
    assert [m for m in mutations(venue) if m[1][0] == wallet(7)] == []
    assert db.job(active['id'])['status'] == 'done'
    [skip] = [e for e in clog.events(cg.EV_JOB_SKIPPED) if e['position_id'] == row['id']]
    assert skip['reason'] == 'inactive_user' and skip['severity'] in ('WARN', 'CRITICAL')


async def test_an_in_flight_user_is_skipped_and_retried_next_cycle(loop, db, venue, creds, clog):
    row, _, _, _ = arm(db, venue, creds)
    tok = follower_guard.mark(7)
    try:
        await loop.process_close_jobs()
    finally:
        follower_guard.clear(7, tok)
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'pending'
    assert clog.events(cg.EV_JOB_SKIPPED)[0]['reason'] == 'user_in_flight'
    await loop.process_close_jobs()
    assert db.job(row['id'])['status'] == 'done'


async def test_the_close_runs_under_the_users_in_flight_token(loop, db, venue, creds):
    arm(db, venue, creds)
    seen = []
    venue.script('order', lambda *a: seen.append(follower_guard.is_inflight(7)) or NotImplemented)
    await loop.process_close_jobs()
    assert seen and all(seen) and not follower_guard.is_inflight(7)


async def test_a_job_past_24h_expires_with_an_alarm_and_no_order(loop, db, venue, creds, notes, clog):
    row, sl, tp, _ = arm(db, venue, creds, job_age_s=cg.CLOSE_JOB_MAX_AGE_S + 60)
    await loop.process_close_jobs()
    assert mutations(venue) == [] and db.job(row['id'])['status'] == 'expired'
    assert {o['oid'] for o in venue.orders(wallet(7))} == {sl, tp}             # keeps its SL/TP
    assert clog.events(cg.EV_JOB_EXPIRED)[0]['severity'] == 'CRITICAL'
    assert any('CLOSE_JOB_EXPIRED' in t for t, _, _ in notes)


async def test_two_users_on_one_master_close_are_each_closed_once(loop, db, venue, creds):
    a, _, _, mid = arm(db, venue, creds, uid=7)
    b = db.add_position(user_id=8, signal_id=1, side='BUY', quantity=50.0, status='open')
    db.add_user(8, encrypt=creds)
    venue.set_position(wallet(8), 'ADA', 50.0, 0.5)
    db.add_job(b['id'], 8, mid)
    await loop.process_close_jobs()
    assert venue.szi(wallet(7), 'ADA') == 0.0 and venue.szi(wallet(8), 'ADA') == 0.0
    assert sorted(a[3] for a in close_orders(venue)) == [50.0, 100.0]
    assert db.job(a['id'])['status'] == db.job(b['id'])['status'] == 'done'


async def test_close_jobs_run_before_the_signal_poll_and_their_errors_never_block_it(loop, monkeypatch):
    order = []

    async def jobs():
        order.append('close')
        raise RuntimeError('close_jobs table missing')

    async def pending():
        order.append('signals')
        return []
    monkeypatch.setattr(loop, 'process_close_jobs', jobs)
    monkeypatch.setattr(loop, 'get_pending_signals_batched', pending)
    await loop.poll_and_execute()
    assert order == ['close', 'signals']


# ================================================================ position_monitor: the TIME label (19.6)
@pytest.fixture
def monitor(venue, db, creds, monkeypatch):
    from cryptography.fernet import Fernet
    monkeypatch.setattr(pm, 'cipher', Fernet(os.environ['CREDENTIALS_ENCRYPTION_KEY'].encode()))
    m = pm.PositionMonitor(FakePool(db))

    async def no_scan(user):
        return []
    monkeypatch.setattr(m, 'scan_exchange_fills', no_scan)
    return m


def bare_flat(db, venue, creds, **kw):
    """A row whose position is already gone from the venue with no legs left (the monitor's 'closed' case)."""
    row, _, _, _ = arm(db, venue, creds, venue_qty=0, **kw)
    for o in list(venue.orders(wallet(row['user_id']))):
        venue.external_cancel(wallet(row['user_id']), o['oid'])
    return row


def monitor_row(db, row):
    u = db.users[row['user_id']]
    return dict(db.positions[row['id']], user_api_key=u['api_key'],
                hl_private_key_encrypted=u['hl_private_key_encrypted'], hl_wallet_address=u['hl_wallet_address'],
                fee_tier='standard')


async def test_monitor_labels_a_time_close_time_and_records_one_trade(loop, db, venue, creds, monitor):
    row, _, _, _ = arm(db, venue, creds)
    await loop.process_close_jobs()
    await monitor.check_position(monitor_row(db, row))
    [trade] = db.trades
    assert trade['exit_type'] == 'TIME' and db.positions[row['id']]['status'] == 'closed'
    assert trade['exit_price'] == 0.49 and trade['position_size'] == 100.0


async def test_monitor_keeps_the_sl_label_when_the_sl_won_the_race(loop, db, venue, creds, monitor):
    row, sl, _, _ = arm(db, venue, creds)

    def sl_fires(a, c, b, sz, px, ot, ro):
        if 'limit' in ot and ro and venue.szi(a, c) != 0:
            venue.set_position(a, c, 0, 0.5)
            venue.external_cancel(a, sl)
            venue.add_fill(a, c, 'A', 100.0, 0.45, 'Close Long', venue.now_ms, sl, 100.0)
        return NotImplemented
    venue.script('order', sl_fires)
    await loop.process_close_jobs()
    await monitor.check_position(monitor_row(db, row))
    [trade] = db.trades
    assert trade['exit_type'] == 'SL'


async def test_monitor_tick_mid_close_waits_for_the_close_then_records_time(db, venue, creds, monitor):
    row = bare_flat(db, venue, creds)
    db.positions[row['id']]['exit_hint'] = 'TIME'                              # hint written, fill not yet recorded
    venue.add_fill(wallet(7), 'ADA', 'A', 100.0, 0.49, 'Close Long', venue.now_ms, 999, 100.0)
    tok = follower_guard.mark(7)                                               # execute_close is mid-flight
    try:
        await monitor.check_position(monitor_row(db, row))
        assert db.trades == [] and db.positions[row['id']]['status'] == 'open'
    finally:
        follower_guard.clear(7, tok)
    db.positions[row['id']]['close_fill_qty'] = 100.0
    await monitor.check_position(monitor_row(db, row))
    [trade] = db.trades
    assert trade['exit_type'] == 'TIME'


async def test_monitor_without_a_hint_labels_by_distance_as_before(db, venue, creds, monitor):
    row = bare_flat(db, venue, creds)
    venue.add_fill(wallet(7), 'ADA', 'A', 100.0, 0.549, 'Close Long', venue.now_ms, 999, 100.0)
    await monitor.check_position(monitor_row(db, row))
    [trade] = db.trades
    assert trade['exit_type'] == 'TP'


async def test_monitor_exit_hint_read_failure_falls_back_to_the_distance_label(db, venue, creds, monitor):
    row = bare_flat(db, venue, creds)
    db.positions[row['id']].update(exit_hint='TIME', close_fill_qty=100.0)
    db.fail_next.append(('t1c:exit_hint', RuntimeError('column does not exist')))
    venue.add_fill(wallet(7), 'ADA', 'A', 100.0, 0.46, 'Close Long', venue.now_ms, 999, 100.0)
    await monitor.check_position(monitor_row(db, row))
    [trade] = db.trades
    assert trade['exit_type'] == 'SL'


# ================================================================ position_guardian: close jobs make the user busy
@pytest.fixture
def g(venue, db, creds, notes, fast_sleep):
    guardian = pg.HLPositionGuardian(FakePool(db))
    guardian.world = fake_hl.World(guardian, db, venue)
    db.add_user(7, encrypt=creds)
    yield guardian
    guardian._pool.shutdown(wait=False)


@pytest.mark.parametrize('status, age_s, busy', [('pending', 0, True), ('done', 10, True),
                                                 ('done', cg.CLOSE_BUSY_WINDOW_S + 5, False),
                                                 ('deferred', 0, False), ('failed', 0, False),
                                                 ('expired', 0, False), ('skipped', 0, False)])
async def test_guardian_busy_rule_for_close_jobs(g, db, status, age_s, busy):
    db.add_master_close('a' * 32)
    job = db.add_job(1, 7, 'a' * 32, status=status)
    job['updated_at'] = db.now() - timedelta(seconds=age_s)
    assert await g._busy(7) is busy
    assert await g._busy(8) is False


async def test_guardian_pass_never_replaces_a_tp_the_close_cancelled(g, db, venue):
    sl = venue.add_order(wallet(7), 'ADA', 'A', 4000, 0.45, 'Stop Market')
    row = db.add_position(user_id=7, status='open', side='BUY', quantity=4000.0, target_sl=0.45, target_tp=0.55,
                          entry_order_id='900', sl_order_id=str(sl), tp_order_id='123',
                          opened_at=g.world.t - timedelta(minutes=10))
    venue.set_position(wallet(7), 'ADA', 4000.0, 0.5)
    db.add_master_close('b' * 32)
    db.add_job(row['id'], 7, 'b' * 32, status='pending')
    await g._pass(dry=False, start=0, deadline=g.world.mono + pg.PASS_S)
    assert mutations(venue) == []


async def test_guardian_close_busy_read_failure_fails_open(g, db):
    db.fail_next.append(('hlg:close_busy', RuntimeError('relation "close_jobs" does not exist')))
    assert await g._busy(7) is False


def test_guardian_busy_window_is_the_shared_constant():
    assert "INTERVAL '%d seconds'" % cg.CLOSE_BUSY_WINDOW_S in pg.SQL_CLOSE_BUSY
    assert "status = 'pending'" in pg.SQL_CLOSE_BUSY and 'deferred' not in pg.SQL_CLOSE_BUSY


# ================================================================ the endpoints (SQLite, real SQL)
NOW_MS = 1_790_000_000_000


@pytest.fixture
def sql(monkeypatch, clog):
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    fm.Base.metadata.create_all(engine)
    with engine.begin() as c:
        c.execute(text('CREATE UNIQUE INDEX ux_signals_master_trade_id ON signals(master_trade_id) '
                       'WHERE master_trade_id IS NOT NULL'))
    session = sessionmaker(bind=engine)()
    app = FastAPI()
    app.include_router(fe.router)
    app.dependency_overrides[fe.get_db] = lambda: session
    monkeypatch.setattr(fe, 'MASTER_API_KEY', MASTER_KEY)
    monkeypatch.setattr(fe, '_now_ms', lambda: NOW_MS)
    client = TestClient(app)
    client.session, client.engine = session, engine
    return client


def q(client, sql_text, **params):
    with client.engine.connect() as c:
        return [dict(r._mapping) for r in c.execute(text(sql_text), params)]


def seed(client):
    """Users 1-6; one v2 entry (MID) and one legacy entry; rows covering every I2 case."""
    s = client.session
    for uid in range(1, 7):
        s.add(fm.User(id=uid, api_key='nk_%d' % uid, email='u%d@example.com' % uid, access_granted=True))
    s.add(fm.Signal(id=1, signal_id='sig-v2', action='BUY', symbol='ADA/USDT', entry_price=0.5, stop_loss=0.45,
                    take_profit=0.55, leverage=2, risk_pct=0.01, master_trade_id=MID))
    s.add(fm.Signal(id=2, signal_id='sig-legacy', action='BUY', symbol='ADA/USDT', entry_price=0.5, stop_loss=0.45,
                    take_profit=0.55, leverage=2, risk_pct=0.02))
    rows = [(1, 1, 'open'), (2, 1, 'needs_recovery'), (3, 1, 'closed'), (4, None, 'open'), (5, 2, 'open')]
    for uid, sig, status in rows:                                              # user 6: dedup-skipped, no row
        s.add(fm.OpenPosition(id=uid, user_id=uid, signal_id=sig, entry_order_id='E%d' % uid, symbol='ADA/USDT',
                              hl_coin='ADA', side='BUY', quantity=100.0, target_tp=0.55, target_sl=0.45,
                              status=status))
    s.commit()


MID = '0123456789abcdef0123456789abcdef'
KEY = {'x-master-key': MASTER_KEY}


def close_body(**kw):
    body = {'protocol': 2, 'action': 'CLOSE', 'master_trade_id': MID, 'symbol': 'ADA/USDT', 'side': 'BUY',
            'reason': 'time_exit', 'master_closed_ms': NOW_MS - 1000}
    body.update(kw)
    return body


def test_close_unset_master_key_is_503_and_writes_nothing(sql, monkeypatch):
    monkeypatch.setattr(fe, 'MASTER_API_KEY', None)
    assert sql.post('/api/broadcast-close', json=close_body(), headers=KEY).status_code == 503
    assert q(sql, 'SELECT * FROM master_closes') == []


@pytest.mark.parametrize('headers', [{}, {'x-master-key': 'wrong'}])
def test_close_wrong_or_missing_key_is_401_and_writes_nothing(sql, headers):
    assert sql.post('/api/broadcast-close', json=close_body(), headers=headers).status_code == 401
    assert q(sql, 'SELECT * FROM master_closes') == []


def test_close_contract_fixture_matrix_through_the_endpoint(sql, monkeypatch):
    with open(FIXTURE, encoding='utf-8') as f:
        fx = json.load(f)
    monkeypatch.setattr(fe, '_now_ms', lambda: fx['now_ms'])
    seen = set()
    for case in fx['cases']:
        payload = case['payload']
        if case['expect'] == 200 and isinstance(payload, dict):
            mid = payload['master_trade_id']
            r = sql.post('/api/broadcast-close', json=payload, headers=KEY)
            assert r.status_code == 200, (case['name'], r.text)
            assert r.json()['duplicate'] is (mid in seen)
            seen.add(mid)
        else:
            r = sql.post('/api/broadcast-close', json=payload, headers=KEY)
            assert r.status_code == 422, (case['name'], r.text)
            assert str(r.json()['detail']).startswith(case['field']), case['name']
    assert {m['master_trade_id'] for m in q(sql, 'SELECT master_trade_id FROM master_closes')} == seen


def test_close_rejects_a_non_json_body_with_422(sql, clog):
    r = sql.post('/api/broadcast-close', content=b'not json', headers=dict(KEY, **{'content-type': 'application/json'}))
    assert r.status_code == 422
    assert q(sql, 'SELECT * FROM master_closes') == [] and clog.events(cg.EV_PAYLOAD_REJECTED)


def test_close_matches_only_by_master_trade_id_and_active_status(sql, clog):
    seed(sql)
    r = sql.post('/api/broadcast-close', json=close_body(), headers=KEY)
    assert r.status_code == 200
    assert r.json() == {'accepted': True, 'duplicate': False, 'followers_matched': 2}
    jobs = q(sql, 'SELECT position_id, user_id, master_trade_id, status, attempts FROM close_jobs ORDER BY position_id')
    assert jobs == [{'position_id': 1, 'user_id': 1, 'master_trade_id': MID, 'status': 'pending', 'attempts': 0},
                    {'position_id': 2, 'user_id': 2, 'master_trade_id': MID, 'status': 'pending', 'attempts': 0}]
    [mc] = q(sql, 'SELECT * FROM master_closes')
    assert (mc['symbol'], mc['side'], mc['reason'], mc['master_closed_ms'], mc['matched']) == (
        'ADA/USDT', 'BUY', 'time_exit', NOW_MS - 1000, 2)
    statuses = {r['id']: r['status'] for r in q(sql, 'SELECT id, status FROM open_positions')}
    assert statuses == {1: 'open', 2: 'needs_recovery', 3: 'closed', 4: 'open', 5: 'open'}   # I5: rows untouched
    [rec] = clog.events(cg.EV_RECEIVED)
    assert rec['master_trade_id'] == MID and rec['followers_matched'] == 2
    assert sorted(m['position_id'] for m in rec['matched_rows']) == [1, 2]
    assert len(clog.events(cg.EV_JOB_CREATED)) == 2


def test_close_repeat_is_an_idempotent_duplicate(sql, clog):
    seed(sql)
    first = sql.post('/api/broadcast-close', json=close_body(), headers=KEY).json()
    again = sql.post('/api/broadcast-close', json=close_body(master_closed_ms=NOW_MS), headers=KEY)
    assert again.status_code == 200
    assert again.json() == {'accepted': True, 'duplicate': True, 'followers_matched': first['followers_matched']}
    assert len(q(sql, 'SELECT * FROM master_closes')) == 1 and len(q(sql, 'SELECT * FROM close_jobs')) == 2
    assert clog.events(cg.EV_DUPLICATE)


def test_close_with_no_matching_row_logs_close_no_match(sql, clog):
    seed(sql)
    r = sql.post('/api/broadcast-close', json=close_body(master_trade_id='f' * 32), headers=KEY)
    assert r.json() == {'accepted': True, 'duplicate': False, 'followers_matched': 0}
    assert clog.events(cg.EV_NO_MATCH) and q(sql, 'SELECT * FROM close_jobs') == []


def test_close_known_symbols_come_from_the_env(sql, monkeypatch):
    assert sql.post('/api/broadcast-close', json=close_body(symbol='BTC/USDT'), headers=KEY).status_code == 422
    monkeypatch.setenv('CLOSE_KNOWN_SYMBOLS', 'ADA/USDT, BTC/USDT')
    assert sql.post('/api/broadcast-close', json=close_body(symbol='BTC/USDT'), headers=KEY).status_code == 200


def test_a_close_posted_to_broadcast_signal_is_422(sql):
    r = sql.post('/api/broadcast-signal', json=close_body(), headers=KEY)
    assert r.status_code == 422
    assert q(sql, 'SELECT * FROM signals') == []


ENTRY = {'action': 'BUY', 'symbol': 'ADA/USDT', 'entry_price': 0.5, 'stop_loss': 0.45, 'take_profit': 0.55,
         'leverage': 2, 'risk_pct': 0.01}


def test_v2_entry_stores_master_trade_id_and_a_repeat_is_a_duplicate_without_deliveries(sql):
    seed(sql)
    body = dict(ENTRY, protocol=2, master_trade_id='e' * 32)
    r = sql.post('/api/broadcast-signal', json=body, headers=KEY)
    assert r.status_code == 200, r.text
    assert r.json()['delivered_to'] == 6 and r.json().get('duplicate') is not True
    [sig] = q(sql, "SELECT id, signal_id, master_trade_id, risk_pct FROM signals WHERE master_trade_id = :m",
              m='e' * 32)
    assert sig['signal_id'] == r.json()['signal_id'] and sig['risk_pct'] == 0.01
    n_deliveries = len(q(sql, 'SELECT * FROM signal_deliveries'))
    again = sql.post('/api/broadcast-signal', json=body, headers=KEY)
    assert again.status_code == 200
    assert again.json()['duplicate'] is True and again.json()['delivered_to'] == 0
    assert again.json()['signal_id'] == sig['signal_id']
    assert len(q(sql, "SELECT * FROM signals WHERE master_trade_id = :m", m='e' * 32)) == 1
    assert len(q(sql, 'SELECT * FROM signal_deliveries')) == n_deliveries


def test_v1_entry_without_an_id_is_unchanged(sql):
    seed(sql)
    r = sql.post('/api/broadcast-signal', json=ENTRY, headers=KEY)
    assert r.status_code == 200 and r.json()['delivered_to'] == 6
    assert q(sql, "SELECT master_trade_id FROM signals WHERE signal_id = :s", s=r.json()['signal_id']) == [
        {'master_trade_id': None}]


@pytest.mark.parametrize('change', [{'master_trade_id': 'E' * 32}, {'master_trade_id': 'e' * 31},
                                    {'master_trade_id': ''}, {'protocol': 3}, {'protocol': 0}])
def test_v2_entry_fields_are_checked_with_422_before_any_write(sql, change):
    r = sql.post('/api/broadcast-signal', json=dict(dict(ENTRY, protocol=2, master_trade_id='e' * 32), **change),
                 headers=KEY)
    assert r.status_code == 422, r.text
    assert q(sql, 'SELECT * FROM signals') == []


# ================================================================ section 21: close events, no secrets
async def test_close_events_carry_the_envelope_and_never_a_secret(loop, db, venue, creds, clog, sql):
    seed(sql)
    sql.post('/api/broadcast-close', json=close_body(), headers=KEY)
    sql.post('/api/broadcast-close', json=close_body(), headers=KEY)
    sql.post('/api/broadcast-close', json=close_body(side='LONG'), headers=KEY)
    arm(db, venue, creds)
    await loop.process_close_jobs()
    recs = clog.events()
    assert {r['event'] for r in recs} >= {cg.EV_RECEIVED, cg.EV_DUPLICATE, cg.EV_PAYLOAD_REJECTED,
                                          cg.EV_JOB_CREATED, cg.EV_JOB_STEP, cg.EV_JOB_OUTCOME}
    for r in recs:
        assert all(k in r for k in cg.REQUIRED_EVENT_FIELDS), r
        assert (r['venue'], r['service']) == ('hyperliquid', 'hyperliquid-nike-rocket-api')
    with open(clog.path, encoding='utf-8') as f:
        raw = f.read()
    for secret in (MASTER_KEY, private_key(7), '%064x' % 7, db.users[7]['hl_private_key_encrypted']):
        assert secret not in raw
    for r in recs:                                                             # sha-named fields stay readable
        rest = {k: v for k, v in r.items() if not cg.SAFE_HEX_KEY_RE.search(k)}
        assert not cg.SECRET_VALUE_RE.search(json.dumps(rest)), r


async def test_close_config_flags_are_logged_once_at_the_first_cycle(loop, db, clog):
    await loop.process_close_jobs()
    await loop.process_close_jobs()
    [cfg] = clog.events('CLOSE_CONFIG')
    assert cfg['CLOSE_JOBS_ENABLED'] is True and cfg['CLOSE_SKIP_INACTIVE_USERS'] is False


# ================================================================ unchanged shared files, dashboard, migration, gates
def _norm_sha(path):
    with open(os.path.join(ROOT, path), 'rb') as f:
        return hashlib.sha256(f.read().replace(b'\r\n', b'\n')).hexdigest()


@pytest.mark.parametrize('path, sha', [
    ('signal_guard.py', 'ed2e51e48e43890dd6d96282e3a8aa38781f9b9bbdd4fc8cb5c272a2ae63b481'),     # I6: = 1570186
    ('follower_guard.py', '10ef3904273e8461b0cad2347bb6cd2eabdd2d96e9b2d9d5cb8c8c8b9e9d007e'),
    ('bracket_guard.py', '3af9754d61c4e2bc1bb357f1b637b9cc7b32f85dbd47bc9dd59578000071500a')])
def test_shared_wave1_modules_are_byte_identical_to_the_wave1_build(path, sha):
    assert _norm_sha(path) == sha


DASHBOARD_FSTRING_SHA256 = '726a54ba90257b5899e851f383d16b6672fb819053122a2bce68065d1b7b7156'


def _main_src():
    with open(os.path.join(ROOT, 'main.py'), encoding='utf-8') as f:
        return f.read().replace('\r\n', '\n')


def test_dashboard_fstring_is_unchanged_and_renders():
    src = _main_src()
    fn = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.AsyncFunctionDef)
              and n.name == 'portfolio_dashboard')
    node = next(n.value for n in ast.walk(fn) if isinstance(n, ast.Assign)
                and getattr(n.targets[0], 'id', None) == 'html' and isinstance(n.value, ast.JoinedStr))
    seg = ast.get_source_segment(src, node)
    assert hashlib.sha256(seg.encode('utf-8')).hexdigest() == DASHBOARD_FSTRING_SHA256
    html = eval(compile(ast.Expression(node), 'main.py', 'eval'), {'__builtins__': {}}, {'api_key': 'nk_x'})
    assert 'nk_x' in html


MIGRATIONS = (
    'ALTER TABLE signals ADD COLUMN IF NOT EXISTS master_trade_id VARCHAR(32)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_master_trade_id ON signals(master_trade_id) '
    'WHERE master_trade_id IS NOT NULL',
    'ALTER TABLE open_positions ADD COLUMN IF NOT EXISTS exit_hint VARCHAR(16)',
    'ALTER TABLE open_positions ADD COLUMN IF NOT EXISTS close_fill_qty NUMERIC',
    'CREATE TABLE IF NOT EXISTS master_closes',
    'CREATE TABLE IF NOT EXISTS close_jobs')


def test_main_migration_is_additive_idempotent_and_isolated():
    src = _main_src()
    strings = [' '.join(n.value.split()) for n in ast.walk(ast.parse(src))
               if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    for stmt in MIGRATIONS:
        assert any(s.startswith(stmt) for s in strings), stmt
    t1 = [s for s in strings if any(k in s for k in ('master_trade_id', 'exit_hint', 'close_fill_qty',
                                                     'master_closes', 'close_jobs'))]
    for s in t1:
        assert 'IF NOT EXISTS' in s and not any(w in s.upper() for w in ('DROP ', 'DELETE ', 'TRUNCATE ')), s


def test_orm_models_carry_the_close_columns_and_tables():
    assert fm.Signal.__table__.c.master_trade_id.nullable
    assert fm.Signal.__table__.c.master_trade_id.type.length == 32
    assert fm.OpenPosition.__table__.c.exit_hint.type.length == 16
    assert fm.OpenPosition.__table__.c.close_fill_qty.nullable
    assert [c.name for c in fm.MasterClose.__table__.primary_key] == ['master_trade_id']
    assert {c.name for c in fm.MasterClose.__table__.c} == {'master_trade_id', 'symbol', 'side', 'reason',
                                                            'master_closed_ms', 'received_at', 'matched'}
    assert [c.name for c in fm.CloseJob.__table__.primary_key] == ['position_id']
    assert {c.name for c in fm.CloseJob.__table__.c} == {'position_id', 'user_id', 'master_trade_id', 'status',
                                                         'attempts', 'last_error', 'created_at', 'updated_at'}


def test_run_gates_lists_the_close_files():
    import importlib.util
    spec = importlib.util.spec_from_file_location('hl_run_gates_t07', os.path.join(ROOT, 'tools', 'run_gates.py'))
    gates = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gates)
    assert {'test_close_common', 'test_master_close'} <= set(gates.TESTS)
    assert 'close_guard.py' in gates.MODULES
    shared = dict(gates.SHARED)
    assert shared['close_guard.py'] == ('kraken-nike-rocket-api',)
    assert shared['tests/test_close_common.py'] == ('kraken-nike-rocket-api',)
    assert set(shared['tests/fixtures/close_v2.json']) == {'kraken-nike-rocket-api', 'massive-rocket-algos'}
    assert {'CLOSE_JOBS_ENABLED', 'CLOSE_SKIP_INACTIVE_USERS', 'CLOSE_KNOWN_SYMBOLS'} <= set(gates.SCRUB_ENV)
