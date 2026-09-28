"""follower_guard: the follower row policy, in-flight registry and helpers shared by both follower services.

Byte-identical in kraken-nike-rocket-api and hyperliquid-nike-rocket-api. Builder B1 writes both copies; B7/B8 import
it and never edit it (run_gates compares sha256). Pure: stdlib + bracket_guard, no DB and no exchange calls.

Spec: docs/superpowers/specs/2026-09-28-wave1-phase0-safety-design.md (massive-rocket-algos), change C33 (rev 3)
plus amendments A2 (fills-evidence window excludes the row's own entry) and A6 (in-flight tokens end with their
task, 600 s age cap, INFLIGHT_TOKEN_EXPIRED) and A10 (followers keep replacing service orders:
decide(..., level_policy=DECIDE_LEVEL_POLICY)).
"""
import asyncio
import itertools
import logging
import math
import os
import threading
import time
from datetime import timezone

import bracket_guard as bg

ACTIVE_STATUSES = ('open', 'needs_recovery')
INFLIGHT_MAX_AGE_S = 600.0          # A6 (was 180): a token also ends as soon as its asyncio task is done
ROW_POS_SKEW_S = 300.0
FLAT_ROW_UNRECORDED_S = 600.0
FILLS_LOOKBACK_PAD_S = 60.0
PLACEHOLDER_OIDS = frozenset({'', 'none', 'unknown', 'unknown-after-exception'})
DECIDE_LEVEL_POLICY = bg.LEVEL_REPLACE   # A10: followers replace (service) legs away from the row levels

_lock = threading.Lock()
_inflight = {}                      # user_id -> {token: (monotonic start, asyncio task or None)}
_expired = []                       # INFLIGHT_TOKEN_EXPIRED events not yet drained by a guardian
_tokens = itertools.count(1)
_clock = time.monotonic             # tests patch this, never time.monotonic itself
_log = logging.getLogger('follower_guard')


def _current_task():
    try:
        return asyncio.current_task()
    except RuntimeError:            # no running event loop
        return None


def mark(user_id):
    """First statement of execute_trade (I12): register one in-flight execution; returns the token clear() needs."""
    task = _current_task()
    with _lock:
        tok = next(_tokens)
        _inflight.setdefault(user_id, {})[tok] = (_clock(), task)
    return tok


def clear(user_id, token):
    """In execute_trade's finally: end this execution's token (other executions of the user stay in flight)."""
    with _lock:
        d = _inflight.get(user_id)
        if d is not None:
            d.pop(token, None)
            if not d:
                _inflight.pop(user_id, None)


def is_inflight(user_id, max_age_s=INFLIGHT_MAX_AGE_S):
    """True while any token of user_id is live: younger than max_age_s and, when marked inside an asyncio task, that
    task not done. A token whose task finished without clear() is dropped. A token that ages out while its task is
    still running is dropped and recorded as INFLIGHT_TOKEN_EXPIRED (critical log line; drain_expired())."""
    now = _clock()
    live = False
    with _lock:
        d = _inflight.get(user_id) or {}
        for tok, (t0, task) in list(d.items()):
            if task is not None and task.done():
                d.pop(tok)
                continue
            age = now - t0
            if age >= max_age_s:
                d.pop(tok)
                if task is not None:
                    _expired.append({'user_id': user_id, 'token': tok, 'age_s': round(age, 1)})
                    _log.critical('INFLIGHT_TOKEN_EXPIRED user_id=%s token=%s age_s=%.1f (execute_trade still running)',
                                  user_id, tok, age)
                continue
            live = True
        if not d:
            _inflight.pop(user_id, None)
    return live


def drain_expired():
    """INFLIGHT_TOKEN_EXPIRED events since the last call: [{'user_id', 'token', 'age_s'}]. Each guardian pass reports
    them as critical alarms (error_logs 'ALARM_INFLIGHT_TOKEN_EXPIRED' + notify_admin)."""
    with _lock:
        out = list(_expired)
        _expired.clear()
    return out


def guardian_mode(allowed, default='shadow'):
    """GUARDIAN_MODE env restricted to `allowed` (Kraken: legacy|shadow|enforce; HL: off|shadow|enforce)."""
    v = (os.getenv('GUARDIAN_MODE') or default).strip().lower()
    return v if v in allowed else default


def real_oid(v):
    """A real exchange order id as str, or None for NULL/''/'none'/'unknown'/'unknown-after-exception'."""
    s = '' if v is None else str(v).strip()
    return None if s.lower() in PLACEHOLDER_OIDS else s


def row_ts(dt):
    """Epoch seconds of an open_positions timestamp; naive values are UTC."""
    if dt is None:
        return None
    if getattr(dt, 'tzinfo', None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def row_side(row):
    s = str(row.get('side') or '').upper()
    return 1 if s in ('BUY', 'LONG') else (-1 if s in ('SELL', 'SHORT') else 0)


def rec_from_row(row, basis, tp_kind):
    """bracket_guard.Rec from a row's target_sl/target_tp (None for missing, non-finite, non-positive or inverted
    levels); only real order ids are trusted."""
    side = row_side(row)
    try:
        sl, tp = float(row['target_sl']), float(row['target_tp'])
    except (KeyError, TypeError, ValueError):
        return None
    if not (math.isfinite(sl) and math.isfinite(tp) and sl > 0 and tp > 0):
        return None
    if not ((side > 0 and sl < tp) or (side < 0 and tp < sl)):
        return None
    sl_oid, tp_oid = real_oid(row.get('sl_order_id')), real_oid(row.get('tp_order_id'))
    return bg.Rec(side, sl, tp, basis, tp_kind, status='active', sl_oid=sl_oid,
                  sl_oid_trusted=sl_oid is not None, tp_oid=tp_oid, created_ts=row_ts(row.get('opened_at')))


def select_row(active_rows, pos, lot, new_position_evidence=frozenset()):
    """active_rows: this (user, symbol)'s rows with status in ACTIVE_STATUSES. Returns (row, None) for exactly one
    row consistent with the live position, (None, 'ROW_AMBIGUOUS') otherwise, (None, None) without rows."""
    if not active_rows:
        return None, None
    if len(active_rows) > 1:
        return None, 'ROW_AMBIGUOUS'
    row = active_rows[0]
    if pos is None or pos is bg.UNKNOWN:
        return row, None
    if row_side(row) != pos.side or row.get('id') in new_position_evidence:
        return None, 'ROW_AMBIGUOUS'
    try:
        row_qty = float(row.get('quantity') or 0)
    except (TypeError, ValueError):
        row_qty = 0.0
    if row_qty > 0 and pos.qty > row_qty + lot / 2:
        return None, 'ROW_AMBIGUOUS'
    opened = row_ts(row.get('opened_at'))
    if (pos.opened_ts is not None and opened is not None and pos.opened_ts > opened + ROW_POS_SKEW_S
            and pos.qty >= row_qty - lot / 2):
        return None, 'ROW_AMBIGUOUS'
    return row, None


def service_oids(rows):
    return {o for r in rows for o in (real_oid(r.get('tp_order_id')), real_oid(r.get('sl_order_id'))) if o}


def filter_actions(acts, row, owned, pos=None, ref_px=None):
    """Follower policy over a decide()/ChurnGuard plan -> (kept_actions, alarms). Plans with ADOPT,
    RECORD_SIDE_MISMATCH or a NAKED close are dropped whole; a PLACE_TP through a crossed level is dropped with its
    TP cancels; only owned (service) oids are cancelled; a flat 'open' row gets no cancel/CLEAR (position_monitor
    owns it); CLEAR on a needs_recovery row becomes MARK_ROW_CLOSED_MANUAL."""
    if any(a.kind == 'ADOPT' or (a.kind == 'ALARM' and a.code == 'RECORD_SIDE_MISMATCH')
           or (a.kind == 'CLOSE' and a.code == 'NAKED_UNKNOWN_LEVEL') for a in acts):
        return [], [('FOLLOWER_UNTRACKED_PLAN_DROPPED', 'critical',
                     {'plan': ['%s:%s' % (a.kind, a.code) for a in acts]})]
    alarms, tp_crossed = [], False
    for a in acts:
        if (a.kind == 'PLACE_TP' and pos is not None and pos is not bg.UNKNOWN and ref_px is not None
                and a.level is not None and (ref_px >= a.level if pos.side > 0 else ref_px <= a.level)):
            tp_crossed = True
            alarms.append(('TP_LEVEL_CROSSED', 'critical', {'tp': a.level, 'px': ref_px}))
    status = (row or {}).get('status')
    kept = []
    for a in acts:
        if tp_crossed and (a.kind == 'PLACE_TP' or (a.kind == 'CANCEL' and a.code == 'TP_EXTRA_OR_INVALID')):
            continue
        if a.kind == 'CANCEL':
            oid = real_oid(a.leg.oid if a.leg is not None else None)
            if oid is None or oid not in owned:
                alarms.append(('FOREIGN_LEG_KEPT', 'info', {'oid': oid, 'code': a.code}))
                continue
            if status == 'open' and a.code == 'ORPHAN_LEG':
                continue
        if a.kind == 'CLEAR_RECORD':
            if status == 'needs_recovery':
                kept.append(bg.Action('MARK_ROW_CLOSED_MANUAL', 'FLAT_CONFIRMED'))
            continue
        kept.append(a)
    return kept, alarms


def legacy_rowless_should_close(symbol_orders, tracked_open_rows):
    """Exactly today's position_guardian._check_user_for_unprotected predicate (Kraken rowless rule)."""
    has_tp = any(o.get('type') in ('limit',) and o.get('reduceOnly', False) for o in symbol_orders)
    has_sl = any(o.get('type') in ('stop', 'stop_market', 'stopMarket') and o.get('reduceOnly', False)
                 for o in symbol_orders)
    return not (has_tp or has_sl or bool(tracked_open_rows and tracked_open_rows > 0))


def fills_show_new_entry(fills, row_side_int, since_ms, entry_order_id=None):
    """fills: normalised dicts {'side': 'buy'|'sell', 'ts_ms': int, 'opening': True|False|None, 'order_id': str|None}.
    True when a fill at/after since_ms on the row's entry side opened (or may have opened) a new position. The row's
    own entry fill (order_id == real_oid(entry_order_id)) is never evidence (A2)."""
    want = 'buy' if row_side_int > 0 else 'sell'
    own = real_oid(entry_order_id)
    for f in fills or ():
        ts = f.get('ts_ms')
        if ts is None or ts < since_ms or f.get('side') != want or f.get('opening') is False:
            continue
        if own is not None and real_oid(f.get('order_id')) == own:
            continue
        return True
    return False


class FlatTracker:
    """Confirmed-flat bookkeeping for active rows, per guardian process (R3-M6, A2). Only a CONFIRM_FLAT-confirmed
    flat is noted; a reappearing position is cleared by an equal fingerprint (Kraken fillTime) or by fills showing no
    new entry since since_ms(row); a new-entry fill makes the row ambiguous (evidence). The HL recovery lane and
    the full passes apply the same rule."""

    def __init__(self):
        self.flat = {}              # row_id -> {'first_ts': epoch s, 'passes': int}
        self.fp = {}                # row_id -> last fingerprint of the row's live, consistent position
        self.evidence = set()       # row ids with proof that a NEW position was opened after the flat

    def note_live(self, row_id, fingerprint):
        if row_id not in self.flat:
            self.fp[row_id] = fingerprint

    def note_confirmed_flat(self, row_id, now):
        s = self.flat.setdefault(row_id, {'first_ts': now, 'passes': 0})
        s['passes'] += 1
        return now - s['first_ts']

    def pending_check(self, row_id):
        return row_id in self.flat and row_id not in self.evidence

    def since_ms(self, row):
        """A2: max(first confirmed flat - FILLS_LOOKBACK_PAD_S, row opened_at) in epoch ms. Takes the ROW (dict-like
        with 'id' and 'opened_at'): rows are written after their own entry fill, so that fill is excluded."""
        if not hasattr(row, 'get'):
            raise TypeError('FlatTracker.since_ms(row) takes the open_positions row (A2), not its id')
        base = self.flat[row['id']]['first_ts'] - FILLS_LOOKBACK_PAD_S
        opened = row_ts(row.get('opened_at'))
        return int((base if opened is None else max(base, opened)) * 1000)

    def resolve(self, row_id, new_position, fingerprint=None):
        if new_position:
            self.evidence.add(row_id)
        else:
            self.flat.pop(row_id, None)
            if fingerprint is not None:
                self.fp[row_id] = fingerprint

    def gc(self, active_row_ids):
        keep = set(active_row_ids)
        for d in (self.flat, self.fp):
            for k in [k for k in d if k not in keep]:
                d.pop(k)
        self.evidence &= keep
