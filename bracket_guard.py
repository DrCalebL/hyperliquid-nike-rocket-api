"""bracket_guard: the two-leg bracket invariant shared by the five master ports and both follower guardians.

Byte-identical in massive-rocket-algos, kraken-nike-rocket-api and hyperliquid-nike-rocket-api. Builder B1 writes
all three copies; nobody else edits them (run_gates compares sha256).

Pure stdlib (dataclasses, typing, math): no I/O, no clock reads. Callers pass `now` (epoch seconds) and venue reads
already parsed into Pos / Leg / Rec with the parsers at the bottom of this file.

Spec: docs/superpowers/specs/2026-09-28-wave1-phase0-safety-design.md, change C01 (rev 3) and amendment A10
(owner, 2026-09-28): on masters a leg whose level differs from the recorded one is alarmed (LEG_LEVEL_MOVED) and
never cancelled, moved or replaced; a MISSING SL or TP is still re-placed (SL first). QC (Wave 1): owner stops on
another trigger basis or split into parts also count as the SL on masters. Followers keep replacing
service orders: they call decide(..., level_policy=LEVEL_REPLACE).
"""
import dataclasses
import math
from dataclasses import dataclass, field
from typing import Optional


class _Unknown:
    """Sentinel for a read that failed or returned an unrecognised shape (invariant I1: never act on it)."""

    def __repr__(self):
        return 'UNKNOWN'


UNKNOWN = _Unknown()

STOP_MARKET, STOP_LIMIT, TP_LIMIT, TP_TRIGGER_LIMIT, TP_TRIGGER_MARKET, OTHER = (
    'STOP_MARKET', 'STOP_LIMIT', 'TP_LIMIT', 'TP_TRIGGER_LIMIT', 'TP_TRIGGER_MARKET', 'OTHER')
SL_KINDS = (STOP_MARKET, STOP_LIMIT)
TP_KINDS = (TP_LIMIT, TP_TRIGGER_LIMIT, TP_TRIGGER_MARKET)

ENTRY_GRACE_S = 90
FLAT_CONFIRM_S = 5
FLAT_PERSIST_S = 240
PENDING_TTL_S = 120
TICK_TOL = 1.01
SL_PLACE_ATTEMPTS = 3
CHURN_WINDOW_S = 1800
SL_MAX_REPAIRS = 3
TP_MAX_REPAIRS = 3
TP_BACKOFF_S = (300, 600, 1200, 2400, 3600)
TP_SUSPEND_S = 3600

LEVEL_ALARM = 'alarm'        # masters (A10): a leg away from the recorded level -> LEG_LEVEL_MOVED only
LEVEL_REPLACE = 'replace'    # followers (rev 3): such a leg is replaced make-before-break (service orders only)
LEVEL_POLICIES = (LEVEL_ALARM, LEVEL_REPLACE)


@dataclass
class Pos:
    side: int                           # +1 long, -1 short
    qty: float                          # base units, > 0
    entry_px: Optional[float] = None
    opened_ts: Optional[float] = None   # epoch s; None when the venue reports no position time (HL)


@dataclass
class Leg:
    oid: str
    side: str                           # 'buy' | 'sell'
    kind: str                           # one of SL_KINDS, TP_KINDS or OTHER
    qty: Optional[float]                # None = whole position
    trigger: Optional[float] = None
    limit: Optional[float] = None
    reduce_only: bool = False
    close_position: bool = False
    position_level: bool = False        # attached to the position itself (Phemex fields): never cancelled
    basis: Optional[str] = None         # 'last' | 'mark' | 'index' | None (unknown)
    src: str = ''

    @property
    def level(self):
        return self.trigger if self.trigger is not None else self.limit


@dataclass
class Rec:
    side: int
    sl: float
    tp: Optional[float]
    sl_basis: Optional[str]
    tp_kind: str
    status: str = 'active'              # 'active' | 'pending' (write-ahead, entry not yet confirmed)
    sl_oid: Optional[str] = None
    sl_oid_trusted: bool = False
    tp_oid: Optional[str] = None
    created_ts: Optional[float] = None


@dataclass
class Action:
    kind: str       # ALARM | CONFIRM_FLAT | CANCEL | CLEAR_RECORD | ADOPT | PLACE_SL | PLACE_TP | CLOSE
    code: str = ''
    severity: str = 'warning'
    level: Optional[float] = None
    qty: Optional[float] = None
    leg: Optional[Leg] = None
    ctx: dict = field(default_factory=dict)


def _f(v):
    """float(v) when finite, else None (never raises)."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _pos(v):
    """A strictly positive finite float, else None."""
    x = _f(v)
    return x if x is not None and x > 0 else None


def beyond_sl(side, px, sl):
    return px <= sl if side > 0 else px >= sl


def _profit_side(pos, px):
    return pos.entry_px is None or (px > pos.entry_px if pos.side > 0 else px < pos.entry_px)


def trusted_sl_oid(rec):
    return str(rec.sl_oid) if (rec is not None and rec.sl_oid_trusted and rec.sl_oid) else None


# ---- venue parsers (rev 2, unchanged): identical payloads classify identically on masters and followers ----

def parse_binance_algo_response(resp):
    """fapiPrivateGetOpenAlgoOrders: a list or {'orders': [...]}; any other shape is UNKNOWN."""
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict) and isinstance(resp.get('orders'), list):
        return resp['orders']
    return UNKNOWN


_BN = {'STOP_MARKET': STOP_MARKET, 'STOP': STOP_LIMIT, 'TAKE_PROFIT_MARKET': TP_TRIGGER_MARKET,
       'TAKE_PROFIT': TP_TRIGGER_LIMIT}


def leg_from_binance_algo(a):
    if str(a.get('algoType', 'CONDITIONAL')).upper() != 'CONDITIONAL':
        return None
    cp = a.get('closePosition') is True or str(a.get('closePosition')).lower() == 'true'
    ro = a.get('reduceOnly') is True or str(a.get('reduceOnly')).lower() == 'true'
    return Leg(str(a.get('algoId') or a.get('orderId')), str(a.get('side', '')).lower(),
               _BN.get(str(a.get('orderType') or a.get('type') or '').upper(), OTHER),
               None if cp else (_f(a.get('quantity')) or 0.0),
               trigger=_pos(a.get('triggerPrice') or a.get('stopPrice')),
               limit=_pos(a.get('price')), reduce_only=ro, close_position=cp,
               basis='mark' if str(a.get('workingType')).upper() == 'MARK_PRICE' else 'last', src='algo')


def leg_from_binance_regular(o):
    """ccxt order from fetch_open_orders(symbol) (regular namespace)."""
    ro = bool(o.get('reduceOnly'))
    kind = TP_LIMIT if (o.get('type') == 'limit' and ro and not o.get('triggerPrice')) else OTHER
    return Leg(str(o['id']), str(o.get('side', '')).lower(), kind,
               _f(o.get('remaining')) if o.get('remaining') is not None else _f(o.get('amount')),
               limit=_pos(o.get('price')), reduce_only=ro, basis='last', src='regular')


def leg_from_kraken(o):
    """ccxt krakenfutures order from fetch_open_orders() (no-arg: includes trigger orders). The open stop's level
    lives in info['stopPrice']; the unified triggerPrice is None on openorders rows."""
    info = o.get('info') or {}
    ot = str(info.get('orderType') or info.get('type') or o.get('type') or '').lower()
    no_lim = _pos(info.get('limitPrice')) is None
    if ot in ('stop', 'stp'):
        kind = STOP_MARKET if no_lim else STOP_LIMIT
    elif ot == 'take_profit':
        kind = TP_TRIGGER_MARKET if no_lim else TP_TRIGGER_LIMIT
    elif ot in ('lmt', 'limit', 'post'):
        kind = TP_LIMIT
    else:
        kind = OTHER
    sig = str(info.get('triggerSignal') or 'last').lower()
    rem = o.get('remaining') if o.get('remaining') is not None else info.get('unfilledSize')
    return Leg(str(o.get('id')), str(o.get('side') or info.get('side') or '').lower(), kind,
               _f(rem) if rem is not None else _f(o.get('amount')),
               trigger=_pos(info.get('stopPrice')) or _pos(o.get('triggerPrice')),
               limit=_pos(info.get('limitPrice')) or _pos(o.get('price')),
               reduce_only=bool(o.get('reduceOnly') or info.get('reduceOnly')),
               basis='mark' if sig.startswith('mark') else ('index' if sig in ('spot', 'index') else 'last'))


def kraken_order_matches(o, unified, market_id):
    return o.get('symbol') == unified or str((o.get('info') or {}).get('symbol', '')).upper() == str(market_id).upper()


_HL = {'Stop Market': STOP_MARKET, 'Stop Limit': STOP_LIMIT, 'Take Profit Market': TP_TRIGGER_MARKET,
       'Take Profit Limit': TP_TRIGGER_LIMIT, 'Limit': TP_LIMIT}


def leg_from_hl(o):
    """One row of info.frontend_open_orders(address). Position TP/SL rows (isPositionTpsl, sz '0.0') are
    whole-position legs: qty None, close_position True."""
    whole = bool(o.get('isPositionTpsl')) or _f(o.get('sz')) == 0.0
    return Leg(str(o.get('oid')), 'sell' if o.get('side') == 'A' else 'buy', _HL.get(str(o.get('orderType')), OTHER),
               None if whole else _f(o.get('sz')), trigger=_pos(o.get('triggerPx')) if o.get('isTrigger') else None,
               limit=_pos(o.get('limitPx')), reduce_only=bool(o.get('reduceOnly')), close_position=whole, basis='mark')


def hl_tick(px, sz_decimals):
    if px and px > 0:
        return max(10 ** (math.floor(math.log10(px)) - 4), 10 ** -(6 - sz_decimals))
    return 10 ** -(6 - sz_decimals)


_PH = {'stop': STOP_MARKET, '3': STOP_MARKET, 'stoplimit': STOP_LIMIT, '4': STOP_LIMIT,
       'marketiftouched': TP_TRIGGER_MARKET, '5': TP_TRIGGER_MARKET, 'limitiftouched': TP_TRIGGER_LIMIT,
       '6': TP_TRIGGER_LIMIT, 'limit': TP_LIMIT, '2': TP_LIMIT}


def leg_from_phemex(o):
    info = o.get('info') or {}
    t = str(info.get('trigger') or '')
    return Leg(str(o.get('id')), str(o.get('side', '')).lower(),
               _PH.get(str(info.get('ordType') or info.get('orderType') or '').replace('_', '').lower(), OTHER),
               _f(o.get('remaining')) if o.get('remaining') is not None else _f(o.get('amount')),
               trigger=_pos(o.get('triggerPrice')) or _pos(info.get('stopPxRp')) or _pos(info.get('stopPx')),
               limit=_pos(o.get('price')),
               reduce_only=bool(o.get('reduceOnly') or info.get('reduceOnly') or info.get('closeOnTrigger')),
               basis='mark' if 'Mark' in t else ('last' if 'Last' in t else None))


def legs_from_phemex_position(p, xs):
    """Position-attached legs (stopLossRp / takeProfitRp): position_level, never cancelled."""
    info = p.get('info') or {}
    out = []
    if _pos(info.get('stopLossRp')):
        out.append(Leg('pos:sl', xs, STOP_MARKET, None, trigger=_pos(info.get('stopLossRp')),
                       close_position=True, position_level=True))
    if _pos(info.get('takeProfitRp')):
        out.append(Leg('pos:tp', xs, TP_TRIGGER_MARKET, None, trigger=_pos(info.get('takeProfitRp')),
                       close_position=True, position_level=True))
    return out


def _recorded_sl_on_book(rec, prot, tol, trusted):
    return any((trusted is not None and l.oid == trusted)
               or (l.kind in SL_KINDS and l.level is not None and abs(l.level - rec.sl) <= tol) for l in prot)


def _recorded_tp_on_book(rec, prot, tol):
    return rec.tp is not None and any(
        l.kind in TP_KINDS and l.level is not None and abs(l.level - rec.tp) <= tol for l in prot)


def _exit_stop_on_book(rec, prot):
    xs = 'sell' if rec.side > 0 else 'buy'
    return any(l.kind in SL_KINDS and l.side == xs for l in prot)


def _moved(leg_name, l, recorded):
    return Action('ALARM', 'LEG_LEVEL_MOVED', 'warning', level=l.level, leg=l,
                  ctx={'leg': leg_name, 'recorded': recorded, 'live': l.level, 'oid': l.oid})


def decide(pos, rec, legs, ref, tick, lot, now, flat_confirmed=False, flat_since=None, in_flight=False,
           level_policy=LEVEL_ALARM):
    """Plan for one symbol (masters) or one (user, symbol) pair (followers). Rules in order:
    0 UNKNOWN read -> ALARM only; 1 in_flight -> []; 2 de-dup by oid, only reduce_only / close_position /
    position_level legs count; 3 flat -> CONFIRM_FLAT, 240 s wait while the recorded SL rests, then CANCEL
    leftovers + CLEAR_RECORD; 4 levels from rec (else RECORD_SIDE_MISMATCH + untracked path); 5 valid SL;
    6 no SL -> CLOSE if price beyond on the SL's basis, else PLACE_SL then SL cancels; 7 TP (unknown level ->
    TP_LEVEL_UNKNOWN only; TP never closes); 8 other reduce-only legs -> FOREIGN_REDUCE_ONLY.
    Untracked path (QC): exactly one exit-side SL leg with a trigger (any level, either SL kind) -> ADOPT it; more
    than one, or only trigger-less ones -> AMBIGUOUS_SL_UNTRACKED; NAKED_UNKNOWN_LEVEL only with no SL leg at all.
    A10: with level_policy=LEVEL_ALARM an exit-side SL/TP leg away from the recorded level is never cancelled
    (LEG_LEVEL_MOVED), nor is a stop-market on another trigger basis (LEG_BASIS_CHANGED) or an exit-side leg of kind
    OTHER (LEG_UNCLASSIFIED_KEPT). The exit-side stop-markets together (any level or basis, quantities summed) count
    as the SL being present when they cover the position; the TP legs likewise. Stop-limits never count as the SL."""
    if level_policy not in LEVEL_POLICIES:
        raise ValueError('level_policy must be one of %s' % (LEVEL_POLICIES,))
    if pos is UNKNOWN or legs is UNKNOWN:
        return [Action('ALARM', 'GUARDIAN_READ_UNKNOWN')]
    if in_flight:
        return []
    legs = [dataclasses.replace(l) for l in {l.oid: l for l in legs}.values()]   # de-dup, never mutate the caller's
    prot = [l for l in legs if l.reduce_only or l.close_position or l.position_level]
    tol = tick * TICK_TOL
    trusted = trusted_sl_oid(rec)
    keep_moved = level_policy == LEVEL_ALARM
    if pos is None:
        if not prot and rec is None:
            return []
        if not flat_confirmed:
            return [Action('CONFIRM_FLAT')]
        if (rec is not None and rec.status != 'pending'
                and (_recorded_sl_on_book(rec, prot, tol, trusted) or (keep_moved and _exit_stop_on_book(rec, prot)))
                and (flat_since is None or now - flat_since < FLAT_PERSIST_S)):
            intact = _recorded_tp_on_book(rec, prot, tol)
            return [Action('ALARM', 'FLAT_WITH_INTACT_BRACKET' if intact else 'FLAT_SL_RESTING',
                           'warning' if intact else 'info')]
        acts = [Action('CANCEL', 'ORPHAN_LEG', leg=l) for l in prot if not l.position_level]
        if rec is not None and (rec.status != 'pending' or rec.created_ts is None
                                or now - rec.created_ts >= PENDING_TTL_S):
            acts.append(Action('CLEAR_RECORD', 'PENDING_EXPIRED' if rec.status == 'pending' else 'FLAT'))
        return acts
    xs = 'sell' if pos.side > 0 else 'buy'
    for l in prot:
        if trusted and l.oid == trusted and l.kind == OTHER:
            l.kind = STOP_MARKET
    sl_c = [l for l in prot if l.side == xs and l.kind in SL_KINDS]
    tp_c = [l for l in prot if l.side == xs and l.kind in TP_KINDS]
    rest = [l for l in prot if l not in sl_c and l not in tp_c and not l.position_level]
    acts = []
    if rec is not None and rec.side == pos.side:
        sl_lvl, tp_lvl, basis, tp_kind = rec.sl, rec.tp, rec.sl_basis, rec.tp_kind
    else:
        if rec is not None:
            acts.append(Action('ALARM', 'RECORD_SIDE_MISMATCH', 'critical'))
        # QC correctness-core-1: I4(d) closes only an untracked position with NO SL leg. Any exit-side stop counts,
        # at whatever level (a break-even or trailed stop sits at/through entry) and of either SL kind.
        cand = [l for l in sl_c if l.trigger is not None]
        if len(cand) > 1 or (not cand and sl_c):
            return acts + [Action('ALARM', 'AMBIGUOUS_SL_UNTRACKED', 'critical')]
        if not cand:
            if pos.opened_ts is not None and now - pos.opened_ts < ENTRY_GRACE_S:
                return acts + [Action('ALARM', 'UNTRACKED_NAKED_YOUNG')]
            return acts + [Action('CLOSE', 'NAKED_UNKNOWN_LEVEL', 'critical', qty=pos.qty)]
        tps = [l for l in tp_c if l.level is not None and _profit_side(pos, l.level)]
        sl_lvl, basis = cand[0].trigger, cand[0].basis or 'last'
        tp_lvl, tp_kind = (tps[0].level, tps[0].kind) if len(tps) == 1 else (None, TP_LIMIT)
        acts.append(Action('ADOPT', 'UNTRACKED_POSITION_ADOPTED', level=sl_lvl,
                           ctx={'tp': tp_lvl, 'basis': basis, 'tp_kind': tp_kind, 'sl_oid': cand[0].oid}))

    def covers(l):
        return l.close_position or l.qty is None or l.qty >= pos.qty - lot / 2

    def basis_ok(l):
        return l.basis is None or basis is None or l.basis == basis

    def sl_valid(l):
        if l.kind != STOP_MARKET:
            return False
        lvl_ok = (trusted is not None and l.oid == trusted) if l.trigger is None else abs(l.trigger - sl_lvl) <= tol
        return lvl_ok and covers(l) and basis_ok(l)

    def sum_covers(ls):
        return bool(ls) and (any(l.close_position or l.qty is None for l in ls)
                             or sum(l.qty for l in ls) >= pos.qty - lot / 2)

    ok_sl = [l for l in sl_c if sl_valid(l)]
    keep_sl = min(ok_sl, key=lambda l: (not l.position_level, l.oid != trusted, -(l.qty or 1e18))) if ok_sl else None
    moved_sl = [l for l in sl_c if keep_moved and l not in ok_sl
                and l.level is not None and abs(l.level - sl_lvl) > tol]                          # A10
    rebased_sl = [l for l in sl_c if keep_moved and l.kind == STOP_MARKET and l not in ok_sl and l not in moved_sl
                  and l.trigger is not None and not basis_ok(l)]                                  # A10 (QC)
    acts += [_moved('SL', l, sl_lvl) for l in moved_sl]
    acts += [Action('ALARM', 'LEG_BASIS_CHANGED', 'warning', level=l.level, leg=l,
                    ctx={'leg': 'SL', 'recorded_basis': basis, 'live_basis': l.basis, 'live': l.level, 'oid': l.oid})
             for l in rebased_sl]
    # QC money-masters-1 (A10): on masters the owner's exit-side stop-markets protect together, at any level, on any
    # basis and split into parts; the SL is missing only when they do not cover the position.
    owner_sl = [l for l in sl_c if keep_moved and l.kind == STOP_MARKET and l.trigger is not None]
    sl_by_owner = keep_sl is None and sum_covers(owner_sl)
    sl_present = keep_sl is not None or sl_by_owner
    if not sl_present:
        px = (ref or {}).get(basis)
        if px is not None and beyond_sl(pos.side, px, sl_lvl):
            return acts + [Action('CLOSE', 'SL_MISSING_PRICE_BEYOND', 'critical', qty=pos.qty,
                                  ctx={'px': px, 'basis': basis})]
        acts.append(Action('PLACE_SL', 'SL_REPAIRED', 'critical', level=sl_lvl, qty=pos.qty, ctx={'basis': basis}))
    elif keep_sl is not None and keep_sl.trigger is None:
        acts.append(Action('ALARM', 'LEG_UNPARSEABLE', leg=keep_sl))
    if not (keep_sl is not None and keep_sl.position_level):
        acts += [Action('CANCEL', 'SL_EXTRA_OR_INVALID', leg=l) for l in sl_c
                 if l is not keep_sl and not l.position_level and l not in moved_sl and l not in rebased_sl
                 and not (sl_by_owner and l in owner_sl)]

    def foreign(ls):
        # QC correctness-core-5 (A10): masters keep an exit-side reduce-only leg they cannot classify (a trailing
        # stop, say) and alarm; wrong-side reduce-only legs cannot protect the position and are still cancelled.
        return [Action('ALARM', 'LEG_UNCLASSIFIED_KEPT', 'warning', level=l.level, leg=l,
                       ctx={'oid': l.oid, 'live': l.level})
                if keep_moved and l.side == xs else Action('CANCEL', 'FOREIGN_REDUCE_ONLY', leg=l) for l in ls]

    if tp_lvl is None:                                                                            # rev 3 (R3-L1)
        acts.append(Action('ALARM', 'TP_LEVEL_UNKNOWN'))
        return acts + foreign(rest)

    def tp_qty_ok(l):
        return l.close_position or l.qty is None or abs(l.qty - pos.qty) <= lot / 2

    ok_tp = [l for l in tp_c if l.level is not None and abs(l.level - tp_lvl) <= tol and tp_qty_ok(l)]
    keep_tp = (min(ok_tp, key=lambda l: (not l.position_level, rec is None or l.oid != str(rec.tp_oid)))
               if ok_tp else None)
    moved_tp = [l for l in tp_c if keep_moved and l not in ok_tp
                and l.level is not None and abs(l.level - tp_lvl) > tol]                          # A10
    acts += [_moved('TP', l, tp_lvl) for l in moved_tp]
    owner_tp = [l for l in tp_c if keep_moved and l.level is not None]                            # A10 (QC)
    tp_by_owner = keep_tp is None and (any(tp_qty_ok(l) for l in moved_tp) or sum_covers(owner_tp))
    if not (keep_tp is not None and keep_tp.position_level):
        acts += [Action('CANCEL', 'TP_EXTRA_OR_INVALID', leg=l) for l in tp_c
                 if l is not keep_tp and not l.position_level and l not in moved_tp
                 and not (tp_by_owner and l in owner_tp)]
    if keep_tp is None and not tp_by_owner:
        acts.append(Action('PLACE_TP', 'TP_REPAIRED', level=tp_lvl, qty=pos.qty, ctx={'tp_kind': tp_kind}))
    return acts + foreign(rest)


class ChurnGuard:
    """Per symbol (masters) or per (user, symbol) (followers). Suppresses repair loops without ever removing SL
    protection: PLACE_SL is dropped only while the guardian's own last accepted SL is on the book, SL churn becomes
    CLOSE, TP state never gates SL actions, and the protected stop is never cancelled unless a PLACE_SL precedes it."""

    def __init__(self):
        self.placed = {'SL': [], 'TP': []}      # exchange-accepted placements [(ts, oid)]
        self.tp_fail, self.tp_next_try, self.tp_suspended_until = 0, 0.0, 0.0

    def reset(self):
        self.__init__()

    def _recent(self, leg, now):
        self.placed[leg] = [(t, o) for t, o in self.placed[leg] if now - t < CHURN_WINDOW_S]
        return self.placed[leg]

    def filter(self, acts, legs, now, qty=None, rec=None):
        if legs is UNKNOWN or any(a.kind == 'CLOSE' for a in acts):
            return acts, []
        on_book = {l.oid for l in legs}
        alarms = []
        sl = self._recent('SL', now)
        if any(a.kind == 'PLACE_SL' for a in acts):
            if sl and sl[-1][1] in on_book:
                alarms.append(('CLASSIFIER_SUSPECT', 'critical', {'leg': 'SL', 'oid': sl[-1][1]}))
                acts = [a for a in acts if a.kind != 'PLACE_SL'
                        and not (a.kind == 'CANCEL' and a.code == 'SL_EXTRA_OR_INVALID')]
            elif len(sl) >= SL_MAX_REPAIRS:
                alarms.append(('GUARDIAN_CHURN', 'critical', {'leg': 'SL', 'repairs_30m': len(sl)}))
                return [Action('CLOSE', 'SL_UNMAINTAINABLE', 'critical', qty=qty)], alarms
        protected = {o for o in ((sl[-1][1] if sl else None), trusted_sl_oid(rec)) if o}       # rev 3 (R3-L2)
        out, place_sl_seen = [], False
        for a in acts:
            place_sl_seen = place_sl_seen or a.kind == 'PLACE_SL'
            if (a.kind == 'CANCEL' and a.leg is not None and a.leg.oid in protected
                    and a.code != 'ORPHAN_LEG' and not place_sl_seen):
                alarms.append(('CLASSIFIER_SUSPECT', 'critical', {'leg': 'SL', 'oid': a.leg.oid, 'dropped': a.code}))
                continue
            out.append(a)
        acts = out
        if any(a.kind == 'PLACE_TP' for a in acts):
            tp = self._recent('TP', now)
            why = None
            if now < self.tp_suspended_until or now < self.tp_next_try:
                why = ('TP_REPAIR_DEFERRED', 'info', {'until': max(self.tp_suspended_until, self.tp_next_try)})
            elif tp and tp[-1][1] in on_book:
                why = ('CLASSIFIER_SUSPECT', 'critical', {'leg': 'TP', 'oid': tp[-1][1]})
                self.tp_suspended_until = now + TP_SUSPEND_S
            elif len(tp) >= TP_MAX_REPAIRS:
                why = ('GUARDIAN_CHURN', 'critical', {'leg': 'TP', 'repairs_30m': len(tp)})
                self.tp_suspended_until = now + TP_SUSPEND_S
            if why:
                alarms.append(why)
                acts = [a for a in acts if a.kind != 'PLACE_TP'
                        and not (a.kind == 'CANCEL' and a.code == 'TP_EXTRA_OR_INVALID')]
        return acts, alarms

    def note_placed(self, leg, oid, now):
        self.placed[leg].append((now, str(oid)))
        if leg == 'TP':
            self.tp_fail, self.tp_next_try = 0, 0.0

    def note_place_failed(self, leg, now):
        if leg == 'TP':
            self.tp_next_try = now + TP_BACKOFF_S[min(self.tp_fail, len(TP_BACKOFF_S) - 1)]
            self.tp_fail += 1
