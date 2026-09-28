"""
hl_rate - process-wide Hyperliquid request-weight meter (Wave 1: spec C45, I18, amendment A1)
===============================================================================================
One sliding 60 s window shared by every Hyperliquid caller in this process (Railway runs 1 replica):

- charge(weight, source): records weight. Never blocks, never raises. The trading path ('trading'),
  position_monitor ('monitor'), guardian mutations and the guardian's recovery lane (A1: recovery-lane
  I/O is safety-critical) only ever charge.
- acquire_background(weight, source='guardian', max_wait_s=240.0): the guardian's FULL-PASS reads wait
  here until total + weight <= HL_WEIGHT_CEILING and guardian_total + weight <= HL_GUARDIAN_WEIGHT_PER_MIN,
  then charge atomically. Returns False after max_wait_s (the caller raises GUARDIAN_BUDGET).
- window(now=None) -> (total, guardian_total, by_source) for the HL_WEIGHT log line.

Hyperliquid's real limit is 1200 weight/min per IP; the 700 ceiling keeps headroom for SL placements.
"""
import asyncio
import collections
import logging
import math
import os
import threading
import time


def _env_int(name, default, lo, hi):
    """QC regression-followers-2: an owner-tuned variable never stops the service importing. Blank, non-numeric or
    non-finite -> default; '700.0' -> 700; clamped to [lo, hi]. A replaced or clamped value is logged."""
    raw = os.getenv(name)
    try:
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError(raw)
        value = int(value)
    except (TypeError, ValueError):
        value = default
    used = min(max(value, lo), hi)
    if raw is not None and str(raw).strip() != str(used):
        logging.getLogger('HL_RATE').warning('%s=%r is not a plain integer in [%d, %d]; using %d',
                                             name, raw, lo, hi, used)
    return used


WINDOW_S = 60.0
CEILING = _env_int('HL_WEIGHT_CEILING', 700, 1, 1200)                 # Hyperliquid's own limit is 1200/min per IP
GUARDIAN_PER_MIN = _env_int('HL_GUARDIAN_WEIGHT_PER_MIN', 300, 1, 1200)
GUARDIAN_SOURCE = 'guardian'

# Hyperliquid request weights (spec C45 WEIGHTS)
W_USER_STATE = 2
W_ALL_MIDS = 2
W_OPEN_ORDERS = 20
W_FRONTEND_OPEN_ORDERS = 20
W_USER_FILLS = 20            # user_fills / user_fills_by_time; + extra_fill_weight(rows) charged after the call
W_META = 20
W_SPOT_META = 20
W_META_AND_ASSET_CTXS = 20
W_CANDLES = 20
W_CTOR_UNCACHED = 40         # Info(...) or Exchange(...) built without cached meta + spot_meta (2 requests)
W_ACTION = 1                 # exchange.order / cancel / update_leverage

_lock = threading.Lock()
_events = collections.deque()      # (monotonic ts, weight, source)
_clock = time.monotonic            # tests patch hl_rate._clock (never patch time.monotonic: asyncio uses it)


def _trim(now):
    while _events and now - _events[0][0] >= WINDOW_S:
        _events.popleft()


def charge(weight, source):
    """Record `weight` for `source` now. Charge-only callers never wait."""
    now = _clock()
    with _lock:
        _trim(now)
        _events.append((now, int(weight), str(source)))


def extra_fill_weight(rows):
    """Hyperliquid charges user_fills / user_fills_by_time 1 extra weight per 20 rows returned."""
    try:
        return len(rows or ()) // 20
    except TypeError:
        return 0


def _sums():
    total = guardian = 0
    by_source = {}
    for _, w, s in _events:
        total += w
        by_source[s] = by_source.get(s, 0) + w
        if s == GUARDIAN_SOURCE:
            guardian += w
    return total, guardian, by_source


def window(now=None):
    """(total, guardian_total, by_source) over the trailing WINDOW_S seconds."""
    now = _clock() if now is None else now
    with _lock:
        _trim(now)
        return _sums()


def _try_acquire(weight, source):
    now = _clock()
    with _lock:
        _trim(now)
        total, guardian, _ = _sums()
        if total + weight <= CEILING and guardian + weight <= GUARDIAN_PER_MIN:
            _events.append((now, int(weight), str(source)))
            return True
    return False


async def acquire_background(weight, source=GUARDIAN_SOURCE, max_wait_s=240.0):
    """Wait (1 s steps) until the read fits under both limits, then charge it. False after max_wait_s."""
    t0 = _clock()
    while True:
        if _try_acquire(weight, source):
            return True
        if _clock() - t0 > max_wait_s:
            return False
        await asyncio.sleep(1.0)
