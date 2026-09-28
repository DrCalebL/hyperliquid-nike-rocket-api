"""
Nike Rocket - HL Position Guardian (Wave 1 / Phase 0: spec C44, I12-I19, amendments A1, A2, A7)
==================================================================================================
The Hyperliquid follower had no guardian. This one runs two independent asyncio tasks:

RECOVERY LANE (every LANE_S=30 s, in EVERY GUARDIAN_MODE incl. 'off'; A1: its own task, charge-only I/O)
  Owns open_positions rows with status 'needs_recovery' (written by execute_trade after any post-entry failure,
  I19). With no such row it makes zero Hyperliquid calls.
  - enforce + valid row levels: the bracket invariant (bracket_guard.decide) re-places the SL, then the TP, and
    sets the row back to 'open' (guarded UPDATE, live qty + entryPx) once a decide() made while the user is not
    in flight wants nothing more, or closes when the mark is beyond the SL;
  - shadow/off, or invalid levels: today's Kraken legacy rule (market close -> 'closed'), except A7: a valid SL
    AND TP resting at the row's recorded levels restore the row to 'open' instead of closing;
  - flat: 'closed_manual' only after 240 s of confirmed flat (5 s CONFIRM re-reads), after cancelling the row's
    own leg ids.
  QC Wave 1 (A1 bound): a tick is two-phase - every lane user is read and every row with a live position is acted
  on first; the flat rows' CONFIRM re-reads then share ONE FLAT_CONFIRM_S sleep (never 5 s per row). Each tick is
  cut off after LANE_STALL_S (critical GUARDIAN_LANE_STALL). QC round 2: phase 1 visits never-visited users
  first (newest row first), then the least recently started, and starts no new user after LANE_PHASE1_S
  (warning GUARDIAN_LANE_SLOW), so a slow or hung user never starves the rest and phase 2 still runs for the users
  it read; the guardian's Info / Exchange carry
  timeout=HL_TIMEOUT_S (QC round 2: a PLACE_SL that raised - e.g. that timeout - is an unknown outcome, settled by
  an orders re-read before any retry or close; SL_PLACE_UNKNOWN and no close while it stays unknown); a failed
  meta_and_asset_ctxs falls back to the cached marks, and BLIND_AFTER consecutive
  failures of it raise GUARDIAN_BLIND (lane and pass alike).

FULL PASS (every PASS_S=240 s; shadow = log-only 'GUARDIAN_WOULD_*', enforce = acts; 'off' = none)
  Never within 180 s of a new signal (GUARDIAN_PASS_DEFERRED_SIGNAL). Users with an 'open' row first, then other
  active users, then users with a row closed in the last 60 min not yet verified clean. follower_guard's row
  policy: one consistent active row per (user, coin), else ROW_AMBIGUOUS (alarm only); only service-placed oids
  are cancelled; an 'open' row on a flat coin is left to position_monitor; rowless positions only alarm.

Reads: frontend_open_orders FIRST, user_state SECOND (I1), with the wallet address and the guardian's own lazily
built Info, on the guardian's own 2-thread executor. Full-pass reads wait on hl_rate.acquire_background; lane
reads and every mutation only charge (A1, I18). An Exchange is built only for a mutation, with cached meta and
spot_meta so the SDK constructor makes no request (R3-M3). A reappearing position after confirmed flats is
judged by user_fills_by_time (R3-M6; A2: fills since max(first flat - 60 s, row.opened_at), never the row's own
entry order) - the lane and the pass apply the same rule. Every decide() call passes
level_policy=follower_guard.DECIDE_LEVEL_POLICY (A10: followers replace service legs away from the row levels);
every lane tick reports follower_guard.drain_expired() as INFLIGHT_TOKEN_EXPIRED (A6).
Alarms: error_logs rows 'GUARDIAN_<CODE>' (codes already named GUARDIAN_* are not prefixed twice); critical
codes also notify_admin, deduped per (user, coin, code) per hour.
"""
import asyncio
import functools
import inspect
import logging
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor

from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from cryptography.fernet import Fernet

import bracket_guard as bg
import follower_guard
import hl_rate
from follower_guard import (ACTIVE_STATUSES, filter_actions, real_oid, rec_from_row, row_side, row_ts,
                            select_row, service_oids)
from hosted_trading_loop import (USE_TESTNET, coin_entry_px, coin_szi, convert_symbol_to_hl,
                                 exchange_ctor_takes_meta, floor_to_lot, hl_close_with_verification,
                                 log_error_to_db)
from order_utils import (cancel_order_verified, definitive_rejection, first_status, notify_admin, sl_order_type,
                         tp_order_type)

logger = logging.getLogger('POSITION_GUARDIAN')

MODES = ('off', 'shadow', 'enforce')
START_DELAY_S = 45.0
LANE_S = 30.0
PASS_S = 240.0
WEIGHT_LOG_S = 300.0
ALARM_DEDUP_S = 3600.0
RECENT_CLOSED_S = 3600.0
CANNOT_TRADE_S = 3600.0
POOL_WORKERS = 2
BLIND_AFTER = 3
HL_TIMEOUT_S = 10.0              # QC: requests timeout for the guardian's Info / Exchange (the SDK default is None)
LANE_STALL_S = 150.0             # QC: a lane tick running longer is cut off with a critical GUARDIAN_LANE_STALL
LANE_PHASE1_S = 90.0             # QC round 2: phase 1 starts no new user after this (warning GUARDIAN_LANE_SLOW)
MARKS_FRESH_S = 120.0            # QC: cached marks older than this never serve as the CLOSE-by-price reference
SL_YIELDED = 'yielded'           # CA-B8-1: _place_sl's third outcome (the user went in flight between attempts)
SL_UNKNOWN = 'unknown'           # QC round 2: a timed-out SL whose re-read failed too (may rest: never close)
PLACE_UNKNOWN = 'place-unknown'  # QC round 2: _place_leg's outcome when ex.order raised (it may have been accepted)
UNKNOWN_RECHECK_S = 2.0          # QC round 2: pause before the orders re-read that settles a timed-out SL
GUARDIAN = hl_rate.GUARDIAN_SOURCE
EVENT_CODES = frozenset({'SL_REPAIRED', 'TP_REPAIRED', 'GUARDIAN_CLOSE', 'GUARDIAN_RECOVERY_CLOSE',
                         'GUARDIAN_RECOVERY_RESTORED', 'ROW_CLOSED_MANUAL'})
NOTIFY_CODES = frozenset({'GUARDIAN_RECOVERY_CLOSE', 'GUARDIAN_RECOVERY_RESTORED'})     # warning + notify

_ROW_COLS = ("op.id, op.user_id, op.signal_id, op.entry_order_id, op.tp_order_id, op.sl_order_id, op.symbol, "
             "op.hl_coin, op.side, op.quantity, op.leverage, op.entry_fill_price, op.target_tp, op.target_sl, "
             "op.opened_at, op.last_fill_at, op.status")
_USER_COLS = ("u.api_key, u.email, u.hl_private_key_encrypted, u.hl_wallet_address, u.agent_active, "
              "u.api_wallet_expires_at")
SQL_LANE_ROWS = ("/* hlg:lane_rows */ SELECT " + _ROW_COLS + ", " + _USER_COLS + " FROM open_positions op "
                 "JOIN follower_users u ON u.id = op.user_id WHERE op.status = 'needs_recovery' ORDER BY op.opened_at")
SQL_USER_ROWS = ("/* hlg:user_rows */ SELECT " + _ROW_COLS + " FROM open_positions op WHERE op.user_id = $1 AND "
                 "(op.status IN ('open', 'needs_recovery') OR op.opened_at > NOW() - INTERVAL '7 days') "
                 "ORDER BY op.opened_at DESC")
SQL_PASS_USERS = ("/* hlg:pass_users */ SELECT u.id AS user_id, " + _USER_COLS + " FROM follower_users u "
                  "WHERE u.hl_private_key_encrypted IS NOT NULL ORDER BY u.id")
SQL_PASS_ROWS = ("/* hlg:pass_rows */ SELECT " + _ROW_COLS + " FROM open_positions op WHERE "
                 "op.status IN ('open', 'needs_recovery') OR op.opened_at > NOW() - INTERVAL '7 days' "
                 "ORDER BY op.opened_at DESC")
SQL_BUSY = ("/* hlg:busy */ SELECT EXISTS (SELECT 1 FROM signal_deliveries WHERE user_id = $1 "
            "AND acknowledged_at > NOW() - INTERVAL '180 seconds' AND ($2::timestamp IS NULL OR acknowledged_at > $2))")
SQL_SIGNAL_WINDOW = ("/* hlg:signal_window */ SELECT EXISTS (SELECT 1 FROM signals "
                     "WHERE created_at > NOW() - INTERVAL '180 seconds')")
SQL_ROW_OPEN = ("/* hlg:row_open */ UPDATE open_positions SET status = 'open', quantity = $2, "
                "entry_fill_price = COALESCE($3, entry_fill_price) WHERE id = $1 AND status = 'needs_recovery'")
SQL_ROW_STATUS = ("/* hlg:row_status */ UPDATE open_positions SET status = $2, last_fill_at = NOW() "
                  "WHERE id = $1 AND status = 'needs_recovery'")
SQL_ROW_SL = ("/* hlg:row_sl */ UPDATE open_positions SET sl_order_id = $2 "
              "WHERE id = $1 AND status IN ('open', 'needs_recovery')")
SQL_ROW_TP = ("/* hlg:row_tp */ UPDATE open_positions SET tp_order_id = $2 "
              "WHERE id = $1 AND status IN ('open', 'needs_recovery')")


def _takes(cls, name):
    """True when cls(...) accepts the keyword `name` (the installed SDK's Info / Exchange take timeout=)."""
    try:
        return name in inspect.signature(cls.__init__).parameters
    except (TypeError, ValueError):
        return False


class GuardianBudget(Exception):
    """The meter held a full-pass read past the pass budget (GUARDIAN_BUDGET)."""


class HLPositionGuardian:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.base_url = constants.TESTNET_API_URL if USE_TESTNET else constants.MAINNET_API_URL
        self._pool = ThreadPoolExecutor(max_workers=POOL_WORKERS, thread_name_prefix='hl-guardian')
        self.info = None                 # built lazily on the first tick with work (40 weight)
        self._meta = None                # universe meta from meta_and_asset_ctxs (cached for Exchange(meta=))
        self._spot_meta = None           # fetched once per process, on the first mutation
        self._marks, self._szd = {}, {}  # coin -> markPx / szDecimals, refreshed per lane tick or pass
        self._ex = {}                    # user_id -> Exchange (mutations only)
        self._churn = {}                 # (user_id, coin, row_id) -> bracket_guard.ChurnGuard
        self._retired = {}               # user_id -> oids this process replaced (still service-owned)
        self._flat = follower_guard.FlatTracker()
        self._alarmed = {}               # (user_id, coin, code) -> epoch of the last recorded alarm
        self._read_failures = {}         # user_id -> consecutive failed reads
        self._cannot_trade_until = {}    # user_id -> epoch
        self._clean_rows = set()         # closed row ids whose legs were verified gone
        self._next_start = 0             # full-pass round-robin resume index
        self._lane_started = {}          # user_id -> clock when a lane tick last started the user (QC round 2)
        self._marks_at = None            # epoch of the last successful meta_and_asset_ctxs
        self._marks_failures = {}        # 'lane' | 'pass' -> consecutive failed meta_and_asset_ctxs (voice-5)
        self._lane_defer = None          # during a lane tick's first phase: user_id -> coins awaiting CONFIRM_FLAT
        self._lane_books = {}            # during its second phase: user_id -> the book re-read after the shared sleep
        self._pass_stats = None          # counters of the running full pass (the GUARDIAN_PASS heartbeat, voice-15)
        self._last_weight_log = None

    # ------------------------------------------------------------------ clocks (tests override)
    def _clock(self):
        return time.monotonic()

    def _epoch(self):
        return time.time()

    async def _sleep(self, seconds):
        await asyncio.sleep(seconds)

    # ------------------------------------------------------------------ loops
    async def run(self):
        await self._sleep(START_DELAY_S)
        logger.info('🛡️ HL POSITION GUARDIAN STARTED mode=%s lane=%ds pass=%ds',
                    follower_guard.guardian_mode(MODES), LANE_S, PASS_S)
        await asyncio.gather(self._lane_loop(), self._pass_loop())

    async def _lane_loop(self):
        """A1: the recovery lane is its own task; it ticks every LANE_S whatever the full pass or the meter do."""
        while True:
            t0 = self._clock()
            try:
                await asyncio.wait_for(self._lane_tick(follower_guard.guardian_mode(MODES)), timeout=LANE_STALL_S)
            except asyncio.TimeoutError:
                await self._alarm(None, '*', 'GUARDIAN_LANE_STALL', 'critical', stall_s=LANE_STALL_S)
            except Exception as e:
                await self._alarm(None, '*', 'GUARDIAN_EXCEPTION', 'critical', where='lane', error=repr(e)[:200])
            self._log_weight_every(WEIGHT_LOG_S)
            await self._sleep(max(1.0, LANE_S - (self._clock() - t0)))

    async def _lane_tick(self, mode):
        await self._recovery_lane(mode)
        await self._report_expired_tokens()

    async def _report_expired_tokens(self):
        """A6: follower_guard records a token that aged out while its execute_trade was still running; every lane
        tick reports each one as a critical INFLIGHT_TOKEN_EXPIRED (error_logs ALARM_INFLIGHT_TOKEN_EXPIRED)."""
        for event in follower_guard.drain_expired():
            logger.critical('INFLIGHT_TOKEN_EXPIRED %s', event)
            await log_error_to_db(self.db_pool, 'system', 'ALARM_INFLIGHT_TOKEN_EXPIRED',
                                  'execute_trade outlived its in-flight token', {k: str(v) for k, v in event.items()})
            await notify_admin(title='INFLIGHT_TOKEN_EXPIRED (HL)',
                               details={'User ID': event.get('user_id'), 'Token': event.get('token'),
                                        'Age s': event.get('age_s')}, level='error')

    def _log_weight_every(self, period):
        now = self._clock()
        if self._last_weight_log is None or now - self._last_weight_log >= period:
            self._last_weight_log = now
            total, guardian, by_source = hl_rate.window()
            logger.info('HL_WEIGHT total=%d guardian=%d ceiling=%d guardian_cap=%d by_source=%s',
                        total, guardian, hl_rate.CEILING, hl_rate.GUARDIAN_PER_MIN, by_source)

    # ------------------------------------------------------------------ database
    async def _fetch(self, sql, *args):
        async with self.db_pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch(sql, *args)]

    async def _fetchval(self, sql, *args):
        async with self.db_pool.acquire() as conn:
            return await conn.fetchval(sql, *args)

    async def _write(self, sql, *args):
        async with self.db_pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def _busy(self, user_id, since=None):
        """I12: an in-process token, or a delivery acknowledged within 180 s (DB clock). For needs_recovery rows
        `since` is the rows' latest opened_at: only deliveries acknowledged after the row count."""
        if follower_guard.is_inflight(user_id):
            return True
        return bool(await self._fetchval(SQL_BUSY, user_id, since))

    async def _yield(self, user, coin='*'):
        await self._alarm(user, coin, 'GUARDIAN_YIELD_INFLIGHT', 'info')

    async def _signal_window_open(self):
        return bool(await self._fetchval(SQL_SIGNAL_WINDOW))

    # ------------------------------------------------------------------ Hyperliquid I/O
    async def _call(self, fn, *args, **kwargs):
        return await asyncio.get_running_loop().run_in_executor(self._pool, functools.partial(fn, *args, **kwargs))

    async def _get(self, weight, fn, *args, lane=False, deadline=None):
        """One read. Lane reads only charge (A1); full-pass reads wait on the meter until the pass deadline."""
        if lane:
            hl_rate.charge(weight, GUARDIAN)
        else:
            wait = 240.0 if deadline is None else max(0.0, deadline - self._clock())
            if not await hl_rate.acquire_background(weight, GUARDIAN, max_wait_s=wait):
                raise GuardianBudget()
        return await self._call(fn, *args)

    async def _ensure_info(self, lane, deadline=None):
        """The guardian's own Info, built once (40 weight: the SDK constructor fetches spot_meta + meta)."""
        if self.info is None:
            if lane:
                hl_rate.charge(hl_rate.W_CTOR_UNCACHED, GUARDIAN)
            else:
                wait = 240.0 if deadline is None else max(0.0, deadline - self._clock())
                if not await hl_rate.acquire_background(hl_rate.W_CTOR_UNCACHED, GUARDIAN, max_wait_s=wait):
                    raise GuardianBudget()
            if self.info is None:
                kw = {'timeout': HL_TIMEOUT_S} if _takes(Info, 'timeout') else {}
                self.info = await self._call(Info, self.base_url, True, **kw)
        return self.info

    async def _refresh_marks(self, lane, deadline=None):
        """meta_and_asset_ctxs once per lane tick with work or per pass: markPx, szDecimals, cached meta."""
        info = await self._ensure_info(lane, deadline)
        meta, ctxs = await self._get(hl_rate.W_META_AND_ASSET_CTXS, info.meta_and_asset_ctxs, lane=lane,
                                     deadline=deadline)
        marks, szd = {}, {}
        for asset, ctx in zip(meta.get('universe') or [], ctxs or []):
            name = asset.get('name')
            szd[name] = int(asset.get('szDecimals', 2))
            try:
                px = float(ctx.get('markPx'))
            except (TypeError, ValueError):
                continue
            if px > 0:
                marks[name] = px
        self._meta, self._marks, self._szd = meta, marks, szd
        self._marks_at = self._epoch()

    def _marks_fresh(self):
        return self._marks_at is not None and self._epoch() - self._marks_at <= MARKS_FRESH_S

    async def _marks_or_cached(self, where, lane, deadline=None, pending=None):
        """meta_and_asset_ctxs for a lane tick / pass. A failure counts toward GUARDIAN_BLIND (QC voice-5: the
        per-user streak never sees it) and falls back to the cached marks when there are any (QC money-followers-4).
        True when the caller may go on reading."""
        try:
            await self._refresh_marks(lane=lane, deadline=deadline)
        except GuardianBudget:
            raise
        except Exception as e:
            n = self._marks_failures[where] = self._marks_failures.get(where, 0) + 1
            await self._alarm(None, '*', 'GUARDIAN_READ_FAILED', 'warning', where=where + '_marks',
                              error=repr(e)[:200])
            if n >= BLIND_AFTER:
                ctx = {'needs_recovery_rows': pending} if pending is not None else {}
                await self._alarm(None, '*', 'GUARDIAN_BLIND', 'critical', where=where + '_marks', failures=n, **ctx)
            return self.info is not None and self._meta is not None
        self._marks_failures[where] = 0
        return True

    async def _read_book(self, user, wallet, lane, deadline=None):
        """I1: orders FIRST, position SECOND. (raw_orders, state), or None after GUARDIAN_READ_FAILED (a read
        raised) or GUARDIAN_READ_UNKNOWN (an unrecognised shape, CA-B8-4); both count toward GUARDIAN_BLIND."""
        uid = user['user_id']
        try:
            raw = await self._get(hl_rate.W_FRONTEND_OPEN_ORDERS, self.info.frontend_open_orders, wallet,
                                  lane=lane, deadline=deadline)
            state = await self._get(hl_rate.W_USER_STATE, self.info.user_state, wallet, lane=lane, deadline=deadline)
        except GuardianBudget:
            raise
        except Exception as e:
            if not lane:
                self._count('read_failures')
            return await self._read_failed(user, 'GUARDIAN_READ_FAILED', error=repr(e)[:200])
        if not self._book_recognised(raw, state):
            if not lane:
                self._count('read_failures')
            return await self._read_failed(user, 'GUARDIAN_READ_UNKNOWN', where='book')
        self._read_failures[uid] = 0
        if not lane:
            self._count('books')
        return raw, state

    async def _read_failed(self, user, code, **ctx):
        """I1 (+ CA-B8-4): a read that raised (GUARDIAN_READ_FAILED) or answered with an unrecognised shape
        (GUARDIAN_READ_UNKNOWN) means no action for the user this round, and counts: BLIND_AFTER of them in a row
        (no recognised read in between) -> GUARDIAN_BLIND (critical). Returns None."""
        uid = user['user_id']
        failures = self._read_failures[uid] = self._read_failures.get(uid, 0) + 1
        await self._alarm(user, '*', code, 'warning', **ctx)
        if failures >= BLIND_AFTER:
            await self._alarm(user, '*', 'GUARDIAN_BLIND', 'critical', failures=failures)
        return None

    @classmethod
    def _book_recognised(cls, raw, state):
        """CA-B8-4: frontend_open_orders is a list of order dicts and user_state parses (assetPositions, numeric
        szi); anything else is UNKNOWN (I1: never act on it)."""
        if not isinstance(raw, list) or not all(isinstance(o, dict) for o in raw):
            return False
        try:
            cls._live_coins(state)
        except (AttributeError, TypeError, ValueError):
            return False
        return True

    # ------------------------------------------------------------------ parsing / rounding
    @staticmethod
    def _coin(row):
        return row.get('hl_coin') or convert_symbol_to_hl(row.get('symbol') or '')

    @staticmethod
    def _pos(state, coin):
        try:
            szi = coin_szi(state, coin)
        except (TypeError, ValueError):
            return bg.UNKNOWN
        if szi == 0:
            return None
        return bg.Pos(1 if szi > 0 else -1, abs(szi), coin_entry_px(state, coin), None)   # HL: no position time

    @staticmethod
    def _legs(raw, coin):
        if not isinstance(raw, list):
            return bg.UNKNOWN
        return [bg.leg_from_hl(o) for o in raw if isinstance(o, dict) and o.get('coin') == coin]

    @staticmethod
    def _live_coins(state):
        if not isinstance(state, dict) or not isinstance(state.get('assetPositions'), list):
            raise ValueError('unrecognised user_state shape')
        return {(p.get('position') or {}).get('coin') for p in state['assetPositions']
                if float((p.get('position') or {}).get('szi') or 0) != 0}

    def _round_size(self, coin, size):
        """Identical to HostedTradingLoop.round_size (szDecimals from meta, default 2)."""
        return floor_to_lot(size, self._szd.get(coin, 2))

    @staticmethod
    def _round_price(price, sig_figs=5):
        """Identical to HostedTradingLoop.round_price (5 significant figures)."""
        if price == 0:
            return 0
        magnitude = math.floor(math.log10(abs(price)))
        factor = 10 ** (sig_figs - 1 - magnitude)
        return round(price * factor) / factor

    def _tick(self, coin):
        return bg.hl_tick(self._marks.get(coin), self._szd.get(coin, 2))

    def _lot(self, coin):
        return 10 ** -self._szd.get(coin, 2)

    # ------------------------------------------------------------------ credentials
    @staticmethod
    def _decrypt(encrypted):
        key = os.getenv('CREDENTIALS_ENCRYPTION_KEY')
        if not key or not encrypted:
            return None
        try:
            return Fernet(key.encode()).decrypt(encrypted.encode()).decode()
        except Exception:
            return None

    def _wallet(self, user):
        """Reads need only the wallet address: hl_wallet_address, else the key's own address (local, no request)."""
        if user.get('hl_wallet_address'):
            return user['hl_wallet_address']
        pk = self._decrypt(user.get('hl_private_key_encrypted'))
        return Account.from_key(pk).address if pk else None

    async def _can_trade(self, user, coin):
        uid = user['user_id']
        now = self._epoch()
        if self._cannot_trade_until.get(uid, 0) > now:
            return False
        expires = row_ts(user.get('api_wallet_expires_at'))
        if expires is not None and expires < now:
            self._cannot_trade_until[uid] = now + CANNOT_TRADE_S
            await self._alarm(user, coin, 'GUARDIAN_CANNOT_TRADE', 'critical', error='API wallet expired')
            return False
        return True

    async def _note_mutation_error(self, user, coin, result):
        """An expired / deregistered API wallet ('... does not exist'): stop mutating for an hour (no retry storm)."""
        text = str(result).lower()
        if 'does not exist' in text or 'expired' in text:
            self._cannot_trade_until[user['user_id']] = self._epoch() + CANNOT_TRADE_S
            self._ex.pop(user['user_id'], None)          # rebuilt from the current (maybe renewed) key next time
            await self._alarm(user, coin, 'GUARDIAN_CANNOT_TRADE', 'critical', error=str(result)[:200])

    async def _exchange(self, user, coin, lane, deadline=None):
        """Mutations only: an Exchange with cached meta= / spot_meta= (no constructor request), cached per user."""
        uid = user['user_id']
        if uid in self._ex:
            return self._ex[uid]
        pk = self._decrypt(user.get('hl_private_key_encrypted'))
        if not pk:
            await self._alarm(user, coin, 'GUARDIAN_CANNOT_TRADE', 'critical', error='credentials unavailable')
            return None
        acct = Account.from_key(pk)
        wallet = user.get('hl_wallet_address') or acct.address
        kwargs = {}
        if wallet.lower() != acct.address.lower():
            kwargs['account_address'] = wallet
        if self._meta is not None and exchange_ctor_takes_meta(Exchange):
            if self._spot_meta is None:
                self._spot_meta = await self._get(hl_rate.W_SPOT_META, self.info.spot_meta, lane=lane,
                                                  deadline=deadline)
            kwargs.update(meta=self._meta, spot_meta=self._spot_meta)
        else:
            hl_rate.charge(hl_rate.W_CTOR_UNCACHED, GUARDIAN)
        if _takes(Exchange, 'timeout'):
            kwargs['timeout'] = HL_TIMEOUT_S
        ex = await self._call(Exchange, acct, self.base_url, **kwargs)
        self._ex[uid] = ex
        return ex

    # ------------------------------------------------------------------ alarms
    async def _alarm(self, user, coin, code, severity='warning', /, **ctx):
        """Positional-only head: ctx may carry keys such as 'code' (filter_actions / ChurnGuard contexts)."""
        uid = (user or {}).get('user_id')
        now = self._epoch()
        key = (uid, coin, code)
        fresh = now - self._alarmed.get(key, float('-inf')) >= ALARM_DEDUP_S
        level = {'critical': logging.CRITICAL, 'warning': logging.WARNING}.get(severity, logging.INFO)
        error_type = code if code.startswith('GUARDIAN_') else 'GUARDIAN_' + code
        logger.log(level, '%s sev=%s user=%s coin=%s ctx=%s', error_type, severity, uid, coin, ctx)   # CA-B8-6
        if not (fresh or code in EVENT_CODES):
            return
        if fresh:
            self._alarmed[key] = now
        details = {k: str(v)[:200] for k, v in ctx.items()}
        await log_error_to_db(self.db_pool, (user or {}).get('api_key') or 'system', error_type,
                              '%s (%s) coin=%s' % (code, severity, coin), dict(details, coin=coin, severity=severity))
        if (severity == 'critical' and fresh) or code in NOTIFY_CODES:
            await notify_admin(title='%s (HL)' % error_type,
                               details=dict({'User': (user or {}).get('email', 'system'), 'Coin': coin,
                                             'Severity': severity}, **details),
                               level='error' if severity == 'critical' else 'warning')

    # ------------------------------------------------------------------ row policy
    async def _select(self, user, wallet, coin, active, pos, lane, deadline=None):
        """follower_guard.select_row plus the R3-M6 evidence check. (row, code): 'ROW_AMBIGUOUS' is alarmed here;
        'SKIP' means a fills read failed (no action this tick)."""
        lot = self._lot(coin)
        row, code = select_row(active, pos, lot, self._flat.evidence)
        if row is not None and isinstance(pos, bg.Pos) and self._flat.pending_check(row['id']):
            new = await self._fills_show_new_entry(user, wallet, coin, row, lane, deadline)
            if new is None:
                return None, 'SKIP'
            self._flat.resolve(row['id'], new)
            if not new:
                await self._alarm(user, coin, 'FALSE_FLAT_CLEARED', 'warning', row=row['id'])
            row, code = select_row(active, pos, lot, self._flat.evidence)
        if code:
            await self._alarm(user, coin, code, 'critical', rows=[r['id'] for r in active])
        return row, code

    async def _fills_show_new_entry(self, user, wallet, coin, row, lane, deadline=None):
        """R3-M6 + A2 (HL: fills decide, entryPx is no fingerprint): True when an entry-side opening fill exists
        since FlatTracker.since_ms(row) = max(first confirmed flat - 60 s, row.opened_at); follower_guard ignores the
        row's own entry order. None on a read error (no action this tick)."""
        since = self._flat.since_ms(row)
        try:
            rows = await self._get(hl_rate.W_USER_FILLS, self.info.user_fills_by_time, wallet, since, lane=lane,
                                   deadline=deadline)
        except GuardianBudget:
            raise
        except Exception as e:
            await self._alarm(user, coin, 'GUARDIAN_READ_FAILED', 'warning', where='fills', error=repr(e)[:200])
            return None
        hl_rate.charge(hl_rate.extra_fill_weight(rows), GUARDIAN)
        fills = []
        for f in rows or ():
            if not isinstance(f, dict) or f.get('coin') != coin:
                continue
            direction = str(f.get('dir') or '')
            fills.append({'side': {'B': 'buy', 'A': 'sell'}.get(f.get('side')),
                          'ts_ms': int(f['time']) if f.get('time') is not None else None,
                          'opening': True if (direction.startswith('Open') or '>' in direction)
                          else (False if direction.startswith('Close') else None),
                          'order_id': str(f['oid']) if f.get('oid') is not None else None})
        return follower_guard.fills_show_new_entry(fills, row_side(row), since, row.get('entry_order_id'))

    async def _confirm_flat(self, user, wallet, coin, lane, deadline=None, since=None):
        """CONFIRM_FLAT: 5 s later re-read orders FIRST and position SECOND. (confirmed, legs, pos).
        Lane ticks (QC A1 bound): in the first phase the coin is only queued for the tick's one shared
        FLAT_CONFIRM_S sleep; in the second phase the book re-read after that sleep is the confirmation."""
        uid = user['user_id']
        if lane and self._lane_defer is not None:
            self._lane_defer.setdefault(uid, set()).add(coin)
            return False, None, None
        if lane and uid in self._lane_books:
            book = self._lane_books[uid]
        else:
            await self._sleep(bg.FLAT_CONFIRM_S)
            if await self._busy(uid, since=since):
                await self._yield(user, coin)
                return False, None, None
            book = await self._read_book(user, wallet, lane, deadline)
            if book is None:
                return False, None, None
        pos, legs = self._pos(book[1], coin), self._legs(book[0], coin)
        return (pos is None and legs is not bg.UNKNOWN), legs, pos

    # ------------------------------------------------------------------ recovery lane
    async def _recovery_lane(self, mode):
        rows = await self._fetch(SQL_LANE_ROWS)
        if not rows:
            return                                   # AC12: no needs_recovery row -> zero Hyperliquid calls
        by_user = {}
        for r in rows:
            by_user.setdefault(r['user_id'], []).append(r)
        if not await self._marks_or_cached('lane', lane=True, pending=len(rows)):
            return
        # QC A1 bound: phase 1 reads every user and acts on every live position; flat rows only queue their
        # CONFIRM_FLAT. Phase 2: ONE shared FLAT_CONFIRM_S sleep, then one re-read per queued user.
        # QC round 2: never-visited users first (newest row first), then the least recently started, so a tick
        # cut off by LANE_STALL_S (or stopped at LANE_PHASE1_S) never starves the same users tick after tick.
        self._lane_started = {u: t for u, t in self._lane_started.items() if u in by_user}
        order = sorted(by_user, key=lambda u: (self._lane_started.get(u, float('-inf')),
                                               -max(row_ts(r['opened_at']) or 0.0 for r in by_user[u])))
        self._lane_defer, self._lane_books = {}, {}
        t0 = self._clock()
        try:
            for i, uid in enumerate(order):
                if self._clock() - t0 >= LANE_PHASE1_S:
                    await self._alarm(None, '*', 'GUARDIAN_LANE_SLOW', 'warning', phase1_s=LANE_PHASE1_S,
                                      users_left=len(order) - i)
                    break
                self._lane_started[uid] = self._clock()
                await self._lane_guarded(self._lane_user(mode, by_user[uid]), by_user[uid][0])
            deferred, self._lane_defer = self._lane_defer, None
            if deferred:
                await self._sleep(bg.FLAT_CONFIRM_S)
                for uid, coins in deferred.items():
                    await self._lane_guarded(self._lane_confirm_user(mode, by_user[uid], coins), by_user[uid][0])
        finally:
            self._lane_defer, self._lane_books = None, {}

    async def _lane_guarded(self, coro, user):
        try:
            await coro
        except Exception as e:
            await self._alarm(user, '*', 'GUARDIAN_EXCEPTION', 'critical', where='lane_user', error=repr(e)[:200])

    async def _lane_confirm_user(self, mode, nr_rows, coins):
        """Phase 2 of a lane tick: the CONFIRM re-read (orders FIRST, position SECOND) of the coins queued in phase 1,
        made after the tick's shared FLAT_CONFIRM_S sleep; the pair logic then runs on it."""
        user = nr_rows[0]
        uid = user['user_id']
        since = max(r['opened_at'] for r in nr_rows)
        if await self._busy(uid, since=since):
            return await self._yield(user)
        wallet = self._wallet(user)
        if wallet is None:
            return
        book = await self._read_book(user, wallet, lane=True)
        if book is None:
            return
        self._lane_books[uid] = book
        await self._lane_user(mode, nr_rows, book=book, coins=coins)

    async def _lane_user(self, mode, nr_rows, book=None, coins=None):
        user = nr_rows[0]                            # lane rows carry the user's columns
        uid = user['user_id']
        since = max(r['opened_at'] for r in nr_rows)
        if book is None:
            if await self._busy(uid, since=since):
                return await self._yield(user)
            wallet = self._wallet(user)
            if wallet is None:
                return await self._alarm(user, '*', 'GUARDIAN_CANNOT_TRADE', 'critical', error='no wallet address')
            book = await self._read_book(user, wallet, lane=True)
            if book is None:
                return
        else:
            wallet = self._wallet(user)
        if await self._busy(uid, since=since):
            return await self._yield(user)
        rows = await self._fetch(SQL_USER_ROWS, uid)
        for coin in sorted(coins if coins is not None else {self._coin(r) for r in nr_rows}):
            pair = [r for r in rows if self._coin(r) == coin]
            await self._lane_pair(mode, user, wallet, coin, pair, book[0], book[1], since)

    async def _lane_pair(self, mode, user, wallet, coin, pair, raw, state, since):
        active = [r for r in pair if r['status'] in ACTIVE_STATUSES]
        pos, legs = self._pos(state, coin), self._legs(raw, coin)
        if pos is bg.UNKNOWN or legs is bg.UNKNOWN:
            return await self._alarm(user, coin, 'GUARDIAN_READ_UNKNOWN', 'warning')
        row, code = await self._select(user, wallet, coin, active, pos, lane=True)
        if code or row is None or row['status'] != 'needs_recovery':
            return
        rec = rec_from_row(row, 'mark', bg.TP_TRIGGER_LIMIT)
        if mode == 'enforce' and rec is not None:
            return await self._invariant(user, wallet, coin, row, rec, pair, pos, legs, dry=False, lane=True,
                                         since=since)
        if rec is None:
            await self._alarm(user, coin, 'ROW_LEVELS_INVALID', 'critical', row=row['id'])
        await self._legacy_recovery(user, wallet, coin, row, rec, pos, legs, since)

    async def _legacy_recovery(self, user, wallet, coin, row, rec, pos, legs, since):
        """Today's Kraken legacy rule for needs_recovery rows (shadow/off, or invalid levels) with the rev 3
        safety edits. A7: a valid SL and TP resting at the row's recorded levels restore the row to 'open'."""
        if pos is None:
            return await self._recovery_flat(user, wallet, coin, row, since)
        if rec is not None and self._bracket_intact(pos, rec, legs, coin):
            return await self._restore_open(user, coin, row, pos)
        if await self._busy(user['user_id'], since=since):
            return await self._yield(user, coin)
        if not await self._can_trade(user, coin):
            return
        ex = await self._exchange(user, coin, lane=True)
        if ex is None:
            return
        errors = []
        ok, close_oid = await hl_close_with_verification(ex, self.info, wallet, coin, pos.side < 0, pos.qty,
                                                         self._round_price, source=GUARDIAN, executor=self._pool,
                                                         errors=errors)
        if not ok:
            await self._note_mutation_error(user, coin, ' | '.join(errors))       # QC voice-6
            return await self._alarm(user, coin, 'GUARDIAN_RECOVERY_CLOSE_FAILED', 'critical', row=row['id'],
                                     qty=pos.qty)
        await self._alarm(user, coin, 'GUARDIAN_RECOVERY_CLOSE', 'warning', row=row['id'], qty=pos.qty,
                          close_oid=close_oid)
        await self._close_recovery_row(user, ex, coin, row)

    async def _close_recovery_row(self, user, ex, coin, row):
        """A needs_recovery row whose position was just closed (verified flat): cancel the row's own leg ids, then
        'closed'. CA-B8-3: a failed cancel keeps the row needs_recovery, as _recovery_flat does (the lane retries it
        and marks it 'closed_manual' once the legs are gone), and raises a critical alarm."""
        if not await self._cancel_row_legs(ex, coin, row):
            return await self._alarm(user, coin, 'GUARDIAN_RECOVERY_CANCEL_FAILED', 'critical', row=row['id'])
        await self._write(SQL_ROW_STATUS, row['id'], 'closed')

    async def _recovery_flat(self, user, wallet, coin, row, since):
        """needs_recovery row, coin flat: CONFIRM re-read after 5 s; 'closed_manual' (after cancelling the row's
        own leg ids) only once confirmed flat for >= 240 s (I5)."""
        confirmed, _, _ = await self._confirm_flat(user, wallet, coin, lane=True, since=since)
        if not confirmed:
            return
        flat_s = self._flat.note_confirmed_flat(row['id'], self._epoch())
        if flat_s < bg.FLAT_PERSIST_S:
            return
        if await self._busy(user['user_id'], since=since):
            return await self._yield(user, coin)
        if any(real_oid(row.get(k)) for k in ('sl_order_id', 'tp_order_id')):
            if not await self._can_trade(user, coin):
                return
            ex = await self._exchange(user, coin, lane=True)
            if ex is None or not await self._cancel_row_legs(ex, coin, row):
                return                               # retried next tick
        await self._write(SQL_ROW_STATUS, row['id'], 'closed_manual')
        await self._alarm(user, coin, 'ROW_CLOSED_MANUAL', 'info', row=row['id'], flat_s=int(flat_s))

    def _bracket_intact(self, pos, rec, legs, coin):
        """A7: a valid SL AND a valid TP rest at the recorded levels (decide() wants no PLACE and no CLOSE)."""
        if rec.side != pos.side:
            return False
        acts = bg.decide(pos, rec, legs, {}, self._tick(coin), self._lot(coin), self._epoch(),
                         level_policy=follower_guard.DECIDE_LEVEL_POLICY)
        return not any(a.kind in ('PLACE_SL', 'PLACE_TP', 'CLOSE') for a in acts)

    async def _restore_open(self, user, coin, row, pos):
        """needs_recovery -> 'open' after a full restore, with the live qty and entryPx (guarded UPDATE, I17)."""
        await self._write(SQL_ROW_OPEN, row['id'], pos.qty, pos.entry_px)
        await self._alarm(user, coin, 'GUARDIAN_RECOVERY_RESTORED', 'warning', row=row['id'], qty=pos.qty)

    async def _cancel_row_legs(self, ex, coin, row):
        """Cancel the row's own real leg ids ('success' or already gone = done). False if any cancel failed."""
        ok = True
        for key in ('sl_order_id', 'tp_order_id'):
            oid = real_oid(row.get(key))
            if oid is not None and not await cancel_order_verified(ex, coin, oid, 'row %s %s' % (row['id'], key),
                                                                   executor=self._pool, source=GUARDIAN):
                ok = False
        return ok

    # ------------------------------------------------------------------ invariant (lane enforce + full pass)
    async def _invariant(self, user, wallet, coin, row, rec, pair, pos, legs, dry, lane, since=None, deadline=None):
        """decide() -> CONFIRM_FLAT -> filter_actions -> ChurnGuard -> executor (enforce) or GUARDIAN_WOULD_*.
        I12: _busy is read right before each decide() and a busy user yields. decide() returns [] while in flight,
        and that [] must never reach the empty-plan restore below: it would set a naked row to 'open' (I17, I19)."""
        uid = user['user_id']
        tick, lot = self._tick(coin), self._lot(coin)
        mark = self._marks.get(coin) if self._marks_fresh() else None
        ref = {'mark': mark} if mark else {}
        now = self._epoch()
        in_flight = await self._busy(uid, since=since)
        if in_flight:
            return await self._yield(user, coin)
        acts = bg.decide(pos, rec, legs, ref, tick, lot, now, in_flight=in_flight,
                         level_policy=follower_guard.DECIDE_LEVEL_POLICY)
        if any(a.kind == 'CONFIRM_FLAT' for a in acts):
            confirmed, legs, pos = await self._confirm_flat(user, wallet, coin, lane, deadline, since=since)
            if not confirmed:
                return
            now = self._epoch()
            flat_since = None
            if row is not None:
                flat_s = self._flat.note_confirmed_flat(row['id'], now)
                if row['status'] == 'needs_recovery' and flat_s < bg.FLAT_PERSIST_S:
                    return                           # I5: 240 s of confirmed flat before 'closed_manual'
                flat_since = now - flat_s
            in_flight = await self._busy(uid, since=since)
            if in_flight:
                return await self._yield(user, coin)
            acts = bg.decide(None, rec, legs, {}, tick, lot, now, flat_confirmed=True, flat_since=flat_since,
                             in_flight=in_flight, level_policy=follower_guard.DECIDE_LEVEL_POLICY)
        owned = service_oids(pair) | self._retired.get(uid, set())
        acts, alarms = filter_actions(acts, row, owned, pos=pos if isinstance(pos, bg.Pos) else None, ref_px=mark)
        churn = self._churn.setdefault((uid, coin, row['id'] if row else None), bg.ChurnGuard())
        acts, more = churn.filter(acts, legs, now, qty=pos.qty if isinstance(pos, bg.Pos) else None, rec=rec)
        for code, severity, ctx in alarms + more:
            await self._alarm(user, coin, code, severity, **ctx)
        for a in acts:
            if a.kind == 'ALARM':
                await self._alarm(user, coin, a.code, a.severity)
        acts = [a for a in acts if a.kind != 'ALARM']
        if not acts:                                 # decided while not in flight: the SL rests
            if row is not None and row['status'] == 'needs_recovery' and isinstance(pos, bg.Pos) and not dry:
                await self._restore_open(user, coin, row, pos)
            return
        if dry:
            self._count('would', len(acts))
            for a in acts:
                await self._alarm(user, coin, 'GUARDIAN_WOULD_' + a.kind, 'info', code=a.code, level=a.level,
                                  qty=a.qty, oid=a.leg.oid if a.leg is not None else None)
            if (isinstance(pos, bg.Pos) and any(a.kind in ('PLACE_SL', 'CLOSE') for a in acts)
                    and not self._has_exit_stop(pos, legs)):
                # QC voice-1: a naked service position must not be the quietest event while nothing enforces
                await self._alarm(user, coin, 'GUARDIAN_SHADOW_SL_MISSING', 'critical', qty=pos.qty,
                                  row=row['id'] if row is not None else None,
                                  codes=[a.code for a in acts if a.kind in ('PLACE_SL', 'CLOSE')],
                                  handling='GUARDIAN_MODE is not enforce: nothing re-places this SL - '
                                           'place it on Hyperliquid now')
            return
        await self._execute(user, wallet, coin, row, pos, acts, churn, lane, since, deadline, legs=legs)

    async def _execute(self, user, wallet, coin, row, pos, acts, churn, lane, since=None, deadline=None, legs=None):
        """Make-before-break executor: _busy re-checked before every mutation; a failed PLACE_SL closes and
        stops the plan (the SL cancels after it never run); a PLACE_SL that yielded to execute_trade between
        attempts stops the plan too (CA-B8-1: no CANCEL may follow a replacement SL that was not placed);
        CLOSE ends the plan; a row is marked 'closed_manual' only when every cancel before it succeeded.
        QC round 2: a PLACE_SL whose outcome stays unknown (timed out, re-read failed) stops the plan WITHOUT a
        close - an SL may rest - with a critical SL_PLACE_UNKNOWN; the next round re-decides from fresh reads."""
        uid = user['user_id']
        ex, cancels_ok = None, True
        for a in acts:
            if await self._busy(uid, since=since):
                return await self._yield(user, coin)
            if a.kind == 'MARK_ROW_CLOSED_MANUAL':
                if cancels_ok:
                    await self._write(SQL_ROW_STATUS, row['id'], 'closed_manual')
                    await self._alarm(user, coin, 'ROW_CLOSED_MANUAL', 'info', row=row['id'])
                continue
            if ex is None:
                if not await self._can_trade(user, coin):
                    return
                ex = await self._exchange(user, coin, lane, deadline)
                if ex is None:
                    return
            if a.kind == 'PLACE_SL':
                placed = await self._place_sl(user, ex, coin, row, pos, a, churn, since, wallet=wallet, legs=legs)
                if placed == SL_YIELDED:
                    return await self._yield(user, coin)     # CA-B8-1 (I2, I16): the next tick re-decides
                if placed == SL_UNKNOWN:
                    return await self._alarm(user, coin, 'SL_PLACE_UNKNOWN', 'critical', level=a.level, qty=a.qty,
                                             handling='an SL may rest on Hyperliquid: not closed; re-decided '
                                                      'from fresh reads next round')
                if not placed:
                    return await self._close(user, ex, wallet, coin, row, pos, 'SL_REPAIR_FAILED_CLOSED')
            elif a.kind == 'PLACE_TP':
                await self._place_tp(user, ex, coin, row, pos, a, churn)
            elif a.kind == 'CANCEL':
                cancels_ok = await cancel_order_verified(ex, coin, a.leg.oid, 'guardian %s' % a.code,
                                                         executor=self._pool, source=GUARDIAN) and cancels_ok
            elif a.kind == 'CLOSE':
                return await self._close(user, ex, wallet, coin, row, pos, a.code)

    async def _place_leg(self, user, ex, coin, pos, level, qty, leg):
        """One PLACE_SL / PLACE_TP with the order_utils trigger dicts; success only on 'resting'. oid, None (not
        resting / rejected), or PLACE_UNKNOWN (QC round 2: ex.order raised - a timeout or dropped connection after
        HL accepted the order is not a rejection; only a definitive 4xx ClientError is)."""
        px = self._round_price(level)
        sz = self._round_size(coin, qty if qty is not None else pos.qty)
        order_type = sl_order_type(px) if leg == 'SL' else tp_order_type(px)
        hl_rate.charge(hl_rate.W_ACTION, GUARDIAN)
        try:
            result = await self._call(ex.order, coin, pos.side < 0, sz, px, order_type, True)
        except Exception as e:
            await self._note_mutation_error(user, coin, e)
            logger.warning('GUARDIAN %s placement raised for %s: %r', leg, coin, e)
            return None if definitive_rejection(e) else PLACE_UNKNOWN
        st = first_status(result)
        if (isinstance(result, dict) and result.get('status') == 'ok' and isinstance(st, dict)
                and isinstance(st.get('resting'), dict) and st['resting'].get('oid') is not None):
            return str(st['resting']['oid'])
        await self._note_mutation_error(user, coin, result)
        logger.warning('GUARDIAN %s placement not resting for %s: %s', leg, coin, str(result)[:200])
        return None

    async def _place_sl(self, user, ex, coin, row, pos, a, churn, since=None, wallet=None, legs=None):
        """True once an SL rests, False after SL_PLACE_ATTEMPTS failures (the caller closes), SL_YIELDED when
        the user went in flight between attempts (CA-B8-1: the caller stops the whole plan), or SL_UNKNOWN.
        QC round 2: an attempt whose ex.order raised is settled by an orders re-read: a new exit-side stop at the
        level (absent from the decision's book `legs`) is adopted as the placed SL; none counts as a failed
        attempt; a failed re-read returns SL_UNKNOWN (the caller never closes while an SL may rest)."""
        uid = user['user_id']
        known = {l.oid for l in legs} if isinstance(legs, list) else set()
        for attempt in range(bg.SL_PLACE_ATTEMPTS):
            if attempt:
                await self._sleep(attempt)
                if await self._busy(uid, since=since):
                    return SL_YIELDED                # yield to execute_trade; the next tick re-decides
            oid = await self._place_leg(user, ex, coin, pos, a.level, a.qty, 'SL')
            if oid is PLACE_UNKNOWN:
                oid = await self._find_placed_sl(user, wallet, coin, pos, a, known)
                if oid is PLACE_UNKNOWN:
                    return SL_UNKNOWN
                if oid is not None:
                    await self._alarm(user, coin, 'SL_ADOPTED_AFTER_TIMEOUT', 'warning', level=a.level, oid=oid)
            if oid is not None:
                churn.note_placed('SL', oid, self._epoch())
                old = real_oid(row.get('sl_order_id')) if row is not None else None
                if old is not None and old != oid:
                    self._retired.setdefault(uid, set()).add(old)
                if row is not None:
                    await self._write(SQL_ROW_SL, row['id'], oid)
                await self._alarm(user, coin, 'SL_REPAIRED', 'critical', level=a.level, qty=a.qty, oid=oid)
                return True
        return False

    async def _find_placed_sl(self, user, wallet, coin, pos, a, known):
        """QC round 2: after a PLACE_SL whose ex.order raised, re-read frontend_open_orders (after
        UNKNOWN_RECHECK_S): the oid of an exit-side reduce-only stop-market at the rounded level and size that is
        not in `known` (the decision's book), None when there is none, PLACE_UNKNOWN when the re-read fails."""
        if wallet is None:
            return PLACE_UNKNOWN
        await self._sleep(UNKNOWN_RECHECK_S)
        try:
            raw = await self._get(hl_rate.W_FRONTEND_OPEN_ORDERS, self.info.frontend_open_orders, wallet, lane=True)
        except Exception as e:
            await self._alarm(user, coin, 'GUARDIAN_READ_FAILED', 'warning', where='sl_recheck', error=repr(e)[:200])
            return PLACE_UNKNOWN
        if not isinstance(raw, list) or not all(isinstance(o, dict) for o in raw):
            return PLACE_UNKNOWN
        px = self._round_price(a.level)
        sz = self._round_size(coin, a.qty if a.qty is not None else pos.qty)
        tol, lot = max(self._tick(coin), abs(px) * 1e-9), self._lot(coin)
        exit_side = 'sell' if pos.side > 0 else 'buy'
        for l in self._legs(raw, coin):
            if (l.oid not in known and l.side == exit_side and l.kind == bg.STOP_MARKET
                    and (l.reduce_only or l.close_position) and l.trigger is not None
                    and abs(l.trigger - px) <= tol and (l.qty is None or abs(l.qty - sz) < lot / 2)):
                known.add(l.oid)
                return l.oid
        return None

    async def _place_tp(self, user, ex, coin, row, pos, a, churn):
        uid = user['user_id']
        oid = await self._place_leg(user, ex, coin, pos, a.level, a.qty, 'TP')
        now = self._epoch()
        if oid is PLACE_UNKNOWN:
            oid = None                               # a TP never closes: the next round re-decides from its reads
        if oid is None:
            churn.note_place_failed('TP', now)
            return await self._alarm(user, coin, 'TP_PLACE_FAILED', 'warning', level=a.level)
        churn.note_placed('TP', oid, now)
        old = real_oid(row.get('tp_order_id')) if row is not None else None
        if old is not None and old != oid:
            self._retired.setdefault(uid, set()).add(old)
        if row is not None:
            await self._write(SQL_ROW_TP, row['id'], oid)
        await self._alarm(user, coin, 'TP_REPAIRED', 'warning', level=a.level, oid=oid)

    async def _close(self, user, ex, wallet, coin, row, pos, code):
        """Reduce-only, flat-verified close. A needs_recovery row becomes 'closed' once its legs are cancelled
        (_close_recovery_row, CA-B8-3); an 'open' row is left to position_monitor, which records the P&L and then
        cancels the leftover leg."""
        errors = []
        ok, close_oid = await hl_close_with_verification(ex, self.info, wallet, coin, pos.side < 0, pos.qty,
                                                         self._round_price, source=GUARDIAN, executor=self._pool,
                                                         errors=errors)
        if not ok:
            await self._note_mutation_error(user, coin, ' | '.join(errors))       # QC voice-6
            return await self._alarm(user, coin, 'GUARDIAN_CLOSE_FAILED', 'critical', code=code, qty=pos.qty)
        await self._alarm(user, coin, 'GUARDIAN_CLOSE', 'critical', code=code, qty=pos.qty, close_oid=close_oid)
        if row is not None and row['status'] == 'needs_recovery':
            await self._close_recovery_row(user, ex, coin, row)

    # ------------------------------------------------------------------ full pass
    async def _pass_loop(self):
        while True:
            t0 = self._clock()
            try:
                mode = follower_guard.guardian_mode(MODES)
                if mode != 'off':
                    if await self._signal_window_open():
                        logger.info('GUARDIAN_PASS_DEFERRED_SIGNAL')
                    else:
                        self._next_start = await self._pass(dry=(mode != 'enforce'), start=self._next_start,
                                                            deadline=t0 + PASS_S)
            except Exception as e:
                await self._alarm(None, '*', 'GUARDIAN_EXCEPTION', 'critical', where='pass', error=repr(e)[:200])
            await self._sleep(max(1.0, PASS_S - (self._clock() - t0)))

    async def _pass(self, dry, start, deadline):
        """One full pass. Returns the round-robin index the next pass resumes from after GUARDIAN_BUDGET.
        QC voice-15: every pass ends with a 'GUARDIAN_PASS dry=.. users=done/total books_read=.. would=..
        read_failures=.. elapsed=..' heartbeat line, so a shadow review can tell 'ran, found nothing' from 'never ran'."""
        t0 = self._clock()
        self._pass_stats = stats = {'done': 0, 'users': 0, 'books': 0, 'would': 0, 'read_failures': 0}
        try:
            return await self._pass_body(dry, start, deadline)
        finally:
            self._pass_stats = None
            logger.info('GUARDIAN_PASS dry=%s users=%d/%d books_read=%d would=%d read_failures=%d elapsed=%.1fs',
                        dry, stats['done'], stats['users'], stats['books'], stats['would'], stats['read_failures'],
                        self._clock() - t0)

    def _count(self, key, n=1):
        if self._pass_stats is not None:
            self._pass_stats[key] += n

    async def _pass_body(self, dry, start, deadline):
        t_start = self._clock()
        users = await self._fetch(SQL_PASS_USERS)
        rows = await self._fetch(SQL_PASS_ROWS)
        by_user = {}
        for r in rows:
            by_user.setdefault(r['user_id'], []).append(r)
        self._flat.gc([r['id'] for r in rows if r['status'] in ACTIVE_STATUSES])
        order = self._pass_order(users, by_user)
        if not order:
            return 0
        try:
            if not await self._marks_or_cached('pass', lane=False, deadline=deadline):
                return start
        except GuardianBudget:
            await self._alarm(None, '*', 'GUARDIAN_BUDGET', 'critical', done=0, users=len(order))
            return start
        n = len(order)
        self._count('users', n)
        start %= n
        for k in range(n):
            i = (start + k) % n
            if k and self._clock() > deadline:
                await self._alarm(None, '*', 'GUARDIAN_BUDGET', 'critical', done=k, users=n)
                return i
            try:
                await self._pass_user(order[i], by_user.get(order[i]['user_id'], []), dry, deadline)
                self._count('done')
            except GuardianBudget:
                await self._alarm(None, '*', 'GUARDIAN_BUDGET', 'critical', done=k, users=n)
                return i
            except Exception as e:
                self._count('done')
                await self._alarm(order[i], '*', 'GUARDIAN_EXCEPTION', 'critical', where='pass_user',
                                  error=repr(e)[:200])
        elapsed = self._clock() - t_start
        if elapsed > 0.8 * PASS_S:
            await self._alarm(None, '*', 'GUARDIAN_PASS_SLOW', 'warning', elapsed_s=int(elapsed), users=n)
        return 0

    def _pass_order(self, users, by_user):
        """'open' rows first, then other active users, then users with a row closed in the last 60 min whose
        legs are not yet verified clean."""
        now = self._epoch()
        first, second, third = [], [], []
        for u in users:
            rows = by_user.get(u['user_id'], [])
            if any(r['status'] == 'open' for r in rows):
                first.append(u)
            elif u.get('agent_active') or any(r['status'] in ACTIVE_STATUSES for r in rows):
                second.append(u)
            elif any(self._unverified_recent_close(r, now) for r in rows):
                third.append(u)
        return first + second + third

    def _unverified_recent_close(self, r, now):
        """A row closed within RECENT_CLOSED_S (last_fill_at) whose legs are not yet verified gone from the book."""
        ts = row_ts(r.get('last_fill_at'))
        return (r['status'] not in ACTIVE_STATUSES and r['id'] not in self._clean_rows
                and ts is not None and now - ts < RECENT_CLOSED_S)

    def _needs_book(self, urows):
        """CA-B8-2 (capacity): besides a live position, only an active row or an unverified row closed within
        60 min earns the full book read (frontend_open_orders 20 + user_state 2) after the 2-weight screen."""
        now = self._epoch()
        return any(r['status'] in ACTIVE_STATUSES or self._unverified_recent_close(r, now) for r in urows)

    async def _pass_user(self, user, urows, dry, deadline):
        uid = user['user_id']
        if await self._busy(uid):
            return await self._yield(user)
        wallet = self._wallet(user)
        if wallet is None:
            return await self._alarm(user, '*', 'GUARDIAN_CANNOT_TRADE', 'critical', error='no wallet address')
        try:
            screen = await self._get(hl_rate.W_USER_STATE, self.info.user_state, wallet, deadline=deadline)
        except GuardianBudget:
            raise
        except Exception as e:
            self._count('read_failures')
            return await self._read_failed(user, 'GUARDIAN_READ_FAILED', where='screen', error=repr(e)[:200])
        try:
            live = self._live_coins(screen)
        except (AttributeError, TypeError, ValueError):
            self._count('read_failures')
            return await self._read_failed(user, 'GUARDIAN_READ_UNKNOWN', where='screen')
        if not live and not self._needs_book(urows):
            self._read_failures[uid] = 0             # CA-B8-4: a recognised screen was this user's whole read
            return
        book = await self._read_book(user, wallet, lane=False, deadline=deadline)
        if book is None:
            return
        raw, state = book
        if await self._busy(uid):
            return await self._yield(user)
        try:
            coins = self._live_coins(state)
        except ValueError:
            return await self._alarm(user, '*', 'GUARDIAN_READ_UNKNOWN', 'warning')
        by_coin = {}
        for r in urows:
            by_coin.setdefault(self._coin(r), []).append(r)
        owned = service_oids(urows) | self._retired.get(uid, set())
        coins |= {c for c, rs in by_coin.items() if any(r['status'] in ACTIVE_STATUSES for r in rs)}
        if isinstance(raw, list):
            coins |= {o.get('coin') for o in raw if isinstance(o, dict) and str(o.get('oid')) in owned}
        for coin in sorted(c for c in coins if c):
            await self._pass_pair(user, wallet, coin, by_coin.get(coin, []), raw, state, dry, deadline)
        if isinstance(raw, list):
            resting = {str(o.get('oid')) for o in raw if isinstance(o, dict)}
            for r in urows:
                if r['status'] not in ACTIVE_STATUSES and not (
                        {real_oid(r.get('tp_order_id')), real_oid(r.get('sl_order_id'))} & resting):
                    self._clean_rows.add(r['id'])

    async def _pass_pair(self, user, wallet, coin, pair, raw, state, dry, deadline):
        uid = user['user_id']
        active = [r for r in pair if r['status'] in ACTIVE_STATUSES]
        if any(r['status'] == 'needs_recovery' for r in active):
            return                                   # the recovery lane owns this pair (incl. its ambiguity alarm)
        pos, legs = self._pos(state, coin), self._legs(raw, coin)
        if pos is bg.UNKNOWN or legs is bg.UNKNOWN:
            return await self._alarm(user, coin, 'GUARDIAN_READ_UNKNOWN', 'warning')
        row, code = await self._select(user, wallet, coin, active, pos, lane=False, deadline=deadline)
        if code:
            return
        if row is None and pos is not None:
            return await self._rowless_position(user, coin, pos, legs)
        if row is not None and pos is None:
            return await self._open_row_flat(user, wallet, coin, row, deadline)
        owned = service_oids(pair) | self._retired.get(uid, set())
        if row is None:
            if not any(l.oid in owned for l in legs):
                return                               # flat, rowless, nothing of ours on the book
            return await self._invariant(user, wallet, coin, None, None, pair, None, legs, dry, lane=False,
                                         deadline=deadline)
        rec = rec_from_row(row, 'mark', bg.TP_TRIGGER_LIMIT)
        if rec is None:
            return await self._alarm(user, coin, 'ROW_LEVELS_INVALID', 'critical', row=row['id'])
        await self._invariant(user, wallet, coin, row, rec, pair, pos, legs, dry, lane=False, deadline=deadline)

    async def _open_row_flat(self, user, wallet, coin, row, deadline):
        """'open' row, coin flat: CONFIRM re-read, FlatTracker, no decide / cancel / UPDATE (position_monitor
        records the close, then cancels the leftover leg); FLAT_ROW_UNRECORDED once confirmed flat > 600 s."""
        confirmed, _, _ = await self._confirm_flat(user, wallet, coin, lane=False, deadline=deadline)
        if not confirmed:
            return
        flat_s = self._flat.note_confirmed_flat(row['id'], self._epoch())
        if flat_s > follower_guard.FLAT_ROW_UNRECORDED_S:
            await self._alarm(user, coin, 'FLAT_ROW_UNRECORDED', 'warning', row=row['id'], flat_s=int(flat_s))

    @staticmethod
    def _has_exit_stop(pos, legs):
        """Any exit-side stop leg (reduce-only / close-position, incl. positionTpsl rows) at any level or size."""
        exit_side = 'sell' if pos.side > 0 else 'buy'
        return any(l.side == exit_side and l.kind in bg.SL_KINDS and (l.reduce_only or l.close_position)
                   for l in (legs if isinstance(legs, list) else ()))

    async def _rowless_position(self, user, coin, pos, legs):
        """Positions the service did not open keep today's behaviour on HL: alarm only (critical when naked)."""
        if self._has_exit_stop(pos, legs):
            await self._alarm(user, coin, 'UNTRACKED_POSITION_NO_ROW', 'warning', qty=pos.qty, side=pos.side)
        else:
            await self._alarm(user, coin, 'UNTRACKED_NAKED_POSITION', 'critical', qty=pos.qty, side=pos.side)


async def start_position_guardian(db_pool):
    """Start the HL position guardian (call from main.py startup; it waits START_DELAY_S itself)."""
    guardian = HLPositionGuardian(db_pool)
    await guardian.run()
