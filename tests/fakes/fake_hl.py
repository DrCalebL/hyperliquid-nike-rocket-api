"""
Offline Hyperliquid SDK + eth_account stubs for the HL follower tests (Wave 1, spec C47).

Importing this module installs stub modules into sys.modules (hyperliquid, hyperliquid.info,
hyperliquid.exchange, hyperliquid.utils, hyperliquid.utils.constants, eth_account), so it must be imported
BEFORE hosted_trading_loop / order_utils / position_monitor / position_guardian / follower_endpoints.
Every stub call is served by the current FakeVenue (fake_hl.VENUE, set by the `venue` fixture), which models:
- frontendOpenOrders rows incl. position TP/SL rows (isPositionTpsl True, sz '0.0'), int oids, int-only cancel;
- user_state without position times; user_fills_by_time rows with dir / startPosition / oid;
- an HL weight counter (constructor requests included), an ordered call log, and a constructor log that
  records whether meta= / spot_meta= were passed (without both the SDK makes 2 requests = 40 weight).
Tests import the fixtures `venue` and `fast_sleep` from here.
"""
import asyncio
import hashlib
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

MAINNET_API_URL = 'https://api.hyperliquid.xyz'
TESTNET_API_URL = 'https://api.hyperliquid-testnet.xyz'
WEIGHTS = {'user_state': 2, 'all_mids': 2, 'open_orders': 20, 'frontend_open_orders': 20, 'user_fills': 20,
           'user_fills_by_time': 20, 'meta': 20, 'spot_meta': 20, 'meta_and_asset_ctxs': 20,
           'order': 1, 'cancel': 1, 'update_leverage': 1}
VENUE = None


def ok_status(status):
    return {'status': 'ok', 'response': {'type': 'order', 'data': {'statuses': [status]}}}


def err_status(msg):
    return ok_status({'error': msg})


class FakeVenue:
    """One fake Hyperliquid venue: accounts keyed by address."""

    def __init__(self):
        self.accounts = {}
        self.mids = {'ADA': 0.5, 'BTC': 60000.0}
        self.marks = dict(self.mids)
        self.meta = {'universe': [{'name': 'ADA', 'szDecimals': 0}, {'name': 'BTC', 'szDecimals': 5}]}
        self.spot = {'universe': [], 'tokens': []}
        self.calls = []            # (target, name, args) in call order; target 'info' | 'exchange' | 'ctor'
        self.weight = 0
        self.ctors = []            # ('Info' | 'Exchange', {'meta': bool, 'spot_meta': bool, 'account_address': ...})
        self.ctor_requests = 0
        self.now_ms = 1_790_000_000_000
        self._scripts = {}
        self._oid = 1000

    # ---------- account model ----------
    def acct(self, address):
        return self.accounts.setdefault(address, {'positions': {}, 'orders': [], 'fills': [], 'equity': 10000.0})

    def set_position(self, address, coin, szi, entry_px=None):
        a = self.acct(address)
        if not szi:
            a['positions'].pop(coin, None)
        else:
            px = entry_px if entry_px is not None else self.mids.get(coin, 1.0)
            a['positions'][coin] = {'szi': float(szi), 'entryPx': float(px)}

    def szi(self, address, coin):
        p = self.acct(address)['positions'].get(coin)
        return p['szi'] if p else 0.0

    def next_oid(self):
        self._oid += 1
        return self._oid

    def add_order(self, address, coin, side, sz, trigger_px, order_type='Stop Market', reduce_only=True,
                  position_tpsl=False, limit_px=None, oid=None):
        """side 'A' = sell (exit of a long), 'B' = buy. Returns the int oid."""
        oid = oid if oid is not None else self.next_oid()
        self.acct(address)['orders'].append({
            'coin': coin, 'side': side, 'limitPx': str(limit_px if limit_px is not None else trigger_px),
            'sz': '0.0' if position_tpsl else str(float(sz)), 'origSz': str(float(sz)), 'oid': oid,
            'timestamp': self.now_ms, 'triggerCondition': 'N/A', 'isTrigger': True, 'triggerPx': str(trigger_px),
            'children': [], 'isPositionTpsl': bool(position_tpsl), 'reduceOnly': bool(reduce_only),
            'orderType': order_type, 'tif': None, 'cloid': None})
        return oid

    def orders(self, address, coin=None):
        return [o for o in self.acct(address)['orders'] if coin is None or o['coin'] == coin]

    def external_cancel(self, address, oid):
        a = self.acct(address)
        a['orders'] = [o for o in a['orders'] if o['oid'] != oid]

    def add_fill(self, address, coin, side, sz, px, dir_, time_ms, oid, start_position=0.0):
        self.acct(address)['fills'].append({
            'coin': coin, 'px': str(px), 'sz': str(sz), 'side': side, 'time': int(time_ms),
            'startPosition': str(start_position), 'dir': dir_, 'closedPnl': '0.0', 'hash': '0x0', 'oid': oid,
            'crossed': True, 'fee': '0.0', 'tid': self.next_oid(), 'feeToken': 'USDC'})

    def script(self, name, fn, times=None):
        """fn(*args) -> response | exception instance (raised) | NotImplemented (fall through to the default)."""
        self._scripts.setdefault(name, []).append({'fn': fn, 'times': times})

    def names(self, target=None):
        return [n for t, n, _ in self.calls if target is None or t == target]

    def order_calls(self):
        return [a for t, n, a in self.calls if n == 'order']

    # ---------- request plumbing ----------
    def request(self, target, name, args):
        self.calls.append((target, name, args))
        self.weight += WEIGHTS.get(name, 0)
        result = NotImplemented
        for s in list(self._scripts.get(name, [])):
            r = s['fn'](*args)
            if r is NotImplemented:
                continue
            if s['times'] is not None:
                s['times'] -= 1
                if s['times'] <= 0:
                    self._scripts[name].remove(s)
            result = r
            break
        if result is NotImplemented:
            result = getattr(self, '_default_' + name)(*args)
        if isinstance(result, BaseException):
            raise result
        if name in ('user_fills', 'user_fills_by_time') and isinstance(result, list):
            self.weight += len(result) // 20
        return result

    def ctor_request(self, name):
        self.ctor_requests += 1
        return self.request('ctor', name, ())

    # ---------- info defaults ----------
    def _default_user_state(self, address, dex=''):
        a = self.acct(address)
        return {'assetPositions': [{'type': 'oneWay', 'position': {'coin': c, 'szi': repr(p['szi']),
                                                                    'entryPx': repr(p['entryPx'])}}
                                   for c, p in a['positions'].items()],
                'marginSummary': {'accountValue': str(a['equity'])}}

    def _default_open_orders(self, address, dex=''):
        return [{k: o[k] for k in ('coin', 'limitPx', 'oid', 'side', 'sz', 'timestamp')}
                for o in self.acct(address)['orders']]

    def _default_frontend_open_orders(self, address, dex=''):
        return [dict(o) for o in self.acct(address)['orders']]

    def _default_all_mids(self, dex=''):
        return {c: str(p) for c, p in self.mids.items()}

    def _default_meta(self, dex=''):
        return self.meta

    def _default_spot_meta(self):
        return self.spot

    def _default_meta_and_asset_ctxs(self):
        return [self.meta, [{'markPx': str(self.marks.get(u['name'], 0))} for u in self.meta['universe']]]

    def _default_user_fills(self, address):
        return [dict(f) for f in self.acct(address)['fills']]

    def _default_user_fills_by_time(self, address, start_time, end_time=None, aggregate_by_time=False):
        return [dict(f) for f in self.acct(address)['fills'] if f['time'] >= start_time]

    # ---------- exchange defaults ----------
    def _default_order(self, account, coin, is_buy, sz, limit_px, order_type, reduce_only=False):
        oid = self.next_oid()
        if 'limit' in order_type:                                   # IOC entry / reduce-only close
            cur = self.szi(account, coin)
            delta = float(sz) if is_buy else -float(sz)
            if reduce_only:
                if cur == 0 or (cur > 0) == is_buy:
                    return err_status('Reduce only order would increase position. asset=0')
                delta = max(-abs(cur), min(abs(cur), delta))
            new = cur + delta
            entry = limit_px if cur == 0 else self.acct(account)['positions'][coin]['entryPx']
            self.set_position(account, coin, new, entry)
            opening = cur == 0 or (cur > 0) == is_buy
            dir_ = ('Open ' if opening else 'Close ') + ('Long' if (is_buy if opening else not is_buy) else 'Short')
            self.add_fill(account, coin, 'B' if is_buy else 'A', abs(delta), limit_px, dir_, self.now_ms, oid, cur)
            return ok_status({'filled': {'totalSz': str(abs(delta)), 'avgPx': str(limit_px), 'oid': oid}})
        trig = order_type['trigger']
        if trig['tpsl'] == 'sl':
            kind = 'Stop Market' if trig['isMarket'] else 'Stop Limit'
        else:
            kind = 'Take Profit Market' if trig['isMarket'] else 'Take Profit Limit'
        self.add_order(account, coin, 'B' if is_buy else 'A', sz, trig['triggerPx'], kind, reduce_only,
                       limit_px=limit_px, oid=oid)
        return ok_status({'resting': {'oid': oid}})

    def _default_cancel(self, account, coin, oid):
        if not isinstance(oid, int) or isinstance(oid, bool):
            raise TypeError('HL cancel requires an int oid, got %r' % (oid,))
        a = self.acct(account)
        for o in a['orders']:
            if o['oid'] == oid and o['coin'] == coin:
                a['orders'].remove(o)
                return {'status': 'ok', 'response': {'type': 'cancel', 'data': {'statuses': ['success']}}}
        return {'status': 'ok', 'response': {'type': 'cancel', 'data': {'statuses': [
            {'error': 'Order was never placed, already canceled, or filled. asset=0'}]}}}

    def _default_update_leverage(self, account, leverage, coin, is_cross=True):
        return {'status': 'ok', 'response': {'type': 'default'}}


# ---------------- SDK stubs ----------------
class Info:
    def __init__(self, base_url=None, skip_ws=False, meta=None, spot_meta=None, perp_dexs=None, timeout=None):
        VENUE.ctors.append(('Info', {'meta': meta is not None, 'spot_meta': spot_meta is not None}))
        if spot_meta is None:
            VENUE.ctor_request('spot_meta')
        if meta is None:
            VENUE.ctor_request('meta')
        self.base_url = base_url

    def user_state(self, address, dex=''):
        return VENUE.request('info', 'user_state', (address,))

    def open_orders(self, address, dex=''):
        return VENUE.request('info', 'open_orders', (address,))

    def frontend_open_orders(self, address, dex=''):
        return VENUE.request('info', 'frontend_open_orders', (address,))

    def all_mids(self, dex=''):
        return VENUE.request('info', 'all_mids', ())

    def meta(self, dex=''):
        return VENUE.request('info', 'meta', ())

    def spot_meta(self):
        return VENUE.request('info', 'spot_meta', ())

    def meta_and_asset_ctxs(self):
        return VENUE.request('info', 'meta_and_asset_ctxs', ())

    def user_fills(self, address):
        return VENUE.request('info', 'user_fills', (address,))

    def user_fills_by_time(self, address, start_time, end_time=None, aggregate_by_time=False):
        return VENUE.request('info', 'user_fills_by_time', (address, start_time))


class Exchange:
    def __init__(self, wallet, base_url=None, meta=None, vault_address=None, account_address=None,
                 spot_meta=None, perp_dexs=None, timeout=None):
        VENUE.ctors.append(('Exchange', {'meta': meta is not None, 'spot_meta': spot_meta is not None,
                                         'account_address': account_address}))
        if spot_meta is None:
            VENUE.ctor_request('spot_meta')
        if meta is None:
            VENUE.ctor_request('meta')
        self.wallet = wallet
        self.account = account_address or wallet.address

    def order(self, name, is_buy, sz, limit_px, order_type, reduce_only=False, cloid=None, builder=None):
        return VENUE.request('exchange', 'order', (self.account, name, is_buy, sz, limit_px, order_type, reduce_only))

    def cancel(self, name, oid):
        return VENUE.request('exchange', 'cancel', (self.account, name, oid))

    def update_leverage(self, leverage, name, is_cross=True):
        return VENUE.request('exchange', 'update_leverage', (self.account, leverage, name, is_cross))


class ExchangeNoMeta(Exchange):
    """An older SDK whose Exchange(...) takes no meta / spot_meta (R3-M3 fallback path)."""

    def __init__(self, wallet, base_url=None, vault_address=None, account_address=None):
        super().__init__(wallet, base_url, None, vault_address, account_address, None)


class LocalAccount:
    def __init__(self, key):
        self.key = key
        self.address = '0x' + hashlib.sha256(str(key).encode()).hexdigest()[:40]


class Account:
    @staticmethod
    def from_key(key):
        return LocalAccount(key)


class World:
    """One clock for a guardian test: FakeDB NOW() (naive UTC), the guardian's _epoch / _clock / _sleep and the
    venue's fill timestamps advance together. `on_sleep` callables run whenever the guardian sleeps."""

    def __init__(self, guardian, db, venue, start=datetime(2026, 9, 28, 12, 0, 0)):
        self.g, self.db, self.venue = guardian, db, venue
        self.t, self.mono, self.on_sleep, self.slept = start, 1000.0, [], []
        self._sync()
        guardian._epoch = self.epoch
        guardian._clock = lambda: self.mono
        guardian._sleep = self.sleep

    def _sync(self):
        self.db.clock = self.t
        self.venue.now_ms = int(self.epoch() * 1000)

    def epoch(self):
        return self.t.replace(tzinfo=timezone.utc).timestamp()

    def advance(self, seconds):
        self.t += timedelta(seconds=seconds)
        self.mono += seconds
        self._sync()

    async def sleep(self, seconds):
        self.slept.append(seconds)
        for fn in list(self.on_sleep):
            fn()
        self.advance(seconds)
        await asyncio.sleep(0)


def install():
    """Register the stubs in sys.modules (idempotent)."""
    if getattr(sys.modules.get('hyperliquid'), '__fake__', False):
        return
    hl = types.ModuleType('hyperliquid')
    hl.__fake__ = True
    hl.__path__ = []
    info = types.ModuleType('hyperliquid.info')
    info.Info = Info
    exchange = types.ModuleType('hyperliquid.exchange')
    exchange.Exchange = Exchange
    utils = types.ModuleType('hyperliquid.utils')
    utils.__path__ = []
    constants = types.ModuleType('hyperliquid.utils.constants')
    constants.MAINNET_API_URL = MAINNET_API_URL
    constants.TESTNET_API_URL = TESTNET_API_URL
    utils.constants = constants
    hl.info, hl.exchange, hl.utils = info, exchange, utils
    eth = types.ModuleType('eth_account')
    eth.Account = Account
    sys.modules.update({'hyperliquid': hl, 'hyperliquid.info': info, 'hyperliquid.exchange': exchange,
                        'hyperliquid.utils': utils, 'hyperliquid.utils.constants': constants, 'eth_account': eth})


install()


@pytest.fixture
def venue():
    """A fresh FakeVenue for the test, and an empty hl_rate meter."""
    global VENUE
    import hl_rate
    VENUE = FakeVenue()
    with hl_rate._lock:
        hl_rate._events.clear()
    yield VENUE
    VENUE = None


@pytest.fixture
def notes(monkeypatch):
    """Captures notify_admin from every already-imported app module: list of (title, level, details)."""
    sent = []

    async def _notify(title, details, level='warning'):
        sent.append((title, level, details))
        return True

    for name in ('order_utils', 'hosted_trading_loop', 'position_guardian'):
        mod = sys.modules.get(name)
        if mod is not None and hasattr(mod, 'notify_admin'):
            monkeypatch.setattr(mod, 'notify_admin', _notify)
    return sent


@pytest.fixture
def creds(monkeypatch):
    """Sets CREDENTIALS_ENCRYPTION_KEY; returns encrypt(private_key) -> the Fernet ciphertext stored in follower_users."""
    from cryptography.fernet import Fernet
    key = Fernet.generate_key()
    monkeypatch.setenv('CREDENTIALS_ENCRYPTION_KEY', key.decode())
    cipher = Fernet(key)
    return lambda private_key: cipher.encrypt(private_key.encode()).decode()


@pytest.fixture
def fast_sleep(monkeypatch):
    """asyncio.sleep that only yields once; the requested delays are recorded in the returned list."""
    real_sleep = asyncio.sleep
    slept = []

    async def _fast(delay, result=None):
        slept.append(delay)
        await real_sleep(0)
        return result

    monkeypatch.setattr(asyncio, 'sleep', _fast)
    return slept
