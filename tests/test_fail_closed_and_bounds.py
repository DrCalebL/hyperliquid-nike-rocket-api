"""
T25 (HL follower) dedup + endpoint half: check_any_open_positions_or_orders fails CLOSED (C42), and
/api/broadcast-signal validates the payload with signal_guard -> 422 before any DB write (C46 / I11).
The endpoint runs through fastapi's TestClient with get_db overridden (gate deps from requirements-gate.txt).
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.fakes import fake_hl
from tests.fakes.fake_hl import creds, fast_sleep, notes, venue  # noqa: F401  (pytest fixtures)
from tests.fakes.fake_pool import FakeDB, FakePool

import follower_endpoints as fe
import hosted_trading_loop as htl

WALLET = '0x' + '%040x' % 7


@pytest.fixture
def db():
    return FakeDB()


def _dedup(loop, info):
    return loop.check_any_open_positions_or_orders(info, None, WALLET, 'nk_user...', user_id=7, signal_id='sig-1')


# ---------------------------------------------------------------- dedup fails closed (C42)
async def test_position_read_error_skips_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.script('user_state', lambda *a: TimeoutError('read timed out'))
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason.startswith('Position check failed (TimeoutError)')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR')


async def test_order_read_error_skips_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.script('open_orders', lambda *a: ConnectionError('reset'))
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason.startswith('Order check failed (ConnectionError)')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR')


async def test_outer_error_skips_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    db.add_signal('sig-1')
    db.fail_next.append(('SELECT COUNT(*) FROM open_positions', RuntimeError('db down')))
    assert await _dedup(loop, loop.info) == (True, 'Safety check error - skipping')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR') and db.errors('SAFETY_CHECK_ERROR')


async def test_clean_reads_allow_the_trade(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    assert await _dedup(loop, loop.info) == (False, None)


async def test_open_order_still_blocks_as_today(venue, db, notes):
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.add_order(WALLET, 'ADA', 'A', 100, 0.45)
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason == 'Open orders exist: 1 orders'


@pytest.mark.parametrize('shape', [None, {}, {'error': 'Could not parse JSON: <html>502</html>'},
                                   {'assetPositions': None}, {'assetPositions': [{'position': {'szi': 'n/a'}}]}])
async def test_qc_mf6_an_unrecognised_position_shape_fails_closed(venue, db, notes, shape):
    """QC money-followers-6 (C42): a 2xx user_state with an unrecognised body (None, {}, the SDK's
    {'error': 'Could not parse JSON: ...'}, unparsable szi) is a read error, never 'no position'."""
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.set_position(WALLET, 'ADA', 4000.0, 0.5)
    venue.script('user_state', lambda *a: shape)
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason.startswith('Position check failed')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR')


@pytest.mark.parametrize('shape', [None, {'error': 'Could not parse JSON: <html>502</html>'}])
async def test_qc_mf6_an_unrecognised_orders_shape_fails_closed(venue, db, notes, shape):
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.script('open_orders', lambda *a: shape)
    has_open, reason = await _dedup(loop, loop.info)
    assert has_open is True and reason.startswith('Order check failed')
    assert db.errors('DEDUP_SKIPPED_READ_ERROR')


async def test_a_read_error_sends_no_entry(venue, db, creds, notes, fast_sleep):
    user = db.add_user(7, encrypt=creds)
    s = db.add_signal('sig-1')
    loop = htl.HostedTradingLoop(FakePool(db))
    venue.script('user_state', lambda *a: TimeoutError('read timed out'))
    ok = await loop.execute_trade(
        {'id': 7, 'api_key': user['api_key'], 'email': user['email'],
         'hl_private_key_encrypted': user['hl_private_key_encrypted'], 'hl_wallet_address': WALLET},
        {'delivery_id': None, 'signal_id': s['signal_id'], 'action': 'BUY', 'symbol': 'ADA/USDT',
         'entry_price': 0.5, 'stop_loss': 0.45, 'take_profit': 0.55, 'leverage': 5.0, 'risk_pct': 0.02})
    assert ok is False and venue.order_calls() == []


# ---------------------------------------------------------------- endpoint validation (C46)
class _Query:
    def filter(self, *a, **k):
        return self

    def all(self):
        return []

    def count(self):
        return 0

    def scalar(self):
        return 0

    def first(self):
        return None


class _Session:
    def __init__(self):
        self.added, self.commits = [], 0

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def refresh(self, obj):
        obj.id = 1

    def query(self, *a):
        return _Query()

    def close(self):
        pass


@pytest.fixture
def client(monkeypatch):
    app = FastAPI()
    app.include_router(fe.router)
    session = _Session()
    app.dependency_overrides[fe.get_db] = lambda: session
    monkeypatch.setattr(fe, 'MASTER_API_KEY', 'test-master-key')
    c = TestClient(app)
    c.session = session
    return c


GOOD = {'action': 'BUY', 'symbol': 'ADA/USDT', 'entry_price': 0.5, 'stop_loss': 0.45, 'take_profit': 0.55,
        'leverage': 5, 'risk_pct': 0.0125}


KEY = {'x-master-key': 'test-master-key'}


@pytest.mark.parametrize('risk', [0.01, 0.0125, 0.02, 0.025, 0.03])
def test_broadcast_accepts_every_master_fraction(client, risk):
    r = client.post('/api/broadcast-signal', json=dict(GOOD, risk_pct=risk), headers=KEY)
    assert r.status_code == 200, r.text
    assert len(client.session.added) == 1 and client.session.added[0].risk_pct == risk


@pytest.mark.parametrize('change', [
    {'risk_pct': 0}, {'risk_pct': 0.05}, {'risk_pct': -0.01},
    {'stop_loss': 0.6}, {'take_profit': 0.4}, {'action': 'SELL'}, {'leverage': 0}, {'leverage': 100},
    {'action': 'HOLD'}])
def test_broadcast_rejects_invalid_payloads_with_422_before_any_db_write(client, change):
    r = client.post('/api/broadcast-signal', json=dict(GOOD, **change), headers=KEY)
    assert r.status_code == 422, r.text
    assert client.session.added == [] and client.session.commits == 0


def test_broadcast_rejects_a_nan_risk_pct_with_422(client):
    body = '{"action": "BUY", "symbol": "ADA/USDT", "entry_price": 0.5, "stop_loss": 0.45, "take_profit": 0.55, '            '"leverage": 5, "risk_pct": NaN}'
    r = client.post('/api/broadcast-signal', content=body, headers=dict(KEY, **{'content-type': 'application/json'}))
    assert r.status_code == 422
    assert client.session.added == []


def test_broadcast_rejects_a_missing_risk_pct_with_422(client):
    payload = {k: v for k, v in GOOD.items() if k != 'risk_pct'}
    r = client.post('/api/broadcast-signal', json=payload, headers=KEY)
    assert r.status_code == 422
    assert client.session.added == []


# ---------------------------------------------------------------- SEC-2 (separate, owner-gated commit)
def test_sec2_unset_master_key_returns_503_for_broadcast_and_admin_stats(client, monkeypatch):
    monkeypatch.setattr(fe, 'MASTER_API_KEY', None)
    assert client.post('/api/broadcast-signal', json=GOOD, headers=KEY).status_code == 503
    assert client.get('/api/admin/stats', headers=KEY).status_code == 503
    assert client.session.added == []


def test_sec2_wrong_missing_or_non_ascii_key_is_401_and_the_right_key_passes(client):
    assert client.post('/api/broadcast-signal', json=GOOD, headers={'x-master-key': 'nope'}).status_code == 401
    assert client.post('/api/broadcast-signal', json=GOOD).status_code == 401
    assert client.post('/api/broadcast-signal', json=GOOD, headers={'x-master-key': 'k\xe9y'.encode('latin-1')}).status_code == 401
    assert client.session.added == []
    assert client.post('/api/broadcast-signal', json=GOOD, headers=KEY).status_code == 200
    assert client.get('/api/admin/stats', headers=KEY).status_code == 200
