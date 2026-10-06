"""
In-memory stand-in for the asyncpg pool (Wave 1 HL follower tests and tools/chaos_follower.py).

FakePool(db).acquire() yields a FakeConn whose execute / fetch / fetchrow / fetchval route the exact SQL used by
hosted_trading_loop.py and position_guardian.py (guardian statements carry a '/* hlg:<name> */' tag) to small
handlers over in-memory tables. Unknown SQL raises AssertionError so a drifted statement fails loudly.
'NOW()' is FakeDB.now(): naive UTC like Postgres timestamp columns; tests may pin FakeDB.clock.
"""
import json
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

ACTIVE = ('open', 'needs_recovery')


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class FakeDB:
    def __init__(self):
        self.users = {}
        self.positions = {}
        self.signals = {}
        self.deliveries = {}
        self.trades = []
        self.error_logs = []
        self.log = []                  # (verb, normalised sql, args)
        self.clock = None              # naive UTC datetime pinned by a test, else the real clock
        self.fail_next = []            # [(substring, exception)]: the next matching statement raises
        self._ids = {'positions': 0, 'signals': 0, 'deliveries': 0}

    # ---------- test helpers ----------
    def now(self):
        return self.clock or _utcnow()

    def add_user(self, uid, encrypt=None, **kw):
        """encrypt: the `creds` fixture's function; the stored key then decrypts to '0x' + 64 hex chars."""
        enc = encrypt('0x%064x' % uid) if encrypt else 'enc-%d' % uid
        u = {'id': uid, 'api_key': 'nk_test_user_%04d_abcdefgh' % uid, 'email': 'u%d@example.com' % uid,
             'hl_private_key_encrypted': enc, 'hl_wallet_address': '0x%040x' % uid,
             'agent_active': True, 'credentials_set': True, 'access_granted': True,
             'api_wallet_expires_at': None, 'billing_cycle_start': None}
        u.update(kw)
        self.users[uid] = u
        return u

    def add_position(self, **kw):
        self._ids['positions'] += 1
        row = {'id': self._ids['positions'], 'user_id': 1, 'signal_id': None, 'entry_order_id': 'E1',
               'tp_order_id': None, 'sl_order_id': None, 'symbol': 'ADA/USDT', 'hl_coin': 'ADA', 'side': 'BUY',
               'quantity': 100.0, 'leverage': 5.0, 'entry_fill_price': 0.5, 'target_tp': 0.55,
               'target_sl': 0.45, 'opened_at': self.now(), 'first_fill_at': None, 'last_fill_at': None,
               'status': 'open'}
        row.update(kw)
        self.positions[row['id']] = row
        return row

    def add_signal(self, signal_id='sig-1', created_at=None):
        self._ids['signals'] += 1
        s = {'id': self._ids['signals'], 'signal_id': signal_id, 'created_at': created_at or self.now()}
        self.signals[s['id']] = s
        return s

    def add_delivery(self, user_id, signal_pk, acknowledged_at=None):
        self._ids['deliveries'] += 1
        d = {'id': self._ids['deliveries'], 'user_id': user_id, 'signal_id': signal_pk,
             'acknowledged': acknowledged_at is not None, 'acknowledged_at': acknowledged_at}
        self.deliveries[d['id']] = d
        return d

    def errors(self, error_type=None):
        return [e for e in self.error_logs if error_type is None or e['error_type'] == error_type]

    def writes(self):
        """INSERT/UPDATE statements on open_positions, in order."""
        return [(v, s, a) for v, s, a in self.log
                if re.sub(r'^/\* hlg:\w+ \*/ ', '', s).startswith(('INSERT INTO open_positions', 'UPDATE open_positions'))]

    # ---------- routing ----------
    def run(self, verb, sql, args):
        s = ' '.join(sql.split())
        self.log.append((verb, s, args))
        for i, (needle, exc) in enumerate(self.fail_next):
            if needle in s:
                del self.fail_next[i]
                raise exc
        tag = re.match(r'/\* hlg:(\w+) \*/', s)
        if tag:
            return getattr(self, '_hlg_' + tag.group(1))(verb, s, args)
        for needle, handler in (
                ('INSERT INTO error_logs', self._error_log),
                ('UPDATE signal_deliveries SET acknowledged = true', self._ack),
                ('SELECT id FROM signals WHERE signal_id = $1', self._signal_pk),
                ('SELECT COUNT(*) FROM open_positions WHERE user_id = $1 AND signal_id = $2', self._count_rows),
                ('SELECT COUNT(*) FROM trades', self._count_trades),
                ('INSERT INTO open_positions', self._insert_position),
                ('UPDATE follower_users SET billing_cycle_start', self._billing_start)):
            if needle in s:
                return handler(verb, s, args)
        raise AssertionError('FakeDB: unhandled SQL: %s' % s[:160])

    # ---------- hosted_trading_loop statements ----------
    def _error_log(self, verb, s, args):
        api_key, error_type, message, context = args[:4]
        self.error_logs.append({'api_key': api_key, 'error_type': error_type, 'error_message': message,
                                'context': json.loads(context) if context else None})
        return 'INSERT 0 1'

    def _ack(self, verb, s, args):
        d = self.deliveries.get(args[0])
        if d is not None:
            d['acknowledged'], d['acknowledged_at'] = True, self.now()
        return 'UPDATE %d' % (d is not None)

    def _signal_pk(self, verb, s, args):
        return next((sid for sid, sg in self.signals.items() if sg['signal_id'] == args[0]), None)

    def _count_rows(self, verb, s, args):
        return sum(1 for r in self.positions.values() if r['user_id'] == args[0] and r['signal_id'] == args[1])

    def _count_trades(self, verb, s, args):
        return 0

    def _insert_position(self, verb, s, args):
        m = re.search(r'INSERT INTO open_positions \((.*?)\) VALUES \((.*)\)$', s)
        cols = [c.strip() for c in m.group(1).split(',')]
        vals = []
        for v in (x.strip() for x in m.group(2).split(',')):
            if v.startswith('$'):
                vals.append(args[int(v[1:]) - 1])
            elif v.upper() == 'NOW()':
                vals.append(self.now())
            else:
                vals.append(v.strip("'"))
        self._ids['positions'] += 1
        row = {'id': self._ids['positions'], 'first_fill_at': None, 'last_fill_at': None}
        row.update(zip(cols, vals))
        self.positions[row['id']] = row
        return 'INSERT 0 1'

    def _billing_start(self, verb, s, args):
        u = self.users.get(args[0])
        if u is not None and u.get('billing_cycle_start') is None:
            u['billing_cycle_start'] = args[1]
        return 'UPDATE 1'

    # ---------- position_guardian statements (tagged) ----------
    def _user_cols(self, uid):
        u = self.users.get(uid, {})
        return {k: u.get(k) for k in ('api_key', 'email', 'hl_private_key_encrypted', 'hl_wallet_address',
                                      'agent_active', 'api_wallet_expires_at')}

    def _recent(self, r):
        return r['status'] in ACTIVE or (r.get('opened_at') and r['opened_at'] > self.now() - timedelta(days=7))

    def _hlg_lane_rows(self, verb, s, args):
        rows = [dict(r, **self._user_cols(r['user_id'])) for r in self.positions.values()
                if r['status'] == 'needs_recovery']
        return sorted(rows, key=lambda r: r['opened_at'])

    def _hlg_user_rows(self, verb, s, args):
        rows = [dict(r) for r in self.positions.values() if r['user_id'] == args[0] and self._recent(r)]
        return sorted(rows, key=lambda r: r['opened_at'], reverse=True)

    def _hlg_pass_users(self, verb, s, args):
        return [dict(user_id=uid, **self._user_cols(uid)) for uid, u in sorted(self.users.items())
                if u.get('hl_private_key_encrypted') is not None]

    def _hlg_pass_rows(self, verb, s, args):
        rows = [dict(r) for r in self.positions.values() if self._recent(r)]
        return sorted(rows, key=lambda r: r['opened_at'], reverse=True)

    def _hlg_busy(self, verb, s, args):
        uid, since = args
        floor = self.now() - timedelta(seconds=180)
        return any(d['user_id'] == uid and d['acknowledged_at'] is not None and d['acknowledged_at'] > floor
                   and (since is None or d['acknowledged_at'] > since) for d in self.deliveries.values())

    def _hlg_signal_window(self, verb, s, args):
        floor = self.now() - timedelta(seconds=180)
        return any(sg['created_at'] > floor for sg in self.signals.values())

    def _hlg_close_busy(self, verb, s, args):
        """T1 CLOSE (19.6): a pending close job, or one done within the window in the SQL (CloseDB holds jobs)."""
        uid = args[0]
        window = int(re.search(r"INTERVAL '(\d+) seconds'", s).group(1))
        floor = self.now() - timedelta(seconds=window)
        return any(j['user_id'] == uid and (j['status'] == 'pending' or (j['status'] == 'done'
                                                                           and j['updated_at'] > floor))
                   for j in getattr(self, 'close_jobs', {}).values())

    def _guarded(self, rid, statuses, **changes):
        r = self.positions.get(rid)
        if r is None or r['status'] not in statuses:
            return 'UPDATE 0'
        r.update(changes)
        return 'UPDATE 1'

    def _hlg_row_open(self, verb, s, args):
        rid, qty, entry_px = args
        r = self.positions.get(rid)
        return self._guarded(rid, ('needs_recovery',), status='open', quantity=qty,
                             entry_fill_price=entry_px if entry_px is not None else (r or {}).get('entry_fill_price'))

    def _hlg_row_status(self, verb, s, args):
        return self._guarded(args[0], ('needs_recovery',), status=args[1], last_fill_at=self.now())

    def _hlg_row_sl(self, verb, s, args):
        return self._guarded(args[0], ACTIVE, sl_order_id=args[1])

    def _hlg_row_tp(self, verb, s, args):
        return self._guarded(args[0], ACTIVE, tp_order_id=args[1])


class FakeConn:
    def __init__(self, db):
        self.db = db

    async def execute(self, sql, *args):
        return self.db.run('execute', sql, args)

    async def fetch(self, sql, *args):
        return self.db.run('fetch', sql, args)

    async def fetchrow(self, sql, *args):
        rows = self.db.run('fetch', sql, args)
        return rows[0] if rows else None

    async def fetchval(self, sql, *args):
        return self.db.run('fetchval', sql, args)


class FakePool:
    def __init__(self, db=None):
        self.db = db if db is not None else FakeDB()

    @asynccontextmanager
    async def acquire(self):
        yield FakeConn(self.db)
