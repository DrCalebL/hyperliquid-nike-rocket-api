"""
In-memory close-path tables for the T1 CLOSE tests (spec massive-rocket-algos 2026-10-06-t1-live-design.md 19.6/19.7).

CloseDB extends fake_pool.FakeDB with master_closes / close_jobs and routes the close-path statements, which carry a
'/* t1c:<name> */' tag (hosted_trading_loop execute_close, position_monitor's exit-hint read), plus the untagged
position_monitor statements a recorded close runs (INSERT INTO trades, the follower_users stats UPDATE, the
'closed' UPDATE). Everything else goes to FakeDB unchanged, so unknown SQL still fails loudly.
"""
import re

from tests.fakes.fake_pool import FakeDB

OPEN_JOB = ('pending', 'deferred')
ACTIVE = ('open', 'needs_recovery')


class CloseDB(FakeDB):
    def __init__(self):
        super().__init__()
        self.master_closes = {}        # master_trade_id -> row
        self.close_jobs = {}           # position_id -> row
        self.user_stats = []           # (user_id, profit) of the monitor's follower_users UPDATE

    # ---------- test helpers ----------
    def add_master_close(self, master_trade_id, side='BUY', symbol='ADA/USDT', reason='time_exit'):
        mc = {'master_trade_id': master_trade_id, 'symbol': symbol, 'side': side, 'reason': reason,
              'master_closed_ms': 1_790_000_000_000, 'received_at': self.now(), 'matched': 0}
        self.master_closes[master_trade_id] = mc
        return mc

    def add_job(self, position_id, user_id, master_trade_id, status='pending', attempts=0, created_at=None):
        job = {'position_id': position_id, 'user_id': user_id, 'master_trade_id': master_trade_id,
               'status': status, 'attempts': attempts, 'last_error': None,
               'created_at': created_at or self.now(), 'updated_at': created_at or self.now()}
        self.close_jobs[position_id] = job
        return job

    def job(self, position_id):
        return self.close_jobs[position_id]

    # ---------- routing ----------
    def run(self, verb, sql, args):
        s = ' '.join(sql.split())
        tag = re.match(r'/\* t1c:(\w+) \*/', s)
        monitor = next((h for needle, h in (
            ('INSERT INTO trades', self._insert_trade),
            ('UPDATE follower_users SET total_trades', self._user_stats),
            ("UPDATE open_positions SET status = 'closed'", self._mark_closed)) if needle in s), None)
        if not tag and monitor is None:
            return super().run(verb, sql, args)
        self.log.append((verb, s, args))
        for i, (needle, exc) in enumerate(self.fail_next):
            if needle in s:
                del self.fail_next[i]
                raise exc
        if tag:
            return getattr(self, '_t1c_' + tag.group(1))(verb, s, args)
        return monitor(verb, s, args)

    # ---------- hosted_trading_loop close path ----------
    def _t1c_jobs(self, verb, s, args):
        out = []
        for job in sorted(self.close_jobs.values(), key=lambda j: j['created_at']):
            if job['status'] not in OPEN_JOB:
                continue
            mc = self.master_closes[job['master_trade_id']]
            u = self.users.get(job['user_id'], {})
            out.append({'position_id': job['position_id'], 'user_id': job['user_id'],
                        'master_trade_id': job['master_trade_id'], 'job_status': job['status'],
                        'attempts': job['attempts'],
                        'age_s': (self.now() - job['created_at']).total_seconds(),
                        'payload_side': mc['side'], 'payload_symbol': mc['symbol'],
                        'api_key': u.get('api_key'), 'email': u.get('email'),
                        'hl_private_key_encrypted': u.get('hl_private_key_encrypted'),
                        'hl_wallet_address': u.get('hl_wallet_address'), 'agent_active': u.get('agent_active'),
                        'access_granted': u.get('access_granted'), 'credentials_set': u.get('credentials_set'),
                        'pending_invoice_id': u.get('pending_invoice_id'),
                        'invoice_due_date': u.get('invoice_due_date')})
        return out

    def _t1c_row(self, verb, s, args):
        r = self.positions.get(args[0])
        return [dict(r)] if r is not None else []

    def _t1c_job_set(self, verb, s, args):
        pid, status, attempts, error = args
        job = self.close_jobs.get(pid)
        if job is None or job['status'] not in OPEN_JOB:
            return 'UPDATE 0'
        job.update(status=status, attempts=attempts, last_error=error, updated_at=self.now())
        return 'UPDATE 1'

    def _t1c_hint(self, verb, s, args):
        r = self.positions.get(args[0])
        if r is None or r['status'] not in ACTIVE:
            return 'UPDATE 0'
        r['exit_hint'] = 'TIME'
        return 'UPDATE 1'

    def _t1c_fill(self, verb, s, args):
        r = self.positions.get(args[0])
        if r is None:
            return 'UPDATE 0'
        r['close_fill_qty'] = (r.get('close_fill_qty') or 0) + args[1]
        return 'UPDATE 1'

    # ---------- position_monitor ----------
    def _t1c_exit_hint(self, verb, s, args):
        r = self.positions.get(args[0])
        return [{'exit_hint': r.get('exit_hint'), 'close_fill_qty': r.get('close_fill_qty')}] if r else []

    def _insert_trade(self, verb, s, args):
        cols = ('user_id', 'signal_id', 'trade_id', 'hl_order_id', 'opened_at', 'closed_at', 'symbol', 'side',
                'entry_price', 'exit_price', 'position_size', 'leverage', 'profit_usd', 'profit_percent',
                'exit_type', 'fee_charged', 'notes')
        self.trades.append(dict(zip(cols, args)))
        return 'INSERT 0 1'

    def _user_stats(self, verb, s, args):
        self.user_stats.append((args[1], args[0]))
        return 'UPDATE 1'

    def _mark_closed(self, verb, s, args):
        r = self.positions.get(args[0])
        if r is not None:
            r['status'], r['last_fill_at'] = 'closed', self.now()
        return 'UPDATE %d' % (r is not None)
