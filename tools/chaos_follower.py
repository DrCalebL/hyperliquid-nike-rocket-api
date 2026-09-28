"""
tools/chaos_follower.py - OWNER-RUN ONLY: Hyperliquid TESTNET chaos for the HL follower guardian
(Wave 1: spec C47, T30, AC15). Never part of the automated gate (tools/run_gates.py only compiles it).

Drives the real HostedTradingLoop.execute_trade and HLPositionGuardian (GUARDIAN_MODE=enforce, real cadence:
recovery lane every 30 s, full pass every 240 s) against Hyperliquid TESTNET, with an in-memory FakePool
(tests/fakes/fake_pool.py) instead of Postgres. Each run opens one small position through execute_trade,
injects one fault, then polls the exchange every 5 s:
  a  cancel the TP                                   -> the TP is re-placed
  b  cancel the SL                                   -> the SL is re-placed
  c  cancel both                                     -> SL, then TP
  d  row SL moved beyond the mark, then SL cancelled -> the position is closed (SL_MISSING_PRICE_BEYOND)
  e  fill confirmation withheld                      -> needs_recovery row -> the recovery lane brackets it
PASS per case: every run restored within 330 s (d: flat within 330 s), exchange-truth SL-absent <= 330 s,
and zero closes of a position whose SL and TP were both live.

Interlocks (I14): --i-accept-testnet-orders; ROCKET_TESTNET=1; FOLLOWER_API_URL and MASTER_API_KEY must be
unset; keys only from CHAOS_HL_TESTNET_PRIVATE_KEY (+ optional CHAOS_HL_TESTNET_ACCOUNT), never printed;
USE_TESTNET forced before any app import and asserted on both clients; socket allowlist (testnet host only);
CHAOS_MAX_NOTIONAL (default 50 USD) sizes every entry; flat preflight; temp cwd; notify_admin prints;
cleanup (cancel the coin's orders, reduce-only close) after every run and on Ctrl-C.
Outputs chaos_out/<UTC>/runs.csv, summary.json, shapes.json.

    set ROCKET_TESTNET=1
    set CHAOS_HL_TESTNET_PRIVATE_KEY=<testnet API-wallet key>
    python tools/chaos_follower.py --cases a,b,c,d,e --runs 20 --i-accept-testnet-orders
"""
import argparse
import asyncio
import csv
import json
import os
import socket
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTNET_HOSTS = ('api.hyperliquid-testnet.xyz',)
BOUND_S = 330.0
POLL_S = 5.0
SL_PCT, TP_PCT, RISK = 0.03, 0.03, 0.01


def refuse(msg):
    print('refusing: ' + msg)
    sys.exit(2)


def check_interlocks(args):
    if not args.i_accept_testnet_orders:
        refuse('pass --i-accept-testnet-orders (this places real TESTNET orders)')
    if os.getenv('ROCKET_TESTNET') != '1':
        refuse('set ROCKET_TESTNET=1')
    for name in ('FOLLOWER_API_URL', 'MASTER_API_KEY'):
        if os.getenv(name):
            refuse('%s is set: chaos runs must never reach production followers' % name)
    if not os.getenv('CHAOS_HL_TESTNET_PRIVATE_KEY'):
        refuse('set CHAOS_HL_TESTNET_PRIVATE_KEY (a TESTNET API-wallet key)')


def allow_only_testnet_sockets():
    real = socket.getaddrinfo

    def guarded(host, *a, **k):
        if host not in TESTNET_HOSTS:
            raise ConnectionRefusedError('chaos socket allowlist: %r is not a testnet host' % host)
        return real(host, *a, **k)
    socket.getaddrinfo = guarded


def legs_state(bg, raw, coin, row, pos, tick):
    """(sl_live, tp_live) for the row's recorded levels on the exit side."""
    if pos is None:
        return False, False
    exit_side = 'sell' if pos > 0 else 'buy'
    legs = [bg.leg_from_hl(o) for o in raw if o.get('coin') == coin]
    tol = tick * bg.TICK_TOL
    sl = any(l.side == exit_side and l.kind == bg.STOP_MARKET and l.level is not None
             and abs(l.level - float(row['target_sl'])) <= tol for l in legs)
    tp = any(l.side == exit_side and l.kind in bg.TP_KINDS and l.level is not None
             and abs(l.level - float(row['target_tp'])) <= tol for l in legs)
    return sl, tp


async def campaign(args, out_dir):
    os.environ['USE_TESTNET'] = 'true'
    os.environ['GUARDIAN_MODE'] = 'enforce'
    from cryptography.fernet import Fernet
    throwaway = Fernet.generate_key()
    os.environ['CREDENTIALS_ENCRYPTION_KEY'] = throwaway.decode()    # in-process only
    sys.path.insert(0, ROOT)
    import bracket_guard as bg
    import hosted_trading_loop as htl
    import order_utils
    import position_guardian as pg
    from eth_account import Account
    from tests.fakes.fake_pool import FakeDB, FakePool

    async def printed(title, details, level='warning'):
        print('[notify_admin %s] %s' % (level, title))
        return True
    for mod in (order_utils, htl, pg):
        mod.notify_admin = printed

    pk = os.environ['CHAOS_HL_TESTNET_PRIVATE_KEY']
    wallet = os.getenv('CHAOS_HL_TESTNET_ACCOUNT') or Account.from_key(pk).address
    db = FakeDB()
    db.add_user(1, hl_private_key_encrypted=Fernet(throwaway).encrypt(pk.encode()).decode(), hl_wallet_address=wallet)
    user = {k: db.users[1][k] for k in ('id', 'api_key', 'email', 'hl_private_key_encrypted', 'hl_wallet_address')}
    pool = FakePool(db)
    loop = htl.HostedTradingLoop(pool)
    guardian = pg.HLPositionGuardian(pool)
    if not (loop.base_url == guardian.base_url == htl.constants.TESTNET_API_URL):
        refuse('clients are not on the testnet URL')
    info, exchange, _ = loop.get_or_create_exchange(user)
    coin = args.coin

    def book():
        return info.frontend_open_orders(wallet), htl.coin_szi(info.user_state(wallet), coin)

    raw, szi = book()
    if szi != 0 or [o for o in raw if o.get('coin') == coin]:
        refuse('the chaos account must be flat with no %s orders before the campaign' % coin)

    async def cleanup():
        for r in db.positions.values():
            r['status'] = 'closed'                           # rowless first: the guardian stops repairing
        for _ in range(3):
            for o in [o for o in info.frontend_open_orders(wallet) if o.get('coin') == coin]:
                await order_utils.cancel_order_verified(exchange, coin, o['oid'], 'chaos cleanup')
            s = htl.coin_szi(info.user_state(wallet), coin)
            if s:
                await htl.hl_close_with_verification(exchange, info, wallet, coin, s < 0, abs(s), loop.round_price)
            await asyncio.sleep(2)
            if not htl.coin_szi(info.user_state(wallet), coin) and not [
                    o for o in info.frontend_open_orders(wallet) if o.get('coin') == coin]:
                return
        print('WARNING: cleanup left orders or a position on %s - check the testnet account' % coin)

    shapes, results = {}, []
    tasks = [asyncio.ensure_future(guardian._lane_loop()), asyncio.ensure_future(guardian._pass_loop())]
    try:
        for case in args.cases:
            for run_no in range(1, args.runs + 1):
                results.append(await one_run(args, case, run_no, bg, htl, loop, user, db, info, exchange, wallet,
                                             coin, book, shapes, legs_state))
                await cleanup()
                print('case %s run %d: %s' % (case, run_no, results[-1]))
    finally:
        for t in tasks:
            t.cancel()
        await cleanup()
    write_outputs(out_dir, results, shapes)
    return results


async def one_run(args, case, run_no, bg, htl, loop, user, db, info, exchange, wallet, coin, book, shapes, legs_state):
    mid = float(info.all_mids()[coin])
    sl, tp = mid * (1 - SL_PCT), mid * (1 + TP_PCT)
    equity = args.max_notional * (mid - sl) / (RISK * mid)             # entry notional ~= CHAOS_MAX_NOTIONAL

    async def capped_equity(*a, **k):
        return equity
    loop.get_user_equity = capped_equity
    if case == 'e':
        async def withheld(*a, **k):
            return None, None
        loop._confirm_entry_fill = withheld
    sig = db.add_signal('chaos-%s-%d-%d' % (case, run_no, int(time.time())),
                        created_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1))
    signal = {'delivery_id': None, 'signal_id': sig['signal_id'], 'action': 'BUY', 'symbol': coin + '/USDT',
              'entry_price': mid, 'stop_loss': sl, 'take_profit': tp, 'leverage': 3.0, 'risk_pct': RISK}
    await loop.execute_trade(user, signal)
    loop.__dict__.pop('_confirm_entry_fill', None)
    row = next((r for r in db.positions.values() if r['status'] in ('open', 'needs_recovery')), None)
    if row is None:
        return {'case': case, 'run': run_no, 'restored': False, 't_restore_s': None, 'max_sl_absent_s': None,
                'false_close': False, 'note': 'no position opened'}
    raw, _ = book()
    shapes.setdefault('frontend_open_orders', [o for o in raw if o.get('coin') == coin][:4])
    oids = {k: row.get(k) for k in ('sl_order_id', 'tp_order_id')}
    if case in ('a', 'c') and oids['tp_order_id']:
        exchange.cancel(coin, int(oids['tp_order_id']))
    if case == 'd':
        meta, ctxs = info.meta_and_asset_ctxs()
        mark = next(float(c['markPx']) for a, c in zip(meta['universe'], ctxs) if a['name'] == coin)
        row['target_sl'] = mark * 1.01                        # the recorded SL is now beyond the mark
    if case in ('b', 'c', 'd') and oids['sl_order_id']:
        exchange.cancel(coin, int(oids['sl_order_id']))
    tick = bg.hl_tick(mid, loop.asset_meta.get(coin, {}).get('szDecimals', 2))
    t0 = time.monotonic()
    last_sl_seen, max_absent, restored_at, false_close, bracketed = t0, 0.0, None, False, False
    while time.monotonic() - t0 <= BOUND_S + args.watch_s:
        await asyncio.sleep(POLL_S)
        raw, szi = book()
        now = time.monotonic()
        if szi == 0:
            if case == 'd' and restored_at is None:
                restored_at = now                            # d: closed by the guardian
            elif bracketed:
                false_close = True                           # a position with SL + TP live was closed
            break
        sl_live, tp_live = legs_state(bg, raw, coin, row, szi, tick)
        if sl_live:
            last_sl_seen = now
        max_absent = max(max_absent, now - last_sl_seen)
        bracketed = sl_live and tp_live
        if bracketed and case != 'd' and restored_at is None:
            restored_at = now                                # keep watching args.watch_s for a false close
        if restored_at is not None and now - restored_at >= args.watch_s:
            break
    restored = restored_at is not None and restored_at - t0 <= BOUND_S
    return {'case': case, 'run': run_no, 'restored': bool(restored),
            't_restore_s': round(restored_at - t0, 1) if restored_at is not None else None,
            'max_sl_absent_s': round(max_absent, 1), 'false_close': false_close, 'note': ''}


def write_outputs(out_dir, results, shapes):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, 'runs.csv'), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['case', 'run', 'restored', 't_restore_s', 'max_sl_absent_s',
                                          'false_close', 'note'])
        w.writeheader()
        w.writerows(results)
    summary = {}
    for case in sorted({r['case'] for r in results}):
        rs = [r for r in results if r['case'] == case]
        summary[case] = {
            'runs': len(rs), 'restored': sum(r['restored'] for r in rs),
            'max_t_restore_s': max((r['t_restore_s'] or 0) for r in rs),
            'max_sl_absent_s': max((r['max_sl_absent_s'] or 0) for r in rs),
            'false_closes': sum(r['false_close'] for r in rs),
            'pass': all(r['restored'] and (r['max_sl_absent_s'] or 0) <= BOUND_S and not r['false_close']
                        for r in rs)}
    summary['PASS'] = bool(results) and all(v['pass'] for k, v in summary.items() if k != 'PASS')
    with open(os.path.join(out_dir, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    with open(os.path.join(out_dir, 'shapes.json'), 'w', encoding='utf-8') as f:
        json.dump(shapes, f, indent=2, default=str)
    print(json.dumps(summary, indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    parser.add_argument('--cases', default='a,b,c,d,e')
    parser.add_argument('--runs', type=int, default=20)
    parser.add_argument('--coin', default='ADA')
    parser.add_argument('--max-notional', type=float, default=float(os.getenv('CHAOS_MAX_NOTIONAL', '50')))
    parser.add_argument('--out', default=os.path.join(ROOT, 'chaos_out'))
    parser.add_argument('--watch-s', type=float, default=60.0, help='keep watching after a restore (false closes)')
    parser.add_argument('--i-accept-testnet-orders', action='store_true')
    args = parser.parse_args(argv)
    args.cases = [c.strip() for c in args.cases.split(',') if c.strip()]
    if set(args.cases) - set('abcde'):
        refuse('unknown case in %s' % args.cases)
    check_interlocks(args)
    allow_only_testnet_sockets()
    out_dir = os.path.join(args.out, time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()))
    os.chdir(tempfile.mkdtemp(prefix='chaos_follower_'))
    results = asyncio.run(campaign(args, out_dir))
    return 0 if results and all(r['restored'] and not r['false_close'] for r in results) else 1


if __name__ == '__main__':
    sys.exit(main())
