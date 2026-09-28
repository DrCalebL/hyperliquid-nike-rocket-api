"""T01 test_bracket_guard_decide_matrix_parsers_and_churn (REQUIRED).

Byte-identical in massive-rocket-algos/tests, kraken-nike-rocket-api/tests and hyperliquid-nike-rocket-api/tests
(stdlib unittest, so `python -m unittest` in the master and pytest in the followers both run it). Imports only
bracket_guard. Covers: the pinned API, the venue parsers on captured shapes, every bg.decide() rule (rev 2 + rev 3),
the A10 level policy (masters alarm-only, followers replace), ChurnGuard, and a 5,000-state x 3-pass property fuzz.
"""
import dataclasses
import inspect
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bracket_guard as bg  # noqa: E402
from bracket_guard import Action, Leg, Pos, Rec  # noqa: E402

TICK, LOT, NOW = 0.0001, 1.0, 1_000_000.0
REF = {'last': 0.47, 'mark': 0.47}
BOTH = (bg.LEVEL_ALARM, bg.LEVEL_REPLACE)


def kinds(acts):
    return ['%s:%s' % (a.kind, a.code) if a.code else a.kind for a in acts]


def P(side=1, qty=800.0, entry=0.47, opened=None):
    return Pos(side, qty, entry, opened)


def R(side=1, sl=None, tp='default', basis='last', tp_kind=bg.TP_LIMIT, **kw):
    if sl is None:
        sl = 0.46 if side > 0 else 0.48
    if tp == 'default':
        tp = 0.49 if side > 0 else 0.45
    return Rec(side, sl, tp, basis, tp_kind, **kw)


def SL(oid='sl1', trigger=0.46, qty=800.0, side='sell', kind=bg.STOP_MARKET, **kw):
    kw.setdefault('reduce_only', True)
    return Leg(oid, side, kind, qty, trigger=trigger, **kw)


def TP(oid='tp1', limit=0.49, qty=800.0, side='sell', kind=bg.TP_LIMIT, **kw):
    kw.setdefault('reduce_only', True)
    return Leg(oid, side, kind, qty, limit=limit, **kw)


def plan(pos, rec, legs, ref=REF, now=NOW, policy=bg.LEVEL_ALARM, **kw):
    return bg.decide(pos, rec, legs, ref, TICK, LOT, now, level_policy=policy, **kw)


class TestApiSurface(unittest.TestCase):
    def test_constants(self):
        self.assertEqual((bg.ENTRY_GRACE_S, bg.FLAT_CONFIRM_S, bg.FLAT_PERSIST_S, bg.PENDING_TTL_S), (90, 5, 240, 120))
        self.assertEqual((bg.TICK_TOL, bg.SL_PLACE_ATTEMPTS, bg.CHURN_WINDOW_S), (1.01, 3, 1800))
        self.assertEqual((bg.SL_MAX_REPAIRS, bg.TP_MAX_REPAIRS, bg.TP_SUSPEND_S), (3, 3, 3600))
        self.assertEqual(bg.TP_BACKOFF_S, (300, 600, 1200, 2400, 3600))
        self.assertEqual((bg.STOP_MARKET, bg.STOP_LIMIT, bg.TP_LIMIT, bg.TP_TRIGGER_LIMIT, bg.TP_TRIGGER_MARKET,
                          bg.OTHER),
                         ('STOP_MARKET', 'STOP_LIMIT', 'TP_LIMIT', 'TP_TRIGGER_LIMIT', 'TP_TRIGGER_MARKET', 'OTHER'))
        self.assertEqual(bg.SL_KINDS, ('STOP_MARKET', 'STOP_LIMIT'))
        self.assertEqual(bg.TP_KINDS, ('TP_LIMIT', 'TP_TRIGGER_LIMIT', 'TP_TRIGGER_MARKET'))
        self.assertEqual(repr(bg.UNKNOWN), 'UNKNOWN')
        self.assertEqual((bg.LEVEL_ALARM, bg.LEVEL_REPLACE, bg.LEVEL_POLICIES),
                         ('alarm', 'replace', ('alarm', 'replace')))

    def test_types(self):
        fields = lambda cls: [(f.name, f.default) for f in dataclasses.fields(cls)]  # noqa: E731
        miss = dataclasses.MISSING
        self.assertEqual(fields(Pos), [('side', miss), ('qty', miss), ('entry_px', None), ('opened_ts', None)])
        self.assertEqual(fields(Leg), [('oid', miss), ('side', miss), ('kind', miss), ('qty', miss), ('trigger', None),
                                       ('limit', None), ('reduce_only', False), ('close_position', False),
                                       ('position_level', False), ('basis', None), ('src', '')])
        self.assertEqual(fields(Rec), [('side', miss), ('sl', miss), ('tp', miss), ('sl_basis', miss),
                                       ('tp_kind', miss), ('status', 'active'), ('sl_oid', None),
                                       ('sl_oid_trusted', False), ('tp_oid', None), ('created_ts', None)])
        self.assertEqual([f.name for f in dataclasses.fields(Action)],
                         ['kind', 'code', 'severity', 'level', 'qty', 'leg', 'ctx'])
        a, b = Action('ALARM'), Action('ALARM')
        a.ctx['x'] = 1
        self.assertEqual((a.code, a.severity, b.ctx), ('', 'warning', {}))
        self.assertEqual(Leg('1', 'sell', bg.STOP_MARKET, 1.0, trigger=0.4, limit=0.3).level, 0.4)
        self.assertEqual(Leg('1', 'sell', bg.TP_LIMIT, 1.0, limit=0.5).level, 0.5)

    def test_trusted_sl_oid(self):
        self.assertEqual(bg.trusted_sl_oid(R(sl_oid=123, sl_oid_trusted=True)), '123')
        self.assertIsNone(bg.trusted_sl_oid(R(sl_oid='x', sl_oid_trusted=False)))
        self.assertIsNone(bg.trusted_sl_oid(R(sl_oid=None, sl_oid_trusted=True)))
        self.assertIsNone(bg.trusted_sl_oid(None))


class TestParsers(unittest.TestCase):
    def test_binance_algo_shapes(self):
        rows = [{'algoId': 1}]
        self.assertIs(bg.parse_binance_algo_response(rows), rows)
        self.assertEqual(bg.parse_binance_algo_response({'orders': rows}), rows)
        for bad in ({'data': rows}, {'orders': None}, None, 'x', 3):
            self.assertIs(bg.parse_binance_algo_response(bad), bg.UNKNOWN)

    def test_binance_algo_leg(self):
        a = {'algoId': 111, 'algoType': 'CONDITIONAL', 'orderType': 'STOP_MARKET', 'symbol': 'ADAUSDT', 'side': 'SELL',
             'quantity': '800', 'triggerPrice': '0.4600', 'price': '0', 'reduceOnly': True, 'closePosition': False,
             'workingType': 'CONTRACT_PRICE'}
        self.assertEqual(bg.leg_from_binance_algo(a),
                         Leg('111', 'sell', bg.STOP_MARKET, 800.0, trigger=0.46, limit=None, reduce_only=True,
                             close_position=False, basis='last', src='algo'))
        cp = bg.leg_from_binance_algo(dict(a, closePosition='true', workingType='MARK_PRICE',
                                           orderType='TAKE_PROFIT_MARKET'))
        self.assertEqual((cp.qty, cp.close_position, cp.basis, cp.kind), (None, True, 'mark', bg.TP_TRIGGER_MARKET))
        self.assertEqual(bg.leg_from_binance_algo(dict(a, orderType='STOP')).kind, bg.STOP_LIMIT)
        self.assertIsNone(bg.leg_from_binance_algo(dict(a, algoType='TWAP')))

    def test_binance_regular_leg(self):
        o = {'id': '222', 'type': 'limit', 'side': 'sell', 'price': 0.49, 'amount': 800, 'remaining': 800,
             'reduceOnly': True, 'triggerPrice': None}
        self.assertEqual(bg.leg_from_binance_regular(o),
                         Leg('222', 'sell', bg.TP_LIMIT, 800.0, limit=0.49, reduce_only=True, basis='last',
                             src='regular'))
        other = bg.leg_from_binance_regular(dict(o, reduceOnly=False, remaining=None, amount=5))
        self.assertEqual((other.kind, other.reduce_only, other.qty), (bg.OTHER, False, 5.0))

    def test_kraken_open_order_rows(self):
        stop = {'id': 'abc', 'symbol': 'ADA/USD:USD', 'type': 'stop', 'side': 'sell', 'amount': 800.0,
                'remaining': 800.0, 'price': None, 'triggerPrice': None, 'reduceOnly': True,
                'info': {'order_id': 'abc', 'symbol': 'PF_ADAUSD', 'side': 'sell', 'orderType': 'stop',
                         'stopPrice': 0.46, 'unfilledSize': 800, 'reduceOnly': True, 'triggerSignal': 'last'}}
        self.assertEqual(bg.leg_from_kraken(stop),
                         Leg('abc', 'sell', bg.STOP_MARKET, 800.0, trigger=0.46, limit=None, reduce_only=True,
                             basis='last'))
        legacy = bg.leg_from_kraken(dict(stop, info=dict(stop['info'], limitPrice=0.46, triggerSignal='mark')))
        self.assertEqual((legacy.kind, legacy.limit, legacy.basis), (bg.STOP_LIMIT, 0.46, 'mark'))
        tp = bg.leg_from_kraken({'id': 't', 'type': 'limit', 'side': 'sell', 'amount': 800.0, 'remaining': None,
                                 'price': 0.49, 'reduceOnly': True,
                                 'info': {'orderType': 'lmt', 'limitPrice': 0.49, 'unfilledSize': 500}})
        self.assertEqual((tp.kind, tp.level, tp.qty), (bg.TP_LIMIT, 0.49, 500.0))
        tpt = bg.leg_from_kraken({'id': 'u', 'side': 'sell', 'info': {'orderType': 'take_profit', 'stopPrice': 0.5,
                                                                        'triggerSignal': 'spot'}})
        self.assertEqual((tpt.kind, tpt.trigger, tpt.basis), (bg.TP_TRIGGER_MARKET, 0.5, 'index'))
        self.assertTrue(bg.kraken_order_matches(stop, 'ADA/USD:USD', 'PF_ADAUSD'))
        self.assertTrue(bg.kraken_order_matches(dict(stop, symbol=None), 'ADA/USD:USD', 'pf_adausd'))
        self.assertFalse(bg.kraken_order_matches(dict(stop, symbol='ETH/USD:USD', info={'symbol': 'PF_ETHUSD'}),
                                                 'ADA/USD:USD', 'PF_ADAUSD'))

    def test_hl_frontend_rows(self):
        row = {'coin': 'ADA', 'side': 'A', 'limitPx': '0.46', 'sz': '800.0', 'oid': 123456789, 'isTrigger': True,
               'triggerPx': '0.46', 'isPositionTpsl': False, 'reduceOnly': True, 'orderType': 'Stop Market'}
        self.assertEqual(bg.leg_from_hl(row), Leg('123456789', 'sell', bg.STOP_MARKET, 800.0, trigger=0.46, limit=0.46,
                                                  reduce_only=True, close_position=False, basis='mark'))
        whole = bg.leg_from_hl(dict(row, sz='0.0', isPositionTpsl=True, side='A'))
        self.assertEqual((whole.qty, whole.close_position), (None, True))
        self.assertEqual(bg.leg_from_hl(dict(row, isPositionTpsl=False, sz='0.0')).qty, None)
        tp = bg.leg_from_hl(dict(row, side='B', orderType='Take Profit Limit', triggerPx='0.45', limitPx='0.45'))
        self.assertEqual((tp.side, tp.kind, tp.trigger), ('buy', bg.TP_TRIGGER_LIMIT, 0.45))
        plain = bg.leg_from_hl(dict(row, orderType='Limit', isTrigger=False, triggerPx='0.0', limitPx='0.49'))
        self.assertEqual((plain.kind, plain.trigger, plain.level), (bg.TP_LIMIT, None, 0.49))

    def test_hl_tick(self):
        self.assertAlmostEqual(bg.hl_tick(0.47, 0), 1e-5)
        self.assertAlmostEqual(bg.hl_tick(65000.0, 5), 1.0)
        self.assertAlmostEqual(bg.hl_tick(0, 0), 1e-6)
        self.assertAlmostEqual(bg.hl_tick(None, 2), 1e-4)

    def test_phemex_order_and_position_legs(self):
        o = {'id': 'p1', 'side': 'sell', 'amount': 800, 'remaining': 800, 'price': None, 'triggerPrice': 0.46,
             'reduceOnly': False, 'info': {'ordType': 'Stop', 'stopPxRp': '0.46', 'trigger': 'ByMarkPrice',
                                           'closeOnTrigger': True}}
        self.assertEqual(bg.leg_from_phemex(o), Leg('p1', 'sell', bg.STOP_MARKET, 800.0, trigger=0.46, limit=None,
                                                    reduce_only=True, basis='mark'))
        lit = bg.leg_from_phemex(dict(o, triggerPrice=None, info={'ordType': 'LimitIfTouched', 'stopPxRp': '0.49',
                                                                  'trigger': 'ByLastPrice', 'reduceOnly': True}))
        self.assertEqual((lit.kind, lit.trigger, lit.basis), (bg.TP_TRIGGER_LIMIT, 0.49, 'last'))
        self.assertEqual(bg.leg_from_phemex(dict(o, info={'orderType': '4'})).kind, bg.STOP_LIMIT)
        self.assertIsNone(bg.leg_from_phemex(dict(o, info={})).basis)
        legs = bg.legs_from_phemex_position({'info': {'stopLossRp': '0.46', 'takeProfitRp': '0.49'}}, 'sell')
        self.assertEqual([(l.oid, l.kind, l.trigger, l.position_level, l.close_position, l.qty) for l in legs],
                         [('pos:sl', bg.STOP_MARKET, 0.46, True, True, None),
                          ('pos:tp', bg.TP_TRIGGER_MARKET, 0.49, True, True, None)])
        self.assertEqual(bg.legs_from_phemex_position({'info': {'stopLossRp': '0', 'takeProfitRp': None}}, 'sell'), [])

    def test_beyond_sl_long_short(self):
        self.assertTrue(bg.beyond_sl(1, 0.46, 0.46))
        self.assertFalse(bg.beyond_sl(1, 0.4601, 0.46))
        self.assertTrue(bg.beyond_sl(-1, 0.48, 0.48))
        self.assertFalse(bg.beyond_sl(-1, 0.4799, 0.48))


class TestDecideApi(unittest.TestCase):
    def test_decide_signature(self):
        d = inspect.signature(bg.decide).parameters
        self.assertEqual(list(d), ['pos', 'rec', 'legs', 'ref', 'tick', 'lot', 'now', 'flat_confirmed', 'flat_since',
                                   'in_flight', 'level_policy'])
        self.assertEqual((d['flat_confirmed'].default, d['flat_since'].default, d['in_flight'].default,
                          d['level_policy'].default), (False, None, False, 'alarm'))

    def test_unknown_level_policy_rejected(self):
        with self.assertRaises(ValueError):
            plan(P(), R(), [], policy='revert')


class TestDecideReadsAndFlat(unittest.TestCase):
    def test_unknown_reads_give_zero_order_actions(self):
        for pos, legs in ((bg.UNKNOWN, [SL()]), (P(), bg.UNKNOWN), (None, bg.UNKNOWN)):
            self.assertEqual(kinds(plan(pos, R(), legs)), ['ALARM:GUARDIAN_READ_UNKNOWN'])

    def test_in_flight_beats_the_flat_branch(self):
        self.assertEqual(plan(None, R(), [SL(), TP()], flat_confirmed=True, in_flight=True), [])
        self.assertEqual(plan(P(), R(), [], in_flight=True), [])

    def test_flat_without_record_or_legs_does_nothing(self):
        self.assertEqual(plan(None, None, []), [])
        self.assertEqual(plan(None, None, [Leg('n', 'buy', bg.TP_LIMIT, 5.0, limit=0.4)]), [])   # not reduce-only

    def test_flat_confirm_then_cleanup(self):
        legs = [TP(), Leg('cust', 'buy', bg.OTHER, 5.0, limit=0.4)]
        self.assertEqual(kinds(plan(None, R(), legs)), ['CONFIRM_FLAT'])
        acts = plan(None, R(), legs, flat_confirmed=True, flat_since=NOW)
        self.assertEqual(kinds(acts), ['CANCEL:ORPHAN_LEG', 'CLEAR_RECORD:FLAT'])
        self.assertEqual(acts[0].leg.oid, 'tp1')
        self.assertEqual(kinds(plan(None, None, [TP()], flat_confirmed=True)), ['CANCEL:ORPHAN_LEG'])

    def test_pending_record_kept_for_120s(self):
        young = R(status='pending', created_ts=NOW - 60)
        self.assertEqual(kinds(plan(None, young, [], flat_confirmed=True)), [])
        old = R(status='pending', created_ts=NOW - 120)
        self.assertEqual(kinds(plan(None, old, [], flat_confirmed=True)), ['CLEAR_RECORD:PENDING_EXPIRED'])
        self.assertEqual(kinds(plan(None, R(status='pending'), [], flat_confirmed=True)),
                         ['CLEAR_RECORD:PENDING_EXPIRED'])
        self.assertEqual(kinds(plan(None, young, [SL()], flat_confirmed=True, flat_since=NOW - 500)),
                         ['CANCEL:ORPHAN_LEG'])

    def test_recorded_sl_resting_waits_240s_then_cleans(self):
        for policy in BOTH:
            with self.subTest(policy=policy):
                acts = plan(None, R(sl_oid='sl1', sl_oid_trusted=True), [SL()], flat_confirmed=True,
                            flat_since=NOW - 100, policy=policy)
                self.assertEqual(kinds(acts), ['ALARM:FLAT_SL_RESTING'])
                self.assertEqual(acts[0].severity, 'info')
                acts = plan(None, R(), [SL(), TP()], flat_confirmed=True, flat_since=NOW - 239, policy=policy)
                self.assertEqual(kinds(acts), ['ALARM:FLAT_WITH_INTACT_BRACKET'])
                self.assertEqual(acts[0].severity, 'warning')
                self.assertEqual(kinds(plan(None, R(), [SL()], flat_confirmed=True, flat_since=None, policy=policy)),
                                 ['ALARM:FLAT_SL_RESTING'])
                self.assertEqual(kinds(plan(None, R(), [SL(), TP()], flat_confirmed=True, flat_since=NOW - 240,
                                            policy=policy)),
                                 ['CANCEL:ORPHAN_LEG', 'CANCEL:ORPHAN_LEG', 'CLEAR_RECORD:FLAT'])
                self.assertEqual(kinds(plan(None, R(), [TP()], flat_confirmed=True, flat_since=NOW - 5, policy=policy)),
                                 ['CANCEL:ORPHAN_LEG', 'CLEAR_RECORD:FLAT'])      # after an SL exit: immediate

    def test_trusted_oid_counts_as_recorded_sl_on_flat(self):
        odd = Leg('sl9', 'sell', bg.OTHER, None, reduce_only=True)
        acts = plan(None, R(sl_oid='sl9', sl_oid_trusted=True), [odd], flat_confirmed=True, flat_since=NOW - 10,
                    policy=bg.LEVEL_REPLACE)
        self.assertEqual(kinds(acts), ['ALARM:FLAT_SL_RESTING'])

    def test_position_level_legs_never_cancelled_on_flat(self):
        legs = bg.legs_from_phemex_position({'info': {'stopLossRp': '0.46', 'takeProfitRp': '0.49'}}, 'sell')
        acts = plan(None, R(), legs, flat_confirmed=True, flat_since=NOW - 300)
        self.assertEqual(kinds(acts), ['CLEAR_RECORD:FLAT'])

    def test_decide_never_mutates_the_callers_legs(self):
        own = Leg('sl9', 'sell', bg.OTHER, 800.0, reduce_only=True)
        plan(P(), R(sl_oid='sl9', sl_oid_trusted=True), [own, TP()])
        self.assertEqual(own.kind, bg.OTHER)


class TestDecidePosition(unittest.TestCase):
    def both(self, pos, rec, legs, expected, ref=REF, **kw):
        for policy in BOTH:
            with self.subTest(policy=policy):
                self.assertEqual(kinds(plan(pos, rec, legs, ref=ref, policy=policy, **kw)), expected)

    def test_healthy_bracket(self):
        self.both(P(), R(), [SL(), TP()], [])
        self.both(P(-1), R(-1), [SL(side='buy', trigger=0.48), TP(side='buy', limit=0.45)], [])

    def test_tp_missing_places_tp_never_closes(self):
        self.both(P(), R(), [SL()], ['PLACE_TP:TP_REPAIRED'])
        acts = plan(P(), R(tp_kind=bg.TP_TRIGGER_LIMIT), [SL()])
        self.assertEqual((acts[0].level, acts[0].qty, acts[0].ctx), (0.49, 800.0, {'tp_kind': bg.TP_TRIGGER_LIMIT}))

    def test_sl_missing_inside_places_sl(self):
        self.both(P(), R(), [TP()], ['PLACE_SL:SL_REPAIRED'])
        a = plan(P(), R(), [TP()])[0]
        self.assertEqual((a.level, a.qty, a.severity, a.ctx), (0.46, 800.0, 'critical', {'basis': 'last'}))

    def test_sl_missing_beyond_on_its_own_basis_closes_only(self):
        self.both(P(), R(), [TP(), Leg('w', 'buy', bg.OTHER, 1.0, reduce_only=True)], ['CLOSE:SL_MISSING_PRICE_BEYOND'],
                  ref={'last': 0.46, 'mark': 0.47})
        self.both(P(-1), R(-1, basis='mark'), [TP(side='buy', limit=0.45)], ['CLOSE:SL_MISSING_PRICE_BEYOND'],
                  ref={'last': 0.47, 'mark': 0.4801})
        self.both(P(), R(basis='mark'), [TP()], ['PLACE_SL:SL_REPAIRED'], ref={'last': 0.45, 'mark': 0.47})
        self.both(P(), R(basis='mark'), [TP()], ['PLACE_SL:SL_REPAIRED'], ref={'last': 0.45})     # missing ref: place
        close = plan(P(), R(), [TP()], ref={'last': 0.459})[0]
        self.assertEqual((close.qty, close.ctx), (800.0, {'px': 0.459, 'basis': 'last'}))

    def test_both_missing_sl_first(self):
        self.both(P(), R(), [], ['PLACE_SL:SL_REPAIRED', 'PLACE_TP:TP_REPAIRED'])

    def test_stop_limit_is_not_protection_make_before_break(self):
        lim = SL('lim', kind=bg.STOP_LIMIT, limit=0.46)
        self.both(P(), R(), [lim, TP()], ['PLACE_SL:SL_REPAIRED', 'CANCEL:SL_EXTRA_OR_INVALID'])

    def test_short_sl_qty_is_replaced(self):
        self.both(P(), R(), [SL(qty=400.0), TP()], ['PLACE_SL:SL_REPAIRED', 'CANCEL:SL_EXTRA_OR_INVALID'])
        self.both(P(), R(), [SL(qty=799.6), TP()], [])                           # within lot/2
        self.both(P(), R(), [SL(qty=None), TP()], [])                            # whole-position leg

    def test_basis_mismatch_is_invalid(self):
        self.both(P(), R(basis='mark'), [SL(basis='last'), TP()],
                  ['PLACE_SL:SL_REPAIRED', 'CANCEL:SL_EXTRA_OR_INVALID'])
        self.both(P(), R(basis='mark'), [SL(basis=None), TP()], [])

    def test_duplicate_valid_sls_cancel_exactly_one(self):
        for policy in BOTH:
            acts = plan(P(), R(sl_oid='b', sl_oid_trusted=True), [SL('a'), SL('b'), TP()], policy=policy)
            self.assertEqual(kinds(acts), ['CANCEL:SL_EXTRA_OR_INVALID'])
            self.assertEqual(acts[0].leg.oid, 'a')                                # the trusted one is kept
            acts = plan(P(), R(), [SL('a'), SL('b', qty=900.0), TP()], policy=policy)
            self.assertEqual([a.leg.oid for a in acts], ['a'])                   # the larger one is kept

    def test_same_order_seen_twice_never_cancels_itself(self):
        self.both(P(), R(), [SL(), SL(), TP(), TP()], [])

    def test_tp_qty_short_cancel_then_place(self):
        self.both(P(), R(), [SL(), TP(qty=400.0)], ['CANCEL:TP_EXTRA_OR_INVALID', 'PLACE_TP:TP_REPAIRED'])

    def test_partial_tp_fill_no_action(self):
        self.both(P(qty=500.0), R(), [SL(qty=800.0), TP(qty=500.0)], [])

    def test_wrong_side_and_other_reduce_only_cancelled(self):
        self.both(P(), R(), [SL(), TP(), SL('w', side='buy', trigger=0.5)], ['CANCEL:FOREIGN_REDUCE_ONLY'])
        self.both(P(), R(), [SL(), TP(), Leg('o', 'sell', bg.OTHER, 800.0, trigger=0.3, reduce_only=True)],
                  ['CANCEL:FOREIGN_REDUCE_ONLY'])

    def test_non_reduce_only_orders_never_touched(self):
        self.both(P(), R(), [SL(), TP(), Leg('n', 'sell', bg.TP_LIMIT, 800.0, limit=0.52),
                             Leg('m', 'buy', bg.STOP_MARKET, 5.0, trigger=0.4)], [])

    def test_untracked_one_sl_adopted_not_cancelled(self):
        for policy in BOTH:
            acts = plan(P(), None, [SL(basis=None), TP()], policy=policy)
            self.assertEqual(kinds(acts), ['ADOPT:UNTRACKED_POSITION_ADOPTED'])
            self.assertEqual((acts[0].level, acts[0].ctx),
                             (0.46, {'tp': 0.49, 'basis': 'last', 'tp_kind': bg.TP_LIMIT, 'sl_oid': 'sl1'}))

    def test_untracked_two_sls_ambiguous(self):
        self.both(P(), None, [SL('a'), SL('b', trigger=0.45), TP()], ['ALARM:AMBIGUOUS_SL_UNTRACKED'])

    def test_untracked_naked_young_waits_old_closes(self):
        self.both(P(opened=NOW - 30), None, [TP()], ['ALARM:UNTRACKED_NAKED_YOUNG'])
        self.both(P(opened=NOW - 120), None, [TP()], ['CLOSE:NAKED_UNKNOWN_LEVEL'])
        self.both(P(opened=None), None, [], ['CLOSE:NAKED_UNKNOWN_LEVEL'])

    def test_record_side_mismatch_alarms_then_untracked_path(self):
        self.both(P(), R(-1), [SL(), TP()], ['ALARM:RECORD_SIDE_MISMATCH', 'ADOPT:UNTRACKED_POSITION_ADOPTED'])
        self.both(P(opened=NOW - 500), R(-1), [], ['ALARM:RECORD_SIDE_MISMATCH', 'CLOSE:NAKED_UNKNOWN_LEVEL'])

    def test_trusted_oid_validates_an_unparseable_leg(self):
        odd = Leg('sl9', 'sell', bg.OTHER, 800.0, reduce_only=True)
        self.both(P(), R(sl_oid='sl9', sl_oid_trusted=True), [odd, TP()], ['ALARM:LEG_UNPARSEABLE'])
        self.both(P(), R(sl_oid='sl9', sl_oid_trusted=False), [odd, TP()],
                  ['PLACE_SL:SL_REPAIRED', 'CANCEL:FOREIGN_REDUCE_ONLY'])

    def test_phemex_position_level_legs_never_cancelled(self):
        legs = bg.legs_from_phemex_position({'info': {'stopLossRp': '0.46', 'takeProfitRp': '0.49'}}, 'sell')
        self.both(P(), R(tp_kind=bg.TP_TRIGGER_MARKET), legs, [])
        wrong = bg.legs_from_phemex_position({'info': {'stopLossRp': '0.45', 'takeProfitRp': '0.49'}}, 'sell')
        acts = plan(P(), R(tp_kind=bg.TP_TRIGGER_MARKET), wrong, policy=bg.LEVEL_REPLACE)
        self.assertEqual(kinds(acts), ['PLACE_SL:SL_REPAIRED'])
        self.assertFalse(any(a.kind == 'CANCEL' for a in acts))

    def test_unknown_tp_level_alarm_only_no_tp_cancel(self):
        self.both(P(), R(tp=None), [SL(), TP(qty=400.0), TP('t2', limit=0.5)], ['ALARM:TP_LEVEL_UNKNOWN'])
        self.both(P(), R(tp=None), [SL(), TP(), SL('w', side='buy', trigger=0.5)],
                  ['ALARM:TP_LEVEL_UNKNOWN', 'CANCEL:FOREIGN_REDUCE_ONLY'])

    def test_adopt_with_two_profit_side_tps_leaves_tps_alone(self):
        for policy in BOTH:
            acts = plan(P(), None, [SL(), TP('t1'), TP('t2', limit=0.5)], policy=policy)
            self.assertEqual(kinds(acts), ['ADOPT:UNTRACKED_POSITION_ADOPTED', 'ALARM:TP_LEVEL_UNKNOWN'])
            self.assertIsNone(acts[0].ctx['tp'])

    def test_hl_position_tpsl_row_is_valid_at_the_recorded_level(self):
        row = {'side': 'A', 'sz': '0.0', 'oid': 77, 'isTrigger': True, 'triggerPx': '0.46', 'limitPx': '0.46',
               'isPositionTpsl': True, 'reduceOnly': True, 'orderType': 'Stop Market'}
        legs = [bg.leg_from_hl(row), TP()]
        self.assertEqual(plan(P(), R(basis='mark'), legs), [])
        self.assertEqual(plan(P(), R(basis='mark'), legs, policy=bg.LEVEL_REPLACE), [])


class TestChurnGuard(unittest.TestCase):
    def test_signatures(self):
        self.assertEqual(list(inspect.signature(bg.ChurnGuard.filter).parameters),
                         ['self', 'acts', 'legs', 'now', 'qty', 'rec'])
        self.assertEqual(list(inspect.signature(bg.ChurnGuard.note_placed).parameters), ['self', 'leg', 'oid', 'now'])
        self.assertEqual(list(inspect.signature(bg.ChurnGuard.note_place_failed).parameters), ['self', 'leg', 'now'])

    def test_tp_rejected_every_pass_never_gates_the_sl(self):
        cg, rec, now = bg.ChurnGuard(), R(), NOW
        for _ in range(6):
            acts, _ = cg.filter(plan(P(), rec, [SL()], now=now), [SL()], now, qty=800.0, rec=rec)
            self.assertNotIn('PLACE_SL:SL_REPAIRED', kinds(acts))
            if any(a.kind == 'PLACE_TP' for a in acts):
                cg.note_place_failed('TP', now)
            now += 300
        acts, alarms = cg.filter(plan(P(), rec, [], now=now), [], now, qty=800.0, rec=rec)
        self.assertEqual(kinds(acts), ['PLACE_SL:SL_REPAIRED'])
        self.assertEqual([a[0] for a in alarms], ['TP_REPAIR_DEFERRED'])

    def test_fourth_sl_repair_within_30_min_closes(self):
        cg = bg.ChurnGuard()
        for i, t in enumerate((NOW, NOW + 300, NOW + 600)):
            cg.note_placed('SL', 'x%d' % i, t)
        acts, alarms = cg.filter(plan(P(), R(), [TP()], now=NOW + 900), [TP()], NOW + 900, qty=800.0, rec=R())
        self.assertEqual(kinds(acts), ['CLOSE:SL_UNMAINTAINABLE'])
        self.assertEqual((acts[0].qty, acts[0].severity), (800.0, 'critical'))
        self.assertEqual(alarms, [('GUARDIAN_CHURN', 'critical', {'leg': 'SL', 'repairs_30m': 3})])
        acts, _ = cg.filter(plan(P(), R(), [TP()], now=NOW + 1801), [TP()], NOW + 1801, qty=800.0, rec=R())
        self.assertEqual(kinds(acts), ['PLACE_SL:SL_REPAIRED'])                 # oldest placement aged out

    def test_own_misparsed_sl_on_book_no_second_placement(self):
        cg = bg.ChurnGuard()
        cg.note_placed('SL', 'own', NOW - 60)
        legs = [SL('own', kind=bg.STOP_LIMIT, limit=0.46), TP()]              # our stop, misparsed as a stop-limit
        for policy in BOTH:
            acts, alarms = cg.filter(plan(P(), R(), legs, policy=policy), legs, NOW, qty=800.0, rec=R())
            self.assertEqual(acts, [])
            self.assertEqual(alarms[0], ('CLASSIFIER_SUSPECT', 'critical', {'leg': 'SL', 'oid': 'own'}))

    def test_protected_stop_misparsed_as_foreign_is_never_cancelled_without_placement(self):
        rec = R(sl_oid='s9', sl_oid_trusted=True)
        legs = [SL('s9', side='buy'), TP()]                                     # side misparsed -> FOREIGN
        raw = plan(P(), rec, legs)
        self.assertEqual(kinds(raw), ['PLACE_SL:SL_REPAIRED', 'CANCEL:FOREIGN_REDUCE_ONLY'])
        cg = bg.ChurnGuard()
        cg.note_placed('SL', 's9', NOW - 100)
        acts, alarms = cg.filter(raw, legs, NOW, qty=800.0, rec=rec)
        self.assertEqual(acts, [])
        self.assertEqual([a[0] for a in alarms], ['CLASSIFIER_SUSPECT', 'CLASSIFIER_SUSPECT'])
        acts, _ = bg.ChurnGuard().filter(raw, legs, NOW, qty=800.0, rec=rec)       # restart: no history, trusted rec
        self.assertEqual(kinds(acts), ['PLACE_SL:SL_REPAIRED', 'CANCEL:FOREIGN_REDUCE_ONLY'])
        legs2 = legs + [SL('other')]                                            # a valid SL elsewhere: no PLACE_SL
        acts, alarms = bg.ChurnGuard().filter(plan(P(), rec, legs2), legs2, NOW, qty=800.0, rec=rec)
        self.assertEqual(kinds(acts), [])
        self.assertEqual(alarms[0][2], {'leg': 'SL', 'oid': 's9', 'dropped': 'FOREIGN_REDUCE_ONLY'})

    def test_orphan_cancel_of_the_protected_stop_passes_after_the_flat_persisted(self):
        rec = R(sl_oid='s9', sl_oid_trusted=True)
        raw = plan(None, rec, [SL('s9')], flat_confirmed=True, flat_since=NOW - 300)
        acts, alarms = bg.ChurnGuard().filter(raw, [SL('s9')], NOW, rec=rec)
        self.assertEqual((kinds(acts), alarms), (['CANCEL:ORPHAN_LEG', 'CLEAR_RECORD:FLAT'], []))

    def test_tp_backoff_schedule_and_suspension(self):
        cg, t = bg.ChurnGuard(), NOW
        waits = []
        for _ in range(6):
            cg.note_place_failed('TP', t)
            waits.append(cg.tp_next_try - t)
        self.assertEqual(waits, [300, 600, 1200, 2400, 3600, 3600])
        acts, alarms = cg.filter(plan(P(), R(), [SL(), TP(qty=1.0)], now=t), [SL(), TP(qty=1.0)], t, rec=R())
        self.assertEqual((kinds(acts), alarms[0][0]), ([], 'TP_REPAIR_DEFERRED'))
        cg.note_placed('TP', 'tp-new', t)
        self.assertEqual((cg.tp_fail, cg.tp_next_try), (0, 0.0))
        cg2 = bg.ChurnGuard()
        cg2.note_placed('TP', 'mine', t - 10)
        legs = [SL(), TP('mine', qty=1.0)]                                      # own TP on the book but judged invalid
        acts, alarms = cg2.filter(plan(P(), R(), legs, now=t), legs, t, rec=R())
        self.assertEqual((kinds(acts), alarms[0][:2]), ([], ('CLASSIFIER_SUSPECT', 'critical')))
        self.assertEqual(cg2.tp_suspended_until, t + 3600)
        acts, alarms = cg2.filter(plan(P(), R(), [SL()], now=t + 60), [SL()], t + 60, rec=R())
        self.assertEqual((kinds(acts), alarms[0][0]), ([], 'TP_REPAIR_DEFERRED'))
        cg3 = bg.ChurnGuard()
        for i in range(3):
            cg3.note_placed('TP', 'z%d' % i, t + i)
        acts, alarms = cg3.filter(plan(P(), R(), [], now=t + 10), [], t + 10, qty=800.0, rec=R())
        self.assertEqual(kinds(acts), ['PLACE_SL:SL_REPAIRED'])                 # TP churn never touches the SL
        self.assertEqual(alarms[0], ('GUARDIAN_CHURN', 'critical', {'leg': 'TP', 'repairs_30m': 3}))

    def test_close_plans_and_unknown_reads_pass_untouched(self):
        cg = bg.ChurnGuard()
        for i in range(3):
            cg.note_placed('SL', 'x%d' % i, NOW)
        close = [Action('CLOSE', 'SL_MISSING_PRICE_BEYOND')]
        self.assertEqual(cg.filter(close, [], NOW), (close, []))
        unk = [Action('ALARM', 'GUARDIAN_READ_UNKNOWN')]
        self.assertEqual(cg.filter(unk, bg.UNKNOWN, NOW), (unk, []))

    def test_reset(self):
        cg = bg.ChurnGuard()
        cg.note_placed('SL', 'a', NOW)
        cg.note_place_failed('TP', NOW)
        cg.reset()
        self.assertEqual((cg.placed, cg.tp_fail, cg.tp_next_try, cg.tp_suspended_until),
                         ({'SL': [], 'TP': []}, 0, 0.0, 0.0))


class TestLevelPolicyA10(unittest.TestCase):
    """A10 (owner, 2026-09-28): masters (default LEVEL_ALARM) never cancel, move or replace a leg whose level
    differs from the recorded one; they raise LEG_LEVEL_MOVED and still re-place a MISSING SL/TP (SL first).
    Followers pass LEVEL_REPLACE and keep rev 3 (replace service orders make-before-break)."""

    def test_default_policy_is_alarm(self):
        acts = bg.decide(P(), R(), [SL(trigger=0.455), TP()], REF, TICK, LOT, NOW)
        self.assertEqual(kinds(acts), ['ALARM:LEG_LEVEL_MOVED'])

    def test_moved_sl(self):
        acts = plan(P(), R(), [SL(trigger=0.455), TP()])
        self.assertEqual(kinds(acts), ['ALARM:LEG_LEVEL_MOVED'])
        self.assertEqual((acts[0].severity, acts[0].ctx), ('warning', {'leg': 'SL', 'recorded': 0.46, 'live': 0.455,
                                                                     'oid': 'sl1'}))
        self.assertEqual(kinds(plan(P(), R(), [SL(trigger=0.455), TP()], policy=bg.LEVEL_REPLACE)),
                         ['PLACE_SL:SL_REPAIRED', 'CANCEL:SL_EXTRA_OR_INVALID'])

    def test_moved_sl_protects_even_when_price_passed_the_recorded_level(self):
        ref = {'last': 0.458}
        self.assertEqual(kinds(plan(P(), R(), [SL(trigger=0.455), TP()], ref=ref)), ['ALARM:LEG_LEVEL_MOVED'])
        self.assertEqual(kinds(plan(P(), R(), [SL(trigger=0.455), TP()], ref=ref, policy=bg.LEVEL_REPLACE)),
                         ['CLOSE:SL_MISSING_PRICE_BEYOND'])

    def test_moved_tp(self):
        acts = plan(P(), R(), [SL(), TP(limit=0.5)])
        self.assertEqual(kinds(acts), ['ALARM:LEG_LEVEL_MOVED'])
        self.assertEqual(acts[0].ctx, {'leg': 'TP', 'recorded': 0.49, 'live': 0.5, 'oid': 'tp1'})
        self.assertEqual(kinds(plan(P(), R(), [SL(), TP(limit=0.5)], policy=bg.LEVEL_REPLACE)),
                         ['CANCEL:TP_EXTRA_OR_INVALID', 'PLACE_TP:TP_REPAIRED'])

    def test_missing_tp_still_placed_next_to_a_moved_sl(self):
        self.assertEqual(kinds(plan(P(), R(), [SL(trigger=0.455)])), ['ALARM:LEG_LEVEL_MOVED', 'PLACE_TP:TP_REPAIRED'])

    def test_missing_sl_still_placed_when_the_moved_leg_does_not_protect(self):
        acts = plan(P(), R(), [SL('lim', kind=bg.STOP_LIMIT, trigger=0.455, limit=0.455), TP()])
        self.assertEqual(kinds(acts), ['ALARM:LEG_LEVEL_MOVED', 'PLACE_SL:SL_REPAIRED'])
        acts = plan(P(), R(), [SL(trigger=0.455, qty=300.0), TP()])
        self.assertEqual(kinds(acts), ['ALARM:LEG_LEVEL_MOVED', 'PLACE_SL:SL_REPAIRED'])

    def test_extra_leg_at_another_level_is_kept(self):
        legs = [SL(), SL('x', trigger=0.44), TP(), TP('y', limit=0.52)]
        self.assertEqual(kinds(plan(P(), R(), legs)), ['ALARM:LEG_LEVEL_MOVED', 'ALARM:LEG_LEVEL_MOVED'])
        self.assertEqual(kinds(plan(P(), R(), legs, policy=bg.LEVEL_REPLACE)),
                         ['CANCEL:SL_EXTRA_OR_INVALID', 'CANCEL:TP_EXTRA_OR_INVALID'])

    def test_legs_at_the_recorded_level_are_still_repaired(self):
        lim = SL('lim', kind=bg.STOP_LIMIT, limit=0.46)
        self.assertEqual(kinds(plan(P(), R(), [lim, TP()])), ['PLACE_SL:SL_REPAIRED', 'CANCEL:SL_EXTRA_OR_INVALID'])
        self.assertEqual(kinds(plan(P(), R(), [SL(), TP(qty=400.0)])),
                         ['CANCEL:TP_EXTRA_OR_INVALID', 'PLACE_TP:TP_REPAIRED'])

    def test_mis_levelled_trusted_stop(self):
        rec = R(sl_oid='s9', sl_oid_trusted=True)
        legs = [SL('s9', trigger=0.455), TP()]
        acts, alarms = bg.ChurnGuard().filter(plan(P(), rec, legs), legs, NOW, qty=800.0, rec=rec)
        self.assertEqual((kinds(acts), alarms), (['ALARM:LEG_LEVEL_MOVED'], []))
        acts, _ = bg.ChurnGuard().filter(plan(P(), rec, legs, policy=bg.LEVEL_REPLACE), legs, NOW, qty=800.0, rec=rec)
        self.assertEqual(kinds(acts), ['PLACE_SL:SL_REPAIRED', 'CANCEL:SL_EXTRA_OR_INVALID'])

    def test_moved_stop_resting_on_a_confirmed_flat_waits_240s_on_masters(self):
        legs = [SL('m', trigger=0.455)]
        self.assertEqual(kinds(plan(None, R(), legs, flat_confirmed=True, flat_since=NOW - 30)),
                         ['ALARM:FLAT_SL_RESTING'])
        self.assertEqual(kinds(plan(None, R(), legs, flat_confirmed=True, flat_since=NOW - 240)),
                         ['CANCEL:ORPHAN_LEG', 'CLEAR_RECORD:FLAT'])
        self.assertEqual(kinds(plan(None, R(), legs, flat_confirmed=True, flat_since=NOW - 30,
                                    policy=bg.LEVEL_REPLACE)),
                         ['CANCEL:ORPHAN_LEG', 'CLEAR_RECORD:FLAT'])


def _truth_valid(l, p, r, xs):
    return (l.kind == bg.STOP_MARKET and l.side == xs and (l.reduce_only or l.close_position) and l.trigger is not None
            and abs(l.trigger - r.sl) <= TICK * bg.TICK_TOL and (l.qty is None or l.qty >= p.qty - LOT / 2)
            and (l.basis is None or l.basis == r.sl_basis))


def _truth_protects(l, p, r, xs, policy):
    if _truth_valid(l, p, r, xs):
        return True
    return (policy == bg.LEVEL_ALARM and l.kind == bg.STOP_MARKET and l.side == xs and l.reduce_only
            and l.trigger is not None and (l.qty is None or l.qty >= p.qty - LOT / 2)
            and (l.basis is None or l.basis == r.sl_basis))


class TestPropertyFuzz(unittest.TestCase):
    """5,000 random states x 3 passes per level policy, with random external cancels, TP rejections, SL placement
    failures and misparsed side/kind/level of the guardian's own stop. Executor contract as the ports: PLACE_SL
    success is noted and merged into the record, a failed PLACE_SL closes, cancels of the protected stop need a
    successful PLACE_SL earlier in the same plan."""
    STATES = 5000

    def test_fuzz_alarm_policy(self):
        self._fuzz(bg.LEVEL_ALARM, seed=7)

    def test_fuzz_replace_policy(self):
        self._fuzz(bg.LEVEL_REPLACE, seed=11)

    def _fuzz(self, policy, seed):
        rng = random.Random(seed)
        pool = [bg.STOP_MARKET, bg.STOP_LIMIT, bg.TP_LIMIT, bg.TP_TRIGGER_MARKET, bg.OTHER]
        tol = TICK * bg.TICK_TOL
        converged = 0
        for n in range(self.STATES):
            faulty = rng.random() < 0.5
            side = rng.choice((1, -1))
            xs, other = ('sell', 'buy') if side > 0 else ('buy', 'sell')
            qty = rng.choice((500.0, 800.0))
            sl_lvl, tp_def = (0.46, 0.49) if side > 0 else (0.48, 0.45)
            tp_lvl = rng.choice((tp_def, None))
            trusted = rng.random() < 0.7
            r = Rec(side, sl_lvl, tp_lvl, 'last', bg.TP_LIMIT, sl_oid='own-sl', sl_oid_trusted=trusted)
            p = Pos(side, qty, 0.47, None)
            book = {}
            if rng.random() < 0.7:
                lvl = rng.choice((sl_lvl, sl_lvl, sl_lvl + 0.003, sl_lvl - 0.003)) if faulty else sl_lvl
                book['own-sl'] = Leg('own-sl', xs, bg.STOP_MARKET, qty, trigger=lvl, reduce_only=True)
            for j in range(rng.randint(0, 3)):
                k = rng.choice(pool)
                lvl = rng.choice((sl_lvl, tp_def, 0.5, 0.44))
                trig, lim = (None, lvl) if k == bg.TP_LIMIT else (lvl, None)
                book['x%d' % j] = Leg('x%d' % j, rng.choice((xs, other)), k, rng.choice((qty, qty / 2, None)),
                                      trigger=trig, limit=lim, reduce_only=rng.random() < 0.8)
            cg = bg.ChurnGuard()
            if 'own-sl' in book and rng.random() < 0.5 and (faulty or trusted):   # fault-free: history implies trust
                cg.note_placed('SL', 'own-sl', NOW - 60)
            mis = rng.choice((None, 'side', 'kind', 'trigger')) if faulty else None
            now, seq = NOW, 0
            for pas in range(3):
                if faulty and pas > 0 and book and rng.random() < 0.5:
                    del book[rng.choice(sorted(book))]
                own = cg.placed['SL'][-1][1] if cg.placed['SL'] else 'own-sl'
                legs = []
                for l in book.values():
                    c = dataclasses.replace(l)
                    if c.oid == own and mis == 'side':
                        c.side = other
                    elif c.oid == own and mis == 'kind':
                        c.kind = bg.OTHER
                    elif c.oid == own and mis == 'trigger' and c.trigger is not None:
                        c.trigger = c.trigger + 0.002
                    legs.append(c)
                protects_before = [l.oid for l in book.values() if _truth_protects(l, p, r, xs, policy)]
                raw = bg.decide(p, r, legs, {'last': 0.47}, TICK, LOT, now, level_policy=policy)
                acts, _ = cg.filter(raw, legs, now, qty=p.qty, rec=r)
                ctx = 'state %d pass %d policy %s raw %s filtered %s' % (n, pas, policy, kinds(raw), kinds(acts))
                last_own = cg.placed['SL'][-1][1] if cg.placed['SL'] else None
                protected = {o for o in (last_own, bg.trusted_sl_oid(r)) if o}
                for i, a in enumerate(acts):
                    if a.kind == 'CANCEL' and a.leg.oid in protected and a.code != 'ORPHAN_LEG':
                        self.assertTrue(any(b.kind == 'PLACE_SL' for b in acts[:i]), 'protected stop cancelled: ' + ctx)
                if protects_before:
                    self.assertFalse(any(a.kind == 'CLOSE' and a.code != 'SL_UNMAINTAINABLE' for a in acts),
                                     'close while a valid SL exists: ' + ctx)
                if (any(a.kind == 'PLACE_SL' for a in raw) and not any(a.kind in ('PLACE_SL', 'CLOSE') for a in acts)):
                    self.assertTrue(cg.placed['SL'] and cg.placed['SL'][-1][1] in {l.oid for l in legs},
                                    'PLACE_SL suppressed without the own stop on the book: ' + ctx)
                if policy == bg.LEVEL_ALARM and mis is None:
                    for a in acts:
                        t = book.get(a.leg.oid) if a.kind == 'CANCEL' and a.leg is not None else None
                        if t is not None and t.side == xs and t.level is not None and t.kind in bg.SL_KINDS:
                            self.assertLessEqual(abs(t.level - sl_lvl), tol, 'A10: moved SL cancelled: ' + ctx)
                        if (t is not None and t.side == xs and t.level is not None and t.kind in bg.TP_KINDS
                                and tp_lvl is not None):
                            self.assertLessEqual(abs(t.level - tp_lvl), tol, 'A10: moved TP cancelled: ' + ctx)
                placed_ok = False
                for a in acts:
                    if a.kind == 'CLOSE':
                        p = None
                        break
                    if a.kind == 'PLACE_SL':
                        if faulty and rng.random() < 0.1:
                            p = None                                            # 3 failed placements -> close
                            break
                        seq += 1
                        oid = 'g%d' % seq
                        book[oid] = Leg(oid, xs, bg.STOP_MARKET, a.qty, trigger=a.level, reduce_only=True,
                                        basis=a.ctx.get('basis'))
                        cg.note_placed('SL', oid, now)
                        r.sl_oid, r.sl_oid_trusted, placed_ok = oid, True, True
                    elif a.kind == 'CANCEL':
                        self.assertTrue(a.leg.oid not in protected or a.code == 'ORPHAN_LEG' or placed_ok,
                                        'executor would cancel the protected stop first: ' + ctx)
                        book.pop(a.leg.oid, None)
                    elif a.kind == 'PLACE_TP':
                        if faulty and rng.random() < 0.5:
                            cg.note_place_failed('TP', now)
                        else:
                            seq += 1
                            oid = 't%d' % seq
                            book[oid] = Leg(oid, xs, bg.TP_LIMIT, a.qty, limit=a.level, reduce_only=True)
                            cg.note_placed('TP', oid, now)
                            r.tp_oid = oid
                if p is None:
                    break
                if protects_before:
                    self.assertTrue(any(_truth_protects(l, p, r, xs, policy) for l in book.values()),
                                    'plan removed the last protecting SL: ' + ctx)
                now += 300
            if faulty or p is None:
                continue
            final = bg.decide(p, r, list(book.values()), {'last': 0.47}, TICK, LOT, now, level_policy=policy)
            acts, _ = cg.filter(final, list(book.values()), now, qty=p.qty, rec=r)
            ctx = 'state %d policy %s final %s' % (n, policy, kinds(acts))
            self.assertFalse([a for a in acts if a.kind in ('PLACE_SL', 'PLACE_TP', 'CANCEL', 'CLOSE')],
                             'no fixed point: ' + ctx)
            self.assertTrue(any(_truth_protects(l, p, r, xs, policy) for l in book.values()), 'unprotected: ' + ctx)
            if policy == bg.LEVEL_REPLACE:
                ro = [l for l in book.values() if l.reduce_only and l.side == xs]
                self.assertEqual(len([l for l in ro if l.kind in bg.SL_KINDS]), 1, 'not exactly one SL: ' + ctx)
                if tp_lvl is not None:
                    self.assertEqual(len([l for l in ro if l.kind in bg.TP_KINDS]), 1, 'not exactly one TP: ' + ctx)
            converged += 1
        self.assertGreater(converged, self.STATES // 4)
