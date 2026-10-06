"""T03 test_close_common (REQUIRED in both follower gates).

Byte-identical in hyperliquid-nike-rocket-api/tests and kraken-nike-rocket-api/tests (T03 of
massive-rocket-algos docs/superpowers/specs/2026-10-06-t1-live-design.md, section 20). Pure: imports close_guard,
bracket_guard and follower_guard only (no DB, no exchange, no fastapi). Covers the wire-contract v2 validator matrix
(19.3, from tests/fixtures/close_v2.json), the plan_close_step table (19.6/19.7), the job constants, and the
section-21 follower event log (required fields, redaction, rotation, disk-full guard, never raises).
"""
import hashlib
import json
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bracket_guard as bg  # noqa: E402
import close_guard as cg  # noqa: E402
import follower_guard as fg  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'close_v2.json')
# Newline-normalised sha256 of tests/fixtures/close_v2.json. The same constant in both follower copies of this file
# plus the gates' sibling comparison pin the fixture byte-identical across the master and both followers.
CLOSE_V2_SHA256 = 'adc28169a1bd06d70b8af7395250a8d715635e7e9e341f3f022701d6a7b2822e'

TID = '0123456789abcdef0123456789abcdef'
LOT = 1.0


def load_fixture():
    with open(FIXTURE, 'rb') as f:
        raw = f.read()
    return raw, json.loads(raw.decode('utf-8'))


def row(**kw):
    r = {'id': 7, 'status': 'open', 'side': 'BUY', 'quantity': 800, 'symbol': 'ADA/USDT'}
    r.update(kw)
    return r


def pos(side=1, qty=800.0):
    return bg.Pos(side, qty, 0.47, None)


# ---- contract fixture (19.3) ----

def test_fixture_sha_is_pinned():
    raw, _ = load_fixture()
    assert hashlib.sha256(raw.replace(b'\r\n', b'\n')).hexdigest() == CLOSE_V2_SHA256


def test_fixture_shape():
    _, fx = load_fixture()
    assert fx['contract'] == 'rocket-close-v2'
    assert fx['endpoint'] == 'POST /api/broadcast-close'
    assert fx['known_symbols'] == ['ADA/USDT']
    assert fx['responses']['first'] == {'accepted': True, 'duplicate': False, 'followers_matched': 3}
    assert fx['responses']['repeat'] == {'accepted': True, 'duplicate': True, 'followers_matched': 3}
    names = [c['name'] for c in fx['cases']]
    assert len(names) == len(set(names))
    assert sum(c['expect'] == 200 for c in fx['cases']) == 5
    assert all(c['expect'] in (200, 422) for c in fx['cases'])
    fields = {c['field'] for c in fx['cases'] if c['expect'] == 422}
    assert fields == {'payload', 'protocol', 'action', 'master_trade_id', 'symbol', 'side', 'reason',
                      'master_closed_ms'}


def _cases():
    return [pytest.param(c, id=c['name']) for c in load_fixture()[1]['cases']]


@pytest.mark.parametrize('case', _cases())
def test_validator_matrix_from_fixture(case):
    _, fx = load_fixture()
    reason = cg.validate_close_payload(case['payload'], fx['known_symbols'], fx['now_ms'])
    if case['expect'] == 200:
        assert reason is None
    else:
        assert isinstance(reason, str) and reason.startswith(case['field']), reason


def test_validator_accepts_symbol_map_dict_and_set():
    _, fx = load_fixture()
    good = fx['cases'][0]['payload']
    assert cg.validate_close_payload(good, {'ADA/USDT': 'PF_ADAUSD'}, fx['now_ms']) is None
    assert cg.validate_close_payload(good, frozenset({'ADA/USDT'}), fx['now_ms']) is None
    assert cg.validate_close_payload(good, (), fx['now_ms']).startswith('symbol')
    assert cg.validate_close_payload(good, None, fx['now_ms']).startswith('symbol')


def test_validator_never_raises_and_does_not_mutate():
    _, fx = load_fixture()
    good = dict(fx['cases'][0]['payload'])
    before = dict(good)
    for bad in (None, 7, 'x', b'x', [], {}, {'protocol': object()}, dict(good, master_trade_id=None),
                dict(good, symbol=['ADA/USDT']), dict(good, symbol={'a': 1}), dict(good, side=None),
                dict(good, master_closed_ms=float('nan'))):
        reason = cg.validate_close_payload(bad, fx['known_symbols'], fx['now_ms'])
        assert isinstance(reason, str) and reason
    assert good == before
    assert cg.validate_close_payload(good, fx['known_symbols'], fx['now_ms']) is None


def test_validator_window_constants():
    assert cg.CLOSE_PROTOCOL == 2
    assert cg.CLOSE_REASONS == frozenset({'time_exit'})
    assert cg.CLOSE_MAX_PAST_MS == 48 * 3_600_000
    assert cg.CLOSE_MAX_FUTURE_MS == 5 * 60_000
    assert cg.is_master_trade_id(TID)
    assert not cg.is_master_trade_id(TID + '\n')
    assert not cg.is_master_trade_id(TID.upper())
    assert not cg.is_master_trade_id(None)


def test_job_constants():
    assert cg.CLOSE_JOB_MAX_ATTEMPTS == 6
    assert cg.CLOSE_JOB_MAX_AGE_S == 86400
    assert cg.CLOSE_BUSY_WINDOW_S == 180
    assert cg.JOB_STATUSES == ('pending', 'deferred', 'done', 'failed', 'skipped', 'expired')
    assert cg.JOB_OPEN_STATUSES == ('pending', 'deferred')


# ---- plan_close_step table (19.6 / 19.7) ----

def codes(step):
    return (step.code, step.qty, step.exit_side)


@pytest.mark.parametrize('status', ['closed', 'cancelled', 'failed', '', None, 'OPEN'])
def test_row_not_active_is_done(status):
    for vp in (pos(), None, bg.UNKNOWN):
        assert cg.plan_close_step(row(status=status), vp, 'BUY', lot=LOT).code == cg.DONE_ROW_INACTIVE


def test_needs_recovery_defers_before_any_read():
    for vp in (pos(), None, bg.UNKNOWN, pos(side=-1)):
        assert cg.plan_close_step(row(status='needs_recovery'), vp, 'BUY', lot=LOT).code == cg.DEFER_RECOVERY
    assert 'needs_recovery' in fg.ACTIVE_STATUSES and 'open' in fg.ACTIVE_STATUSES


@pytest.mark.parametrize('vp', [bg.UNKNOWN, 'garbage', 3, {'side': 1}, bg.Pos(1, float('nan')),
                                bg.Pos(1, -5.0), bg.Pos(0, 800.0), bg.Pos(2, 800.0), bg.Pos(1, None)])
def test_read_unknown_never_acts(vp):
    step = cg.plan_close_step(row(), vp, 'BUY', lot=LOT)
    assert step.code == cg.RETRY_READ_UNKNOWN and step.qty is None and not step.acts


@pytest.mark.parametrize('vp', [None, bg.Pos(1, 0.0), bg.Pos(-1, 0)])
def test_flat_is_done(vp):
    step = cg.plan_close_step(row(), vp, 'BUY', lot=LOT)
    assert step.code == cg.DONE_ALREADY_FLAT and step.qty is None and not step.acts


@pytest.mark.parametrize('row_side,venue_side,payload_side', [
    ('BUY', -1, 'BUY'),      # venue flipped short under a long row
    ('BUY', 1, 'SELL'),      # payload names the other side
    ('SELL', 1, 'SELL'),
    ('SELL', -1, 'BUY'),
    ('', 1, 'BUY'),          # row side unreadable
    ('BUY', 1, 'buy'),       # payload side not canonical
    ('BUY', 1, None),
])
def test_side_mismatch_fails_without_order(row_side, venue_side, payload_side):
    step = cg.plan_close_step(row(side=row_side), pos(side=venue_side), payload_side, lot=LOT)
    assert step.code == cg.FAIL_SIDE_MISMATCH and step.qty is None and not step.acts


def test_close_long_full():
    step = cg.plan_close_step(row(), pos(), 'BUY', lot=LOT)
    assert codes(step) == (cg.CLOSE, 800.0, 'sell')
    assert step.acts and step.reduce_only and not step.residual
    assert (step.venue_qty, step.row_qty) == (800.0, 800.0)


def test_close_short_full_and_long_alias():
    step = cg.plan_close_step(row(side='SELL', quantity='800'), pos(side=-1), 'SELL', lot=LOT)
    assert codes(step) == (cg.CLOSE, 800.0, 'buy')
    step = cg.plan_close_step(row(side='LONG'), pos(), 'BUY', lot=LOT)
    assert codes(step) == (cg.CLOSE, 800.0, 'sell')


def test_close_is_min_of_venue_and_row():
    # TP partially filled before the cancel: the venue holds less than the row -> close only what is there.
    step = cg.plan_close_step(row(), pos(qty=300.0), 'BUY', lot=LOT)
    assert codes(step) == (cg.CLOSE, 300.0, 'sell') and not step.residual
    # Venue holds more than the row (user added by hand): close the row's size only, residual expected (I1, I2).
    step = cg.plan_close_step(row(), pos(qty=1000.0), 'BUY', lot=LOT)
    assert codes(step) == (cg.CLOSE, 800.0, 'sell') and step.residual


def test_close_qty_floored_to_lot_exactly():
    step = cg.plan_close_step(row(quantity=0.3), bg.Pos(1, 0.35), 'BUY', lot=0.1)
    assert codes(step) == (cg.CLOSE, 0.3, 'sell')                    # 0.3 / 0.1 must not floor to 0.2
    step = cg.plan_close_step(row(quantity=123.456), bg.Pos(1, 200.0), 'BUY', lot=0.01)
    assert step.qty == 123.45
    step = cg.plan_close_step(row(quantity=812.9), pos(qty=900.0), 'BUY', lot=10)
    assert step.qty == 810.0 and step.residual
    step = cg.plan_close_step(row(side='SELL', quantity='15.7'), bg.Pos(-1, 15.7), 'SELL', lot=0.1)
    assert codes(step) == (cg.CLOSE, 15.7, 'buy')


@pytest.mark.parametrize('qty', [None, '', 'abc', 0, -1, float('nan'), float('inf')])
def test_bad_row_quantity_fails_without_order(qty):
    step = cg.plan_close_step(row(quantity=qty), pos(), 'BUY', lot=LOT)
    assert step.code == cg.FAIL_BAD_QTY and step.qty is None and not step.acts


def test_qty_below_one_lot_fails_without_order():
    step = cg.plan_close_step(row(quantity=0.4), pos(), 'BUY', lot=LOT)
    assert step.code == cg.FAIL_BAD_QTY and not step.acts


@pytest.mark.parametrize('lot', [0, -1, None, float('nan'), 'x'])
def test_bad_lot_fails_without_order(lot):
    step = cg.plan_close_step(row(), pos(), 'BUY', lot=lot)
    assert step.code == cg.FAIL_BAD_QTY and not step.acts


def test_lot_is_keyword_only():
    with pytest.raises(TypeError):
        cg.plan_close_step(row(), pos(), 'BUY', LOT)


# ---- QC fix (reviews 1+2 BLOCKING): a retry sizes against the row's REMAINDER, never the user's own size ----

def test_retry_after_full_row_close_never_touches_user_position():
    # venue 150 = row 100 + 50 the user added by hand. Attempt 1 closed the row's 100 (close_fill_qty 100).
    # Attempt 2 sees venue 50: that is the user's own position -> no order, the row's share is closed.
    step = cg.plan_close_step(row(quantity=100, close_fill_qty=100), pos(qty=50.0), 'BUY', lot=LOT)
    assert step.code == cg.DONE_ROW_SHARE_CLOSED and step.qty is None and not step.acts
    assert (step.venue_qty, step.row_qty, step.close_fill_qty) == (50.0, 100.0, 100.0)


def test_retry_after_partial_fill_closes_only_the_row_remainder():
    # 60 of the row's 100 filled (venue 150 -> 90): attempt 2 closes 40, never 90.
    step = cg.plan_close_step(row(quantity=100, close_fill_qty=60), pos(qty=90.0), 'BUY', lot=LOT)
    assert codes(step) == (cg.CLOSE, 40.0, 'sell') and step.residual
    step = cg.plan_close_step(row(quantity=100, close_fill_qty='60'), pos(qty=40.0), 'BUY', lot=LOT)
    assert codes(step) == (cg.CLOSE, 40.0, 'sell') and not step.residual


@pytest.mark.parametrize('cf', [None, '', 0, '0', 0.0])
def test_no_previous_fill_is_the_full_row(cf):
    step = cg.plan_close_step(row(close_fill_qty=cf), pos(), 'BUY', lot=LOT)
    assert codes(step) == (cg.CLOSE, 800.0, 'sell') and step.close_fill_qty == 0.0


def test_fill_at_or_over_row_or_under_one_lot_left_is_share_closed():
    for cf in (800, 1000, 799.5):
        step = cg.plan_close_step(row(close_fill_qty=cf), pos(qty=300.0), 'BUY', lot=LOT)
        assert step.code == cg.DONE_ROW_SHARE_CLOSED and not step.acts, cf


@pytest.mark.parametrize('cf', [-1, float('nan'), float('inf'), 'abc', [1], True])
def test_bad_close_fill_qty_fails_without_order(cf):
    step = cg.plan_close_step(row(close_fill_qty=cf), pos(), 'BUY', lot=LOT)
    assert step.code == cg.FAIL_BAD_QTY and not step.acts


# ---- QC fix (review 1 #7): venue dust under one lot is effectively closed, not a failed job ----

def test_venue_dust_under_one_lot_is_done_without_order():
    step = cg.plan_close_step(row(), pos(qty=0.4), 'BUY', lot=LOT)
    assert step.code == cg.DONE_DUST and step.qty is None and not step.acts
    assert step.venue_qty == 0.4
    step = cg.plan_close_step(row(quantity=0.3), bg.Pos(1, 0.05), 'BUY', lot=0.1)
    assert step.code == cg.DONE_DUST


def test_only_close_acts_and_step_codes_are_complete():
    assert cg.STEP_CODES == (cg.DONE_ROW_INACTIVE, cg.DEFER_RECOVERY, cg.RETRY_READ_UNKNOWN, cg.DONE_ALREADY_FLAT,
                             cg.FAIL_SIDE_MISMATCH, cg.FAIL_BAD_QTY, cg.DONE_ROW_SHARE_CLOSED, cg.DONE_DUST,
                             cg.CLOSE)
    for code in cg.STEP_CODES:
        assert cg.CloseStep(code).acts == (code == cg.CLOSE)


def test_plan_does_not_mutate_row():
    r = row()
    before = dict(r)
    cg.plan_close_step(r, pos(), 'BUY', lot=LOT)
    assert r == before


def test_step_as_log_is_json_ready():
    step = cg.plan_close_step(row(), pos(qty=1000.0), 'BUY', lot=LOT)
    d = step.as_log()
    assert d == {'step': 'CLOSE', 'qty': 800.0, 'exit_side': 'sell', 'reduce_only': True, 'residual': True,
                 'venue_qty': 1000.0, 'row_qty': 800.0, 'close_fill_qty': 0.0, 'detail': step.detail}
    json.dumps(d)


# ---- section 21: follower close event log ----

SECRETS = ('sk_live_abcdef0123456789', 'Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig', 'mk-SuperSecretMasterKey-42',
           '0x' + 'ab' * 32, 'hunter2-password', 'QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5Kys=')


def make_log(tmp_path, **kw):
    kw.setdefault('venue', 'hyperliquid')
    kw.setdefault('service', 'hyperliquid-nike-rocket-api')
    kw.setdefault('host', 'test-host')
    kw.setdefault('code_sha', 'abc1234')
    kw.setdefault('clock', lambda: 1760000000.0)
    return cg.CloseEventLog(str(tmp_path / 'logs' / 'close_events.jsonl'), **kw)


def read_lines(path):
    with open(path, encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


def test_event_log_required_fields(tmp_path):
    log = make_log(tmp_path)
    rec = log.emit(cg.EV_RECEIVED, master_trade_id=TID, matched=2, symbol='ADA/USDT')
    lines = read_lines(log.path)
    assert lines == [rec]
    for k in cg.REQUIRED_EVENT_FIELDS:
        assert k in rec, k
    assert rec['ts_utc'] == '2025-10-09T08:53:20.000Z'
    assert (rec['host'], rec['venue'], rec['service'], rec['mode'], rec['code_sha']) == (
        'test-host', 'hyperliquid', 'hyperliquid-nike-rocket-api', 'live', 'abc1234')
    assert (rec['event'], rec['severity'], rec['master_trade_id']) == ('CLOSE_RECEIVED', 'INFO', TID)
    assert rec['matched'] == 2


def test_event_log_default_severities_and_override(tmp_path):
    log = make_log(tmp_path)
    assert log.emit(cg.EV_SIDE_MISMATCH, master_trade_id=TID)['severity'] == 'CRITICAL'
    assert log.emit(cg.EV_RESIDUAL)['severity'] == 'CRITICAL'
    assert log.emit(cg.EV_JOB_EXPIRED)['severity'] == 'CRITICAL'
    assert log.emit(cg.EV_JOB_FAILED)['severity'] == 'CRITICAL'
    assert log.emit(cg.EV_LEG_LEFT)['severity'] == 'WARN'
    assert log.emit(cg.EV_PAYLOAD_REJECTED)['severity'] == 'WARN'
    assert log.emit(cg.EV_NO_MATCH)['severity'] == 'INFO'
    assert log.emit(cg.EV_JOB_STEP, severity='warning')['severity'] == 'WARN'
    assert log.emit('SOMETHING_NEW')['severity'] == 'INFO'
    assert log.emit(cg.EV_JOB_STEP, severity='nonsense')['severity'] == 'INFO'
    assert log.emit(cg.EV_JOB_STEP)['master_trade_id'] is None     # key present before the id is known
    assert log.emit(cg.EV_RESIDUAL_NOT_OURS)['severity'] == 'WARN'
    assert log.emit(cg.EV_DUST)['severity'] == 'WARN'
    assert set(cg.EVENT_SEVERITY) >= {'CLOSE_RECEIVED', 'CLOSE_DUPLICATE', 'CLOSE_NO_MATCH', 'CLOSE_PAYLOAD_REJECTED',
                                      'CLOSE_JOB_CREATED', 'CLOSE_JOB_STEP', 'CLOSE_JOB_OUTCOME', 'CLOSE_JOB_SKIPPED',
                                      'CLOSE_SIDE_MISMATCH', 'CLOSE_RESIDUAL', 'CLOSE_LEG_LEFT', 'CLOSE_JOB_EXPIRED',
                                      'CLOSE_JOB_FAILED'}


def test_event_log_step_record(tmp_path):
    log = make_log(tmp_path)
    step = cg.plan_close_step(row(), pos(), 'BUY', lot=LOT)
    rec = log.emit(cg.EV_JOB_STEP, master_trade_id=TID, user_id=11, position_id=7, **step.as_log())
    assert (rec['step'], rec['qty'], rec['exit_side'], rec['user_id'], rec['position_id']) == (
        'CLOSE', 800.0, 'sell', 11, 7)


def test_event_log_mirrors_to_python_logger(tmp_path, caplog):
    log = make_log(tmp_path)
    with caplog.at_level(logging.INFO, logger='close_events'):
        log.emit(cg.EV_RESIDUAL, master_trade_id=TID)
    recs = [r for r in caplog.records if r.name == 'close_events']
    assert recs and recs[-1].levelno == logging.CRITICAL
    assert json.loads(recs[-1].getMessage())['event'] == 'CLOSE_RESIDUAL'


def test_event_log_redacts_secrets(tmp_path, caplog):
    log = make_log(tmp_path)
    with caplog.at_level(logging.DEBUG, logger='close_events'):
        log.emit(cg.EV_RECEIVED, master_trade_id=TID, api_key=SECRETS[0],
                 headers={'Authorization': SECRETS[1], 'X-Master-Key': SECRETS[2], 'Content-Type': 'json'},
                 note='signed with %s by %s' % (SECRETS[3], SECRETS[1]), password=SECRETS[4],
                 nested=[{'hl_private_key_encrypted': SECRETS[5]}, {'secret': 1}], blob=SECRETS[5],
                 credentials={'k': 'v'}, token='tkn-123456')
        log.emit(cg.EV_JOB_STEP, error='auth failed: Bearer %s' % SECRETS[0])
    text = open(log.path, encoding='utf-8').read() + '\n'.join(r.getMessage() for r in caplog.records)
    for s in SECRETS + ('tkn-123456', 'eyJhbGciOiJIUzI1NiJ9', SECRETS[0]):
        assert s not in text, s
    assert cg.SECRET_VALUE_RE.search(text) is None
    first = read_lines(log.path)[0]
    assert first['master_trade_id'] == TID                       # a 32-hex trade id is not a secret
    assert first['headers']['Content-Type'] == 'json'
    assert first['headers']['Authorization'] == cg.REDACTED
    assert first['api_key'] == cg.REDACTED and first['password'] == cg.REDACTED


def test_redact_helper_keeps_plain_values():
    d = {'master_trade_id': TID, 'symbol': 'ADA/USDT', 'qty': 800.0, 'ok': True, 'n': None, 'user_id': 5,
         'wallet': '0x' + 'cd' * 20}
    assert cg.redact(d) == d


def test_event_log_non_json_values_and_bad_field_names(tmp_path):
    import datetime
    import decimal
    log = make_log(tmp_path)
    rec = log.emit(cg.EV_JOB_STEP, when=datetime.datetime(2026, 10, 6, 1, 2, 3), qty=decimal.Decimal('1.5'),
                   obj=object())
    assert rec is not None
    line = read_lines(log.path)[0]
    assert line['when'] == '2026-10-06 01:02:03' and line['qty'] == '1.5'
    # fields cannot overwrite the envelope
    rec = log.emit(cg.EV_JOB_STEP, venue='spoof', ts_utc='x', event_override=1, extra={'severity': 'x'})
    assert rec['venue'] == 'hyperliquid' and rec['ts_utc'] != 'x' and rec['field_venue'] == 'spoof'


def test_event_log_rotates_with_retention_cap(tmp_path):
    log = make_log(tmp_path, max_bytes=600, backups=2)
    for i in range(40):
        log.emit(cg.EV_JOB_STEP, master_trade_id=TID, i=i, pad='x' * 50)
    d = os.path.dirname(log.path)
    files = sorted(os.listdir(d))
    assert files == ['close_events.jsonl', 'close_events.jsonl.1', 'close_events.jsonl.2']
    for f in files:
        assert os.path.getsize(os.path.join(d, f)) <= 600 + 400
    assert read_lines(log.path)[-1]['i'] == 39


def test_event_log_never_raises_on_disk_errors(tmp_path, monkeypatch):
    log = make_log(tmp_path)

    def boom(*a, **k):
        raise OSError(28, 'No space left on device')
    monkeypatch.setattr(cg, '_open_append', boom)
    rec = log.emit(cg.EV_RESIDUAL, master_trade_id=TID)
    assert rec is not None and rec['event'] == 'CLOSE_RESIDUAL'
    assert log.dropped == 1


def test_event_log_skips_file_when_disk_nearly_full(tmp_path):
    log = make_log(tmp_path, min_free_bytes=10 ** 9, free_bytes_fn=lambda p: 10)
    assert log.emit(cg.EV_RECEIVED, master_trade_id=TID) is not None
    assert not os.path.exists(log.path) and log.dropped == 1


def test_event_log_bad_path_never_raises(tmp_path):
    blocker = tmp_path / 'file'
    blocker.write_text('x')
    log = cg.CloseEventLog(str(blocker / 'sub' / 'close_events.jsonl'), venue='kraken', service='s', host='h',
                           code_sha='c')
    assert log.emit(cg.EV_RECEIVED) is not None and log.dropped == 1


def test_event_log_defaults_need_no_env(tmp_path, monkeypatch):
    monkeypatch.delenv('RAILWAY_GIT_COMMIT_SHA', raising=False)
    log = cg.CloseEventLog(str(tmp_path / 'e.jsonl'), venue='kraken', service='kraken-nike-rocket-api')
    rec = log.emit(cg.EV_NO_MATCH, master_trade_id=TID)
    assert rec['code_sha'] == 'unknown' and rec['host']
    assert rec['ts_utc'].endswith('Z')
    monkeypatch.setenv('RAILWAY_GIT_COMMIT_SHA', 'deadbeef')
    log = cg.CloseEventLog(str(tmp_path / 'e2.jsonl'), venue='kraken', service='kraken-nike-rocket-api')
    assert log.emit(cg.EV_NO_MATCH)['code_sha'] == 'deadbeef'


# ---- QC fix (review 4 #4/#9/#10): one redactor, one adversarial corpus (the same corpus is in
# massive-rocket-algos tests/test_t1_eventlog.py), sha/tx ids and paths stay readable, config_sha in the envelope ----

_S = ''.join(('Q7x', 'P2m', 'Vt9', 'Lk'))          # built at runtime: no literal secret in the source
LEAK_TEMPLATES = (
    'X-Master-Key: {s}', '{{"X-Master-Key": "{s}"}}', "{{'x-master-key': '{s}'}}", 'masterKey={s}',
    'FOLLOWER_MASTER_KEY={s}', 'HL_PRIVATE_KEY = {s}', 'GET https://x.example/api?key={s}&a=1',
    'GET https://x.example/api?apikey={s}', 'the password is {s}', 'Authorization: Bearer {s}',
    'Authorization: Basic {s}', 'token={s}', "{{'api_secret': '{s}'}}", 'sig={s}', 'signature: {s}',
    'auth failed: bearer {s}', 'passphrase="{s}"', 'refresh_token: {s}',
)
BARE_SECRETS = ('0x' + '9f' * 32, '7e' * 32, 'QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5Kys=',
                'aZ3' * 12, 'Ab1_' + 'xY9z' * 9)
KEEP = {'inputs_sha256': 'ab' * 32, 'tx_hash': '0x' + 'cd' * 32, 'code_sha': 'ef' * 32,
        'path': 'C:/Users/loopy/massive-rocket-algos-wt-t1live/logs/close_events.jsonl',
        'url': 'https://api.hyperliquid.xyz/info', 'master_trade_id': TID, 'wallet': '0x' + 'aB' * 20,
        'symbol': 'ADA/USDT', 'ts': '2026-10-06T01:02:03.000Z', 'note': 'retry 3 of 6, venue qty 800.0',
        'service': 'hyperliquid-nike-rocket-api', 'module': 'rocket_t1/vendor/rocket_bt/gpt_top100/entry_gate'}


@pytest.mark.parametrize('tpl', LEAK_TEMPLATES)
def test_redact_labelled_secret_inside_text(tpl):
    out = cg.redact({'detail': tpl.format(s=_S)})['detail']
    assert _S not in out and cg.REDACTED in out, out


@pytest.mark.parametrize('secret', BARE_SECRETS)
def test_redact_bare_secret_values(secret):
    out = cg.redact({'detail': 'value %s end' % secret, 'list': [secret]})
    assert secret not in json.dumps(out), out


def test_redact_keeps_shas_ids_paths_and_urls():
    assert cg.redact(KEEP) == KEEP
    assert cg.redact({'x': {'tx_hash': '0x' + 'cd' * 32}}) == {'x': {'tx_hash': '0x' + 'cd' * 32}}
    # outside a sha/hash-named field a bare 64-hex value is treated as a key
    assert cg.redact({'note': 'ab' * 32}) == {'note': cg.REDACTED}


def test_event_log_config_sha_in_envelope(tmp_path):
    log = make_log(tmp_path, config={'CLOSE_JOBS_ENABLED': '1', 'CLOSE_SKIP_INACTIVE_USERS': '0'})
    rec = log.emit(cg.EV_RECEIVED, master_trade_id=TID)
    assert 'config_sha' in cg.REQUIRED_EVENT_FIELDS
    assert isinstance(rec['config_sha'], str) and len(rec['config_sha']) == 64
    assert rec['config_sha'] == make_log(tmp_path, config={'CLOSE_SKIP_INACTIVE_USERS': '0',
                                                           'CLOSE_JOBS_ENABLED': '1'}).config_sha
    assert make_log(tmp_path).emit(cg.EV_RECEIVED)['config_sha'] == 'unknown'
