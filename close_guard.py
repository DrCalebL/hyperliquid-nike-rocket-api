"""close_guard: the master CLOSE contract (protocol 2) and the per-position close planner, shared by both follower
services.

Byte-identical in hyperliquid-nike-rocket-api (original) and kraken-nike-rocket-api (copy); run_gates compares the
sha256. Never edit one copy alone. No DB and no exchange calls: stdlib + bracket_guard + follower_guard. The only I/O
is CloseEventLog's append to its own JSON-lines file (section 21), which never raises.

Spec: massive-rocket-algos docs/superpowers/specs/2026-10-06-t1-live-design.md, sections 19.3 (wire contract v2,
fixture tests/fixtures/close_v2.json), 19.6/19.7 (plan_close_step, job constants) and 21 (logging).

- validate_close_payload(d, known_symbols, now_ms) -> None or the 422 reason (it starts with the failing field).
  /api/broadcast-close calls it after verify_master_key and before any DB write.
- plan_close_step(row, venue_pos, payload_side, *, lot) -> CloseStep: what execute_close does next for one
  open_positions row, from a fresh row read and a fresh venue read. Only CLOSE acts; every other code places no
  order. A read error (bracket_guard.UNKNOWN or an unrecognised shape) never acts. venue_pos None is FLAT
  (bracket_guard's convention): callers must pass bg.UNKNOWN, never None, on a read error.
  The close is sized against the row's REMAINDER, row.quantity - row.close_fill_qty, so a retry can never close
  a position the user added by hand: execute_close must write close_fill_qty CUMULATIVELY after every fill.
- CloseEventLog: structured follower close events -> logs/close_events.jsonl (+ the 'close_events' logger, which is
  what Railway keeps), redacted, size-rotated, never raises.
"""
import datetime as _dt
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import threading
import time
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal, InvalidOperation

import bracket_guard as bg
import follower_guard as fg

# ---- wire contract v2 (19.3) ----
CLOSE_PROTOCOL = 2
CLOSE_ACTION = 'CLOSE'
CLOSE_SIDES = ('BUY', 'SELL')
CLOSE_REASONS = frozenset({'time_exit'})
CLOSE_MAX_PAST_MS = 48 * 3_600_000
CLOSE_MAX_FUTURE_MS = 5 * 60_000
_TRADE_ID_RE = re.compile(r'[0-9a-f]{32}')

# ---- close jobs (19.6 / 19.7) ----
CLOSE_JOB_MAX_ATTEMPTS = 6
CLOSE_JOB_MAX_AGE_S = 86400
CLOSE_BUSY_WINDOW_S = 180
JOB_STATUSES = ('pending', 'deferred', 'done', 'failed', 'skipped', 'expired')
JOB_OPEN_STATUSES = ('pending', 'deferred')

# plan_close_step codes. Additions to the six codes of 19.6, none of which places an order:
# - FAIL_BAD_QTY (row quantity, close_fill_qty or lot unusable, or the row is under one lot): job failed + critical.
# - DONE_ROW_SHARE_CLOSED (QC): close_fill_qty already covers the row (under one lot left). Whatever the venue still
#   holds is not ours (the user added it by hand): job done, alarm CLOSE_RESIDUAL_NOT_OURS (warning), then cancel
#   the row's SL by recorded id (19.7 step 8) so it can never close the user's own position.
# - DONE_DUST (QC): the venue holds less than one lot (e.g. after a partial TP): nothing closable; job done, alarm
#   CLOSE_DUST (warning); the legs stay with the monitor/guardian.
DONE_ROW_INACTIVE = 'DONE_ROW_INACTIVE'
DEFER_RECOVERY = 'DEFER_RECOVERY'
RETRY_READ_UNKNOWN = 'RETRY_READ_UNKNOWN'
DONE_ALREADY_FLAT = 'DONE_ALREADY_FLAT'
FAIL_SIDE_MISMATCH = 'FAIL_SIDE_MISMATCH'
FAIL_BAD_QTY = 'FAIL_BAD_QTY'
DONE_ROW_SHARE_CLOSED = 'DONE_ROW_SHARE_CLOSED'
DONE_DUST = 'DONE_DUST'
CLOSE = 'CLOSE'
STEP_CODES = (DONE_ROW_INACTIVE, DEFER_RECOVERY, RETRY_READ_UNKNOWN, DONE_ALREADY_FLAT, FAIL_SIDE_MISMATCH,
              FAIL_BAD_QTY, DONE_ROW_SHARE_CLOSED, DONE_DUST, CLOSE)


def is_master_trade_id(v):
    """True for exactly 32 lowercase hex characters (uuid4().hex), nothing else."""
    return isinstance(v, str) and _TRADE_ID_RE.fullmatch(v) is not None


def _strict_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def validate_close_payload(d, known_symbols, now_ms):
    """None when the CLOSE payload is acceptable, else the 422 reason (prefixed with the failing field):
    protocol == 2 (int); action == 'CLOSE'; master_trade_id 32 lowercase hex; symbol a non-empty str in
    known_symbols (the follower's symbol map, entry-broadcast format such as 'ADA/USDT'); side BUY|SELL (the side of
    the ENTRY being closed); reason in CLOSE_REASONS; master_closed_ms an int in [now_ms - 48 h, now_ms + 5 min].
    Unknown extra keys are ignored (forward compatible). Never raises, never mutates d."""
    if not isinstance(d, dict):
        return 'payload must be a JSON object'
    p = d.get('protocol')
    if not _strict_int(p) or p != CLOSE_PROTOCOL:
        return 'protocol must be the integer %d' % CLOSE_PROTOCOL
    if d.get('action') != CLOSE_ACTION:
        return 'action must be %s' % CLOSE_ACTION
    if not is_master_trade_id(d.get('master_trade_id')):
        return 'master_trade_id must be 32 lowercase hex characters'
    sym = d.get('symbol')
    if not isinstance(sym, str) or not sym:
        return 'symbol must be a non-empty string'
    try:
        known = known_symbols is not None and sym in known_symbols
    except TypeError:
        known = False
    if not known:
        return 'symbol is not traded by this service'
    if d.get('side') not in CLOSE_SIDES:
        return 'side must be BUY or SELL'
    if not isinstance(d.get('reason'), str) or d.get('reason') not in CLOSE_REASONS:
        return 'reason must be one of %s' % ', '.join(sorted(CLOSE_REASONS))
    ms = d.get('master_closed_ms')
    if not _strict_int(ms):
        return 'master_closed_ms must be an integer (epoch milliseconds)'
    if not (now_ms - CLOSE_MAX_PAST_MS <= ms <= now_ms + CLOSE_MAX_FUTURE_MS):
        return 'master_closed_ms must be within 48 h before and 5 min after now'
    return None


# ---- the close planner (19.6 / 19.7) ----

@dataclass
class CloseStep:
    code: str
    qty: float = None           # CLOSE only: base units, floored to the lot, <= min(venue, row remainder)
    exit_side: str = None       # CLOSE only: 'sell' closes a long, 'buy' closes a short
    venue_qty: float = None
    row_qty: float = None
    close_fill_qty: float = None    # what earlier attempts already closed for this row (0 when none)
    residual: bool = False      # CLOSE only: the venue holds more than qty, so the close cannot reach flat
    detail: str = ''

    @property
    def acts(self):
        return self.code == CLOSE

    @property
    def reduce_only(self):
        return True             # every order of the close path is reduce-only (I1); there is no other kind

    def as_log(self):
        return {'step': self.code, 'qty': self.qty, 'exit_side': self.exit_side, 'reduce_only': self.reduce_only,
                'residual': self.residual, 'venue_qty': self.venue_qty, 'row_qty': self.row_qty,
                'close_fill_qty': self.close_fill_qty, 'detail': self.detail}


def _row_get(row, key):
    try:
        return row.get(key)
    except AttributeError:
        try:
            return row[key]
        except (KeyError, IndexError, TypeError):
            return None


def _dec(x):
    try:
        return Decimal(repr(float(x)))
    except (TypeError, ValueError, InvalidOperation):
        return None


def plan_close_step(row, venue_pos, payload_side, *, lot):
    """One decision for execute_close (19.7), in this order:
    1. row status not in ACTIVE_STATUSES -> DONE_ROW_INACTIVE; 'needs_recovery' -> DEFER_RECOVERY (the recovery lane
       owns it) - both before looking at the venue read;
    2. venue_pos UNKNOWN or not a valid bracket_guard.Pos -> RETRY_READ_UNKNOWN (a read error never acts);
    3. venue_pos None or qty 0 -> DONE_ALREADY_FLAT;
    4. venue side != row side, or row side != payload_side ('BUY'|'SELL') -> FAIL_SIDE_MISMATCH;
    5. row quantity, close_fill_qty (None/'' = 0) or lot unusable -> FAIL_BAD_QTY;
    6. close_fill_qty > 0 and the row remainder (quantity - close_fill_qty) is under one lot ->
       DONE_ROW_SHARE_CLOSED (what the venue still holds is not ours);
    7. venue qty under one lot -> DONE_DUST; row quantity under one lot -> FAIL_BAD_QTY;
    8. CLOSE(qty = min(venue qty, row remainder) floored to lot), exit_side opposite the position, residual=True
       when the venue holds more than qty (the caller keeps the SL and alarms CLOSE_RESIDUAL if not flat
       afterwards; the next attempt sees the larger close_fill_qty and stops at the row's share).
    Pure: never raises on bad row or venue values, never mutates row."""
    status = _row_get(row, 'status')
    if status not in fg.ACTIVE_STATUSES:
        return CloseStep(DONE_ROW_INACTIVE, detail='row status %r' % (status,))
    if status == 'needs_recovery':
        return CloseStep(DEFER_RECOVERY, detail='row needs_recovery: the recovery lane owns it')
    if venue_pos is None:
        return CloseStep(DONE_ALREADY_FLAT, venue_qty=0.0, detail='venue flat')
    if not isinstance(venue_pos, bg.Pos):
        return CloseStep(RETRY_READ_UNKNOWN, detail='venue read unknown')
    vq = bg._f(venue_pos.qty)
    if vq is None or vq < 0:
        return CloseStep(RETRY_READ_UNKNOWN, detail='venue qty unreadable')
    if vq == 0:
        return CloseStep(DONE_ALREADY_FLAT, venue_qty=0.0, detail='venue flat')
    if venue_pos.side not in (1, -1) or isinstance(venue_pos.side, bool):
        return CloseStep(RETRY_READ_UNKNOWN, venue_qty=vq, detail='venue side unreadable')
    rs = fg.row_side({'side': _row_get(row, 'side')})
    ps = {'BUY': 1, 'SELL': -1}.get(payload_side) if isinstance(payload_side, str) else None
    if rs == 0 or ps is None or venue_pos.side != rs or rs != ps:
        return CloseStep(FAIL_SIDE_MISMATCH, venue_qty=vq,
                         detail='venue side %s, row side %r, payload side %r' % (venue_pos.side,
                                                                                  _row_get(row, 'side'), payload_side))
    rq = bg._pos(_row_get(row, 'quantity'))
    lt = None if isinstance(lot, bool) else bg._pos(lot)
    if rq is None or lt is None:
        return CloseStep(FAIL_BAD_QTY, venue_qty=vq, row_qty=rq,
                         detail='row quantity or lot unusable (lot %r)' % (lot,))
    cf_raw = _row_get(row, 'close_fill_qty')
    cf = 0.0 if cf_raw is None or cf_raw == '' else (None if isinstance(cf_raw, bool) else bg._f(cf_raw))
    if cf is None or cf < 0 or cf != cf or cf in (float('inf'), float('-inf')):
        return CloseStep(FAIL_BAD_QTY, venue_qty=vq, row_qty=rq,
                         detail='close_fill_qty unusable (%r)' % (cf_raw,))
    dv, dr, dl, dcf = _dec(vq), _dec(rq), _dec(lt), _dec(cf)
    remainder = dr - dcf
    if dcf > 0 and remainder < dl:
        return CloseStep(DONE_ROW_SHARE_CLOSED, venue_qty=vq, row_qty=rq, close_fill_qty=cf,
                         detail='row share already closed; the venue rest is not ours')
    if dv < dl:
        return CloseStep(DONE_DUST, venue_qty=vq, row_qty=rq, close_fill_qty=cf,
                         detail='venue holds less than one lot')
    lots = (min(dv, remainder) / dl).to_integral_value(rounding=ROUND_FLOOR)
    dq = lots * dl
    if dq <= 0:
        return CloseStep(FAIL_BAD_QTY, venue_qty=vq, row_qty=rq, close_fill_qty=cf,
                         detail='less than one lot to close')
    qty = float(dq)
    return CloseStep(CLOSE, qty=qty, exit_side='sell' if rs > 0 else 'buy', venue_qty=vq, row_qty=rq,
                     close_fill_qty=cf, residual=dv > dq, detail='reduce-only close')


# ---- section 21: follower close event log ----

LOG_PATH_DEFAULT = os.path.join('logs', 'close_events.jsonl')
REQUIRED_EVENT_FIELDS = ('ts_utc', 'host', 'venue', 'service', 'mode', 'event', 'severity', 'master_trade_id',
                         'code_sha', 'config_sha')
_ENVELOPE = frozenset(REQUIRED_EVENT_FIELDS)

EV_RECEIVED = 'CLOSE_RECEIVED'                  # a valid CLOSE arrived (master_trade_id, matched rows)
EV_DUPLICATE = 'CLOSE_DUPLICATE'                # repeat id: answered duplicate, nothing new written
EV_NO_MATCH = 'CLOSE_NO_MATCH'                  # followers_matched == 0
EV_PAYLOAD_REJECTED = 'CLOSE_PAYLOAD_REJECTED'  # 422 (the validator's reason)
EV_JOB_CREATED = 'CLOSE_JOB_CREATED'
EV_JOB_STEP = 'CLOSE_JOB_STEP'                  # read / cancel TP / close / verify flat / cancel SL (+ as_log())
EV_JOB_OUTCOME = 'CLOSE_JOB_OUTCOME'            # job status change: done / deferred / failed / ...
EV_JOB_SKIPPED = 'CLOSE_JOB_SKIPPED'            # skip reason (user in flight, inactive user policy, kill switch)
EV_SIDE_MISMATCH = 'CLOSE_SIDE_MISMATCH'
EV_RESIDUAL = 'CLOSE_RESIDUAL'
EV_LEG_LEFT = 'CLOSE_LEG_LEFT'
EV_JOB_EXPIRED = 'CLOSE_JOB_EXPIRED'
EV_JOB_FAILED = 'CLOSE_JOB_FAILED'
EV_RESIDUAL_NOT_OURS = 'CLOSE_RESIDUAL_NOT_OURS'  # DONE_ROW_SHARE_CLOSED: the venue rest belongs to the user
EV_DUST = 'CLOSE_DUST'                          # DONE_DUST: under one lot left on the venue
EVENT_SEVERITY = {
    EV_RECEIVED: 'INFO', EV_DUPLICATE: 'INFO', EV_NO_MATCH: 'INFO', EV_PAYLOAD_REJECTED: 'WARN',
    EV_JOB_CREATED: 'INFO', EV_JOB_STEP: 'INFO', EV_JOB_OUTCOME: 'INFO', EV_JOB_SKIPPED: 'INFO',
    EV_SIDE_MISMATCH: 'CRITICAL', EV_RESIDUAL: 'CRITICAL', EV_LEG_LEFT: 'WARN', EV_JOB_EXPIRED: 'CRITICAL',
    EV_JOB_FAILED: 'CRITICAL', EV_RESIDUAL_NOT_OURS: 'WARN', EV_DUST: 'WARN',
}
_SEVERITIES = {'debug': 'DEBUG', 'info': 'INFO', 'warn': 'WARN', 'warning': 'WARN', 'error': 'ERROR',
               'critical': 'CRITICAL'}
_PY_LEVEL = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARN': logging.WARNING, 'ERROR': logging.ERROR,
             'CRITICAL': logging.CRITICAL}

REDACTED = '[REDACTED]'
# ---- shared redactor (QC 2026-10-06): the same rules and the same adversarial test corpus in the master
# (massive-rocket-algos t1_eventlog.py) and both followers (this file). ----
# A dict value is masked when its KEY looks secret. A string is scanned for: bearer values; labelled secrets in text
# ('X-Master-Key: v', '{"api_key": "v"}', 'masterKey=v', 'FOO_KEY=v', '?key=v', 'password is v', 'sig=v');
# 0x + 64 hex and bare 64 hex (private keys); long mixed-case+digit tokens (base64 / API secrets).
# Readable on purpose: 32-hex trade ids, 40-hex addresses, paths/URLs (many separators), and a 64-hex value under
# a sha/hash-named key (inputs_sha256, code_sha, tx_hash, ...).
SECRET_KEY_RE = re.compile(r'(?i)(passw|passphrase|secret|token|api[_-]?key|apikey|private|credential|authori[sz]|'
                           r'master[_-]?key|signature|cookie|mnemonic|seed[_-]?phrase|(?:^|[_-])key$)')
SAFE_HEX_KEY_RE = re.compile(r'(?i)(sha(256)?|hash|digest|txid)$')
_HEX64_FULL_RE = re.compile(r'(0x)?[0-9a-fA-F]{64}')
_BEARER_RE = re.compile(r'(?i)\b(bearer\s+)[^\s"\',;&}\]]+')
_LABELLED_RE = re.compile(
    r'(?i)(?<![A-Za-z0-9])([A-Za-z0-9_-]*(?:key|secret|token|passw(?:or)?d|passphrase|signature|authorization|cookie'
    r'|credentials?|mnemonic)|sig)(["\']?\s*(?:[:=]|\s+is\s+)\s*["\']?)(?:(?:bearer|basic)\s+)?(?!\[REDACTED\])([^\s"\',;&}\]]+)')
_HEX64_RE = re.compile(r'(?<![0-9A-Za-z])(?:0x)?[0-9a-fA-F]{64}(?![0-9A-Za-z])')
_LONG_RUN_RE = re.compile(r'[A-Za-z0-9+/_=-]{32,}')
_ADDRESS_RE = re.compile(r'0x[0-9a-fA-F]{40}')
SECRET_VALUE_RE = re.compile(r'(?i)\bbearer\s+(?!\[REDACTED\])\S|(?<![0-9A-Za-z])(?:0x)?[0-9a-fA-F]{64}'
                             r'(?![0-9A-Za-z])')


def _long_run(m):
    s = m.group(0)
    core = s.rstrip('=')
    if _ADDRESS_RE.fullmatch(core):
        return s
    if not (re.search('[A-Z]', core) and re.search('[a-z]', core) and re.search('[0-9]', core)):
        return s
    if sum(core.count(c) for c in '/_-') > 3:           # paths, URLs and slugs, not random tokens
        return s
    return REDACTED


def redact_text(s, secrets=()):
    """s with every secret-looking substring replaced by REDACTED (plus any literal in secrets)."""
    for lit in secrets or ():
        if lit:
            s = s.replace(lit, REDACTED)
    s = _BEARER_RE.sub(lambda m: m.group(1) + REDACTED, s)
    s = _LABELLED_RE.sub(lambda m: m.group(1) + m.group(2) + REDACTED, s)
    s = _HEX64_RE.sub(REDACTED, s)
    return _LONG_RUN_RE.sub(_long_run, s)


def redact(obj, secrets=()):
    """A copy of obj: values under a secret-looking key -> REDACTED, strings scanned by redact_text(); a 64-hex
    value under a sha/hash-named key stays readable. Never raises on JSON-like data."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            ks = str(k)
            if SECRET_KEY_RE.search(ks):
                out[k] = REDACTED
            elif isinstance(v, str) and SAFE_HEX_KEY_RE.search(ks) and _HEX64_FULL_RE.fullmatch(v):
                out[k] = v
            else:
                out[k] = redact(v, secrets)
        return out
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [redact(v, secrets) for v in obj]
    if isinstance(obj, str):
        return redact_text(obj, secrets)
    return obj


_log = logging.getLogger('close_events')


def _open_append(path):
    return open(path, 'a', encoding='utf-8', newline='\n')


def _free_bytes(path):
    return shutil.disk_usage(path).free


def _iso_ms(ts):
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.') + '%03dZ' % (
        int(round(ts * 1000)) % 1000)


class CloseEventLog:
    """Section 21 follower log: one JSON object per line. emit() never raises; when the file cannot be written
    (disk full, bad path, free space under min_free_bytes) the record still goes to the 'close_events' logger and
    `dropped` counts the lost file line. Size rotation keeps `backups` older files (path.1 newest)."""

    def __init__(self, path=LOG_PATH_DEFAULT, *, venue, service, mode='live', host=None, code_sha=None,
                 config=None, max_bytes=5_000_000, backups=5, min_free_bytes=50_000_000, clock=time.time,
                 free_bytes_fn=None):
        self.path = path
        self.venue, self.service, self.mode = venue, service, mode
        if host is None:
            try:
                host = socket.gethostname()
            except Exception:
                host = 'unknown'
        self.host = host
        self.code_sha = code_sha or os.getenv('RAILWAY_GIT_COMMIT_SHA') or 'unknown'
        # config: the close-path env flags (CLOSE_JOBS_ENABLED, CLOSE_SKIP_INACTIVE_USERS, ...); their sha goes in
        # every record so a flag flip is visible in the log. Log the flags themselves once at startup.
        try:
            self.config_sha = (hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode('utf-8'))
                               .hexdigest() if config is not None else 'unknown')
        except Exception:
            self.config_sha = 'unknown'
        self.max_bytes, self.backups, self.min_free_bytes = max_bytes, backups, min_free_bytes
        self._clock = clock
        self._free = free_bytes_fn or _free_bytes
        self._lock = threading.Lock()
        self.dropped = 0

    def emit(self, event, severity=None, master_trade_id=None, **fields):
        """Write one event; returns the record as written (after redaction), or None only if even building it
        failed. Field names that collide with the envelope are kept as 'field_<name>'."""
        try:
            sev = _SEVERITIES.get(str(severity).lower()) if severity is not None else None
            sev = sev or EVENT_SEVERITY.get(event, 'INFO')
            try:
                ts = float(self._clock())
            except Exception:
                ts = time.time()
            rec = {'ts_utc': _iso_ms(ts), 'host': self.host, 'venue': self.venue, 'service': self.service,
                   'mode': self.mode, 'event': str(event), 'severity': sev, 'master_trade_id': master_trade_id,
                   'code_sha': self.code_sha, 'config_sha': self.config_sha}
            for k, v in fields.items():
                rec['field_%s' % k if k in _ENVELOPE else k] = v
            line = json.dumps(redact(rec), default=str, ensure_ascii=True)
            out = json.loads(line)
        except Exception:
            _log.exception('close event could not be built: %r', event)
            return None
        try:
            _log.log(_PY_LEVEL.get(sev, logging.INFO), line)
        except Exception:
            pass
        with self._lock:
            try:
                self._write(line)
            except Exception:
                self.dropped += 1
        return out

    def _write(self, line):
        d = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(d, exist_ok=True)
        try:
            free = self._free(d)
        except Exception:
            free = None
        if free is not None and free < self.min_free_bytes:
            raise OSError('free disk space under min_free_bytes')
        data = line + '\n'
        try:
            size = os.path.getsize(self.path)
        except OSError:
            size = 0
        if size and size + len(data) > self.max_bytes:
            self._rotate()
        with _open_append(self.path) as f:
            f.write(data)

    def _rotate(self):
        if self.backups <= 0:
            os.remove(self.path)
            return
        oldest = '%s.%d' % (self.path, self.backups)
        if os.path.exists(oldest):
            os.remove(oldest)
        for i in range(self.backups - 1, 0, -1):
            src = '%s.%d' % (self.path, i)
            if os.path.exists(src):
                os.replace(src, '%s.%d' % (self.path, i + 1))
        os.replace(self.path, self.path + '.1')
