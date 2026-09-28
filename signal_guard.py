"""signal_guard: validation of the master's broadcast payload (I11), shared by both follower services.

Byte-identical in kraken-nike-rocket-api and hyperliquid-nike-rocket-api (builder B1; B7/B8 never edit it). Pure:
math + os. follower_endpoints.broadcast_signal calls validate_signal_payload() as its FIRST statement, before its
try block and any DB write, and answers HTTPException(422, reason) when it returns a reason.
Spec: change C33 (signal_guard unchanged from rev 2) of docs/superpowers/specs/2026-09-28-wave1-phase0-safety-design.md.
"""
import math
import os

MAX_LEVERAGE = 50.0


def _num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def validate_signal_payload(action, entry_price, stop_loss, take_profit, leverage, risk_pct, max_risk=None):
    """None when the payload is acceptable, else the 422 reason: action BUY/SELL; finite numbers;
    0 < risk_pct <= max_risk (MAX_BROADCAST_RISK_PCT, default 0.03); 1 <= leverage <= 50;
    BUY 0 < SL < entry < TP; SELL 0 < TP < entry < SL."""
    if max_risk is None:
        max_risk = _num(os.getenv('MAX_BROADCAST_RISK_PCT', '0.03')) or 0.03
    act = str(action or '').upper()
    if act not in ('BUY', 'SELL'):
        return 'action must be BUY or SELL'
    e, sl, tp, lev, r = (_num(x) for x in (entry_price, stop_loss, take_profit, leverage, risk_pct))
    if None in (e, sl, tp, lev, r):
        return 'entry_price, stop_loss, take_profit, leverage and risk_pct must be finite numbers'
    if not (0.0 < r <= max_risk):
        return 'risk_pct must be in (0, %s]' % max_risk
    if not (1.0 <= lev <= MAX_LEVERAGE):
        return 'leverage must be in [1, 50]'
    if act == 'BUY' and not (0.0 < sl < e < tp):
        return 'BUY requires 0 < stop_loss < entry_price < take_profit'
    if act == 'SELL' and not (0.0 < tp < e < sl):
        return 'SELL requires 0 < take_profit < entry_price < stop_loss'
    return None
