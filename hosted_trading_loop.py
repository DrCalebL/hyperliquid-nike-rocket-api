"""
Nike Rocket - Hosted Trading Loop (Hyperliquid) v1.0
==================================================================================
Background task that polls for signals and executes trades for ALL active users.
Runs on Railway as part of main.py startup.

Hyperliquid SDK trading loop.
- Polls for latest signal ONCE per cycle (batched, not per-user)
- Checks which users haven't acknowledged the signal
- Decrypts user HL private key from database
- Calculates position size (2-3% risk formula)
- Executes 3-order bracket (Entry IOC + TP trigger + SL trigger) WITH RETRY LOGIC
- ORDER RETRY: 3 attempts with exponential backoff (1s -> 2s -> 4s)
- Logs all activity for admin dashboard
- ENFORCES 30-DAY BILLING: Skips users with overdue invoices
- ERROR LOGGING: All errors logged to error_logs table

- Uses hyperliquid SDK instead of CCXT
- Symbol: "ADA/USDT" → "ADA" (simple strip)
- Balance: info.user_state(wallet) → marginSummary.accountValue
- Orders: exchange.order() with IOC/trigger params
- Leverage: exchange.update_leverage()
- Credentials: private key + wallet address (not API key + secret)
- Anti-abuse: wallet address IS the fingerprint

SCALED FOR 5000+ USERS:
- Random shuffle for fair execution order
- Parallel batch execution (25 users per batch)
- 50ms stagger delay between batches

Author: Nike Rocket Team
"""

import asyncio
import functools
import inspect
import logging
import math
import json
import random
import os
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Optional, Dict, List

from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from cryptography.fernet import Fernet

import follower_guard
import hl_rate
from order_utils import (
    place_entry_order_with_retry,
    place_tp_order_with_retry,
    place_sl_order_with_retry,
    cancel_order_verified,
    first_status,
    notify_admin,
    notify_entry_failed,
    notify_bracket_incomplete,
    notify_signal_invalid,
    notify_signal_invalid_values,
    notify_critical_error
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('HOSTED_TRADING')


async def log_error_to_db(pool, api_key: str, error_type: str, error_message: str, context: Optional[Dict] = None):
    """Log error to error_logs table for admin dashboard visibility"""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO error_logs (api_key, error_type, error_message, context, created_at) 
                   VALUES ($1, $2, $3, $4, NOW())""",
                api_key[:20] + "..." if api_key and len(api_key) > 20 else api_key,
                error_type,
                error_message[:500] if error_message else None,
                json.dumps(context) if context else None
            )
    except Exception as e:
        logger.error(f"Failed to log error to DB: {e}")


# ==================== CONFIGURATION ====================

DEFAULT_RISK_PERCENTAGE = 0.02  # 2% default, signal can override
POLL_INTERVAL_SECONDS = 10
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "50"))  # 50 basis points = 0.5%
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"
GUARDIAN_MODES = ('off', 'shadow', 'enforce')    # HL follower GUARDIAN_MODE values (follower_guard.guardian_mode)
CLOSE_LADDER = (0.02, 0.05, 0.10)                 # C41: reduce-only IOC slippage ladder, fresh all_mids each attempt


def convert_symbol_to_hl(api_symbol: str) -> str:
    """
    Convert API symbol format to Hyperliquid coin format.
    
    "BTC/USDT" → "BTC"
    "ADA/USDT" → "ADA"
    "ADA" → "ADA"
    """
    if '/' in api_symbol:
        return api_symbol.split('/')[0].upper()
    
    # Handle legacy format if accidentally sent
    base = api_symbol.upper()
    base = base.replace('PF_', '').replace('USD', '').replace('USDT', '')
    return base


def floor_to_lot(size: float, sz_decimals: int) -> float:
    """Floor `size` to the lot grid (sz_decimals) on the float's shortest decimal repr (QC correctness-followers-5):
    math.floor(0.29 * 100) / 100 == 0.28 in binary floats, which bracketed an exact fill one lot short."""
    return float(Decimal(repr(float(size))).quantize(Decimal(1).scaleb(-int(sz_decimals)), rounding=ROUND_DOWN))


def coin_szi(state, coin: str) -> float:
    """Signed position size of `coin` in an HL user_state (0.0 when flat). ValueError on an unrecognised shape."""
    if not isinstance(state, dict) or not isinstance(state.get('assetPositions'), list):
        raise ValueError('unrecognised user_state shape')
    for p in state['assetPositions']:
        pos = (p or {}).get('position') or {}
        if pos.get('coin') == coin:
            szi = float(pos.get('szi') or 0)
            if szi != 0:
                return szi
    return 0.0


def coin_entry_px(state, coin: str) -> Optional[float]:
    """entryPx of the open `coin` position in an HL user_state, or None."""
    try:
        for p in state.get('assetPositions') or []:
            pos = (p or {}).get('position') or {}
            if pos.get('coin') == coin and float(pos.get('szi') or 0) != 0:
                px = float(pos.get('entryPx') or 0)
                return px if px > 0 else None
    except (AttributeError, TypeError, ValueError):
        return None
    return None


async def hl_close_with_verification(exchange, info, wallet, coin, is_buy_to_close, qty, round_price,
                                     source='trading', executor=None, errors=None):
    """
    C41: reduce-only IOC close ladder (2% / 5% / 10% off a fresh all_mids each attempt, 1 s then 2 s apart).
    An attempt succeeds only when the envelope is 'ok' AND statuses[0] contains 'filled' (HL answers 'ok' for
    an unfilled IOC). QC money-followers-8: a 'filled' for less than the quantity re-reads the position and the
    ladder continues on the remainder. A final user_state re-read with any remaining szi makes the close a failure;
    a flat re-read after any fill is a success. `source` labels the hl_rate charges (charges never wait);
    `executor` runs the SDK calls (the guardian's pool; the default executor when None); `errors` (a list)
    collects each failed attempt's answer (the guardian feeds them to _note_mutation_error). A failed close
    e-mails EMERGENCY_CLOSE_FAILED (critical) only for the trading path: the guardian raises its own alarm,
    deduped hourly (QC voice-6). Returns (ok, close_oid).
    """
    loop = asyncio.get_running_loop()
    ok, close_oid, filled_any = False, None, False
    for i, slip in enumerate(CLOSE_LADDER):
        if i:
            await asyncio.sleep(i)
        try:
            hl_rate.charge(hl_rate.W_ALL_MIDS, source)
            mids = await loop.run_in_executor(executor, info.all_mids)
            mid = float((mids or {}).get(coin) or 0)
            if mid <= 0:
                logger.critical(f"   ❌ EMERGENCY CLOSE attempt {i + 1}: no mid price for {coin}")
                if errors is not None:
                    errors.append(f"no mid price for {coin}")
                continue
            px = round_price(mid * (1 + slip) if is_buy_to_close else mid * (1 - slip))
            hl_rate.charge(hl_rate.W_ACTION, source)
            result = await loop.run_in_executor(executor, functools.partial(
                exchange.order, coin, is_buy_to_close, qty, px, {"limit": {"tif": "Ioc"}}, True))
            st = first_status(result)
            if (result or {}).get('status') == 'ok' and isinstance(st, dict) and 'filled' in st:
                filled_any = True
                close_oid = (st.get('filled') or {}).get('oid')
                try:
                    total = float((st.get('filled') or {}).get('totalSz'))
                except (TypeError, ValueError):
                    total = None
                if total is None or total >= qty - 1e-9:
                    ok = True
                    break
                hl_rate.charge(hl_rate.W_USER_STATE, source)
                state = await loop.run_in_executor(executor, info.user_state, wallet)
                left = abs(coin_szi(state, coin))
                logger.critical(f"   ⚠️ EMERGENCY CLOSE attempt {i + 1} ({slip:.0%}) partial: {total} of {qty}, "
                                f"{left} left")
                if left == 0:
                    ok = True
                    break
                qty = left
                continue
            logger.critical(f"   ❌ EMERGENCY CLOSE attempt {i + 1} ({slip:.0%}) not filled: {str(result)[:200]}")
            if errors is not None:
                errors.append(str(result)[:300])
        except Exception as e:
            logger.critical(f"   ❌ EMERGENCY CLOSE attempt {i + 1} ({slip:.0%}) raised: {e}")
            if errors is not None:
                errors.append(repr(e)[:300])
    try:
        hl_rate.charge(hl_rate.W_USER_STATE, source)
        state = await loop.run_in_executor(executor, info.user_state, wallet)
        if coin_szi(state, coin) != 0:
            ok = False
        elif filled_any:
            ok = True
    except Exception as e:
        logger.critical(f"   ❌ EMERGENCY CLOSE re-read failed for {coin}: {e}")
        ok = False
    if not ok:
        logger.critical(f"   🚨 EMERGENCY_CLOSE_FAILED {coin}: the position may still be open")
        if source != hl_rate.GUARDIAN_SOURCE:
            await notify_admin(
                title="EMERGENCY_CLOSE_FAILED (HL)",
                details={"Wallet": str(wallet)[:12] + "...", "Coin": coin, "Quantity": qty, "Source": source,
                         "Action Required": "Check the account; needs_recovery rows are retried by the guardian"},
                level="error")
    return ok, close_oid


def exchange_ctor_takes_meta(cls=None) -> bool:
    """R3-M3: True when the installed SDK's Exchange(...) accepts meta= and spot_meta= (its constructor then makes
    no request). `cls` defaults to this module's Exchange (tests patch it)."""
    try:
        params = inspect.signature((cls or Exchange).__init__).parameters
    except (TypeError, ValueError):
        return False
    return 'meta' in params and 'spot_meta' in params


class HostedTradingLoop:
    """
    Main hosted trading loop that executes for all users on Hyperliquid.
    """
    
    ENTRY_CONFIRM_TIMEOUT_S = 8.0   # C40.2: without totalSz, poll user_state every 1 s for up to 8 s
    ENTRY_CONFIRM_POLL_S = 1.0
    VERIFY_FLAT_GAP_S = 2.0         # _verified_flat: two symbol-matched user_state reads 2 s apart

    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.active_exchanges = {}  # Cache: {api_key: (info, exchange, wallet_address)}
        self.asset_meta = {}  # Cache: {coin: {szDecimals, ...}}
        self.logger = logging.getLogger('HOSTED_TRADING')
        self._hl_meta = None        # raw meta() dict, passed as Exchange(meta=...) (R3-M3)
        self._hl_spot_meta = None   # spot_meta() dict, passed as Exchange(spot_meta=...)

        # Initialize HL info client (shared, read-only)
        base_url = constants.TESTNET_API_URL if USE_TESTNET else constants.MAINNET_API_URL
        hl_rate.charge(hl_rate.W_CTOR_UNCACHED, 'trading')   # Info(...) fetches spot_meta + meta
        self.info = Info(base_url, skip_ws=True)
        self.base_url = base_url

        # Load asset metadata for size/price rounding, and the cached constructor metadata
        self._load_asset_meta()
        self._load_spot_meta()
        self.logger.info(f"HL_SDK ctor_meta={self._cached_ctor_meta()}")

    def _load_asset_meta(self):
        """Load asset metadata for precision rounding (the raw dict is kept for Exchange(meta=...))"""
        try:
            hl_rate.charge(hl_rate.W_META, 'trading')
            meta = self.info.meta()
            if meta and 'universe' in meta:
                self._hl_meta = meta
                for asset in meta['universe']:
                    self.asset_meta[asset['name']] = asset
                self.logger.info(f"📋 Loaded metadata for {len(self.asset_meta)} assets")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load asset metadata: {e}")

    def _load_spot_meta(self):
        """Load spot_meta once, so Exchange(...) is built without a constructor request (R3-M3)"""
        try:
            hl_rate.charge(hl_rate.W_SPOT_META, 'trading')
            self._hl_spot_meta = self.info.spot_meta()
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load spot metadata: {e}")

    def _cached_ctor_meta(self) -> bool:
        """True when get_or_create_exchange passes cached meta= and spot_meta= (zero constructor requests)."""
        return self._hl_meta is not None and self._hl_spot_meta is not None and exchange_ctor_takes_meta()

    def round_size(self, coin: str, size: float) -> float:
        """Round size to asset's szDecimals precision"""
        meta = self.asset_meta.get(coin, {})
        return floor_to_lot(size, meta.get('szDecimals', 2))
    
    def round_price(self, price: float, sig_figs: int = 5) -> float:
        """Round price to significant figures (HL standard)"""
        if price == 0:
            return 0
        magnitude = math.floor(math.log10(abs(price)))
        factor = 10 ** (sig_figs - 1 - magnitude)
        return round(price * factor) / factor
    
    async def get_pending_signals_batched(self) -> List[Dict]:
        """
        OPTIMIZED: Get all pending signals with user info in ONE query.
        HL SDK version.
        """
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    u.id as user_id,
                    u.api_key,
                    u.email,
                    u.hl_private_key_encrypted,
                    u.hl_wallet_address,
                    sd.id as delivery_id,
                    s.signal_id,
                    s.action,
                    s.symbol,
                    s.entry_price,
                    s.stop_loss,
                    s.take_profit,
                    s.leverage,
                    COALESCE(s.risk_pct, 0.02) as risk_pct,
                    s.created_at as signal_created_at
                FROM follower_users u
                JOIN signal_deliveries sd ON sd.user_id = u.id
                JOIN signals s ON sd.signal_id = s.id
                WHERE u.agent_active = true
                  AND u.credentials_set = true
                  AND u.access_granted = true
                  AND (
                      u.pending_invoice_id IS NULL 
                      OR u.invoice_due_date IS NULL 
                      OR u.invoice_due_date > CURRENT_TIMESTAMP
                  )
                  AND sd.acknowledged = false
                  AND s.created_at > NOW() - INTERVAL '15 minutes'
                ORDER BY s.created_at DESC
            """)
            
            return [dict(row) for row in rows]
    
    def decrypt_private_key(self, encrypted_key: str) -> str:
        """Decrypt Hyperliquid private key"""
        encryption_key = os.getenv("CREDENTIALS_ENCRYPTION_KEY")
        if not encryption_key:
            raise Exception("CREDENTIALS_ENCRYPTION_KEY not set")
        
        cipher = Fernet(encryption_key.encode())
        return cipher.decrypt(encrypted_key.encode()).decode()
    
    def get_or_create_exchange(self, user: Dict) -> tuple:
        """
        Get or create HL exchange instance for user.
        
        Returns: (info, exchange, wallet_address) tuple
        """
        api_key = user['api_key']
        
        # Return cached
        if api_key in self.active_exchanges:
            return self.active_exchanges[api_key]
        
        if not user.get('hl_private_key_encrypted'):
            raise ValueError(f"No encrypted credentials for user {api_key[:15]}...")
        
        # Decrypt private key
        private_key = self.decrypt_private_key(user['hl_private_key_encrypted'])
        
        if not private_key:
            raise ValueError(f"Failed to decrypt credentials for user {api_key[:15]}...")
        
        # Derive API wallet address from private key
        account = Account.from_key(private_key)
        api_wallet_address = account.address
        
        # Main account address (what we query balances/positions for)
        # For API wallets, this differs from the derived address
        wallet_address = user.get('hl_wallet_address') or api_wallet_address
        
        # Create exchange instance
        # If API wallet differs from main account, pass account_address
        # so the SDK knows to trade on behalf of the main account
        kwargs = {}
        if wallet_address.lower() != api_wallet_address.lower():
            kwargs['account_address'] = wallet_address
        if self._cached_ctor_meta():
            # R3-M3: cached meta + spot_meta -> the SDK constructor makes no request
            exchange = Exchange(account, self.base_url, meta=self._hl_meta, spot_meta=self._hl_spot_meta, **kwargs)
        else:
            hl_rate.charge(hl_rate.W_CTOR_UNCACHED, 'trading')
            exchange = Exchange(account, self.base_url, **kwargs)

        # Cache it
        self.active_exchanges[api_key] = (self.info, exchange, wallet_address)
        
        return self.info, exchange, wallet_address
    
    async def acknowledge_signal(self, delivery_id: int):
        """Mark signal as acknowledged after execution"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE signal_deliveries 
                SET acknowledged = true, 
                    acknowledged_at = NOW(),
                    executed = true,
                    executed_at = NOW()
                WHERE id = $1
            """, delivery_id)
    
    async def get_user_equity(self, info: Info, wallet_address: str) -> float:
        """Get user's Hyperliquid account equity"""
        try:
            hl_rate.charge(hl_rate.W_USER_STATE, 'trading')
            state = info.user_state(wallet_address)
            if state and 'marginSummary' in state:
                return float(state['marginSummary']['accountValue'])
            
            self.logger.warning("Could not determine equity from user_state")
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Error fetching balance: {e}")
            return 0.0
    
    async def check_any_open_positions_or_orders(
        self, info: Info, exchange: Exchange, wallet_address: str,
        user_short: str, user_id: int = None, signal_id: str = None
    ) -> tuple:
        """
        SAFETY CHECK: Verify user has NO open positions or orders.

        Returns: (has_open, reason_string)
        - (True, "reason") → SKIP trade
        - (False, None) → safe to trade

        Wave 1 (C42): FAIL CLOSED. A position-read error, an order-read error or any other error returns
        (True, reason) and logs DEDUP_SKIPPED_READ_ERROR: a flaky read must never stack a second position.
        The calls stay synchronous (trading-path concurrency unchanged); CHECK 2 (any open order) is unchanged.
        HL SDK version.
        """
        try:
            # ===== CHECK 0: Database check for existing position on same signal =====
            if user_id and signal_id:
                async with self.db_pool.acquire() as conn:
                    signal_int_id = await conn.fetchval(
                        "SELECT id FROM signals WHERE signal_id = $1 LIMIT 1", signal_id
                    )

                    if signal_int_id:
                        existing_position = await conn.fetchval("""
                            SELECT COUNT(*) FROM open_positions
                            WHERE user_id = $1 AND signal_id = $2
                        """, user_id, int(signal_int_id))

                        if existing_position and existing_position > 0:
                            self.logger.warning(f"   🚫 {user_short}: Position already exists for signal")
                            return (True, "Position already exists for this signal")

                        recent_trade = await conn.fetchval("""
                            SELECT COUNT(*) FROM trades
                            WHERE user_id = $1 AND signal_id = $2::text
                            AND closed_at > NOW() - INTERVAL '60 seconds'
                        """, user_id, str(signal_int_id))

                        if recent_trade and recent_trade > 0:
                            self.logger.warning(f"   🚫 {user_short}: Trade recently closed for signal")
                            return (True, "Trade recently closed for this signal")

            # ===== CHECK 1: Any open positions (any symbol) =====
            try:
                hl_rate.charge(hl_rate.W_USER_STATE, 'trading')
                state = info.user_state(wallet_address)
                if not isinstance(state, dict) or not isinstance(state.get('assetPositions'), list):
                    # QC money-followers-6: None, {}, the SDK's {'error': 'Could not parse JSON: ...'} -> read error
                    raise ValueError(f"unrecognised user_state shape: {str(state)[:120]}")
                for pos in state['assetPositions']:
                    position = pos.get('position', {})
                    szi = float(position.get('szi', 0))
                    if abs(szi) > 0:
                        coin = position.get('coin', 'Unknown')
                        side = 'LONG' if szi > 0 else 'SHORT'
                        self.logger.warning(
                            f"   🚫 {user_short}: Found open position - {coin} {side} ({abs(szi)} units)"
                        )
                        return (True, f"Open position exists: {coin} {side}")
            except Exception as e:
                self.logger.warning(f"   ⚠️ {user_short}: Error fetching positions: {e}")
                await log_error_to_db(self.db_pool, user_short, "DEDUP_SKIPPED_READ_ERROR",
                    f"positions: {str(e)[:200]}", {"signal_id": signal_id})
                return (True, f"Position check failed ({type(e).__name__}) - skipping to avoid stacking")

            # ===== CHECK 2: Any open orders =====
            try:
                hl_rate.charge(hl_rate.W_OPEN_ORDERS, 'trading')
                open_orders = info.open_orders(wallet_address)
                if not isinstance(open_orders, list):
                    raise ValueError(f"unrecognised open_orders shape: {str(open_orders)[:120]}")   # QC money-followers-6
                if len(open_orders) > 0:
                    coins_with_orders = set(o.get('coin', 'Unknown') for o in open_orders)
                    self.logger.warning(
                        f"   🚫 {user_short}: Found {len(open_orders)} open order(s) on: {', '.join(coins_with_orders)}"
                    )
                    return (True, f"Open orders exist: {len(open_orders)} orders")
            except Exception as e:
                self.logger.warning(f"   ⚠️ {user_short}: Error fetching open orders: {e}")
                await log_error_to_db(self.db_pool, user_short, "DEDUP_SKIPPED_READ_ERROR",
                    f"open orders: {str(e)[:200]}", {"signal_id": signal_id})
                return (True, f"Order check failed ({type(e).__name__}) - skipping to avoid stacking")

            return (False, None)

        except Exception as e:
            self.logger.error(f"   ❌ {user_short}: Error in safety check: {e}")
            await log_error_to_db(self.db_pool, user_short, "SAFETY_CHECK_ERROR", str(e))
            await log_error_to_db(self.db_pool, user_short, "DEDUP_SKIPPED_READ_ERROR",
                f"safety check: {str(e)[:200]}", {"signal_id": signal_id})
            return (True, "Safety check error - skipping")  # fail closed: never trade on an unverified account

    async def execute_trade(self, user: Dict, signal: Dict) -> bool:
        """
        Execute trade for a user on Hyperliquid.

        Position sizing formula (2% risk):
        - risk_amount = equity * risk_pct
        - risk_per_unit = |entry - stop_loss|
        - position_size = risk_amount / risk_per_unit
        - DO NOT multiply by leverage!

        Wave 1 bracket flow: in-flight token -> acknowledge -> dedup (fail closed) -> IOC entry (a raised IOC is
        never re-sent) -> fill confirmation (totalSz, else user_state polls; no blind sleep) ->
        SL (stop-market) FIRST -> TP -> live-size cross-check -> INSERT 'open'. Every exit reached after the
        entry may have filled ends with an SL + an 'open' row, a verified-flat account with no service orders,
        or a needs_recovery row for the guardian's recovery lane (I19).
        """
        tok = follower_guard.mark(user['id'])          # I12: FIRST statement - before acknowledgment and dedup
        try:
            return await self._execute_trade_body(user, signal)
        finally:
            follower_guard.clear(user['id'], tok)        # A6: follower_guard also ends the token with this task

    async def _execute_trade_body(self, user: Dict, signal: Dict) -> bool:
        user_api_key = user['api_key']
        user_short = user_api_key[:15] + "..."
        delivery_id = signal.get('delivery_id')
        stage = 'pre_entry'   # 'entry_sent' once the IOC returned an order; 'done' after the INSERT attempt,
        post = {}             # a needs_recovery record, or a verified-flat clean exit

        # ==================== EARLY ACKNOWLEDGMENT ====================
        if delivery_id:
            await self.acknowledge_signal(delivery_id)
            self.logger.info(f"   🔒 {user_short}: Signal acknowledged (preventing re-execution)")

        try:
            # Get exchange
            try:
                info, exchange, wallet_address = self.get_or_create_exchange(user)
            except ValueError as cred_error:
                self.logger.warning(f"   ⚠️ {user_short}: Skipping - {cred_error}")
                await log_error_to_db(
                    self.db_pool, user_api_key, "CREDENTIALS_NOT_FOUND",
                    str(cred_error), {"signal_id": signal.get('signal_id')}
                )
                return False

            # Convert symbol
            api_symbol = signal['symbol']
            coin = convert_symbol_to_hl(api_symbol)

            self.logger.info(f"   📊 {user_short}: Executing {signal['action']} {api_symbol} (coin={coin})")

            # ===== SAFETY CHECK (fail closed) =====
            has_open, reason = await self.check_any_open_positions_or_orders(
                info, exchange, wallet_address, user_short,
                user_id=user['id'], signal_id=signal.get('signal_id')
            )
            if has_open:
                self.logger.warning(f"   ⏭️ {user_short}: SKIPPING TRADE - {reason}")
                return False

            # Get equity
            equity = await self.get_user_equity(info, wallet_address)
            if equity <= 0:
                self.logger.error(f"   ❌ {user_short}: No equity found")
                return False

            # ==================== VALIDATE SL/TP ====================
            if not signal.get('stop_loss') or not signal.get('take_profit'):
                missing = []
                if not signal.get('stop_loss'): missing.append('stop_loss')
                if not signal.get('take_profit'): missing.append('take_profit')
                self.logger.error(f"   ❌ {user_short}: Signal missing {', '.join(missing)}")
                await notify_signal_invalid(signal.get('signal_id'), signal.get('symbol'), signal.get('action'), missing, f"Missing: {', '.join(missing)}")
                return False

            # Extract signal data
            action = signal['action']  # BUY or SELL
            entry_price = float(signal['entry_price'])
            stop_loss = float(signal['stop_loss'])
            take_profit = float(signal['take_profit'])
            leverage = float(signal.get('leverage', 5.0))

            # Validate values
            if stop_loss <= 0 or take_profit <= 0:
                self.logger.error(f"   ❌ {user_short}: Invalid SL ({stop_loss}) or TP ({take_profit})")
                await notify_signal_invalid_values(signal.get('signal_id'), signal.get('symbol'), action, entry_price, stop_loss, take_profit, "Zero or negative")
                return False

            risk_pct = float(signal.get('risk_pct', DEFAULT_RISK_PERCENTAGE))

            # ==================== POSITION SIZING ====================
            risk_amount = equity * risk_pct
            risk_per_unit = abs(entry_price - stop_loss)

            if risk_per_unit <= 0:
                self.logger.error(f"   ❌ {user_short}: Invalid SL distance")
                await notify_signal_invalid_values(signal.get('signal_id'), signal.get('symbol'), action, entry_price, stop_loss, take_profit, f"SL distance zero")
                return False

            # Position size = risk amount / risk per unit (NO leverage multiplication!)
            position_size = risk_amount / risk_per_unit
            quantity = self.round_size(coin, position_size)

            if quantity <= 0:
                self.logger.error(f"   ❌ {user_short}: Position size rounds to 0")
                return False

            position_value = quantity * entry_price

            self.logger.info(f"   💰 Equity: ${equity:,.2f}")
            self.logger.info(f"   🎯 Risk: ${risk_amount:,.2f} ({risk_pct*100:.2f}%)")
            self.logger.info(f"   📐 Position: {quantity} {coin} @ {leverage}x leverage")
            self.logger.info(f"   💵 Position value: ${position_value:,.2f}, Margin needed: ${position_value/leverage:,.2f}")

            # ==================== SET LEVERAGE ====================
            try:
                hl_rate.charge(hl_rate.W_ACTION, 'trading')
                exchange.update_leverage(int(leverage), coin, is_cross=True)
                self.logger.info(f"   ⚙️ Leverage set to {int(leverage)}x")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Could not set leverage: {e}")

            # ==================== EXECUTE 3-ORDER BRACKET (SL FIRST) ====================

            is_buy = action.upper() == 'BUY'
            user_email = user.get('email', 'unknown')

            # 1. Entry IOC order (market-like with slippage)
            # Use LIVE mid price for IOC limit, not stale signal entry price
            try:
                hl_rate.charge(hl_rate.W_ALL_MIDS, 'trading')
                all_mids = info.all_mids()
                live_price = float(all_mids.get(coin, 0))
                if live_price <= 0:
                    self.logger.warning(f"   ⚠️ Could not fetch live price for {coin}, falling back to signal entry")
                    live_price = entry_price
                else:
                    self.logger.info(f"   📊 Live mid price: ${live_price:.6f} (signal entry: ${entry_price:.6f})")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Live price fetch failed: {e}, using signal entry")
                live_price = entry_price

            slippage_mult = 1 + (SLIPPAGE_BPS / 10000) if is_buy else 1 - (SLIPPAGE_BPS / 10000)
            entry_limit_price = self.round_price(live_price * slippage_mult)

            self.logger.info(f"   📝 Placing entry IOC order...")
            entry_order = await place_entry_order_with_retry(
                exchange=exchange, coin=coin, is_buy=is_buy,
                quantity=quantity, limit_price=entry_limit_price,
                user_email=user_email, user_api_key=user_api_key,
                resend_after_exception=False     # QC money-followers-2: a raised IOC is never re-sent
            )

            if not entry_order:
                self.logger.error(f"   ❌ Entry order FAILED - ABORTING TRADE")
                await log_error_to_db(self.db_pool, user_api_key, "ENTRY_ORDER_FAILED",
                    "Entry failed after all retries", {"coin": coin, "side": action, "quantity": quantity})
                return False

            stage = 'entry_sent'
            await self._log_unexpected_status(user_api_key, 'Entry', coin, entry_order)
            post.update(info=info, exchange=exchange, wallet=wallet_address, coin=coin, side=action.upper(),
                        qty=quantity, leverage=leverage, entry_order_id=str(entry_order.get('id')),
                        entry_fill_price=entry_order.get('avgPx'), sl_oid=None, tp_oid=None)
            self.logger.info(f"   ✅ Entry: {entry_order['id']}")

            # ==================== CONFIRM THE FILL (no blind sleep) ====================
            bracket_qty, confirmed_px = await self._confirm_entry_fill(info, wallet_address, coin, is_buy, entry_order)
            if bracket_qty is None:
                # No bracket is sent: HL reduce-only triggers can rest on a flat account and block dedup CHECK 2.
                self.logger.critical(f"   🚨 ENTRY_UNCONFIRMED: no {coin} position seen - recording needs_recovery")
                await self._record_unprotected_position(
                    user, signal, coin, action.upper(), quantity, leverage, entry_order.get('id'),
                    entry_order.get('avgPx'), reason='ENTRY_UNCONFIRMED')
                await log_error_to_db(self.db_pool, user_api_key, "ENTRY_UNCONFIRMED",
                    f"No {coin} position after the IOC (oid {entry_order.get('id')}); no bracket sent",
                    {"coin": coin, "side": action, "quantity": quantity})
                await notify_admin(
                    title="ENTRY_UNCONFIRMED (HL)",
                    details={"User": user_email, "Coin": coin, "Side": action, "Quantity": quantity,
                             "Entry Order": entry_order.get('id'),
                             "Handling": "needs_recovery row written; the guardian recovery lane owns it"},
                    level="error")
                stage = 'done'
                return False
            if bracket_qty < quantity:
                self.logger.warning(f"   ⚠️ Partial fill: confirmed {bracket_qty} < requested {quantity}; bracket sized to {bracket_qty}")
            entry_fill_price = entry_order.get('avgPx') or confirmed_px or entry_price
            post.update(qty=bracket_qty, entry_fill_price=entry_fill_price)

            # 2. Stop-loss trigger order FIRST (market, reduce-only), sized to the confirmed fill
            sl_price = self.round_price(stop_loss)
            self.logger.info(f"   📝 Placing stop-loss order...")
            sl_order = await place_sl_order_with_retry(
                exchange=exchange, coin=coin, is_buy=not is_buy,
                quantity=bracket_qty, sl_price=sl_price,
                user_email=user_email, user_api_key=user_api_key
            )

            if not sl_order:
                self.logger.error(f"   ❌ SL order FAILED - EMERGENCY CLOSE!")
                closed = await self._emergency_close_position(
                    exchange, info, wallet_address, coin, not is_buy, bracket_qty,
                    user_email, user_api_key, entry_order['id'],
                    reason="SL order failed after all retries"
                )
                if not closed:
                    await self._record_unprotected_position(
                        user, signal, coin, action.upper(), bracket_qty, leverage, entry_order.get('id'),
                        entry_fill_price, reason='SL_FAILED_CLOSE_FAILED')
                stage = 'done'
                return False

            sl_oid = str(sl_order['id'])
            await self._log_unexpected_status(user_api_key, 'SL', coin, sl_order)
            post['sl_oid'] = sl_oid
            self.logger.info(f"   ✅ SL @ ${sl_price}: {sl_oid}")

            # 3. Take-profit trigger order SECOND (limit, reduce-only)
            tp_price = self.round_price(take_profit)
            self.logger.info(f"   📝 Placing take-profit order...")
            tp_order = await place_tp_order_with_retry(
                exchange=exchange, coin=coin, is_buy=not is_buy,
                quantity=bracket_qty, tp_price=tp_price,
                user_email=user_email, user_api_key=user_api_key
            )

            if tp_order:
                post['tp_oid'] = str(tp_order['id'])
                await self._log_unexpected_status(user_api_key, 'TP', coin, tp_order)
                self.logger.info(f"   ✅ TP @ ${tp_price}: {tp_order['id']}")
            elif follower_guard.guardian_mode(GUARDIAN_MODES) == 'enforce':
                # Keep the SL-protected position: the guardian re-places the TP (TP-failure-keep, enforce only)
                self.logger.error(f"   ❌ TP order FAILED - keeping the SL-protected position (GUARDIAN_MODE=enforce)")
                await log_error_to_db(self.db_pool, user_api_key, "TP_PLACE_FAILED",
                    "TP failed after all retries; position kept under its SL, the guardian re-places the TP",
                    {"coin": coin, "sl_order_id": sl_oid})
                await notify_admin(
                    title="TP_PLACE_FAILED - position kept under its SL (HL)",
                    details={"User": user_email, "Coin": coin, "SL Order": sl_oid, "TP Price": tp_price},
                    level="warning")
            else:
                # R3-M1: close FIRST while the SL is live; cancel the SL only once the close is verified flat
                self.logger.error(f"   ❌ TP order FAILED - EMERGENCY CLOSE (SL kept until verified flat)")
                closed = await self._emergency_close_position(
                    exchange, info, wallet_address, coin, not is_buy, bracket_qty,
                    user_email, user_api_key, entry_order['id'],
                    reason="TP order failed after all retries", sl_placed=True
                )
                if closed:
                    if not await cancel_order_verified(exchange, coin, sl_oid, 'Leftover SL after TP-failure close'):
                        await self._record_unprotected_position(
                            user, signal, coin, action.upper(), bracket_qty, leverage, entry_order.get('id'),
                            entry_fill_price, sl_order_id=sl_oid, reason='LEFTOVER_SL_CANCEL_FAILED')
                else:
                    await self._record_unprotected_position(
                        user, signal, coin, action.upper(), bracket_qty, leverage, entry_order.get('id'),
                        entry_fill_price, sl_order_id=sl_oid, reason='TP_FAILED_CLOSE_FAILED')
                stage = 'done'
                return False

            # ==================== SIZE CROSS-CHECK (QC money-followers-2) ====================
            row_qty = bracket_qty
            live_qty = await self._live_size_above_bracket(info, wallet_address, coin, is_buy, bracket_qty)
            if live_qty is not None:
                row_qty = live_qty
                self.logger.critical(f"   🚨 ENTRY_SIZE_MISMATCH: live {coin} size {live_qty} > bracket {bracket_qty}")
                await log_error_to_db(self.db_pool, user_api_key, "ENTRY_SIZE_MISMATCH",
                    f"Live {coin} size {live_qty} exceeds the bracket size {bracket_qty}; row recorded at {live_qty}",
                    {"coin": coin, "bracket_qty": bracket_qty, "live_qty": live_qty, "sl_order_id": sl_oid})
                await notify_admin(
                    title="ENTRY_SIZE_MISMATCH (HL)",
                    details={"User": user_email, "Coin": coin, "Bracket Size": bracket_qty, "Live Size": live_qty,
                             "SL Order": sl_oid,
                             "Handling": "Row recorded at the live size. The SL/TP cover only the bracket size: "
                                         "GUARDIAN_MODE=enforce re-sizes the SL on its next pass; in shadow/off "
                                         "place a full-size SL on Hyperliquid now"},
                    level="error")

            # ==================== RECORD OPEN POSITION ====================
            # QC correctness-followers-6 (CA-B7-2 on HL): the INSERT autocommits, so the billing UPDATE gets its own
            # try - its failure must never read as a missing row (a hand-added duplicate becomes ROW_AMBIGUOUS).
            position_opened_at = datetime.utcnow()
            inserted = False
            try:
                async with self.db_pool.acquire() as conn:
                    signal_db_id = await conn.fetchval(
                        "SELECT id FROM signals WHERE signal_id = $1", signal.get('signal_id')
                    )

                    await conn.execute("""
                        INSERT INTO open_positions
                        (user_id, signal_id, entry_order_id, tp_order_id, sl_order_id,
                         symbol, hl_coin, side, quantity, leverage,
                         entry_fill_price, target_tp, target_sl, opened_at, status)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    """,
                        user['id'],
                        signal_db_id,
                        str(entry_order['id']),
                        str(tp_order['id']) if tp_order else None,
                        sl_oid,
                        signal['symbol'],   # BTC/USDT format
                        coin,               # BTC (HL format)
                        action.upper(),     # BUY or SELL
                        row_qty,
                        leverage,
                        entry_fill_price,
                        tp_price,
                        sl_price,
                        position_opened_at,
                        'open'
                    )
                    inserted = True
                    self.logger.info(f"   📝 Open position recorded in database")
            except Exception as e:
                self.logger.error(f"   ⚠️ Failed to record position (trade still placed): {e}")
                await log_error_to_db(self.db_pool, user_api_key, "POSITION_RECORD_FAILED",
                    f"open_positions INSERT failed: {str(e)[:200]}", {"coin": coin, "sl_order_id": sl_oid})
                await notify_admin(
                    title="POSITION_RECORD_FAILED (HL)",
                    details={"User": user_email, "Coin": coin, "Quantity": row_qty, "SL Order": sl_oid,
                             "TP Order": post.get('tp_oid'), "Error": str(e)[:300],
                             "Note": "The position keeps its SL (and TP). Verify in open_positions that no active "
                                     "row exists for this user and coin before adding one (a second active row "
                                     "is ROW_AMBIGUOUS and stops the guardian repairing it)"},
                    level="error")

            if inserted:
                try:
                    async with self.db_pool.acquire() as conn:
                        # Start billing cycle if not started
                        await conn.execute("""
                            UPDATE follower_users SET billing_cycle_start = $2
                            WHERE id = $1 AND billing_cycle_start IS NULL
                        """, user['id'], position_opened_at)
                except Exception as e:
                    self.logger.error(f"   ⚠️ billing_cycle_start UPDATE failed (the position row exists): {e}")
                    await log_error_to_db(self.db_pool, user_api_key, "BILLING_CYCLE_START_FAILED",
                        f"billing_cycle_start UPDATE failed after the open_positions INSERT: {str(e)[:200]}",
                        {"coin": coin, "user_id": user['id']})
            stage = 'done'

            self.logger.info(f"   🎉 {user_short}: Trade executed successfully!")
            return True

        except Exception as e:
            self.logger.error(f"   ❌ {user_short}: Error - {type(e).__name__}: {str(e)[:100]}")
            await log_error_to_db(self.db_pool, user_api_key, "TRADE_EXECUTION_ERROR",
                str(e)[:200], {"coin": convert_symbol_to_hl(signal.get('symbol', '')), "side": signal.get('action')})
            if stage == 'entry_sent':
                await self._post_entry_exception(user, signal, post, e)
            return False

    async def _log_unexpected_status(self, user_api_key: str, leg: str, coin: str, order: Dict):
        """QC voice-19: an 'ok' envelope with unexpected statuses (order_utils returns id 'unknown' and e-mails
        HL_UNEXPECTED_STATUS) also gets an error_logs row, so the weekly review finds it."""
        if str((order or {}).get('id')) == 'unknown':
            await log_error_to_db(self.db_pool, user_api_key, "HL_UNEXPECTED_STATUS",
                f"{leg} returned ok with unexpected statuses; recorded with id 'unknown'",
                {"coin": coin, "leg": leg, "raw": str((order or {}).get('raw'))[:300]})

    async def _live_size_above_bracket(self, info, wallet_address: str, coin: str, is_buy: bool,
                                       bracket_qty: float) -> Optional[float]:
        """QC money-followers-2: one user_state read once the bracket rests. The live |szi| when it exceeds
        bracket_qty by more than half a lot (the row must then carry the live size, or select_row calls it
        ROW_AMBIGUOUS and the guardian never repairs it); None otherwise or on a failed read."""
        try:
            hl_rate.charge(hl_rate.W_USER_STATE, 'trading')
            state = await asyncio.to_thread(info.user_state, wallet_address)
            szi = coin_szi(state, coin)
        except Exception as e:
            self.logger.warning(f"   ⚠️ Post-bracket size check read failed: {e}")
            return None
        live = abs(szi) if ((szi > 0) if is_buy else (szi < 0)) else 0.0
        lot = 10 ** -int(self.asset_meta.get(coin, {}).get('szDecimals', 2))
        return live if live > bracket_qty + lot / 2 else None

    async def _confirm_entry_fill(self, info, wallet_address: str, coin: str, is_buy: bool, entry_order: Dict):
        """
        C40.2: (bracket_qty, entry_px). totalSz > 0 -> round_size(totalSz). Otherwise poll user_state every
        ENTRY_CONFIRM_POLL_S for up to ENTRY_CONFIRM_TIMEOUT_S for a correctly signed szi. (None, None) when
        still unconfirmed.
        """
        total_sz = float(entry_order.get('totalSz') or 0)
        if total_sz > 0:
            return self.round_size(coin, total_sz), None
        polls = max(1, int(round(self.ENTRY_CONFIRM_TIMEOUT_S / self.ENTRY_CONFIRM_POLL_S)))
        for i in range(polls):
            if i:
                await asyncio.sleep(self.ENTRY_CONFIRM_POLL_S)
            try:
                hl_rate.charge(hl_rate.W_USER_STATE, 'trading')
                state = await asyncio.to_thread(info.user_state, wallet_address)
                szi = coin_szi(state, coin)
                if (szi > 0) if is_buy else (szi < 0):
                    return self.round_size(coin, abs(szi)), coin_entry_px(state, coin)
            except Exception as e:
                self.logger.warning(f"   ⚠️ Fill confirmation read {i + 1}/{polls} failed: {e}")
        return None, None

    async def _verified_flat(self, info, wallet_address: str, coin: str) -> bool:
        """Two symbol-matched user_state reads VERIFY_FLAT_GAP_S apart, both flat. Any read error -> False."""
        for i in range(2):
            if i:
                await asyncio.sleep(self.VERIFY_FLAT_GAP_S)
            try:
                hl_rate.charge(hl_rate.W_USER_STATE, 'trading')
                state = await asyncio.to_thread(info.user_state, wallet_address)
                if coin_szi(state, coin) != 0:
                    return False
            except Exception:
                return False
        return True

    async def _record_unprotected_position(self, user, signal, coin, side, qty, leverage, entry_order_id,
                                           entry_fill_price, sl_order_id=None, tp_order_id=None, reason=''):
        """R3-H1: a needs_recovery row carrying the signal levels, the qty and every known service leg id."""
        try:
            async with self.db_pool.acquire() as conn:
                sig_id = await conn.fetchval('SELECT id FROM signals WHERE signal_id = $1', signal.get('signal_id'))
                await conn.execute(
                    "INSERT INTO open_positions (user_id, signal_id, entry_order_id, tp_order_id, sl_order_id, symbol, hl_coin, side, "
                    "quantity, leverage, entry_fill_price, target_tp, target_sl, opened_at, status) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW(),'needs_recovery')",
                    user['id'], sig_id, str(entry_order_id or 'unknown'), tp_order_id, sl_order_id, signal['symbol'], coin, side,
                    qty, leverage, entry_fill_price, self.round_price(float(signal['take_profit'])), self.round_price(float(signal['stop_loss'])))
            await log_error_to_db(self.db_pool, user['api_key'], 'NEEDS_RECOVERY_RECORDED', reason, {'coin': coin})
            await notify_admin(title='Position recorded for guardian recovery (HL)', details={'User': user.get('email'), 'Coin': coin, 'Reason': reason}, level='error')
        except Exception as e:
            self.logger.critical(f'   ❌ needs_recovery INSERT failed: {e}')
            await notify_admin(title='NEEDS_RECOVERY_RECORD_FAILED (HL)', details={'User': user.get('email'), 'Coin': coin, 'Reason': reason, 'Error': str(e)[:300]}, level='error')

    async def _post_entry_exception(self, user, signal, post, err):
        """
        R3-H1: an exception after the IOC returned an order. Coin open or unknown -> needs_recovery row with the
        known leg ids; verified flat (two reads 2 s apart) -> cancel the known service legs, and a failed cancel
        -> needs_recovery row carrying the leftover ids.
        """
        reason = f"POST_ENTRY_EXCEPTION:{type(err).__name__}"
        try:
            coin = post['coin']
            self.logger.critical(f"   🚨 {reason} after the entry was sent ({coin}): {str(err)[:200]}")
            await log_error_to_db(self.db_pool, user['api_key'], 'POST_ENTRY_EXCEPTION', f"{reason}: {str(err)[:300]}", {'coin': coin})
            await notify_admin(title='POST_ENTRY_EXCEPTION (HL)', details={'User': user.get('email'), 'Coin': coin, 'Error': f"{reason}: {str(err)[:300]}"}, level='error')
            args = (user, signal, coin, post['side'], post['qty'], post['leverage'], post['entry_order_id'], post.get('entry_fill_price'))
            if not await self._verified_flat(post['info'], post['wallet'], coin):
                await self._record_unprotected_position(*args, sl_order_id=post.get('sl_oid'), tp_order_id=post.get('tp_oid'), reason=reason)
                return
            leftover = {}
            for key, label in (('sl_oid', 'SL'), ('tp_oid', 'TP')):
                oid = post.get(key)
                if oid and not await cancel_order_verified(post['exchange'], coin, oid, f'{label} after a post-entry exception'):
                    leftover[key] = oid
            if leftover:
                await self._record_unprotected_position(*args, sl_order_id=leftover.get('sl_oid'), tp_order_id=leftover.get('tp_oid'), reason='POST_ENTRY_EXCEPTION_LEFTOVER')
        except Exception as e:
            self.logger.critical(f"   ❌ post-entry recovery handling failed: {e}")
            await notify_admin(title='POST_ENTRY_EXCEPTION handling failed (HL)', details={'User': user.get('email'), 'Error': str(e)[:300], 'Original': str(err)[:300]}, level='error')

    async def _emergency_close_position(
        self, exchange, info, wallet_address, coin, is_buy_to_close, quantity,
        user_email, user_api_key, entry_order_id, reason, sl_placed=False
    ) -> bool:
        """
        Emergency reduce-only close when a bracket leg cannot be placed.
        C41: verified by an IOC 'filled' status AND a flat user_state re-read (hl_close_with_verification).
        Returns True only when the coin is verified flat; the caller records needs_recovery otherwise.
        """
        self.logger.critical(f"   🚨🚨🚨 EMERGENCY CLOSE TRIGGERED 🚨🚨🚨")
        self.logger.critical(f"   Reason: {reason}")

        close_success, close_order_id = await hl_close_with_verification(
            exchange, info, wallet_address, coin, is_buy_to_close, quantity, self.round_price, source='trading')
        if close_success:
            self.logger.critical(f"   ✅ EMERGENCY CLOSE VERIFIED (filled + flat): {close_order_id}")
        else:
            self.logger.critical(f"   ❌❌❌ EMERGENCY CLOSE FAILED ❌❌❌")
            self.logger.critical(f"   🚨 MANUAL INTERVENTION REQUIRED!")

        await notify_bracket_incomplete(
            user_email=user_email, user_api_key=user_api_key,
            coin=coin, entry_order_id=entry_order_id,
            tp_placed=False, sl_placed=sl_placed,
            error=f"{reason} - Emergency close {'SUCCESSFUL' if close_success else 'FAILED'}"
        )

        await log_error_to_db(self.db_pool, user_api_key, "EMERGENCY_CLOSE",
            f"{reason}. Close: {'OK' if close_success else 'FAILED'}",
            {"coin": coin, "entry_order_id": entry_order_id, "close_success": close_success})
        if not close_success:
            await log_error_to_db(self.db_pool, user_api_key, "EMERGENCY_CLOSE_FAILED",
                f"{reason}: the verified close failed; the position may still be open",
                {"coin": coin, "entry_order_id": entry_order_id})
        return close_success

    async def poll_and_execute(self):
        """
        Single poll cycle - check all users for signals.
        Batched execution.
        """
        pending = await self.get_pending_signals_batched()
        
        if not pending:
            return
        
        random.shuffle(pending)
        
        self.logger.info(f"📡 Found {len(pending)} pending signal(s) to execute")
        
        BATCH_SIZE = 25
        BATCH_DELAY = 0.05  # 50ms
        
        for i in range(0, len(pending), BATCH_SIZE):
            batch = pending[i:i + BATCH_SIZE]
            
            await asyncio.gather(*[
                self._execute_signal_for_user(item) for item in batch
            ], return_exceptions=True)
            
            if i + BATCH_SIZE < len(pending):
                await asyncio.sleep(BATCH_DELAY)
    
    async def _execute_signal_for_user(self, item: Dict):
        """Execute a single signal for a single user."""
        user_short = item['api_key'][:15] + "..."
        
        try:
            user = {
                'id': item['user_id'],
                'api_key': item['api_key'],
                'email': item['email'],
                'hl_private_key_encrypted': item['hl_private_key_encrypted'],
                'hl_wallet_address': item['hl_wallet_address'],
            }
            
            signal = {
                'delivery_id': item['delivery_id'],
                'signal_id': item['signal_id'],
                'action': item['action'],
                'symbol': item['symbol'],
                'entry_price': item['entry_price'],
                'stop_loss': item['stop_loss'],
                'take_profit': item['take_profit'],
                'leverage': item['leverage'],
                'risk_pct': item['risk_pct'],
                'created_at': item['signal_created_at'],
            }
            
            self.logger.info(f"✨ {user_short}: Signal found - {signal['action']} {signal['symbol']}")
            
            success = await self.execute_trade(user, signal)
            
            if success:
                self.logger.info(f"✅ {user_short}: Trade executed successfully")
            else:
                self.logger.warning(f"⚠️ {user_short}: Trade failed (signal already acknowledged, no retry)")
                
        except Exception as e:
            self.logger.error(f"❌ {user_short}: Error - {e}")
            await log_error_to_db(self.db_pool, item.get('api_key', 'unknown'),
                "SIGNAL_PROCESSING_ERROR", str(e)[:200], {"signal_id": item.get('signal_id')})
    
    async def run(self):
        """Main loop - polls continuously every 10 seconds."""
        self.logger.info("=" * 60)
        self.logger.info("🚀 HOSTED TRADING LOOP (HYPERLIQUID) STARTED")
        self.logger.info(f"   Network: {'TESTNET' if USE_TESTNET else 'MAINNET'}")
        self.logger.info("=" * 60)
        self.logger.info(f"🔄 Poll interval: {POLL_INTERVAL_SECONDS} seconds")
        self.logger.info(f"💰 Risk per trade: From signal (2-3%), default {DEFAULT_RISK_PERCENTAGE*100:.0f}%")
        self.logger.info(f"📊 Slippage tolerance: {SLIPPAGE_BPS} bps")
        self.logger.info("=" * 60)
        
        poll_count = 0
        last_status_log = datetime.now()
        
        while True:
            try:
                poll_count += 1
                await self.poll_and_execute()
                
                if (datetime.now() - last_status_log).total_seconds() >= 300:
                    self.logger.info(f"💓 Trading loop alive - Poll #{poll_count}")
                    last_status_log = datetime.now()
                
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    
            except asyncio.CancelledError:
                self.logger.info("🛑 Trading loop cancelled")
                break
                
            except Exception as e:
                self.logger.error(f"❌ Error in trading loop: {e}")
                import traceback
                traceback.print_exc()
                await log_error_to_db(self.db_pool, "system", "TRADING_LOOP_ERROR",
                    str(e)[:200], {"poll_count": poll_count})
                await notify_critical_error("TRADING_LOOP_ERROR", str(e), "hosted_trading_loop.run")
                await asyncio.sleep(10)


async def start_hosted_trading(db_pool):
    """Start the hosted trading loop. Call from main.py startup."""
    await asyncio.sleep(35)  # Wait for DB tables to be ready
    
    loop = HostedTradingLoop(db_pool)
    await loop.run()
