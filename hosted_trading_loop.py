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
import logging
import math
import json
import random
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List

from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from cryptography.fernet import Fernet

from order_utils import (
    place_entry_order_with_retry,
    place_tp_order_with_retry,
    place_sl_order_with_retry,
    cancel_order_with_retry,
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
                """INSERT INTO error_logs (api_key, error_type, error_message, context) 
                   VALUES ($1, $2, $3, $4)""",
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


class HostedTradingLoop:
    """
    Main hosted trading loop that executes for all users on Hyperliquid.
    """
    
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.active_exchanges = {}  # Cache: {api_key: (info, exchange, wallet_address)}
        self.asset_meta = {}  # Cache: {coin: {szDecimals, ...}}
        self.logger = logging.getLogger('HOSTED_TRADING')
        
        # Initialize HL info client (shared, read-only)
        base_url = constants.TESTNET_API_URL if USE_TESTNET else constants.MAINNET_API_URL
        self.info = Info(base_url, skip_ws=True)
        self.base_url = base_url
        
        # Load asset metadata for size/price rounding
        self._load_asset_meta()
    
    def _load_asset_meta(self):
        """Load asset metadata for precision rounding"""
        try:
            meta = self.info.meta()
            if meta and 'universe' in meta:
                for asset in meta['universe']:
                    self.asset_meta[asset['name']] = asset
                self.logger.info(f"📋 Loaded metadata for {len(self.asset_meta)} assets")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load asset metadata: {e}")
    
    def round_size(self, coin: str, size: float) -> float:
        """Round size to asset's szDecimals precision"""
        meta = self.asset_meta.get(coin, {})
        sz_decimals = meta.get('szDecimals', 2)
        factor = 10 ** sz_decimals
        return math.floor(size * factor) / factor
    
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
        if wallet_address.lower() != api_wallet_address.lower():
            exchange = Exchange(account, self.base_url, account_address=wallet_address)
        else:
            exchange = Exchange(account, self.base_url)
        
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
                state = info.user_state(wallet_address)
                if state and 'assetPositions' in state:
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
            
            # ===== CHECK 2: Any open orders =====
            try:
                open_orders = info.open_orders(wallet_address)
                if open_orders and len(open_orders) > 0:
                    coins_with_orders = set(o.get('coin', 'Unknown') for o in open_orders)
                    self.logger.warning(
                        f"   🚫 {user_short}: Found {len(open_orders)} open order(s) on: {', '.join(coins_with_orders)}"
                    )
                    return (True, f"Open orders exist: {len(open_orders)} orders")
            except Exception as e:
                self.logger.warning(f"   ⚠️ {user_short}: Error fetching open orders: {e}")
            
            return (False, None)
            
        except Exception as e:
            self.logger.error(f"   ❌ {user_short}: Error in safety check: {e}")
            await log_error_to_db(self.db_pool, user_short, "SAFETY_CHECK_ERROR", str(e))
            return (False, None)  # On error, allow trade but log
    
    async def execute_trade(self, user: Dict, signal: Dict) -> bool:
        """
        Execute trade for a user on Hyperliquid.
        
        Position sizing formula (2% risk):
        - risk_amount = equity * risk_pct
        - risk_per_unit = |entry - stop_loss|
        - position_size = risk_amount / risk_per_unit
        - DO NOT multiply by leverage!
        
        3-order bracket:
        1. IOC entry order (market-like with slippage protection)
        2. TP trigger order (limit, reduce-only)
        3. SL trigger order (market, reduce-only)
        
        Acknowledges signal at START to prevent race condition.
        """
        user_api_key = user['api_key']
        user_short = user_api_key[:15] + "..."
        delivery_id = signal.get('delivery_id')
        
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
            
            # ===== SAFETY CHECK =====
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
            self.logger.info(f"   🎯 Risk: ${risk_amount:,.2f} ({risk_pct*100:.0f}%)")
            self.logger.info(f"   📐 Position: {quantity} {coin} @ {leverage}x leverage")
            self.logger.info(f"   💵 Position value: ${position_value:,.2f}, Margin needed: ${position_value/leverage:,.2f}")
            
            # ==================== SET LEVERAGE ====================
            try:
                exchange.update_leverage(int(leverage), coin, is_cross=True)
                self.logger.info(f"   ⚙️ Leverage set to {int(leverage)}x")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Could not set leverage: {e}")
            
            # ==================== EXECUTE 3-ORDER BRACKET ====================
            
            is_buy = action.upper() == 'BUY'
            user_email = user.get('email', 'unknown')
            
            # 1. Entry IOC order (market-like with slippage)
            slippage_mult = 1 + (SLIPPAGE_BPS / 10000) if is_buy else 1 - (SLIPPAGE_BPS / 10000)
            entry_limit_price = self.round_price(entry_price * slippage_mult)
            
            self.logger.info(f"   📝 Placing entry IOC order...")
            entry_order = await place_entry_order_with_retry(
                exchange=exchange, coin=coin, is_buy=is_buy,
                quantity=quantity, limit_price=entry_limit_price,
                user_email=user_email, user_api_key=user_api_key
            )
            
            if not entry_order:
                self.logger.error(f"   ❌ Entry order FAILED - ABORTING TRADE")
                await log_error_to_db(self.db_pool, user_api_key, "ENTRY_ORDER_FAILED",
                    "Entry failed after all retries", {"coin": coin, "side": action, "quantity": quantity})
                return False
            
            self.logger.info(f"   ✅ Entry: {entry_order['id']}")
            
            # Wait for fill confirmation
            await asyncio.sleep(2)
            
            # 2. Take-profit trigger order (limit, reduce-only)
            tp_price = self.round_price(take_profit)
            self.logger.info(f"   📝 Placing take-profit order...")
            tp_order = await place_tp_order_with_retry(
                exchange=exchange, coin=coin, is_buy=not is_buy,
                quantity=quantity, tp_price=tp_price,
                user_email=user_email, user_api_key=user_api_key
            )
            
            if not tp_order:
                self.logger.error(f"   ❌ TP order FAILED - EMERGENCY CLOSE!")
                await self._emergency_close_position(
                    exchange, info, wallet_address, coin, not is_buy, quantity,
                    user_email, user_api_key, entry_order['id'],
                    reason="TP order failed after all retries"
                )
                return False
            
            self.logger.info(f"   ✅ TP @ ${tp_price}: {tp_order['id']}")
            
            # 3. Stop-loss trigger order (market, reduce-only)
            sl_price = self.round_price(stop_loss)
            self.logger.info(f"   📝 Placing stop-loss order...")
            sl_order = await place_sl_order_with_retry(
                exchange=exchange, coin=coin, is_buy=not is_buy,
                quantity=quantity, sl_price=sl_price,
                user_email=user_email, user_api_key=user_api_key
            )
            
            if not sl_order:
                self.logger.error(f"   ❌ SL order FAILED - EMERGENCY CLOSE!")
                # Cancel orphaned TP first
                try:
                    await cancel_order_with_retry(exchange, coin, int(tp_order['id']), "Orphaned TP")
                except Exception:
                    pass
                await self._emergency_close_position(
                    exchange, info, wallet_address, coin, not is_buy, quantity,
                    user_email, user_api_key, entry_order['id'],
                    reason="SL order failed after all retries"
                )
                return False
            
            self.logger.info(f"   ✅ SL @ ${sl_price}: {sl_order['id']}")
            
            # ==================== RECORD OPEN POSITION ====================
            entry_fill_price = entry_order.get('avgPx', entry_price)
            
            try:
                async with self.db_pool.acquire() as conn:
                    signal_db_id = await conn.fetchval(
                        "SELECT id FROM signals WHERE signal_id = $1", signal.get('signal_id')
                    )
                    
                    position_opened_at = datetime.utcnow()
                    
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
                        str(tp_order['id']),
                        str(sl_order['id']),
                        signal['symbol'],   # BTC/USDT format
                        coin,               # BTC (HL format)
                        action.upper(),     # BUY or SELL
                        quantity,
                        leverage,
                        entry_fill_price,
                        tp_price,
                        sl_price,
                        position_opened_at,
                        'open'
                    )
                    
                    # Start billing cycle if not started
                    await conn.execute("""
                        UPDATE follower_users SET billing_cycle_start = $2
                        WHERE id = $1 AND billing_cycle_start IS NULL
                    """, user['id'], position_opened_at)
                    
                    self.logger.info(f"   📝 Open position recorded in database")
            except Exception as e:
                self.logger.error(f"   ⚠️ Failed to record position (trade still placed): {e}")
            
            self.logger.info(f"   🎉 {user_short}: Trade executed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"   ❌ {user_short}: Error - {type(e).__name__}: {str(e)[:100]}")
            await log_error_to_db(self.db_pool, user_api_key, "TRADE_EXECUTION_ERROR",
                str(e)[:200], {"coin": convert_symbol_to_hl(signal.get('symbol', '')), "side": signal.get('action')})
            return False
    
    async def _emergency_close_position(
        self, exchange, info, wallet_address, coin, is_buy_to_close, quantity,
        user_email, user_api_key, entry_order_id, reason
    ):
        """
        Emergency market close when TP/SL placement fails.
        Protects users from unprotected positions.
        """
        self.logger.critical(f"   🚨🚨🚨 EMERGENCY CLOSE TRIGGERED 🚨🚨🚨")
        self.logger.critical(f"   Reason: {reason}")
        
        close_success = False
        close_order_id = None
        
        try:
            # Get current price for IOC limit
            # Use a wide slippage for emergency (2%)
            state = info.user_state(wallet_address)
            # Get mid price from meta
            all_mids = info.all_mids()
            mid_price = float(all_mids.get(coin, 0))
            
            if mid_price > 0:
                emergency_slippage = 0.02  # 2% for emergency
                if is_buy_to_close:
                    limit_px = self.round_price(mid_price * (1 + emergency_slippage))
                else:
                    limit_px = self.round_price(mid_price * (1 - emergency_slippage))
                
                close_result = exchange.order(
                    coin, is_buy_to_close, quantity, limit_px,
                    {"limit": {"tif": "Ioc"}},
                    reduce_only=True
                )
                
                if close_result.get("status") == "ok":
                    close_success = True
                    statuses = close_result.get("response", {}).get("data", {}).get("statuses", [])
                    if statuses and "filled" in statuses[0]:
                        close_order_id = statuses[0]["filled"].get("oid")
                    self.logger.critical(f"   ✅ EMERGENCY CLOSE SUCCESSFUL: {close_order_id}")
                else:
                    self.logger.critical(f"   ❌ EMERGENCY CLOSE RETURNED: {close_result}")
            else:
                self.logger.critical(f"   ❌ Cannot get mid price for {coin}")
                
        except Exception as e:
            self.logger.critical(f"   ❌❌❌ EMERGENCY CLOSE FAILED: {e} ❌❌❌")
            self.logger.critical(f"   🚨 MANUAL INTERVENTION REQUIRED!")
        
        await notify_bracket_incomplete(
            user_email=user_email, user_api_key=user_api_key,
            coin=coin, entry_order_id=entry_order_id,
            tp_placed=False, sl_placed=False,
            error=f"{reason} - Emergency close {'SUCCESSFUL' if close_success else 'FAILED'}"
        )
        
        await log_error_to_db(self.db_pool, user_api_key, "EMERGENCY_CLOSE",
            f"{reason}. Close: {'OK' if close_success else 'FAILED'}",
            {"coin": coin, "entry_order_id": entry_order_id, "close_success": close_success})
    
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
