"""
Nike Rocket Position Monitor v1.0 - Hyperliquid
============================================================================

Monitors positions and records P&L when they close.
Mirrors Kraken Position Monitor v3.0 with HL SDK adaptations.

KEY DIFFERENCES FROM KRAKEN:
- Uses info.user_state(wallet) for positions instead of fetch_positions()
- Uses info.user_fills(wallet) for fill history instead of fetch_my_trades()
- Uses info.open_orders(wallet) for order status
- No CCXT - direct Hyperliquid SDK
- Fingerprint: wallet address (no trade history hashing needed)
- Symbol matching: coin name (e.g., "ADA") instead of PF_ADAUSD

30-DAY BILLING FLOW (same as Kraken):
1. Trade closes → Record profit in trades table (fee_charged = 0)
2. Update current_cycle_profit in follower_users (accumulated)
3. At end of 30 days, billing service calculates fee on total profit
4. Coinbase invoice sent if profitable

Author: Nike Rocket Team
"""

import asyncio
import logging
import os
import random
import secrets
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from cryptography.fernet import Fernet

from config import (
    utc_now, to_naive_utc,
    ERROR_MESSAGE_MAX_LENGTH, ERROR_CONTEXT_MAX_LENGTH,
    get_fee_rate, get_tier_display
)

# Configuration
CHECK_INTERVAL_SECONDS = 60
FILL_LOOKBACK_HOURS = 24

USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("POSITION_MONITOR")

# Encryption for credentials
ENCRYPTION_KEY = os.getenv("CREDENTIALS_ENCRYPTION_KEY")
cipher = Fernet(ENCRYPTION_KEY.encode()) if ENCRYPTION_KEY else None


async def log_error_to_db(pool, api_key: str, error_type: str, error_message: str, context: Optional[Dict] = None):
    """Log error to error_logs table"""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO error_logs (api_key, error_type, error_message, context) 
                   VALUES ($1, $2, $3, $4)""",
                api_key[:20] + "..." if api_key and len(api_key) > 20 else api_key,
                error_type,
                error_message[:ERROR_MESSAGE_MAX_LENGTH] if error_message else None,
                json.dumps(context)[:ERROR_CONTEXT_MAX_LENGTH] if context else None
            )
    except Exception as e:
        logger.error(f"Failed to log error to DB: {e}")


class PositionMonitor:
    """
    Monitors positions and records actual P&L when they close.
    Hyperliquid version - mirrors Kraken v3.0.
    """
    
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.logger = logger
        self.active_exchanges: Dict[str, tuple] = {}  # {api_key: (info, exchange, wallet)}
        
        # Shared info client
        base_url = constants.TESTNET_API_URL if USE_TESTNET else constants.MAINNET_API_URL
        self.info = Info(base_url, skip_ws=True)
        self.base_url = base_url
    
    # ==================== Credential & Exchange Management ====================
    
    def decrypt_private_key(self, encrypted_key: str) -> Optional[str]:
        """Decrypt HL private key"""
        if not cipher or not encrypted_key:
            return None
        try:
            return cipher.decrypt(encrypted_key.encode()).decode()
        except Exception:
            return None
    
    def get_exchange(self, user_api_key: str, hl_private_key_encrypted: str, hl_wallet_address: str = None) -> Optional[tuple]:
        """Get or create exchange instance. Returns (info, exchange, wallet_address)"""
        if user_api_key in self.active_exchanges:
            return self.active_exchanges[user_api_key]
        
        try:
            private_key = self.decrypt_private_key(hl_private_key_encrypted)
            if not private_key:
                return None
            
            account = Account.from_key(private_key)
            wallet_address = hl_wallet_address or account.address
            exchange = Exchange(account, self.base_url)
            
            result = (self.info, exchange, wallet_address)
            self.active_exchanges[user_api_key] = result
            return result
        except Exception as e:
            self.logger.error(f"Failed to create exchange: {e}")
            return None
    
    @staticmethod
    def get_base_symbol(symbol: str) -> str:
        """
        Extract base symbol for consistent matching.
        "ADA/USDT" → "ADA", "BTC/USDT" → "BTC", "ADA" → "ADA"
        """
        if not symbol:
            return ''
        base = symbol.upper()
        if '/' in base:
            base = base.split('/')[0]
        base = base.replace('PF_', '').replace('USD', '').replace('USDT', '')
        return base.strip()
    
    # ==================== Signal Matching ====================
    
    async def find_matching_signal(self, symbol: str, side: str, lookback_hours: int = 48) -> Optional[dict]:
        """
        Check if there's a Nike Rocket signal matching this trade.
        Only tracks/charges fees on copytraded positions.
        """
        try:
            async with self.db_pool.acquire() as conn:
                symbol_base = self.get_base_symbol(symbol)
                
                action_map = {'long': 'BUY', 'short': 'SELL'}
                signal_action = action_map.get(side.lower(), side.upper())
                
                lookback_threshold = to_naive_utc(utc_now() - timedelta(hours=lookback_hours))
                
                signal = await conn.fetchrow("""
                    SELECT id, signal_id, symbol, action, created_at
                    FROM signals
                    WHERE UPPER(symbol) LIKE $1
                      AND UPPER(action) = UPPER($2)
                      AND created_at >= $3
                    ORDER BY created_at DESC
                    LIMIT 1
                """, f'%{symbol_base}%', signal_action, lookback_threshold)
                
                if signal:
                    self.logger.info(f"✅ Found matching signal: {signal['symbol']} {signal['action']}")
                    return dict(signal)
                else:
                    self.logger.info(f"⚠️ No matching signal for {symbol} {side} - likely manual trade")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error checking for matching signal: {e}")
            return None
    
    # ==================== Fill Recording ====================
    
    async def record_fill(self, user_id: int, fill: dict) -> bool:
        """Record a single execution fill to position_fills table."""
        try:
            # HL fills have different structure than CCXT
            fill_id = fill.get('tid') or fill.get('id') or f"{fill.get('oid', '')}_{fill.get('time', int(time.time() * 1000))}"
            order_id = fill.get('oid', '')
            
            async with self.db_pool.acquire() as conn:
                existing = await conn.fetchval(
                    "SELECT id FROM position_fills WHERE user_id = $1 AND fill_id = $2",
                    user_id, str(fill_id)
                )
                
                if existing:
                    return False
                
                # Parse timestamp
                fill_timestamp = None
                ts = fill.get('time', 0)
                if isinstance(ts, (int, float)):
                    fill_timestamp = datetime.utcfromtimestamp(ts / 1000) if ts > 1e12 else datetime.utcfromtimestamp(ts)
                
                price = float(fill.get('px', 0))
                quantity = float(fill.get('sz', 0))
                side_str = fill.get('side', 'unknown')
                coin = fill.get('coin', 'UNKNOWN')
                
                await conn.execute("""
                    INSERT INTO position_fills 
                    (user_id, kraken_order_id, fill_id, symbol, side, price, quantity, cost, fill_timestamp, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (user_id, fill_id) DO NOTHING
                """,
                    user_id,
                    str(order_id),
                    str(fill_id),
                    coin,
                    side_str.lower() if side_str else 'unknown',
                    price,
                    quantity,
                    price * quantity,
                    fill_timestamp,
                    'hl_api'
                )
                
                self.logger.info(f"   📝 Fill recorded: {coin} {side_str} {quantity} @ ${price:.4f}")
                return True
                
        except Exception as e:
            self.logger.error(f"   Failed to record fill: {e}")
            return False
    
    # ==================== Exchange History Scanning ====================
    
    async def scan_exchange_fills(self, user_info: dict) -> List[dict]:
        """Scan exchange for recent fills and record new ones."""
        user_short = user_info['api_key'][:15] + "..."
        
        try:
            result = self.get_exchange(
                user_info['api_key'],
                user_info['hl_private_key_encrypted'],
                user_info.get('hl_wallet_address')
            )
            
            if not result:
                return []
            
            info, exchange, wallet_address = result
            
            # Fetch recent fills from Hyperliquid
            fills = info.user_fills(wallet_address)
            
            if not fills:
                return []
            
            new_fills = []
            coins_affected = set()
            
            for fill in fills:
                # Check if recent
                fill_time = fill.get('time', 0)
                if isinstance(fill_time, (int, float)):
                    ts_seconds = fill_time / 1000 if fill_time > 1e12 else fill_time
                    fill_age_hours = (time.time() - ts_seconds) / 3600
                    if fill_age_hours > FILL_LOOKBACK_HOURS:
                        continue
                
                was_new = await self.record_fill(user_info['id'], fill)
                if was_new:
                    new_fills.append(fill)
                    coins_affected.add(fill.get('coin', ''))
            
            if new_fills:
                self.logger.info(f"⚡ {user_short}: Recorded {len(new_fills)} new fills")
            
            return new_fills
            
        except Exception as e:
            self.logger.debug(f"Could not scan fills for {user_short}: {e}")
            return []
    
    # ==================== Position Monitoring ====================
    
    async def get_active_users(self) -> list:
        """Get all active users with HL credentials"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    id,
                    api_key,
                    hl_private_key_encrypted,
                    hl_wallet_address,
                    fee_tier
                FROM follower_users
                WHERE hl_private_key_encrypted IS NOT NULL
            """)
            return [dict(row) for row in rows]
    
    async def get_open_positions(self) -> list:
        """Fetch all open positions from database"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    op.id,
                    op.user_id,
                    op.signal_id,
                    op.entry_order_id,
                    op.tp_order_id,
                    op.sl_order_id,
                    op.symbol,
                    op.hl_coin,
                    op.side,
                    op.quantity,
                    op.leverage,
                    op.entry_fill_price,
                    op.avg_entry_price,
                    op.filled_quantity,
                    op.fill_count,
                    op.total_cost_basis,
                    op.target_tp,
                    op.target_sl,
                    op.opened_at,
                    op.first_fill_at,
                    op.last_fill_at,
                    u.api_key as user_api_key,
                    u.hl_private_key_encrypted,
                    u.hl_wallet_address,
                    u.fee_tier
                FROM open_positions op
                JOIN follower_users u ON op.user_id = u.id
                WHERE op.status = 'open'
            """)
            return [dict(row) for row in rows]
    
    async def check_position_closed(
        self, info: Info, wallet_address: str, coin: str,
        side: str, quantity: float, tp_order_id: str, sl_order_id: str,
        user_api_key: str = 'unknown'
    ) -> Dict[str, Any]:
        """
        Check if position is still open on Hyperliquid.
        
        Returns:
        - {'closed': False} if position still exists
        - {'closed': True, 'exit_price': price, 'exit_type': 'TP'/'SL'} if closed
        """
        try:
            state = info.user_state(wallet_address)
            
            if state and 'assetPositions' in state:
                for pos in state['assetPositions']:
                    position = pos.get('position', {})
                    pos_coin = position.get('coin', '')
                    szi = float(position.get('szi', 0))
                    
                    if pos_coin == coin and abs(szi) > 0:
                        self.logger.info(f"✅ Position found: {coin} {'LONG' if szi > 0 else 'SHORT'} ({abs(szi)} units)")
                        return {'closed': False, 'current_size': abs(szi)}
                
                # Check for ANY position (safety)
                for pos in state['assetPositions']:
                    position = pos.get('position', {})
                    szi = float(position.get('szi', 0))
                    if abs(szi) > 0:
                        self.logger.info(f"✅ Position found (any): {position.get('coin')} ({abs(szi)})")
                        return {'closed': False, 'current_size': abs(szi)}
            
            self.logger.info(f"📭 No open positions found for {wallet_address[:10]}...")
            
            # Position closed - determine exit type from open orders
            exit_type = 'UNKNOWN'
            try:
                open_orders = info.open_orders(wallet_address)
                
                tp_exists = any(str(o.get('oid', '')) == str(tp_order_id) for o in open_orders)
                sl_exists = any(str(o.get('oid', '')) == str(sl_order_id) for o in open_orders)
                
                if tp_exists and sl_exists:
                    self.logger.warning(f"⚠️ No position but both TP/SL exist")
                    return {'closed': False, 'anomaly': True}
                
                if not tp_exists and sl_exists:
                    exit_type = 'TP'
                elif tp_exists and not sl_exists:
                    exit_type = 'SL'
            except Exception as e:
                self.logger.warning(f"Could not check open orders: {e}")
            
            # Get exit price from recent fills
            exit_price = None
            try:
                fills = info.user_fills(wallet_address)
                if fills:
                    # Sort by time descending
                    fills.sort(key=lambda x: x.get('time', 0), reverse=True)
                    # Find most recent fill for this coin
                    for fill in fills:
                        if fill.get('coin') == coin:
                            exit_price = float(fill.get('px', 0))
                            break
            except Exception as e:
                self.logger.warning(f"Could not get exit price: {e}")
            
            return {
                'closed': True,
                'exit_price': exit_price,
                'exit_type': exit_type
            }
            
        except Exception as e:
            self.logger.error(f"Error checking position: {e}")
            return {'closed': False, 'error': str(e)}
    
    async def get_hl_realized_pnl(self, info: Info, wallet_address: str, coin: str, since_timestamp: datetime = None) -> Optional[Dict]:
        """
        Get realized P&L from Hyperliquid fills.
        
        Mirrors Kraken's get_kraken_realized_pnl().
        HL fills include 'closedPnl' field for realized P&L.
        """
        try:
            fills = info.user_fills(wallet_address)
            
            if not fills:
                return None
            
            # Filter to this coin
            coin_fills = [f for f in fills if f.get('coin') == coin]
            
            if not coin_fills:
                return None
            
            # Sort descending
            coin_fills.sort(key=lambda x: x.get('time', 0), reverse=True)
            
            # Filter by time
            if since_timestamp:
                since_ms = int(since_timestamp.timestamp() * 1000)
                coin_fills = [f for f in coin_fills if f.get('time', 0) >= since_ms]
            
            if not coin_fills:
                return None
            
            # Sum realized P&L
            total_pnl = 0
            total_qty = 0
            weighted_price_sum = 0
            
            for fill in coin_fills:
                # HL fills have 'closedPnl' for realized P&L
                closed_pnl = float(fill.get('closedPnl', 0))
                total_pnl += closed_pnl
                
                qty = float(fill.get('sz', 0))
                price = float(fill.get('px', 0))
                total_qty += qty
                weighted_price_sum += qty * price
            
            avg_exit_price = weighted_price_sum / total_qty if total_qty > 0 else 0
            
            self.logger.info(f"📊 HL realized P&L for {coin}: ${total_pnl:+.2f} from {len(coin_fills)} fills")
            
            return {
                'realized_pnl': total_pnl,
                'trade_count': len(coin_fills),
                'avg_exit_price': avg_exit_price,
                'total_quantity': total_qty,
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching HL realized P&L: {e}")
            return None
    
    async def record_trade_close(self, position: dict, exit_price: float, exit_type: str, closed_at: datetime, hl_pnl: float = None):
        """
        Record a closed position as ONE trade with actual P&L.
        Only records trades matching a Nike Rocket signal.
        Mirrors Kraken version v3.0 exactly.
        """
        try:
            entry_price = position.get('avg_entry_price') or position.get('entry_fill_price')
            position_size = position.get('filled_quantity') or position.get('quantity')
            fill_count = position.get('fill_count') or 1
            leverage = position.get('leverage', 1)
            side = position.get('side')
            
            if side and side.upper() in ('BUY', 'LONG'):
                side = 'LONG'
            elif side and side.upper() in ('SELL', 'SHORT'):
                side = 'SHORT'
            
            symbol = position['symbol']
            
            if not entry_price or not position_size:
                self.logger.error("Missing entry price or position size")
                return False
            
            # Safety: entry != exit
            if abs(entry_price - exit_price) < 0.00001:
                self.logger.error(f"⚠️ BUG: entry ({entry_price}) == exit ({exit_price})")
                entry_price = position.get('entry_fill_price')
                if not entry_price or abs(entry_price - exit_price) < 0.00001:
                    return False
            
            # Signal matching
            signal_id = position.get('signal_id')
            
            if not signal_id:
                matching_signal = await self.find_matching_signal(symbol, side)
                if not matching_signal:
                    self.logger.info(f"⏭️ SKIPPING manual trade: {symbol} {side}")
                    async with self.db_pool.acquire() as conn:
                        if position.get('id'):
                            await conn.execute(
                                "UPDATE open_positions SET status = 'closed_manual', last_fill_at = NOW() WHERE id = $1",
                                position['id']
                            )
                    return False
                signal_id = matching_signal.get('signal_id') or matching_signal.get('id')
            
            # P&L calculation - prefer HL realized P&L
            if side == 'LONG':
                calculated_pnl = (exit_price - entry_price) * position_size
            else:
                calculated_pnl = (entry_price - exit_price) * position_size
            
            if hl_pnl is not None and abs(hl_pnl) > 0.01:
                profit_usd = hl_pnl
                pnl_source = "hyperliquid"
                diff = abs(profit_usd - calculated_pnl)
                if diff > 0.50:
                    self.logger.info(f"   ℹ️ P&L diff: HL=${profit_usd:+.2f}, Calc=${calculated_pnl:+.2f}")
            else:
                profit_usd = calculated_pnl
                pnl_source = "calculated"
            
            # Profit percent
            if entry_price > 0:
                profit_percent = ((exit_price - entry_price) / entry_price) * 100
                if side == 'SHORT':
                    profit_percent = -profit_percent
            else:
                profit_percent = 0
            
            fee_charged = 0  # 30-day billing
            trade_id = f"trade_{secrets.token_urlsafe(12)}"
            
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trades 
                    (user_id, signal_id, trade_id, kraken_order_id, opened_at, closed_at,
                     symbol, side, entry_price, exit_price, position_size, leverage,
                     profit_usd, profit_percent, exit_type, fee_charged, notes)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                """,
                    position['user_id'],
                    str(signal_id) if signal_id else None,
                    trade_id,
                    position.get('entry_order_id'),
                    position.get('opened_at') or position.get('first_fill_at'),
                    closed_at,
                    symbol,
                    side,
                    entry_price,
                    exit_price,
                    position_size,
                    leverage,
                    profit_usd,
                    profit_percent,
                    exit_type,
                    fee_charged,
                    f"Signal trade. {fill_count} fills. P&L source: {pnl_source}"
                )
                
                # Update user stats
                await conn.execute("""
                    UPDATE follower_users SET 
                        total_trades = COALESCE(total_trades, 0) + 1,
                        total_profit = COALESCE(total_profit, 0) + $1,
                        current_cycle_profit = COALESCE(current_cycle_profit, 0) + $1,
                        current_cycle_trades = COALESCE(current_cycle_trades, 0) + 1
                    WHERE id = $2
                """, profit_usd, position['user_id'])
                
                # Start billing cycle if not started
                position_opened_at = position.get('opened_at') or position.get('first_fill_at')
                if position_opened_at:
                    await conn.execute("""
                        UPDATE follower_users SET billing_cycle_start = $2
                        WHERE id = $1 AND billing_cycle_start IS NULL
                    """, position['user_id'], position_opened_at)
                
                # Mark position closed
                if position.get('id'):
                    await conn.execute(
                        "UPDATE open_positions SET status = 'closed', last_fill_at = NOW() WHERE id = $1",
                        position['id']
                    )
            
            emoji = "🟢" if profit_usd >= 0 else "🔴"
            self.logger.info(f"{emoji} SIGNAL TRADE closed: {symbol} {side}")
            self.logger.info(f"   Entry: ${entry_price:.4f}, Exit: ${exit_price:.4f} ({exit_type})")
            self.logger.info(f"   P&L: ${profit_usd:+.2f} ({profit_percent:+.2f}%)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error recording trade close: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def check_position(self, position: dict):
        """Check a single position - is it still open on Hyperliquid?"""
        user_short = position['user_api_key'][:15] + "..."
        
        try:
            result = self.get_exchange(
                position['user_api_key'],
                position['hl_private_key_encrypted'],
                position.get('hl_wallet_address')
            )
            
            if not result:
                self.logger.error(f"❌ {user_short}: Cannot create exchange")
                return
            
            info, exchange, wallet_address = result
            coin = position.get('hl_coin') or self.get_base_symbol(position['symbol'])
            
            # Check if position is still open
            check_result = await self.check_position_closed(
                info, wallet_address, coin,
                position['side'],
                position.get('filled_quantity') or position['quantity'],
                position['tp_order_id'],
                position['sl_order_id'],
                position['user_api_key']
            )
            
            if not check_result.get('closed'):
                if check_result.get('anomaly'):
                    self.logger.warning(f"⚠️ {user_short}: Anomaly detected - continuing to monitor")
                return
            
            # Position closed
            exit_price = check_result.get('exit_price')
            exit_type = check_result.get('exit_type', 'UNKNOWN')
            
            if exit_price is None:
                self.logger.warning(f"⚠️ {user_short}: Could not get exit price")
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE open_positions SET status = 'needs_review' WHERE id = $1",
                        position['id']
                    )
                return
            
            # Verify exit type from price proximity
            entry = position.get('avg_entry_price') or position['entry_fill_price']
            tp = position['target_tp']
            sl = position['target_sl']
            
            dist_to_tp = abs(exit_price - tp)
            dist_to_sl = abs(exit_price - sl)
            
            if dist_to_tp < dist_to_sl:
                exit_type = 'TP'
                self.logger.info(f"🎯 {user_short}: TP HIT on {position['symbol']} @ ${exit_price:.4f}")
            else:
                exit_type = 'SL'
                self.logger.info(f"🛑 {user_short}: SL HIT on {position['symbol']} @ ${exit_price:.4f}")
            
            # Fetch HL realized P&L
            hl_pnl_data = await self.get_hl_realized_pnl(
                info, wallet_address, coin,
                since_timestamp=position.get('opened_at')
            )
            
            hl_pnl = None
            if hl_pnl_data and hl_pnl_data.get('realized_pnl') is not None:
                hl_pnl = hl_pnl_data['realized_pnl']
                self.logger.info(f"   💰 HL realized P&L: ${hl_pnl:+.2f}")
            
            # Record trade closure
            await self.record_trade_close(position, exit_price, exit_type, datetime.utcnow(), hl_pnl=hl_pnl)
            
            # Cancel remaining order
            try:
                remaining_oid = position['sl_order_id'] if exit_type == 'TP' else position['tp_order_id']
                exchange.cancel(coin, int(remaining_oid))
                self.logger.info(f"   ✅ Cancelled remaining {('SL' if exit_type == 'TP' else 'TP')} order")
            except Exception as e:
                self.logger.debug(f"   Could not cancel order: {e}")
            
            # Scan fills for audit trail
            user_info = {
                'id': position['user_id'],
                'api_key': position['user_api_key'],
                'hl_private_key_encrypted': position['hl_private_key_encrypted'],
                'hl_wallet_address': position.get('hl_wallet_address'),
            }
            await self.scan_exchange_fills(user_info)
                
        except Exception as e:
            self.logger.error(f"❌ Error checking position {position['id']}: {e}")
            import traceback
            traceback.print_exc()
    
    async def check_all_positions(self):
        """
        Check all open positions and scan for new fills.
        SCALED: Parallel batch execution (50 per batch).
        """
        BATCH_SIZE = 50
        BATCH_DELAY = 0.1
        
        positions = await self.get_open_positions()
        
        if positions:
            positions_list = list(positions)
            random.shuffle(positions_list)
            
            self.logger.debug(f"📊 Checking {len(positions_list)} open positions...")
            
            for i in range(0, len(positions_list), BATCH_SIZE):
                batch = positions_list[i:i + BATCH_SIZE]
                await asyncio.gather(*[self.check_position(p) for p in batch], return_exceptions=True)
                if i + BATCH_SIZE < len(positions_list):
                    await asyncio.sleep(BATCH_DELAY)
        
        # Scan fills for ALL active users
        users = await self.get_active_users()
        users_to_scan = list(users)
        random.shuffle(users_to_scan)
        
        for i in range(0, len(users_to_scan), BATCH_SIZE):
            batch = users_to_scan[i:i + BATCH_SIZE]
            await asyncio.gather(*[self.scan_exchange_fills(u) for u in batch], return_exceptions=True)
            if i + BATCH_SIZE < len(users_to_scan):
                await asyncio.sleep(BATCH_DELAY)
    
    async def run(self):
        """Main loop - checks positions every 60 seconds"""
        self.logger.info("=" * 60)
        self.logger.info("📊 POSITION MONITOR v1.0 (HYPERLIQUID) STARTED")
        self.logger.info("=" * 60)
        self.logger.info(f"🔄 Check interval: {CHECK_INTERVAL_SECONDS} seconds")
        self.logger.info(f"📅 30-Day Rolling Billing")
        self.logger.info("=" * 60)
        
        check_count = 0
        last_status_log = datetime.now()
        
        while True:
            try:
                check_count += 1
                await self.check_all_positions()
                
                if (datetime.now() - last_status_log).total_seconds() >= 300:
                    positions = await self.get_open_positions()
                    users = await self.get_active_users()
                    self.logger.info(
                        f"💓 Monitor alive - Check #{check_count}, "
                        f"{len(positions)} positions, {len(users)} users"
                    )
                    last_status_log = datetime.now()
                
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                
            except asyncio.CancelledError:
                self.logger.info("🛑 Position monitor cancelled")
                break
                
            except Exception as e:
                self.logger.error(f"❌ Error in position monitor: {e}")
                import traceback
                traceback.print_exc()
                await log_error_to_db(self.db_pool, "system", "POSITION_MONITOR_LOOP_ERROR",
                    str(e), {"check_count": check_count})
                await asyncio.sleep(10)


async def start_position_monitor(db_pool):
    """Start the position monitor (call from main.py startup)"""
    await asyncio.sleep(40)
    
    logger.info("🚀 Starting HL position monitor...")
    
    monitor = PositionMonitor(db_pool)
    await monitor.run()
