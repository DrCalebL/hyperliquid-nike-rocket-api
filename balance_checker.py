"""
Nike Rocket - Balance Checker (Hyperliquid)
================================================
Automated balance monitoring and deposit/withdrawal detection.

Uses info.user_state(wallet) → marginSummary.accountValue
Mirrors Kraken balance_checker.py consolidated v2.

Author: Nike Rocket Team
"""

import asyncio
import asyncpg
import os
import json
from decimal import Decimal
from datetime import datetime, timedelta
import logging
from cryptography.fernet import Fernet
from typing import Optional, Dict

from hyperliquid.info import Info
from hyperliquid.utils import constants
from eth_account import Account

from order_utils import notify_api_failure, notify_database_error, notify_critical_error

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("balance_checker")

USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"


async def log_error_to_db(pool, api_key: str, error_type: str, error_message: str, context: Optional[Dict] = None):
    """Log error to error_logs table"""
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


# Encryption
ENCRYPTION_KEY = os.getenv("CREDENTIALS_ENCRYPTION_KEY")
cipher = Fernet(ENCRYPTION_KEY.encode()) if ENCRYPTION_KEY else None


def decrypt_private_key(encrypted_key: str) -> Optional[str]:
    """Decrypt HL private key"""
    if not cipher or not encrypted_key:
        return None
    try:
        return cipher.decrypt(encrypted_key.encode()).decode()
    except Exception as e:
        logger.error(f"Error decrypting credentials: {e}")
        return None


class BalanceChecker:
    """
    Monitors user Hyperliquid balances and detects deposits/withdrawals.
    """
    
    def __init__(self, db_pool):
        self.db_pool = db_pool
        base_url = constants.TESTNET_API_URL if USE_TESTNET else constants.MAINNET_API_URL
        self.info = Info(base_url, skip_ws=True)

    async def check_all_users(self):
        """Check balance for all users with portfolio tracking enabled"""
        try:
            async with self.db_pool.acquire() as conn:
                # Check if tables exist
                table_check = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'follower_users'
                    )
                """)
                
                if not table_check:
                    logger.info("✓ Tables not yet created")
                    return
                
                users = await conn.fetch("""
                    SELECT DISTINCT
                        fu.id,
                        fu.api_key,
                        fu.hl_private_key_encrypted,
                        fu.hl_wallet_address
                    FROM follower_users fu
                    WHERE fu.credentials_set = true
                      AND fu.hl_private_key_encrypted IS NOT NULL
                      AND fu.portfolio_initialized = true
                """)
                
                if not users:
                    logger.debug("No users to check")
                    return
                
                logger.info(f"📊 Checking balances for {len(users)} users...")
                
                for user in users:
                    await self._check_user_balance(user)
                    
        except Exception as e:
            logger.error(f"Error in check_all_users: {e}")
            await log_error_to_db(self.db_pool, "system", "BALANCE_CHECK_ERROR", str(e))
    
    async def _check_user_balance(self, user):
        """Check balance for a single user"""
        user_short = user['api_key'][:15] + "..."
        
        try:
            wallet_address = user.get('hl_wallet_address')
            
            if not wallet_address:
                # Derive from private key
                private_key = decrypt_private_key(user['hl_private_key_encrypted'])
                if not private_key:
                    return
                account = Account.from_key(private_key)
                wallet_address = account.address
            
            # Get current balance from HL
            state = self.info.user_state(wallet_address)
            if not state or 'marginSummary' not in state:
                return
            
            current_balance = float(state['marginSummary']['accountValue'])
            
            async with self.db_pool.acquire() as conn:
                # Get last known balance
                last_record = await conn.fetchrow("""
                    SELECT balance_usd FROM portfolio_snapshots 
                    WHERE follower_user_id = $1
                    ORDER BY recorded_at DESC LIMIT 1
                """, user['id'])
                
                if last_record:
                    last_balance = float(last_record['balance_usd'])
                    diff = current_balance - last_balance
                    
                    # Detect significant changes (> $10 that aren't from trading)
                    if abs(diff) > 10:
                        # Check if there are open positions (could explain change)
                        open_pos = await conn.fetchval("""
                            SELECT COUNT(*) FROM open_positions 
                            WHERE user_id = $1 AND status = 'open'
                        """, user['id'])
                        
                        if open_pos == 0 and abs(diff) > 10:
                            tx_type = 'deposit' if diff > 0 else 'withdrawal'
                            
                            await conn.execute("""
                                INSERT INTO portfolio_transactions 
                                (follower_user_id, type, amount_usd, detected_at, notes)
                                VALUES ($1, $2, $3, NOW(), $4)
                            """, user['id'], tx_type, abs(diff),
                                f"Auto-detected: balance changed ${diff:+.2f}")
                            
                            logger.info(f"💰 {user_short}: {tx_type.upper()} detected: ${abs(diff):,.2f}")
                
                # Record current snapshot
                await conn.execute("""
                    INSERT INTO portfolio_snapshots 
                    (follower_user_id, balance_usd, recorded_at)
                    VALUES ($1, $2, NOW())
                """, user['id'], current_balance)
                    
        except Exception as e:
            logger.debug(f"Could not check balance for {user_short}: {e}")


class BalanceCheckerScheduler:
    """Scheduler that runs balance checks at intervals"""
    
    def __init__(self, db_pool, check_interval_minutes: int = 60, startup_delay_seconds: int = 30):
        self.checker = BalanceChecker(db_pool)
        self.check_interval = check_interval_minutes * 60
        self.startup_delay = startup_delay_seconds
        self._running = False
    
    async def start(self):
        """Start the scheduler loop"""
        self._running = True
        
        # Wait for DB to be ready
        await asyncio.sleep(self.startup_delay)
        
        logger.info(f"📊 Balance checker started (interval: {self.check_interval // 60} minutes)")
        
        while self._running:
            try:
                await self.checker.check_all_users()
            except Exception as e:
                logger.error(f"Balance checker error: {e}")
            
            await asyncio.sleep(self.check_interval)
    
    def stop(self):
        self._running = False
