# HYPERLIQUID PORTFOLIO API - AUTO-DETECT INITIAL CAPITAL
# ========================================================
# Adapted for Hyperliquid SDK.
# EXACT same formulas for ROI, Profit Factor, Sharpe Ratio, Max Drawdown, etc.
#
# Key features:
# - Uses Hyperliquid SDK (info.user_state) instead of CCXT (fetch_balance)
# - Credentials: hl_private_key_encrypted + hl_wallet_address (HL-native)
# - No symbol conversion needed (Hyperliquid uses simple symbols like "ADA")

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime, timedelta
from decimal import Decimal
import asyncpg
import os
import statistics
import json
import io
import csv
from cryptography.fernet import Fernet
from typing import Optional, Dict

router = APIRouter()

# Setup encryption
ENCRYPTION_KEY = os.getenv("CREDENTIALS_ENCRYPTION_KEY")
if ENCRYPTION_KEY:
    cipher = Fernet(ENCRYPTION_KEY.encode())
else:
    cipher = None


async def log_error_async(api_key: str, error_type: str, error_message: str, context: Optional[Dict] = None):
    """Log error to error_logs table for admin dashboard visibility"""
    try:
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """INSERT INTO error_logs (api_key, error_type, error_message, context) 
               VALUES ($1, $2, $3, $4)""",
            api_key[:20] + "..." if api_key and len(api_key) > 20 else api_key,
            error_type,
            error_message[:500] if error_message else None,
            json.dumps(context) if context else None
        )
        await conn.close()
    except Exception as e:
        print(f"Failed to log error: {e}")


async def validate_api_key(api_key: str, db_pool=None) -> dict:
    """Validate API key exists in database."""
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    close_pool = False
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        close_pool = True
    
    conn = await db_pool.acquire()
    user = await conn.fetchrow(
        "SELECT id, portfolio_initialized FROM follower_users WHERE api_key = $1",
        api_key
    )
    await db_pool.release(conn)
    
    if close_pool:
        await db_pool.close()
    
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return dict(user)


def decrypt_private_key(encrypted_key: str) -> Optional[str]:
    """Decrypt Hyperliquid private key"""
    if not cipher or not encrypted_key:
        return None
    try:
        return cipher.decrypt(encrypted_key.encode()).decode()
    except Exception as e:
        print(f"Error decrypting credentials: {e}")
        return None


async def get_hl_credentials(api_key: str):
    """Get user's Hyperliquid credentials from database"""
    try:
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        user = await conn.fetchrow("""
            SELECT 
                hl_private_key_encrypted, 
                hl_wallet_address,
                credentials_set
            FROM follower_users
            WHERE api_key = $1
            AND credentials_set = true
        """, api_key)
        
        await conn.close()
        
        if not user:
            return None
        
        private_key = decrypt_private_key(user['hl_private_key_encrypted'])
        
        if not private_key:
            await log_error_async(
                api_key, "CREDENTIAL_DECRYPT_FAILED",
                "Failed to decrypt Hyperliquid private key",
                {"function": "get_hl_credentials"}
            )
            return None
        
        return {
            'private_key': private_key,
            'wallet_address': user['hl_wallet_address']
        }
    except Exception as e:
        print(f"Error getting Hyperliquid credentials: {e}")
        import traceback
        traceback.print_exc()
        await log_error_async(
            api_key, "GET_CREDENTIALS_ERROR",
            str(e),
            {"function": "get_hl_credentials", "traceback": traceback.format_exc()[:500]}
        )
        return None


async def get_current_hl_balance(wallet_address: str, user_api_key: str = None):
    """
    Get current USD balance from Hyperliquid using SDK.
    
    Uses Hyperliquid SDK for balance fetching:
    info.user_state(wallet_address) -> marginSummary.accountValue
    """
    try:
        import asyncio
        from hyperliquid.info import Info
        from hyperliquid.utils import constants
        
        info = Info(constants.MAINNET_API_URL, skip_ws=True)
        
        user_state = await asyncio.to_thread(info.user_state, wallet_address)
        
        if not user_state or 'marginSummary' not in user_state:
            print(f"⚠️ Could not fetch user state for {wallet_address[:10]}...")
            return None
        
        margin_summary = user_state['marginSummary']
        account_value = float(margin_summary.get('accountValue', 0))
        
        print(f"💵 Hyperliquid balance detected: ${account_value:.2f}")
        return Decimal(str(account_value))
        
    except Exception as e:
        error_msg = f"Error getting Hyperliquid balance: {e}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        
        if user_api_key:
            await log_error_async(
                user_api_key,
                "HL_BALANCE_ERROR",
                str(e),
                {"function": "get_current_hl_balance", "traceback": traceback.format_exc()[:500]}
            )
        return None


@router.post("/api/portfolio/initialize")
async def initialize_portfolio_autodetect(request: Request):
    """
    Initialize portfolio tracking - AUTO-DETECTS initial capital from Hyperliquid.
    Uses follower_users as single source of truth.
    """
    api_key = request.headers.get("X-API-Key")
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    try:
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        credentials = await get_hl_credentials(api_key)
        
        if not credentials:
            await conn.close()
            print(f"ℹ️ User {api_key[:15]}... attempted portfolio init without credentials")
            return {
                "status": "setup_required",
                "message": "Please set up your trading agent first. Go to the Setup page to enter your Hyperliquid credentials."
            }
        
        # Check if already initialized
        fu_existing = await conn.fetchrow(
            "SELECT portfolio_initialized, initial_capital FROM follower_users WHERE api_key = $1",
            api_key
        )
        
        if fu_existing and fu_existing['portfolio_initialized']:
            await conn.close()
            return {
                "status": "already_initialized",
                "message": "Portfolio already initialized",
                "initial_capital": float(fu_existing['initial_capital'] or 0)
            }
        
        hl_balance = await get_current_hl_balance(
            credentials['wallet_address'],
            api_key
        )
        
        if hl_balance is None:
            await conn.close()
            await log_error_async(
                api_key, "HL_CONNECTION_FAILED",
                "Could not connect to Hyperliquid or fetch balance",
                {"endpoint": "/api/portfolio/initialize"}
            )
            return {
                "status": "error",
                "message": "Could not connect to Hyperliquid. Please check your agent setup."
            }
        
        if hl_balance <= 0:
            await conn.close()
            return {
                "status": "error",
                "message": "Your Hyperliquid balance is $0. Please deposit funds first."
            }
        
        MINIMUM_BALANCE = 10
        if hl_balance < MINIMUM_BALANCE:
            await conn.close()
            return {
                "status": "error",
                "message": f"Minimum balance: ${MINIMUM_BALANCE}. Your balance: ${float(hl_balance):.2f}"
            }
        
        initial_capital = float(hl_balance)
        
        from decimal import Decimal as Dec
        await conn.execute("""
            UPDATE follower_users SET
                initial_capital = $1,
                last_known_balance = $2,
                portfolio_initialized = true,
                started_tracking_at = CURRENT_TIMESTAMP
            WHERE api_key = $3
        """, initial_capital, Dec(str(initial_capital)), api_key)
        
        user_id = await conn.fetchval(
            "SELECT id FROM follower_users WHERE api_key = $1",
            api_key
        )
        
        await conn.execute("""
            INSERT INTO portfolio_transactions (
                follower_user_id, user_id, transaction_type, amount, detection_method, notes
            ) VALUES ($1, $2, 'initial', $3, 'automatic', $4)
        """, user_id, api_key, initial_capital,
            f'Auto-detected from Hyperliquid balance: ${initial_capital:,.2f}')
        
        await conn.close()
        
        return {
            "status": "success",
            "message": f"Portfolio initialized with ${initial_capital:,.2f}",
            "initial_capital": initial_capital,
            "detected_from": "hyperliquid_balance"
        }
        
    except Exception as e:
        print(f"Error initializing portfolio: {e}")
        import traceback
        traceback.print_exc()
        await log_error_async(
            api_key, "PORTFOLIO_INIT_ERROR",
            str(e),
            {"endpoint": "/api/portfolio/initialize", "traceback": traceback.format_exc()[:500]}
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/portfolio/balance-summary")
async def get_balance_summary(request: Request):
    """
    Get portfolio balance summary for the dashboard overview cards.
    Returns current value, initial capital, deposits/withdrawals, ROI, and profit.
    """
    api_key = request.headers.get("X-API-Key") or request.query_params.get("key")
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    try:
        await validate_api_key(api_key)
        
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        try:
            # Get user info
            user_info = await conn.fetchrow("""
                SELECT initial_capital, last_known_balance, hl_wallet_address, 
                       started_tracking_at, created_at
                FROM follower_users WHERE api_key = $1
            """, api_key)
            
            if not user_info or not user_info['initial_capital']:
                return {"status": "no_data", "message": "Portfolio not initialized"}
            
            initial_capital = float(user_info['initial_capital'] or 0)
            wallet_address = user_info.get('hl_wallet_address')
            
            # Try to get live balance from HL
            current_value = float(user_info['last_known_balance'] or 0)
            last_balance_check = None
            
            if wallet_address:
                try:
                    live_balance = await get_current_hl_balance(wallet_address, api_key)
                    if live_balance is not None:
                        current_value = float(live_balance)
                        last_balance_check = datetime.utcnow().isoformat()
                        # Update last_known_balance
                        from decimal import Decimal as Dec
                        await conn.execute(
                            "UPDATE follower_users SET last_known_balance = $1 WHERE api_key = $2",
                            Dec(str(current_value)), api_key
                        )
                except Exception as e:
                    print(f"Could not fetch live balance: {e}")
            
            # Get deposit/withdrawal totals
            txn_totals = await conn.fetchrow("""
                SELECT 
                    COALESCE(SUM(CASE WHEN transaction_type = 'deposit' THEN amount ELSE 0 END), 0) as total_deposits,
                    COALESCE(SUM(CASE WHEN transaction_type = 'withdrawal' THEN amount ELSE 0 END), 0) as total_withdrawals
                FROM portfolio_transactions
                WHERE user_id = $1
            """, api_key)
            
            total_deposits = float(txn_totals['total_deposits']) if txn_totals else 0
            total_withdrawals = float(txn_totals['total_withdrawals']) if txn_totals else 0
            net_deposits = total_deposits - total_withdrawals
            
            # Total capital = initial + net deposits
            total_capital = initial_capital + net_deposits
            
            # Total profit = current value - total capital
            total_profit = current_value - total_capital
            
            # ROI calculations
            roi_on_initial = (total_profit / initial_capital * 100) if initial_capital > 0 else 0
            roi_on_total = (total_profit / total_capital * 100) if total_capital > 0 else 0
            
            # Get last snapshot time
            if not last_balance_check:
                last_snap = await conn.fetchrow("""
                    SELECT recorded_at FROM portfolio_snapshots 
                    WHERE follower_user_id = (SELECT id FROM follower_users WHERE api_key = $1)
                    ORDER BY recorded_at DESC LIMIT 1
                """, api_key)
                if last_snap:
                    last_balance_check = last_snap['recorded_at'].isoformat()
            
            return {
                "status": "success",
                "current_value": round(current_value, 2),
                "initial_capital": round(initial_capital, 2),
                "net_deposits": round(net_deposits, 2),
                "total_profit": round(total_profit, 2),
                "total_deposits": round(total_deposits, 2),
                "total_withdrawals": round(total_withdrawals, 2),
                "total_capital": round(total_capital, 2),
                "roi_on_initial": round(roi_on_initial, 2),
                "roi_on_total": round(roi_on_total, 2),
                "last_balance_check": last_balance_check
            }
        finally:
            await conn.close()
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"Error in balance-summary: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/portfolio/stats")
async def get_portfolio_stats(request: Request, period: str = "30d"):
    """
    Get portfolio statistics for a specific time period.
    
    ═══════════════════════════════════════════════════════════════
    FORMULA DOCUMENTATION:
    ═══════════════════════════════════════════════════════════════
    
    1. TOTAL PROFIT (period): SUM(profit_usd) for trades in period
    2. ROI ON INITIAL: (period_profit / initial_capital) × 100
    3. ROI ON TOTAL: (period_profit / total_capital) × 100
       Where: total_capital = initial_capital + net_deposits
    4. PROFIT FACTOR: SUM(winning_pnl) / ABS(SUM(losing_pnl))
    5. WIN RATE: (winning_trades / total_trades) × 100
    6. BEST TRADE: MAX(profit_usd)
    7. WORST TRADE: MIN(profit_usd)
    8. AVG MONTHLY PROFIT: total_profit / months_active
    9. MAX DRAWDOWN: MAX((peak - trough) / peak) × 100
       Built from TRADING-ONLY equity curve (ignores deposits/withdrawals)
    10. SHARPE RATIO: (avg_return / std_dev) × sqrt(trades_per_year)
        Uses actual trade frequency, not assumed 252 daily trades
    11. DAYS ACTIVE: (current_date - first_trade_date).days
    ═══════════════════════════════════════════════════════════════
    """
    api_key = request.headers.get("X-API-Key") or request.query_params.get("key")
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    try:
        await validate_api_key(api_key)
        
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Get user info for initial capital
        user_info = await conn.fetchrow("""
            SELECT initial_capital, started_tracking_at, created_at
            FROM follower_users WHERE api_key = $1
        """, api_key)
        
        if not user_info or not user_info['initial_capital']:
            await conn.close()
            return {"status": "no_data", "message": "Portfolio not initialized"}
        
        initial_capital = float(user_info['initial_capital'] or 0)
        
        # Get deposit/withdrawal totals
        txn_totals = await conn.fetchrow("""
            SELECT 
                COALESCE(SUM(CASE WHEN transaction_type = 'deposit' THEN amount ELSE 0 END), 0) as total_deposits,
                COALESCE(SUM(CASE WHEN transaction_type = 'withdrawal' THEN amount ELSE 0 END), 0) as total_withdrawals
            FROM portfolio_transactions
            WHERE user_id = $1
        """, api_key)
        
        total_deposits = float(txn_totals['total_deposits']) if txn_totals else 0
        total_withdrawals = float(txn_totals['total_withdrawals']) if txn_totals else 0
        
        # Calculate date range based on period
        now = datetime.utcnow()
        if period == "7d":
            start_date = now - timedelta(days=7)
            period_label = "Last 7 Days"
        elif period == "30d":
            start_date = now - timedelta(days=30)
            period_label = "Last 30 Days"
        elif period == "90d":
            start_date = now - timedelta(days=90)
            period_label = "Last 90 Days"
        elif period == "1y":
            start_date = now - timedelta(days=365)
            period_label = "Last 1 Year"
        else:
            start_date = datetime(2020, 1, 1)
            period_label = "All Time"
        
        # Period-specific trades
        trades_query = await conn.fetch("""
            SELECT 
                t.profit_usd as pnl_usd,
                t.profit_percent as pnl_percent,
                t.closed_at as exit_time,
                t.opened_at as entry_time
            FROM trades t
            JOIN follower_users fu ON t.user_id = fu.id
            WHERE fu.api_key = $1
            AND t.closed_at >= $2
            ORDER BY t.closed_at DESC
        """, api_key, start_date)
        
        # ALL-TIME trades
        all_trades_query = await conn.fetch("""
            SELECT 
                t.profit_usd as pnl_usd,
                t.profit_percent as pnl_percent,
                t.closed_at as exit_time,
                t.opened_at as entry_time
            FROM trades t
            JOIN follower_users fu ON t.user_id = fu.id
            WHERE fu.api_key = $1
            ORDER BY t.closed_at DESC
        """, api_key)
        
        first_trade = await conn.fetchval("""
            SELECT MIN(t.opened_at)
            FROM trades t
            JOIN follower_users fu ON t.user_id = fu.id
            WHERE fu.api_key = $1
        """, api_key)
        
        # All-time profit for summary
        all_time_profit = await conn.fetchval("""
            SELECT COALESCE(SUM(t.profit_usd), 0)
            FROM trades t
            JOIN follower_users fu ON t.user_id = fu.id
            WHERE fu.api_key = $1
        """, api_key)
        
        await conn.close()
        
        total_trades = len(trades_query)
        all_time_total_trades = len(all_trades_query)
        
        # ═══════════════════════════════════════════════════════════════
        # ALL-TIME CALCULATIONS
        # ═══════════════════════════════════════════════════════════════
        all_time_days_active = max(1, (now - first_trade).days) if first_trade else 0
        
        if all_time_total_trades > 0:
            all_pnl_values = [float(t['pnl_usd'] or 0) for t in all_trades_query]
            all_winning_pnl = [p for p in all_pnl_values if p > 0]
            all_losing_pnl = [p for p in all_pnl_values if p < 0]
            all_total_wins = sum(all_winning_pnl) if all_winning_pnl else 0
            all_total_losses = abs(sum(all_losing_pnl)) if all_losing_pnl else 0
            
            if all_total_losses == 0 and all_total_wins > 0:
                all_time_profit_factor = None  # Infinite
            elif all_total_losses == 0:
                all_time_profit_factor = 0
            else:
                all_time_profit_factor = round(all_total_wins / all_total_losses, 2)
            
            if len(all_pnl_values) > 1 and all_time_days_active > 0:
                all_avg_return = statistics.mean(all_pnl_values)
                all_std_dev = statistics.stdev(all_pnl_values)
                if all_std_dev > 0:
                    trades_per_year = all_time_total_trades * (365 / all_time_days_active)
                    all_time_sharpe = round((all_avg_return / all_std_dev) * (trades_per_year ** 0.5), 2)
                else:
                    all_time_sharpe = 0
            else:
                all_time_sharpe = None
        else:
            all_time_profit_factor = None
            all_time_sharpe = None
        
        total_capital = initial_capital + total_deposits - total_withdrawals
        
        if total_trades == 0:
            return {
                "status": "no_trades",
                "period": period,
                "period_label": period_label,
                "total_profit": 0,
                "all_time_profit": float(all_time_profit or 0),
                "roi_on_initial": 0,
                "roi_on_total": 0,
                "initial_capital": initial_capital,
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0,
                "profit_factor": None,
                "gross_wins": 0,
                "gross_losses": 0,
                "best_trade": 0,
                "worst_trade": 0,
                "avg_trade": 0,
                "avg_monthly_profit": 0,
                "max_drawdown": 0,
                "recovery_from_dd": 100,
                "sharpe_ratio": None,
                "days_active": 0,
                "all_time_profit_factor": all_time_profit_factor,
                "all_time_sharpe": all_time_sharpe,
                "all_time_days_active": all_time_days_active,
                "total_deposits": total_deposits,
                "total_withdrawals": total_withdrawals
            }
        
        # Extract PnL values
        pnl_values = [float(t['pnl_usd'] or 0) for t in trades_query]
        
        # 1. TOTAL PROFIT (period)
        period_profit = sum(pnl_values)
        
        # 2. WINNING/LOSING TRADES
        winning_pnl = [p for p in pnl_values if p > 0]
        losing_pnl = [p for p in pnl_values if p < 0]
        winning_trades = len(winning_pnl)
        losing_trades = len(losing_pnl)
        
        # 3. WIN RATE
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        # 4. PROFIT FACTOR
        total_wins = sum(winning_pnl) if winning_pnl else 0
        total_losses = abs(sum(losing_pnl)) if losing_pnl else 0
        
        # 5. BEST TRADE
        best_trade = max(pnl_values) if pnl_values else 0
        
        # 6. WORST TRADE
        worst_trade = min(pnl_values) if pnl_values else 0
        
        # 7. AVERAGE TRADE
        avg_trade = period_profit / total_trades if total_trades > 0 else 0
        
        # 8. DAYS ACTIVE
        days_active = (now - first_trade).days if first_trade else 0
        
        # 9. AVG MONTHLY PROFIT
        months_active = max(1, days_active / 30)
        avg_monthly_profit = float(all_time_profit or 0) / months_active
        
        # 10. MAX DRAWDOWN (TRADING-ONLY equity curve)
        if initial_capital <= 0:
            initial_capital = 1000  # Fallback
        
        trades_sorted = sorted(trades_query, key=lambda t: t['exit_time'])
        
        equity_curve = [initial_capital]
        cumulative_pnl = 0
        
        for trade in trades_sorted:
            cumulative_pnl += float(trade['pnl_usd'] or 0)
            equity_curve.append(initial_capital + cumulative_pnl)
        
        max_drawdown = 0
        running_peak = equity_curve[0]
        
        for value in equity_curve:
            if value > running_peak:
                running_peak = value
            if running_peak > 0:
                drawdown = (running_peak - value) / running_peak * 100
                max_drawdown = max(max_drawdown, drawdown)
        
        # 11. SHARPE RATIO
        if len(pnl_values) > 1 and days_active > 0:
            avg_return = statistics.mean(pnl_values)
            std_dev = statistics.stdev(pnl_values)
            if std_dev > 0:
                trades_per_year = total_trades * (365 / days_active)
                sharpe_ratio = round((avg_return / std_dev) * (trades_per_year ** 0.5), 2)
            else:
                sharpe_ratio = 0
        else:
            sharpe_ratio = None
        
        # Period ROI
        period_roi_initial = (period_profit / initial_capital * 100) if initial_capital > 0 else 0
        period_roi_total = (period_profit / total_capital * 100) if total_capital > 0 else 0
        
        # Period-specific Days Active
        trades_sorted_list = sorted(trades_query, key=lambda t: t['exit_time'])
        first_trade_in_period = trades_sorted_list[0]['entry_time'] if trades_sorted_list else None
        
        if period == "all" and first_trade:
            days_active = max(1, (now - first_trade).days)
        elif first_trade_in_period:
            days_active = max(1, (now - first_trade_in_period).days)
        else:
            days_active = 0
        
        # Profit Factor
        if total_losses == 0 and total_wins > 0:
            profit_factor = None
        elif total_losses == 0:
            profit_factor = 0
        else:
            profit_factor = round(total_wins / total_losses, 2)
        
        # Recovery from drawdown
        current_equity = equity_curve[-1] if equity_curve else initial_capital
        if max_drawdown > 0 and running_peak > 0:
            current_drawdown = (running_peak - current_equity) / running_peak * 100
            recovery_from_dd = max(0, (max_drawdown - current_drawdown) / max_drawdown * 100) if max_drawdown > 0 else 100
        else:
            recovery_from_dd = 100
        
        return {
            "status": "success",
            "period": period,
            "period_label": period_label,
            "total_profit": round(period_profit, 2),
            "all_time_profit": round(float(all_time_profit or 0), 2),
            "roi_on_initial": round(period_roi_initial, 2),
            "roi_on_total": round(period_roi_total, 2),
            "initial_capital": round(initial_capital, 2),
            "total_deposits": round(total_deposits, 2),
            "total_withdrawals": round(total_withdrawals, 2),
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "win_rate": round(win_rate, 1),
            "profit_factor": profit_factor,
            "gross_wins": round(total_wins, 2),
            "gross_losses": round(total_losses, 2),
            "best_trade": round(best_trade, 2),
            "worst_trade": round(worst_trade, 2),
            "avg_trade": round(avg_trade, 2),
            "avg_monthly_profit": round(avg_monthly_profit, 2),
            "max_drawdown": round(max_drawdown, 1),
            "recovery_from_dd": round(recovery_from_dd, 1),
            "sharpe_ratio": sharpe_ratio,
            "days_active": days_active,
            "all_time_profit_factor": all_time_profit_factor,
            "all_time_sharpe": all_time_sharpe,
            "all_time_days_active": all_time_days_active,
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in portfolio stats: {e}")
        import traceback
        traceback.print_exc()
        await log_error_async(
            api_key, "PORTFOLIO_STATS_ERROR",
            str(e),
            {"endpoint": "/api/portfolio/stats", "period": period, "traceback": traceback.format_exc()[:500]}
        )
        raise HTTPException(status_code=500, detail="Error loading portfolio stats")


@router.get("/api/portfolio/equity-curve")
async def get_equity_curve(request: Request):
    """
    Get trading-only equity curve for charting.
    Starts at initial capital, adds each trade's PnL chronologically.
    IGNORES deposits and withdrawals - pure trading performance.
    """
    api_key = request.headers.get("X-API-Key") or request.query_params.get("key")
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    try:
        await validate_api_key(api_key)
        
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        user_info = await conn.fetchrow("""
            SELECT initial_capital, created_at
            FROM follower_users WHERE api_key = $1
        """, api_key)
        
        initial_capital = float(user_info['initial_capital'] or 0) if user_info else 0
        start_date = user_info['created_at'] if user_info else None
        
        if initial_capital <= 0:
            initial_capital = 1000
        
        trades = await conn.fetch("""
            SELECT 
                t.profit_usd as pnl_usd,
                t.closed_at as exit_time,
                t.symbol,
                t.side
            FROM trades t
            JOIN follower_users fu ON t.user_id = fu.id
            WHERE fu.api_key = $1
            AND t.closed_at IS NOT NULL
            ORDER BY t.closed_at ASC
        """, api_key)
        
        await conn.close()
        
        if not trades:
            return {
                "status": "no_trades",
                "equity_curve": [{
                    "date": start_date.isoformat() if start_date else datetime.utcnow().isoformat(),
                    "equity": initial_capital,
                    "pnl": 0,
                    "cumulative_pnl": 0
                }],
                "initial_capital": initial_capital,
                "current_equity": initial_capital,
                "max_equity": initial_capital,
                "min_equity": initial_capital,
                "max_drawdown": 0,
                "total_trades": 0,
                "total_pnl": 0
            }
        
        equity_curve = []
        cumulative_pnl = 0
        running_peak = initial_capital
        max_drawdown = 0
        max_equity = initial_capital
        min_equity = initial_capital
        
        equity_curve.append({
            "date": start_date.isoformat() if start_date else trades[0]['exit_time'].isoformat(),
            "equity": round(initial_capital, 2),
            "pnl": 0,
            "cumulative_pnl": 0,
            "trade": "Starting Balance"
        })
        
        for trade in trades:
            pnl = float(trade['pnl_usd'] or 0)
            cumulative_pnl += pnl
            current_equity = initial_capital + cumulative_pnl
            
            max_equity = max(max_equity, current_equity)
            min_equity = min(min_equity, current_equity)
            
            if current_equity > running_peak:
                running_peak = current_equity
            if running_peak > 0:
                drawdown = (running_peak - current_equity) / running_peak * 100
                max_drawdown = max(max_drawdown, drawdown)
            
            # Clean symbol for display
            raw_symbol = trade['symbol'] or ''
            if '/' in raw_symbol:
                clean_symbol = raw_symbol.split('/')[0].upper()
            else:
                clean_symbol = raw_symbol.upper()
            
            equity_curve.append({
                "date": trade['exit_time'].isoformat(),
                "equity": round(current_equity, 2),
                "pnl": round(pnl, 2),
                "cumulative_pnl": round(cumulative_pnl, 2),
                "trade": f"{trade['side']} {clean_symbol}"
            })
        
        current_equity = initial_capital + cumulative_pnl
        
        return {
            "status": "success",
            "equity_curve": equity_curve,
            "initial_capital": round(initial_capital, 2),
            "current_equity": round(current_equity, 2),
            "max_equity": round(max_equity, 2),
            "min_equity": round(min_equity, 2),
            "max_drawdown": round(max_drawdown, 2),
            "total_trades": len(trades),
            "total_pnl": round(cumulative_pnl, 2)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in equity curve: {e}")
        import traceback
        traceback.print_exc()
        await log_error_async(
            api_key, "EQUITY_CURVE_ERROR",
            str(e),
            {"endpoint": "/api/portfolio/equity-curve", "traceback": traceback.format_exc()[:500]}
        )
        raise HTTPException(status_code=500, detail="Error loading equity curve")


# ==================== TRADE EXPORT ENDPOINTS ====================

@router.get("/api/portfolio/trades/monthly-csv")
async def export_monthly_trades(request: Request, key: str, year: int, month: int):
    """Export monthly trades as CSV"""
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        user = await conn.fetchrow(
            "SELECT id, email, fee_tier FROM follower_users WHERE api_key = $1",
            key
        )
        
        if not user:
            await conn.close()
            raise HTTPException(status_code=404, detail="User not found")
        
        start_date = datetime(year, month, 1)
        end_date = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
        
        trades = await conn.fetch("""
            SELECT closed_at, symbol, side, entry_price, exit_price,
                   position_size, leverage, profit_usd, profit_percent, notes
            FROM trades
            WHERE user_id = $1 AND closed_at >= $2 AND closed_at < $3
            ORDER BY closed_at ASC
        """, user['id'], start_date, end_date)
        
        await conn.close()
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        month_name = start_date.strftime('%B %Y')
        writer.writerow([f"Trade History - {month_name} (Hyperliquid)"])
        writer.writerow([f"User: {user['email']}"])
        writer.writerow([f"Fee Tier: {user['fee_tier'] or 'standard'}"])
        writer.writerow([])
        writer.writerow(['Date (UTC)', 'Symbol', 'Side', 'Entry Price', 'Exit Price',
                        'Position Size', 'Leverage', 'P&L ($)', 'P&L (%)', 'Notes'])
        
        total_pnl = 0
        winning_trades = 0
        losing_trades = 0
        
        for trade in trades:
            pnl = float(trade['profit_usd'] or 0)
            total_pnl += pnl
            if pnl > 0: winning_trades += 1
            elif pnl < 0: losing_trades += 1
            
            writer.writerow([
                trade['closed_at'].strftime('%Y-%m-%d %H:%M:%S'),
                trade['symbol'], trade['side'],
                f"${trade['entry_price']:.4f}", f"${trade['exit_price']:.4f}",
                trade['position_size'], f"{trade['leverage']}x",
                f"${pnl:+.2f}", f"{trade['profit_percent']:+.2f}%",
                trade['notes'] or ''
            ])
        
        writer.writerow([])
        writer.writerow(['MONTHLY SUMMARY'])
        writer.writerow(['Total Trades', len(trades)])
        writer.writerow(['Winning', winning_trades])
        writer.writerow(['Losing', losing_trades])
        writer.writerow(['Win Rate', f"{(winning_trades/len(trades)*100):.1f}%" if trades else "N/A"])
        writer.writerow(['NET P&L', f"${total_pnl:+.2f}"])
        
        fee_rates = {'team': 0.0, 'vip': 0.05, 'standard': 0.10}
        fee_rate = fee_rates.get(user['fee_tier'] or 'standard', 0.10)
        fee_due = max(0, total_pnl * fee_rate) if total_pnl > 0 else 0
        writer.writerow(['Fee Rate', f"{int(fee_rate * 100)}%"])
        writer.writerow(['Fee Due', f"${fee_due:.2f}"])
        
        output.seek(0)
        filename = f"hl_trades_{year}_{month:02d}_{user['email'].split('@')[0]}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error exporting monthly trades: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# MISSING ENDPOINTS - Transactions, Yearly CSV, Deposit/Withdraw
# ============================================================

@router.get("/api/portfolio/transactions")
async def get_transactions(request: Request, key: str = "", limit: int = 20, offset: int = 0, start_date: str = None, end_date: str = None):
    """
    Get paginated portfolio transactions (deposits, withdrawals, fees).
    Dashboard calls: /api/portfolio/transactions?key=...&limit=20&offset=0
    Expected response: { status: 'success', transactions: [...] }
    Each tx needs: created_at, transaction_type, amount, detection_method
    """
    api_key = request.headers.get("X-API-Key") or key
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        try:
            user = await conn.fetchrow(
                "SELECT id FROM follower_users WHERE api_key = $1",
                api_key
            )
            
            if not user:
                await conn.close()
                raise HTTPException(status_code=404, detail="User not found")
            
            # Build query with optional date filters
            query = """
                SELECT id, transaction_type, amount, detection_method, detected_at, notes
                FROM portfolio_transactions
                WHERE follower_user_id = $1
            """
            params = [user['id']]
            param_idx = 2
            
            if start_date:
                try:
                    sd = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                    query += f" AND detected_at >= ${param_idx}"
                    params.append(sd)
                    param_idx += 1
                except ValueError:
                    pass
            
            if end_date:
                try:
                    ed = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                    query += f" AND detected_at <= ${param_idx}"
                    params.append(ed)
                    param_idx += 1
                except ValueError:
                    pass
            
            query += f" ORDER BY detected_at DESC LIMIT ${param_idx} OFFSET ${param_idx + 1}"
            params.extend([limit, offset])
            
            rows = await conn.fetch(query, *params)
            
            transactions = []
            for row in rows:
                transactions.append({
                    "id": row['id'],
                    "transaction_type": row['transaction_type'],
                    "amount": float(row['amount']) if row['amount'] else 0.0,
                    "detection_method": row['detection_method'] or "automatic",
                    "created_at": row['detected_at'].isoformat() if row['detected_at'] else None,
                    "notes": row['notes']
                })
            
            return {
                "status": "success",
                "transactions": transactions,
                "offset": offset,
                "limit": limit
            }
        
        finally:
            await conn.close()
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in transactions endpoint: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/portfolio/trades/yearly-csv")
async def export_yearly_trades(request: Request, key: str = "", year: int = 2026):
    """
    Export yearly trades as CSV.
    Dashboard calls: /api/portfolio/trades/yearly-csv?key=...&year=2026
    """
    api_key = request.headers.get("X-API-Key") or key
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        user = await conn.fetchrow(
            "SELECT id, email, fee_tier FROM follower_users WHERE api_key = $1",
            api_key
        )
        
        if not user:
            await conn.close()
            raise HTTPException(status_code=404, detail="User not found")
        
        start_date = datetime(year, 1, 1)
        end_date = datetime(year + 1, 1, 1)
        
        trades = await conn.fetch("""
            SELECT closed_at, symbol, side, entry_price, exit_price,
                   position_size, leverage, profit_usd, profit_percent, notes
            FROM trades
            WHERE user_id = $1 AND closed_at >= $2 AND closed_at < $3
            ORDER BY closed_at ASC
        """, user['id'], start_date, end_date)
        
        await conn.close()
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        writer.writerow([f"Trade History - {year} (Hyperliquid)"])
        writer.writerow([f"User: {user['email']}"])
        writer.writerow([f"Fee Tier: {user['fee_tier'] or 'standard'}"])
        writer.writerow([])
        writer.writerow(['Date (UTC)', 'Symbol', 'Side', 'Entry Price', 'Exit Price',
                         'Position Size', 'Leverage', 'P&L ($)', 'P&L (%)', 'Notes'])
        
        total_pnl = 0
        winning_trades = 0
        losing_trades = 0
        monthly_pnl = {}
        
        for trade in trades:
            pnl = float(trade['profit_usd'] or 0)
            total_pnl += pnl
            if pnl >= 0:
                winning_trades += 1
            else:
                losing_trades += 1
            
            if trade['closed_at']:
                month_key = trade['closed_at'].strftime('%Y-%m')
                monthly_pnl[month_key] = monthly_pnl.get(month_key, 0) + pnl
            
            writer.writerow([
                trade['closed_at'].strftime('%Y-%m-%d %H:%M:%S') if trade['closed_at'] else '',
                trade['symbol'] or '',
                trade['side'] or '',
                f"{float(trade['entry_price'] or 0):.6f}",
                f"{float(trade['exit_price'] or 0):.6f}",
                f"{float(trade['position_size'] or 0):.4f}",
                f"{int(trade['leverage'] or 1)}x",
                f"${pnl:+.2f}",
                f"{float(trade['profit_percent'] or 0):+.2f}%",
                trade['notes'] or ''
            ])
        
        writer.writerow([])
        writer.writerow(['YEARLY SUMMARY'])
        writer.writerow(['Total Trades', len(trades)])
        writer.writerow(['Winning', winning_trades])
        writer.writerow(['Losing', losing_trades])
        writer.writerow(['Win Rate', f"{(winning_trades/len(trades)*100):.1f}%" if trades else "N/A"])
        writer.writerow(['NET P&L', f"${total_pnl:+.2f}"])
        
        if monthly_pnl:
            writer.writerow([])
            writer.writerow(['MONTHLY BREAKDOWN'])
            writer.writerow(['Month', 'P&L ($)'])
            for month, pnl in sorted(monthly_pnl.items()):
                writer.writerow([month, f"${pnl:+.2f}"])
        
        fee_rates = {'team': 0.0, 'vip': 0.05, 'standard': 0.10}
        fee_rate = fee_rates.get(user['fee_tier'] or 'standard', 0.10)
        fee_due = max(0, total_pnl * fee_rate) if total_pnl > 0 else 0
        writer.writerow([])
        writer.writerow(['Fee Rate', f"{int(fee_rate * 100)}%"])
        writer.writerow(['Total Fee Due', f"${fee_due:.2f}"])
        
        output.seek(0)
        filename = f"hl_trades_{year}_{user['email'].split('@')[0]}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error exporting yearly trades: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/portfolio/deposit")
async def record_deposit(request: Request):
    """
    Manually record a deposit to adjust portfolio tracking.
    Body: { key: "...", amount: 100.0, notes: "optional" }
    """
    body = await request.json()
    api_key = body.get("key") or request.headers.get("X-API-Key")
    amount = body.get("amount")
    notes = body.get("notes", "Manual deposit")
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    if not amount or float(amount) <= 0:
        raise HTTPException(status_code=400, detail="Positive amount required")
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        try:
            user = await conn.fetchrow(
                "SELECT id FROM follower_users WHERE api_key = $1",
                api_key
            )
            
            if not user:
                await conn.close()
                raise HTTPException(status_code=404, detail="User not found")
            
            await conn.execute("""
                INSERT INTO portfolio_transactions 
                    (follower_user_id, user_id, transaction_type, amount, detection_method, detected_at, notes)
                VALUES ($1, $2, 'deposit', $3, 'manual', NOW(), $4)
            """, user['id'], api_key, float(amount), notes)
            
            return {
                "status": "success",
                "message": f"Deposit of ${float(amount):.2f} recorded",
                "transaction_type": "deposit",
                "amount": float(amount)
            }
        
        finally:
            await conn.close()
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error recording deposit: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/portfolio/withdraw")
async def record_withdrawal(request: Request):
    """
    Manually record a withdrawal to adjust portfolio tracking.
    Body: { key: "...", amount: 50.0, notes: "optional" }
    """
    body = await request.json()
    api_key = body.get("key") or request.headers.get("X-API-Key")
    amount = body.get("amount")
    notes = body.get("notes", "Manual withdrawal")
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    if not amount or float(amount) <= 0:
        raise HTTPException(status_code=400, detail="Positive amount required")
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        try:
            user = await conn.fetchrow(
                "SELECT id FROM follower_users WHERE api_key = $1",
                api_key
            )
            
            if not user:
                await conn.close()
                raise HTTPException(status_code=404, detail="User not found")
            
            await conn.execute("""
                INSERT INTO portfolio_transactions 
                    (follower_user_id, user_id, transaction_type, amount, detection_method, detected_at, notes)
                VALUES ($1, $2, 'withdrawal', $3, 'manual', NOW(), $4)
            """, user['id'], api_key, float(amount), notes)
            
            return {
                "status": "success",
                "message": f"Withdrawal of ${float(amount):.2f} recorded",
                "transaction_type": "withdrawal",
                "amount": float(amount)
            }
        
        finally:
            await conn.close()
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error recording withdrawal: {e}")
        raise HTTPException(status_code=500, detail=str(e))
