"""
Nike Rocket Follower System - API Endpoints
============================================

FastAPI endpoints for managing followers, signals, and payments.

Endpoints:
- POST /api/broadcast-signal - Receive signals from master algo
- GET /api/latest-signal - Followers poll for new signals
- POST /api/report-pnl - Followers report trade results
- POST /api/users/register - New user signup
- GET /api/users/verify - Verify user access
- GET /api/users/stats - Get user statistics
- POST /api/setup-agent - Setup hosted trading agent (NEW!)
- GET /api/agent-status - Get agent status (NEW!)
- POST /api/stop-agent - Stop trading agent (NEW!)
- POST /api/start-agent - Start/resume trading agent (NEW!)
- POST /api/payments/create - Create payment link
- POST /api/payments/webhook - Coinbase Commerce webhook

Author: Nike Rocket Team
Updated: November 24, 2025
"""

from fastapi import APIRouter, HTTPException, Header, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict
import os
import secrets
import hashlib
import hmac
import json
import logging
from pydantic import BaseModel, EmailStr
from hyperliquid.info import Info
from hyperliquid.utils import constants
from eth_account import Account

from follower_models import (
    User, Signal, SignalDelivery, Trade, Payment, SystemStats,
    get_db_session
)

# Import email service
from email_service import send_welcome_email, send_api_key_resend_email

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()

# Environment variables
MASTER_API_KEY = os.getenv("MASTER_API_KEY", "your-master-key-here")
COINBASE_WEBHOOK_SECRET = os.getenv("COINBASE_WEBHOOK_SECRET", "")
COINBASE_API_KEY = os.getenv("COINBASE_COMMERCE_API_KEY", "")

# Signal expiration settings
SIGNAL_EXPIRATION_MINUTES = 15  # Signals expire after 15 minutes


# ═══════════════════════════════════════════════════════════════════════════
# HYPERLIQUID WALLET VERIFICATION (Anti-Abuse)
# ═══════════════════════════════════════════════════════════════════════════

USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"

async def verify_hl_credentials(private_key: str, wallet_address: str) -> tuple[str, Optional[str]]:
    """
    Verify Hyperliquid credentials by checking wallet access.
    
    For Hyperliquid, the wallet address IS the unique account identifier.
    No fingerprinting needed - wallet address is deterministic from private key.
    
    Args:
        private_key: Hyperliquid private key (hex)
        wallet_address: Hyperliquid wallet address (0x...)
    Returns:
        Tuple of (verified_wallet_address, error_message)
    """
    try:
        # Step 1: Derive wallet from private key
        try:
            account = Account.from_key(private_key)
            derived_address = account.address
        except Exception as e:
            return (None, f"Invalid private key format: {str(e)}")
        
        # Step 2: Verify wallet address matches
        if wallet_address and wallet_address.lower() != derived_address.lower():
            return (None, f"Wallet address mismatch. Private key derives to {derived_address[:10]}... but you provided {wallet_address[:10]}...")
        
        verified_address = derived_address
        
        # Step 3: Verify account exists on Hyperliquid
        try:
            base_url = constants.TESTNET_API_URL if USE_TESTNET else constants.MAINNET_API_URL
            info = Info(base_url, skip_ws=True)
            state = info.user_state(verified_address)
            if state and "marginSummary" in state:
                account_value = float(state["marginSummary"].get("accountValue", 0))
                logger.info(f"✅ HL wallet verified: {verified_address[:10]}... (value: ${account_value:.2f})")
            else:
                logger.info(f"✅ HL wallet verified: {verified_address[:10]}... (new/empty account)")
        except Exception as e:
            logger.warning(f"⚠️ Could not verify wallet state (may be new account): {e}")
        
        return (verified_address, None)
    except Exception as e:
        logger.error(f"Error verifying HL credentials: {e}")
        return (None, f"Failed to verify Hyperliquid credentials: {str(e)}")


async def check_wallet_abuse(wallet_address: str, current_user_id: int, db: Session) -> tuple[bool, Optional[str]]:
    """
    Check if this wallet address has unpaid invoices or is blocked.
    """
    existing_user = db.query(User).filter(
        User.hl_wallet_address == wallet_address,
        User.id != current_user_id
    ).first()
    
    if existing_user:
        from sqlalchemy import text
        result = db.execute(text("""
            SELECT COUNT(*) as unpaid_count, 
                   COALESCE(SUM(amount_usd), 0) as total_owed
            FROM billing_invoices 
            WHERE user_id = :user_id 
            AND status IN ('pending', 'overdue')
        """), {"user_id": existing_user.id})
        row = result.fetchone()
        unpaid_count = row[0] if row else 0
        total_owed = row[1] if row else 0
        if unpaid_count > 0:
            return (True, f"This wallet has ${total_owed:.2f} in unpaid invoices from a previous account ({existing_user.email}).")
        if existing_user.suspension_reason and "unpaid" in existing_user.suspension_reason.lower():
            return (True, "This wallet was previously suspended for non-payment. Please contact support.")
    
    return (False, None)

class SignalBroadcast(BaseModel):
    """Signal from master algorithm"""
    action: str  # BUY or SELL
    symbol: str  # ADA/USDT
    entry_price: float
    stop_loss: float
    take_profit: float
    leverage: float
    risk_pct: Optional[float] = 0.02  # Risk % (0.02=2% aggressive, 0.03=3% conservative)
    timeframe: Optional[str] = None
    trend_strength: Optional[float] = None
    volatility: Optional[float] = None
    notes: Optional[str] = None


class TradeReport(BaseModel):
    """Trade result from follower agent"""
    trade_id: str
    signal_id: Optional[str] = None
    hl_order_id: Optional[str] = None
    
    opened_at: str  # ISO datetime
    closed_at: str  # ISO datetime
    
    symbol: str
    side: str  # BUY or SELL
    
    entry_price: float
    exit_price: float
    position_size: float
    leverage: float
    
    profit_usd: float
    profit_percent: Optional[float] = None
    notes: Optional[str] = None


class UserRegistration(BaseModel):
    """New user signup"""
    email: EmailStr
    wallet_address: Optional[str] = None


class PaymentCreate(BaseModel):
    """Create payment charge"""
    amount: float
    for_month: str  # "2025-11"


class ExecutionConfirmation(BaseModel):
    """Confirm signal execution"""
    delivery_id: int


class ExecutionConfirmRequest(BaseModel):
    """Confirm signal execution with details (BUG #5 FIX)"""
    delivery_id: int
    signal_id: Optional[str] = None
    executed_at: Optional[str] = None
    execution_price: Optional[float] = None


class RetryFailedSignalRequest(BaseModel):
    """Request to retry a failed signal (ISSUE #2)"""
    failed_signal_id: int


class SetupAgentRequest(BaseModel):
    """Request to setup trading agent with Hyperliquid credentials"""
    hl_private_key: str
    hl_wallet_address: str


# ==================== DEPENDENCY INJECTION ====================

def get_db():
    """Database session dependency"""
    from sqlalchemy import create_engine
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise Exception("DATABASE_URL not set")
    
    # Handle Railway postgres:// to postgresql://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    engine = create_engine(DATABASE_URL)
    session = get_db_session(engine)
    try:
        yield session
    finally:
        session.close()


def verify_master_key(x_master_key: str = Header(None)):
    """Verify master API key from broadcasting algo"""
    if x_master_key != MASTER_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid master API key")
    return True


def verify_user_key(x_api_key: str = Header(None), db: Session = Depends(get_db)):
    """Verify user API key and return user"""
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=404, detail="Invalid API key")
    
    return user


# ==================== SIGNAL ENDPOINTS ====================

@router.post("/api/broadcast-signal")
async def broadcast_signal(
    signal: SignalBroadcast,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _: bool = Depends(verify_master_key)
):
    """
    Receive signal from master algorithm
    
    Called by: Your algo when it opens a position
    Auth: Requires MASTER_API_KEY
    """
    try:
        # Generate signal ID
        signal_id = secrets.token_urlsafe(16)
        
        # Store signal in database
        db_signal = Signal(
            signal_id=signal_id,
            action=signal.action,
            symbol=signal.symbol,
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            leverage=signal.leverage,
            risk_pct=signal.risk_pct,
            timeframe=signal.timeframe,
            trend_strength=signal.trend_strength,
            volatility=signal.volatility,
            notes=signal.notes
        )
        db.add(db_signal)
        db.commit()
        db.refresh(db_signal)
        
        # Get all active users
        active_users = db.query(User).filter(
            User.access_granted == True
        ).all()
        
        # Create delivery records
        for user in active_users:
            delivery = SignalDelivery(
                signal_id=db_signal.id,
                user_id=user.id
            )
            db.add(delivery)
        
        db.commit()
        
        logger.info(f"📡 Signal broadcast: {signal.action} on {signal.symbol}")
        logger.info(f"   Delivered to {len(active_users)} active followers")
        logger.info(f"   ⏰ Expires in {SIGNAL_EXPIRATION_MINUTES} minutes")
        
        return {
            "status": "success",
            "signal_id": signal_id,
            "delivered_to": len(active_users),
            "expires_in_minutes": SIGNAL_EXPIRATION_MINUTES,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Error broadcasting signal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/latest-signal")
async def get_latest_signal(
    user: User = Depends(verify_user_key),
    db: Session = Depends(get_db)
):
    """
    Get latest signal for follower
    
    Called by: Follower agents every 10 seconds
    Auth: Requires user API key
    Returns: Latest unacknowledged signal, or null
    
    FIXES APPLIED (Nov 27, 2025):
    - BUG #3: Now uses timezone-aware datetime comparison
    - BUG #2: Now returns risk_pct in signal response
    """
    try:
        # Check if user has access
        if not user.access_granted:
            # Use pending_invoice_amount (30-day billing) instead of legacy monthly_fee_due
            amount_due = getattr(user, 'pending_invoice_amount', 0) or 0
            return {
                "access_granted": False,
                "reason": user.suspension_reason or "Payment required",
                "amount_due": amount_due
            }
        
        # Get latest unacknowledged signal for this user
        # ISSUE #1 FIX: Also exclude failed signals
        delivery = db.query(SignalDelivery).join(Signal).filter(
            SignalDelivery.user_id == user.id,
            SignalDelivery.acknowledged == False,
            SignalDelivery.failed == False  # Don't return failed signals
        ).order_by(Signal.created_at.desc()).first()
        
        if not delivery:
            return {
                "access_granted": True,
                "signal": None,
                "message": "No new signals"
            }
        
        # BUG #3 FIX: Use timezone-aware datetime comparison
        now_utc = datetime.now(timezone.utc)
        signal_created = delivery.signal.created_at
        
        # Make signal_created timezone-aware if it isn't
        if signal_created.tzinfo is None:
            signal_created = signal_created.replace(tzinfo=timezone.utc)
        
        signal_age_seconds = (now_utc - signal_created).total_seconds()
        signal_age_minutes = signal_age_seconds / 60
        
        if signal_age_minutes > SIGNAL_EXPIRATION_MINUTES:
            # Signal is too old - mark as acknowledged to prevent execution
            delivery.acknowledged = True
            db.commit()
            
            logger.info(f"⚠️ Signal expired and skipped:")
            logger.info(f"   Signal ID: {delivery.signal.signal_id}")
            logger.info(f"   Symbol: {delivery.signal.symbol}")
            logger.info(f"   Age: {signal_age_minutes:.1f} minutes")
            
            return {
                "access_granted": True,
                "signal": None,
                "message": f"Signal expired ({signal_age_minutes:.0f} min old)"
            }
        
        # Return signal for execution (BUG #2 FIX: include risk_pct)
        return {
            "access_granted": True,
            "signal": {
                "signal_id": delivery.signal.signal_id,
                "delivery_id": delivery.id,
                "action": delivery.signal.action,
                "symbol": delivery.signal.symbol,
                "entry_price": delivery.signal.entry_price,
                "stop_loss": delivery.signal.stop_loss,
                "take_profit": delivery.signal.take_profit,
                "leverage": delivery.signal.leverage,
                "risk_pct": getattr(delivery.signal, 'risk_pct', 0.02),  # Include risk percentage!
                "timeframe": delivery.signal.timeframe,
                "trend_strength": delivery.signal.trend_strength,
                "volatility": delivery.signal.volatility,
                "notes": delivery.signal.notes,
                "created_at": delivery.signal.created_at.isoformat(),
                "age_seconds": int(signal_age_seconds)
            }
        }
    
    except Exception as e:
        logger.error(f"❌ Error fetching signal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/acknowledge-signal")
async def acknowledge_signal(
    data: ExecutionConfirmation,
    user: User = Depends(verify_user_key),
    db: Session = Depends(get_db)
):
    """
    Acknowledge signal receipt/execution
    
    Called by: Follower agent after executing signal
    Auth: Requires user API key
    """
    try:
        # Find delivery
        delivery = db.query(SignalDelivery).filter(
            SignalDelivery.id == data.delivery_id,
            SignalDelivery.user_id == user.id
        ).first()
        
        if not delivery:
            raise HTTPException(status_code=404, detail="Delivery not found")
        
        # Mark as acknowledged
        delivery.acknowledged = True
        delivery.acknowledged_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"✓ Signal acknowledged by {user.email}")
        
        return {
            "status": "success",
            "message": "Signal acknowledged"
        }
    
    except Exception as e:
        logger.error(f"❌ Error acknowledging signal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== BUG #5 FIX: Missing /api/confirm-execution endpoint ====================

@router.post("/api/confirm-execution")
async def confirm_execution(
    data: ExecutionConfirmRequest,
    x_api_key: str = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Confirm successful signal execution (two-phase acknowledgment)
    
    BUG #5 FIX: This endpoint was missing, causing 404 errors and potential duplicate trades.
    
    Called by: Follower agent after successfully executing trade
    Auth: Requires user API key in header
    
    Purpose:
    - Marks signal as executed (prevents re-delivery)
    - Records execution timestamp and price
    - Idempotent - safe to call multiple times
    """
    try:
        # Verify API key
        if not x_api_key:
            raise HTTPException(status_code=401, detail="API key required in X-API-Key header")
        
        user = db.query(User).filter(User.api_key == x_api_key).first()
        if not user:
            raise HTTPException(status_code=404, detail="Invalid API key")
        
        # Find delivery
        delivery = db.query(SignalDelivery).filter(
            SignalDelivery.id == data.delivery_id,
            SignalDelivery.user_id == user.id
        ).first()
        
        if not delivery:
            raise HTTPException(status_code=404, detail="Delivery not found")
        
        # Check if already confirmed (idempotent)
        if getattr(delivery, 'executed', False):
            logger.info(f"✓ Signal {data.delivery_id} already confirmed for {user.email}")
            return {
                "status": "already_confirmed",
                "message": "Signal was already confirmed",
                "delivery_id": data.delivery_id
            }
        
        # Parse execution timestamp
        executed_at = datetime.utcnow()
        if data.executed_at:
            try:
                executed_at = datetime.fromisoformat(data.executed_at.replace('Z', '+00:00'))
            except:
                pass  # Use default
        
        # Mark as acknowledged AND executed
        delivery.acknowledged = True
        delivery.acknowledged_at = executed_at
        
        # Set executed if column exists
        if hasattr(delivery, 'executed'):
            delivery.executed = True
        if hasattr(delivery, 'executed_at'):
            delivery.executed_at = executed_at
        if hasattr(delivery, 'execution_price') and data.execution_price:
            delivery.execution_price = data.execution_price
        
        db.commit()
        
        logger.info(f"✅ Signal execution confirmed:")
        logger.info(f"   User: {user.email}")
        logger.info(f"   Delivery ID: {data.delivery_id}")
        logger.info(f"   Signal ID: {data.signal_id}")
        
        return {
            "status": "confirmed",
            "message": "Signal execution confirmed",
            "delivery_id": data.delivery_id,
            "executed_at": executed_at.isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error confirming execution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ISSUE #2 FIX: Failed Signals Retry Queue ====================

@router.post("/api/mark-signal-failed")
async def mark_signal_failed(
    delivery_id: int,
    failure_reason: str,
    x_api_key: str = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Mark a signal delivery as failed for later retry (ISSUE #2)
    
    Called by: Follower agent when all retry attempts fail
    Purpose: Allows admin to review and retry failed signals
    """
    try:
        if not x_api_key:
            raise HTTPException(status_code=401, detail="API key required")
        
        user = db.query(User).filter(User.api_key == x_api_key).first()
        if not user:
            raise HTTPException(status_code=404, detail="Invalid API key")
        
        delivery = db.query(SignalDelivery).filter(
            SignalDelivery.id == delivery_id,
            SignalDelivery.user_id == user.id
        ).first()
        
        if not delivery:
            raise HTTPException(status_code=404, detail="Delivery not found")
        
        # Mark as failed
        delivery.failed = True
        delivery.failure_reason = failure_reason
        if hasattr(delivery, 'retry_count'):
            delivery.retry_count = (delivery.retry_count or 0) + 1
        
        db.commit()
        
        logger.warning(f"⚠️ Signal marked as failed:")
        logger.warning(f"   User: {user.email}")
        logger.warning(f"   Delivery ID: {delivery_id}")
        logger.warning(f"   Reason: {failure_reason}")
        
        return {
            "status": "marked_failed",
            "delivery_id": delivery_id,
            "retry_count": getattr(delivery, 'retry_count', 1)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error marking signal failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/failed-signals")
async def get_failed_signals(
    x_api_key: str = Header(None, alias="X-API-Key"),
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """
    Get list of failed signals for a user (ISSUE #2)
    
    Called by: Dashboard or admin panel
    Purpose: Review and retry failed trades
    """
    try:
        if not x_api_key:
            raise HTTPException(status_code=401, detail="API key required")
        
        user = db.query(User).filter(User.api_key == x_api_key).first()
        if not user:
            raise HTTPException(status_code=404, detail="Invalid API key")
        
        failed_deliveries = db.query(SignalDelivery).join(Signal).filter(
            SignalDelivery.user_id == user.id,
            SignalDelivery.failed == True
        ).order_by(Signal.created_at.desc()).limit(limit).all()
        
        signals = []
        for delivery in failed_deliveries:
            signals.append({
                "delivery_id": delivery.id,
                "signal_id": delivery.signal.signal_id,
                "action": delivery.signal.action,
                "symbol": delivery.signal.symbol,
                "entry_price": delivery.signal.entry_price,
                "failure_reason": getattr(delivery, 'failure_reason', None),
                "retry_count": getattr(delivery, 'retry_count', 0),
                "created_at": delivery.signal.created_at.isoformat()
            })
        
        return {
            "status": "success",
            "failed_signals": signals,
            "count": len(signals)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting failed signals: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/retry-failed-signal")
async def retry_failed_signal(
    data: RetryFailedSignalRequest,
    x_api_key: str = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Reset a failed signal for retry (ISSUE #2)
    
    Called by: Admin or user dashboard
    Purpose: Allow retry of failed trades
    """
    try:
        if not x_api_key:
            raise HTTPException(status_code=401, detail="API key required")
        
        user = db.query(User).filter(User.api_key == x_api_key).first()
        if not user:
            raise HTTPException(status_code=404, detail="Invalid API key")
        
        delivery = db.query(SignalDelivery).filter(
            SignalDelivery.id == data.failed_signal_id,
            SignalDelivery.user_id == user.id,
            SignalDelivery.failed == True
        ).first()
        
        if not delivery:
            raise HTTPException(status_code=404, detail="Failed signal not found")
        
        # Reset for retry
        delivery.failed = False
        delivery.failure_reason = None
        delivery.acknowledged = False
        if hasattr(delivery, 'executed'):
            delivery.executed = False
        
        db.commit()
        
        logger.info(f"🔄 Signal reset for retry:")
        logger.info(f"   User: {user.email}")
        logger.info(f"   Delivery ID: {data.failed_signal_id}")
        
        return {
            "status": "reset_for_retry",
            "delivery_id": data.failed_signal_id,
            "message": "Signal will be delivered on next poll"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error retrying signal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== TRADE REPORTING ====================

@router.post("/api/report-pnl")
async def report_pnl(
    trade: TradeReport,
    user: User = Depends(verify_user_key),
    db: Session = Depends(get_db)
):
    """
    Report trade result from follower
    
    Called by: Follower agent after closing trade
    Auth: Requires user API key
    
    DEPRECATED: This endpoint uses legacy per-trade fee logic.
    The position_monitor now handles P&L tracking with 30-day billing.
    Kept for backwards compatibility but fees are NO LONGER charged here.
    """
    try:
        # Parse timestamps
        opened_at = datetime.fromisoformat(trade.opened_at.replace('Z', '+00:00'))
        closed_at = datetime.fromisoformat(trade.closed_at.replace('Z', '+00:00'))
        
        # 30-DAY BILLING: No per-trade fees - handled by billing_service_30day.py
        # This prevents double-billing when position_monitor also records the trade
        fee_charged = 0.0  # ALWAYS 0 - fees calculated at cycle end
        
        # Store trade record
        db_trade = Trade(
            user_id=user.id,
            trade_id=trade.trade_id,
            hl_order_id=trade.hl_order_id,
            opened_at=opened_at,
            closed_at=closed_at,
            symbol=trade.symbol,
            side=trade.side,
            entry_price=trade.entry_price,
            exit_price=trade.exit_price,
            position_size=trade.position_size,
            leverage=trade.leverage,
            profit_usd=trade.profit_usd,
            profit_percent=trade.profit_percent,
            fee_charged=fee_charged,  # Always 0 for 30-day billing
            notes=trade.notes
        )
        db.add(db_trade)
        
        # Update user stats - accumulate profit for 30-day billing
        # Uses current_cycle_profit instead of legacy monthly_profit
        user.current_cycle_profit = (user.current_cycle_profit or 0) + trade.profit_usd
        user.current_cycle_trades = (user.current_cycle_trades or 0) + 1
        user.total_profit += trade.profit_usd
        user.total_trades += 1
        
        # 30-DAY BILLING: No per-trade fees - calculated at cycle end
        
        db.commit()
        
        logger.info(f"💰 Trade reported by {user.email}:")
        logger.info(f"   Symbol: {trade.symbol}")
        logger.info(f"   Profit: ${trade.profit_usd:.2f}")
        logger.info(f"   Fee: $0 (30-day billing - fees at cycle end)")
        
        return {
            "status": "success",
            "trade_id": db_trade.id,
            "profit_usd": trade.profit_usd,
            "fee_charged": 0,  # Always 0 - 30-day billing
            "billing_note": "Fees calculated at end of 30-day cycle",
            "current_cycle_profit": user.current_cycle_profit
        }
    
    except Exception as e:
        logger.error(f"❌ Error reporting trade: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== USER MANAGEMENT ====================

@router.post("/api/users/register")
async def register_user(
    data: UserRegistration,
    db: Session = Depends(get_db)
):
    """
    Register new user (EMAIL-ONLY FLOW for security)
    
    Called by: Signup form
    Public: No auth required
    
    Flow:
    - New user: Create account, send welcome email with API key
    - Existing user: Resend API key via email
    - NEVER returns API key in response (email only!)
    """
    try:
        # Check if email already exists
        existing = db.query(User).filter(User.email == data.email).first()
        
        if existing:
            # EXISTING USER - Resend API key via email
            logger.info(f"🔄 Existing user requesting API key: {data.email}")
            
            # Send API key via email
            email_sent = send_api_key_resend_email(existing.email, existing.api_key)
            
            if email_sent:
                return {
                    "status": "success",
                    "message": "API key sent to your email",
                    "email": existing.email
                }
            else:
                # Email failed but don't expose this to user
                logger.error(f"❌ Failed to send email to {existing.email}")
                return {
                    "status": "success",
                    "message": "API key sent to your email",
                    "email": existing.email
                }
        
        # NEW USER - Create account
        api_key = f"nk_{secrets.token_urlsafe(32)}"
        
        user = User(
            email=data.email,
            api_key=api_key,
            hl_wallet_address=data.wallet_address if data.wallet_address else None,
            access_granted=True  # Grant access immediately (30-day billing starts on first trade)
        )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        logger.info(f"✅ New user registered: {data.email}")
        
        # Send welcome email with API key
        email_sent = send_welcome_email(user.email, user.api_key)
        
        if not email_sent:
            logger.error(f"⚠️ Email failed for {user.email}, but user created")
        
        # SECURITY: Never return API key in response!
        return {
            "status": "success",
            "message": "Account created! Check your email for API key.",
            "email": user.email
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error registering user: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/users/verify")
async def verify_user(
    user: User = Depends(verify_user_key),
    db: Session = Depends(get_db)
):
    """
    Verify user access status
    
    Called by: Follower agent on startup
    Auth: Requires user API key
    """
    # Check payment status
    payment_ok = user.check_payment_status()
    
    if not payment_ok and user.access_granted:
        # Suspend user for non-payment
        user.access_granted = False
        user.suspended_at = datetime.utcnow()
        user.suspension_reason = "Monthly fee overdue"
        db.commit()
        logger.warning(f"⚠️ User suspended for non-payment: {user.email}")
    
    return {
        "access_granted": user.access_granted,
        "email": user.email,
        # 30-day billing fields
        "current_cycle_profit": user.current_cycle_profit or 0,
        "current_cycle_trades": user.current_cycle_trades or 0,
        "pending_invoice_amount": user.pending_invoice_amount or 0,
        "invoice_due_date": user.invoice_due_date.isoformat() if user.invoice_due_date else None,
        "suspension_reason": user.suspension_reason if not user.access_granted else None
    }


@router.get("/api/users/stats")
async def get_user_stats(
    user: User = Depends(verify_user_key),
    db: Session = Depends(get_db)
):
    """
    Get user statistics
    
    Called by: User dashboard or follower agent
    Auth: Requires user API key
    """
    # Get recent trades
    recent_trades = db.query(Trade).filter(
        Trade.user_id == user.id
    ).order_by(Trade.closed_at.desc()).limit(10).all()
    
    return {
        "email": user.email,
        "access_granted": user.access_granted,
        
        # 30-day billing cycle stats
        "billing_cycle_start": user.billing_cycle_start.isoformat() if user.billing_cycle_start else None,
        "current_cycle_profit": user.current_cycle_profit or 0,
        "current_cycle_trades": user.current_cycle_trades or 0,
        "pending_invoice_amount": user.pending_invoice_amount or 0,
        "invoice_due_date": user.invoice_due_date.isoformat() if user.invoice_due_date else None,
        
        # All-time stats
        "total_profit": user.total_profit,
        "total_trades": user.total_trades,
        "total_fees_paid": user.total_fees_paid,
        
        # Recent trades
        "recent_trades": [
            {
                "trade_id": trade.trade_id,
                "symbol": trade.symbol,
                "profit": trade.profit_usd,
                "closed_at": trade.closed_at.isoformat()
            }
            for trade in recent_trades
        ]
    }


# ==================== HOSTED AGENT SETUP (NEW!) ====================

@router.post("/api/setup-agent")
async def setup_agent(
    data: SetupAgentRequest,
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Setup customer's trading agent with Hyperliquid credentials
    
    Called by: /setup page after customer enters credentials
    Auth: Requires user API key
    
    This endpoint:
    1. Validates Hyperliquid credentials
    2. Fetches and stores Hyperliquid account UID (anti-abuse measure)
    3. Checks if this Hyperliquid account has unpaid invoices from previous accounts
    4. Encrypts and stores Hyperliquid credentials
    5. Marks user as ready for agent activation
    6. Multi-agent manager will pick them up automatically
    
    ANTI-ABUSE: Users cannot create new accounts to avoid paying invoices.
    The Hyperliquid account UID is tied to their KYC-verified identity.
    """
    
    # Find user
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Validate Hyperliquid credentials format
    if not data.hl_private_key or not data.hl_wallet_address:
        raise HTTPException(status_code=400, detail="Both private key and wallet address required")
    
    if len(data.hl_private_key) < 10:
        raise HTTPException(status_code=400, detail="Invalid private key format")
    
    if len(data.hl_wallet_address) < 20:
        raise HTTPException(status_code=400, detail="Invalid wallet address format")
    
    try:
        # ═══════════════════════════════════════════════════════════════
        # STEP 1: Validate credentials and verify wallet
        # ═══════════════════════════════════════════════════════════════
        logger.info(f"🔐 Validating HL credentials for: {user.email}")
        
        wallet_address_verified, error = await verify_hl_credentials(
            data.hl_private_key, 
            data.hl_wallet_address
        )
        
        if error:
            logger.warning(f"❌ Credential validation failed for {user.email}: {error}")
            raise HTTPException(status_code=400, detail=error)
        
        logger.info(f"✅ HL credentials valid. Wallet: {wallet_address_verified[:20]}...")
        
        # ═══════════════════════════════════════════════════════════════
        # STEP 2: Check for abuse (unpaid invoices from previous accounts)
        # ═══════════════════════════════════════════════════════════════
        is_blocked, block_reason = await check_wallet_abuse(
            wallet_address_verified, 
            user.id, 
            db
        )
        
        if is_blocked:
            logger.warning(f"🚫 Setup blocked for {user.email}: {block_reason}")
            raise HTTPException(
                status_code=403, 
                detail=block_reason
            )
        
        # ═══════════════════════════════════════════════════════════════
        # STEP 3: Store credentials and Hyperliquid account UID
        # ═══════════════════════════════════════════════════════════════
        
        # Encrypt and store credentials
        user.set_hl_credentials(data.hl_private_key, data.hl_wallet_address)
        
        # Store Hyperliquid account UID (for future abuse checks)
        user.hl_wallet_address = wallet_address_verified
        
        # Mark as ready
        user.credentials_set = True
        
        # Ensure access is granted
        if not user.access_granted:
            user.access_granted = True
            user.suspended_at = None
            user.suspension_reason = None
        
        db.commit()
        
        logger.info(f"✅ Credentials set for user: {user.email}")
        logger.info(f"   HL Wallet Address: {wallet_address_verified[:20]}...")
        logger.info(f"   Agent will start automatically within 5 minutes")
        
        return {
            "status": "success",
            "message": "Trading agent configured successfully",
            "agent_status": "starting",
            "note": "Your agent will start automatically within 5 minutes",
            "email": user.email
        }
        
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error setting up agent: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to setup agent: {str(e)}")


@router.get("/api/agent-status")
async def get_agent_status(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Get customer's agent status
    
    Called by: Dashboard to show if agent is running
    Auth: Requires user API key
    """
    
    # Find user
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Check if credentials are set
    if not user.credentials_set:
        return {
            "agent_configured": False,
            "agent_active": False,
            "message": "Agent not configured. Please set up your Hyperliquid credentials.",
            "setup_url": "/setup"
        }
    
    # Check if agent is active
    return {
        "agent_configured": True,
        "agent_active": user.agent_active,
        "agent_started_at": user.agent_started_at.isoformat() if user.agent_started_at else None,
        "agent_last_poll": user.agent_last_poll.isoformat() if user.agent_last_poll else None,
        "access_granted": user.access_granted,
        "email": user.email
    }


@router.post("/api/stop-agent")
async def stop_agent(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Stop customer's trading agent
    
    Called by: User dashboard when they want to pause trading
    Auth: Requires user API key
    
    NOTE: This only pauses the agent - credentials are preserved
    so users can easily restart without re-entering API keys.
    """
    
    # Find user
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Mark agent as inactive (but keep credentials!)
    user.agent_active = False
    
    # DON'T clear credentials - user can restart easily
    # If user wants full reset, they can go through setup again
    
    db.commit()
    
    logger.info(f"⏸️ Agent paused for user: {user.email}")
    
    return {
        "status": "success",
        "message": "Trading agent paused",
        "agent_active": False,
        "agent_configured": user.credentials_set  # Still configured!
    }


@router.post("/api/start-agent")
async def start_agent(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Start/resume customer's trading agent
    
    Called by: User dashboard when they want to start/resume trading
    Auth: Requires user API key
    
    NOTE: Agent must already be configured with credentials.
    If not configured, directs user to /setup page.
    """
    
    # Find user
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Check if credentials are set
    if not user.credentials_set:
        return {
            "status": "error",
            "message": "Agent not configured. Please set up your Hyperliquid credentials first.",
            "redirect": f"/setup?key={x_api_key}"
        }
    
    # Activate agent
    user.agent_active = True
    user.agent_started_at = datetime.utcnow()
    
    db.commit()
    
    logger.info(f"▶️ Agent started for user: {user.email}")
    
    return {
        "status": "success",
        "message": "Trading agent started",
        "agent_active": True,
        "agent_configured": True
    }


# ==================== PAYMENT ENDPOINTS ====================

@router.get("/api/pay/{api_key}")
async def create_payment_page(
    api_key: str,
    db: Session = Depends(get_db)
):
    """
    Generate payment page for user
    
    Called by: User clicking payment link
    Public: No auth required (uses API key in URL)
    
    DEPRECATED: This endpoint is legacy. The 30-day billing system in 
    billing_service_30day.py now automatically creates invoices via Coinbase Commerce.
    Kept for backwards compatibility but should not be used for new integrations.
    """
    # Find user
    user = db.query(User).filter(User.api_key == api_key).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check if payment needed (30-day billing system)
    pending_amount = user.pending_invoice_amount or 0
    if pending_amount <= 0:
        return {
            "message": "No payment due",
            "current_cycle_profit": user.current_cycle_profit or 0,
            "access_granted": user.access_granted,
            "note": "Invoices are automatically created at the end of each 30-day billing cycle"
        }
    
    # If user already has a pending invoice, return that
    if user.pending_invoice_id:
        # Look up the existing invoice
        from billing_service_30day import BillingServiceV2
        return {
            "message": "Invoice already exists",
            "pending_invoice_id": user.pending_invoice_id,
            "amount": pending_amount,
            "due_date": user.invoice_due_date.isoformat() if user.invoice_due_date else None,
            "note": "Pay existing invoice - new invoices created automatically at cycle end"
        }
    
    # Create Coinbase Commerce charge (legacy behavior for backwards compatibility)
    try:
        import requests
        
        response = requests.post(
            "https://api.commerce.coinbase.com/charges",
            json={
                "name": "Nike Rocket - Trading Fee",
                "description": f"Profit sharing fee for {user.email}",
                "pricing_type": "fixed_price",
                "local_price": {
                    "amount": str(pending_amount),
                    "currency": "USD"
                },
                "metadata": {
                    "user_id": user.id,
                    "user_email": user.email,
                    "api_key": api_key,
                    "billing_cycle": "30-day",
                    "profit_amount": user.current_cycle_profit or 0
                }
            },
            headers={
                "X-CC-Api-Key": COINBASE_API_KEY,
                "X-CC-Version": "2018-03-22"
            }
        )
        
        if response.status_code == 201:
            charge = response.json()["data"]
            
            # Store payment record
            payment = Payment(
                user_id=user.id,
                amount_usd=pending_amount,
                currency="USD",
                coinbase_charge_id=charge["id"],
                status="pending",
                for_month=datetime.utcnow().strftime("%Y-%m"),
                profit_amount=user.current_cycle_profit or 0
            )
            db.add(payment)
            db.commit()
            
            return {
                "payment_url": charge["hosted_url"],
                "amount": pending_amount,
                "current_cycle_profit": user.current_cycle_profit or 0
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create payment")
    
    except Exception as e:
        logger.error(f"❌ Error creating payment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/payments/webhook")
async def coinbase_webhook(
    request: dict,
    x_cc_webhook_signature: str = Header(None),
    db: Session = Depends(get_db)
):
    """
    Coinbase Commerce webhook
    
    Called by: Coinbase when payment completes
    Auth: Webhook signature verification
    """
    try:
        # Verify webhook signature
        if COINBASE_WEBHOOK_SECRET:
            payload = json.dumps(request)
            signature = hmac.new(
                COINBASE_WEBHOOK_SECRET.encode(),
                payload.encode(),
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, x_cc_webhook_signature or ""):
                raise HTTPException(status_code=401, detail="Invalid signature")
        
        # Process payment event
        event = request.get("event", {})
        event_type = event.get("type")
        
        if event_type == "charge:confirmed":
            # Payment completed
            charge = event.get("data", {})
            metadata = charge.get("metadata", {})
            
            user_id = metadata.get("user_id")
            if not user_id:
                logger.warning("⚠️ Payment webhook missing user_id")
                return {"status": "ignored"}
            
            # Find user
            user = db.query(User).filter(User.id == int(user_id)).first()
            if not user:
                logger.warning(f"⚠️ User not found: {user_id}")
                return {"status": "user_not_found"}
            
            # Update payment record
            payment = db.query(Payment).filter(
                Payment.coinbase_charge_id == charge["id"]
            ).first()
            
            if payment:
                payment.status = "completed"
                payment.completed_at = datetime.utcnow()
                payment.tx_hash = charge.get("payments", [{}])[0].get("transaction_id")
            
            # Mark user as paid and restore access (30-day billing system)
            paid_amount = user.pending_invoice_amount or 0
            user.pending_invoice_id = None
            user.pending_invoice_amount = 0
            user.invoice_due_date = None
            user.total_fees_paid = (user.total_fees_paid or 0) + paid_amount
            user.access_granted = True
            user.suspended_at = None
            user.suspension_reason = None
            
            db.commit()
            
            logger.info(f"✅ Payment confirmed for {user.email}")
            logger.info(f"   Amount: ${paid_amount:.2f}")
            logger.info(f"   Access restored!")
            
            return {"status": "processed"}
        
        return {"status": "ignored"}
    
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        return {"status": "error", "message": str(e)}


# ==================== ADMIN ENDPOINTS ====================

@router.get("/api/admin/stats")
async def get_system_stats(
    db: Session = Depends(get_db),
    _: bool = Depends(verify_master_key)
):
    """
    Get system-wide statistics
    
    Called by: Admin dashboard
    Auth: Requires MASTER_API_KEY
    """
    from sqlalchemy import func
    
    total_users = db.query(User).count()
    active_users = db.query(User).filter(User.access_granted == True).count()
    suspended_users = db.query(User).filter(User.access_granted == False).count()
    
    total_trades = db.query(Trade).count()
    total_profit = db.query(func.sum(Trade.profit_usd)).scalar() or 0
    total_fees = db.query(func.sum(Trade.fee_charged)).scalar() or 0
    
    total_signals = db.query(Signal).count()
    
    return {
        "users": {
            "total": total_users,
            "active": active_users,
            "suspended": suspended_users
        },
        "trading": {
            "total_signals": total_signals,
            "total_trades": total_trades,
            "total_profit": total_profit,
            "total_fees_collected": total_fees
        },
        "updated_at": datetime.utcnow().isoformat()
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MONITORING ENDPOINTS - For agent heartbeats, errors, and event logging
# ═══════════════════════════════════════════════════════════════════════════════

class HeartbeatRequest(BaseModel):
    """Heartbeat from running agent"""
    api_key: str
    status: Optional[str] = "alive"
    details: Optional[Dict] = None


class ErrorLogRequest(BaseModel):
    """Error report from agent"""
    api_key: str
    error_type: str
    error_message: str
    context: Optional[Dict] = None


class AgentEventRequest(BaseModel):
    """General agent event"""
    api_key: str
    event_type: str
    event_data: Optional[Dict] = None


@router.post("/api/heartbeat")
async def receive_heartbeat(request: HeartbeatRequest):
    """
    Receive heartbeat from running trading agent.
    
    Called by: Trading agent every 60 seconds
    Purpose: Let admin dashboard know agent is alive
    
    Status displayed in admin:
    - 🟢 Active (heartbeat < 5 min ago)
    - 🟠 Idle (heartbeat 5-60 min ago)
    - 🟡 Ready (no recent heartbeat but configured)
    """
    try:
        from admin_dashboard import log_agent_event
        
        log_agent_event(
            api_key=request.api_key,
            event_type="heartbeat",
            event_data={
                "status": request.status,
                "timestamp": datetime.utcnow().isoformat(),
                **(request.details or {})
            }
        )
        
        return {
            "status": "ok",
            "message": "Heartbeat received",
            "server_time": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to log heartbeat: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/api/log-error")
async def receive_error_log(request: ErrorLogRequest):
    """
    Receive error report from agent for troubleshooting.
    
    Common error_types:
    - hl_auth_failed: HL credentials invalid
    - trade_failed: Trade execution failed
    - api_error: Nike Rocket API communication error
    - signal_expired: Signal too old to execute
    - insufficient_balance: Not enough funds
    """
    try:
        from admin_dashboard import log_error
        
        log_error(
            api_key=request.api_key,
            error_type=request.error_type,
            error_message=request.error_message,
            context=request.context
        )
        
        logger.warning(f"⚠️ Error logged for {request.api_key[:15]}...: {request.error_type}")
        
        return {
            "status": "ok",
            "message": "Error logged",
            "error_type": request.error_type
        }
        
    except Exception as e:
        logger.error(f"Failed to log error: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/api/log-event")
async def receive_agent_event(request: AgentEventRequest):
    """
    Receive general agent event for monitoring.
    
    Common event_types:
    - agent_start: Agent started running
    - agent_stop: Agent stopped gracefully
    - hl_auth_success: HL credentials validated
    - trade_executed: Trade was executed successfully
    - signal_received: New signal received from API
    """
    try:
        from admin_dashboard import log_agent_event
        
        log_agent_event(
            api_key=request.api_key,
            event_type=request.event_type,
            event_data=request.event_data
        )
        
        logger.info(f"📝 Event logged for {request.api_key[:15]}...: {request.event_type}")
        
        return {
            "status": "ok",
            "message": "Event logged",
            "event_type": request.event_type
        }
        
    except Exception as e:
        logger.error(f"Failed to log event: {e}")
        return {"status": "error", "message": str(e)}


@router.get("/api/agent-logs")
async def get_agent_logs(
    x_api_key: str = Header(..., alias="X-API-Key"),
    limit: int = 50
):
    """Get recent agent logs for a specific user."""
    try:
        import psycopg2
        DATABASE_URL = os.getenv("DATABASE_URL")
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT timestamp, event_type, event_data
            FROM agent_logs
            WHERE api_key = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (x_api_key, limit))
        
        logs = []
        for row in cur.fetchall():
            logs.append({
                "timestamp": row[0].isoformat() if row[0] else None,
                "event_type": row[1],
                "event_data": row[2]
            })
        
        cur.close()
        conn.close()
        
        return {"status": "success", "logs": logs, "count": len(logs)}
        
    except Exception as e:
        logger.error(f"Failed to get agent logs: {e}")
        return {"status": "error", "message": str(e), "logs": []}


@router.get("/api/my-errors")
async def get_my_errors(
    x_api_key: str = Header(..., alias="X-API-Key"),
    hours: int = 24,
    limit: int = 20
):
    """Get recent errors for a specific user."""
    try:
        import psycopg2
        DATABASE_URL = os.getenv("DATABASE_URL")
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT timestamp, error_type, error_message, context
            FROM error_logs
            WHERE api_key = %s
            AND timestamp > NOW() - INTERVAL '%s hours'
            ORDER BY timestamp DESC
            LIMIT %s
        """, (x_api_key, hours, limit))
        
        errors = []
        for row in cur.fetchall():
            errors.append({
                "timestamp": row[0].isoformat() if row[0] else None,
                "error_type": row[1],
                "error_message": row[2],
                "context": row[3]
            })
        
        cur.close()
        conn.close()
        
        return {"status": "success", "errors": errors, "count": len(errors)}
        
    except Exception as e:
        logger.error(f"Failed to get errors: {e}")
        return {"status": "error", "message": str(e), "errors": []}


# Export router
__all__ = ["router"]
