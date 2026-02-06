"""
Nike Rocket Hyperliquid Follower System - API Endpoints
=========================================================

Mirrors Kraken follower_endpoints.py exactly, adapted for Hyperliquid.

Endpoints:
- POST /api/broadcast-signal - Receive signals from master algo
- GET /api/latest-signal - Followers poll for new signals
- POST /api/confirm-execution - Two-phase acknowledgment
- POST /api/report-pnl - Trade results (DEPRECATED for 30-day billing)
- POST /api/users/register - New user signup
- GET /api/users/verify - Verify user access
- GET /api/users/stats - Get user statistics
- POST /api/setup-agent - Setup Hyperliquid credentials
- GET /api/agent-status - Get agent status
- POST /api/stop-agent - Pause trading agent
- POST /api/start-agent - Resume trading agent
- POST /api/mark-signal-failed - Mark failed signal
- GET /api/failed-signals - List failed signals
- POST /api/retry-failed-signal - Retry failed signal
- POST /api/heartbeat - Agent heartbeat
- POST /api/log-error - Error reporting
- POST /api/payments/webhook - Coinbase Commerce webhook

Author: Nike Rocket Team
"""

from fastapi import APIRouter, HTTPException, Header, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
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

from follower_models import (
    User, Signal, SignalDelivery, Trade, Payment, SystemStats,
    get_db_session
)

from email_service import send_welcome_email, send_api_key_resend_email

from config import SIGNAL_EXPIRATION_MINUTES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Environment variables
MASTER_API_KEY = os.getenv("MASTER_API_KEY", "your-master-key-here")
COINBASE_WEBHOOK_SECRET = os.getenv("COINBASE_WEBHOOK_SECRET", "")
COINBASE_API_KEY = os.getenv("COINBASE_COMMERCE_API_KEY", "")


# ═══════════════════════════════════════════════════════════════════════════
# HYPERLIQUID WALLET VERIFICATION (Anti-Abuse)
# ═══════════════════════════════════════════════════════════════════════════

async def verify_hl_wallet(private_key: str, wallet_address: str) -> tuple[str, Optional[str]]:
    """
    Verify Hyperliquid credentials and generate account fingerprint.
    
    Unlike Kraken (which needs trade history fingerprinting), Hyperliquid
    wallet addresses ARE the identity. Each wallet is unique by definition.
    
    Flow:
    1. Validate the private key can sign for the given wallet address
    2. Verify connectivity by fetching user state
    3. Return wallet address as the account fingerprint
    
    Args:
        private_key: Hyperliquid wallet private key
        wallet_address: Public wallet address
        
    Returns:
        Tuple of (account_fingerprint, error_message)
    """
    try:
        import asyncio
        from hyperliquid.info import Info
        from hyperliquid.exchange import Exchange
        from hyperliquid.utils import constants
        from eth_account import Account

        # Step 1: Validate private key matches wallet address
        try:
            account = Account.from_key(private_key)
            derived_address = account.address.lower()
            provided_address = wallet_address.lower()
            
            if derived_address != provided_address:
                return (None, f"Private key does not match wallet address. Expected {derived_address}, got {provided_address}")
        except Exception as e:
            return (None, f"Invalid private key format: {str(e)}")
        
        # Step 2: Verify connectivity by fetching user state
        try:
            info = Info(constants.MAINNET_API_URL, skip_ws=True)
            user_state = await asyncio.to_thread(info.user_state, wallet_address)
            
            if not user_state:
                return (None, "Could not fetch account state from Hyperliquid")
            
            # Check balance
            margin_summary = user_state.get('marginSummary', {})
            account_value = float(margin_summary.get('accountValue', 0))
            
            logger.info(f"✅ Hyperliquid wallet verified. Balance: ${account_value:.2f}")
            
        except Exception as e:
            return (None, f"Hyperliquid API error: {str(e)}")
        
        # Step 3: Generate fingerprint from wallet address
        # Wallet address IS the fingerprint (unique by definition)
        address_hash = hashlib.sha256(wallet_address.lower().encode()).hexdigest()
        formatted_uid = f"{address_hash[:8]}-{address_hash[8:12]}-{address_hash[12:16]}-{address_hash[16:20]}-{address_hash[20:32]}"
        
        logger.info(f"✅ Account fingerprint: {formatted_uid[:20]}...")
        return (formatted_uid, None)
        
    except ImportError as e:
        return (None, f"Missing dependency: {str(e)}. Install hyperliquid-python-sdk and eth-account.")
    except Exception as e:
        logger.error(f"Error verifying Hyperliquid wallet: {e}")
        return (None, f"Failed to verify credentials: {str(e)}")


async def check_hl_account_abuse(wallet_address: str, current_user_id: int, db: Session) -> tuple[bool, Optional[str]]:
    """
    Check if this Hyperliquid wallet has unpaid invoices or is blocked.
    Same logic as Kraken but uses wallet address instead of account UID.
    """
    existing_user = db.query(User).filter(
        User.hl_wallet_address == wallet_address.lower(),
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
            return (True, f"This wallet has ${total_owed:.2f} in unpaid invoices from a previous account ({existing_user.email}). Please pay the outstanding balance before creating a new account.")
        
        if existing_user.suspension_reason and 'unpaid' in existing_user.suspension_reason.lower():
            return (True, f"This wallet was previously suspended for non-payment. Please contact support.")
    
    return (False, None)


# ==================== REQUEST MODELS ====================

class SignalBroadcast(BaseModel):
    """Signal from master algorithm"""
    action: str  # BUY or SELL
    symbol: str  # ADA/USDT or ADA
    entry_price: float
    stop_loss: float
    take_profit: float
    leverage: float
    risk_pct: Optional[float] = 0.02
    timeframe: Optional[str] = None
    trend_strength: Optional[float] = None
    volatility: Optional[float] = None
    notes: Optional[str] = None


class TradeReport(BaseModel):
    """Trade result from follower agent"""
    trade_id: str
    signal_id: Optional[str] = None
    hl_order_id: Optional[str] = None
    
    opened_at: str
    closed_at: str
    
    symbol: str
    side: str
    
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


class ExecutionConfirmation(BaseModel):
    """Confirm signal execution"""
    delivery_id: int


class ExecutionConfirmRequest(BaseModel):
    """Confirm signal execution with details"""
    delivery_id: int
    signal_id: Optional[str] = None
    executed_at: Optional[str] = None
    execution_price: Optional[float] = None


class RetryFailedSignalRequest(BaseModel):
    """Retry a failed signal"""
    failed_signal_id: int


class SetupAgentRequest(BaseModel):
    """Setup trading agent with Hyperliquid credentials"""
    hl_private_key: str
    hl_wallet_address: str


# ==================== DEPENDENCY INJECTION ====================

def get_db():
    """Database session dependency"""
    from sqlalchemy import create_engine
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise Exception("DATABASE_URL not set")
    
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
    Receive signal from master algorithm and distribute to all active followers.
    Called by: Hyperliquid master algo when it opens a position.
    Auth: Requires MASTER_API_KEY
    """
    try:
        signal_id = secrets.token_urlsafe(16)
        
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
        
        active_users = db.query(User).filter(
            User.access_granted == True
        ).all()
        
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
    Get latest signal for follower.
    Called by: Follower agents every 10 seconds during active window.
    Auth: Requires user API key.
    
    FIXES APPLIED:
    - BUG #3: Timezone-aware datetime comparison
    - BUG #2: Returns risk_pct in signal response
    """
    try:
        if not user.access_granted:
            amount_due = getattr(user, 'pending_invoice_amount', 0) or 0
            return {
                "access_granted": False,
                "reason": user.suspension_reason or "Payment required",
                "amount_due": amount_due
            }
        
        delivery = db.query(SignalDelivery).join(Signal).filter(
            SignalDelivery.user_id == user.id,
            SignalDelivery.acknowledged == False,
            SignalDelivery.failed == False
        ).order_by(Signal.created_at.desc()).first()
        
        if not delivery:
            return {
                "access_granted": True,
                "signal": None,
                "message": "No new signals"
            }
        
        # BUG #3 FIX: Timezone-aware datetime comparison
        now_utc = datetime.now(timezone.utc)
        signal_created = delivery.signal.created_at
        
        if signal_created.tzinfo is None:
            signal_created = signal_created.replace(tzinfo=timezone.utc)
        
        signal_age_seconds = (now_utc - signal_created).total_seconds()
        signal_age_minutes = signal_age_seconds / 60
        
        if signal_age_minutes > SIGNAL_EXPIRATION_MINUTES:
            delivery.acknowledged = True
            db.commit()
            
            logger.info(f"⚠️ Signal expired: {delivery.signal.signal_id} ({signal_age_minutes:.1f} min old)")
            
            return {
                "access_granted": True,
                "signal": None,
                "message": f"Signal expired ({signal_age_minutes:.0f} min old)"
            }
        
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
                "risk_pct": getattr(delivery.signal, 'risk_pct', 0.02),
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
    """Acknowledge signal receipt/execution"""
    try:
        delivery = db.query(SignalDelivery).filter(
            SignalDelivery.id == data.delivery_id,
            SignalDelivery.user_id == user.id
        ).first()
        
        if not delivery:
            raise HTTPException(status_code=404, detail="Delivery not found")
        
        delivery.acknowledged = True
        delivery.acknowledged_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"✓ Signal acknowledged by {user.email}")
        return {"status": "success", "message": "Signal acknowledged"}
    
    except Exception as e:
        logger.error(f"❌ Error acknowledging signal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== TWO-PHASE ACKNOWLEDGMENT ====================

@router.post("/api/confirm-execution")
async def confirm_execution(
    data: ExecutionConfirmRequest,
    x_api_key: str = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Confirm successful signal execution (two-phase acknowledgment).
    Marks signal as executed (prevents re-delivery). Idempotent.
    """
    try:
        if not x_api_key:
            raise HTTPException(status_code=401, detail="API key required in X-API-Key header")
        
        user = db.query(User).filter(User.api_key == x_api_key).first()
        if not user:
            raise HTTPException(status_code=404, detail="Invalid API key")
        
        delivery = db.query(SignalDelivery).filter(
            SignalDelivery.id == data.delivery_id,
            SignalDelivery.user_id == user.id
        ).first()
        
        if not delivery:
            raise HTTPException(status_code=404, detail="Delivery not found")
        
        if getattr(delivery, 'executed', False):
            logger.info(f"✓ Signal {data.delivery_id} already confirmed for {user.email}")
            return {
                "status": "already_confirmed",
                "message": "Signal was already confirmed",
                "delivery_id": data.delivery_id
            }
        
        executed_at = datetime.utcnow()
        if data.executed_at:
            try:
                executed_at = datetime.fromisoformat(data.executed_at.replace('Z', '+00:00'))
            except:
                pass
        
        delivery.acknowledged = True
        delivery.acknowledged_at = executed_at
        
        if hasattr(delivery, 'executed'):
            delivery.executed = True
        if hasattr(delivery, 'executed_at'):
            delivery.executed_at = executed_at
        if hasattr(delivery, 'execution_price') and data.execution_price:
            delivery.execution_price = data.execution_price
        
        db.commit()
        
        logger.info(f"✅ Signal execution confirmed: User={user.email}, Delivery={data.delivery_id}")
        
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


# ==================== FAILED SIGNAL RETRY QUEUE ====================

@router.post("/api/mark-signal-failed")
async def mark_signal_failed(
    delivery_id: int,
    failure_reason: str,
    x_api_key: str = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """Mark a signal delivery as failed for later retry"""
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
        
        delivery.failed = True
        delivery.failure_reason = failure_reason
        if hasattr(delivery, 'retry_count'):
            delivery.retry_count = (delivery.retry_count or 0) + 1
        
        db.commit()
        
        logger.warning(f"⚠️ Signal marked as failed: User={user.email}, Delivery={delivery_id}, Reason={failure_reason}")
        
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
    """Get list of failed signals for a user"""
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
        
        return {"status": "success", "failed_signals": signals, "count": len(signals)}
    
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
    """Reset a failed signal for retry"""
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
        
        delivery.failed = False
        delivery.failure_reason = None
        delivery.acknowledged = False
        if hasattr(delivery, 'executed'):
            delivery.executed = False
        
        db.commit()
        
        logger.info(f"🔄 Signal reset for retry: User={user.email}, Delivery={data.failed_signal_id}")
        
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
    Report trade result from follower.
    DEPRECATED: 30-day billing handles fees. Kept for backwards compatibility.
    """
    try:
        opened_at = datetime.fromisoformat(trade.opened_at.replace('Z', '+00:00'))
        closed_at = datetime.fromisoformat(trade.closed_at.replace('Z', '+00:00'))
        
        fee_charged = 0.0  # Always 0 - fees calculated at cycle end
        
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
            fee_charged=fee_charged,
            notes=trade.notes
        )
        db.add(db_trade)
        
        user.current_cycle_profit = (user.current_cycle_profit or 0) + trade.profit_usd
        user.current_cycle_trades = (user.current_cycle_trades or 0) + 1
        user.total_profit += trade.profit_usd
        user.total_trades += 1
        
        db.commit()
        
        logger.info(f"💰 Trade reported by {user.email}: {trade.symbol} ${trade.profit_usd:.2f}")
        
        return {
            "status": "success",
            "trade_id": db_trade.id,
            "profit_usd": trade.profit_usd,
            "fee_charged": 0,
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
    Register new user (EMAIL-ONLY FLOW for security).
    NEVER returns API key in response (email only!).
    """
    try:
        existing = db.query(User).filter(User.email == data.email).first()
        
        if existing:
            logger.info(f"🔄 Existing user requesting API key: {data.email}")
            email_sent = send_api_key_resend_email(existing.email, existing.api_key)
            return {
                "status": "success",
                "message": "API key sent to your email",
                "email": existing.email
            }
        
        api_key = f"nk_{secrets.token_urlsafe(32)}"
        
        user = User(
            email=data.email,
            api_key=api_key,
            access_granted=True
        )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        logger.info(f"✅ New user registered: {data.email}")
        
        email_sent = send_welcome_email(user.email, user.api_key)
        if not email_sent:
            logger.error(f"⚠️ Email failed for {user.email}, but user created")
        
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
    """Verify user access status"""
    payment_ok = user.check_payment_status()
    
    if not payment_ok and user.access_granted:
        user.access_granted = False
        user.suspended_at = datetime.utcnow()
        user.suspension_reason = "Monthly fee overdue"
        db.commit()
        logger.warning(f"⚠️ User suspended for non-payment: {user.email}")
    
    return {
        "access_granted": user.access_granted,
        "email": user.email,
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
    """Get user statistics"""
    recent_trades = db.query(Trade).filter(
        Trade.user_id == user.id
    ).order_by(Trade.closed_at.desc()).limit(10).all()
    
    return {
        "email": user.email,
        "access_granted": user.access_granted,
        "billing_cycle_start": user.billing_cycle_start.isoformat() if user.billing_cycle_start else None,
        "current_cycle_profit": user.current_cycle_profit or 0,
        "current_cycle_trades": user.current_cycle_trades or 0,
        "pending_invoice_amount": user.pending_invoice_amount or 0,
        "invoice_due_date": user.invoice_due_date.isoformat() if user.invoice_due_date else None,
        "total_profit": user.total_profit,
        "total_trades": user.total_trades,
        "total_fees_paid": user.total_fees_paid,
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


# ==================== HYPERLIQUID AGENT SETUP ====================

@router.post("/api/setup-agent")
async def setup_agent(
    data: SetupAgentRequest,
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """
    Setup customer's trading agent with Hyperliquid credentials.
    
    This endpoint:
    1. Validates Hyperliquid credentials (private key matches wallet)
    2. Verifies connectivity by fetching user state
    3. Checks for unpaid invoices from previous accounts (anti-abuse)
    4. Encrypts and stores credentials
    5. Marks user as ready for agent activation
    """
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    if not data.hl_private_key or not data.hl_wallet_address:
        raise HTTPException(status_code=400, detail="Both private key and wallet address required")
    
    if len(data.hl_private_key) < 20:
        raise HTTPException(status_code=400, detail="Invalid private key format")
    
    if not data.hl_wallet_address.startswith("0x") or len(data.hl_wallet_address) != 42:
        raise HTTPException(status_code=400, detail="Invalid wallet address format (must be 0x... 42 chars)")
    
    try:
        # Step 1: Validate credentials
        logger.info(f"🔐 Validating Hyperliquid credentials for: {user.email}")
        
        account_uid, error = await verify_hl_wallet(
            data.hl_private_key,
            data.hl_wallet_address
        )
        
        if error:
            logger.warning(f"❌ Credential validation failed for {user.email}: {error}")
            raise HTTPException(status_code=400, detail=error)
        
        logger.info(f"✅ Hyperliquid credentials valid. Account UID: {account_uid[:20]}...")
        
        # Step 2: Check for abuse
        is_blocked, block_reason = await check_hl_account_abuse(
            data.hl_wallet_address,
            user.id,
            db
        )
        
        if is_blocked:
            logger.warning(f"🚫 Setup blocked for {user.email}: {block_reason}")
            raise HTTPException(status_code=403, detail=block_reason)
        
        # Step 3: Store credentials
        user.set_hl_credentials(data.hl_private_key, data.hl_wallet_address.lower())
        user.credentials_set = True
        
        if not user.access_granted:
            user.access_granted = True
            user.suspended_at = None
            user.suspension_reason = None
        
        db.commit()
        
        logger.info(f"✅ Credentials set for user: {user.email}")
        logger.info(f"   Wallet: {data.hl_wallet_address[:10]}...")
        logger.info(f"   Agent will start automatically within 5 minutes")
        
        return {
            "status": "success",
            "message": "Trading agent configured successfully",
            "agent_status": "starting",
            "note": "Your agent will start automatically within 5 minutes",
            "email": user.email
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error setting up agent: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to setup agent: {str(e)}")


@router.get("/api/agent-status")
async def get_agent_status(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """Get customer's agent status"""
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    if not user.credentials_set:
        return {
            "agent_configured": False,
            "agent_active": False,
            "message": "Agent not configured. Please set up your Hyperliquid credentials.",
            "setup_url": "/setup"
        }
    
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
    """Stop customer's trading agent (preserves credentials)"""
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    user.agent_active = False
    db.commit()
    
    logger.info(f"⏸️ Agent paused for user: {user.email}")
    
    return {
        "status": "success",
        "message": "Trading agent paused",
        "agent_active": False,
        "agent_configured": user.credentials_set
    }


@router.post("/api/start-agent")
async def start_agent(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
):
    """Start/resume customer's trading agent"""
    user = db.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    if not user.credentials_set:
        return {
            "status": "error",
            "message": "Agent not configured. Please set up your Hyperliquid credentials first.",
            "redirect": f"/setup?key={x_api_key}"
        }
    
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

@router.post("/api/payments/webhook")
async def coinbase_webhook(
    request: dict,
    x_cc_webhook_signature: str = Header(None),
    db: Session = Depends(get_db)
):
    """Coinbase Commerce webhook - processes payment confirmations"""
    try:
        if COINBASE_WEBHOOK_SECRET:
            payload = json.dumps(request)
            signature = hmac.new(
                COINBASE_WEBHOOK_SECRET.encode(),
                payload.encode(),
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, x_cc_webhook_signature or ""):
                raise HTTPException(status_code=401, detail="Invalid signature")
        
        event = request.get("event", {})
        event_type = event.get("type")
        
        if event_type == "charge:confirmed":
            charge = event.get("data", {})
            metadata = charge.get("metadata", {})
            
            user_id = metadata.get("user_id")
            if not user_id:
                return {"status": "ignored"}
            
            user = db.query(User).filter(User.id == int(user_id)).first()
            if not user:
                return {"status": "user_not_found"}
            
            payment = db.query(Payment).filter(
                Payment.coinbase_charge_id == charge["id"]
            ).first()
            
            if payment:
                payment.status = "completed"
                payment.completed_at = datetime.utcnow()
                payment.tx_hash = charge.get("payments", [{}])[0].get("transaction_id")
            
            paid_amount = user.pending_invoice_amount or 0
            user.pending_invoice_id = None
            user.pending_invoice_amount = 0
            user.invoice_due_date = None
            user.total_fees_paid = (user.total_fees_paid or 0) + paid_amount
            user.access_granted = True
            user.suspended_at = None
            user.suspension_reason = None
            
            db.commit()
            
            logger.info(f"✅ Payment confirmed for {user.email}: ${paid_amount:.2f}")
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
    """Get system-wide statistics"""
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


# ==================== MONITORING ENDPOINTS ====================

class HeartbeatRequest(BaseModel):
    api_key: str
    status: Optional[str] = "alive"
    details: Optional[Dict] = None


class ErrorLogRequest(BaseModel):
    api_key: str
    error_type: str
    error_message: str
    context: Optional[Dict] = None


class AgentEventRequest(BaseModel):
    api_key: str
    event_type: str
    event_data: Optional[Dict] = None


@router.post("/api/heartbeat")
async def receive_heartbeat(request: HeartbeatRequest):
    """Receive heartbeat from running trading agent"""
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
        return {"status": "ok", "message": "Heartbeat received", "server_time": datetime.utcnow().isoformat()}
    except Exception as e:
        logger.error(f"Failed to log heartbeat: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/api/log-error")
async def receive_error_log(request: ErrorLogRequest):
    """Receive error report from agent"""
    try:
        from admin_dashboard import log_error
        log_error(
            api_key=request.api_key,
            error_type=request.error_type,
            error_message=request.error_message,
            context=request.context
        )
        logger.warning(f"⚠️ Error logged for {request.api_key[:15]}...: {request.error_type}")
        return {"status": "ok", "message": "Error logged", "error_type": request.error_type}
    except Exception as e:
        logger.error(f"Failed to log error: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/api/log-event")
async def receive_agent_event(request: AgentEventRequest):
    """Receive general agent event"""
    try:
        from admin_dashboard import log_agent_event
        log_agent_event(
            api_key=request.api_key,
            event_type=request.event_type,
            event_data=request.event_data
        )
        return {"status": "ok", "message": "Event logged", "event_type": request.event_type}
    except Exception as e:
        logger.error(f"Failed to log event: {e}")
        return {"status": "error", "message": str(e)}


@router.get("/api/agent-logs")
async def get_agent_logs(
    x_api_key: str = Header(..., alias="X-API-Key"),
    limit: int = 50
):
    """Get recent agent logs for a specific user"""
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


__all__ = ["router"]
