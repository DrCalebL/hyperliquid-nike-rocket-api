"""
Nike Rocket Billing Endpoints - 30-Day Rolling
===============================================

FastAPI endpoints for billing functionality:
1. Coinbase Commerce webhook handler
2. Payment status endpoint
3. Billing cycle info endpoint

Author: Nike Rocket Team
Version: 2.0 (30-Day Rolling)
"""

import hashlib
import hmac
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import JSONResponse

from config import is_production, utc_now, BILLING_CYCLE_DAYS

# Configuration
COINBASE_WEBHOOK_SECRET = os.getenv("COINBASE_WEBHOOK_SECRET", "")

# Setup logging
logger = logging.getLogger("BILLING_API")

# Router
router = APIRouter(prefix="/api/billing", tags=["Billing"])


def verify_coinbase_signature(payload: bytes, signature: str) -> bool:
    """
    Verify Coinbase Commerce webhook signature
    
    Args:
        payload: Raw request body bytes
        signature: X-CC-Webhook-Signature header value
        
    Returns:
        True if signature is valid
    """
    if not COINBASE_WEBHOOK_SECRET:
        if is_production():
            # Hard fail in production - don't allow unverified webhooks
            logger.error("❌ COINBASE_WEBHOOK_SECRET not set in production!")
            return False
        
        # Only allow bypass in local development
        logger.warning("⚠️ Dev mode - COINBASE_WEBHOOK_SECRET not set, skipping verification")
        return True
    
    expected = hmac.new(
        COINBASE_WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected, signature)


@router.post("/webhook/coinbase")
async def coinbase_webhook(request: Request):
    """
    Handle Coinbase Commerce payment webhooks
    
    Events:
    - charge:created - Invoice created (we already know this)
    - charge:confirmed - Payment confirmed on blockchain
    - charge:completed - Payment fully complete
    - charge:failed - Payment failed
    - charge:expired - Invoice expired without payment
    """
    # Get raw body for signature verification
    body = await request.body()
    
    # Verify signature
    signature = request.headers.get("X-CC-Webhook-Signature", "")
    if not verify_coinbase_signature(body, signature):
        if is_production():
            logger.error("❌ Invalid Coinbase webhook signature in production - rejecting")
            raise HTTPException(status_code=401, detail="Invalid signature")
        else:
            logger.warning("⚠️ Invalid Coinbase webhook signature in dev mode - allowing")
    
    # Parse event
    try:
        import json
        event = json.loads(body)
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    
    event_type = event.get("event", {}).get("type")
    charge_data = event.get("event", {}).get("data", {})
    charge_id = charge_data.get("id")
    
    logger.info(f"📥 Coinbase webhook: {event_type} for charge {charge_id}")
    
    if not charge_id:
        return JSONResponse({"status": "ignored", "reason": "no charge_id"})
    
    # Get billing service from app state
    from main import get_db_pool
    db_pool = await get_db_pool()
    
    if not db_pool:
        logger.error("❌ Database pool not available")
        raise HTTPException(status_code=500, detail="Database unavailable")
    
    # Import billing service
    from billing_service_30day import BillingServiceV2
    billing = BillingServiceV2(db_pool)
    
    # Process the event
    if event_type in ['charge:confirmed', 'charge:completed']:
        await billing.process_webhook_payment(charge_id, event_type)
        logger.info(f"✅ Payment processed for charge {charge_id}")
        
    elif event_type in ['charge:failed', 'charge:expired']:
        await billing.process_webhook_payment(charge_id, event_type)
        logger.warning(f"⚠️ Payment failed/expired for charge {charge_id}")
    
    return JSONResponse({"status": "ok"})


@router.get("/status")
async def get_billing_status(
    key: str = Query(..., description="User API key")
):
    """
    Get user's billing status
    
    Returns current billing cycle info, pending invoices, etc.
    """
    from main import get_db_pool
    db_pool = await get_db_pool()
    
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database unavailable")
    
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("""
            SELECT 
                id, email, fee_tier, next_cycle_fee_tier,
                billing_cycle_start, current_cycle_profit, current_cycle_trades,
                pending_invoice_id, pending_invoice_amount, invoice_due_date,
                total_profit, total_trades, total_fees_paid
            FROM follower_users
            WHERE api_key = $1
        """, key)
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Calculate cycle info
        cycle_start = user['billing_cycle_start']
        if cycle_start:
            cycle_end = cycle_start + timedelta(days=BILLING_CYCLE_DAYS)
            now = utc_now()
            # Make cycle_end timezone-aware for comparison
            if cycle_end.tzinfo is None:
                from datetime import timezone
                cycle_end = cycle_end.replace(tzinfo=timezone.utc)
            days_remaining = max(0, (cycle_end - now).days)
        else:
            cycle_end = None
            days_remaining = None
        
        # Get pending invoice details
        pending_invoice = None
        if user['pending_invoice_id']:
            invoice = await conn.fetchrow("""
                SELECT hosted_url, amount_usd, created_at, expires_at, status
                FROM billing_invoices
                WHERE coinbase_charge_id = $1
            """, user['pending_invoice_id'])
            
            if invoice:
                pending_invoice = {
                    "amount": float(invoice['amount_usd']),
                    "payment_url": invoice['hosted_url'],
                    "created_at": invoice['created_at'].isoformat() if invoice['created_at'] else None,
                    "expires_at": invoice['expires_at'].isoformat() if invoice['expires_at'] else None,
                    "status": invoice['status']
                }
        
        return {
            "status": "success",
            "billing": {
                "fee_tier": user['fee_tier'],
                "next_cycle_fee_tier": user['next_cycle_fee_tier'],
                "current_cycle": {
                    "start": cycle_start.isoformat() if cycle_start else None,
                    "end": cycle_end.isoformat() if cycle_end else None,
                    "days_remaining": days_remaining,
                    "profit": float(user['current_cycle_profit'] or 0),
                    "trades": int(user['current_cycle_trades'] or 0)
                },
                "pending_invoice": pending_invoice,
                "lifetime": {
                    "total_profit": float(user['total_profit'] or 0),
                    "total_trades": int(user['total_trades'] or 0),
                    "total_fees_paid": float(user['total_fees_paid'] or 0)
                }
            }
        }


@router.get("/cycles")
async def get_billing_cycles(
    key: str = Query(..., description="User API key"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Get user's billing cycle history
    """
    from main import get_db_pool
    db_pool = await get_db_pool()
    
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database unavailable")
    
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("""
            SELECT id FROM follower_users WHERE api_key = $1
        """, key)
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        cycles = await conn.fetch("""
            SELECT 
                cycle_number, cycle_start, cycle_end,
                total_profit, total_trades,
                fee_tier, fee_percentage, fee_amount,
                invoice_status, invoice_paid_at
            FROM billing_cycles
            WHERE user_id = $1
            ORDER BY cycle_number DESC
            LIMIT $2
        """, user['id'], limit)
        
        return {
            "status": "success",
            "cycles": [
                {
                    "cycle_number": c['cycle_number'],
                    "start": c['cycle_start'].isoformat() if c['cycle_start'] else None,
                    "end": c['cycle_end'].isoformat() if c['cycle_end'] else None,
                    "profit": float(c['total_profit']),
                    "trades": c['total_trades'],
                    "fee_tier": c['fee_tier'],
                    "fee_percentage": float(c['fee_percentage']),
                    "fee_amount": float(c['fee_amount']),
                    "invoice_status": c['invoice_status'],
                    "paid_at": c['invoice_paid_at'].isoformat() if c['invoice_paid_at'] else None
                }
                for c in cycles
            ]
        }


# Export router
__all__ = ['router']
