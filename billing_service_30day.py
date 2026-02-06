"""
Nike Rocket Billing Service v2 - 30-Day Rolling Cycles + Error Logging
========================================================================

Implements rolling 30-day billing cycles with Coinbase Commerce invoices.

How it works:
1. User's first trade starts their 30-day billing cycle
2. Profits accumulate during the cycle (no immediate fees)
3. At cycle end (30 days):
   - Calculate total profit for the cycle
   - If profitable: Generate Coinbase invoice for tier% of profits
   - If losing/breakeven: No invoice, start new cycle
4. User has 7 days to pay invoice
5. If unpaid after 7 days: Agent paused until payment

Fee Tiers:
- team: 0% (free)
- vip: 5% of profits  
- standard: 10% of profits

Tier changes apply to NEXT billing cycle, not current.

Author: Nike Rocket Team
Version: 2.1 (30-Day Rolling + Error Logging)
"""

import asyncio
import logging
import os
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Dict, Any

import asyncpg
import requests

# Import centralized configuration
from config import (
    get_fee_rate, get_tier_display, get_tier_percentage_str, get_valid_tiers,
    utc_now, to_naive_utc,
    BILLING_CYCLE_DAYS, PAYMENT_GRACE_DAYS, REMINDER_DAYS,
    ERROR_MESSAGE_MAX_LENGTH, ERROR_CONTEXT_MAX_LENGTH
)

# Configuration
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_API_URL = "https://api.resend.com/emails"
FROM_EMAIL = os.getenv("FROM_EMAIL", "$NIKEPIG's Massive Rocket <noreply@nikerocket.io>")
BASE_URL = os.getenv("BASE_URL", "https://nike-rocket-api-production.up.railway.app")

# Coinbase Commerce
COINBASE_API_KEY = os.getenv("COINBASE_COMMERCE_API_KEY", "")
COINBASE_API_URL = "https://api.commerce.coinbase.com"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BILLING")


async def log_error_to_db(pool, api_key: str, error_type: str, error_message: str, context: Optional[Dict] = None):
    """Log error to error_logs table for admin dashboard visibility"""
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


class BillingServiceV2:
    """
    30-Day Rolling Billing Service
    
    Key methods:
    - start_billing_cycle(user_id): Start user's first billing cycle
    - record_profit(user_id, amount): Add profit to current cycle
    - check_cycle_end(): Check all users for cycle endings
    - generate_invoice(user_id): Create Coinbase invoice
    - process_payment(charge_id): Handle webhook payment confirmation
    """
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        self.logger = logging.getLogger("BILLING")
    
    # =========================================================================
    # CYCLE MANAGEMENT
    # =========================================================================
    
    async def start_billing_cycle(self, user_id: int) -> bool:
        """
        Start a user's billing cycle (called on first trade)
        
        Args:
            user_id: The user's ID
            
        Returns:
            True if cycle started, False if already has one
        """
        async with self.db_pool.acquire() as conn:
            # Check if user already has a billing cycle
            user = await conn.fetchrow("""
                SELECT billing_cycle_start FROM follower_users WHERE id = $1
            """, user_id)
            
            if user and user['billing_cycle_start']:
                self.logger.debug(f"User {user_id} already has billing cycle")
                return False
            
            # Start new cycle
            now = to_naive_utc(utc_now())
            await conn.execute("""
                UPDATE follower_users SET
                    billing_cycle_start = $1,
                    current_cycle_profit = 0,
                    current_cycle_trades = 0
                WHERE id = $2
            """, now, user_id)
            
            self.logger.info(f"📅 Started billing cycle for user {user_id} at {now}")
            return True
    
    async def record_profit(self, user_id: int, profit_amount: float) -> bool:
        """
        Record profit from a closed trade (accumulates in current cycle)
        
        Called by position_monitor when a trade closes.
        Does NOT charge fees - just accumulates for end-of-cycle billing.
        
        Args:
            user_id: The user's ID
            profit_amount: The trade's profit (can be negative for losses)
            
        Returns:
            True if recorded successfully
        """
        async with self.db_pool.acquire() as conn:
            # Get current billing status
            user = await conn.fetchrow("""
                SELECT billing_cycle_start, current_cycle_profit, current_cycle_trades
                FROM follower_users WHERE id = $1
            """, user_id)
            
            if not user:
                self.logger.error(f"User {user_id} not found")
                return False
            
            # Start cycle if not started
            if not user['billing_cycle_start']:
                await self.start_billing_cycle(user_id)
            
            # Accumulate profit
            await conn.execute("""
                UPDATE follower_users SET
                    current_cycle_profit = COALESCE(current_cycle_profit, 0) + $1,
                    current_cycle_trades = COALESCE(current_cycle_trades, 0) + 1,
                    total_profit = COALESCE(total_profit, 0) + $1,
                    total_trades = COALESCE(total_trades, 0) + 1
                WHERE id = $2
            """, profit_amount, user_id)
            
            self.logger.debug(f"Recorded ${profit_amount:.2f} profit for user {user_id}")
            return True
    
    async def check_all_cycles(self) -> Dict[str, Any]:
        """
        Check all users for billing cycle endings
        
        Called hourly by the billing scheduler.
        
        Returns:
            Summary of actions taken
        """
        self.logger.info("📊 Checking billing cycles...")
        
        results = {
            "cycles_ended": 0,
            "invoices_generated": 0,
            "cycles_renewed": 0,
            "errors": 0
        }
        
        async with self.db_pool.acquire() as conn:
            # Find users whose 30-day cycle has ended
            cycle_end_threshold = to_naive_utc(utc_now() - timedelta(days=BILLING_CYCLE_DAYS))
            
            users = await conn.fetch("""
                SELECT 
                    id, email, api_key, fee_tier,
                    billing_cycle_start, current_cycle_profit, current_cycle_trades,
                    next_cycle_fee_tier, pending_invoice_id
                FROM follower_users
                WHERE billing_cycle_start IS NOT NULL
                AND billing_cycle_start <= $1
                AND pending_invoice_id IS NULL
                AND access_granted = true
            """, cycle_end_threshold)
            
            for user in users:
                try:
                    result = await self._end_billing_cycle(user)
                    if result == 'invoice_generated':
                        results['invoices_generated'] += 1
                    elif result == 'cycle_renewed':
                        results['cycles_renewed'] += 1
                    results['cycles_ended'] += 1
                except Exception as e:
                    self.logger.error(f"Error processing cycle for user {user['id']}: {e}")
                    await log_error_to_db(
                        self.db_pool, user['api_key'], "BILLING_CYCLE_ERROR",
                        str(e), {"user_id": user['id'], "function": "check_and_process_cycles"}
                    )
                    results['errors'] += 1
        
        self.logger.info(
            f"✅ Cycle check complete: {results['cycles_ended']} ended, "
            f"{results['invoices_generated']} invoices, {results['cycles_renewed']} renewed"
        )
        return results
    
    async def _end_billing_cycle(self, user: dict) -> str:
        """
        End a user's billing cycle and process accordingly
        
        Returns:
            'invoice_generated' - profitable cycle, invoice sent
            'cycle_renewed' - no profit, started new cycle
        """
        user_id = user['id']
        profit = float(user['current_cycle_profit'] or 0)
        trades = int(user['current_cycle_trades'] or 0)
        # Use centralized fee tier handling (handles None and empty string)
        fee_tier = user['fee_tier'] if user['fee_tier'] else 'standard'
        cycle_start = user['billing_cycle_start']
        cycle_end = to_naive_utc(utc_now())
        
        self.logger.info(f"📅 Ending cycle for user {user_id}: ${profit:.2f} profit, {trades} trades")
        
        async with self.db_pool.acquire() as conn:
            # Use transaction to ensure atomicity
            async with conn.transaction():
                # Get cycle number
                cycle_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM billing_cycles WHERE user_id = $1
                """, user_id) or 0
                cycle_number = cycle_count + 1
                
                # Calculate fee using centralized config
                fee_percentage = get_fee_rate(fee_tier)
                fee_amount = max(0, profit * fee_percentage) if profit > 0 else 0
                
                # Record the billing cycle
                cycle_id = await conn.fetchval("""
                    INSERT INTO billing_cycles 
                    (user_id, cycle_start, cycle_end, cycle_number, 
                     total_profit, total_trades, fee_tier, fee_percentage, fee_amount)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id
                """, user_id, cycle_start, cycle_end, cycle_number,
                    profit, trades, fee_tier, fee_percentage, fee_amount)
                
                result = 'cycle_renewed'
                
                if profit > 0 and fee_amount > 0 and fee_tier != 'team':
                    # Profitable cycle - generate invoice
                    invoice_result = await self._generate_coinbase_invoice(
                        conn=conn,  # Pass connection to avoid FK violation
                        user_id=user_id,
                        email=user['email'],
                        api_key=user['api_key'],
                        amount=fee_amount,
                        profit=profit,
                        fee_tier=fee_tier,
                        fee_percentage=fee_percentage,
                        cycle_start=cycle_start,
                        cycle_end=cycle_end,
                        cycle_id=cycle_id
                    )
                    
                    if invoice_result:
                        # Calculate due date from cycle_end (not current time)
                        due_date = cycle_end + timedelta(days=PAYMENT_GRACE_DAYS)
                        
                        # Update user with pending invoice
                        await conn.execute("""
                            UPDATE follower_users SET
                                pending_invoice_id = $1,
                                pending_invoice_amount = $2,
                                invoice_due_date = $3,
                                current_cycle_profit = 0,
                                current_cycle_trades = 0,
                                billing_cycle_start = $4
                            WHERE id = $5
                        """, invoice_result['charge_id'], fee_amount, 
                            due_date, cycle_end, user_id)
                        
                        result = 'invoice_generated'
                else:
                    # No profit or team tier - just start new cycle
                    await conn.execute("""
                        UPDATE follower_users SET
                            current_cycle_profit = 0,
                            current_cycle_trades = 0,
                            billing_cycle_start = $1
                        WHERE id = $2
                    """, cycle_end, user_id)
                    
                    # Mark cycle as waived (no invoice needed)
                    await conn.execute("""
                        UPDATE billing_cycles SET invoice_status = 'waived' WHERE id = $1
                    """, cycle_id)
                
                # Apply tier change if pending (moved outside if/else to always apply)
                if user['next_cycle_fee_tier']:
                    await conn.execute("""
                        UPDATE follower_users SET
                            fee_tier = next_cycle_fee_tier,
                            next_cycle_fee_tier = NULL
                        WHERE id = $1
                    """, user_id)
                
                return result
    
    # =========================================================================
    # COINBASE COMMERCE INTEGRATION
    # =========================================================================
    
    async def _generate_coinbase_invoice(
        self,
        conn,  # Database connection (passed to avoid FK violation)
        user_id: int,
        email: str,
        api_key: str,
        amount: float,
        profit: float,
        fee_tier: str,
        fee_percentage: float,
        cycle_start: datetime,
        cycle_end: datetime,
        cycle_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Generate a Coinbase Commerce charge for the billing invoice
        
        Returns:
            Dict with charge_id and hosted_url, or None on failure
        """
        if not COINBASE_API_KEY:
            self.logger.warning("⚠️ COINBASE_API_KEY not set - invoice not created")
            return None
        
        cycle_label = f"{cycle_start.strftime('%b %d')} - {cycle_end.strftime('%b %d, %Y')}"
        
        try:
            headers = {
                "Content-Type": "application/json",
                "X-CC-Api-Key": COINBASE_API_KEY,
                "X-CC-Version": "2018-03-22"
            }
            
            payload = {
                "name": f"Nike Rocket - 30-Day Billing",
                "description": f"Profit share for cycle: {cycle_label}\n"
                               f"Profit: ${profit:.2f}\n"
                               f"Fee ({fee_tier.upper()}): {int(fee_percentage*100)}%",
                "pricing_type": "fixed_price",
                "local_price": {
                    "amount": f"{amount:.2f}",
                    "currency": "USD"
                },
                "metadata": {
                    "user_id": str(user_id),
                    "api_key": api_key[:20] + "...",
                    "cycle_id": str(cycle_id),
                    "profit": str(profit),
                    "fee_tier": fee_tier,
                    "cycle_start": cycle_start.isoformat(),
                    "cycle_end": cycle_end.isoformat()
                },
                "redirect_url": f"{BASE_URL}/dashboard?key={api_key}&payment=success",
                "cancel_url": f"{BASE_URL}/dashboard?key={api_key}&payment=canceled"
            }
            
            response = requests.post(
                f"{COINBASE_API_URL}/charges",
                json=payload,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 201:
                data = response.json()['data']
                charge_id = data['id']
                hosted_url = data['hosted_url']
                charge_code = data['code']
                # Parse expires_at and convert to naive datetime (strip timezone)
                expires_at_str = data['expires_at'].replace('Z', '+00:00')
                expires_at = datetime.fromisoformat(expires_at_str).replace(tzinfo=None)
                
                # Record invoice in database (use passed connection to see uncommitted billing_cycle)
                await conn.execute("""
                    INSERT INTO billing_invoices
                    (user_id, billing_cycle_id, coinbase_charge_id, coinbase_charge_code,
                     hosted_url, amount_usd, profit_amount, fee_tier, fee_percentage,
                     cycle_start, cycle_end, expires_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """, user_id, cycle_id, charge_id, charge_code,
                    hosted_url, amount, profit, fee_tier, fee_percentage,
                    cycle_start, cycle_end, expires_at)
                
                # Update billing cycle with invoice info
                await conn.execute("""
                    UPDATE billing_cycles SET
                        invoice_id = $1,
                        invoice_created_at = CURRENT_TIMESTAMP
                    WHERE id = $2
                """, charge_id, cycle_id)
                
                self.logger.info(f"💳 Created Coinbase invoice for user {user_id}: ${amount:.2f}")
                
                # Send invoice email
                self._send_invoice_email(email, api_key, amount, profit, fee_tier, 
                                        cycle_label, hosted_url)
                
                return {
                    "charge_id": charge_id,
                    "hosted_url": hosted_url,
                    "charge_code": charge_code
                }
            else:
                self.logger.error(f"Coinbase API error: {response.status_code} - {response.text}")
                await log_error_to_db(
                    self.db_pool, api_key, "COINBASE_API_ERROR",
                    f"Status {response.status_code}: {response.text[:200]}",
                    {"user_id": user_id, "amount": amount, "function": "create_coinbase_invoice"}
                )
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating Coinbase invoice: {e}")
            await log_error_to_db(
                self.db_pool, api_key, "COINBASE_INVOICE_ERROR",
                str(e), {"user_id": user_id, "amount": amount, "function": "create_coinbase_invoice"}
            )
            return None
    
    async def process_webhook_payment(self, charge_id: str, event_type: str) -> bool:
        """
        Process Coinbase Commerce webhook for payment events
        
        Args:
            charge_id: The Coinbase charge ID
            event_type: The webhook event type (charge:confirmed, charge:failed, etc)
            
        Returns:
            True if processed successfully
        """
        async with self.db_pool.acquire() as conn:
            # Find the invoice
            invoice = await conn.fetchrow("""
                SELECT bi.*, fu.id as user_id, fu.email, fu.api_key
                FROM billing_invoices bi
                JOIN follower_users fu ON fu.id = bi.user_id
                WHERE bi.coinbase_charge_id = $1
            """, charge_id)
            
            if not invoice:
                self.logger.warning(f"Invoice not found for charge {charge_id}")
                return False
            
            # IDEMPOTENCY CHECK: Don't process if already in final state
            if invoice['status'] == 'paid':
                self.logger.info(f"ℹ️ Invoice {charge_id} already marked as paid - skipping duplicate webhook")
                return True
            
            if invoice['status'] == 'expired' and event_type in ['charge:failed', 'charge:expired']:
                self.logger.info(f"ℹ️ Invoice {charge_id} already marked as expired - skipping duplicate webhook")
                return True
            
            if event_type in ['charge:confirmed', 'charge:completed']:
                # Payment successful!
                await conn.execute("""
                    UPDATE billing_invoices SET
                        status = 'paid',
                        paid_at = CURRENT_TIMESTAMP
                    WHERE coinbase_charge_id = $1
                """, charge_id)
                
                await conn.execute("""
                    UPDATE billing_cycles SET
                        invoice_status = 'paid',
                        invoice_paid_at = CURRENT_TIMESTAMP
                    WHERE invoice_id = $1
                """, charge_id)
                
                # Clear pending invoice AND reactivate agent if suspended for non-payment
                await conn.execute("""
                    UPDATE follower_users SET
                        pending_invoice_id = NULL,
                        pending_invoice_amount = 0,
                        invoice_due_date = NULL,
                        total_fees_paid = COALESCE(total_fees_paid, 0) + $1,
                        -- REACTIVATE if suspended for non-payment
                        agent_active = CASE 
                            WHEN suspension_reason = 'Unpaid invoice - agent paused' THEN true 
                            ELSE agent_active 
                        END,
                        access_granted = CASE 
                            WHEN suspension_reason = 'Unpaid invoice - agent paused' THEN true 
                            ELSE access_granted 
                        END,
                        suspended_at = CASE 
                            WHEN suspension_reason = 'Unpaid invoice - agent paused' THEN NULL 
                            ELSE suspended_at 
                        END,
                        suspension_reason = CASE 
                            WHEN suspension_reason = 'Unpaid invoice - agent paused' THEN NULL 
                            ELSE suspension_reason 
                        END
                    WHERE id = $2
                """, float(invoice['amount_usd']), invoice['user_id'])
                
                self.logger.info(f"✅ Payment confirmed for user {invoice['user_id']}: ${invoice['amount_usd']:.2f}")
                self.logger.info(f"🔄 Agent reactivated (if was suspended for non-payment)")
                
                # Send confirmation email
                self._send_payment_confirmation_email(
                    invoice['email'], 
                    invoice['api_key'],
                    float(invoice['amount_usd'])
                )
                
                return True
                
            elif event_type in ['charge:failed', 'charge:expired']:
                # Payment failed/expired
                await conn.execute("""
                    UPDATE billing_invoices SET status = 'expired' WHERE coinbase_charge_id = $1
                """, charge_id)
                
                await conn.execute("""
                    UPDATE billing_cycles SET invoice_status = 'overdue' WHERE invoice_id = $1
                """, charge_id)
                
                self.logger.warning(f"⚠️ Payment failed/expired for charge {charge_id}")
                return True
        
        return False
    
    # =========================================================================
    # PAYMENT REMINDERS & SUSPENSIONS
    # =========================================================================
    
    async def check_overdue_invoices(self) -> Dict[str, int]:
        """
        Check for overdue invoices and take action
        
        - Send reminders at 3, 5, 7 days
        - Pause agent after 7 days
        """
        self.logger.info("📧 Checking for overdue invoices...")
        
        results = {"reminders_sent": 0, "agents_paused": 0}
        
        async with self.db_pool.acquire() as conn:
            # Get users with pending invoices
            users = await conn.fetch("""
                SELECT 
                    fu.id, fu.email, fu.api_key, fu.pending_invoice_id,
                    fu.pending_invoice_amount, fu.invoice_due_date,
                    bi.hosted_url, bi.created_at as invoice_created_at
                FROM follower_users fu
                JOIN billing_invoices bi ON bi.coinbase_charge_id = fu.pending_invoice_id
                WHERE fu.pending_invoice_id IS NOT NULL
                AND fu.access_granted = true
                AND bi.status = 'pending'
            """)
            
            now = to_naive_utc(utc_now())
            
            for user in users:
                days_since_invoice = (now - user['invoice_created_at']).days
                
                # Check if past due date (7 days)
                if user['invoice_due_date'] and now > user['invoice_due_date']:
                    # Pause agent
                    await conn.execute("""
                        UPDATE follower_users SET
                            agent_active = false,
                            suspended_at = CURRENT_TIMESTAMP,
                            suspension_reason = 'Unpaid invoice - agent paused'
                        WHERE id = $1
                    """, user['id'])
                    
                    await conn.execute("""
                        UPDATE billing_invoices SET status = 'overdue' 
                        WHERE coinbase_charge_id = $1
                    """, user['pending_invoice_id'])
                    
                    results['agents_paused'] += 1
                    self.logger.warning(f"⏸️ Paused agent for user {user['id']} - unpaid invoice")
                    
                    # Send final notice
                    self._send_suspension_email(
                        user['email'], user['api_key'],
                        float(user['pending_invoice_amount']),
                        user['hosted_url']
                    )
                    
                elif days_since_invoice in REMINDER_DAYS:
                    # Send reminder
                    self._send_reminder_email(
                        user['email'], user['api_key'],
                        float(user['pending_invoice_amount']),
                        user['hosted_url'],
                        days_remaining=PAYMENT_GRACE_DAYS - days_since_invoice
                    )
                    results['reminders_sent'] += 1
        
        self.logger.info(f"✅ Overdue check: {results['reminders_sent']} reminders, {results['agents_paused']} paused")
        return results
    
    async def reactivate_after_payment(self, user_id: int) -> bool:
        """
        Reactivate a user's agent after they pay their invoice
        """
        async with self.db_pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE follower_users SET
                    agent_active = true,
                    access_granted = true,
                    suspended_at = NULL,
                    suspension_reason = NULL
                WHERE id = $1 AND suspension_reason = 'Unpaid invoice - agent paused'
            """, user_id)
            
            if result == "UPDATE 1":
                self.logger.info(f"✅ Reactivated agent for user {user_id} after payment")
                return True
            return False
    
    # =========================================================================
    # EMAIL FUNCTIONS
    # =========================================================================
    
    def _send_invoice_email(
        self, email: str, api_key: str, amount: float, profit: float,
        fee_tier: str, cycle_label: str, payment_url: str
    ) -> bool:
        """Send invoice email with payment link"""
        if not RESEND_API_KEY:
            self.logger.warning("⚠️ RESEND_API_KEY not set - email not sent")
            return False
        
        fee_rate_str = get_tier_percentage_str(fee_tier)
        dashboard_link = f"{BASE_URL}/dashboard?key={api_key}"
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; background: #f5f5f5;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #f5f5f5; padding: 40px 20px;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="max-width: 600px; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
                    
                    <!-- Header -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 40px 30px; text-align: center;">
                            <h1 style="margin: 0; color: white; font-size: 28px; font-weight: bold;">
                                🚀 30-Day Billing Invoice
                            </h1>
                            <p style="margin: 8px 0 0 0; color: rgba(255,255,255,0.9); font-size: 16px;">
                                {cycle_label}
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Body -->
                    <tr>
                        <td style="padding: 40px 30px;">
                            
                            <!-- Profit Summary -->
                            <div style="background: #f0fdf4; border-radius: 12px; padding: 25px; margin-bottom: 25px; text-align: center;">
                                <p style="margin: 0 0 5px 0; color: #6b7280; font-size: 14px;">Your Profit This Cycle</p>
                                <p style="margin: 0; color: #059669; font-size: 36px; font-weight: bold;">
                                    ${profit:,.2f}
                                </p>
                            </div>
                            
                            <!-- Fee Breakdown -->
                            <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 30px;">
                                <tr>
                                    <td style="padding: 15px 0; border-bottom: 1px solid #e5e7eb;">
                                        <span style="color: #6b7280;">Profit Share Rate</span>
                                    </td>
                                    <td style="padding: 15px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">
                                        <span style="color: #374151; font-weight: 600;">{fee_rate_str}</span>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 15px 0;">
                                        <span style="color: #374151; font-weight: 700; font-size: 18px;">Amount Due</span>
                                    </td>
                                    <td style="padding: 15px 0; text-align: right;">
                                        <span style="color: #059669; font-weight: 700; font-size: 24px;">${amount:,.2f}</span>
                                    </td>
                                </tr>
                            </table>
                            
                            <!-- Payment Button -->
                            <div style="text-align: center; margin-bottom: 30px;">
                                <a href="{payment_url}" style="display: inline-block; background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: white; text-decoration: none; padding: 16px 40px; border-radius: 8px; font-weight: 600; font-size: 16px;">
                                    💳 Pay Now with Crypto
                                </a>
                            </div>
                            
                            <p style="margin: 0; color: #9ca3af; font-size: 13px; text-align: center;">
                                Payment accepted via USDC, USDT, BTC, or ETH
                            </p>
                            
                            <!-- Grace Period Notice -->
                            <div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; border-radius: 6px; margin-top: 25px;">
                                <p style="margin: 0; color: #92400e; font-size: 13px;">
                                    ⏰ <strong>Payment Due:</strong> Please pay within 7 days to keep your trading agent active.
                                </p>
                            </div>
                            
                            <!-- Dashboard Link -->
                            <div style="margin-top: 25px; text-align: center;">
                                <a href="{dashboard_link}" style="color: #667eea; text-decoration: none; font-size: 14px;">
                                    📊 View Your Dashboard →
                                </a>
                            </div>
                            
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="padding: 25px 30px; background: #f9fafb; text-align: center; border-top: 1px solid #e5e7eb;">
                            <p style="margin: 0; color: #6b7280; font-size: 12px;">
                                🚀 Nike Rocket | Questions? Reply to this email.
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
        """
        
        try:
            response = requests.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "from": FROM_EMAIL,
                    "to": [email],
                    "subject": f"🚀 Nike Rocket Invoice - ${amount:.2f} ({cycle_label})",
                    "html": html_content
                },
                timeout=30
            )
            
            if response.status_code == 200:
                self.logger.info(f"📧 Invoice email sent to {email}")
                return True
            else:
                self.logger.error(f"Email send failed: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Email error: {e}")
            return False
    
    def _send_reminder_email(
        self, email: str, api_key: str, amount: float, payment_url: str, days_remaining: int
    ) -> bool:
        """Send payment reminder email"""
        if not RESEND_API_KEY:
            return False
        
        urgency = "⚠️" if days_remaining <= 2 else "📧"
        
        html_content = f"""
<!DOCTYPE html>
<html>
<body style="margin: 0; padding: 20px; font-family: Arial, sans-serif; background: #f5f5f5;">
    <div style="max-width: 500px; margin: 0 auto; background: white; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
        <h2 style="color: #374151; margin-bottom: 20px;">{urgency} Payment Reminder</h2>
        
        <p style="color: #6b7280; line-height: 1.6;">
            Your Nike Rocket invoice for <strong>${amount:.2f}</strong> is still pending.
        </p>
        
        <p style="color: #dc2626; font-weight: 600;">
            ⏰ {days_remaining} day{'s' if days_remaining != 1 else ''} remaining before your trading agent is paused.
        </p>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="{payment_url}" style="display: inline-block; background: #10b981; color: white; text-decoration: none; padding: 14px 30px; border-radius: 8px; font-weight: 600;">
                Pay ${amount:.2f} Now
            </a>
        </div>
        
        <p style="color: #9ca3af; font-size: 12px; text-align: center;">
            Questions? Reply to this email.
        </p>
    </div>
</body>
</html>
        """
        
        try:
            response = requests.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "from": FROM_EMAIL,
                    "to": [email],
                    "subject": f"{urgency} Payment Reminder - ${amount:.2f} due in {days_remaining} days",
                    "html": html_content
                },
                timeout=30
            )
            return response.status_code == 200
        except:
            return False
    
    def _send_suspension_email(self, email: str, api_key: str, amount: float, payment_url: str) -> bool:
        """Send agent suspension notice"""
        if not RESEND_API_KEY:
            return False
        
        dashboard_link = f"{BASE_URL}/dashboard?key={api_key}"
        
        html_content = f"""
<!DOCTYPE html>
<html>
<body style="margin: 0; padding: 20px; font-family: Arial, sans-serif; background: #f5f5f5;">
    <div style="max-width: 500px; margin: 0 auto; background: white; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
        <h2 style="color: #dc2626; margin-bottom: 20px;">⏸️ Trading Agent Paused</h2>
        
        <p style="color: #6b7280; line-height: 1.6;">
            Your Nike Rocket trading agent has been paused due to an unpaid invoice 
            of <strong>${amount:.2f}</strong>.
        </p>
        
        <p style="color: #374151; font-weight: 600;">
            Pay now to resume trading immediately.
        </p>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="{payment_url}" style="display: inline-block; background: #dc2626; color: white; text-decoration: none; padding: 14px 30px; border-radius: 8px; font-weight: 600;">
                Pay ${amount:.2f} to Resume
            </a>
        </div>
        
        <p style="color: #9ca3af; font-size: 13px;">
            Once payment is confirmed, your trading agent will automatically resume.
        </p>
        
        <div style="margin-top: 20px; text-align: center;">
            <a href="{dashboard_link}" style="color: #667eea; text-decoration: none; font-size: 14px;">
                View Dashboard →
            </a>
        </div>
    </div>
</body>
</html>
        """
        
        try:
            response = requests.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "from": FROM_EMAIL,
                    "to": [email],
                    "subject": "⏸️ Nike Rocket - Trading Agent Paused (Unpaid Invoice)",
                    "html": html_content
                },
                timeout=30
            )
            return response.status_code == 200
        except:
            return False
    
    def _send_payment_confirmation_email(self, email: str, api_key: str, amount: float) -> bool:
        """Send payment confirmation email"""
        if not RESEND_API_KEY:
            return False
        
        dashboard_link = f"{BASE_URL}/dashboard?key={api_key}"
        
        html_content = f"""
<!DOCTYPE html>
<html>
<body style="margin: 0; padding: 20px; font-family: Arial, sans-serif; background: #f5f5f5;">
    <div style="max-width: 500px; margin: 0 auto; background: white; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
        <h2 style="color: #059669; margin-bottom: 20px;">✅ Payment Confirmed!</h2>
        
        <p style="color: #6b7280; line-height: 1.6;">
            Thank you! Your payment of <strong>${amount:.2f}</strong> has been received.
        </p>
        
        <p style="color: #374151;">
            Your trading agent is active and ready for the next 30-day cycle.
        </p>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="{dashboard_link}" style="display: inline-block; background: #10b981; color: white; text-decoration: none; padding: 14px 30px; border-radius: 8px; font-weight: 600;">
                View Dashboard
            </a>
        </div>
        
        <p style="color: #9ca3af; font-size: 12px; text-align: center;">
            🚀 Happy Trading!
        </p>
    </div>
</body>
</html>
        """
        
        try:
            response = requests.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "from": FROM_EMAIL,
                    "to": [email],
                    "subject": "✅ Nike Rocket - Payment Confirmed!",
                    "html": html_content
                },
                timeout=30
            )
            return response.status_code == 200
        except:
            return False
    
    # =========================================================================
    # BILLING VERIFICATION
    # =========================================================================

    async def verify_billing_accuracy(self, auto_fix: bool = False) -> Dict[str, Any]:
        """
        Verify that current_cycle_profit matches sum of trades for all users.

        This is a monitoring function to detect billing discrepancies.
        Run periodically to ensure data integrity.

        Args:
            auto_fix: If True, automatically fix discrepancies by recalculating
                     current_cycle_profit from trades table.

        Returns:
            Dict with verification results including any discrepancies found
        """
        self.logger.info("🔍 Verifying billing accuracy...")

        results = {
            "users_checked": 0,
            "discrepancies_found": 0,
            "discrepancies_fixed": 0,
            "total_discrepancy_amount": 0.0,
            "discrepancy_details": []
        }

        async with self.db_pool.acquire() as conn:
            # Query to find discrepancies between stored profit and calculated profit
            discrepancies = await conn.fetch("""
                SELECT
                    fu.id,
                    fu.email,
                    fu.api_key,
                    fu.current_cycle_profit,
                    fu.billing_cycle_start,
                    COALESCE(SUM(t.profit_usd), 0) as calculated_profit,
                    fu.current_cycle_profit - COALESCE(SUM(t.profit_usd), 0) as discrepancy
                FROM follower_users fu
                LEFT JOIN trades t ON t.user_id = fu.id
                    AND t.closed_at >= fu.billing_cycle_start
                WHERE fu.billing_cycle_start IS NOT NULL
                GROUP BY fu.id, fu.email, fu.api_key, fu.current_cycle_profit, fu.billing_cycle_start
                HAVING ABS(COALESCE(fu.current_cycle_profit, 0) - COALESCE(SUM(t.profit_usd), 0)) > 0.01
            """)

            # Count total users with active billing cycles
            total_users = await conn.fetchval("""
                SELECT COUNT(*) FROM follower_users WHERE billing_cycle_start IS NOT NULL
            """)
            results["users_checked"] = total_users or 0

            if discrepancies:
                results["discrepancies_found"] = len(discrepancies)

                for row in discrepancies:
                    discrepancy_info = {
                        "user_id": row['id'],
                        "email": row['email'][:3] + "***" if row['email'] else None,
                        "stored_profit": float(row['current_cycle_profit'] or 0),
                        "calculated_profit": float(row['calculated_profit'] or 0),
                        "discrepancy": float(row['discrepancy'] or 0),
                        "billing_cycle_start": row['billing_cycle_start'].isoformat() if row['billing_cycle_start'] else None
                    }
                    results["discrepancy_details"].append(discrepancy_info)
                    results["total_discrepancy_amount"] += abs(float(row['discrepancy'] or 0))

                    if auto_fix:
                        # Fix discrepancy by setting current_cycle_profit to calculated value
                        calculated = float(row['calculated_profit'] or 0)
                        await conn.execute("""
                            UPDATE follower_users
                            SET current_cycle_profit = $1
                            WHERE id = $2
                        """, calculated, row['id'])
                        results["discrepancies_fixed"] += 1
                        self.logger.warning(
                            f"🔧 Fixed discrepancy for user {row['id']}: "
                            f"${row['current_cycle_profit']:.2f} -> ${calculated:.2f}"
                        )
                    else:
                        self.logger.warning(
                            f"⚠️ Discrepancy found for user {row['id']}: "
                            f"stored=${row['current_cycle_profit']:.2f}, "
                            f"calculated=${row['calculated_profit']:.2f}, "
                            f"diff=${row['discrepancy']:.2f}"
                        )

                # Log to error_logs table for admin visibility
                if results["discrepancies_found"] > 0 and not auto_fix:
                    await log_error_to_db(
                        self.db_pool, "system", "BILLING_DISCREPANCY",
                        f"Found {results['discrepancies_found']} billing discrepancies totaling ${results['total_discrepancy_amount']:.2f}",
                        {"discrepancies": results["discrepancy_details"][:10]}  # Limit to first 10
                    )

        if results["discrepancies_found"] == 0:
            self.logger.info(f"✅ Billing verification complete: {results['users_checked']} users checked, no discrepancies found")
        else:
            action = "fixed" if auto_fix else "logged"
            self.logger.warning(
                f"⚠️ Billing verification complete: {results['discrepancies_found']} discrepancies {action} "
                f"(total ${results['total_discrepancy_amount']:.2f})"
            )

        return results

    # =========================================================================
    # ADMIN & REPORTING
    # =========================================================================

    async def get_billing_summary(self) -> Dict[str, Any]:
        """Get billing summary for admin dashboard"""
        async with self.db_pool.acquire() as conn:
            summary = await conn.fetchrow("""
                SELECT 
                    COUNT(*) FILTER (WHERE pending_invoice_id IS NOT NULL) as pending_invoices,
                    COALESCE(SUM(pending_invoice_amount) FILTER (WHERE pending_invoice_id IS NOT NULL), 0) as pending_amount,
                    COUNT(*) FILTER (WHERE billing_cycle_start IS NOT NULL) as active_cycles,
                    COALESCE(SUM(current_cycle_profit) FILTER (WHERE billing_cycle_start IS NOT NULL), 0) as current_cycle_total_profit
                FROM follower_users
                WHERE access_granted = true
            """)
            
            paid_summary = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_paid,
                    COALESCE(SUM(amount_usd), 0) as total_collected
                FROM billing_invoices
                WHERE status = 'paid'
            """)
            
            return {
                "pending_invoices": summary['pending_invoices'],
                "pending_amount": float(summary['pending_amount']),
                "active_cycles": summary['active_cycles'],
                "current_cycle_profit": float(summary['current_cycle_total_profit']),
                "total_invoices_paid": paid_summary['total_paid'],
                "total_collected": float(paid_summary['total_collected'])
            }
    
    async def change_user_tier(self, user_id: int, new_tier: str, immediate: bool = False) -> bool:
        """
        Change a user's fee tier
        
        Args:
            user_id: User ID
            new_tier: New tier ('team', 'vip', 'standard')
            immediate: If True, apply now. If False, apply at next cycle.
        """
        if new_tier not in get_valid_tiers():
            return False
        
        async with self.db_pool.acquire() as conn:
            if immediate:
                await conn.execute("""
                    UPDATE follower_users SET
                        fee_tier = $1,
                        next_cycle_fee_tier = NULL
                    WHERE id = $2
                """, new_tier, user_id)
            else:
                await conn.execute("""
                    UPDATE follower_users SET next_cycle_fee_tier = $1 WHERE id = $2
                """, new_tier, user_id)
            
            self.logger.info(f"Tier change for user {user_id}: {new_tier} ({'immediate' if immediate else 'next cycle'})")
            return True


# =============================================================================
# BACKGROUND SCHEDULER
# =============================================================================

async def billing_scheduler_v2(db_pool: asyncpg.Pool):
    """
    Background scheduler for 30-day rolling billing
    
    Runs hourly to:
    - Check for billing cycle endings
    - Check for overdue invoices
    - Send reminders
    - Process suspensions
    - Clean up old error logs (daily at midnight UTC)
    """
    logger.info("📅 Billing scheduler v2 started (30-day rolling)")
    
    billing = BillingServiceV2(db_pool)
    
    while True:
        try:
            now = utc_now()
            
            # Every hour: Check for cycle endings
            await billing.check_all_cycles()
            
            # Every 6 hours: Check for overdue invoices and verify billing accuracy
            if now.hour % 6 == 0:
                await billing.check_overdue_invoices()
                await billing.verify_billing_accuracy(auto_fix=False)
            
            # Daily at midnight UTC: Clean up old error logs
            if now.hour == 0:
                try:
                    from admin_dashboard import cleanup_old_errors
                    deleted = cleanup_old_errors(days=30)
                    if deleted > 0:
                        logger.info(f"🧹 Error log cleanup: removed {deleted} entries older than 30 days")
                except Exception as cleanup_err:
                    logger.warning(f"Error log cleanup failed: {cleanup_err}")
            
            # Sleep for 1 hour
            await asyncio.sleep(3600)
            
        except asyncio.CancelledError:
            logger.info("📅 Billing scheduler stopped")
            break
        except Exception as e:
            logger.error(f"❌ Billing scheduler error: {e}")
            import traceback
            traceback.print_exc()
            await log_error_to_db(
                db_pool, "system", "BILLING_SCHEDULER_ERROR",
                str(e), {"function": "billing_scheduler_v2", "traceback": traceback.format_exc()[:500]}
            )
            await asyncio.sleep(300)  # Wait 5 min on error


async def start_billing_scheduler_v2(db_pool: asyncpg.Pool):
    """Entry point for billing scheduler (call from main.py)"""
    await asyncio.sleep(60)  # Wait for app startup
    logger.info("💰 Starting billing scheduler v2...")
    await billing_scheduler_v2(db_pool)


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'BillingServiceV2',
    'billing_scheduler_v2', 
    'start_billing_scheduler_v2'
]
