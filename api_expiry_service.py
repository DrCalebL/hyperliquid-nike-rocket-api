"""
API Wallet Expiry Notification Service
========================================

Monitors API wallet expiration dates and sends email reminders.

Two systems:
1. VAULT LEADER BOT - Single API wallet controlled by admin
   - Expiry date stored in DB (system_settings table) or env var
   - Reminders sent to ADMIN_EMAIL

2. FOLLOWER AGENTS - Per-user API wallets
   - Expiry date stored in follower_users.api_wallet_expires_at
   - Reminders sent to each user's email

Reminder schedule: 30 days, 14 days, 7 days, 3 days, 1 day before expiry
"""

import os
import logging
import asyncio
import asyncpg
import aiohttp
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_API_URL = "https://api.resend.com/emails"
FROM_EMAIL = os.getenv("FROM_EMAIL", "$NIKEPIG's Massive Rocket <noreply@rocket.nikepig.com>")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "calebws87@gmail.com")
SITE_URL = os.getenv("SITE_URL", "https://rocket-hla.nikepig.com")

# Reminder thresholds (days before expiry)
REMINDER_DAYS = [30, 14, 7, 3, 1]

# Check interval (seconds) - once every 6 hours
CHECK_INTERVAL = 6 * 60 * 60


# ═══════════════════════════════════════════════════════════════════════════
# EMAIL TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════

def _vault_leader_email_html(days_left: int, expiry_date: str) -> str:
    urgency = "🔴 CRITICAL" if days_left <= 3 else "🟡 WARNING" if days_left <= 7 else "🔵 REMINDER"
    return f"""
    <div style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #667eea;">🚀 Nike Rocket - Vault API Wallet Expiry</h2>
        <div style="background: {'#fee2e2' if days_left <= 3 else '#fef3c7' if days_left <= 7 else '#dbeafe'}; 
                    border-radius: 8px; padding: 20px; margin: 20px 0;">
            <h3 style="margin-top: 0;">{urgency}: Vault Leader API Wallet expires in {days_left} day{'s' if days_left != 1 else ''}!</h3>
            <p><strong>Expiry Date:</strong> {expiry_date}</p>
            <p>Your vault trading bot's API wallet is expiring soon. Without renewal, the vault will <strong>stop trading</strong>.</p>
        </div>
        <h3>How to renew:</h3>
        <ol>
            <li>Go to <a href="https://app.hyperliquid.xyz/API">app.hyperliquid.xyz → More → API</a></li>
            <li>Remove the old API wallet</li>
            <li>Generate a new one and authorize for <strong>180 days</strong></li>
            <li>Update the private key in your vault bot environment</li>
            <li>Update the expiry date: <code>POST {SITE_URL}/admin/api-expiry/vault</code></li>
        </ol>
        <p style="color: #6b7280; font-size: 12px;">This is an automated reminder from noreply@rocket.nikepig.com.</p>
    </div>
    """


def _follower_expiry_email_html(days_left: int, expiry_date: str, user_email: str) -> str:
    urgency = "🔴 URGENT" if days_left <= 3 else "🟡 HEADS UP" if days_left <= 7 else "🔵 REMINDER"
    return f"""
    <div style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #667eea;">🚀 Nike Rocket - API Wallet Expiring Soon</h2>
        <div style="background: {'#fee2e2' if days_left <= 3 else '#fef3c7' if days_left <= 7 else '#dbeafe'}; 
                    border-radius: 8px; padding: 20px; margin: 20px 0;">
            <h3 style="margin-top: 0;">{urgency}: Your API wallet expires in {days_left} day{'s' if days_left != 1 else ''}!</h3>
            <p><strong>Expiry Date:</strong> {expiry_date}</p>
            <p>Your Hyperliquid API wallet is expiring soon. Without renewal, your trading agent will <strong>stop executing trades</strong>.</p>
        </div>
        <h3>How to renew (2 minutes):</h3>
        <ol>
            <li>Go to <a href="https://app.hyperliquid.xyz/API">app.hyperliquid.xyz → More → API</a></li>
            <li>Remove your old API wallet</li>
            <li>Generate a new one, name it (e.g. <code>nike_rocket</code>)</li>
            <li>Click <strong>"Authorize API Wallet"</strong> → set to <strong>180 days</strong></li>
            <li><strong>Copy the private key</strong> from the red box (shown only once!)</li>
            <li>Go to <a href="{SITE_URL}/setup">Nike Rocket Setup</a> and re-enter your credentials</li>
        </ol>
        <p style="color: #6b7280; font-size: 12px;">
            If you've already renewed, you can ignore this email.<br>
            Questions? Reply to this email or contact support.
        </p>
    </div>
    """


def _follower_expired_email_html(user_email: str) -> str:
    return f"""
    <div style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #667eea;">🚀 Nike Rocket - API Wallet Expired</h2>
        <div style="background: #fee2e2; border-radius: 8px; padding: 20px; margin: 20px 0;">
            <h3 style="margin-top: 0; color: #991b1b;">⛔ Your API wallet has expired</h3>
            <p>Your Hyperliquid API wallet has expired. Your trading agent has been <strong>paused</strong> and is no longer executing trades.</p>
        </div>
        <h3>To resume trading:</h3>
        <ol>
            <li>Go to <a href="https://app.hyperliquid.xyz/API">app.hyperliquid.xyz → More → API</a></li>
            <li>Generate a new API wallet and authorize for <strong>180 days</strong></li>
            <li>Go to <a href="{SITE_URL}/setup">Nike Rocket Setup</a> and re-enter your new credentials</li>
        </ol>
        <p>Your account, billing history, and settings are all preserved. You just need fresh API credentials.</p>
        <p style="color: #6b7280; font-size: 12px;">Questions? Reply to this email or contact support.</p>
    </div>
    """


# ═══════════════════════════════════════════════════════════════════════════
# EMAIL SENDER
# ═══════════════════════════════════════════════════════════════════════════

async def _send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send email via Resend API"""
    if not RESEND_API_KEY:
        logger.warning(f"⚠️ RESEND_API_KEY not set - skipping email to {to_email}")
        return False
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "from": FROM_EMAIL,
                    "to": [to_email],
                    "subject": subject,
                    "html": html_body
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status in (200, 201):
                    logger.info(f"📧 Expiry reminder sent to {to_email}: {subject}")
                    return True
                else:
                    text = await response.text()
                    logger.error(f"❌ Email failed ({response.status}): {text}")
                    return False
    except Exception as e:
        logger.error(f"❌ Email send error: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# VAULT LEADER EXPIRY CHECK
# ═══════════════════════════════════════════════════════════════════════════

async def _get_vault_expiry(conn) -> dict:
    """Get vault leader API wallet expiry from system_settings table"""
    try:
        row = await conn.fetchrow(
            "SELECT value FROM system_settings WHERE key = 'vault_api_expiry_date'"
        )
        if row:
            expiry_str = row['value']
            expiry_date = datetime.fromisoformat(expiry_str).replace(tzinfo=timezone.utc)
            return {"expiry_date": expiry_date, "source": "db"}
    except Exception as e:
        logger.debug(f"Could not read vault expiry from DB: {e}")
    
    # Fallback to env var
    env_expiry = os.getenv("VAULT_API_EXPIRY_DATE")
    if env_expiry:
        try:
            expiry_date = datetime.fromisoformat(env_expiry).replace(tzinfo=timezone.utc)
            return {"expiry_date": expiry_date, "source": "env"}
        except:
            pass
    
    return None


async def _get_vault_last_reminded(conn) -> dict:
    """Get last reminder days sent for vault"""
    try:
        row = await conn.fetchrow(
            "SELECT value FROM system_settings WHERE key = 'vault_api_last_reminder_days'"
        )
        if row:
            return int(row['value'])
    except:
        pass
    return None


async def _set_vault_last_reminded(conn, days: int):
    """Record which reminder was last sent"""
    try:
        await conn.execute("""
            INSERT INTO system_settings (key, value, updated_at)
            VALUES ('vault_api_last_reminder_days', $1, NOW())
            ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
        """, str(days))
    except Exception as e:
        logger.error(f"Failed to update vault reminder state: {e}")


async def check_vault_leader_expiry(conn):
    """Check vault leader bot API wallet expiry and send reminders"""
    vault_info = await _get_vault_expiry(conn)
    if not vault_info:
        return
    
    expiry_date = vault_info['expiry_date']
    now = datetime.now(timezone.utc)
    days_left = (expiry_date - now).days
    
    if days_left < 0:
        # Already expired!
        logger.error(f"🔴 VAULT API WALLET EXPIRED {abs(days_left)} days ago!")
        last_reminded = await _get_vault_last_reminded(conn)
        if last_reminded != -1:  # Only send expired notice once
            await _send_email(
                ADMIN_EMAIL,
                "🔴 CRITICAL: Vault API Wallet EXPIRED - Trading Stopped!",
                _vault_leader_email_html(0, expiry_date.strftime('%Y-%m-%d %H:%M UTC'))
            )
            await _set_vault_last_reminded(conn, -1)
        return
    
    # Check if we should send a reminder
    last_reminded = await _get_vault_last_reminded(conn)
    
    for threshold in REMINDER_DAYS:
        if days_left <= threshold:
            if last_reminded is None or last_reminded > threshold:
                logger.info(f"📧 Sending vault expiry reminder: {days_left} days left (threshold: {threshold})")
                await _send_email(
                    ADMIN_EMAIL,
                    f"{'🔴' if threshold <= 3 else '🟡' if threshold <= 7 else '🔵'} Vault API Wallet expires in {days_left} days",
                    _vault_leader_email_html(days_left, expiry_date.strftime('%Y-%m-%d %H:%M UTC'))
                )
                await _set_vault_last_reminded(conn, threshold)
            break


# ═══════════════════════════════════════════════════════════════════════════
# FOLLOWER AGENTS EXPIRY CHECK
# ═══════════════════════════════════════════════════════════════════════════

async def check_follower_expiry(conn):
    """Check all follower API wallet expiries and send reminders"""
    now = datetime.now(timezone.utc)
    
    # Get all active users with expiry dates
    rows = await conn.fetch("""
        SELECT id, email, api_wallet_expires_at, api_expiry_last_reminder_days, agent_active
        FROM follower_users
        WHERE credentials_set = true 
          AND api_wallet_expires_at IS NOT NULL
          AND access_granted = true
    """)
    
    for row in rows:
        user_id = row['id']
        email = row['email']
        expiry_date = row['api_wallet_expires_at']
        last_reminded = row['api_expiry_last_reminder_days']
        agent_active = row['agent_active']
        
        # Ensure timezone aware
        if expiry_date.tzinfo is None:
            expiry_date = expiry_date.replace(tzinfo=timezone.utc)
        
        days_left = (expiry_date - now).days
        
        if days_left < 0:
            # Expired - deactivate agent and notify once
            if last_reminded != -1:
                logger.warning(f"⛔ User {email}: API wallet expired {abs(days_left)} days ago - deactivating")
                await _send_email(
                    email,
                    "⛔ Nike Rocket: Your API Wallet Has Expired",
                    _follower_expired_email_html(email)
                )
                # Also notify admin
                await _send_email(
                    ADMIN_EMAIL,
                    f"📋 Follower API expired: {email}",
                    f"<p>User <strong>{email}</strong>'s API wallet expired on {expiry_date.strftime('%Y-%m-%d')}. Agent deactivated.</p>"
                )
                await conn.execute("""
                    UPDATE follower_users 
                    SET api_expiry_last_reminder_days = -1,
                        agent_active = false
                    WHERE id = $1
                """, user_id)
            continue
        
        # Check reminder thresholds
        for threshold in REMINDER_DAYS:
            if days_left <= threshold:
                if last_reminded is None or last_reminded > threshold:
                    logger.info(f"📧 Sending expiry reminder to {email}: {days_left} days left")
                    await _send_email(
                        email,
                        f"{'🔴' if threshold <= 3 else '🟡' if threshold <= 7 else '🔵'} Nike Rocket: API Wallet expires in {days_left} days",
                        _follower_expiry_email_html(days_left, expiry_date.strftime('%Y-%m-%d %H:%M UTC'), email)
                    )
                    await conn.execute("""
                        UPDATE follower_users 
                        SET api_expiry_last_reminder_days = $1
                        WHERE id = $2
                    """, threshold, user_id)
                break


# ═══════════════════════════════════════════════════════════════════════════
# MAIN SCHEDULER LOOP
# ═══════════════════════════════════════════════════════════════════════════

async def start_expiry_checker(db_pool):
    """Background task that checks API wallet expiry dates every 6 hours"""
    logger.info("⏰ API Expiry Checker started (checking every 6 hours)")
    
    # Wait 60 seconds for app to fully start
    await asyncio.sleep(60)
    
    while True:
        try:
            conn = await asyncpg.connect(dsn=_get_db_url())
            try:
                # Check vault leader
                await check_vault_leader_expiry(conn)
                
                # Check all followers
                await check_follower_expiry(conn)
                
                logger.info("✅ API expiry check completed")
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"❌ Expiry checker error: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)


def _get_db_url():
    """Get database URL"""
    db_url = os.getenv("DATABASE_URL", "")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return db_url


# ═══════════════════════════════════════════════════════════════════════════
# ADMIN API HELPERS
# ═══════════════════════════════════════════════════════════════════════════

async def set_vault_expiry_date(expiry_date_str: str) -> dict:
    """Set/update vault leader API wallet expiry date"""
    try:
        expiry_date = datetime.fromisoformat(expiry_date_str).replace(tzinfo=timezone.utc)
    except ValueError:
        return {"error": f"Invalid date format: {expiry_date_str}. Use ISO format like 2026-05-05T22:23:27"}
    
    now = datetime.now(timezone.utc)
    days_left = (expiry_date - now).days
    
    try:
        conn = await asyncpg.connect(dsn=_get_db_url())
        try:
            # Upsert expiry date
            await conn.execute("""
                INSERT INTO system_settings (key, value, updated_at)
                VALUES ('vault_api_expiry_date', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
            """, expiry_date.isoformat())
            
            # Reset reminder state so new reminders trigger correctly
            await conn.execute("""
                INSERT INTO system_settings (key, value, updated_at)
                VALUES ('vault_api_last_reminder_days', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
            """, str(999))  # Reset to high number so all thresholds re-trigger
            
        finally:
            await conn.close()
        
        return {
            "status": "success",
            "expiry_date": expiry_date.strftime('%Y-%m-%d %H:%M UTC'),
            "days_remaining": days_left,
            "reminders_reset": True
        }
    except Exception as e:
        return {"error": str(e)}


async def get_expiry_status() -> dict:
    """Get current expiry status for vault and all followers"""
    now = datetime.now(timezone.utc)
    
    try:
        conn = await asyncpg.connect(dsn=_get_db_url())
        try:
            # Vault status
            vault_info = await _get_vault_expiry(conn)
            vault_status = None
            if vault_info:
                days_left = (vault_info['expiry_date'] - now).days
                vault_status = {
                    "expiry_date": vault_info['expiry_date'].strftime('%Y-%m-%d %H:%M UTC'),
                    "days_remaining": days_left,
                    "status": "expired" if days_left < 0 else "critical" if days_left <= 3 else "warning" if days_left <= 14 else "ok",
                    "source": vault_info['source']
                }
            
            # Follower statuses
            rows = await conn.fetch("""
                SELECT email, api_wallet_expires_at, agent_active, api_expiry_last_reminder_days
                FROM follower_users
                WHERE credentials_set = true AND api_wallet_expires_at IS NOT NULL
                ORDER BY api_wallet_expires_at ASC
            """)
            
            followers = []
            for row in rows:
                expiry = row['api_wallet_expires_at']
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                days_left = (expiry - now).days
                followers.append({
                    "email": row['email'],
                    "expiry_date": expiry.strftime('%Y-%m-%d %H:%M UTC'),
                    "days_remaining": days_left,
                    "agent_active": row['agent_active'],
                    "status": "expired" if days_left < 0 else "critical" if days_left <= 3 else "warning" if days_left <= 14 else "ok",
                    "last_reminder": row['api_expiry_last_reminder_days']
                })
            
            return {
                "vault_leader": vault_status,
                "followers": followers,
                "total_followers_tracked": len(followers),
                "expiring_soon": len([f for f in followers if f['days_remaining'] <= 14 and f['days_remaining'] >= 0]),
                "expired": len([f for f in followers if f['days_remaining'] < 0]),
                "checked_at": now.strftime('%Y-%m-%d %H:%M UTC')
            }
        finally:
            await conn.close()
    except Exception as e:
        return {"error": str(e)}
