"""
Order Utilities with Retry Logic & Email Notifications - Hyperliquid
=====================================================================

Provides robust order execution with:
- Automatic retry on failures (3 attempts, exponential backoff)
- Email notifications via Resend on failures
- Detailed logging

Hyperliquid SDK adaptation:
- entry: exchange.order() with IOC limit (market-like)
- TP: exchange.order() with trigger (limit, reduce_only)
- SL: exchange.order() with trigger (market, reduce_only)

Author: Nike Rocket Team
"""

import asyncio
import logging
import os
from typing import Any, Dict, Optional
from datetime import datetime

import aiohttp

# Setup logging
logger = logging.getLogger("ORDER_UTILS")

# Configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # seconds
MAX_BACKOFF = 8.0  # seconds

# Resend email configuration
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_API_URL = "https://api.resend.com/emails"
FROM_EMAIL = os.getenv("FROM_EMAIL", "$NIKEPIG's Massive Rocket <noreply@rocket.nikepig.com>")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "calebws87@gmail.com")


async def notify_admin(
    title: str,
    details: Dict[str, Any],
    level: str = "warning"  # "info", "warning", "error"
) -> bool:
    """
    Send notification to admin via Resend email.
    """
    if not RESEND_API_KEY:
        logger.warning("⚠️ RESEND_API_KEY not set - notification skipped")
        return False
    
    prefixes = {
        "info": "ℹ️ INFO",
        "warning": "⚠️ WARNING", 
        "error": "🚨 CRITICAL",
        "success": "✅ SUCCESS",
    }
    prefix = prefixes.get(level, prefixes["warning"])
    subject = f"{prefix}: {title}"
    
    html_rows = ""
    for key, value in details.items():
        html_rows += f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f5f5f5;">{key.replace('_', ' ').title()}</td>
            <td style="padding: 8px; border: 1px solid #ddd; word-break: break-all;">{value}</td>
        </tr>
        """
    
    colors = {
        "info": "#3498db",
        "warning": "#f39c12",
        "error": "#e74c3c",
        "success": "#2ecc71",
    }
    color = colors.get(level, colors["warning"])
    
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <div style="border-left: 4px solid {color}; padding-left: 15px; margin-bottom: 20px;">
            <h2 style="color: {color}; margin: 0;">{title}</h2>
            <p style="color: #666; margin: 5px 0;">Nike Rocket HL Alert - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        </div>
        
        <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
            {html_rows}
        </table>
        
        <p style="color: #999; font-size: 12px; margin-top: 20px;">
            This is an automated notification from noreply@rocket.nikepig.com.
        </p>
    </body>
    </html>
    """
    
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
                    "to": [ADMIN_EMAIL],
                    "subject": subject,
                    "html": html_body
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status in (200, 201):
                    logger.info(f"✅ Email notification sent: {title}")
                    return True
                else:
                    error_text = await resp.text()
                    logger.warning(f"⚠️ Resend API returned {resp.status}: {error_text}")
                    return False
                    
    except Exception as e:
        logger.error(f"❌ Failed to send email notification: {e}")
        return False


# ==================== HYPERLIQUID ORDER PLACEMENT WITH RETRY ====================


async def place_entry_order_with_retry(
    exchange,
    coin: str,
    is_buy: bool,
    quantity: float,
    limit_price: float,
    user_email: str = "unknown",
    user_api_key: str = "unknown"
) -> Optional[Dict]:
    """
    Place IOC entry order with retry (market-like execution with slippage protection).
    
    Hyperliquid: exchange.order(coin, is_buy, sz, limit_px, {"limit": {"tif": "Ioc"}})
    """
    backoff = INITIAL_BACKOFF
    last_error = None
    attempts_log = []
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"📝 Placing Entry IOC order (attempt {attempt}/{MAX_RETRIES})...")
            
            result = exchange.order(
                coin, is_buy, quantity, limit_price,
                {"limit": {"tif": "Ioc"}}
            )
            
            # Check result
            status = result.get("status", "")
            response = result.get("response", {})
            
            if status == "ok":
                statuses = response.get("data", {}).get("statuses", [])
                if statuses and "filled" in statuses[0]:
                    fill_info = statuses[0]["filled"]
                    order_id = fill_info.get("oid", "unknown")
                    avg_price = float(fill_info.get("avgPx", 0))
                    total_sz = float(fill_info.get("totalSz", 0))
                    logger.info(f"✅ Entry filled: oid={order_id}, avg_px=${avg_price:.4f}, sz={total_sz}")
                    return {
                        "id": str(order_id),
                        "avgPx": avg_price,
                        "totalSz": total_sz,
                        "raw": result
                    }
                elif statuses and "resting" in statuses[0]:
                    rest_info = statuses[0]["resting"]
                    order_id = rest_info.get("oid", "unknown")
                    logger.info(f"✅ Entry resting (IOC partial): oid={order_id}")
                    return {
                        "id": str(order_id),
                        "raw": result
                    }
                elif statuses and "error" in statuses[0]:
                    error_msg = statuses[0]["error"]
                    raise Exception(f"HL order error: {error_msg}")
                else:
                    # Unknown status but OK - treat as success
                    logger.warning(f"⚠️ Entry returned ok but unexpected statuses: {statuses}")
                    return {"id": "unknown", "raw": result}
            else:
                raise Exception(f"HL order failed: status={status}, response={response}")
            
        except Exception as e:
            last_error = e
            error_msg = str(e)[:200]
            attempts_log.append(f"Attempt {attempt}: {error_msg}")
            logger.warning(f"⚠️ Entry failed (attempt {attempt}/{MAX_RETRIES}): {error_msg}")
            
            if attempt < MAX_RETRIES:
                logger.info(f"⏳ Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
    
    # All retries exhausted
    logger.error(f"❌ Entry order FAILED after {MAX_RETRIES} attempts")
    await notify_admin(
        title="🚨 CRITICAL: Entry Order Failed (HL)",
        details={
            "User": user_email,
            "API Key": user_api_key[:20] + "..." if len(user_api_key) > 20 else user_api_key,
            "Coin": coin,
            "Side": "BUY" if is_buy else "SELL",
            "Quantity": quantity,
            "Limit Price": limit_price,
            "Attempts": "\n".join(attempts_log),
            "Final Error": str(last_error)[:500],
        },
        level="error"
    )
    return None


async def place_tp_order_with_retry(
    exchange,
    coin: str,
    is_buy: bool,
    quantity: float,
    tp_price: float,
    user_email: str = "unknown",
    user_api_key: str = "unknown"
) -> Optional[Dict]:
    """
    Place Take-Profit trigger order with retry.
    
    Hyperliquid: exchange.order(coin, is_buy, sz, tp_price,
        {"trigger": {"isMarket": False, "triggerPx": tp_price, "tpsl": "tp"}},
        reduce_only=True
    )
    """
    backoff = INITIAL_BACKOFF
    last_error = None
    attempts_log = []
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"📝 Placing TP order (attempt {attempt}/{MAX_RETRIES})...")
            
            result = exchange.order(
                coin, is_buy, quantity, tp_price,
                {"trigger": {"isMarket": False, "triggerPx": tp_price, "tpsl": "tp"}},
                reduce_only=True
            )
            
            status = result.get("status", "")
            response = result.get("response", {})
            
            if status == "ok":
                statuses = response.get("data", {}).get("statuses", [])
                if statuses and "resting" in statuses[0]:
                    rest_info = statuses[0]["resting"]
                    order_id = rest_info.get("oid", "unknown")
                    logger.info(f"✅ TP order placed: oid={order_id}")
                    return {"id": str(order_id), "raw": result}
                elif statuses and "error" in statuses[0]:
                    error_msg = statuses[0]["error"]
                    raise Exception(f"HL TP error: {error_msg}")
                else:
                    logger.warning(f"⚠️ TP returned ok but unexpected statuses: {statuses}")
                    return {"id": "unknown", "raw": result}
            else:
                raise Exception(f"HL TP failed: status={status}, response={response}")
                
        except Exception as e:
            last_error = e
            error_msg = str(e)[:200]
            attempts_log.append(f"Attempt {attempt}: {error_msg}")
            logger.warning(f"⚠️ TP failed (attempt {attempt}/{MAX_RETRIES}): {error_msg}")
            
            if attempt < MAX_RETRIES:
                logger.info(f"⏳ Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ TP order FAILED after {MAX_RETRIES} attempts")
    await notify_admin(
        title="🚨 DANGER: TP Order Failed (HL)",
        details={
            "Severity": "CRITICAL - Position may be UNPROTECTED",
            "User": user_email,
            "API Key": user_api_key[:20] + "..." if len(user_api_key) > 20 else user_api_key,
            "Coin": coin,
            "TP Price": tp_price,
            "Attempts": "\n".join(attempts_log),
            "Final Error": str(last_error)[:500],
        },
        level="error"
    )
    return None


async def place_sl_order_with_retry(
    exchange,
    coin: str,
    is_buy: bool,
    quantity: float,
    sl_price: float,
    user_email: str = "unknown",
    user_api_key: str = "unknown"
) -> Optional[Dict]:
    """
    Place Stop-Loss trigger order with retry.
    
    Hyperliquid: exchange.order(coin, is_buy, sz, sl_price,
        {"trigger": {"isMarket": True, "triggerPx": sl_price, "tpsl": "sl"}},
        reduce_only=True
    )
    """
    backoff = INITIAL_BACKOFF
    last_error = None
    attempts_log = []
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"📝 Placing SL order (attempt {attempt}/{MAX_RETRIES})...")
            
            result = exchange.order(
                coin, is_buy, quantity, sl_price,
                {"trigger": {"isMarket": True, "triggerPx": sl_price, "tpsl": "sl"}},
                reduce_only=True
            )
            
            status = result.get("status", "")
            response = result.get("response", {})
            
            if status == "ok":
                statuses = response.get("data", {}).get("statuses", [])
                if statuses and "resting" in statuses[0]:
                    rest_info = statuses[0]["resting"]
                    order_id = rest_info.get("oid", "unknown")
                    logger.info(f"✅ SL order placed: oid={order_id}")
                    return {"id": str(order_id), "raw": result}
                elif statuses and "error" in statuses[0]:
                    error_msg = statuses[0]["error"]
                    raise Exception(f"HL SL error: {error_msg}")
                else:
                    logger.warning(f"⚠️ SL returned ok but unexpected statuses: {statuses}")
                    return {"id": "unknown", "raw": result}
            else:
                raise Exception(f"HL SL failed: status={status}, response={response}")
                
        except Exception as e:
            last_error = e
            error_msg = str(e)[:200]
            attempts_log.append(f"Attempt {attempt}: {error_msg}")
            logger.warning(f"⚠️ SL failed (attempt {attempt}/{MAX_RETRIES}): {error_msg}")
            
            if attempt < MAX_RETRIES:
                logger.info(f"⏳ Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ SL order FAILED after {MAX_RETRIES} attempts")
    await notify_admin(
        title="🚨 DANGER: SL Order Failed (HL)",
        details={
            "Severity": "CRITICAL - Position UNPROTECTED",
            "User": user_email,
            "API Key": user_api_key[:20] + "..." if len(user_api_key) > 20 else user_api_key,
            "Coin": coin,
            "SL Price": sl_price,
            "Attempts": "\n".join(attempts_log),
            "Final Error": str(last_error)[:500],
        },
        level="error"
    )
    return None


async def cancel_order_with_retry(
    exchange,
    coin: str,
    order_id: int,
    order_description: str = "Order",
    user_email: str = "unknown",
    user_api_key: str = "unknown",
    notify_on_failure: bool = True
) -> bool:
    """
    Cancel an order with retry.
    
    Hyperliquid: exchange.cancel(coin, oid)
    """
    backoff = INITIAL_BACKOFF
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = exchange.cancel(coin, order_id)
            
            status = result.get("status", "")
            if status == "ok":
                logger.info(f"✅ {order_description} cancelled: oid={order_id}")
                return True
            else:
                raise Exception(f"Cancel failed: {result}")
                
        except Exception as e:
            last_error = e
            error_msg = str(e)[:200]
            
            # Order might already be filled/cancelled
            if "not found" in error_msg.lower() or "already" in error_msg.lower():
                logger.info(f"ℹ️ {order_description} already filled/cancelled")
                return True
            
            logger.warning(f"⚠️ Cancel failed (attempt {attempt}/{MAX_RETRIES}): {error_msg}")
            
            if attempt < MAX_RETRIES:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ Failed to cancel {order_description} after {MAX_RETRIES} attempts")
    
    if notify_on_failure:
        await notify_admin(
            title=f"⚠️ Failed to Cancel {order_description} (HL)",
            details={
                "User": user_email,
                "Order ID": order_id,
                "Coin": coin,
                "Error": str(last_error)[:500],
            },
            level="warning"
        )
    
    return False


# ==================== CRITICAL FAILURE NOTIFICATIONS ====================


async def notify_entry_failed(
    user_email: str, user_api_key: str, coin: str, side: str, quantity: float, error: str
):
    """Notify admin when entry order fails."""
    await notify_admin(
        title="🚨 CRITICAL: Entry Order Failed (HL)",
        details={
            "Severity": "HIGH - Position NOT opened",
            "User": user_email,
            "API Key": user_api_key[:20] + "...",
            "Coin": coin, "Side": side, "Quantity": quantity,
            "Error": error[:500],
            "Action Required": "Check user account manually on Hyperliquid",
        },
        level="error"
    )


async def notify_bracket_incomplete(
    user_email: str, user_api_key: str, coin: str, entry_order_id: str,
    tp_placed: bool, sl_placed: bool, error: str
):
    """Notify admin when TP/SL fails."""
    await notify_admin(
        title="🚨 DANGER: Bracket Order Incomplete (HL)",
        details={
            "Severity": "CRITICAL - Position UNPROTECTED",
            "User": user_email,
            "API Key": user_api_key[:20] + "...",
            "Coin": coin, "Entry Order": entry_order_id,
            "TP Placed": "✅ Yes" if tp_placed else "❌ NO",
            "SL Placed": "✅ Yes" if sl_placed else "❌ NO",
            "Error": error[:500],
            "Action Required": "IMMEDIATELY place missing orders manually on Hyperliquid!",
        },
        level="error"
    )


async def notify_signal_invalid(signal_id, symbol, action, missing_fields, reason):
    """Notify admin when signal is missing SL/TP."""
    await notify_admin(
        title="🚫 SIGNAL REJECTED - Missing SL/TP (HL)",
        details={
            "Signal ID": signal_id or "unknown", "Symbol": symbol or "unknown",
            "Action": action or "unknown",
            "Missing Fields": ", ".join(missing_fields) if missing_fields else "none",
            "Reason": reason, "Impact": "Trade NOT executed for ANY user",
        },
        level="error"
    )


async def notify_signal_invalid_values(signal_id, symbol, action, entry_price, stop_loss, take_profit, reason):
    """Notify admin when signal has invalid SL/TP values."""
    await notify_admin(
        title="🚫 SIGNAL REJECTED - Invalid SL/TP Values (HL)",
        details={
            "Signal ID": signal_id or "unknown", "Symbol": symbol or "unknown",
            "Action": action or "unknown", "Entry Price": entry_price,
            "Stop Loss": stop_loss, "Take Profit": take_profit, "Reason": reason,
        },
        level="error"
    )


async def notify_critical_error(error_type, error, location=None, user_api_key=None, context=None):
    """Notify admin for any critical/unhandled error."""
    details = {
        "Error Type": error_type,
        "Error": str(error)[:500],
        "Severity": "CRITICAL - Requires investigation",
    }
    if location:
        details["Location"] = location
    if user_api_key:
        details["User API Key"] = user_api_key[:20] + "..."
    if context:
        for k, v in list(context.items())[:5]:
            details[k] = str(v)[:200]
    
    await notify_admin(title=f"🚨 CRITICAL ERROR: {error_type} (HL)", details=details, level="error")


async def notify_security_alert(alert_type, details_dict=None, ip_address=None, user_agent=None):
    """Notify admin of potential security threats (SQL injection, etc)."""
    details = {
        "Alert Type": alert_type,
        "Severity": "🔴 SECURITY - Potential attack detected",
    }
    if ip_address:
        details["IP Address"] = ip_address
    if user_agent:
        details["User Agent"] = str(user_agent)[:200]
    if details_dict:
        for k, v in list(details_dict.items())[:5]:
            details[k] = str(v)[:200]

    await notify_admin(title=f"🛡️ SECURITY ALERT: {alert_type} (HL)", details=details, level="error")


async def notify_api_failure(service, endpoint, error, status_code=None, user_api_key=None, impact="Operation skipped"):
    """Notify admin when external API fails."""
    details = {"Service": service, "Endpoint": endpoint[:100], "Error": str(error)[:500], "Impact": impact}
    if status_code: details["Status Code"] = status_code
    if user_api_key: details["User API Key"] = user_api_key[:20] + "..."
    await notify_admin(title=f"🔌 API FAILURE: {service} (HL)", details=details, level="error")


async def notify_database_error(operation, error, table=None, user_api_key=None, query_snippet=None):
    """Notify admin when database operation fails."""
    details = {"Operation": operation, "Error": str(error)[:500], "Impact": "Data may not be saved"}
    if table: details["Table"] = table
    if user_api_key: details["User API Key"] = user_api_key[:20] + "..."
    if query_snippet: details["Query"] = query_snippet[:200]
    await notify_admin(title=f"🗄️ DATABASE ERROR: {operation} (HL)", details=details, level="error")
