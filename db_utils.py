"""
Database Utilities with Retry Logic
====================================

Provides robust database connection handling with:
- Automatic retry on connection failures
- Exponential backoff
- Connection pool management
- Health checks
- Email notifications via Resend on failures

Usage:
    from db_utils import get_db_pool, db_execute, db_fetch, db_fetchrow

    # Get pool (creates if needed, with retry)
    pool = await get_db_pool()

    # Execute with automatic retry
    await db_execute("UPDATE users SET active = $1 WHERE id = $2", True, user_id)

    # Fetch with automatic retry
    rows = await db_fetch("SELECT * FROM users WHERE active = $1", True)
    row = await db_fetchrow("SELECT * FROM users WHERE id = $1", user_id)
"""

import asyncio
import logging
import os
from typing import Any, List, Optional
from functools import wraps
from datetime import datetime

import asyncpg
import aiohttp

from config import get_admin_email, utc_now

# Setup logging
logger = logging.getLogger("DB_UTILS")

# Configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # seconds
MAX_BACKOFF = 10.0  # seconds
POOL_MIN_SIZE = 2
POOL_MAX_SIZE = 10

# Database URL
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Resend email configuration
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_API_URL = "https://api.resend.com/emails"
FROM_EMAIL = os.getenv("FROM_EMAIL", "$NIKEPIG's Massive Rocket <noreply@rocket.nikepig.com>")
ADMIN_EMAIL = get_admin_email()

# Global pool reference
_db_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


async def notify_db_failure(error_type: str, error_message: str, context: Optional[dict] = None):
    """
    Send email notification via Resend for database failures.
    
    Args:
        error_type: Type of failure (e.g., "POOL_CREATION", "QUERY_TIMEOUT")
        error_message: Error details
        context: Additional context dict
    """
    if not RESEND_API_KEY:
        logger.warning("⚠️ RESEND_API_KEY not set - DB failure notification skipped")
        return
    
    subject = f"🚨 CRITICAL: DATABASE {error_type}"
    now = utc_now()
    
    # Build HTML rows
    html_rows = f"""
    <tr>
        <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f5f5f5;">Error Type</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{error_type}</td>
    </tr>
    <tr>
        <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f5f5f5;">Timestamp</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{now.strftime('%Y-%m-%d %H:%M:%S')} UTC</td>
    </tr>
    <tr>
        <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f5f5f5;">Error</td>
        <td style="padding: 8px; border: 1px solid #ddd; word-break: break-all;">{str(error_message)[:500]}</td>
    </tr>
    """
    
    if context:
        for key, value in context.items():
            html_rows += f"""
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f5f5f5;">{key.replace('_', ' ').title()}</td>
                <td style="padding: 8px; border: 1px solid #ddd; word-break: break-all;">{str(value)[:500]}</td>
            </tr>
            """
    
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <div style="border-left: 4px solid #e74c3c; padding-left: 15px; margin-bottom: 20px;">
            <h2 style="color: #e74c3c; margin: 0;">🚨 DATABASE CONNECTION FAILURE</h2>
            <p style="color: #666; margin: 5px 0;">Nike Rocket Alert - {now.strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
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
                    logger.info(f"✅ Email notification sent: DB {error_type}")
                else:
                    error_text = await resp.text()
                    logger.warning(f"⚠️ Resend API returned {resp.status}: {error_text}")
    except Exception as e:
        logger.error(f"❌ Failed to send email notification: {e}")


async def create_pool_with_retry() -> asyncpg.Pool:
    """
    Create database connection pool with retry logic.
    
    Uses exponential backoff on failures.
    """
    backoff = INITIAL_BACKOFF
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"🔌 Creating database pool (attempt {attempt}/{MAX_RETRIES})...")
            
            pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=POOL_MIN_SIZE,
                max_size=POOL_MAX_SIZE,
                command_timeout=30,
                # Connection health check
                setup=lambda conn: conn.execute("SELECT 1"),
            )
            
            # Test the pool
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            
            logger.info(f"✅ Database pool created successfully (size: {POOL_MIN_SIZE}-{POOL_MAX_SIZE})")
            return pool
            
        except Exception as e:
            last_error = e
            logger.warning(f"⚠️ Database pool creation failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES:
                logger.info(f"⏳ Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ Failed to create database pool after {MAX_RETRIES} attempts")
    
    # Notify admin
    await notify_db_failure(
        error_type="POOL_CREATION_FAILED",
        error_message=str(last_error),
        context={"attempts": MAX_RETRIES}
    )
    
    raise last_error


async def get_db_pool() -> asyncpg.Pool:
    """
    Get or create database connection pool.
    
    Thread-safe with lock to prevent multiple pool creation.
    """
    global _db_pool
    
    if _db_pool is not None:
        # Check if pool is still healthy
        try:
            async with _db_pool.acquire(timeout=5) as conn:
                await conn.execute("SELECT 1")
            return _db_pool
        except Exception as e:
            logger.warning(f"⚠️ Existing pool unhealthy, recreating: {e}")
            
            # Notify admin about pool health issue
            await notify_db_failure(
                error_type="POOL_UNHEALTHY",
                error_message=str(e),
                context={"action": "Attempting to recreate pool"}
            )
            try:
                await _db_pool.close()
            except:
                pass
            _db_pool = None
    
    async with _pool_lock:
        # Double-check after acquiring lock
        if _db_pool is None:
            _db_pool = await create_pool_with_retry()
        return _db_pool


async def close_db_pool():
    """Close the database pool gracefully."""
    global _db_pool
    if _db_pool is not None:
        logger.info("🔌 Closing database pool...")
        await _db_pool.close()
        _db_pool = None
        logger.info("✅ Database pool closed")


async def acquire_with_retry(pool: asyncpg.Pool, timeout: float = 10.0) -> asyncpg.Connection:
    """
    Acquire a connection from pool with retry logic.
    
    Note: Caller must release the connection!
    """
    backoff = INITIAL_BACKOFF
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = await asyncio.wait_for(
                pool.acquire(),
                timeout=timeout
            )
            return conn
            
        except asyncio.TimeoutError:
            last_error = TimeoutError(f"Connection acquire timed out after {timeout}s")
            logger.warning(f"⚠️ Connection acquire timeout (attempt {attempt}/{MAX_RETRIES})")
            
        except Exception as e:
            last_error = e
            logger.warning(f"⚠️ Connection acquire failed (attempt {attempt}/{MAX_RETRIES}): {e}")
        
        if attempt < MAX_RETRIES:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
    
    raise last_error


async def db_execute(query: str, *args, timeout: float = 30.0) -> str:
    """
    Execute a query with automatic retry.
    
    Args:
        query: SQL query string
        *args: Query parameters
        timeout: Query timeout in seconds
        
    Returns:
        Status string from execute
    """
    pool = await get_db_pool()
    backoff = INITIAL_BACKOFF
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with pool.acquire(timeout=10) as conn:
                result = await asyncio.wait_for(
                    conn.execute(query, *args),
                    timeout=timeout
                )
                return result
                
        except asyncio.TimeoutError:
            last_error = TimeoutError(f"Query timed out after {timeout}s")
            logger.warning(f"⚠️ Query timeout (attempt {attempt}/{MAX_RETRIES})")
            
        except asyncpg.PostgresConnectionError as e:
            last_error = e
            logger.warning(f"⚠️ Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
            
        except Exception as e:
            # Non-retryable errors (syntax, constraint violations, etc.)
            raise e
        
        if attempt < MAX_RETRIES:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ Query failed after {MAX_RETRIES} attempts: {query[:100]}...")
    
    # Notify admin
    await notify_db_failure(
        error_type="QUERY_EXECUTE_FAILED",
        error_message=str(last_error),
        context={"query": query[:200], "attempts": MAX_RETRIES}
    )
    
    raise last_error


async def db_fetch(query: str, *args, timeout: float = 30.0) -> List[asyncpg.Record]:
    """
    Fetch multiple rows with automatic retry.
    
    Args:
        query: SQL query string
        *args: Query parameters
        timeout: Query timeout in seconds
        
    Returns:
        List of Record objects
    """
    pool = await get_db_pool()
    backoff = INITIAL_BACKOFF
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with pool.acquire(timeout=10) as conn:
                result = await asyncio.wait_for(
                    conn.fetch(query, *args),
                    timeout=timeout
                )
                return result
                
        except asyncio.TimeoutError:
            last_error = TimeoutError(f"Query timed out after {timeout}s")
            logger.warning(f"⚠️ Query timeout (attempt {attempt}/{MAX_RETRIES})")
            
        except asyncpg.PostgresConnectionError as e:
            last_error = e
            logger.warning(f"⚠️ Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
            
        except Exception as e:
            raise e
        
        if attempt < MAX_RETRIES:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ Query failed after {MAX_RETRIES} attempts: {query[:100]}...")
    
    # Notify admin
    await notify_db_failure(
        error_type="QUERY_FETCH_FAILED",
        error_message=str(last_error),
        context={"query": query[:200], "attempts": MAX_RETRIES}
    )
    
    raise last_error


async def db_fetchrow(query: str, *args, timeout: float = 30.0) -> Optional[asyncpg.Record]:
    """
    Fetch single row with automatic retry.
    
    Args:
        query: SQL query string
        *args: Query parameters
        timeout: Query timeout in seconds
        
    Returns:
        Single Record or None
    """
    pool = await get_db_pool()
    backoff = INITIAL_BACKOFF
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with pool.acquire(timeout=10) as conn:
                result = await asyncio.wait_for(
                    conn.fetchrow(query, *args),
                    timeout=timeout
                )
                return result
                
        except asyncio.TimeoutError:
            last_error = TimeoutError(f"Query timed out after {timeout}s")
            logger.warning(f"⚠️ Query timeout (attempt {attempt}/{MAX_RETRIES})")
            
        except asyncpg.PostgresConnectionError as e:
            last_error = e
            logger.warning(f"⚠️ Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
            
        except Exception as e:
            raise e
        
        if attempt < MAX_RETRIES:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ Query failed after {MAX_RETRIES} attempts: {query[:100]}...")
    
    # Notify admin
    await notify_db_failure(
        error_type="QUERY_FETCHROW_FAILED",
        error_message=str(last_error),
        context={"query": query[:200], "attempts": MAX_RETRIES}
    )
    
    raise last_error


async def db_fetchval(query: str, *args, column: int = 0, timeout: float = 30.0) -> Any:
    """
    Fetch single value with automatic retry.
    
    Args:
        query: SQL query string
        *args: Query parameters
        column: Column index to return
        timeout: Query timeout in seconds
        
    Returns:
        Single value or None
    """
    pool = await get_db_pool()
    backoff = INITIAL_BACKOFF
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with pool.acquire(timeout=10) as conn:
                result = await asyncio.wait_for(
                    conn.fetchval(query, *args, column=column),
                    timeout=timeout
                )
                return result
                
        except asyncio.TimeoutError:
            last_error = TimeoutError(f"Query timed out after {timeout}s")
            logger.warning(f"⚠️ Query timeout (attempt {attempt}/{MAX_RETRIES})")
            
        except asyncpg.PostgresConnectionError as e:
            last_error = e
            logger.warning(f"⚠️ Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
            
        except Exception as e:
            raise e
        
        if attempt < MAX_RETRIES:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
    
    logger.error(f"❌ Query failed after {MAX_RETRIES} attempts: {query[:100]}...")
    
    # Notify admin
    await notify_db_failure(
        error_type="QUERY_FETCHVAL_FAILED",
        error_message=str(last_error),
        context={"query": query[:200], "attempts": MAX_RETRIES}
    )
    
    raise last_error


async def health_check() -> dict:
    """
    Check database health.
    
    Returns:
        Dict with health status
    """
    try:
        pool = await get_db_pool()
        
        async with pool.acquire(timeout=5) as conn:
            result = await conn.fetchval("SELECT 1")
            
        return {
            "status": "healthy",
            "pool_size": pool.get_size(),
            "pool_free": pool.get_idle_size(),
            "pool_used": pool.get_size() - pool.get_idle_size(),
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }
