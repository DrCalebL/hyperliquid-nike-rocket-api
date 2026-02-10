"""
Nike Rocket Follower System - Main API
=======================================
Updated main.py with hosted agents system + Admin Dashboard.
Includes automatic deposit/withdrawal detection via balance_checker.
Now includes 30-DAY ROLLING billing scheduler with Coinbase Commerce!

FIXED VERSION with startup_delay_seconds=30 to prevent race condition.
UPDATED: Added global exception handler for error logging.

Author: Nike Rocket Team
Updated: November 29, 2025 - WITH ERROR LOGGING
"""
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from typing import Optional
import json
import traceback
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from sqlalchemy import create_engine
import os
import asyncio
import asyncpg

# Import follower system
from follower_models import init_db
from follower_endpoints import router as follower_router

# Import portfolio system
from portfolio_models import init_portfolio_db
from portfolio_api import router as portfolio_router

# Import balance checker for automatic deposit/withdrawal detection
# FIXED: Using the version with startup delay support
from balance_checker import BalanceCheckerScheduler

# Import admin dashboard
from admin_dashboard import (
    get_all_users_with_status,
    get_recent_errors,
    get_stats_summary,
    get_positions_needing_review,
    get_users_by_tier,
    generate_admin_html,
    create_error_logs_table,
    ADMIN_PASSWORD
)

# Import hosted trading loop for automatic signal execution
from hosted_trading_loop import start_hosted_trading

# Import position monitor to track P&L when trades close
from position_monitor import start_position_monitor

# Import tax reports for income tracking
from tax_reports import (
    get_monthly_income,
    get_yearly_income,
    get_user_fees,
    get_earliest_trade_year,
    generate_monthly_csv,
    generate_yearly_csv,
    generate_user_fees_csv
)

# Import 30-day rolling billing service for automated invoicing
from billing_service_30day import BillingServiceV2, start_billing_scheduler_v2
from api_expiry_service import start_expiry_checker, set_vault_expiry_date, get_expiry_status

# Import billing API endpoints (webhooks, status)
from billing_endpoints_30day import router as billing_router

# Import trade reconciliation for backfilling historical trades
from trade_reconciliation import reconcile_single_user, reconcile_all_users

# Import notification functions for critical errors
from order_utils import notify_critical_error, notify_security_alert

# Initialize FastAPI
app = FastAPI(
    title="Nike Rocket Follower API",
    description="Trading signal distribution and profit tracking",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== GLOBAL EXCEPTION HANDLER ====================
# Catches ALL unhandled exceptions and logs them to error_logs table
# for visibility in the admin dashboard

async def log_error_to_db_global(api_key: str, error_type: str, error_message: str, context: dict = None):
    """Log error to error_logs table (used by global exception handler)"""
    try:
        db_url = os.getenv("DATABASE_URL")
        if db_url and db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        
        if not db_url:
            return
            
        conn = await asyncpg.connect(db_url)
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
        print(f"Failed to log error to DB: {e}")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler - catches ALL unhandled exceptions.
    Logs to error_logs table for admin dashboard visibility.
    """
    # Try to get API key from various sources
    api_key = (
        request.headers.get("X-API-Key") or 
        request.query_params.get("key") or 
        request.query_params.get("api_key") or
        "unknown"
    )
    
    # Get error details
    error_type = type(exc).__name__
    error_message = str(exc)
    tb = traceback.format_exc()
    
    # Log to console
    print(f"❌ UNHANDLED EXCEPTION: {error_type}: {error_message}")
    print(f"   Endpoint: {request.method} {request.url.path}")
    print(f"   Traceback: {tb[:500]}")
    
    # Log to database (async, non-blocking)
    try:
        await log_error_to_db_global(
            api_key,
            f"UNHANDLED_{error_type}",
            error_message,
            {
                "endpoint": str(request.url.path),
                "method": request.method,
                "traceback": tb[:500]
            }
        )
    except:
        pass  # Don't fail the response if logging fails
    
    # Send email notification for critical/security errors
    try:
        # Check for SQL injection patterns
        sql_patterns = ['DBMS_PIPE', 'waitfor', 'sleep(', 'benchmark(', 'UNION SELECT', 
                        'DROP TABLE', 'DELETE FROM', '--', ';--', 'xp_cmdshell']
        is_security_alert = any(p.lower() in error_message.lower() for p in sql_patterns)
        
        if is_security_alert:
            # Security alert - potential attack
            await notify_security_alert(
                alert_type="Potential SQL Injection",
                details_dict={
                    "Error": error_message[:200],
                    "Endpoint": str(request.url.path),
                    "Method": request.method,
                },
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent")
            )
        elif error_type not in ['ValueError', 'KeyError', 'AttributeError']:
            # Critical unhandled error (skip common validation errors)
            await notify_critical_error(
                error_type=f"UNHANDLED_{error_type}",
                error=error_message,
                location=f"{request.method} {request.url.path}",
                user_api_key=api_key if api_key != "unknown" else None,
                context={"traceback": tb[:200]}
            )
    except:
        pass  # Don't fail if notification fails
    
    # Return error response
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error_type": error_type}
    )


# ==================== END GLOBAL EXCEPTION HANDLER ====================

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL:
    engine = create_engine(DATABASE_URL)
    init_db(engine)
    init_portfolio_db(engine)
    
    # Run schema migrations BEFORE any ORM queries
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            ALTER TABLE follower_users 
            ADD COLUMN IF NOT EXISTS fee_tier VARCHAR(20) DEFAULT 'standard'
        """)
        
        # Create billing tables (used by asyncpg billing service)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS billing_cycles (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                cycle_start TIMESTAMP,
                cycle_end TIMESTAMP,
                cycle_number INTEGER DEFAULT 1,
                total_profit NUMERIC(20,8) DEFAULT 0,
                total_trades INTEGER DEFAULT 0,
                fee_tier VARCHAR(20) DEFAULT 'standard',
                fee_percentage NUMERIC(5,4) DEFAULT 0,
                fee_amount NUMERIC(20,8) DEFAULT 0,
                invoice_id TEXT,
                invoice_status VARCHAR(20),
                invoice_created_at TIMESTAMP,
                invoice_paid_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS billing_invoices (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                billing_cycle_id INTEGER,
                coinbase_charge_id TEXT,
                coinbase_charge_code TEXT,
                hosted_url TEXT,
                amount_usd NUMERIC(20,8) DEFAULT 0,
                profit_amount NUMERIC(20,8) DEFAULT 0,
                fee_tier VARCHAR(20),
                fee_percentage NUMERIC(5,4) DEFAULT 0,
                cycle_start TIMESTAMP,
                cycle_end TIMESTAMP,
                expires_at TIMESTAMP,
                status VARCHAR(20) DEFAULT 'pending',
                paid_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create error_logs table (used by global exception handler)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS error_logs (
                id SERIAL PRIMARY KEY,
                api_key TEXT,
                error_type TEXT,
                error_message TEXT,
                context TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create system_settings table (for vault API expiry tracking etc)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key VARCHAR(100) PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Add API wallet expiry columns to follower_users
        cur.execute("""
            ALTER TABLE follower_users 
            ADD COLUMN IF NOT EXISTS api_wallet_expires_at TIMESTAMP
        """)
        cur.execute("""
            ALTER TABLE follower_users 
            ADD COLUMN IF NOT EXISTS api_expiry_last_reminder_days INTEGER
        """)
        
        # Add last_known_balance for portfolio tracking
        cur.execute("""
            ALTER TABLE follower_users 
            ADD COLUMN IF NOT EXISTS last_known_balance NUMERIC
        """)
        
        # Add started_tracking_at for portfolio tracking
        cur.execute("""
            ALTER TABLE follower_users 
            ADD COLUMN IF NOT EXISTS started_tracking_at TIMESTAMP
        """)
        
        # Fix portfolio_transactions schema
        cur.execute("""
            ALTER TABLE portfolio_transactions 
            ADD COLUMN IF NOT EXISTS user_id VARCHAR
        """)
        cur.execute("""
            ALTER TABLE portfolio_transactions 
            ADD COLUMN IF NOT EXISTS transaction_type VARCHAR
        """)
        cur.execute("""
            ALTER TABLE portfolio_transactions 
            ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION
        """)
        cur.execute("""
            ALTER TABLE portfolio_transactions 
            ADD COLUMN IF NOT EXISTS detection_method VARCHAR
        """)
        conn.commit()
        
        # Drop NOT NULL on legacy columns that may still exist
        for col in ['type', 'amount_usd']:
            try:
                cur.execute(f"ALTER TABLE portfolio_transactions ALTER COLUMN {col} DROP NOT NULL")
                conn.commit()
            except Exception:
                conn.rollback()
        
        # Add unique constraint for ON CONFLICT in position_monitor
        try:
            cur.execute("""
                ALTER TABLE position_fills 
                ADD CONSTRAINT uq_position_fills_user_fill UNIQUE (user_id, fill_id)
            """)
            conn.commit()
        except Exception:
            conn.rollback()
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database schema up to date")
    except Exception as e:
        print(f"Note: Schema migration - {e}")
    
    print("✅ Database initialized")
else:
    print("⚠️ DATABASE_URL not set - database features disabled")

# Include routers
app.include_router(follower_router, tags=["follower"])
app.include_router(portfolio_router, tags=["portfolio"])
app.include_router(billing_router)  # 30-day rolling billing endpoints

# Global db_pool reference for billing endpoints
_db_pool = None

async def get_db_pool():
    """Get database pool for billing endpoints"""
    global _db_pool
    return _db_pool

# Health check
@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "$NIKEPIG's Massive Rocket API",
        "version": "1.1.0",
        "endpoints": {
            "signup": "/signup",
            "login": "/login",
            "setup": "/setup",
            "dashboard": "/dashboard",
            "admin": "/admin?password=xxx",
            "reset_database": "/admin/reset-database?password=xxx",
            "broadcast": "/api/broadcast-signal",
            "latest_signal": "/api/latest-signal",
            "report_pnl": "/api/report-pnl",
            "register": "/api/users/register",
            "verify": "/api/users/verify",
            "stats": "/api/users/stats",
            "agent_status": "/api/agent-status",
            "setup_agent": "/api/setup-agent",
            "stop_agent": "/api/stop-agent",
            "portfolio_stats": "/api/portfolio/stats",
            "portfolio_trades": "/api/portfolio/trades",
            "portfolio_deposit": "/api/portfolio/deposit",
            "portfolio_withdraw": "/api/portfolio/withdraw",
            "pay": "/api/pay/{api_key}",
            "webhook": "/api/payments/webhook",
            "billing_summary": "/api/admin/billing/summary",
            "process_billing": "/api/admin/billing/process-monthly",
            "verify_billing": "/api/admin/billing/verify-accuracy",
            "reconcile_trades": "/api/admin/reconcile-trades/{user_id}",
            "reconcile_all": "/api/admin/reconcile-all-trades"
        },
        "user_links": {
            "new_users": "Visit /signup to create an account",
            "returning_users": "Visit /login to access your dashboard"
        }
    }

@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"status": "healthy"}

# Test email notification endpoint
@app.get("/test-email")
async def test_email():
    """Test Resend email notifications for failsafe alerts"""
    from order_utils import notify_admin
    result = await notify_admin(
        title="🧪 Test Notification",
        details={
            "Status": "✅ Email notifications working!",
            "System": "Nike Rocket Failsafe Alerts",
            "Features": "DB retry, Order retry, Signal validation"
        },
        level="info"
    )
    return {"status": "sent" if result else "failed", "to": "calebws87@gmail.com"}

# Admin Dashboard (NEW!)
@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(password: str = ""):
    """
    Admin dashboard to monitor hosted follower agents
    
    Access: /admin?password=YOUR_ADMIN_PASSWORD
    
    Shows:
    - User signups
    - Setup completion rates
    - Active agents
    - Trading activity
    - Error logs
    """
    # Check password
    if password != ADMIN_PASSWORD:
        return HTMLResponse("""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>$NIKEPIG Admin Access</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        margin: 0;
                    }}
                    .login-box {{
                        background: white;
                        padding: 40px;
                        border-radius: 12px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                        text-align: center;
                        min-width: 300px;
                    }}
                    h1 {{
                        color: #667eea;
                        margin-bottom: 10px;
                        font-size: 24px;
                    }}
                    .subtitle {{
                        color: #666;
                        font-size: 14px;
                        margin-bottom: 25px;
                    }}
                    input {{
                        padding: 12px;
                        border: 2px solid #e5e7eb;
                        border-radius: 8px;
                        width: 100%;
                        font-size: 14px;
                        box-sizing: border-box;
                    }}
                    input:focus {{
                        outline: none;
                        border-color: #667eea;
                    }}
                    button {{
                        padding: 12px 24px;
                        background: #667eea;
                        color: white;
                        border: none;
                        border-radius: 8px;
                        font-weight: 600;
                        cursor: pointer;
                        margin-top: 15px;
                        width: 100%;
                        font-size: 14px;
                        transition: all 0.1s ease;
                        transform: translateY(0);
                        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    }}
                    button:hover {{
                        background: #5568d3;
                        transform: translateY(-1px);
                        box-shadow: 0 6px 8px rgba(0, 0, 0, 0.15);
                    }}
                    button:active {{
                        transform: translateY(2px);
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                    }}
                    .error {{
                        color: #ef4444;
                        margin-top: 15px;
                        font-size: 14px;
                    }}
                </style>
            </head>
            <body>
                <div class="login-box">
                    <h1>🔒 $NIKEPIG Admin</h1>
                    <p class="subtitle">Hosted Follower Agents Dashboard</p>
                    <form method="GET">
                        <input 
                            type="password" 
                            name="password" 
                            placeholder="Enter admin password" 
                            required 
                            autofocus
                        >
                        <button type="submit">Access Dashboard</button>
                    </form>
                    """ + (f"""<p class="error">❌ Invalid password</p>""" if password else "") + """
                </div>
            </body>
            </html>
        """)
    
    # Ensure error_logs table exists
    try:
        create_error_logs_table()
    except Exception as e:
        print(f"Note: Error logs table setup - {e}")
    
    # Get dashboard data
    try:
        users = get_all_users_with_status()
        errors = get_recent_errors(hours=None, limit=500)  # Get all errors, paginated
        stats = get_stats_summary()
        positions_review = get_positions_needing_review()
        users_by_tier = get_users_by_tier()
        
        # Generate and return HTML
        html = generate_admin_html(users, errors, stats, positions_review, users_by_tier)
        return HTMLResponse(html)
        
    except Exception as e:
        return HTMLResponse(f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Admin Dashboard Error</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        padding: 40px;
                        background: #f5f5f5;
                    }}
                    .error-box {{
                        background: white;
                        padding: 30px;
                        border-radius: 12px;
                        border-left: 4px solid #ef4444;
                        max-width: 600px;
                        margin: 0 auto;
                    }}
                    h1 {{ color: #ef4444; }}
                    code {{
                        background: #f9fafb;
                        padding: 2px 6px;
                        border-radius: 4px;
                        font-family: monospace;
                    }}
                </style>
            </head>
            <body>
                <div class="error-box">
                    <h1>⚠️ Dashboard Error</h1>
                    <p><strong>Error:</strong> {str(e)}</p>
                    <p>Make sure <code>admin_dashboard.py</code> is in your repo and DATABASE_URL is set.</p>
                </div>
            </body>
            </html>
        """)

# Database Reset Endpoint (NEW!)
@app.post("/admin/reset-database")
async def reset_database(password: str = ""):
    """
    DANGER ZONE: Reset entire database
    
    Deletes all data from all tables while preserving structure.
    Access: POST /admin/reset-database?password=YOUR_ADMIN_PASSWORD
    
    Returns:
        JSON with status and deleted row counts
    """
    
    # Check password
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Tables to clear (in dependency order - children first, parents last)
        tables = [
            'trades',
            'portfolio_trades',
            'portfolio_withdrawals',
            'portfolio_deposits',
            'error_logs',
            'agent_logs',
            'signal_deliveries',
            'signals',
            'payments',
            'follower_users',
            'portfolio_users',
            'users',
            'system_stats'
        ]
        
        deleted_counts = {}
        
        # Delete all data from each table
        for table in tables:
            try:
                # Count rows before deletion
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count_before = cur.fetchone()[0]
                
                # Delete all rows
                cur.execute(f"DELETE FROM {table}")
                
                deleted_counts[table] = {
                    'rows_deleted': count_before,
                    'status': 'success'
                }
                
                print(f"✅ Cleared {table}: {count_before} rows deleted")
                
            except Exception as e:
                deleted_counts[table] = {
                    'rows_deleted': 0,
                    'status': 'error',
                    'error': str(e)[:100]
                }
                print(f"⚠️ Error clearing {table}: {str(e)[:100]}")
        
        # Commit all deletions
        conn.commit()
        cur.close()
        conn.close()
        
        total_deleted = sum(
            t.get('rows_deleted', 0) 
            for t in deleted_counts.values() 
            if isinstance(t, dict)
        )
        
        print(f"🎉 Database reset complete! {total_deleted} total rows deleted")
        
        return {
            "status": "success",
            "message": f"🎉 Database reset complete! Deleted {total_deleted} rows",
            "deleted": deleted_counts,
            "tables_cleared": len([t for t in deleted_counts.values() if t.get('status') == 'success'])
        }
        
    except Exception as e:
        print(f"❌ Database reset failed: {str(e)}")
        return {
            "status": "error",
            "message": f"Database reset failed: {str(e)}",
            "error": str(e)
        }

@app.delete("/admin/delete-review-position/{position_id}")
async def delete_review_position(
    position_id: int,
    x_admin_key: Optional[str] = Header(None)
):
    """
    Delete a position from the review list
    
    Admin only endpoint to clean up positions that have been manually reviewed
    """
    # Verify admin authentication
    if x_admin_key != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Delete the position
        cur.execute(
            "DELETE FROM open_positions WHERE id = %s AND status = 'needs_review'",
            (position_id,)
        )
        
        rows_deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        
        if rows_deleted == 0:
            raise HTTPException(status_code=404, detail="Position not found or not in review status")
        
        return {"status": "success", "message": f"Position {position_id} deleted"}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/update-user-tier")
async def update_user_tier_endpoint(
    request: Request,
    x_admin_key: Optional[str] = Header(None)
):
    """
    Update a user's fee tier
    
    Admin only endpoint to change user fee tier:
    - team: 0% fees
    - vip: 5% fees
    - standard: 10% fees
    """
    print(f"[DEBUG] update-user-tier called")
    print(f"[DEBUG] x_admin_key header received: {x_admin_key[:10] if x_admin_key else 'None'}...")
    print(f"[DEBUG] Expected: {ADMIN_PASSWORD[:10]}...")
    
    # Verify admin authentication
    if x_admin_key != ADMIN_PASSWORD:
        print(f"[DEBUG] Auth failed - header doesn't match")
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        data = await request.json()
        user_id = data.get('user_id')
        new_tier = data.get('new_tier')
        print(f"[DEBUG] user_id={user_id}, new_tier={new_tier}")
        
        if not user_id or not new_tier:
            raise HTTPException(status_code=400, detail="Missing user_id or new_tier")
        
        if new_tier not in ['team', 'vip', 'standard']:
            raise HTTPException(status_code=400, detail="Invalid tier. Must be: team, vip, or standard")
        
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Update the user's tier
        cur.execute(
            "UPDATE follower_users SET fee_tier = %s WHERE id = %s",
            (new_tier, user_id)
        )
        
        rows_updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        
        if rows_updated == 0:
            raise HTTPException(status_code=404, detail="User not found")
        
        tier_names = {'team': 'Team (0%)', 'vip': 'VIP (5%)', 'standard': 'Standard (10%)'}
        return {"status": "success", "message": f"User moved to {tier_names[new_tier]}"}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== API WALLET EXPIRY ENDPOINTS ====================

@app.get("/admin/api-expiry/status")
async def admin_expiry_status(password: str = ""):
    """
    Get API wallet expiry status for vault leader and all followers.
    Shows days remaining, reminder state, and any expired wallets.
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    result = await get_expiry_status()
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@app.post("/admin/api-expiry/vault")
async def admin_set_vault_expiry(
    expiry_date: str,
    password: str = ""
):
    """
    Set/update vault leader API wallet expiry date.
    
    Args:
        expiry_date: ISO format date string, e.g. "2026-05-05T22:23:27"
        password: Admin password
    
    After renewing your vault API wallet on Hyperliquid, call this
    to update the tracked expiry date and reset reminders.
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    result = await set_vault_expiry_date(expiry_date)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.post("/admin/api-expiry/check-now")
async def admin_force_expiry_check(password: str = ""):
    """
    Force an immediate expiry check (normally runs every 6 hours).
    Use this to test email reminders after setting a near-future expiry date.
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    from api_expiry_service import check_vault_leader_expiry, check_follower_expiry, _get_db_url
    
    try:
        conn = await asyncpg.connect(dsn=_get_db_url())
        try:
            await check_vault_leader_expiry(conn)
            await check_follower_expiry(conn)
        finally:
            await conn.close()
        
        # Return current status too
        status = await get_expiry_status()
        return {
            "status": "success",
            "message": "Expiry check completed - emails sent if thresholds met",
            "current_status": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== TAX REPORTS ENDPOINTS ====================

@app.get("/admin/reports/monthly-csv")
async def download_monthly_csv(
    year: int,
    month: int,
    password: str = ""
):
    """
    Download monthly income report as CSV
    
    Query params:
        year: Year (e.g., 2025)
        month: Month (1-12)
        password: Admin password
    
    Returns CSV file for download
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        csv_content = generate_monthly_csv(year, month)
        filename = f"nike_rocket_income_{year}_{month:02d}.csv"
        
        from fastapi.responses import Response
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/reports/yearly-csv")
async def download_yearly_csv(
    year: int,
    password: str = ""
):
    """
    Download yearly income summary as CSV
    
    Query params:
        year: Year (e.g., 2025)
        password: Admin password
    
    Returns CSV file for download
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        csv_content = generate_yearly_csv(year)
        filename = f"nike_rocket_income_{year}_yearly.csv"
        
        from fastapi.responses import Response
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/reports/user-fees-csv")
async def download_user_fees_csv(
    start_date: str,
    end_date: str,
    password: str = ""
):
    """
    Download per-user fee breakdown as CSV
    
    Query params:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        password: Admin password
    
    Returns CSV file for download
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        csv_content = generate_user_fees_csv(start_date, end_date)
        filename = f"nike_rocket_user_fees_{start_date}_to_{end_date}.csv"
        
        from fastapi.responses import Response
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/reports/income-summary")
async def get_income_summary(
    year: int,
    password: str = ""
):
    """
    Get income summary data (for dashboard display)
    
    Query params:
        year: Year (e.g., 2025)
        password: Admin password
    
    Returns JSON with income data
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        data = get_yearly_income(year)
        return {
            "status": "success",
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/reports/available-years")
async def get_available_years(password: str = ""):
    """
    Get list of years with trade data
    
    Returns years from earliest trade to current year
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        from datetime import datetime
        current_year = datetime.now().year
        earliest_year = get_earliest_trade_year()
        
        # Generate list of years from earliest to current
        years = list(range(current_year, earliest_year - 1, -1))
        
        return {
            "status": "success",
            "years": years,
            "current_year": current_year,
            "earliest_year": earliest_year
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════
# BILLING ADMIN ENDPOINTS (30-DAY ROLLING)
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/admin/billing/check-cycles")
async def admin_check_billing_cycles(password: str = ""):
    """
    Manually trigger billing cycle check
    
    This will:
    1. Find users whose 30-day cycle has ended
    2. Generate Coinbase invoices for profitable cycles
    3. Start new cycles for all affected users
    
    Auth: Admin password required
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        billing = BillingServiceV2(db_pool)
        result = await billing.check_all_cycles()
        await db_pool.close()
        
        return {
            "status": "success",
            "message": "Checked all billing cycles",
            **result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/billing/check-overdue")
async def admin_check_overdue(password: str = ""):
    """Manually trigger overdue invoice check (reminders & suspensions)"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        billing = BillingServiceV2(db_pool)
        result = await billing.check_overdue_invoices()
        await db_pool.close()
        
        return {
            "status": "success",
            "message": "Checked overdue invoices",
            **result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.api_route("/api/admin/billing/verify-accuracy", methods=["GET", "POST"])
async def admin_verify_billing_accuracy(password: str = "", auto_fix: bool = False):
    """
    Verify billing accuracy by checking current_cycle_profit matches sum of trades.

    This endpoint runs the recommended monitoring query to detect discrepancies
    between stored profit values and calculated profit from the trades table.

    Args:
        password: Admin password
        auto_fix: If True, automatically fix discrepancies by recalculating

    Returns:
        Verification results including any discrepancies found
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        billing = BillingServiceV2(db_pool)
        result = await billing.verify_billing_accuracy(auto_fix=auto_fix)
        await db_pool.close()

        return {
            "status": "success" if result["discrepancies_found"] == 0 else "discrepancies_found",
            "message": f"Verified {result['users_checked']} users with active billing cycles",
            **result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/billing/summary")
async def admin_billing_summary(password: str = ""):
    """
    Get 30-day billing summary
    
    Returns:
    - Pending invoices count/amount
    - Active billing cycles
    - Current cycle total profit
    - Total collected lifetime
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        billing = BillingServiceV2(db_pool)
        summary = await billing.get_billing_summary()
        await db_pool.close()
        
        return {
            "status": "success",
            "billing_summary": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/billing/change-tier/{user_id}")
async def admin_change_user_tier(
    user_id: int, 
    tier: str,
    immediate: bool = False,
    password: str = ""
):
    """
    Change a user's fee tier
    
    Args:
        user_id: User ID
        tier: New tier ('team', 'vip', 'standard')
        immediate: If True, apply now. If False (default), apply at next cycle.
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    if tier not in ['team', 'vip', 'standard']:
        raise HTTPException(status_code=400, detail="Invalid tier. Must be: team, vip, standard")
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        billing = BillingServiceV2(db_pool)
        success = await billing.change_user_tier(user_id, tier, immediate)
        await db_pool.close()
        
        if success:
            return {
                "status": "success",
                "message": f"Tier changed to {tier}" + (" immediately" if immediate else " (effective next cycle)")
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to change tier")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/billing/waive-invoice/{user_id}")
async def admin_waive_invoice(user_id: int, password: str = ""):
    """Waive current pending invoice for a user"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        
        async with db_pool.acquire() as conn:
            # Clear pending invoice
            result = await conn.execute("""
                UPDATE follower_users
                SET 
                    pending_invoice_id = NULL,
                    pending_invoice_amount = 0,
                    invoice_due_date = NULL
                WHERE id = $1 AND pending_invoice_id IS NOT NULL
            """, user_id)
            
            if result == "UPDATE 0":
                await db_pool.close()
                return {
                    "status": "skipped",
                    "message": "No pending invoice for this user"
                }
            
            # Update billing cycle status
            await conn.execute("""
                UPDATE billing_cycles
                SET invoice_status = 'waived'
                WHERE user_id = $1 
                AND invoice_status = 'pending'
                ORDER BY id DESC LIMIT 1
            """, user_id)
        
        await db_pool.close()
        
        return {
            "status": "success",
            "message": f"Invoice waived for user {user_id}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/billing/restore-access/{user_id}")
async def admin_restore_access(user_id: int, password: str = ""):
    """Manually restore access for suspended user"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        billing = BillingServiceV2(db_pool)
        success = await billing.reactivate_after_payment(user_id)
        
        if not success:
            # Force restore even if not suspended for non-payment
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE follower_users
                    SET 
                        access_granted = true,
                        agent_active = true,
                        suspended_at = NULL,
                        suspension_reason = NULL
                    WHERE id = $1
                """, user_id)
        
        await db_pool.close()
        
        return {
            "status": "success",
            "message": f"Access restored for user {user_id}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/billing/user-cycles/{user_id}")
async def admin_get_user_cycles(user_id: int, password: str = ""):
    """Get billing cycle history for a user"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        
        async with db_pool.acquire() as conn:
            # Get user info
            user = await conn.fetchrow("""
                SELECT 
                    email, fee_tier, billing_cycle_start,
                    current_cycle_profit, current_cycle_trades,
                    pending_invoice_id, pending_invoice_amount
                FROM follower_users WHERE id = $1
            """, user_id)
            
            if not user:
                await db_pool.close()
                raise HTTPException(status_code=404, detail="User not found")
            
            # Get cycle history
            cycles = await conn.fetch("""
                SELECT 
                    cycle_number, cycle_start, cycle_end,
                    total_profit, total_trades,
                    fee_tier, fee_percentage, fee_amount,
                    invoice_status, invoice_paid_at
                FROM billing_cycles
                WHERE user_id = $1
                ORDER BY cycle_number DESC
                LIMIT 20
            """, user_id)
        
        await db_pool.close()
        
        return {
            "status": "success",
            "user": {
                "email": user['email'],
                "fee_tier": user['fee_tier'],
                "current_cycle_start": user['billing_cycle_start'].isoformat() if user['billing_cycle_start'] else None,
                "current_cycle_profit": float(user['current_cycle_profit'] or 0),
                "current_cycle_trades": int(user['current_cycle_trades'] or 0),
                "pending_invoice_amount": float(user['pending_invoice_amount'] or 0)
            },
            "cycles": [
                {
                    "cycle_number": c['cycle_number'],
                    "start": c['cycle_start'].isoformat(),
                    "end": c['cycle_end'].isoformat(),
                    "profit": float(c['total_profit']),
                    "trades": c['total_trades'],
                    "fee_tier": c['fee_tier'],
                    "fee_amount": float(c['fee_amount']),
                    "status": c['invoice_status'],
                    "paid_at": c['invoice_paid_at'].isoformat() if c['invoice_paid_at'] else None
                }
                for c in cycles
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════
# TRADE RECONCILIATION ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/admin/reconcile-trades/{user_id}")
async def admin_reconcile_user_trades(user_id: int, password: str = ""):
    """
    Reconcile trades for a specific user
    
    Reads closed trades from Hyperliquid history and backfills into portfolio_trades.
    Use this to fix missing P&L tracking for historical trades.
    
    Auth: Admin password required
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        await reconcile_single_user(user_id)
        return {
            "status": "success",
            "message": f"Trade reconciliation complete for user {user_id}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/reconcile-all-trades")
async def admin_reconcile_all_trades(password: str = ""):
    """
    Reconcile trades for ALL users
    
    Reads closed trades from Hyperliquid history and backfills into portfolio_trades.
    This may take several minutes for many users.
    
    Auth: Admin password required
    """
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        await reconcile_all_users()
        return {
            "status": "success",
            "message": "Trade reconciliation complete for all users"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== OPEN POSITIONS API ====================
# Returns current open positions for a user with TP/SL levels

@app.get("/api/portfolio/open-positions")
async def get_open_positions(request: Request):
    """
    Get all open positions for a user with TP/SL levels.
    Query params:
        - key: API key (required)
    """
    api_key = request.query_params.get('key') or request.headers.get('X-API-Key')
    
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    try:
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        # First validate the API key and get user_id
        user = await conn.fetchrow(
            "SELECT id FROM follower_users WHERE api_key = $1",
            api_key
        )
        
        if not user:
            await conn.close()
            raise HTTPException(status_code=401, detail="Invalid API key")
        
        user_id = user['id']
        
        # Get open positions from database
        rows = await conn.fetch("""
            SELECT 
                id,
                symbol,
                hl_coin,
                side,
                quantity,
                filled_quantity,
                leverage,
                avg_entry_price,
                entry_fill_price,
                target_tp,
                target_sl,
                opened_at,
                first_fill_at,
                last_fill_at,
                fill_count,
                status
            FROM open_positions
            WHERE user_id = $1 AND status = 'open'
            ORDER BY opened_at DESC
        """, user_id)
        
        positions = []
        for row in rows:
            positions.append({
                "id": row['id'],
                "symbol": row['symbol'],
                "hl_coin": row['hl_coin'],
                "side": row['side'],
                "quantity": float(row['quantity']) if row['quantity'] else 0,
                "filled_quantity": float(row['filled_quantity']) if row['filled_quantity'] else 0,
                "leverage": float(row['leverage']) if row['leverage'] else 1,
                "avg_entry_price": float(row['avg_entry_price']) if row['avg_entry_price'] else 0,
                "entry_fill_price": float(row['entry_fill_price']) if row['entry_fill_price'] else 0,
                "target_tp": float(row['target_tp']) if row['target_tp'] else None,
                "target_sl": float(row['target_sl']) if row['target_sl'] else None,
                "opened_at": (row['opened_at'].isoformat() + 'Z') if row['opened_at'] else None,
                "first_fill_at": (row['first_fill_at'].isoformat() + 'Z') if row['first_fill_at'] else None,
                "last_fill_at": (row['last_fill_at'].isoformat() + 'Z') if row['last_fill_at'] else None,
                "fill_count": row['fill_count'] or 1,
                "status": row['status']
            })
        
        await conn.close()
        
        return {
            "status": "success",
            "positions": positions,
            "count": len(positions)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        await log_error_to_db_global(
            api_key[:20] + "..." if api_key else "unknown",
            "OPEN_POSITIONS_ERROR",
            str(e),
            {"traceback": traceback.format_exc()[:500]}
        )
        raise HTTPException(status_code=500, detail=str(e))


# Live prices endpoint with server-side caching (rate-limit safe)
from price_cache import price_cache

# Shared Hyperliquid Info instance for public price data (no auth needed)
_shared_info = None

def get_shared_info():
    """Get or create shared Hyperliquid Info for public price data"""
    global _shared_info
    if _shared_info is None:
        from hyperliquid.info import Info
        from hyperliquid.utils import constants
        use_testnet = os.getenv("USE_TESTNET", "false").lower() == "true"
        base_url = constants.TESTNET_API_URL if use_testnet else constants.MAINNET_API_URL
        _shared_info = Info(base_url, skip_ws=True)
    return _shared_info

@app.get("/api/prices")
async def get_live_prices(symbols: str = ""):
    """
    Get live prices for coins (server-side cached, rate-limit safe).
    
    Uses Hyperliquid all_mids() - single API call for ALL coins.
    
    Query params:
        symbols: Comma-separated list of coins (e.g., "ADA,BTC,ETH")
    
    Returns:
        {"prices": {"ADA": 0.3605, "BTC": 104500.0}, "cached": true}
    """
    if not symbols:
        return {"prices": {}, "cached": False}
    
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return {"prices": {}, "cached": False}
    
    results = {}
    any_fetched = False
    
    try:
        # Check cache for each symbol
        to_fetch = []
        for symbol in symbol_list:
            cached = price_cache.get(symbol)
            if cached is not None:
                results[symbol] = cached
            else:
                to_fetch.append(symbol)
        
        # Fetch missing prices from Hyperliquid (single API call)
        if to_fetch:
            info = get_shared_info()
            all_mids = await asyncio.to_thread(info.all_mids)
            for symbol in to_fetch:
                price = float(all_mids.get(symbol, 0))
                if price > 0:
                    price_cache.set(symbol, price)
                    results[symbol] = price
                    any_fetched = True
                else:
                    results[symbol] = None
        
        return {
            "prices": results,
            "cached": not any_fetched
        }
        
    except Exception as e:
        print(f"❌ Price fetch error: {e}")
        return {"prices": {}, "error": str(e)}


# Serve static background images (NEW!)
@app.get("/static/backgrounds/{filename}")
async def get_background(filename: str):
    """Serve background images for performance cards"""
    filepath = f"backgrounds/{filename}"
    if os.path.exists(filepath):
        return FileResponse(filepath)
    else:
        raise HTTPException(status_code=404, detail="Background image not found")

# Serve all static files (images, etc.)
@app.get("/static/{filename}")
async def get_static_file(filename: str):
    """Serve static files (og-preview.png, logos, etc.)"""
    filepath = f"static/{filename}"
    if os.path.exists(filepath):
        return FileResponse(filepath)
    else:
        raise HTTPException(status_code=404, detail="Static file not found")

# Signup page
@app.get("/signup", response_class=HTMLResponse)
async def signup_page():
    """Serve the signup HTML page"""
    try:
        with open("signup.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>Signup page not found</h1><p>Please contact support.</p>",
            status_code=404
        )

# Setup page (NEW!)
@app.get("/setup", response_class=HTMLResponse)
async def setup_page():
    """Setup page for configuring trading agent"""
    try:
        with open("setup.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>Setup page not found</h1><p>Please contact support.</p>",
            status_code=404
        )

# Login page for returning users (NEW!)
@app.get("/login", response_class=HTMLResponse)
@app.get("/access", response_class=HTMLResponse)
async def login_page():
    """Login page for returning users to access their dashboard"""
    try:
        with open("login.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse("""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Access Dashboard - $NIKEPIG's Massive Rocket</title>
                <style>
                    * { margin: 0; padding: 0; box-sizing: border-box; }
                    body {
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        padding: 20px;
                    }
                    .container {
                        background: white;
                        border-radius: 16px;
                        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
                        max-width: 500px;
                        width: 100%;
                        overflow: hidden;
                    }
                    .header {
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        padding: 40px 30px;
                        text-align: center;
                    }
                    .header h1 {
                        color: white;
                        font-size: 28px;
                        margin-bottom: 8px;
                    }
                    .header p {
                        color: rgba(255,255,255,0.9);
                        font-size: 14px;
                    }
                    .content {
                        padding: 40px 30px;
                    }
                    .welcome {
                        text-align: center;
                        margin-bottom: 30px;
                    }
                    .welcome h2 {
                        color: #374151;
                        font-size: 24px;
                        margin-bottom: 10px;
                    }
                    .welcome p {
                        color: #6b7280;
                        font-size: 14px;
                    }
                    .form-group {
                        margin-bottom: 20px;
                    }
                    label {
                        display: block;
                        color: #374151;
                        font-weight: 600;
                        margin-bottom: 8px;
                        font-size: 14px;
                    }
                    input {
                        width: 100%;
                        padding: 14px;
                        border: 2px solid #e5e7eb;
                        border-radius: 8px;
                        font-size: 14px;
                        font-family: 'Courier New', monospace;
                        transition: border-color 0.2s;
                    }
                    input:focus {
                        outline: none;
                        border-color: #667eea;
                    }
                    .button {
                        width: 100%;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        border: none;
                        padding: 16px;
                        border-radius: 8px;
                        font-size: 16px;
                        font-weight: 600;
                        cursor: pointer;
                        transition: transform 0.1s, box-shadow 0.1s;
                        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    }
                    .button:hover {
                        transform: translateY(-2px);
                        box-shadow: 0 6px 12px rgba(102, 126, 234, 0.4);
                    }
                    .button:active {
                        transform: translateY(2px);
                        box-shadow: 0 2px 4px rgba(102, 126, 234, 0.2);
                    }
                    .help-box {
                        background: #f9fafb;
                        border-left: 4px solid #667eea;
                        padding: 15px;
                        border-radius: 8px;
                        margin-top: 20px;
                    }
                    .help-box p {
                        color: #6b7280;
                        font-size: 13px;
                        margin: 0 0 10px 0;
                    }
                    .help-box p:last-child {
                        margin: 0;
                    }
                    .new-user-link {
                        text-align: center;
                        margin-top: 20px;
                        padding-top: 20px;
                        border-top: 1px solid #e5e7eb;
                    }
                    .new-user-link a {
                        color: #667eea;
                        text-decoration: none;
                        font-weight: 600;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🚀 $NIKEPIG's Massive Rocket</h1>
                        <p>Access Your Trading Dashboard</p>
                    </div>
                    
                    <div class="content">
                        <div class="welcome">
                            <h2>👋 Welcome Back!</h2>
                            <p>Enter your API key to access your dashboard</p>
                        </div>
                        
                        <form onsubmit="event.preventDefault(); window.location.href='/dashboard?key='+document.getElementById('apiKey').value">
                            <div class="form-group">
                                <label for="apiKey">Your API Key</label>
                                <input 
                                    type="text" 
                                    id="apiKey" 
                                    name="apiKey" 
                                    placeholder="nk_..." 
                                    required
                                    autocomplete="off"
                                >
                            </div>
                            
                            <button type="submit" class="button">
                                🔓 Access Dashboard
                            </button>
                        </form>
                        
                        <div class="help-box">
                            <p><strong>💡 Where to find your API key:</strong></p>
                            <p>• Check the welcome email sent to your inbox</p>
                            <p>• Your API key starts with "nk_"</p>
                            <p>• If you lost it, contact support to recover your account</p>
                        </div>
                        
                        <div class="new-user-link">
                            <p style="color: #6b7280; font-size: 14px; margin-bottom: 8px;">
                                Don't have an account yet?
                            </p>
                            <a href="/signup">🚀 Sign Up Now - It's Free!</a>
                        </div>
                    </div>
                </div>
            </body>
            </html>
        """, status_code=200)

# Portfolio Dashboard (USER-FRIENDLY VERSION) - COMPLETE HTML!
@app.get("/dashboard", response_class=HTMLResponse)
async def portfolio_dashboard(request: Request):
    """Portfolio tracking dashboard with API key input"""
    
    # Get API key from query parameter (optional)
    api_key = request.query_params.get('key', '')
    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>$NIKEPIG's Massive Rocket - Portfolio Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        /* API Key Login Screen */
        .login-screen {{
            max-width: 500px;
            margin: 100px auto;
            background: white;
            padding: 40px;
            border-radius: 16px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.2);
        }}
        
        .login-screen h1 {{
            color: #667eea;
            text-align: center;
            margin-bottom: 10px;
            font-size: 32px;
        }}
        
        .login-screen p {{
            text-align: center;
            color: #6b7280;
            margin-bottom: 30px;
        }}
        
        .input-group {{
            margin-bottom: 20px;
        }}
        
        .input-group label {{
            display: block;
            margin-bottom: 8px;
            color: #374151;
            font-weight: 600;
        }}
        
        .input-group input {{
            width: 100%;
            padding: 12px;
            border: 2px solid #e5e7eb;
            border-radius: 8px;
            font-size: 16px;
        }}
        
        .input-group input:focus {{
            outline: none;
            border-color: #667eea;
        }}
        
        .btn {{
            width: 100%;
            padding: 14px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.1s ease;
            transform: translateY(0);
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}
        
        .btn:hover {{
            background: #5568d3;
            transform: translateY(-1px);
            box-shadow: 0 6px 8px rgba(0, 0, 0, 0.15);
        }}
        
        .btn:active {{
            transform: translateY(2px);
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }}
        
        .btn:disabled {{
            background: #9ca3af;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }}
        
        /* Setup Wizard */
        .setup-wizard {{
            max-width: 600px;
            margin: 50px auto;
            background: white;
            padding: 40px;
            border-radius: 16px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.2);
        }}
        
        .setup-wizard h2 {{
            color: #667eea;
            margin-bottom: 10px;
        }}
        
        .setup-wizard p {{
            color: #6b7280;
            margin-bottom: 20px;
        }}
        
        /* Dashboard */
        .hero {{
            text-align: center;
            color: white;
            padding: 40px 20px;
            margin-bottom: 40px;
        }}
        
        .hero h1 {{
            font-size: 48px;
            font-weight: 700;
            margin-bottom: 20px;
        }}
        
        .period-selector {{
            margin: 20px 0;
        }}
        
        .period-selector select {{
            padding: 12px 24px;
            font-size: 16px;
            border-radius: 25px;
            border: 2px solid rgba(255,255,255,0.3);
            background: rgba(255,255,255,0.1);
            color: white;
            cursor: pointer;
            font-weight: 600;
        }}
        
        .period-selector option {{
            background: #764ba2;
            color: white;
        }}
        
        .hero-profit {{
            font-size: 72px;
            font-weight: 800;
            margin: 20px 0;
            text-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }}
        
        .hero-label {{
            font-size: 24px;
            opacity: 0.9;
        }}
        
        .hero-subtext {{
            font-size: 16px;
            opacity: 0.7;
            margin-top: 10px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        
        .stat-card {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            position: relative;
            cursor: help;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .stat-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(0,0,0,0.15);
        }}
        
        .stat-card .tooltip {{
            visibility: hidden;
            opacity: 0;
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: #1f2937;
            color: white;
            padding: 12px 16px;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 400;
            white-space: normal;
            width: 250px;
            text-align: left;
            z-index: 1000;
            margin-bottom: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            transition: opacity 0.2s, visibility 0.2s;
            line-height: 1.5;
        }}
        
        .stat-card .tooltip::after {{
            content: '';
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            border: 8px solid transparent;
            border-top-color: #1f2937;
        }}
        
        .stat-card:hover .tooltip {{
            visibility: visible;
            opacity: 1;
        }}
        
        .tooltip-formula {{
            background: rgba(255,255,255,0.1);
            padding: 6px 10px;
            border-radius: 4px;
            margin-top: 8px;
            font-family: monospace;
            font-size: 12px;
        }}
        
        .stat-label {{
            font-size: 14px;
            color: #6b7280;
            margin-bottom: 8px;
        }}
        
        .stat-value {{
            font-size: 32px;
            font-weight: 700;
            color: #1f2937;
        }}
        
        .stat-detail {{
            font-size: 12px;
            color: #9ca3af;
            margin-top: 4px;
        }}
        
        .error {{
            background: #fee2e2;
            color: #991b1b;
            padding: 20px;
            border-radius: 12px;
            margin: 20px 0;
            text-align: center;
        }}
        
        .success {{
            background: #d1fae5;
            color: #065f46;
            padding: 20px;
            border-radius: 12px;
            margin: 20px 0;
            text-align: center;
        }}
        
        .logout-btn {{
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 10px 20px;
            background: rgba(255,255,255,0.2);
            color: white;
            border: 2px solid white;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.1s ease;
            transform: translateY(0);
        }}
        
        .logout-btn:hover {{
            background: rgba(255,255,255,0.3);
            transform: translateY(-1px);
        }}
        
        .logout-btn:active {{
            transform: translateY(2px);
        }}
        .agent-status-container {{
            margin: 20px 0;
            padding: 0;
        }}
        
        .agent-status {{
            padding: 16px 24px;
            border-radius: 12px;
            font-size: 16px;
            font-weight: 500;
            text-align: center;
            transition: all 0.3s ease;
            border: 2px solid transparent;
        }}
        
        .status-active {{
            background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
            color: #065f46;
            border-color: #10b981;
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.2);
        }}
        
        .status-configuring {{
            background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
            color: #1e40af;
            border-color: #3b82f6;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
        }}
        
        .status-ready {{
            background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
            color: #92400e;
            border-color: #f59e0b;
            box-shadow: 0 4px 12px rgba(245, 158, 11, 0.2);
        }}
        
        .status-error {{
            background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%);
            color: #991b1b;
            border-color: #ef4444;
            box-shadow: 0 4px 12px rgba(239, 68, 68, 0.2);
        }}
        
        .status-unknown {{
            background: linear-gradient(135deg, #f3f4f6 0%, #e5e7eb 100%);
            color: #4b5563;
            border-color: #9ca3af;
            box-shadow: 0 4px 12px rgba(156, 163, 175, 0.2);
        }}
        
        .agent-status a {{
            color: inherit;
            text-decoration: underline;
            font-weight: 600;
        }}
        
        .agent-status a:hover {{
            opacity: 0.8;
        }}
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* Agent Status Monitoring Styles */
        /* ═══════════════════════════════════════════════════════════════ */
        
        /* Global Tactile Button Effect - applies to ALL buttons */
        button, .tactile-btn {{
            transition: all 0.1s ease !important;
            transform: translateY(0);
        }}
        
        button:hover, .tactile-btn:hover {{
            transform: translateY(-1px);
        }}
        
        button:active, .tactile-btn:active {{
            transform: translateY(3px) !important;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1) !important;
        }}
        
        button:disabled {{
            transform: none !important;
        }}
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* Mobile Responsive Styles */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 15px;
            margin-bottom: 20px;
        }}
        
        .section-header-actions {{
            display: flex;
            gap: 10px;
            align-items: center;
            flex-shrink: 0;
        }}
        
        .export-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
            
            .section-header {{
                flex-direction: column;
                align-items: stretch;
            }}
            
            .section-header-actions {{
                width: 100%;
                justify-content: flex-start;
                flex-wrap: wrap;
            }}
            
            .export-grid {{
                grid-template-columns: 1fr;
            }}
            
            .logout-btn {{
                position: static !important;
                display: block;
                width: 100%;
                margin-bottom: 15px;
                text-align: center;
            }}
            
            .portfolio-overview,
            .transaction-history,
            .trade-export {{
                padding: 20px !important;
            }}
            
            .portfolio-overview h2,
            .transaction-history h2,
            .trade-export h2 {{
                font-size: 20px !important;
            }}
        }}
        
        /* Safety Section Styles */
        .safety-section {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border-radius: 16px;
            padding: 30px;
            margin-top: 30px;
            border: 1px solid rgba(102, 126, 234, 0.3);
        }}

        .safety-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 20px;
            cursor: pointer;
        }}

        .safety-header h2 {{
            color: #fff;
            font-size: 24px;
            display: flex;
            align-items: center;
            gap: 12px;
            margin: 0;
        }}

        .safety-header .toggle-icon {{
            color: #667eea;
            font-size: 20px;
            transition: transform 0.3s;
        }}

        .safety-header.collapsed .toggle-icon {{
            transform: rotate(-90deg);
        }}

        .safety-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            transition: all 0.3s ease;
        }}

        .safety-grid.hidden {{
            display: none;
        }}

        .safety-card {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 24px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            transition: transform 0.2s, border-color 0.2s;
        }}

        .safety-card:hover {{
            transform: translateY(-2px);
            border-color: rgba(102, 126, 234, 0.5);
        }}

        .safety-card .icon {{
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
            margin-bottom: 16px;
        }}

        .safety-card h3 {{
            color: #fff;
            font-size: 18px;
            margin-bottom: 12px;
        }}

        .safety-card p {{
            color: #9ca3af;
            font-size: 14px;
            line-height: 1.6;
            margin-bottom: 12px;
        }}

        .safety-card ul {{
            list-style: none;
            padding: 0;
            margin: 0;
        }}

        .safety-card li {{
            color: #9ca3af;
            font-size: 13px;
            padding: 6px 0;
            display: flex;
            align-items: flex-start;
            gap: 8px;
        }}

        .safety-card li::before {{
            content: "✓";
            color: #10b981;
            font-weight: bold;
            flex-shrink: 0;
        }}

        .github-link {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            background: #24292e;
            color: #fff;
            padding: 10px 20px;
            border-radius: 8px;
            text-decoration: none;
            font-size: 14px;
            font-weight: 600;
            margin-top: 12px;
            transition: background 0.2s;
        }}

        .github-link:hover {{
            background: #3a3f47;
        }}

        .github-link svg {{
            width: 20px;
            height: 20px;
        }}

        .fee-tiers {{
            display: flex;
            gap: 12px;
            margin-top: 12px;
        }}

        .fee-tier {{
            flex: 1;
            text-align: center;
            padding: 12px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 8px;
        }}

        .fee-tier .tier-name {{
            color: #667eea;
            font-weight: 600;
            font-size: 12px;
            text-transform: uppercase;
            margin-bottom: 4px;
        }}

        .fee-tier .tier-rate {{
            color: #fff;
            font-size: 20px;
            font-weight: bold;
        }}

        .tech-badges {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 12px;
        }}

        .tech-badge {{
            background: rgba(102, 126, 234, 0.2);
            color: #a5b4fc;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 500;
        }}

        /* Backtest Results Section */
        .backtest-section {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border-radius: 16px;
            padding: 24px;
            margin-top: 30px;
            margin-bottom: 30px;
            border: 1px solid rgba(16, 185, 129, 0.3);
        }}

        .backtest-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0 0 16px 0;
            margin-bottom: 20px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            cursor: pointer;
        }}

        .backtest-header h2 {{
            color: #fff;
            font-size: 20px;
            margin: 0;
        }}

        .backtest-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }}

        @media (max-width: 700px) {{
            .backtest-grid {{
                grid-template-columns: 1fr;
            }}
        }}

        .backtest-card {{
            background: rgba(255, 255, 255, 0.06);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            transition: transform 0.2s, border-color 0.2s;
        }}

        .backtest-card:hover {{
            transform: translateY(-2px);
            border-color: rgba(16, 185, 129, 0.4);
        }}

        .backtest-card .market-badge {{
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            margin-bottom: 12px;
            letter-spacing: 0.5px;
        }}

        .backtest-card .market-badge.bull {{
            background: rgba(16, 185, 129, 0.25);
            color: #34d399;
        }}

        .backtest-card .market-badge.bear {{
            background: rgba(239, 68, 68, 0.25);
            color: #f87171;
        }}

        .backtest-card .market-badge.sideways {{
            background: rgba(251, 191, 36, 0.25);
            color: #fcd34d;
        }}

        .backtest-card .market-badge.full {{
            background: rgba(139, 92, 246, 0.25);
            color: #c4b5fd;
        }}

        .backtest-card .period {{
            color: #9ca3af;
            font-size: 13px;
            margin-bottom: 8px;
        }}

        .backtest-card .result {{
            font-size: 32px;
            font-weight: bold;
            margin-bottom: 4px;
            color: #10b981;
        }}

        .backtest-card .result.profit {{
            color: #10b981;
        }}

        .backtest-card .final-value {{
            color: #e5e7eb;
            font-size: 16px;
            margin-bottom: 14px;
            font-weight: 500;
        }}

        .backtest-card .stats {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
            padding-top: 14px;
            border-top: 1px solid rgba(255, 255, 255, 0.1);
        }}

        .backtest-card .stat {{
            text-align: center;
            padding: 8px;
            background: rgba(0, 0, 0, 0.2);
            border-radius: 8px;
        }}

        .backtest-card .stat-label {{
            color: #9ca3af;
            font-size: 10px;
            text-transform: uppercase;
            margin-bottom: 4px;
            letter-spacing: 0.5px;
        }}

        .backtest-card .stat-value {{
            color: #fff;
            font-size: 15px;
            font-weight: 700;
        }}

        .backtest-card .stat-value.negative {{
            color: #f87171;
        }}

        .backtest-card .stat-value.positive {{
            color: #34d399;
        }}

        .backtest-disclaimer {{
            margin-top: 20px;
            padding: 14px 18px;
            background: rgba(251, 191, 36, 0.15);
            border: 1px solid rgba(251, 191, 36, 0.3);
            border-radius: 10px;
            font-size: 13px;
            color: #fcd34d;
            line-height: 1.5;
        }}

        .max-pain-section {{
            margin-top: 24px;
            padding: 24px;
            background: rgba(239, 68, 68, 0.08);
            border: 1px solid rgba(239, 68, 68, 0.25);
            border-radius: 12px;
        }}

        .max-pain-section h3 {{
            color: #f87171;
            font-size: 18px;
            margin: 0 0 20px 0;
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .max-pain-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 20px;
        }}

        @media (max-width: 900px) {{
            .max-pain-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
        }}

        @media (max-width: 500px) {{
            .max-pain-grid {{
                grid-template-columns: 1fr;
            }}
        }}

        .pain-stat {{
            text-align: center;
            padding: 20px 16px;
            background: rgba(0, 0, 0, 0.25);
            border-radius: 10px;
            border: 1px solid rgba(239, 68, 68, 0.15);
        }}

        .pain-stat .pain-value {{
            font-size: 36px;
            font-weight: bold;
            color: #f87171;
            margin-bottom: 6px;
        }}

        .pain-stat .pain-label {{
            color: #e5e7eb;
            font-size: 13px;
            font-weight: 600;
            text-transform: uppercase;
            margin-bottom: 10px;
            letter-spacing: 0.5px;
        }}

        .pain-stat .pain-explanation {{
            color: #9ca3af;
            font-size: 12px;
            line-height: 1.6;
        }}

        .max-pain-summary {{
            padding: 18px 20px;
            background: rgba(0, 0, 0, 0.25);
            border-radius: 10px;
            color: #d1d5db;
            font-size: 14px;
            line-height: 1.7;
            border: 1px solid rgba(255, 255, 255, 0.08);
        }}

        .max-pain-summary strong {{
            color: #fff;
        }}
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
</head>
<body>
    <div class="container">
        <!-- Login Screen -->
        <div id="login-screen" class="login-screen">
            <h1>🚀 $NIKEPIG's Massive Rocket</h1>
            <p>Portfolio Performance Tracker</p>
            
            <div class="input-group">
                <label for="api-key-input">Enter Your API Key:</label>
                <input 
                    type="text" 
                    id="api-key-input" 
                    placeholder="nk_..." 
                    value="{api_key}"
                >
            </div>
            
            <button class="btn" onclick="login()">View Dashboard</button>
            
            <div id="login-error" style="display: none;"></div>
        </div>
        
        <!-- Setup Wizard -->
        <div id="setup-wizard" class="setup-wizard" style="display: none;">
            <h2>🎯 Welcome to $NIKEPIG's Massive Rocket!</h2>
            <p>We'll automatically detect your Hyperliquid balance and start tracking your performance!</p>
            
            <div style="background: #f0f9ff; border-left: 4px solid #3b82f6; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <div style="color: #1e40af; font-weight: 600; margin-bottom: 5px;">📊 Auto-Detection</div>
                <div style="color: #1e40af; font-size: 14px;">
                    We'll query your current Hyperliquid balance and use it as your starting capital. 
                    Make sure your trading agent is set up first!
                </div>
            </div>
            
            <button class="btn" onclick="initializePortfolio()">Start Tracking</button>
            
            <div id="setup-message" style="display: none;"></div>
        </div>
        
        <!-- Dashboard -->
        <div id="dashboard" style="display: none;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <button class="refresh-btn" onclick="refreshDashboard()" style="
                    padding: 12px 24px;
                    background: linear-gradient(135deg, #10b981 0%, #059669 100%);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    font-weight: 600;
                    cursor: pointer;
                    font-size: 14px;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    transition: all 0.1s ease;
                    box-shadow: 0 4px 6px rgba(16, 185, 129, 0.3);
                ">
                    🔄 Refresh Dashboard
                </button>
                <button class="logout-btn" onclick="logout()">Change API Key</button>
            </div>
            
            <!-- Agent Status Display -->
            <div class="agent-status-container">
                <div id="agent-status-display" class="agent-status status-unknown">
                    ⏳ Checking agent status...
                </div>
            </div>
            
            <!-- Portfolio Overview Section (NEW!) -->
            <div class="portfolio-overview" style="
                background: white;
                border-radius: 12px;
                padding: 30px;
                margin-bottom: 30px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            ">
                <h2 style="margin: 0 0 20px 0; color: #667eea; font-size: 24px;">
                    💰 Portfolio Overview
                </h2>
                
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 25px;">
                    <div class="overview-card">
                        <div style="color: #6b7280; font-size: 14px; margin-bottom: 5px;" title="Total portfolio value including unrealized P&amp;L from all open positions">Current Value <span style="font-size: 11px; opacity: 0.7;">ℹ️</span></div>
                        <div id="current-value" style="font-size: 32px; font-weight: bold; color: #10b981;">$0</div>
                        <div style="font-size: 10px; color: #9ca3af; margin-top: 3px;">Includes unrealized P&amp;L • Updates every 60 mins</div>
                    </div>
                    
                    <div class="overview-card">
                        <div style="color: #6b7280; font-size: 14px; margin-bottom: 5px;">Initial Capital</div>
                        <div id="initial-capital-display" style="font-size: 28px; font-weight: 600; color: #374151;">$0</div>
                    </div>
                    
                    <div class="overview-card">
                        <div style="color: #6b7280; font-size: 14px; margin-bottom: 5px;">Net Deposits</div>
                        <div id="net-deposits" style="font-size: 28px; font-weight: 600; color: #3b82f6;">$0</div>
                    </div>
                    
                    <div class="overview-card">
                        <div style="color: #6b7280; font-size: 14px; margin-bottom: 5px;" title="Realized P&amp;L from closed signal trades only. Excludes unrealized P&amp;L and manual trades.">Total Profit <span style="font-size: 11px; opacity: 0.7;">ℹ️</span></div>
                        <div id="total-profit-overview" style="font-size: 28px; font-weight: 600; color: #10b981;">$0</div>
                        <div style="font-size: 10px; color: #9ca3af; margin-top: 3px;">Realized from closed signal trades</div>
                    </div>
                </div>
                
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; padding: 15px; background: #f9fafb; border-radius: 8px;">
                    <div>
                        <div style="font-size: 13px; color: #6b7280;">Funding/Deposits</div>
                        <div id="total-deposits" style="font-size: 18px; font-weight: 600; color: #10b981;">+$0</div>
                    </div>
                    <div>
                        <div style="font-size: 13px; color: #6b7280;">Fees/Funding/Withdrawals</div>
                        <div id="total-withdrawals" style="font-size: 18px; font-weight: 600; color: #ef4444;">-$0</div>
                    </div>
                    <div>
                        <div style="font-size: 13px; color: #6b7280;">Total Capital</div>
                        <div id="total-capital" style="font-size: 18px; font-weight: 600; color: #374151;">$0</div>
                    </div>
                    <div>
                        <div style="font-size: 13px; color: #6b7280;">Last Balance Check</div>
                        <div id="last-check" style="font-size: 14px; color: #6b7280;">—</div>
                    </div>
                </div>
            </div>
            
            <!-- Agent Control Section (NEW!) -->
            <div class="agent-control" style="
                background: white;
                border-radius: 12px;
                padding: 30px;
                margin-bottom: 30px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            ">
                <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 20px;">
                    <div>
                        <h2 style="margin: 0 0 10px 0; color: #667eea; font-size: 24px;">
                            🤖 Trading Agent Control
                        </h2>
                        <div style="display: flex; align-items: center; gap: 10px;">
                            <div style="font-size: 14px; color: #6b7280;">Status:</div>
                            <div id="agent-status-badge" style="
                                padding: 4px 12px;
                                border-radius: 12px;
                                font-size: 13px;
                                font-weight: 600;
                                background: #fee2e2;
                                color: #991b1b;
                            ">
                                Checking...
                            </div>
                        </div>
                        <div id="agent-details" style="font-size: 13px; color: #6b7280; margin-top: 5px;">
                            <!-- Agent details will load here -->
                        </div>
                    </div>
                    
                    <div style="display: flex; gap: 10px;">
                        <button id="start-agent-btn" onclick="startAgent()" style="
                            padding: 12px 24px;
                            background: #10b981;
                            color: white;
                            border: none;
                            border-radius: 8px;
                            font-weight: 600;
                            cursor: pointer;
                            transition: all 0.1s ease;
                            display: none;
                            box-shadow: 0 4px 6px rgba(16, 185, 129, 0.3);
                        ">
                            ▶️ Start Agent
                        </button>
                        
                        <button id="stop-agent-btn" onclick="stopAgent()" style="
                            padding: 12px 24px;
                            background: #ef4444;
                            color: white;
                            border: none;
                            border-radius: 8px;
                            font-weight: 600;
                            cursor: pointer;
                            transition: all 0.1s ease;
                            display: none;
                            box-shadow: 0 4px 6px rgba(239, 68, 68, 0.3);
                        ">
                            ⏸️ Stop Agent
                        </button>
                    </div>
                </div>
                
                <div id="agent-message" style="
                    margin-top: 15px;
                    padding: 12px;
                    border-radius: 6px;
                    display: none;
                "></div>
            </div>
            
            <div class="hero">
                <h1>🚀 $NIKEPIG'S MASSIVE ROCKET PERFORMANCE</h1>
                
                <div class="period-selector">
                    <select id="period-selector" onchange="changePeriod()">
                        <option value="7d">Last 7 Days</option>
                        <option value="30d" selected>Last 30 Days</option>
                        <option value="90d">Last 90 Days</option>
                        <option value="1y">Last 1 Year</option>
                        <option value="all">All-Time</option>
                    </select>
                </div>
                
                <div class="hero-profit" id="total-profit">$0</div>
                <div class="hero-label" id="profit-label">Total Profit</div>
                <div style="font-size: 12px; color: rgba(255,255,255,0.6); margin-top: 5px;">Realized P&amp;L from closed signal trades only</div>
                <div class="hero-subtext" id="time-tracking">Trading since...</div>
                
                <!-- Social Sharing Buttons (NEW!) -->
                <div style="margin: 30px 0;">
                    <div style="display: flex; gap: 15px; justify-content: center; flex-wrap: wrap; margin-bottom: 20px;">
                        <button onclick="showBackgroundSelectorForTwitter()" style="
                            padding: 12px 24px;
                            background: #1DA1F2;
                            color: white;
                            border: none;
                            border-radius: 8px;
                            font-weight: 600;
                            cursor: pointer;
                            font-size: 14px;
                            display: flex;
                            align-items: center;
                            gap: 8px;
                            box-shadow: 0 4px 12px rgba(29, 161, 242, 0.3);
                            transition: all 0.1s ease;
                        ">
                            <span>𝕏</span> Share to X (+ Download Image)
                        </button>
                        
                        <button onclick="showBackgroundSelectorForDownload()" style="
                            padding: 12px 24px;
                            background: #8b5cf6;
                            color: white;
                            border: none;
                            border-radius: 8px;
                            font-weight: 600;
                            cursor: pointer;
                            font-size: 14px;
                            display: flex;
                            align-items: center;
                            gap: 8px;
                            box-shadow: 0 4px 12px rgba(139, 92, 246, 0.3);
                            transition: all 0.1s ease;
                        ">
                            <span>📸</span> Download Image
                        </button>
                    </div>
                    
                    <!-- Background Selector (Hidden by default) -->
                    <div id="background-selector" style="
                        display: none;
                        background: rgba(255,255,255,0.95);
                        padding: 20px;
                        border-radius: 12px;
                        max-width: 600px;
                        margin: 0 auto;
                        box-shadow: 0 8px 24px rgba(0,0,0,0.2);
                    ">
                        <h3 style="color: #667eea; margin: 0 0 15px 0; font-size: 18px;">Choose Your Background</h3>
                        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin-bottom: 15px;">
                            <div onclick="selectBackground('charles')" class="bg-option" data-bg="charles" style="
                                height: 150px;
                                border-radius: 8px;
                                cursor: pointer;
                                background-image: url('https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-charles.png');
                                background-size: cover;
                                background-position: center;
                                border: 3px solid #667eea;
                                transition: all 0.2s;
                                position: relative;
                                overflow: hidden;
                            ">
                                <div style="position: absolute; bottom: 0; left: 0; right: 0; background: rgba(0,0,0,0.7); padding: 8px; text-align: center; color: white; font-weight: 600;">
                                    📚 Charles & Nike
                                </div>
                            </div>
                            
                            <div onclick="selectBackground('casino')" class="bg-option" data-bg="casino" style="
                                height: 150px;
                                border-radius: 8px;
                                cursor: pointer;
                                background-image: url('https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-casino.png');
                                background-size: cover;
                                background-position: center;
                                border: 3px solid transparent;
                                transition: all 0.2s;
                                position: relative;
                                overflow: hidden;
                            ">
                                <div style="position: absolute; bottom: 0; left: 0; right: 0; background: rgba(0,0,0,0.7); padding: 8px; text-align: center; color: white; font-weight: 600;">
                                    🎰 Casino Wins
                                </div>
                            </div>
                            
                            <div onclick="selectBackground('gaming')" class="bg-option" data-bg="gaming" style="
                                height: 150px;
                                border-radius: 8px;
                                cursor: pointer;
                                background-image: url('https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-gaming.png');
                                background-size: cover;
                                background-position: center;
                                border: 3px solid transparent;
                                transition: all 0.2s;
                                position: relative;
                                overflow: hidden;
                            ">
                                <div style="position: absolute; bottom: 0; left: 0; right: 0; background: rgba(0,0,0,0.7); padding: 8px; text-align: center; color: white; font-weight: 600;">
                                    🎮 Couch Trading
                                </div>
                            </div>
                            
                            <div onclick="selectBackground('money')" class="bg-option" data-bg="money" style="
                                height: 150px;
                                border-radius: 8px;
                                cursor: pointer;
                                background-image: url('https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-money.png');
                                background-size: cover;
                                background-position: center;
                                border: 3px solid transparent;
                                transition: all 0.2s;
                                position: relative;
                                overflow: hidden;
                            ">
                                <div style="position: absolute; bottom: 0; left: 0; right: 0; background: rgba(0,0,0,0.7); padding: 8px; text-align: center; color: white; font-weight: 600;">
                                    💰 Money Rain
                                </div>
                            </div>
                        </div>
                        <button id="selector-action-btn" onclick="handleSelectorAction()" style="
                            width: 100%;
                            padding: 12px;
                            background: #10b981;
                            color: white;
                            border: none;
                            border-radius: 8px;
                            font-weight: 600;
                            cursor: pointer;
                            font-size: 14px;
                            box-shadow: 0 4px 6px rgba(16, 185, 129, 0.3);
                            transition: all 0.1s ease;
                        ">
                            ✅ Download Image
                        </button>
                    </div>
                </div>
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="tooltip">
                        How much profit you made compared to your starting balance.
                        <div class="tooltip-formula">Profit ÷ Initial Capital × 100</div>
                    </div>
                    <div class="stat-label" id="label-roi-initial">ROI on Initial Capital</div>
                    <div class="stat-value" id="roi-initial">0%</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        How much profit you made compared to your total invested (including deposits minus withdrawals).
                        <div class="tooltip-formula">Profit ÷ (Initial + Deposits - Withdrawals) × 100</div>
                    </div>
                    <div class="stat-label" id="label-roi-total">ROI on Total Capital</div>
                    <div class="stat-value" id="roi-total">0%</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        How much you win vs how much you lose. Above 1.0 means you're profitable. ∞ means no losses yet!
                        <div class="tooltip-formula">Total $ Won ÷ Total $ Lost</div>
                    </div>
                    <div class="stat-label">Profit Factor <span style="opacity: 0.6; font-size: 11px;">(All Time)</span></div>
                    <div class="stat-value" id="profit-factor">0x</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        Your single most profitable trade in this period.
                        <div class="tooltip-formula">MAX(trade profits)</div>
                    </div>
                    <div class="stat-label" id="label-best-trade">Best Trade</div>
                    <div class="stat-value" id="best-trade">$0</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        Average profit/loss per trade in this period.
                        <div class="tooltip-formula">Total Profit ÷ Number of Trades</div>
                    </div>
                    <div class="stat-label" id="label-avg-trade">Avg Trade</div>
                    <div class="stat-value" id="avg-trade">$0</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        Number of completed trades in this period.
                        <div class="tooltip-formula">COUNT(closed trades)</div>
                    </div>
                    <div class="stat-label" id="label-total-trades">Total Trades</div>
                    <div class="stat-value" id="total-trades">0</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        Largest peak-to-valley drop in your portfolio. Lower is better!
                        <div class="tooltip-formula">(Peak Value - Lowest Point) ÷ Peak × 100</div>
                    </div>
                    <div class="stat-label" id="label-max-dd">Max Drawdown</div>
                    <div class="stat-value" id="max-dd">0%</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        Risk-adjusted return. Above 1.0 is good, above 2.0 is great! N/A if less than 2 trades.
                        <div class="tooltip-formula">Avg Return ÷ Volatility × √252</div>
                    </div>
                    <div class="stat-label">Sharpe Ratio <span style="opacity: 0.6; font-size: 11px;">(All Time)</span></div>
                    <div class="stat-value" id="sharpe">0.0</div>
                </div>
                
                <div class="stat-card">
                    <div class="tooltip">
                        Days since your very first trade with Nike Rocket.
                        <div class="tooltip-formula">Today - First Trade Date</div>
                    </div>
                    <div class="stat-label">Days Active <span style="opacity: 0.6; font-size: 11px;">(All Time)</span></div>
                    <div class="stat-value" id="days-active">0</div>
                </div>
            </div>
            
            <!-- Equity Curve Chart Section (NEW!) -->
            <div class="equity-curve-section" style="
                background: white;
                border-radius: 12px;
                padding: 30px;
                margin-top: 30px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            ">
                <div class="section-header">
                    <div>
                        <h2 style="margin: 0; color: #667eea; font-size: 24px;">
                            📈 Trading Equity Curve
                        </h2>
                        <p style="margin: 5px 0 0 0; font-size: 13px; color: #6b7280;">
                            Realized P&amp;L from closed signal trades (excludes unrealized P&amp;L, manual trades, deposits/withdrawals)
                        </p>
                    </div>
                    <div class="section-header-actions">
                        <span id="equity-stats" style="font-size: 13px; color: #6b7280;"></span>
                    </div>
                </div>
                
                <div id="equity-chart-container" style="position: relative; height: 350px; width: 100%;">
                    <canvas id="equity-chart"></canvas>
                </div>
                
                <div id="equity-summary" style="
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
                    gap: 15px;
                    margin-top: 20px;
                    padding-top: 20px;
                    border-top: 1px solid #e5e7eb;
                ">
                    <div style="text-align: center;">
                        <div style="font-size: 12px; color: #6b7280;">Starting</div>
                        <div id="eq-initial" style="font-size: 18px; font-weight: 600; color: #374151;">$0</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 12px; color: #6b7280;">Current</div>
                        <div id="eq-current" style="font-size: 18px; font-weight: 600; color: #10b981;">$0</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 12px; color: #6b7280;">Peak</div>
                        <div id="eq-peak" style="font-size: 18px; font-weight: 600; color: #667eea;">$0</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 12px; color: #6b7280;">Trough</div>
                        <div id="eq-trough" style="font-size: 18px; font-weight: 600; color: #ef4444;">$0</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 12px; color: #6b7280;">Max Drawdown</div>
                        <div id="eq-maxdd" style="font-size: 18px; font-weight: 600; color: #f59e0b;">0%</div>
                    </div>
                </div>
            </div>
            
            <!-- Open Positions Section (LIVE TRADES) -->
            <div class="open-positions" style="
                background: white;
                border-radius: 12px;
                padding: 30px;
                margin-top: 30px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            ">
                <div class="section-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                    <h2 style="margin: 0; color: #667eea; font-size: 24px;">
                        🔥 Open Positions
                    </h2>
                    <div style="text-align: right;">
                        <span id="open-positions-count" style="font-size: 14px; color: #6b7280; display: block;"></span>
                        <span style="font-size: 11px; color: #9ca3af;">🕐 Times in your local timezone</span>
                    </div>
                </div>
                
                <div id="open-positions-list" style="overflow-x: auto;">
                    <!-- Positions will be loaded here -->
                    <div style="text-align: center; padding: 40px; color: #9ca3af;">
                        Loading open positions...
                    </div>
                </div>
            </div>
            
            <!-- Transaction History Section (NEW!) -->
            <div class="transaction-history" style="
                background: white;
                border-radius: 12px;
                padding: 30px;
                margin-top: 30px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            ">
                <div class="section-header">
                    <h2 style="margin: 0; color: #667eea; font-size: 24px;">
                        📜 Transaction History
                    </h2>
                </div>
                
                <!-- Date Filter Controls -->
                <div style="
                    display: flex;
                    gap: 15px;
                    align-items: center;
                    padding: 15px;
                    background: #f9fafb;
                    border-radius: 8px;
                    margin-bottom: 15px;
                    flex-wrap: wrap;
                ">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <label style="font-size: 14px; color: #374151; font-weight: 500;">From:</label>
                        <input type="date" id="tx-start-date" style="
                            padding: 8px 12px;
                            border: 1px solid #e5e7eb;
                            border-radius: 6px;
                            font-size: 14px;
                            color: #374151;
                        ">
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <label style="font-size: 14px; color: #374151; font-weight: 500;">To:</label>
                        <input type="date" id="tx-end-date" style="
                            padding: 8px 12px;
                            border: 1px solid #e5e7eb;
                            border-radius: 6px;
                            font-size: 14px;
                            color: #374151;
                        ">
                    </div>
                    <button onclick="applyDateFilter()" style="
                        background: #667eea;
                        color: white;
                        border: none;
                        padding: 8px 16px;
                        border-radius: 6px;
                        cursor: pointer;
                        font-size: 14px;
                    ">
                        🔍 Search
                    </button>
                    <button onclick="clearDateFilter()" style="
                        background: #f3f4f6;
                        color: #374151;
                        border: 1px solid #e5e7eb;
                        padding: 8px 16px;
                        border-radius: 6px;
                        cursor: pointer;
                        font-size: 14px;
                    ">
                        ✕ Clear
                    </button>
                    <div id="date-filter-status" style="font-size: 12px; color: #6b7280; margin-left: auto;">
                    </div>
                </div>
                
                <div id="transaction-list" style="max-height: 500px; overflow-y: auto;">
                    <!-- Transactions will be loaded here -->
                    <div style="text-align: center; padding: 40px; color: #9ca3af;">
                        Loading transactions...
                    </div>
                </div>
                
                <div id="transaction-load-more" style="display: none; text-align: center; padding: 15px;">
                    <button onclick="loadMoreTransactions()" style="
                        background: #f3f4f6;
                        color: #374151;
                        border: 1px solid #e5e7eb;
                        padding: 10px 24px;
                        border-radius: 6px;
                        cursor: pointer;
                        font-size: 14px;
                        transition: all 0.1s ease;
                    ">
                        📜 Load More
                    </button>
                    <div id="transaction-count" style="font-size: 12px; color: #9ca3af; margin-top: 8px;">
                    </div>
                </div>
            </div>
            
            <!-- Trade Export Section (NEW!) -->
            <div class="trade-export" style="
                background: white;
                border-radius: 12px;
                padding: 30px;
                margin-top: 30px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            ">
                <h2 style="margin: 0 0 20px 0; color: #667eea; font-size: 24px;">
                    📊 Export Trade History
                </h2>
                
                <div class="export-grid">
                    <!-- Monthly Export -->
                    <div style="background: #f9fafb; padding: 20px; border-radius: 8px;">
                        <h3 style="margin: 0 0 15px 0; color: #374151; font-size: 18px;">📅 Monthly Report</h3>
                        <div style="display: flex; gap: 10px; margin-bottom: 15px;">
                            <select id="export-month" style="flex: 1; padding: 10px; border: 2px solid #e5e7eb; border-radius: 6px; font-size: 14px;">
                                <option value="01">January</option>
                                <option value="02">February</option>
                                <option value="03">March</option>
                                <option value="04">April</option>
                                <option value="05">May</option>
                                <option value="06">June</option>
                                <option value="07">July</option>
                                <option value="08">August</option>
                                <option value="09">September</option>
                                <option value="10">October</option>
                                <option value="11">November</option>
                                <option value="12">December</option>
                            </select>
                            <select id="export-month-year" style="width: 100px; padding: 10px; border: 2px solid #e5e7eb; border-radius: 6px; font-size: 14px;">
                            </select>
                        </div>
                        <button onclick="downloadMonthlyTrades()" style="
                            width: 100%;
                            padding: 12px;
                            background: #10b981;
                            color: white;
                            border: none;
                            border-radius: 6px;
                            font-size: 14px;
                            font-weight: 600;
                            cursor: pointer;
                            transition: all 0.1s ease;
                        ">
                            ⬇️ Download Monthly CSV
                        </button>
                    </div>
                    
                    <!-- Yearly Export -->
                    <div style="background: #f9fafb; padding: 20px; border-radius: 8px;">
                        <h3 style="margin: 0 0 15px 0; color: #374151; font-size: 18px;">📆 Yearly Report</h3>
                        <div style="margin-bottom: 15px;">
                            <select id="export-year" style="width: 100%; padding: 10px; border: 2px solid #e5e7eb; border-radius: 6px; font-size: 14px;">
                            </select>
                        </div>
                        <button onclick="downloadYearlyTrades()" style="
                            width: 100%;
                            padding: 12px;
                            background: #3b82f6;
                            color: white;
                            border: none;
                            border-radius: 6px;
                            font-size: 14px;
                            font-weight: 600;
                            cursor: pointer;
                            transition: all 0.1s ease;
                        ">
                            ⬇️ Download Yearly CSV
                        </button>
                    </div>
                </div>
                
                <div style="margin-top: 15px; padding: 12px; background: #eff6ff; border-radius: 6px; font-size: 13px; color: #1e40af;">
                    💡 CSV exports include: Date, Symbol, Side, Entry Price, Exit Price, Position Size, P&L, and Net P&L summary
                </div>
            </div>
            
            <!-- Backtest Results Section -->
            <div class="backtest-section">
                <div class="backtest-header" onclick="toggleBacktestSection()">
                    <h2>📊 Backtested Performance (ADA/USDT)</h2>
                    <span class="toggle-icon" id="backtestToggle">▼</span>
                </div>
                
                <div class="backtest-grid" id="backtestGrid">
                    <!-- Bull Market -->
                    <div class="backtest-card">
                        <span class="market-badge bull">🚀 BULL MARKET</span>
                        <div class="period">Jan 2020 → Aug 2021 (1.6 years)</div>
                        <div class="result profit">+10,132%</div>
                        <div class="final-value">$300 → $30,695</div>
                        <div class="stats">
                            <div class="stat">
                                <div class="stat-label">Buy & Hold</div>
                                <div class="stat-value positive">+4,377%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Outperformance</div>
                                <div class="stat-value">2.3x</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Win Rate</div>
                                <div class="stat-value">42.2%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Max Drawdown</div>
                                <div class="stat-value">37.6%</div>
                            </div>
                        </div>
                    </div>

                    <!-- Bear Market -->
                    <div class="backtest-card">
                        <span class="market-badge bear">🐻 BEAR MARKET</span>
                        <div class="period">Sep 2021 → Jun 2023 (1.75 years)</div>
                        <div class="result profit">+5,402%</div>
                        <div class="final-value">$300 → $16,505</div>
                        <div class="stats">
                            <div class="stat">
                                <div class="stat-label">Buy & Hold</div>
                                <div class="stat-value negative">-84.4%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Outperformance</div>
                                <div class="stat-value">∞</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Win Rate</div>
                                <div class="stat-value">42.0%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Max Drawdown</div>
                                <div class="stat-value">38.9%</div>
                            </div>
                        </div>
                    </div>

                    <!-- Sideways/Chop Market -->
                    <div class="backtest-card">
                        <span class="market-badge sideways">🦀 SIDEWAYS CHOP</span>
                        <div class="period">Jun 2023 → Nov 2025 (2.5 years)</div>
                        <div class="result profit">+9,850%</div>
                        <div class="final-value">$300 → $29,849</div>
                        <div class="stats">
                            <div class="stat">
                                <div class="stat-label">Buy & Hold</div>
                                <div class="stat-value positive">+60.9%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Outperformance</div>
                                <div class="stat-value">162x</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Win Rate</div>
                                <div class="stat-value">41.1%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Max Drawdown</div>
                                <div class="stat-value">40.2%</div>
                            </div>
                        </div>
                    </div>

                    <!-- Full Cycle -->
                    <div class="backtest-card">
                        <span class="market-badge full">📈 FULL CYCLE</span>
                        <div class="period">Jan 2020 → Nov 2025 (5.8 years)</div>
                        <div class="result profit">+83.8M%</div>
                        <div class="final-value">$300 → $251.4M</div>
                        <div class="stats">
                            <div class="stat">
                                <div class="stat-label">Buy & Hold</div>
                                <div class="stat-value positive">+582%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Outperformance</div>
                                <div class="stat-value">144,008x</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Win Rate</div>
                                <div class="stat-value">41.8%</div>
                            </div>
                            <div class="stat">
                                <div class="stat-label">Max Drawdown</div>
                                <div class="stat-value">40.2%</div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Max Pain / What to Expect Section -->
                <div class="max-pain-section">
                    <h3>🔥 What to Expect: The Hard Truth</h3>
                    
                    <div class="max-pain-grid">
                        <div class="pain-stat">
                            <div class="pain-value">~40%</div>
                            <div class="pain-label">Max Drawdown</div>
                            <div class="pain-explanation">Your account could drop 40% from its peak before recovering. This is normal and expected.</div>
                        </div>
                        
                        <div class="pain-stat">
                            <div class="pain-value">7-9</div>
                            <div class="pain-label">Max Losses in a Row</div>
                            <div class="pain-explanation">You may experience 7-9 consecutive losing trades. Stay disciplined - winners follow.</div>
                        </div>
                        
                        <div class="pain-stat">
                            <div class="pain-value">~42%</div>
                            <div class="pain-label">Win Rate</div>
                            <div class="pain-explanation">You'll lose more trades than you win. But winners are ~1.7x larger than losers on average.</div>
                        </div>
                        
                        <div class="pain-stat">
                            <div class="pain-value">2-3%</div>
                            <div class="pain-label">Risk Per Trade</div>
                            <div class="pain-explanation">Each trade risks only 2-3% of your account. This is how we survive losing streaks.</div>
                        </div>
                    </div>
                    
                    <div class="max-pain-summary">
                        <strong>💡 The Psychology:</strong> Most traders quit during drawdowns or losing streaks - right before the system recovers. 
                        The algo made money in <strong>bull markets</strong>, <strong>bear markets</strong>, and <strong>sideways chop</strong> because it stayed consistent. 
                        Your job is simple: <strong>don't interfere</strong>. Let the math play out over hundreds of trades.
                    </div>
                </div>

                <div class="backtest-disclaimer">
                    ⚠️ <strong>Past performance does not guarantee future results.</strong> Backtests use historical Binance data with 20x max leverage, 2-3% risk per trade. Real trading involves slippage, fees, and market conditions that may differ. Trade responsibly.
                </div>
            </div>
            
            <!-- Safety & Trust Section -->
            <div class="safety-section">
                <div class="safety-header" onclick="toggleSafetySection()">
                    <h2>🛡️ Safety & Trust</h2>
                    <span class="toggle-icon">▼</span>
                </div>
                
                <div class="safety-grid" id="safetyGrid">
                    <!-- Non-Custodial -->
                    <div class="safety-card">
                        <div class="icon">🔐</div>
                        <h3>100% Non-Custodial</h3>
                        <p>Your funds never leave your Hyperliquid account. We can't touch them.</p>
                        <ul>
                            <li>Trade-only API permissions</li>
                            <li>Withdrawals NEVER enabled</li>
                            <li>Funds stay in YOUR wallet</li>
                            <li>Revoke access anytime</li>
                        </ul>
                    </div>

                    <!-- Open Source -->
                    <div class="safety-card">
                        <div class="icon">📖</div>
                        <h3>Fully Open Source</h3>
                        <p>Don't trust, verify. Our entire codebase is public.</p>
                        <ul>
                            <li>Audit every line of code</li>
                            <li>See how trades execute</li>
                            <li>Verify billing logic</li>
                            <li>Community reviewed</li>
                        </ul>
                        <a href="https://github.com/DrCalebL/hyperliquid-nike-rocket-api" target="_blank" class="github-link">
                            <svg viewBox="0 0 16 16" fill="currentColor">
                                <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
                            </svg>
                            nike-rocket-api
                        </a>

                    </div>

                    <!-- Risk Management -->
                    <div class="safety-card">
                        <div class="icon">📊</div>
                        <h3>Professional Risk Management</h3>
                        <p>Strict protocols used by professional traders.</p>
                        <ul>
                            <li>2-3% max risk per trade</li>
                            <li>ATR-based dynamic stops</li>
                            <li>TP & SL on every trade</li>
                            <li>Risk-based position sizing</li>
                        </ul>
                    </div>

                    <!-- Order Retry Logic -->
                    <div class="safety-card">
                        <div class="icon">🔄</div>
                        <h3>Bulletproof Order Execution</h3>
                        <p>Automatic retry logic protects every trade.</p>
                        <ul>
                            <li>3 retry attempts per order</li>
                            <li>Exponential backoff timing</li>
                            <li>Entry/TP/SL fails = trade aborted safely</li>
                            <li>Admin alerted on any failure</li>
                        </ul>
                    </div>

                    <!-- Fair Billing -->
                    <div class="safety-card">
                        <div class="icon">💰</div>
                        <h3>Fair Profit-Share Billing</h3>
                        <p>We only profit when YOU profit.</p>
                        <ul>
                            <li>10% of net profits only</li>
                            <li>Losses offset wins</li>
                            <li>30-day billing cycles</li>
                            <li>No profit = no fee</li>
                        </ul>
                    </div>

                    <!-- Signal Matching -->
                    <div class="safety-card">
                        <div class="icon">🎯</div>
                        <h3>Smart Signal Matching</h3>
                        <p>We know the difference between our trades and yours.</p>
                        <ul>
                            <li>Only billed for our signals</li>
                            <li>Your manual trades are free</li>
                            <li>No surprise fees</li>
                            <li>Full audit trail</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        let currentApiKey = '{api_key}';
        let currentPeriod = '30d';
        
        // Safety section toggle
        function toggleSafetySection() {{
            const header = document.querySelector('.safety-header');
            const grid = document.getElementById('safetyGrid');
            header.classList.toggle('collapsed');
            grid.classList.toggle('hidden');
        }}

        function toggleBacktestSection() {{
            const grid = document.getElementById('backtestGrid');
            const toggle = document.getElementById('backtestToggle');
            const disclaimer = grid.nextElementSibling;
            if (grid.style.display === 'none') {{
                grid.style.display = 'grid';
                disclaimer.style.display = 'block';
                toggle.textContent = '▼';
            }} else {{
                grid.style.display = 'none';
                disclaimer.style.display = 'none';
                toggle.textContent = '▶';
            }}
        }}
        
        // On page load
        if (currentApiKey) {{
            document.getElementById('api-key-input').value = currentApiKey;
            login();
        }}
        
        // ═══════════════════════════════════════════════════════════════
        // Agent Status Monitoring Functions
        // ═══════════════════════════════════════════════════════════════
        
        async function checkAgentStatusAPI() {{
            try {{
                const response = await fetch('/api/agent-status', {{
                    headers: {{'X-API-Key': currentApiKey}}
                }});
                
                const data = await response.json();
                return data;
                
            }} catch (error) {{
                console.error('Error checking agent status:', error);
                return {{ agent_active: false, agent_configured: false, message: error.message }};
            }}
        }}
        
        async function displayAgentStatus() {{
            try {{
                const statusData = await checkAgentStatusAPI();
                
                const statusElement = document.getElementById('agent-status-display');
                if (!statusElement) return;
                
                let statusHTML = '';
                let statusClass = '';
                
                // API returns: agent_active, agent_configured, message
                if (statusData.agent_active) {{
                    statusHTML = '🟢 <strong>Agent Active</strong> - Following signals';
                    statusClass = 'status-active';
                }} else if (statusData.agent_configured) {{
                    statusHTML = '🟡 <strong>Ready</strong> - Agent configured but stopped';
                    statusClass = 'status-ready';
                }} else {{
                    statusHTML = '🔴 <strong>Not Configured</strong> - <a href="/setup?key=' + currentApiKey + '" style="color: #dc2626;">Complete setup</a>';
                    statusClass = 'status-error';
                }}
                
                statusElement.innerHTML = statusHTML;
                statusElement.className = 'agent-status ' + statusClass;
                
            }} catch (error) {{
                console.error('Error displaying agent status:', error);
            }}
        }}
        
        let agentStatusInterval = null;
        
        function startAgentStatusMonitoring() {{
            if (agentStatusInterval) {{
                clearInterval(agentStatusInterval);
            }}
            
            // Display immediately
            displayAgentStatus();
            
            // Then update every 30 seconds
            agentStatusInterval = setInterval(() => {{
                displayAgentStatus();
            }}, 30000);
        }}
        
        function stopAgentStatusMonitoring() {{
            if (agentStatusInterval) {{
                clearInterval(agentStatusInterval);
                agentStatusInterval = null;
            }}
        }}
        
        // ═══════════════════════════════════════════════════════════════
        
        function login() {{
            const apiKey = document.getElementById('api-key-input').value.trim();
            
            if (!apiKey) {{
                showError('login-error', 'Please enter your API key');
                return;
            }}
            
            if (!apiKey.startsWith('nk_')) {{
                showError('login-error', 'Invalid API key format. Keys should start with "nk_"');
                return;
            }}
            
            currentApiKey = apiKey;
            localStorage.setItem('apiKey', apiKey);
            
            // Try to load stats
            checkPortfolioStatus();
        }}
        
        function logout() {{
            stopAgentStatusMonitoring();
            localStorage.removeItem('apiKey');
            currentApiKey = '';
            document.getElementById('login-screen').style.display = 'block';
            document.getElementById('setup-wizard').style.display = 'none';
            document.getElementById('dashboard').style.display = 'none';
        }}
        
        async function checkPortfolioStatus() {{
            try {{
                const response = await fetch(`/api/portfolio/balance-summary?key=${{currentApiKey}}`, {{
                    headers: {{'X-API-Key': currentApiKey}}
                }});
                
                if (response.status === 401) {{
                    showError('login-error', 'Invalid API key. Please check and try again.');
                    return;
                }}
                
                if (!response.ok) {{
                    // Handle other errors (500, 404, etc.)
                    console.error('Portfolio stats error:', response.status);
                    // Still try to show setup wizard for new users
                    showSetupWizard();
                    return;
                }}
                
                const data = await response.json();
                
                if (data.status === 'success' || data.total_profit !== undefined) {{
                    // Portfolio initialized - show dashboard with data
                    showDashboard(data);
                    // Initialize export controls
                    initExportControls();
                    // Load balance summary and transactions
                    await loadBalanceSummary();
                    await loadPositions();
                    await loadTransactionHistory();
                    // Load equity curve chart
                    await loadEquityCurve();
                    // Load performance stats for default 30d period (fixes hero section showing $0)
                    await changePeriod();
                    // Check agent status
                    await checkAgentStatus();
                }} else if (data.status === 'not_initialized') {{
                    // Portfolio not yet initialized - show setup wizard
                    showSetupWizard();
                }} else {{
                    // Unknown status - show setup wizard
                    showSetupWizard();
                }}
                
            }} catch (error) {{
                console.error('Error:', error);
                // If error, assume needs setup
                showSetupWizard();
            }}
        }}
        
        function showSetupWizard() {{
            document.getElementById('login-screen').style.display = 'none';
            document.getElementById('setup-wizard').style.display = 'block';
            document.getElementById('dashboard').style.display = 'none';
        }}
        
        async function initializePortfolio() {{
            // No need to get initial capital - it will be auto-detected!
            
            try {{
                // Show loading message
                showSuccess('setup-message', '🔍 Detecting your Hyperliquid balance...');
                
                const response = await fetch('/api/portfolio/initialize', {{
                    method: 'POST',
                    headers: {{
                        'X-API-Key': currentApiKey,
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{}})  // Empty - auto-detect!
                }});
                
                const data = await response.json();
                
                if (data.status === 'success') {{
                    showSuccess('setup-message', 
                        `✅ Portfolio initialized with $${{data.initial_capital.toLocaleString()}} detected from your Hyperliquid account!`);
                    setTimeout(() => checkPortfolioStatus(), 2000);
                }} else if (data.status === 'already_initialized') {{
                    showSuccess('setup-message', 'Portfolio already initialized! Loading dashboard...');
                    setTimeout(() => checkPortfolioStatus(), 1000);
                }} else if (data.status === 'error') {{
                    // Show error with setup link if needed
                    if (data.message.includes('set up your trading agent')) {{
                        showError('setup-message', 
                            data.message + '<br><br>' +
                            '<a href="/setup?key=' + currentApiKey + '" ' +
                            'style="color: #ffffff; text-decoration: underline; font-weight: bold;">' +
                            '→ Go to Agent Setup</a>');
                    }} else {{
                        showError('setup-message', data.message);
                    }}
                }} else {{
                    showError('setup-message', data.message || 'Failed to initialize portfolio');
                }}
                
            }} catch (error) {{
                showError('setup-message', 'Error initializing portfolio: ' + error.message);
            }}
        }}
        
        function showDashboard(stats) {{
            document.getElementById('login-screen').style.display = 'none';
            document.getElementById('setup-wizard').style.display = 'none';
            document.getElementById('dashboard').style.display = 'block';
            
            // Start agent status monitoring
            startAgentStatusMonitoring();
            
            // Don't call updateDashboard - portfolio data loaded separately
            // The loadBalanceSummary() function handles all portfolio updates
        }}
        
        function updateDashboard(stats) {{
            // Update profit label with readable period
            const periodDisplayLabels = {{
                '7d': '7d',
                '30d': '30d',
                '90d': '90d',
                '1y': '1y',
                'all': 'All-Time'
            }};
            document.getElementById('profit-label').textContent = `${{periodDisplayLabels[stats.period] || stats.period}} Profit`;
            
            // Handle negative total profit
            const totalProfit = stats.total_profit || 0;
            document.getElementById('total-profit').textContent = 
                totalProfit >= 0 
                    ? `+$${{totalProfit.toLocaleString()}}` 
                    : `-$${{Math.abs(totalProfit).toLocaleString()}}`;
            document.getElementById('total-profit').style.color = totalProfit >= 0 ? '#10b981' : '#ef4444';
            
            // ═══════════════════════════════════════════════════════════════
            // PERIOD-SPECIFIC LABELS
            // ═══════════════════════════════════════════════════════════════
            const periodLabels = {{
                '7d': '7D',
                '30d': '30D',
                '90d': '90D',
                '1y': '1Y',
                'all': 'All Time'
            }};
            const periodTag = `<span style="opacity: 0.6; font-size: 11px;">(${{periodLabels[currentPeriod] || '30D'}})</span>`;
            
            // Update period-specific labels
            document.getElementById('label-roi-initial').innerHTML = `ROI on Initial Capital ${{periodTag}}`;
            document.getElementById('label-roi-total').innerHTML = `ROI on Total Capital ${{periodTag}}`;
            document.getElementById('label-best-trade').innerHTML = `Best Trade ${{periodTag}}`;
            document.getElementById('label-avg-trade').innerHTML = `Avg Trade ${{periodTag}}`;
            document.getElementById('label-total-trades').innerHTML = `Total Trades ${{periodTag}}`;
            document.getElementById('label-max-dd').innerHTML = `Max Drawdown ${{periodTag}}`;
            
            // ═══════════════════════════════════════════════════════════════
            // PERIOD-SPECIFIC VALUES
            // ═══════════════════════════════════════════════════════════════
            // Handle negative ROI values (period-specific)
            const roiInitial = stats.roi_on_initial || 0;
            const roiTotal = stats.roi_on_total || roiInitial;
            document.getElementById('roi-initial').textContent = 
                roiInitial >= 0 ? `+${{roiInitial.toFixed(1)}}%` : `${{roiInitial.toFixed(1)}}%`;
            document.getElementById('roi-initial').style.color = roiInitial >= 0 ? '#10b981' : '#ef4444';
            document.getElementById('roi-total').textContent = 
                roiTotal >= 0 ? `+${{roiTotal.toFixed(1)}}%` : `${{roiTotal.toFixed(1)}}%`;
            document.getElementById('roi-total').style.color = roiTotal >= 0 ? '#10b981' : '#ef4444';
            
            // Handle negative best trade (period-specific)
            const bestTrade = stats.best_trade || 0;
            document.getElementById('best-trade').textContent = 
                bestTrade >= 0 ? `+$${{bestTrade.toLocaleString()}}` : `-$${{Math.abs(bestTrade).toLocaleString()}}`;
            document.getElementById('best-trade').style.color = bestTrade >= 0 ? '#10b981' : '#ef4444';
            
            // Handle negative avg trade (period-specific)
            const avgTrade = stats.avg_trade || 0;
            document.getElementById('avg-trade').textContent = 
                avgTrade >= 0 ? `+$${{avgTrade.toLocaleString()}}` : `-$${{Math.abs(avgTrade).toLocaleString()}}`;
            document.getElementById('avg-trade').style.color = avgTrade >= 0 ? '#10b981' : '#ef4444';
            
            // Total trades (period-specific)
            document.getElementById('total-trades').textContent = stats.total_trades;
            
            // Max drawdown (period-specific, no minus for 0%)
            const maxDD = stats.max_drawdown || 0;
            document.getElementById('max-dd').textContent = maxDD > 0 ? `-${{maxDD}}%` : `0%`;
            
            // ═══════════════════════════════════════════════════════════════
            // ALL-TIME VALUES (Profit Factor, Sharpe Ratio, Days Active)
            // ═══════════════════════════════════════════════════════════════
            // Profit Factor (all-time)
            if (stats.all_time_profit_factor === null) {{
                document.getElementById('profit-factor').textContent = '∞';
                document.getElementById('profit-factor').style.color = '#10b981';
            }} else {{
                const pf = stats.all_time_profit_factor || 0;
                document.getElementById('profit-factor').textContent = `${{pf}}x`;
                document.getElementById('profit-factor').style.color = pf >= 1 ? '#10b981' : '#ef4444';
            }}
            
            // Sharpe ratio (all-time)
            if (stats.all_time_sharpe === null) {{
                document.getElementById('sharpe').textContent = 'N/A';
                document.getElementById('sharpe').style.color = '#9ca3af';
            }} else {{
                const sharpe = stats.all_time_sharpe || 0;
                document.getElementById('sharpe').textContent = sharpe.toFixed(1);
                document.getElementById('sharpe').style.color = sharpe >= 1 ? '#10b981' : (sharpe >= 0 ? '#fbbf24' : '#ef4444');
            }}
            
            // Days active (all-time)
            document.getElementById('days-active').textContent = stats.all_time_days_active || '< 1';
            
            if (stats.started_tracking) {{
                const startDate = new Date(stats.started_tracking);
                document.getElementById('time-tracking').textContent = 
                    `Trading since ${{startDate.toLocaleDateString()}} • ${{stats.period}}`;
            }}
        }}
        
        // NEW: Load balance summary
        async function loadBalanceSummary() {{
            try {{
                const response = await fetch(`/api/portfolio/balance-summary?key=${{currentApiKey}}`);
                
                if (response.status === 401) {{
                    // Invalid API key - redirect to login
                    alert('Invalid API key. Please log in again.');
                    logout();
                    return;
                }}
                
                const data = await response.json();
                
                if (data.status === 'success') {{
                    // Update portfolio overview
                    document.getElementById('current-value').textContent = 
                        `$${{data.current_value.toLocaleString()}}`;
                    document.getElementById('initial-capital-display').textContent = 
                        `$${{data.initial_capital.toLocaleString()}}`;
                    document.getElementById('net-deposits').textContent = 
                        data.net_deposits >= 0 
                            ? `+$${{data.net_deposits.toLocaleString()}}`
                            : `-$${{Math.abs(data.net_deposits).toLocaleString()}}`;
                    
                    // Handle negative total profit with color
                    const totalProfit = data.total_profit || 0;
                    const profitEl = document.getElementById('total-profit-overview');
                    profitEl.textContent = totalProfit >= 0 
                        ? `+$${{totalProfit.toLocaleString()}}` 
                        : `-$${{Math.abs(totalProfit).toLocaleString()}}`;
                    profitEl.style.color = totalProfit >= 0 ? '#10b981' : '#ef4444';
                    
                    document.getElementById('total-deposits').textContent = 
                        `+$${{data.total_deposits.toLocaleString()}}`;
                    document.getElementById('total-withdrawals').textContent = 
                        data.total_withdrawals > 0 
                            ? `-$${{data.total_withdrawals.toLocaleString()}}`
                            : `$0`;
                    document.getElementById('total-capital').textContent = 
                        `$${{data.total_capital.toLocaleString()}}`;
                    
                    // Handle negative ROI with colors
                    const roiInitial = data.roi_on_initial || 0;
                    const roiTotal = data.roi_on_total || 0;
                    
                    const roiInitialEl = document.getElementById('roi-initial');
                    roiInitialEl.textContent = roiInitial >= 0 
                        ? `+${{roiInitial.toFixed(1)}}%` 
                        : `${{roiInitial.toFixed(1)}}%`;
                    roiInitialEl.style.color = roiInitial >= 0 ? '#10b981' : '#ef4444';
                    
                    const roiTotalEl = document.getElementById('roi-total');
                    roiTotalEl.textContent = roiTotal >= 0 
                        ? `+${{roiTotal.toFixed(1)}}%` 
                        : `${{roiTotal.toFixed(1)}}%`;
                    roiTotalEl.style.color = roiTotal >= 0 ? '#10b981' : '#ef4444';
                    
                    // Update last check time
                    if (data.last_balance_check) {{
                        const checkTime = new Date(data.last_balance_check);
                        document.getElementById('last-check').textContent = 
                            checkTime.toLocaleString();
                    }}
                }}
            }} catch (error) {{
                console.error('Error loading balance summary:', error);
            }}
        }}
        
        // NEW: Transaction pagination state
        let loadedTransactions = [];
        let transactionOffset = 0;
        const TRANSACTIONS_PER_PAGE = 20;
        let hasMoreTransactions = true;
        let txStartDate = null;
        let txEndDate = null;
        
        // Apply date filter
        function applyDateFilter() {{
            txStartDate = document.getElementById('tx-start-date').value || null;
            txEndDate = document.getElementById('tx-end-date').value || null;
            
            // Update status display
            const statusEl = document.getElementById('date-filter-status');
            if (txStartDate || txEndDate) {{
                let filterText = 'Filtering: ';
                if (txStartDate && txEndDate) {{
                    filterText += `${{txStartDate}} to ${{txEndDate}}`;
                }} else if (txStartDate) {{
                    filterText += `from ${{txStartDate}}`;
                }} else {{
                    filterText += `until ${{txEndDate}}`;
                }}
                statusEl.textContent = filterText;
                statusEl.style.color = '#667eea';
            }} else {{
                statusEl.textContent = '';
            }}
            
            // Reload with filter
            loadTransactionHistory(true);
        }}
        
        // Clear date filter
        function clearDateFilter() {{
            document.getElementById('tx-start-date').value = '';
            document.getElementById('tx-end-date').value = '';
            txStartDate = null;
            txEndDate = null;
            document.getElementById('date-filter-status').textContent = '';
            loadTransactionHistory(true);
        }}
        
        // Render transactions to the list
        function renderTransactions() {{
            const listElement = document.getElementById('transaction-list');
            
            if (loadedTransactions.length > 0) {{
                let html = '';
                for (const tx of loadedTransactions) {{
                    const date = new Date(tx.created_at).toLocaleDateString();
                    const time = new Date(tx.created_at).toLocaleTimeString();
                    
                    // Determine icon, color, sign, and label based on transaction type
                    let icon, color, sign, label, subtitle;
                    
                    if (tx.transaction_type === 'deposit') {{
                        icon = '💰';
                        color = '#10b981';
                        sign = '+';
                        label = 'Funding/Deposit';
                        subtitle = `${{date}} at ${{time}}`;
                    }} else if (tx.transaction_type === 'fees_funding_withdrawal') {{
                        icon = '💸';
                        color = '#ef4444';
                        sign = '-';
                        label = 'Fees / Funding / Withdrawal';
                        subtitle = `${{date}} (daily total)`;
                    }} else if (tx.transaction_type === 'withdrawal') {{
                        // Legacy withdrawal entries
                        icon = '💸';
                        color = '#ef4444';
                        sign = '-';
                        label = 'Withdrawal';
                        subtitle = `${{date}} at ${{time}}`;
                    }} else {{
                        icon = '🎯';
                        color = '#667eea';
                        sign = '';
                        label = tx.transaction_type;
                        subtitle = `${{date}} at ${{time}}`;
                    }}
                    
                    html += `
                        <div style="
                            padding: 15px;
                            border-bottom: 1px solid #e5e7eb;
                            display: flex;
                            justify-content: space-between;
                            align-items: center;
                        ">
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <div style="font-size: 24px;">${{icon}}</div>
                                <div>
                                    <div style="font-weight: 600; color: #374151;">
                                        ${{label}}
                                    </div>
                                    <div style="font-size: 12px; color: #9ca3af;">
                                        ${{subtitle}}
                                    </div>
                                </div>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-size: 20px; font-weight: 600; color: ${{color}};">
                                    ${{sign}}$${{tx.amount.toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}})}}
                                </div>
                                <div style="font-size: 11px; color: #9ca3af;">
                                    ${{tx.detection_method}}
                                </div>
                            </div>
                        </div>
                    `;
                }}
                
                // Add info note at the bottom
                html += `
                    <div style="padding: 15px; background: #f9fafb; border-top: 1px solid #e5e7eb;">
                        <div style="font-size: 12px; color: #6b7280; text-align: center;">
                            ℹ️ <strong>Note:</strong> Hyperliquid API cannot distinguish between trading fees, 
                            funding payments, and spot↔futures transfers. These are grouped as 
                            "Fees / Funding / Withdrawal" (aggregated daily).
                        </div>
                    </div>
                `;
                
                listElement.innerHTML = html;
                
                // Show/hide Load More button
                const loadMoreDiv = document.getElementById('transaction-load-more');
                const countDiv = document.getElementById('transaction-count');
                if (hasMoreTransactions) {{
                    loadMoreDiv.style.display = 'block';
                    countDiv.textContent = `Showing ${{loadedTransactions.length}} transactions`;
                }} else {{
                    loadMoreDiv.style.display = 'block';
                    countDiv.textContent = `All ${{loadedTransactions.length}} transactions loaded`;
                    loadMoreDiv.querySelector('button').style.display = 'none';
                }}
            }} else {{
                listElement.innerHTML = `
                    <div style="text-align: center; padding: 40px; color: #9ca3af;">
                        No transactions yet. System will automatically detect deposits and withdrawals.
                    </div>
                `;
                document.getElementById('transaction-load-more').style.display = 'none';
            }}
        }}
        
        // Load transaction history (reset = true to start fresh)
        async function loadTransactionHistory(reset = false) {{
            try {{
                if (reset) {{
                    loadedTransactions = [];
                    transactionOffset = 0;
                    hasMoreTransactions = true;
                }}
                
                // Build URL with optional date filters
                let url = `/api/portfolio/transactions?key=${{currentApiKey}}&limit=${{TRANSACTIONS_PER_PAGE}}&offset=${{transactionOffset}}`;
                if (txStartDate) {{
                    url += `&start_date=${{txStartDate}}`;
                }}
                if (txEndDate) {{
                    url += `&end_date=${{txEndDate}}`;
                }}
                
                const response = await fetch(url);
                const data = await response.json();
                
                if (data.status === 'success') {{
                    if (data.transactions.length > 0) {{
                        loadedTransactions = loadedTransactions.concat(data.transactions);
                        transactionOffset += data.transactions.length;
                        
                        // Check if there are more to load
                        if (data.transactions.length < TRANSACTIONS_PER_PAGE) {{
                            hasMoreTransactions = false;
                        }}
                    }} else {{
                        hasMoreTransactions = false;
                    }}
                    
                    renderTransactions();
                }}
            }} catch (error) {{
                console.error('Error loading transactions:', error);
                document.getElementById('transaction-list').innerHTML = `
                    <div style="text-align: center; padding: 40px; color: #ef4444;">
                        Error loading transactions
                    </div>
                `;
            }}
        }}
        
        // Load more transactions (append to existing)
        async function loadMoreTransactions() {{
            await loadTransactionHistory(false);
        }}
        
        // ==================== EQUITY CURVE CHART ====================
        let equityChart = null;
        
        async function loadEquityCurve() {{
            try {{
                const response = await fetch(`/api/portfolio/equity-curve?key=${{currentApiKey}}`);
                const data = await response.json();
                
                // Update summary stats (these should always be available)
                const initialCap = data.initial_capital || 0;
                const currentEq = data.current_equity || initialCap;
                const maxEq = data.max_equity || initialCap;
                const minEq = data.min_equity || initialCap;
                const maxDD = data.max_drawdown || 0;
                const totalTrades = data.total_trades || 0;
                const totalPnl = data.total_pnl || 0;
                
                document.getElementById('eq-initial').textContent = `$${{initialCap.toLocaleString()}}`;
                document.getElementById('eq-current').textContent = `$${{currentEq.toLocaleString()}}`;
                document.getElementById('eq-peak').textContent = `$${{maxEq.toLocaleString()}}`;
                document.getElementById('eq-trough').textContent = `$${{minEq.toLocaleString()}}`;
                document.getElementById('eq-maxdd').textContent = `${{maxDD.toFixed(1)}}%`;
                
                // Color current equity based on profit/loss
                const currentEl = document.getElementById('eq-current');
                currentEl.style.color = currentEq >= initialCap ? '#10b981' : '#ef4444';
                
                // Update stats text
                document.getElementById('equity-stats').textContent = 
                    `${{totalTrades}} trades | Total PnL: $${{totalPnl >= 0 ? '+' : ''}}${{totalPnl.toLocaleString()}}`;
                
                // Check if we have actual trading data to chart
                if (data.status === 'success' && data.equity_curve && data.equity_curve.length > 1) {{
                    // We have trades - render chart with time-proportional X axis
                    // Busy periods show as clustered dots
                    const equityData = data.equity_curve.map(point => ({{
                        x: new Date(point.date),
                        y: point.equity
                    }}));
                    
                    const startingCapitalData = data.equity_curve.map(point => ({{
                        x: new Date(point.date),
                        y: data.initial_capital
                    }}));
                    
                    // Destroy existing chart if any
                    if (equityChart) {{
                        equityChart.destroy();
                    }}
                    
                    // Create gradient
                    const ctx = document.getElementById('equity-chart').getContext('2d');
                    const gradient = ctx.createLinearGradient(0, 0, 0, 350);
                    
                    // Color based on profit/loss
                    if (data.current_equity >= data.initial_capital) {{
                        gradient.addColorStop(0, 'rgba(16, 185, 129, 0.3)');
                        gradient.addColorStop(1, 'rgba(16, 185, 129, 0.0)');
                    }} else {{
                        gradient.addColorStop(0, 'rgba(239, 68, 68, 0.3)');
                        gradient.addColorStop(1, 'rgba(239, 68, 68, 0.0)');
                    }}
                    
                    // Create chart with time scale (clustered dots = busy periods)
                    equityChart = new Chart(ctx, {{
                        type: 'line',
                        data: {{
                            datasets: [{{
                                label: 'Trading Equity',
                                data: equityData,
                                borderColor: data.current_equity >= data.initial_capital ? '#10b981' : '#ef4444',
                                backgroundColor: gradient,
                                borderWidth: 2,
                                fill: true,
                                tension: 0.3,
                                pointRadius: equityData.length > 100 ? 2 : 3,
                                pointHoverRadius: 6,
                                pointBackgroundColor: data.current_equity >= data.initial_capital ? '#10b981' : '#ef4444',
                                pointBorderColor: '#fff',
                                pointBorderWidth: 2
                            }},
                            {{
                                label: 'Starting Capital',
                                data: startingCapitalData,
                                borderColor: '#9ca3af',
                                borderWidth: 1,
                                borderDash: [5, 5],
                                fill: false,
                                pointRadius: 0,
                                tension: 0
                            }}]
                        }},
                        options: {{
                            responsive: true,
                            maintainAspectRatio: false,
                            interaction: {{
                                intersect: false,
                                mode: 'index'
                            }},
                            plugins: {{
                                legend: {{
                                    display: true,
                                    position: 'top',
                                    labels: {{
                                        usePointStyle: true,
                                        padding: 20
                                    }}
                                }},
                                tooltip: {{
                                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                                    titleColor: '#fff',
                                    bodyColor: '#fff',
                                    padding: 12,
                                    displayColors: false,
                                    callbacks: {{
                                        title: function(context) {{
                                            const idx = context[0].dataIndex;
                                            const point = data.equity_curve[idx];
                                            return point.trade || 'Balance';
                                        }},
                                        label: function(context) {{
                                            const idx = context.dataIndex;
                                            const point = data.equity_curve[idx];
                                            const lines = [`Equity: $${{point.equity.toLocaleString()}}`];
                                            if (point.pnl !== 0) {{
                                                const pnlStr = point.pnl >= 0 ? `+$${{point.pnl}}` : `-$${{Math.abs(point.pnl)}}`;
                                                lines.push(`Trade PnL: ${{pnlStr}}`);
                                            }}
                                            lines.push(`Cumulative: $${{point.cumulative_pnl >= 0 ? '+' : ''}}${{point.cumulative_pnl}}`);
                                            return lines;
                                        }}
                                    }}
                                }}
                            }},
                            scales: {{
                                x: {{
                                    type: 'time',
                                    time: {{
                                        unit: 'day',
                                        displayFormats: {{
                                            day: 'MMM d'
                                        }},
                                        tooltipFormat: 'MMM d, yyyy h:mm a'
                                    }},
                                    grid: {{
                                        display: false
                                    }},
                                    ticks: {{
                                        maxTicksLimit: 8,
                                        color: '#6b7280'
                                    }}
                                }},
                                y: {{
                                    grid: {{
                                        color: 'rgba(0, 0, 0, 0.05)'
                                    }},
                                    ticks: {{
                                        callback: function(value) {{
                                            return '$ ' + value.toLocaleString();
                                        }},
                                        color: '#6b7280'
                                    }}
                                }}
                            }}
                        }}
                    }});
                }} else if (data.status === 'no_trades' || data.equity_curve?.length <= 1) {{
                    // No trades yet - show friendly message with flat line hint
                    document.getElementById('equity-chart-container').innerHTML = `
                        <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: #6b7280; text-align: center; padding: 20px;">
                            <div style="font-size: 48px; margin-bottom: 15px;">📊</div>
                            <div style="font-size: 18px; font-weight: 600; margin-bottom: 8px;">No Trades Yet</div>
                            <div style="font-size: 14px; color: #9ca3af;">Your equity curve will appear here once trades are executed</div>
                        </div>
                    `;
                }} else {{
                    // Unknown status - show waiting message
                    document.getElementById('equity-chart-container').innerHTML = `
                        <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: #6b7280; text-align: center; padding: 20px;">
                            <div style="font-size: 48px; margin-bottom: 15px;">⏳</div>
                            <div style="font-size: 18px; font-weight: 600; margin-bottom: 8px;">Waiting for Data</div>
                            <div style="font-size: 14px; color: #9ca3af;">Chart will load once trading begins</div>
                        </div>
                    `;
                }}
            }} catch (error) {{
                console.error('Error loading equity curve:', error);
                // Show friendly message instead of scary red error
                document.getElementById('equity-chart-container').innerHTML = `
                    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: #6b7280; text-align: center; padding: 20px;">
                        <div style="font-size: 48px; margin-bottom: 15px;">📊</div>
                        <div style="font-size: 18px; font-weight: 600; margin-bottom: 8px;">No Trading Data</div>
                        <div style="font-size: 14px; color: #9ca3af;">Start trading to see your equity curve</div>
                    </div>
                `;
            }}
        }}
        
        // ==================== OPEN POSITIONS FUNCTIONS ====================
        async function loadPositions() {{
            try {{
                const response = await fetch(`/api/portfolio/open-positions?key=${{currentApiKey}}`);
                const data = await response.json();
                
                const listDiv = document.getElementById('open-positions-list');
                const countSpan = document.getElementById('open-positions-count');
                
                if (data.status === 'success' && data.positions && data.positions.length > 0) {{
                    // Store positions globally for price updates
                    window.openPositions = data.positions;
                    
                    countSpan.textContent = `${{data.count}} open position${{data.count > 1 ? 's' : ''}}`;
                    
                    let html = `
                        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                            <thead>
                                <tr style="background: #f9fafb; border-bottom: 2px solid #e5e7eb;">
                                    <th style="padding: 12px 10px; text-align: left; color: #374151; font-weight: 600;">Symbol</th>
                                    <th style="padding: 12px 10px; text-align: left; color: #374151; font-weight: 600;">Side</th>
                                    <th style="padding: 12px 10px; text-align: right; color: #374151; font-weight: 600;">Size</th>
                                    <th style="padding: 12px 10px; text-align: right; color: #374151; font-weight: 600;">Entry Price</th>
                                    <th style="padding: 12px 10px; text-align: right; color: #374151; font-weight: 600;">Current Price</th>
                                    <th style="padding: 12px 10px; text-align: right; color: #374151; font-weight: 600;">Live P&L</th>
                                    <th style="padding: 12px 10px; text-align: right; color: #374151; font-weight: 600;">Take Profit</th>
                                    <th style="padding: 12px 10px; text-align: right; color: #374151; font-weight: 600;">Stop Loss</th>
                                    <th style="padding: 12px 10px; text-align: left; color: #374151; font-weight: 600;">Opened</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;
                    
                    for (const pos of data.positions) {{
                        const sideLower = (pos.side || '').toLowerCase();
                        const isLong = sideLower === 'long' || sideLower === 'buy';
                        const sideColor = isLong ? '#10b981' : '#ef4444';
                        const sideIcon = isLong ? '📈' : '📉';
                        const sideLabel = isLong ? 'LONG' : 'SHORT';
                        
                        const openedDate = pos.opened_at ? new Date(pos.opened_at).toLocaleString() : '-';
                        const entryPrice = pos.avg_entry_price || pos.entry_fill_price || 0;
                        const posId = pos.id || pos.symbol.replace('/', '-');
                        
                        html += `
                            <tr style="border-bottom: 1px solid #e5e7eb;" data-position-id="${{posId}}" data-hl-coin="${{pos.hl_coin}}" data-entry="${{entryPrice}}" data-size="${{pos.filled_quantity || pos.quantity || 0}}" data-side="${{sideLower}}">
                                <td style="padding: 12px 10px; font-weight: 600; color: #374151;">
                                    ${{pos.symbol || pos.hl_coin || '-'}}
                                </td>
                                <td style="padding: 12px 10px;">
                                    <span style="
                                        display: inline-flex;
                                        align-items: center;
                                        gap: 4px;
                                        padding: 4px 10px;
                                        border-radius: 12px;
                                        font-size: 12px;
                                        font-weight: 600;
                                        background: ${{sideColor}}20;
                                        color: ${{sideColor}};
                                    ">
                                        ${{sideIcon}} ${{sideLabel}}
                                    </span>
                                </td>
                                <td style="padding: 12px 10px; text-align: right; color: #374151;">
                                    ${{(pos.filled_quantity || pos.quantity || 0).toLocaleString()}}
                                </td>
                                <td style="padding: 12px 10px; text-align: right; color: #374151; font-weight: 500;">
                                    $${{entryPrice.toFixed(5)}}
                                </td>
                                <td style="padding: 12px 10px; text-align: right; color: #374151;" id="price-${{posId}}">
                                    <span style="color: #9ca3af;">Loading...</span>
                                </td>
                                <td style="padding: 12px 10px; text-align: right; font-weight: 600;" id="pnl-${{posId}}">
                                    <span style="color: #9ca3af;">--</span>
                                </td>
                                <td style="padding: 12px 10px; text-align: right;">
                                    ${{pos.target_tp ? `<span style="color: #10b981; font-weight: 500;">$${{pos.target_tp.toFixed(5)}}</span>` : '<span style="color: #9ca3af;">-</span>'}}
                                </td>
                                <td style="padding: 12px 10px; text-align: right;">
                                    ${{pos.target_sl ? `<span style="color: #ef4444; font-weight: 500;">$${{pos.target_sl.toFixed(5)}}</span>` : '<span style="color: #9ca3af;">-</span>'}}
                                </td>
                                <td style="padding: 12px 10px; color: #6b7280; font-size: 13px;">
                                    ${{openedDate}}
                                </td>
                            </tr>
                        `;
                    }}
                    
                    html += `
                            </tbody>
                        </table>
                    `;
                    
                    listDiv.innerHTML = html;
                    
                    // Start fetching live prices
                    updateLivePrices();
                }} else {{
                    window.openPositions = [];
                    countSpan.textContent = '0 positions';
                    listDiv.innerHTML = `
                        <div style="text-align: center; padding: 40px; color: #9ca3af;">
                            <div style="font-size: 48px; margin-bottom: 15px;">✨</div>
                            <div style="font-size: 16px; font-weight: 500; margin-bottom: 8px;">No Open Positions</div>
                            <div style="font-size: 14px;">Your active trades will appear here</div>
                        </div>
                    `;
                }}
            }} catch (error) {{
                console.error('Error loading positions:', error);
                document.getElementById('open-positions-list').innerHTML = `
                    <div style="text-align: center; padding: 40px; color: #ef4444;">
                        Error loading positions. Please try again.
                    </div>
                `;
            }}
        }}
        
        // Live price updates for open positions (refreshes every 10 seconds)
        let priceUpdateInterval = null;
        
        async function updateLivePrices() {{
            // Get all position rows with HL coins
            const rows = document.querySelectorAll('tr[data-hl-coin]');
            if (rows.length === 0) return;
            
            // Collect unique symbols
            const symbols = [...new Set([...rows].map(r => r.dataset.hlCoin).filter(s => s))];
            if (symbols.length === 0) return;
            
            try {{
                // Fetch prices from cached endpoint
                const response = await fetch(`/api/prices?symbols=${{symbols.join(',')}}`);
                const data = await response.json();
                
                if (data.prices) {{
                    // Update each position row
                    rows.forEach(row => {{
                        const symbol = row.dataset.hlCoin;
                        const entryPrice = parseFloat(row.dataset.entry);
                        const size = parseFloat(row.dataset.size);
                        const side = row.dataset.side?.toLowerCase();
                        const posId = row.dataset.positionId;
                        
                        const currentPrice = data.prices[symbol];
                        
                        if (currentPrice && entryPrice && size) {{
                            // Update current price display
                            const priceCell = document.getElementById(`price-${{posId}}`);
                            if (priceCell) {{
                                priceCell.innerHTML = `$${{currentPrice.toFixed(5)}}`;
                            }}
                            
                            // Calculate P&L based on side
                            let pnl;
                            if (side === 'long' || side === 'buy') {{
                                pnl = (currentPrice - entryPrice) * size;
                            }} else {{
                                pnl = (entryPrice - currentPrice) * size;
                            }}
                            
                            // Update P&L display
                            const pnlCell = document.getElementById(`pnl-${{posId}}`);
                            if (pnlCell) {{
                                const pnlColor = pnl >= 0 ? '#10b981' : '#ef4444';
                                const pnlSign = pnl >= 0 ? '+' : '';
                                pnlCell.innerHTML = `<span style="color: ${{pnlColor}};">${{pnlSign}}$${{pnl.toFixed(2)}}</span>`;
                            }}
                        }}
                    }});
                }}
            }} catch (error) {{
                console.error('Error fetching live prices:', error);
            }}
            
            // Schedule next update (10 seconds)
            if (priceUpdateInterval) clearTimeout(priceUpdateInterval);
            priceUpdateInterval = setTimeout(updateLivePrices, 10000);
        }}
        
        // Refresh entire dashboard
        async function refreshDashboard() {{
            const refreshBtn = document.querySelector('.refresh-btn');
            const originalText = refreshBtn.innerHTML;
            
            try {{
                // Show loading state
                refreshBtn.innerHTML = '⏳ Refreshing...';
                refreshBtn.disabled = true;
                
                // Refresh all dashboard data
                await loadBalanceSummary();
                await loadPositions();
                await loadTransactionHistory(true);
                await loadEquityCurve();
                await changePeriod();
                await checkAgentStatus();
                
                // Success feedback
                refreshBtn.innerHTML = '✅ Refreshed!';
                setTimeout(() => {{
                    refreshBtn.innerHTML = originalText;
                    refreshBtn.disabled = false;
                }}, 1500);
            }} catch (error) {{
                console.error('Error refreshing dashboard:', error);
                refreshBtn.innerHTML = '❌ Error';
                setTimeout(() => {{
                    refreshBtn.innerHTML = originalText;
                    refreshBtn.disabled = false;
                }}, 2000);
            }}
        }}
        
        async function changePeriod() {{
            currentPeriod = document.getElementById('period-selector').value;
            
            try {{
                const response = await fetch(`/api/portfolio/stats?period=${{currentPeriod}}`, {{
                    headers: {{'X-API-Key': currentApiKey}}
                }});
                
                const stats = await response.json();
                
                if (stats.status !== 'no_data') {{
                    updateDashboard(stats);
                }}
            }} catch (error) {{
                console.error('Error loading stats:', error);
            }}
        }}
        
        // ==================== SOCIAL SHARING FUNCTIONS (NEW!) ====================
        
        let selectedBackground = 'charles'; // Default background
        let selectorMode = 'download'; // 'download' or 'twitter'
        
        function shareToTwitter() {{
            // Get profit from the MASSIVE ROCKET PERFORMANCE section (period-specific)
            const profitElement = document.getElementById('total-profit');
            const roiElement = document.getElementById('roi-initial');
            
            if (!profitElement || !roiElement) {{
                alert('Portfolio data not loaded yet. Please wait a moment and try again.');
                return;
            }}
            
            const profit = profitElement.textContent;
            const roi = roiElement.textContent;
            
            // Get the ACTUAL selected period from dropdown
            const periodSelector = document.getElementById('period-selector');
            const period = periodSelector ? periodSelector.value : '30d';
            
            const periodLabels = {{
                '7d': '7 days',
                '30d': '30 days',
                '90d': '90 days',
                '1y': '1 year',
                'all': 'all-time'
            }};
            
            // Prepare Twitter URL BEFORE generating image
            const text = `$NIKEPIG's Massive Rocket ${{periodLabels[period] || period}} Performance Card

Profit: ${{profit}}
ROI: ${{roi}}`;
            
            const twitterUrl = `https://twitter.com/intent/tweet?text=${{encodeURIComponent(text)}}`;
            
            // Generate the performance card image
            generateImageForShare(profit, roi, period, periodLabels[period], (imageBlob) => {{
                // Download the image automatically
                const url = URL.createObjectURL(imageBlob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `nikepig-performance-${{period}}.png`;
                a.click();
                URL.revokeObjectURL(url);
                
                // Try to copy image to clipboard (modern browsers only)
                if (navigator.clipboard && navigator.clipboard.write) {{
                    const item = new ClipboardItem({{ 'image/png': imageBlob }});
                    navigator.clipboard.write([item]).then(() => {{
                        console.log('✅ Image copied to clipboard!');
                    }}).catch(err => {{
                        console.log('⚠️ Could not copy to clipboard:', err);
                    }});
                }}
                
                // IMMEDIATELY open Twitter (no setTimeout, no blocking alert!)
                const twitterWindow = window.open(twitterUrl, '_blank');
                
                // Close the background selector modal
                toggleBackgroundSelector();
                
                // Show non-blocking alert AFTER opening Twitter
                setTimeout(() => {{
                    alert('📸 Performance card downloaded!\\n\\n💡 Tip: The image may be in your clipboard - just paste it into your tweet!\\n\\nOr click "Add photos" to attach the downloaded image.');
                }}, 100);
            }});
        }}
        
        function generateImageForShare(profit, roi, period, periodLabel, callback) {{
            // Create canvas with same specs as downloadPerformanceCard
            const canvas = document.createElement('canvas');
            canvas.width = 1200;
            canvas.height = 630;
            const ctx = canvas.getContext('2d');
            
            const backgroundUrls = {{
                'charles': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-charles.png',
                'casino': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-casino.png',
                'gaming': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-gaming.png',
                'money': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-money.png'
            }};
            
            const logoUrl = 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/nikepig-logo.png';
            
            // Load background image
            const bgImage = new Image();
            bgImage.crossOrigin = 'anonymous';
            bgImage.onload = function() {{
                // Draw background
                ctx.drawImage(bgImage, 0, 0, canvas.width, canvas.height);
                
                // Add overlay
                ctx.fillStyle = 'rgba(0,0,0,0.35)';
                ctx.fillRect(0, 0, canvas.width, canvas.height);
                
                // Load logo
                const logo = new Image();
                logo.crossOrigin = 'anonymous';
                logo.onload = function() {{
                    // Draw logo
                    const logoHeight = 100;
                    const logoWidth = (logo.width / logo.height) * logoHeight;
                    ctx.drawImage(logo, 50, 50, logoWidth, logoHeight);
                    
                    // PROFIT label
                    ctx.fillStyle = 'white';
                    ctx.font = '40px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.textAlign = 'left';
                    ctx.shadowColor = 'rgba(0,0,0,0.8)';
                    ctx.shadowBlur = 8;
                    ctx.shadowOffsetX = 2;
                    ctx.shadowOffsetY = 2;
                    ctx.fillText('PROFIT', 50, 230);
                    
                    // Profit number
                    ctx.fillStyle = '#00FF88';
                    ctx.font = 'bold 140px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.shadowBlur = 15;
                    ctx.shadowOffsetX = 3;
                    ctx.shadowOffsetY = 3;
                    ctx.fillText(profit, 50, 360);
                    
                    // ROI label
                    ctx.fillStyle = 'white';
                    ctx.font = '40px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.shadowBlur = 8;
                    ctx.shadowOffsetX = 2;
                    ctx.shadowOffsetY = 2;
                    ctx.fillText('ROI', 50, 450);
                    
                    // ROI number
                    const roiColor = roi.includes('+') || !roi.includes('-') ? '#00FF88' : '#FF4444';
                    ctx.fillStyle = roiColor;
                    ctx.font = 'bold 100px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.shadowBlur = 12;
                    ctx.shadowOffsetX = 3;
                    ctx.shadowOffsetY = 3;
                    ctx.fillText(roi, 50, 540);
                    
                    // "over X days"
                    ctx.fillStyle = 'white';
                    ctx.font = '32px Arial, sans-serif';
                    ctx.shadowBlur = 8;
                    ctx.fillText(`over ${{periodLabel}}`, 50, 580);
                    
                    // Convert to blob and callback
                    canvas.toBlob(callback);
                }};
                logo.src = logoUrl;
            }};
            bgImage.src = backgroundUrls[selectedBackground];
        }}
        
        
        function toggleBackgroundSelector() {{
            const selector = document.getElementById('background-selector');
            selector.style.display = selector.style.display === 'none' ? 'block' : 'none';
        }}
        
        function showBackgroundSelectorForDownload() {{
            selectorMode = 'download';
            const btn = document.getElementById('selector-action-btn');
            btn.textContent = '✅ Download Image';
            btn.style.background = '#10b981';
            toggleBackgroundSelector();
        }}
        
        function showBackgroundSelectorForTwitter() {{
            selectorMode = 'twitter';
            const btn = document.getElementById('selector-action-btn');
            btn.textContent = '𝕏 Share to Twitter';
            btn.style.background = '#1da1f2';
            toggleBackgroundSelector();
        }}
        
        function handleSelectorAction() {{
            if (selectorMode === 'twitter') {{
                shareToTwitter();
            }} else {{
                downloadPerformanceCard();
            }}
        }}
        
        function selectBackground(bgType) {{
            selectedBackground = bgType;
            
            // Update visual selection - highlight selected background
            document.querySelectorAll('.bg-option').forEach(el => {{
                el.style.border = '3px solid transparent';
                el.style.transform = 'scale(1)';
                el.style.boxShadow = 'none';
            }});
            
            const selected = document.querySelector(`[data-bg="${{bgType}}"]`);
            selected.style.border = '3px solid #667eea';
            selected.style.transform = 'scale(1.05)';
            selected.style.boxShadow = '0 4px 12px rgba(102, 126, 234, 0.5)';
        }}
        
        function downloadPerformanceCard() {{
            // Get profit from the MASSIVE ROCKET PERFORMANCE section (period-specific)
            const profitElement = document.getElementById('total-profit');
            const roiElement = document.getElementById('roi-initial');
            
            if (!profitElement || !roiElement) {{
                alert('Portfolio data not loaded yet. Please wait a moment and try again.');
                toggleBackgroundSelector();
                return;
            }}
            
            const profit = profitElement.textContent;
            const roi = roiElement.textContent;
            const period = currentPeriod; // Use selected time period
            
            const periodLabels = {{
                '7d': '7 days',
                '30d': '30 days',
                '90d': '90 days',
                '1y': '1 year',
                'all': 'all-time'
            }};
            
            // Create canvas
            const canvas = document.createElement('canvas');
            canvas.width = 1200;
            canvas.height = 630;
            const ctx = canvas.getContext('2d');
            
            // Background image URLs from GitHub
            const backgroundUrls = {{
                'charles': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-charles.png',
                'casino': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-casino.png',
                'gaming': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-gaming.png',
                'money': 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/bg-money.png'
            }};
            
            const logoUrl = 'https://raw.githubusercontent.com/DrCalebL/hyperliquid-nike-rocket-api/main/static/nikepig-logo.png';
            
            // Load background image
            const bgImage = new Image();
            bgImage.crossOrigin = 'anonymous';
            bgImage.onload = function() {{
                // Draw background image (cover the entire canvas)
                ctx.drawImage(bgImage, 0, 0, canvas.width, canvas.height);
                
                // Add dark overlay for text readability
                ctx.fillStyle = 'rgba(0,0,0,0.35)';
                ctx.fillRect(0, 0, canvas.width, canvas.height);
                
                // Load and draw NIKEPIG logo
                const logo = new Image();
                logo.crossOrigin = 'anonymous';
                logo.onload = function() {{
                    // Draw logo (top-left, scaled)
                    const logoHeight = 100;
                    const logoWidth = (logo.width / logo.height) * logoHeight;
                    ctx.drawImage(logo, 50, 50, logoWidth, logoHeight);
                    
                    // PROFIT label (fully opaque + shadow)
                    ctx.fillStyle = 'white';
                    ctx.font = '40px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.textAlign = 'left';
                    ctx.shadowColor = 'rgba(0,0,0,0.8)';
                    ctx.shadowBlur = 8;
                    ctx.shadowOffsetX = 2;
                    ctx.shadowOffsetY = 2;
                    ctx.fillText('PROFIT', 50, 230);
                    
                    // HUGE Profit number (bright green + shadow)
                    ctx.fillStyle = '#00FF88';
                    ctx.font = 'bold 140px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.shadowColor = 'rgba(0,0,0,0.8)';
                    ctx.shadowBlur = 15;
                    ctx.shadowOffsetX = 3;
                    ctx.shadowOffsetY = 3;
                    ctx.fillText(profit, 50, 360);
                    
                    // ROI label (fully opaque + shadow)
                    ctx.fillStyle = 'white';
                    ctx.font = '40px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.shadowColor = 'rgba(0,0,0,0.8)';
                    ctx.shadowBlur = 8;
                    ctx.shadowOffsetX = 2;
                    ctx.shadowOffsetY = 2;
                    ctx.fillText('ROI', 50, 450);
                    
                    // ROI percentage (bright green + shadow)
                    const roiColor = roi.includes('+') || !roi.includes('-') ? '#00FF88' : '#FF4444';
                    ctx.fillStyle = roiColor;
                    ctx.font = 'bold 100px "Bebas Neue", Impact, Arial, sans-serif';
                    ctx.shadowColor = 'rgba(0,0,0,0.8)';
                    ctx.shadowBlur = 12;
                    ctx.shadowOffsetX = 3;
                    ctx.shadowOffsetY = 3;
                    ctx.fillText(roi, 50, 540);
                    
                    // "over X days" text DIRECTLY BELOW ROI (fully opaque + shadow)
                    ctx.fillStyle = 'white';
                    ctx.font = '32px Arial, sans-serif';
                    ctx.textAlign = 'left';
                    ctx.shadowColor = 'rgba(0,0,0,0.8)';
                    ctx.shadowBlur = 8;
                    ctx.shadowOffsetX = 2;
                    ctx.shadowOffsetY = 2;
                    ctx.fillText(`over ${{periodLabels[period]}}`, 50, 580);
                    
                    // Download
                    canvas.toBlob((blob) => {{
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = `nikepig-massive-rocket-${{period}}-performance.png`;
                        a.click();
                        URL.revokeObjectURL(url);
                        
                        // Hide selector after download
                        document.getElementById('background-selector').style.display = 'none';
                    }});
                }};
                logo.onerror = function() {{
                    console.error('Failed to load NIKEPIG logo');
                    // Continue without logo
                    finishCard();
                }};
                logo.src = logoUrl;
            }};
            bgImage.onerror = function() {{
                console.error('Failed to load background image');
                alert('Failed to load background image. Please make sure images are uploaded to GitHub at: static/bg-' + selectedBackground + '.png');
            }};
            bgImage.src = backgroundUrls[selectedBackground];
            
            function finishCard() {{
                // Fallback if logo fails - still download the card
                canvas.toBlob((blob) => {{
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `nikepig-massive-rocket-${{period}}-performance.png`;
                    a.click();
                    URL.revokeObjectURL(url);
                    document.getElementById('background-selector').style.display = 'none';
                }});
            }}
        }}
        
        // ==================== AGENT CONTROL FUNCTIONS (NEW!) ====================
        
        async function checkAgentStatus() {{
            try {{
                const response = await fetch('/api/agent-status', {{
                    headers: {{'X-API-Key': currentApiKey}}
                }});
                
                const data = await response.json();
                
                // ========== UPDATE TOP BANNER ==========
                const topBanner = document.getElementById('agent-status-display');
                
                // API returns: agent_configured, agent_active, message
                if (data.agent_active) {{
                    // Agent is running
                    if (topBanner) {{
                        topBanner.innerHTML = '🟢 <strong>Agent Active</strong> - Following signals';
                        topBanner.className = 'agent-status status-active';
                    }}
                    
                    document.getElementById('agent-status-badge').innerHTML = '🟢 Running';
                    document.getElementById('agent-status-badge').style.background = '#d1fae5';
                    document.getElementById('agent-status-badge').style.color = '#065f46';
                    
                    document.getElementById('start-agent-btn').style.display = 'none';
                    document.getElementById('stop-agent-btn').style.display = 'block';
                    
                    document.getElementById('agent-details').textContent = 'Agent is active and following signals';
                    
                }} else if (data.agent_configured) {{
                    // Agent configured but not active
                    if (topBanner) {{
                        topBanner.innerHTML = '🟡 <strong>Ready</strong> - Agent configured but stopped';
                        topBanner.className = 'agent-status status-ready';
                    }}
                    
                    document.getElementById('agent-status-badge').innerHTML = '🟡 Ready';
                    document.getElementById('agent-status-badge').style.background = '#fef3c7';
                    document.getElementById('agent-status-badge').style.color = '#92400e';
                    
                    document.getElementById('start-agent-btn').style.display = 'block';
                    document.getElementById('stop-agent-btn').style.display = 'none';
                    
                    document.getElementById('agent-details').textContent = 'Agent configured - click Start to begin trading';
                    
                }} else {{
                    // Agent not configured
                    if (topBanner) {{
                        topBanner.innerHTML = '🔴 <strong>Not Configured</strong> - <a href="/setup?key=' + currentApiKey + '" style="color: #dc2626;">Complete setup</a>';
                        topBanner.className = 'agent-status status-error';
                    }}
                    
                    document.getElementById('agent-status-badge').innerHTML = '🔴 Not Configured';
                    document.getElementById('agent-status-badge').style.background = '#fee2e2';
                    document.getElementById('agent-status-badge').style.color = '#991b1b';
                    
                    document.getElementById('start-agent-btn').style.display = 'none';
                    document.getElementById('stop-agent-btn').style.display = 'none';
                    
                    document.getElementById('agent-details').innerHTML = 
                        '<a href="/setup?key=' + currentApiKey + '" style="color: #667eea;">Set up your agent first →</a>';
                }}
                
            }} catch (error) {{
                console.error('Error checking agent status:', error);
                
                const topBanner = document.getElementById('agent-status-display');
                if (topBanner) {{
                    topBanner.innerHTML = '❌ <strong>Error</strong> - Could not check status';
                    topBanner.className = 'agent-status status-error';
                }}
                
                document.getElementById('agent-status-badge').innerHTML = '❌ Error';
                document.getElementById('agent-status-badge').style.background = '#fee2e2';
                document.getElementById('agent-status-badge').style.color = '#991b1b';
                document.getElementById('agent-details').textContent = 'Could not check agent status';
            }}
        }}
        
        async function startAgent() {{
            const startBtn = document.getElementById('start-agent-btn');
            const stopBtn = document.getElementById('stop-agent-btn');
            startBtn.disabled = true;
            startBtn.textContent = '⏳ Starting...';
            
            try {{
                const response = await fetch('/api/start-agent', {{
                    method: 'POST',
                    headers: {{
                        'X-API-Key': currentApiKey,
                        'Content-Type': 'application/json'
                    }}
                }});
                
                const data = await response.json();
                
                const messageEl = document.getElementById('agent-message');
                
                if (data.status === 'success') {{
                    messageEl.style.display = 'block';
                    messageEl.style.background = '#d1fae5';
                    messageEl.style.color = '#065f46';
                    messageEl.textContent = '✅ Agent started successfully!';
                    
                    // Reset start button state BEFORE hiding it
                    startBtn.disabled = false;
                    startBtn.textContent = '▶️ Start Agent';
                    
                    // Ensure stop button is in correct state
                    stopBtn.disabled = false;
                    stopBtn.textContent = '⏸️ Stop Agent';
                    
                    // Refresh status after 2 seconds
                    setTimeout(() => {{
                        checkAgentStatus();
                        messageEl.style.display = 'none';
                    }}, 2000);
                }} else if (data.redirect) {{
                    // Not configured - redirect to setup
                    messageEl.style.display = 'block';
                    messageEl.style.background = '#fef3c7';
                    messageEl.style.color = '#92400e';
                    messageEl.textContent = '⚠️ ' + data.message;
                    
                    setTimeout(() => {{
                        window.location.href = data.redirect;
                    }}, 2000);
                }} else {{
                    messageEl.style.display = 'block';
                    messageEl.style.background = '#fee2e2';
                    messageEl.style.color = '#991b1b';
                    messageEl.textContent = '❌ ' + (data.message || 'Failed to start agent');
                    
                    startBtn.disabled = false;
                    startBtn.textContent = '▶️ Start Agent';
                }}
            }} catch (error) {{
                console.error('Error starting agent:', error);
                const messageEl = document.getElementById('agent-message');
                messageEl.style.display = 'block';
                messageEl.style.background = '#fee2e2';
                messageEl.style.color = '#991b1b';
                messageEl.textContent = '❌ Error starting agent: ' + error.message;
                
                startBtn.disabled = false;
                startBtn.textContent = '▶️ Start Agent';
            }}
        }}
        
        async function stopAgent() {{
            const stopBtn = document.getElementById('stop-agent-btn');
            const startBtn = document.getElementById('start-agent-btn');
            stopBtn.disabled = true;
            stopBtn.textContent = '⏳ Stopping...';
            
            try {{
                const response = await fetch('/api/stop-agent', {{
                    method: 'POST',
                    headers: {{
                        'X-API-Key': currentApiKey,
                        'Content-Type': 'application/json'
                    }}
                }});
                
                const data = await response.json();
                
                const messageEl = document.getElementById('agent-message');
                
                if (data.status === 'success') {{
                    messageEl.style.display = 'block';
                    messageEl.style.background = '#d1fae5';
                    messageEl.style.color = '#065f46';
                    messageEl.textContent = '✅ Agent stopped successfully!';
                    
                    // Reset stop button state BEFORE hiding it
                    stopBtn.disabled = false;
                    stopBtn.textContent = '⏸️ Stop Agent';
                    
                    // Ensure start button is in correct state
                    startBtn.disabled = false;
                    startBtn.textContent = '▶️ Start Agent';
                    
                    // Refresh status after 2 seconds
                    setTimeout(() => {{
                        checkAgentStatus();
                        messageEl.style.display = 'none';
                    }}, 2000);
                }} else {{
                    messageEl.style.display = 'block';
                    messageEl.style.background = '#fee2e2';
                    messageEl.style.color = '#991b1b';
                    messageEl.textContent = '❌ ' + (data.message || 'Failed to stop agent');
                    
                    stopBtn.disabled = false;
                    stopBtn.textContent = '⏸️ Stop Agent';
                }}
            }} catch (error) {{
                console.error('Error stopping agent:', error);
                const messageEl = document.getElementById('agent-message');
                messageEl.style.display = 'block';
                messageEl.style.background = '#fee2e2';
                messageEl.style.color = '#991b1b';
                messageEl.textContent = '❌ Error stopping agent: ' + error.message;
                
                stopBtn.disabled = false;
                stopBtn.textContent = '⏸️ Stop Agent';
            }}
        }}
        
        // ═══════════════════════════════════════════════════════════════
        // Trade Export Functions
        // ═══════════════════════════════════════════════════════════════
        
        function initExportControls() {{
            // Populate year dropdowns
            const currentYear = new Date().getFullYear();
            const currentMonth = new Date().getMonth() + 1;
            
            const monthYearSelect = document.getElementById('export-month-year');
            const yearSelect = document.getElementById('export-year');
            
            // Add years (current and past 2 years)
            for (let y = currentYear; y >= currentYear - 2; y--) {{
                monthYearSelect.innerHTML += `<option value="${{y}}">${{y}}</option>`;
                yearSelect.innerHTML += `<option value="${{y}}">${{y}}</option>`;
            }}
            
            // Set current month
            document.getElementById('export-month').value = String(currentMonth).padStart(2, '0');
        }}
        
        function downloadMonthlyTrades() {{
            const month = document.getElementById('export-month').value;
            const year = document.getElementById('export-month-year').value;
            
            const url = `/api/portfolio/trades/monthly-csv?key=${{currentApiKey}}&year=${{year}}&month=${{month}}`;
            window.location.href = url;
        }}
        
        function downloadYearlyTrades() {{
            const year = document.getElementById('export-year').value;
            
            const url = `/api/portfolio/trades/yearly-csv?key=${{currentApiKey}}&year=${{year}}`;
            window.location.href = url;
        }}
        
        function showError(elementId, message) {{
            const el = document.getElementById(elementId);
            el.className = 'error';
            el.innerHTML = '❌ ' + message;  // Use innerHTML to render HTML tags
            el.style.display = 'block';
        }}
        
        function showSuccess(elementId, message) {{
            const el = document.getElementById(elementId);
            el.className = 'success';
            el.textContent = '✅ ' + message;
            el.style.display = 'block';
        }}
    </script>
</body>
</html>
    """
    
    return html

# Startup event - CRITICAL FIX HERE!
@app.on_event("startup")
async def startup_event():
    global _db_pool
    
    print("=" * 60)
    print("🚀 NIKE ROCKET FOLLOWER API STARTED")
    print("=" * 60)
    print("✅ Database connected")
    print("✅ Follower routes loaded")
    print("✅ Portfolio routes loaded")
    print("✅ Billing routes loaded (30-day rolling)")
    print("✅ Signup page available at /signup")
    print("✅ Setup page available at /setup")
    print("✅ Dashboard available at /dashboard")
    print("✅ Ready to receive signals")
    
    # Start balance checker for automatic deposit/withdrawal detection
    # CRITICAL FIX: WITH STARTUP DELAY TO PREVENT RACE CONDITION!
    if DATABASE_URL:
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            _db_pool = db_pool  # Set global for billing endpoints
            
            # ═══════════════════════════════════════════════════════════
            # DEFENSIVE MIGRATIONS: Ensure all tables/columns exist
            # Prevents "relation does not exist" errors from race conditions
            # ═══════════════════════════════════════════════════════════
            try:
                async with db_pool.acquire() as conn:
                    # Ensure billing_invoices exists
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS billing_invoices (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER NOT NULL,
                            billing_cycle_id INTEGER,
                            coinbase_charge_id TEXT,
                            coinbase_charge_code TEXT,
                            hosted_url TEXT,
                            amount_usd NUMERIC(20,8) DEFAULT 0,
                            profit_amount NUMERIC(20,8) DEFAULT 0,
                            fee_tier VARCHAR(20),
                            fee_percentage NUMERIC(5,4) DEFAULT 0,
                            cycle_start TIMESTAMP,
                            cycle_end TIMESTAMP,
                            expires_at TIMESTAMP,
                            status VARCHAR(20) DEFAULT 'pending',
                            paid_at TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    # Ensure last_known_balance column exists
                    await conn.execute("""
                        ALTER TABLE follower_users 
                        ADD COLUMN IF NOT EXISTS last_known_balance NUMERIC
                    """)
                    # Ensure error_logs has DEFAULT on created_at
                    await conn.execute("""
                        ALTER TABLE error_logs ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP
                    """)
                    # Backfill any NULL timestamps (30 days ago so they don't inflate recent counts)
                    await conn.execute("""
                        UPDATE error_logs SET created_at = NOW() - INTERVAL '30 days' WHERE created_at IS NULL
                    """)
                print("✅ Async defensive migrations complete")
            except Exception as e:
                print(f"Note: Async migration check - {e}")
            
            # ═══════════════════════════════════════════════════════════
            # CRITICAL FIX: Added startup_delay_seconds parameter!
            # This prevents the "relation does not exist" error by waiting
            # for database tables to be created before starting balance checker
            # ═══════════════════════════════════════════════════════════
            scheduler = BalanceCheckerScheduler(
                db_pool, 
                check_interval_minutes=60,
                startup_delay_seconds=30  # ← CRITICAL FIX: Wait 30s!
            )
            
            asyncio.create_task(scheduler.start())
            print("⏳ Balance checker scheduled (starts in 30 seconds)")
            
            # ═══════════════════════════════════════════════════════════
            # HOSTED TRADING LOOP: Executes trades for all active users
            # Polls signals and places orders on Hyperliquid
            # ═══════════════════════════════════════════════════════════
            asyncio.create_task(start_hosted_trading(db_pool))
            print("🤖 Hosted trading loop scheduled (starts in 35 seconds)")
            
            # ═══════════════════════════════════════════════════════════
            # POSITION MONITOR: Tracks open positions for TP/SL fills
            # Records P&L when trades close. Profits accumulated for 30-day billing.
            # ═══════════════════════════════════════════════════════════
            asyncio.create_task(start_position_monitor(db_pool))
            print("📊 Position monitor scheduled (starts in 40 seconds)")
            
            # ═══════════════════════════════════════════════════════════
            # BILLING SCHEDULER v2: 30-Day Rolling Billing
            # Checks for cycle endings every hour, generates Coinbase invoices
            # ═══════════════════════════════════════════════════════════
            asyncio.create_task(start_billing_scheduler_v2(db_pool))
            print("💰 Billing scheduler v2 scheduled (30-day rolling, starts in 60 seconds)")
            
            # ═══════════════════════════════════════════════════════════
            # API EXPIRY CHECKER: Monitors API wallet expiration dates
            # Checks every 6 hours, sends email reminders at 30/14/7/3/1 days
            # ═══════════════════════════════════════════════════════════
            asyncio.create_task(start_expiry_checker(db_pool))
            print("🔑 API expiry checker scheduled (checks every 6 hours)")
            
        except Exception as e:
            print(f"⚠️ Background tasks failed to start: {e}")
    
    print("=" * 60)

# Run locally for testing
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
