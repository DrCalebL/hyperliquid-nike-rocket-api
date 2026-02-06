"""
Nike Rocket Hyperliquid Follower System - Main API
=====================================================

Main FastAPI app mirroring Kraken follower API, adapted for Hyperliquid.
Includes:
- Signal distribution endpoints
- Portfolio tracking dashboard
- Admin dashboard
- Signup/setup/login pages
- Background tasks (balance checker, billing)

Author: Nike Rocket Team
"""
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from typing import Optional
import os
import asyncio
import asyncpg
import json
import traceback
import logging

# Import follower system
from follower_models import init_db
from follower_endpoints import router as follower_router

# Import portfolio system
from portfolio_api import router as portfolio_router

logger = logging.getLogger(__name__)

# Database URL
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "nikerocket2025")

# Initialize FastAPI
app = FastAPI(
    title="Nike Rocket Hyperliquid Follower API",
    description="Trading signal distribution and profit tracking for Hyperliquid",
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

# Include routers
app.include_router(follower_router)
app.include_router(portfolio_router)

# Global DB pool reference
_db_pool = None


# ==================== GLOBAL EXCEPTION HANDLER ====================

async def log_error_to_db_global(api_key: str, error_type: str, error_message: str, context: dict = None):
    """Log unhandled exceptions to error_logs table"""
    try:
        if not DATABASE_URL:
            return
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """INSERT INTO error_logs (api_key, error_type, error_message, context) 
               VALUES ($1, $2, $3, $4)""",
            api_key or "system",
            error_type,
            error_message[:500] if error_message else None,
            json.dumps(context) if context else None
        )
        await conn.close()
    except Exception:
        pass


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch ALL unhandled exceptions and log to error_logs table"""
    error_message = f"{type(exc).__name__}: {str(exc)[:500]}"
    api_key = request.headers.get("X-API-Key") or request.query_params.get("key") or "unknown"
    
    try:
        await log_error_to_db_global(
            api_key=str(api_key)[:50],
            error_type=f"unhandled_{type(exc).__name__}",
            error_message=error_message,
            context={
                "path": str(request.url.path),
                "method": request.method,
                "traceback": traceback.format_exc()[:1000]
            }
        )
    except Exception:
        pass
    
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error": str(exc)[:200]}
    )


# ==================== CORE ROUTES ====================

@app.get("/")
async def root():
    return {
        "service": "Nike Rocket Hyperliquid Follower API",
        "exchange": "Hyperliquid",
        "status": "operational",
        "pages": {
            "signup": "/signup",
            "dashboard": "/dashboard",
            "setup": "/setup",
            "login": "/login",
            "admin": "/admin"
        },
        "api": {
            "broadcast_signal": "/api/broadcast-signal",
            "latest_signal": "/api/latest-signal",
            "confirm_execution": "/api/confirm-execution",
            "register": "/api/users/register",
            "verify": "/api/users/verify",
            "setup_agent": "/api/setup-agent",
            "portfolio_stats": "/api/portfolio/stats",
            "equity_curve": "/api/portfolio/equity-curve"
        }
    }


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"status": "healthy", "exchange": "hyperliquid"}


# ==================== SIGNUP PAGE ====================

@app.get("/signup", response_class=HTMLResponse)
async def signup_page():
    return HTMLResponse(SIGNUP_HTML)


# ==================== SETUP PAGE ====================

@app.get("/setup", response_class=HTMLResponse)
async def setup_page():
    return HTMLResponse(SETUP_HTML)


# ==================== LOGIN PAGE ====================

@app.get("/login", response_class=HTMLResponse)
@app.get("/access", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(LOGIN_HTML)


# ==================== DASHBOARD ====================

@app.get("/dashboard", response_class=HTMLResponse)
async def portfolio_dashboard(request: Request):
    api_key = request.query_params.get('key', '')
    return HTMLResponse(generate_dashboard_html(api_key))


# ==================== ADMIN DASHBOARD ====================

@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(password: str = ""):
    if password != ADMIN_PASSWORD:
        return HTMLResponse("""
        <!DOCTYPE html>
        <html><head><title>Admin Login</title>
        <style>
            body { font-family: sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; background: #1a1a2e; color: #fff; }
            .login { background: #16213e; padding: 40px; border-radius: 12px; text-align: center; }
            input { padding: 12px; margin: 10px; border-radius: 6px; border: 1px solid #555; background: #0f3460; color: #fff; }
            button { padding: 12px 24px; background: #e94560; border: none; border-radius: 6px; color: #fff; cursor: pointer; font-size: 16px; }
        </style></head>
        <body><div class="login">
            <h2>🐷 Nike Rocket Admin</h2>
            <form><input type="password" name="password" placeholder="Admin Password">
            <br><button type="submit">Login</button></form>
        </div></body></html>
        """)
    
    try:
        if not DATABASE_URL:
            return HTMLResponse("<h1>DATABASE_URL not set</h1>")
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        users = await conn.fetch("""
            SELECT id, email, api_key, fee_tier, access_granted, credentials_set,
                   agent_active, total_profit, total_trades, current_cycle_profit,
                   current_cycle_trades, pending_invoice_amount, hl_wallet_address,
                   created_at
            FROM follower_users ORDER BY created_at DESC
        """)
        
        errors = await conn.fetch("""
            SELECT api_key, error_type, error_message, created_at
            FROM error_logs ORDER BY created_at DESC LIMIT 50
        """)
        
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_users,
                COUNT(*) FILTER (WHERE access_granted = true) as active_users,
                COUNT(*) FILTER (WHERE credentials_set = true) as configured_users,
                COUNT(*) FILTER (WHERE agent_active = true) as active_agents
            FROM follower_users
        """)
        
        trade_stats = await conn.fetchrow("""
            SELECT COUNT(*) as total_trades, COALESCE(SUM(profit_usd), 0) as total_profit
            FROM trades
        """)
        
        signal_count = await conn.fetchval("SELECT COUNT(*) FROM signals")
        
        await conn.close()
        
        # Build admin HTML
        users_html = ""
        for u in users:
            status = "🟢" if u['access_granted'] else "🔴"
            agent = "🤖 Active" if u['agent_active'] else ("⚙️ Configured" if u['credentials_set'] else "❌ Not setup")
            wallet = u['hl_wallet_address'][:10] + "..." if u['hl_wallet_address'] else "N/A"
            users_html += f"""
            <tr>
                <td>{u['email']}</td>
                <td>{status} {u['fee_tier'] or 'standard'}</td>
                <td>{agent}</td>
                <td>{wallet}</td>
                <td>${float(u['total_profit'] or 0):,.2f}</td>
                <td>{u['total_trades'] or 0}</td>
                <td>${float(u['current_cycle_profit'] or 0):,.2f}</td>
            </tr>"""
        
        errors_html = ""
        for e in errors[:20]:
            errors_html += f"""
            <tr>
                <td>{e['created_at'].strftime('%Y-%m-%d %H:%M') if e['created_at'] else 'N/A'}</td>
                <td>{e['error_type']}</td>
                <td>{(e['error_message'] or '')[:100]}</td>
            </tr>"""
        
        return HTMLResponse(f"""
        <!DOCTYPE html>
        <html><head><title>Nike Rocket Admin - Hyperliquid</title>
        <style>
            body {{ font-family: sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 20px; }}
            h1 {{ color: #e94560; }}
            h2 {{ color: #0f3460; margin-top: 30px; }}
            .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }}
            .stat {{ background: #16213e; padding: 20px; border-radius: 10px; text-align: center; }}
            .stat .number {{ font-size: 32px; font-weight: bold; color: #e94560; }}
            .stat .label {{ color: #999; margin-top: 5px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
            th, td {{ padding: 10px; border-bottom: 1px solid #333; text-align: left; }}
            th {{ background: #0f3460; color: #fff; }}
            tr:hover {{ background: #16213e; }}
        </style></head>
        <body>
        <h1>🐷🚀 Nike Rocket Admin - Hyperliquid</h1>
        
        <div class="stats">
            <div class="stat"><div class="number">{stats['total_users']}</div><div class="label">Total Users</div></div>
            <div class="stat"><div class="number">{stats['active_agents']}</div><div class="label">Active Agents</div></div>
            <div class="stat"><div class="number">{signal_count}</div><div class="label">Signals Broadcast</div></div>
            <div class="stat"><div class="number">${float(trade_stats['total_profit'] or 0):,.2f}</div><div class="label">Total Profit</div></div>
        </div>
        
        <h2>Users</h2>
        <table>
            <tr><th>Email</th><th>Status/Tier</th><th>Agent</th><th>Wallet</th><th>Total P&L</th><th>Trades</th><th>Cycle P&L</th></tr>
            {users_html}
        </table>
        
        <h2>Recent Errors</h2>
        <table>
            <tr><th>Time</th><th>Type</th><th>Message</th></tr>
            {errors_html}
        </table>
        </body></html>
        """)
    
    except Exception as e:
        return HTMLResponse(f"<h1>Error: {e}</h1><pre>{traceback.format_exc()}</pre>")


# ==================== STARTUP EVENT ====================

@app.on_event("startup")
async def startup_event():
    global _db_pool
    
    print("=" * 60)
    print("🚀 NIKE ROCKET HYPERLIQUID FOLLOWER API STARTED")
    print("=" * 60)
    
    # Initialize database tables
    if DATABASE_URL:
        try:
            engine = create_engine(DATABASE_URL)
            init_db(engine)
            print("✅ Database tables initialized")
        except Exception as e:
            print(f"⚠️ Database init warning: {e}")
    
    print("✅ Follower routes loaded")
    print("✅ Portfolio routes loaded")
    print("✅ Signup page available at /signup")
    print("✅ Setup page available at /setup")
    print("✅ Dashboard available at /dashboard")
    print("✅ Admin dashboard at /admin")
    print("✅ Ready to receive signals")
    print("=" * 60)


# ==================== HTML TEMPLATES ====================

SIGNUP_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nike Rocket - Signup (Hyperliquid)</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, sans-serif; background: linear-gradient(135deg, #667eea, #764ba2); min-height: 100vh; display: flex; align-items: center; justify-content: center; }
        .card { background: rgba(255,255,255,0.95); padding: 40px; border-radius: 16px; max-width: 500px; width: 90%; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }
        h1 { text-align: center; margin-bottom: 8px; }
        .subtitle { text-align: center; color: #666; margin-bottom: 30px; }
        input { width: 100%; padding: 14px; border: 2px solid #ddd; border-radius: 8px; font-size: 16px; margin-bottom: 15px; }
        button { width: 100%; padding: 14px; background: linear-gradient(135deg, #667eea, #764ba2); color: white; border: none; border-radius: 8px; font-size: 18px; cursor: pointer; }
        button:hover { opacity: 0.9; }
        .result { margin-top: 20px; padding: 15px; border-radius: 8px; display: none; }
        .success { background: #d4edda; color: #155724; }
        .error { background: #f8d7da; color: #721c24; }
        .login-link { text-align: center; margin-top: 20px; }
        a { color: #667eea; }
    </style>
</head>
<body>
<div class="card">
    <h1>🐷🚀 Nike Rocket</h1>
    <p class="subtitle">Hyperliquid Copy Trading</p>
    <form onsubmit="signup(event)">
        <input type="email" id="email" placeholder="your@email.com" required>
        <button type="submit">Create Account</button>
    </form>
    <div id="result" class="result"></div>
    <div class="login-link">Already have an account? <a href="/login">Login here</a></div>
</div>
<script>
async function signup(e) {
    e.preventDefault();
    const result = document.getElementById('result');
    try {
        const resp = await fetch('/api/users/register', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({email: document.getElementById('email').value})
        });
        const data = await resp.json();
        result.style.display = 'block';
        if (resp.ok) {
            result.className = 'result success';
            result.innerHTML = '✅ ' + data.message;
        } else {
            result.className = 'result error';
            result.innerHTML = '❌ ' + (data.detail || 'Error');
        }
    } catch(err) {
        result.style.display = 'block';
        result.className = 'result error';
        result.innerHTML = '❌ Connection error';
    }
}
</script>
</body></html>
"""

SETUP_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nike Rocket - Setup Agent (Hyperliquid)</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, sans-serif; background: linear-gradient(135deg, #667eea, #764ba2); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }
        .card { background: rgba(255,255,255,0.95); padding: 40px; border-radius: 16px; max-width: 600px; width: 100%; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }
        h1 { text-align: center; margin-bottom: 8px; }
        .subtitle { text-align: center; color: #666; margin-bottom: 30px; }
        label { display: block; margin-bottom: 5px; font-weight: 600; color: #333; }
        input { width: 100%; padding: 14px; border: 2px solid #ddd; border-radius: 8px; font-size: 14px; margin-bottom: 20px; font-family: monospace; }
        button { width: 100%; padding: 14px; background: linear-gradient(135deg, #667eea, #764ba2); color: white; border: none; border-radius: 8px; font-size: 18px; cursor: pointer; }
        button:hover { opacity: 0.9; }
        button:disabled { opacity: 0.5; cursor: not-allowed; }
        .result { margin-top: 20px; padding: 15px; border-radius: 8px; display: none; }
        .success { background: #d4edda; color: #155724; }
        .error { background: #f8d7da; color: #721c24; }
        .warning { background: #fff3cd; color: #856404; padding: 12px; border-radius: 8px; margin-bottom: 20px; font-size: 14px; }
        .step { background: #f0f0f0; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
        .step h3 { margin-bottom: 8px; }
        .step p { color: #666; font-size: 14px; }
    </style>
</head>
<body>
<div class="card">
    <h1>🐷🚀 Setup Trading Agent</h1>
    <p class="subtitle">Connect your Hyperliquid wallet</p>
    
    <div class="warning">
        ⚠️ Your private key is encrypted before storage. Never share it with anyone.
    </div>
    
    <div class="step">
        <h3>Step 1: Enter your Nike Rocket API key</h3>
        <p>Check your email for the API key sent during registration.</p>
    </div>
    
    <form onsubmit="setupAgent(event)">
        <label>Nike Rocket API Key</label>
        <input type="text" id="apiKey" placeholder="nk_..." required>
        
        <label>Hyperliquid Wallet Address</label>
        <input type="text" id="walletAddress" placeholder="0x..." required>
        
        <label>Hyperliquid Private Key</label>
        <input type="password" id="privateKey" placeholder="Your private key (will be encrypted)" required>
        
        <button type="submit" id="submitBtn">🔐 Connect Wallet & Start Agent</button>
    </form>
    <div id="result" class="result"></div>
</div>
<script>
// Auto-fill API key from URL param
const urlParams = new URLSearchParams(window.location.search);
if (urlParams.get('key')) document.getElementById('apiKey').value = urlParams.get('key');

async function setupAgent(e) {
    e.preventDefault();
    const btn = document.getElementById('submitBtn');
    const result = document.getElementById('result');
    btn.disabled = true;
    btn.textContent = '⏳ Validating credentials...';
    
    try {
        const resp = await fetch('/api/setup-agent', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-Key': document.getElementById('apiKey').value
            },
            body: JSON.stringify({
                hl_wallet_address: document.getElementById('walletAddress').value,
                hl_private_key: document.getElementById('privateKey').value
            })
        });
        const data = await resp.json();
        result.style.display = 'block';
        if (resp.ok) {
            result.className = 'result success';
            result.innerHTML = '✅ ' + data.message + '<br><br><a href="/dashboard?key=' + document.getElementById('apiKey').value + '">Go to Dashboard →</a>';
        } else {
            result.className = 'result error';
            result.innerHTML = '❌ ' + (data.detail || 'Setup failed');
        }
    } catch(err) {
        result.style.display = 'block';
        result.className = 'result error';
        result.innerHTML = '❌ Connection error';
    }
    
    btn.disabled = false;
    btn.textContent = '🔐 Connect Wallet & Start Agent';
}
</script>
</body></html>
"""

LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nike Rocket - Login (Hyperliquid)</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, sans-serif; background: linear-gradient(135deg, #667eea, #764ba2); min-height: 100vh; display: flex; align-items: center; justify-content: center; }
        .card { background: rgba(255,255,255,0.95); padding: 40px; border-radius: 16px; max-width: 500px; width: 90%; box-shadow: 0 20px 60px rgba(0,0,0,0.3); text-align: center; }
        h1 { margin-bottom: 8px; }
        .subtitle { color: #666; margin-bottom: 30px; }
        input { width: 100%; padding: 14px; border: 2px solid #ddd; border-radius: 8px; font-size: 16px; margin-bottom: 15px; font-family: monospace; }
        button { width: 100%; padding: 14px; background: linear-gradient(135deg, #667eea, #764ba2); color: white; border: none; border-radius: 8px; font-size: 18px; cursor: pointer; }
        .options { margin-top: 20px; }
        .options a { color: #667eea; margin: 0 10px; }
    </style>
</head>
<body>
<div class="card">
    <h1>🐷🚀 Nike Rocket</h1>
    <p class="subtitle">Hyperliquid Copy Trading</p>
    <form onsubmit="event.preventDefault(); window.location.href='/dashboard?key='+document.getElementById('apiKey').value">
        <input type="text" id="apiKey" placeholder="Enter your API key (nk_...)" required>
        <button type="submit">Access Dashboard</button>
    </form>
    <div class="options">
        <a href="/signup">Create Account</a> | <a href="/setup">Setup Agent</a>
    </div>
</div>
</body></html>
"""


def generate_dashboard_html(api_key: str = "") -> str:
    """Generate the portfolio dashboard HTML (mirrors Kraken dashboard exactly)"""
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nike Rocket - Portfolio Dashboard (Hyperliquid)</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        
        /* Login Screen */
        .login-screen {{ max-width: 500px; margin: 80px auto; background: rgba(255,255,255,0.95); padding: 40px; border-radius: 16px; text-align: center; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }}
        .login-screen h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .login-screen input {{ width: 100%; padding: 14px; border: 2px solid #ddd; border-radius: 8px; font-size: 16px; margin: 15px 0; font-family: monospace; }}
        .login-screen button {{ width: 100%; padding: 14px; background: linear-gradient(135deg, #667eea, #764ba2); color: white; border: none; border-radius: 8px; font-size: 18px; cursor: pointer; }}
        
        /* Dashboard */
        .dashboard {{ display: none; }}
        .header {{ background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); border-radius: 16px; padding: 20px 30px; margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center; color: white; }}
        .header h1 {{ font-family: 'Bebas Neue', sans-serif; font-size: 32px; letter-spacing: 2px; }}
        
        /* Period Tabs */
        .period-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; }}
        .period-tab {{ padding: 10px 20px; background: rgba(255,255,255,0.15); color: white; border: none; border-radius: 8px; cursor: pointer; font-size: 14px; }}
        .period-tab.active {{ background: rgba(255,255,255,0.9); color: #333; font-weight: bold; }}
        
        /* Stats Grid */
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .stat-card {{ background: rgba(255,255,255,0.95); padding: 20px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
        .stat-card .label {{ font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; }}
        .stat-card .value {{ font-size: 24px; font-weight: bold; margin-top: 5px; }}
        .stat-card .value.positive {{ color: #10b981; }}
        .stat-card .value.negative {{ color: #ef4444; }}
        
        /* Chart Area */
        .chart-area {{ background: rgba(255,255,255,0.95); border-radius: 12px; padding: 20px; margin-bottom: 20px; min-height: 300px; }}
        .chart-area h3 {{ margin-bottom: 15px; color: #333; }}
        
        /* Agent Status */
        .agent-status {{ background: rgba(255,255,255,0.95); border-radius: 12px; padding: 20px; margin-bottom: 20px; }}
        
        /* Trades Table */
        .trades-section {{ background: rgba(255,255,255,0.95); border-radius: 12px; padding: 20px; }}
        .trades-section h3 {{ margin-bottom: 15px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #eee; font-size: 14px; }}
        th {{ background: #f9fafb; font-weight: 600; color: #666; }}
        
        .loading {{ text-align: center; padding: 40px; color: #888; }}
        .btn {{ padding: 8px 16px; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; }}
        .btn-primary {{ background: #667eea; color: white; }}
        .btn-sm {{ padding: 6px 12px; font-size: 12px; }}
        
        @media (max-width: 768px) {{
            .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .header {{ flex-direction: column; text-align: center; gap: 10px; }}
        }}
    </style>
</head>
<body>

<div class="container">
    <!-- Login Screen -->
    <div id="loginScreen" class="login-screen">
        <h1>🐷🚀 Nike Rocket</h1>
        <p style="color:#666;margin-bottom:20px;">Hyperliquid Copy Trading Dashboard</p>
        <input type="text" id="apiKeyInput" placeholder="Enter your API key (nk_...)" value="{api_key}">
        <button onclick="loadDashboard()">Access Dashboard</button>
        <div style="margin-top:15px;font-size:14px;">
            <a href="/signup" style="color:#667eea;">Create Account</a> |
            <a href="/setup" style="color:#667eea;">Setup Agent</a>
        </div>
    </div>
    
    <!-- Dashboard -->
    <div id="dashboard" class="dashboard">
        <div class="header">
            <h1>🐷🚀 NIKE ROCKET — HYPERLIQUID</h1>
            <div>
                <span id="userEmail" style="margin-right:15px;"></span>
                <button class="btn btn-sm" onclick="initPortfolio()">Initialize Portfolio</button>
                <a href="/setup" class="btn btn-sm btn-primary" style="text-decoration:none;margin-left:8px;">⚙️ Setup</a>
            </div>
        </div>
        
        <!-- Agent Status -->
        <div id="agentStatus" class="agent-status">
            <strong>Agent Status:</strong> <span id="agentStatusText">Loading...</span>
        </div>
        
        <!-- Period Tabs -->
        <div class="period-tabs">
            <button class="period-tab" data-period="7d">7D</button>
            <button class="period-tab active" data-period="30d">30D</button>
            <button class="period-tab" data-period="90d">90D</button>
            <button class="period-tab" data-period="1y">1Y</button>
            <button class="period-tab" data-period="all">All</button>
        </div>
        
        <!-- Stats Grid -->
        <div id="statsGrid" class="stats-grid">
            <div class="loading">Loading portfolio statistics...</div>
        </div>
        
        <!-- Equity Curve Chart -->
        <div class="chart-area">
            <h3>📈 Equity Curve (Trading Performance Only)</h3>
            <canvas id="equityChart" height="250"></canvas>
        </div>
        
        <!-- Trades -->
        <div class="trades-section">
            <h3>Recent Trades</h3>
            <div id="tradesTable"><div class="loading">Loading trades...</div></div>
        </div>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
let currentApiKey = '{api_key}';
let currentPeriod = '30d';
let equityChart = null;

// Auto-load if API key in URL
if (currentApiKey) {{
    loadDashboard();
}}

function loadDashboard() {{
    currentApiKey = currentApiKey || document.getElementById('apiKeyInput').value.trim();
    if (!currentApiKey) return alert('Please enter your API key');
    
    document.getElementById('loginScreen').style.display = 'none';
    document.getElementById('dashboard').style.display = 'block';
    
    // Update URL
    window.history.replaceState(null, '', '/dashboard?key=' + currentApiKey);
    
    loadAgentStatus();
    loadStats();
    loadEquityCurve();
    loadTrades();
}}

// Period tabs
document.querySelectorAll('.period-tab').forEach(tab => {{
    tab.addEventListener('click', function() {{
        document.querySelectorAll('.period-tab').forEach(t => t.classList.remove('active'));
        this.classList.add('active');
        currentPeriod = this.dataset.period;
        loadStats();
    }});
}});

async function loadAgentStatus() {{
    try {{
        const resp = await fetch('/api/agent-status', {{ headers: {{'X-API-Key': currentApiKey}} }});
        const data = await resp.json();
        const el = document.getElementById('agentStatusText');
        document.getElementById('userEmail').textContent = data.email || '';
        
        if (!data.agent_configured) {{
            el.innerHTML = '🔴 <strong>Not Configured</strong> — <a href="/setup?key=' + currentApiKey + '">Complete setup</a>';
        }} else if (data.agent_active) {{
            el.innerHTML = '🟢 <strong>Active</strong> — Trading enabled';
        }} else {{
            el.innerHTML = '🟡 <strong>Paused</strong> — Agent configured but not active';
        }}
    }} catch(e) {{
        document.getElementById('agentStatusText').textContent = '⚠️ Error loading status';
    }}
}}

async function loadStats() {{
    try {{
        const resp = await fetch('/api/portfolio/stats?period=' + currentPeriod + '&key=' + currentApiKey, {{
            headers: {{'X-API-Key': currentApiKey}}
        }});
        const data = await resp.json();
        
        if (data.status === 'no_data' || data.status === 'error') {{
            document.getElementById('statsGrid').innerHTML = '<div class="stat-card" style="grid-column:1/-1;text-align:center;padding:40px;"><p>' + (data.message || 'No data available') + '</p><button class="btn btn-primary" onclick="initPortfolio()" style="margin-top:15px;">Initialize Portfolio</button></div>';
            return;
        }}
        
        const profit = data.total_profit || 0;
        const profitClass = profit >= 0 ? 'positive' : 'negative';
        const pf = data.profit_factor === null ? '∞' : (data.profit_factor || 'N/A');
        const sharpe = data.sharpe_ratio !== null ? data.sharpe_ratio : 'N/A';
        const allPf = data.all_time_profit_factor === null ? '∞' : (data.all_time_profit_factor || 'N/A');
        const allSharpe = data.all_time_sharpe !== null ? data.all_time_sharpe : 'N/A';
        
        document.getElementById('statsGrid').innerHTML = `
            <div class="stat-card"><div class="label">Period Profit</div><div class="value ${{profitClass}}">${{profit >= 0 ? '+' : ''}}${{profit.toFixed(2)}}</div></div>
            <div class="stat-card"><div class="label">All-Time Profit</div><div class="value ${{(data.all_time_profit||0) >= 0 ? 'positive' : 'negative'}}">${{(data.all_time_profit||0) >= 0 ? '+' : ''}}${{(data.all_time_profit||0).toFixed(2)}}</div></div>
            <div class="stat-card"><div class="label">ROI (Initial Capital)</div><div class="value ${{profitClass}}">${{(data.roi_on_initial||0).toFixed(1)}}%</div></div>
            <div class="stat-card"><div class="label">Initial Capital</div><div class="value">${{(data.initial_capital||0).toFixed(2)}}</div></div>
            <div class="stat-card"><div class="label">Win Rate</div><div class="value">${{(data.win_rate||0).toFixed(1)}}%</div></div>
            <div class="stat-card"><div class="label">Trades (W/L)</div><div class="value">${{data.total_trades||0}} (${{data.winning_trades||0}}/${{data.losing_trades||0}})</div></div>
            <div class="stat-card"><div class="label">Best Trade</div><div class="value positive">+${{(data.best_trade||0).toFixed(2)}}</div></div>
            <div class="stat-card"><div class="label">Worst Trade</div><div class="value negative">${{(data.worst_trade||0).toFixed(2)}}</div></div>
            <div class="stat-card"><div class="label">Profit Factor (Period)</div><div class="value">${{pf}}</div></div>
            <div class="stat-card"><div class="label">Profit Factor (All-Time)</div><div class="value">${{allPf}}</div></div>
            <div class="stat-card"><div class="label">Max Drawdown</div><div class="value negative">${{(data.max_drawdown||0).toFixed(1)}}%</div></div>
            <div class="stat-card"><div class="label">Sharpe Ratio (All-Time)</div><div class="value">${{allSharpe}}</div></div>
            <div class="stat-card"><div class="label">Days Active</div><div class="value">${{data.all_time_days_active||0}}</div></div>
            <div class="stat-card"><div class="label">Avg Monthly Profit</div><div class="value">${{(data.avg_monthly_profit||0).toFixed(2)}}</div></div>
        `;
    }} catch(e) {{
        document.getElementById('statsGrid').innerHTML = '<div class="loading">⚠️ Error loading stats</div>';
    }}
}}

async function loadEquityCurve() {{
    try {{
        const resp = await fetch('/api/portfolio/equity-curve?key=' + currentApiKey, {{
            headers: {{'X-API-Key': currentApiKey}}
        }});
        const data = await resp.json();
        
        if (!data.equity_curve || data.equity_curve.length < 2) return;
        
        const ctx = document.getElementById('equityChart').getContext('2d');
        if (equityChart) equityChart.destroy();
        
        const labels = data.equity_curve.map(p => new Date(p.date).toLocaleDateString());
        const values = data.equity_curve.map(p => p.equity);
        
        const gradient = ctx.createLinearGradient(0, 0, 0, 250);
        const isPositive = values[values.length-1] >= values[0];
        gradient.addColorStop(0, isPositive ? 'rgba(16,185,129,0.3)' : 'rgba(239,68,68,0.3)');
        gradient.addColorStop(1, 'rgba(255,255,255,0)');
        
        equityChart = new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: labels,
                datasets: [{{
                    label: 'Equity',
                    data: values,
                    borderColor: isPositive ? '#10b981' : '#ef4444',
                    backgroundColor: gradient,
                    fill: true,
                    tension: 0.3,
                    pointRadius: values.length > 50 ? 0 : 3,
                    borderWidth: 2,
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    y: {{ beginAtZero: false, ticks: {{ callback: v => '$' + v.toFixed(0) }} }},
                    x: {{ display: values.length < 30 }}
                }}
            }}
        }});
    }} catch(e) {{
        console.error('Equity curve error:', e);
    }}
}}

async function loadTrades() {{
    try {{
        const resp = await fetch('/api/users/stats', {{ headers: {{'X-API-Key': currentApiKey}} }});
        const data = await resp.json();
        
        if (!data.recent_trades || data.recent_trades.length === 0) {{
            document.getElementById('tradesTable').innerHTML = '<p style="text-align:center;padding:20px;color:#888;">No trades yet</p>';
            return;
        }}
        
        let html = '<table><tr><th>Date</th><th>Symbol</th><th>P&L</th></tr>';
        data.recent_trades.forEach(t => {{
            const pnl = t.profit || 0;
            const cls = pnl >= 0 ? 'positive' : 'negative';
            html += `<tr><td>${{new Date(t.closed_at).toLocaleDateString()}}</td><td>${{t.symbol}}</td><td class="value ${{cls}}">${{pnl >= 0 ? '+' : ''}}${{pnl.toFixed(2)}}</td></tr>`;
        }});
        html += '</table>';
        document.getElementById('tradesTable').innerHTML = html;
    }} catch(e) {{
        document.getElementById('tradesTable').innerHTML = '<p style="color:#888;">Error loading trades</p>';
    }}
}}

async function initPortfolio() {{
    try {{
        const resp = await fetch('/api/portfolio/initialize', {{
            method: 'POST',
            headers: {{'X-API-Key': currentApiKey}}
        }});
        const data = await resp.json();
        alert(data.message || JSON.stringify(data));
        if (data.status === 'success') {{
            loadStats();
            loadEquityCurve();
        }}
    }} catch(e) {{
        alert('Error initializing portfolio');
    }}
}}
</script>
</body>
</html>
"""


# Run locally for testing
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
