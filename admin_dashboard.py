"""
Admin Dashboard - SCHEMA-AGNOSTIC VERSION
==========================================
Works with ANY database schema by detecting columns dynamically

UPDATED: Now reads initial_capital and last_known_balance from follower_users
(consolidated schema) with fallback to portfolio_users for backwards compatibility.
"""

import os
import html
import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict, Optional

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme123")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def table_exists(table_name: str) -> bool:
    """Check if a table exists"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table_name,))
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        return exists
    except:
        return False


def get_table_columns(table_name: str) -> List[str]:
    """Get all column names for a table"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        columns = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return columns
    except:
        return []


def create_error_logs_table():
    """Create monitoring tables and ensure schema is up to date"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Error logs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS error_logs (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            api_key VARCHAR(100),
            error_type VARCHAR(100),
            error_message TEXT,
            context JSONB
        )
    """)
    
    # Agent logs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agent_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            api_key VARCHAR(100),
            event_type VARCHAR(100),
            event_data JSONB
        )
    """)
    
    # Trades table is created by position_monitor.py
    # No need to create it here as it has a different schema
    
    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_error_logs_timestamp ON error_logs(created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_logs_timestamp ON agent_logs(timestamp DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_closed_at ON trades(closed_at DESC)")
    
    # Backfill NULL timestamps in error_logs (use 30 days ago so they don't inflate recent counts)
    try:
        cur.execute("UPDATE error_logs SET created_at = NOW() - INTERVAL '30 days' WHERE created_at IS NULL")
    except Exception:
        pass
    
    # Ensure DEFAULT is set on created_at (in case table was created without it)
    try:
        cur.execute("ALTER TABLE error_logs ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP")
    except Exception:
        pass
    
    # ========== SCHEMA MIGRATIONS ==========
    # Add fee_tier column to follower_users if it doesn't exist
    try:
        cur.execute("""
            ALTER TABLE follower_users 
            ADD COLUMN IF NOT EXISTS fee_tier VARCHAR(20) DEFAULT 'standard'
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Note: fee_tier column may already exist: {e}")
    
    conn.commit()
    cur.close()
    conn.close()


def get_all_users_with_status() -> List[Dict]:
    """Get all users from follower_users table with portfolio data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Check if follower_users table exists
    if not table_exists('follower_users'):
        cur.close()
        conn.close()
        return []
    
    try:
        # Check if follower_users has consolidated columns (initial_capital, last_known_balance)
        fu_columns = get_table_columns('follower_users')
        has_consolidated = 'initial_capital' in fu_columns and 'last_known_balance' in fu_columns
        
        if has_consolidated:
            # NEW: Read directly from follower_users (consolidated schema)
            cur.execute("""
                SELECT 
                    fu.email,
                    fu.api_key,
                    fu.credentials_set,
                    fu.agent_active,
                    fu.total_profit,
                    fu.total_trades,
                    fu.created_at,
                    COALESCE(fu.initial_capital, 0) as initial_capital,
                    COALESCE(fu.last_known_balance, 0) as current_balance,
                    fu.hl_wallet_address
                FROM follower_users fu
                ORDER BY fu.id DESC
            """)
        else:
            # FALLBACK: Join with portfolio_users (legacy schema)
            cur.execute("""
                SELECT 
                    fu.email,
                    fu.api_key,
                    fu.credentials_set,
                    fu.agent_active,
                    fu.total_profit,
                    fu.total_trades,
                    fu.created_at,
                    COALESCE(pu.initial_capital, 0) as initial_capital,
                    COALESCE(pu.last_known_balance, 0) as current_balance,
                    fu.hl_wallet_address
                FROM follower_users fu
                LEFT JOIN portfolio_users pu ON fu.api_key = pu.api_key
                ORDER BY fu.id DESC
            """)
        
        users = []
        for row in cur.fetchall():
            email, api_key, credentials_set, agent_active, total_profit, total_trades, created_at, initial_capital, current_balance, hl_wallet_address = row
            
            # Determine status
            if agent_active:
                status = {'status': 'active', 'status_text': 'Active', 'emoji': '🟢'}
            elif credentials_set:
                status = {'status': 'configured', 'status_text': 'Ready', 'emoji': '🟡'}
            else:
                status = {'status': 'pending', 'status_text': 'Pending', 'emoji': '⏳'}
            
            # Calculate ROI
            capital = float(initial_capital) if initial_capital else 0
            profit = float(total_profit) if total_profit else 0
            roi = (profit / capital * 100) if capital > 0 else 0
            
            # Get error count
            recent_errors = 0
            if table_exists('error_logs'):
                try:
                    cur.execute("""
                        SELECT COUNT(*) FROM error_logs 
                        WHERE api_key = %s AND created_at > NOW() - INTERVAL '24 hours'
                    """, (api_key,))
                    recent_errors = cur.fetchone()[0]
                except:
                    pass
            
            # Format Hyperliquid account ID for display (show first 8 chars only)
            wallet_display = hl_wallet_address[:8] + '...' if hl_wallet_address else None
            
            users.append({
                'email': email,
                'api_key': api_key,
                'agent_status': status['status'],
                'status_text': status['status_text'],
                'status_emoji': status['emoji'],
                'total_trades': total_trades or 0,
                'total_profit': profit,
                'capital': capital,
                'current_balance': float(current_balance) if current_balance else 0,
                'roi': roi,
                'recent_errors': recent_errors,
                'created_at': created_at,
                'hl_wallet_address': hl_wallet_address,
                'wallet_display': wallet_display
            })
        
        cur.close()
        conn.close()
        return users
        
    except Exception as e:
        print(f"Error in get_all_users_with_status: {e}")
        cur.close()
        conn.close()
        return []


def get_recent_errors(hours: int = None, limit: int = 500) -> List[Dict]:
    """Get errors - all historical or filtered by hours
    
    Args:
        hours: Optional - filter to last X hours. None = all errors
        limit: Max errors to return (prevents lag with thousands)
    """
    if not table_exists('error_logs'):
        return []
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Build query based on whether we filter by time
        if hours:
            cur.execute("""
                SELECT 
                    COALESCE(el.created_at, el.id::text::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Singapore' as timestamp_sgt,
                    el.api_key, 
                    el.error_type, 
                    el.error_message,
                    fu.email,
                    el.context
                FROM error_logs el
                LEFT JOIN follower_users fu ON el.api_key = fu.api_key
                WHERE el.created_at > NOW() - INTERVAL '%s hours'
                ORDER BY el.created_at DESC
                LIMIT %s
            """, (hours, limit))
        else:
            # Get ALL errors (with reasonable limit)
            cur.execute("""
                SELECT 
                    COALESCE(el.created_at, NOW()) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Singapore' as timestamp_sgt,
                    el.api_key, 
                    el.error_type, 
                    el.error_message,
                    fu.email,
                    el.context
                FROM error_logs el
                LEFT JOIN follower_users fu ON el.api_key = fu.api_key
                ORDER BY el.created_at DESC
                LIMIT %s
            """, (limit,))
        
        errors = []
        for row in cur.fetchall():
            timestamp_sgt, api_key, error_type, error_message, email, context = row
            errors.append({
                'timestamp': timestamp_sgt,
                'api_key': api_key,
                'error_type': error_type or 'Unknown',
                'error_message': error_message or '',
                'email': email or (api_key[:20] + '...' if api_key else 'N/A'),
                'context': context
            })
        
        cur.close()
        conn.close()
        return errors
    except Exception as e:
        print(f"Error getting recent errors: {e}")
        return []


def get_positions_needing_review() -> List[Dict]:
    """Get all positions that need manual review"""
    if not table_exists('open_positions'):
        return []
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                op.id,
                op.user_id,
                fu.email,
                fu.api_key,
                op.symbol,
                op.hl_coin,
                op.side,
                op.quantity,
                op.leverage,
                op.entry_fill_price,
                op.target_tp,
                op.target_sl,
                op.opened_at,
                op.status
            FROM open_positions op
            JOIN follower_users fu ON op.user_id::text = fu.id::text
            WHERE op.status = 'needs_review'
            ORDER BY op.opened_at DESC
        """)
        
        positions = []
        for row in cur.fetchall():
            pid, user_id, email, api_key, symbol, hl_coin, side, qty, leverage, entry, tp, sl, opened_at, status = row
            
            # Calculate potential P&L if it was closed
            # (This is theoretical since we don't know actual close price)
            risk_amount = abs(float(entry) - float(sl)) * float(qty)
            reward_amount = abs(float(tp) - float(entry)) * float(qty)
            
            positions.append({
                'id': pid,
                'user_id': user_id,
                'email': email,
                'api_key': api_key[:20] + '...',
                'symbol': symbol,
                'side': side,
                'quantity': float(qty),
                'leverage': float(leverage),
                'entry': float(entry),
                'tp': float(tp),
                'sl': float(sl),
                'risk_amount': risk_amount,
                'reward_amount': reward_amount,
                'opened_at': opened_at,
                'reason': 'Manual close detected (both TP/SL canceled)'
            })
        
        cur.close()
        conn.close()
        return positions
    except Exception as e:
        print(f"Error getting positions needing review: {e}")
        return []


def get_stats_summary() -> Dict:
    """Get summary statistics from follower_users (consolidated schema)"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Count users from follower_users
    total_users = 0
    configured_users = 0
    active_now = 0
    total_profit = 0.0
    total_trades = 0
    
    if table_exists('follower_users'):
        try:
            cur.execute("SELECT COUNT(*) FROM follower_users")
            total_users = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM follower_users WHERE credentials_set = true")
            configured_users = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM follower_users WHERE agent_active = true")
            active_now = cur.fetchone()[0]
            
            cur.execute("SELECT COALESCE(SUM(total_profit), 0), COALESCE(SUM(total_trades), 0) FROM follower_users")
            row = cur.fetchone()
            total_profit = float(row[0]) if row else 0.0
            total_trades = int(row[1]) if row else 0
        except Exception as e:
            print(f"Error getting follower_users stats: {e}")
    
    # Get platform capital - try follower_users first (consolidated), then portfolio_users (legacy)
    platform_capital = 0.0
    current_value = 0.0
    
    fu_columns = get_table_columns('follower_users') if table_exists('follower_users') else []
    has_consolidated = 'initial_capital' in fu_columns and 'last_known_balance' in fu_columns
    
    if has_consolidated:
        # NEW: Query follower_users directly (consolidated schema)
        try:
            cur.execute("""
                SELECT 
                    COALESCE(SUM(initial_capital), 0),
                    COALESCE(SUM(last_known_balance), 0)
                FROM follower_users
                WHERE portfolio_initialized = true
            """)
            row = cur.fetchone()
            platform_capital = float(row[0]) if row else 0.0
            current_value = float(row[1]) if row else 0.0
        except Exception as e:
            print(f"Error getting consolidated portfolio stats: {e}")
    elif table_exists('portfolio_users'):
        # FALLBACK: Query portfolio_users (legacy schema)
        try:
            cur.execute("""
                SELECT 
                    COALESCE(SUM(initial_capital), 0),
                    COALESCE(SUM(last_known_balance), 0)
                FROM portfolio_users
            """)
            row = cur.fetchone()
            platform_capital = float(row[0]) if row else 0.0
            current_value = float(row[1]) if row else 0.0
        except Exception as e:
            print(f"Error getting portfolio stats: {e}")
    
    # Current Value = actual AUM (sum of last_known_balance from follower_users)
    # This matches main dashboard behavior and reflects withdrawals
    if table_exists('follower_users'):
        try:
            cur.execute("""
                SELECT COALESCE(SUM(last_known_balance), 0)
                FROM follower_users
                WHERE portfolio_initialized = true
            """)
            row = cur.fetchone()
            current_value = float(row[0]) if row else 0.0
        except Exception as e:
            print(f"Error getting AUM: {e}")
            current_value = platform_capital + total_profit  # Fallback
    
    # Calculate platform ROI (based on profit vs capital invested)
    platform_roi = (total_profit / platform_capital * 100) if platform_capital > 0 else 0.0
    
    # Active percentage
    active_percent = (active_now / configured_users * 100) if configured_users > 0 else 0.0
    
    # Average profit per user
    avg_profit = total_profit / total_users if total_users > 0 else 0.0
    
    # Count recent errors
    errors_1h = 0
    if table_exists('error_logs'):
        try:
            cur.execute("SELECT COUNT(*) FROM error_logs WHERE created_at > NOW() - INTERVAL '1 hour'")
            errors_1h = cur.fetchone()[0]
        except:
            pass
    
    cur.close()
    conn.close()
    
    return {
        'total_users': total_users,
        'configured_users': configured_users,
        'active_now': active_now,
        'active_percent': active_percent,
        'total_trades': total_trades,
        'total_profit': total_profit,
        'avg_profit': avg_profit,
        'platform_capital': platform_capital,
        'current_value': current_value,
        'platform_roi': platform_roi,
        'errors_1h': errors_1h
    }


def log_error(api_key: str, error_type: str, error_message: str, context: Optional[Dict] = None):
    """Log error"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        import json
        cur.execute(
            "INSERT INTO error_logs (api_key, error_type, error_message, context, created_at) VALUES (%s, %s, %s, %s, NOW())",
            (api_key, error_type, error_message, json.dumps(context) if context else None)
        )
        conn.commit()
        cur.close()
        conn.close()
    except:
        pass


def log_agent_event(api_key: str, event_type: str, event_data: Optional[Dict] = None):
    """Log agent event"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        import json
        cur.execute(
            "INSERT INTO agent_logs (api_key, event_type, event_data) VALUES (%s, %s, %s)",
            (api_key, event_type, json.dumps(event_data) if event_data else None)
        )
        conn.commit()
        cur.close()
        conn.close()
    except:
        pass


def get_users_by_tier() -> Dict[str, List[Dict]]:
    """Get all users grouped by fee tier"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    result = {
        'team': [],    # 0% fees
        'vip': [],     # 5% fees
        'standard': [] # 10% fees
    }
    
    try:
        cur.execute("""
            SELECT id, email, fee_tier, total_profit, total_trades, agent_active
            FROM follower_users
            ORDER BY email
        """)
        rows = cur.fetchall()
        
        for row in rows:
            user = {
                'id': row[0],
                'email': row[1],
                'fee_tier': row[2] or 'standard',
                'total_profit': row[3] or 0,
                'total_trades': row[4] or 0,
                'agent_active': row[5] or False
            }
            tier = user['fee_tier']
            if tier in result:
                result[tier].append(user)
            else:
                result['standard'].append(user)
    except Exception as e:
        print(f"Error getting users by tier: {e}")
    finally:
        cur.close()
        conn.close()
    
    return result


def update_user_tier(user_id: int, new_tier: str) -> bool:
    """Update a user's fee tier"""
    if new_tier not in ['team', 'vip', 'standard']:
        return False
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(
            "UPDATE follower_users SET fee_tier = %s WHERE id = %s",
            (new_tier, user_id)
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        print(f"Error updating user tier: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def cleanup_old_errors(days: int = 30) -> int:
    """
    Delete errors older than specified days to prevent table bloat.
    
    Args:
        days: Number of days to keep errors (default 30)
        
    Returns:
        Number of errors deleted
    """
    if not table_exists('error_logs'):
        return 0
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            DELETE FROM error_logs 
            WHERE created_at < NOW() - INTERVAL '%s days'
        """, (days,))
        
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        
        if deleted > 0:
            print(f"🧹 Cleaned up {deleted} old error logs (older than {days} days)")
        
        return deleted
    except Exception as e:
        print(f"Error cleaning up old errors: {e}")
        return 0


def get_error_stats() -> Dict:
    """Get error statistics for monitoring"""
    if not table_exists('error_logs'):
        return {'total': 0, 'last_24h': 0, 'last_7d': 0, 'by_type': {}}
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Total errors
        cur.execute("SELECT COUNT(*) FROM error_logs")
        total = cur.fetchone()[0]
        
        # Last 24 hours
        cur.execute("SELECT COUNT(*) FROM error_logs WHERE created_at > NOW() - INTERVAL '24 hours'")
        last_24h = cur.fetchone()[0]
        
        # Last 7 days
        cur.execute("SELECT COUNT(*) FROM error_logs WHERE created_at > NOW() - INTERVAL '7 days'")
        last_7d = cur.fetchone()[0]
        
        # By type (top 10)
        cur.execute("""
            SELECT error_type, COUNT(*) as cnt 
            FROM error_logs 
            WHERE created_at > NOW() - INTERVAL '7 days'
            GROUP BY error_type 
            ORDER BY cnt DESC 
            LIMIT 10
        """)
        by_type = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return {
            'total': total,
            'last_24h': last_24h,
            'last_7d': last_7d,
            'by_type': by_type
        }
    except Exception as e:
        print(f"Error getting error stats: {e}")
        return {'total': 0, 'last_24h': 0, 'last_7d': 0, 'by_type': {}}


def generate_admin_html(users: List[Dict], errors: List[Dict], stats: Dict, review_positions: List[Dict] = None, users_by_tier: Dict = None) -> str:
    """Generate admin dashboard HTML - Dark Theme with Error Tooltips"""
    
    # Handle backward compatibility
    if review_positions is None:
        review_positions = []
    if users_by_tier is None:
        users_by_tier = {'team': [], 'vip': [], 'standard': []}
    
    # User rows
    user_rows = ""
    if not users:
        user_rows = "<tr><td colspan='9' style='text-align: center; padding: 40px; color: #9ca3af;'>No users yet</td></tr>"
    else:
        for user in users:
            status_class = f"status-{user['agent_status']}"
            profit_class = "profit-positive" if user['total_profit'] >= 0 else "profit-negative"
            profit_prefix = "+" if user['total_profit'] >= 0 else ""
            roi_prefix = "+" if user.get('roi', 0) >= 0 else ""
            
            # Error indicator with tooltip
            error_count = user.get('recent_errors', 0)
            if error_count > 0:
                error_cell = f'''<span class="error-indicator error-has-errors" title="⚠️ {error_count} error(s) in last 24h - see Error History below">⚠️</span>'''
            else:
                error_cell = '''<span class="error-indicator error-none" title="✅ No errors in last 24h">✅</span>'''
            
            # Fingerprint display (first 8 chars or "Not Set")
            fingerprint = user.get('wallet_display', None)
            fingerprint_cell = f'<span class="api-key" title="Full: {user.get("hl_wallet_address", "")}">{fingerprint}</span>' if fingerprint else '<span style="color: #6b7280;">—</span>'
            
            user_rows += f"""
            <tr>
                <td><span class="status-badge {status_class}">{user['status_emoji']} {user['status_text']}</span></td>
                <td style="color: #e5e7eb;">{user['email']}</td>
                <td class="api-key">{user['api_key'][:15]}...</td>
                <td>{fingerprint_cell}</td>
                <td style="color: #e5e7eb;">${user.get('capital', 0):.2f}</td>
                <td style="color: #e5e7eb;">{user['total_trades']}</td>
                <td class="{profit_class}">{profit_prefix}${abs(user['total_profit']):.2f}</td>
                <td class="{profit_class}">{roi_prefix}{user.get('roi', 0):.1f}%</td>
                <td style="text-align: center;">{error_cell}</td>
            </tr>
            """
    
    # Review positions section
    review_positions_section = ""
    if review_positions:
        review_rows = ""
        for pos in review_positions:
            side_color = "#10b981" if pos['side'].upper() in ('BUY', 'LONG') else "#ef4444"
            review_rows += f"""
                <tr>
                    <td>{pos['email']}</td>
                    <td><span style="color: {side_color}; font-weight: 600;">{pos['side']}</span> {pos['symbol']}</td>
                    <td>{pos['quantity']:.4f} @ {pos['leverage']}x</td>
                    <td>${pos['entry']:.2f}</td>
                    <td><span style="color: #10b981">${pos['tp']:.2f}</span></td>
                    <td><span style="color: #ef4444">${pos['sl']:.2f}</span></td>
                    <td>{(pos['opened_at'] + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M') + ' SGT' if pos['opened_at'] else 'N/A'}</td>
                    <td style="color: #f59e0b;">{pos['reason']}</td>
                    <td>
                        <a href="#" onclick="deletePosition({pos['id']}); return false;" style="color: #ef4444; text-decoration: none;">🗑️ Delete</a>
                    </td>
                </tr>
            """
        
        review_positions_section = f"""
        <div class="users-section" style="border: 2px solid #f59e0b;">
            <h2 style="color: #fbbf24;">🔍 Positions Needing Review ({len(review_positions)})</h2>
            <p style="color: #9ca3af; margin-bottom: 15px; font-size: 13px;">
                These positions were manually closed or had unusual closure patterns. Review and delete when confirmed.
            </p>
            <table>
                <thead>
                    <tr>
                        <th>User</th>
                        <th>Position</th>
                        <th>Size</th>
                        <th>Entry</th>
                        <th>TP</th>
                        <th>SL</th>
                        <th>Opened</th>
                        <th>Reason</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>{review_rows}</tbody>
            </table>
        </div>
        """
    
    # Error items with detailed view
    error_items = ""
    if not errors:
        error_items = "<div style='text-align: center; padding: 40px; color: #9ca3af;'>No errors recorded 🎉</div>"
    else:
        for error in errors:
            # Determine error severity color
            error_type = error.get('error_type', 'unknown').lower()
            
            # Categorize error type (EXPANDED to catch more patterns)
            error_type = error.get('error_type', 'unknown').lower()
            error_msg = error.get('error_message', '').lower()
            combined = error_type + ' ' + error_msg  # Check both for keywords
            
            if any(k in combined for k in ['auth', 'credential', 'decrypt', 'key_error', 'secret', 'permission', '401', 'unauthorized', 'api_key', 'invalid key']):
                border_color = '#ef4444'  # Red - authentication
                badge_class = 'error-badge-critical'
                error_category = 'auth'
            elif any(k in combined for k in ['network', 'connection', 'timeout', 'socket', 'dns', 'ssl', 'certificate', 'refused', 'unreachable']):
                border_color = '#f59e0b'  # Orange - network
                badge_class = 'error-badge-warning'
                error_category = 'network'
            elif any(k in combined for k in ['insufficient', 'balance', 'funds', 'margin', 'capital', 'deposit', 'zero_balance', 'minimum']):
                border_color = '#8b5cf6'  # Purple - funds
                badge_class = 'error-badge-funds'
                error_category = 'funds'
            elif any(k in combined for k in ['trade', 'order', 'execution', 'position', 'fill', 'market', 'limit', 'portfolio', 'init']):
                border_color = '#3b82f6'  # Blue - trade
                badge_class = 'error-badge-info'
                error_category = 'trade'
            elif any(k in combined for k in ['database', 'sql', 'table', 'column', 'relation', 'asyncpg', 'postgres', 'undefined', 'does not exist']):
                border_color = '#ec4899'  # Pink - database
                badge_class = 'error-badge-database'
                error_category = 'database'
            elif any(k in combined for k in ['module', 'import', 'attribute', 'typeerror', 'valueerror', 'keyerror', 'index', 'syntax']):
                border_color = '#06b6d4'  # Cyan - code/system
                badge_class = 'error-badge-code'
                error_category = 'code'
            elif any(k in combined for k in ['hyperliquid', 'exchange', 'hl_sdk', 'api']):
                border_color = '#f97316'  # Orange-red - exchange
                badge_class = 'error-badge-exchange'
                error_category = 'exchange'
            else:
                border_color = '#6b7280'  # Gray - other
                badge_class = 'error-badge-info'
                error_category = 'other'
            
            # Format error message
            error_msg = error.get('error_message', '')
            if len(error_msg) > 300:
                error_msg = error_msg[:300] + '...'
            
            # HTML-escape for safe display (prevents breaking HTML parsing)
            error_msg_escaped = html.escape(error_msg)
            
            # User email for display
            user_display = error.get('email', 'Unknown User')
            
            # Format timestamp for Singapore timezone
            timestamp = error.get('timestamp', '')
            if timestamp:
                try:
                    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S') + ' SGT'
                except:
                    timestamp_str = str(timestamp)
            else:
                timestamp_str = 'N/A'
            
            error_items += f"""
            <div class="error-item" 
                 style="border-left-color: {border_color};" 
                 data-error-type="{error_category}"
                 data-user="{html.escape(user_display.lower())}"
                 data-message="{error_msg_escaped.lower()}"
                 data-error-category="{error_type.lower()}"
                 data-timestamp="{timestamp_str}">
                <div class="error-header">
                    <div style="display: flex; gap: 10px; align-items: center;">
                        <span class="error-type {badge_class}">{error.get('error_type', 'Unknown')}</span>
                        <span style="color: #60a5fa; font-size: 12px;">👤 {html.escape(user_display)}</span>
                    </div>
                    <span class="error-timestamp">{timestamp_str}</span>
                </div>
                <div class="error-message">{error_msg_escaped}</div>
                <div class="error-context">API Key: {error.get('api_key', 'N/A')[:15]}...</div>
            </div>
            """
    
    profit_color = "#10b981" if stats.get('total_profit', 0) >= 0 else "#ef4444"
    profit_prefix = "+" if stats.get('total_profit', 0) >= 0 else ""
    roi_color = "#10b981" if stats.get('platform_roi', 0) >= 0 else "#ef4444"
    roi_prefix = "+" if stats.get('platform_roi', 0) >= 0 else ""
    
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>$NIKEPIG Admin Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            background: #0f1218;
            min-height: 100vh; 
            padding: 20px;
            color: #e5e7eb;
        }}
        .container {{ max-width: 1600px; margin: 0 auto; }}
        
        /* Header */
        .header {{ 
            background: linear-gradient(135deg, #1e3a5f 0%, #2d1f47 100%);
            border-radius: 12px; 
            padding: 25px 30px; 
            margin-bottom: 20px; 
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .header h1 {{ color: #4ade80; font-size: 28px; }}
        .header .timestamp {{ color: #9ca3af; font-size: 14px; margin-top: 5px; }}
        
        /* Tactile Refresh Button */
        .refresh-btn {{
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            font-size: 14px;
            display: flex;
            align-items: center;
            gap: 8px;
            transition: all 0.1s ease;
            transform: translateY(0);
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3), 0 2px 4px rgba(16, 185, 129, 0.2);
        }}
        .refresh-btn:hover {{
            background: linear-gradient(135deg, #34d399 0%, #10b981 100%);
            transform: translateY(-2px);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.4), 0 4px 8px rgba(16, 185, 129, 0.3);
        }}
        .refresh-btn:active {{
            transform: translateY(2px);
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
        }}
        
        /* Stats Grid */
        .stats-grid {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); 
            gap: 15px; 
            margin-bottom: 20px; 
        }}
        .stat-card {{ 
            background: #1a1f2e;
            border-radius: 12px; 
            padding: 20px; 
            border: 1px solid #2d3748;
        }}
        .stat-label {{ 
            color: #9ca3af; 
            font-size: 11px; 
            margin-bottom: 8px; 
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-value {{ font-size: 32px; font-weight: bold; }}
        .stat-sub {{ color: #6b7280; font-size: 11px; margin-top: 4px; }}
        
        /* Tax Reports Section */
        .tax-reports-section {{
            background: linear-gradient(135deg, #1a3a1f 0%, #1a1f2e 100%);
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 20px;
            border: 2px solid #10b981;
        }}
        .tax-reports-section h2 {{
            color: #10b981;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .report-controls {{
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            margin-bottom: 20px;
        }}
        .report-select {{
            padding: 12px 16px;
            background: #1f2937;
            border: 1px solid #374151;
            border-radius: 8px;
            color: #e5e7eb;
            font-size: 14px;
            cursor: pointer;
            min-width: 150px;
        }}
        .report-select:focus {{
            outline: none;
            border-color: #10b981;
        }}
        .download-btn {{
            padding: 12px 24px;
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            border: none;
            border-radius: 8px;
            color: white;
            font-weight: 600;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .download-btn:hover {{
            background: linear-gradient(135deg, #34d399 0%, #10b981 100%);
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(16, 185, 129, 0.3);
        }}
        .download-btn:active {{
            transform: translateY(0);
        }}
        .income-summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        .income-card {{
            background: #0f1218;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid #374151;
        }}
        .income-label {{
            color: #9ca3af;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }}
        .income-value {{
            color: #10b981;
            font-size: 24px;
            font-weight: bold;
        }}
        
        /* Users Section */
        .users-section {{ 
            background: #1a1f2e;
            border-radius: 12px; 
            padding: 20px; 
            margin-bottom: 20px;
            border: 1px solid #2d3748;
        }}
        .users-section h2 {{ color: #e5e7eb; margin-bottom: 15px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ 
            background: #0f1218;
            padding: 12px; 
            text-align: left; 
            font-weight: 600;
            color: #9ca3af;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        td {{ padding: 12px; border-bottom: 1px solid #2d3748; }}
        .api-key {{ color: #6b7280; font-family: monospace; font-size: 12px; }}
        
        /* Status Badges */
        .status-badge {{ 
            display: inline-block; 
            padding: 4px 12px; 
            border-radius: 12px; 
            font-size: 12px; 
            font-weight: 600; 
        }}
        .status-active {{ background: #064e3b; color: #34d399; }}
        .status-pending, .status-configured {{ background: #1e3a5f; color: #60a5fa; }}
        .status-inactive {{ background: #374151; color: #9ca3af; }}
        .status-error {{ background: #7f1d1d; color: #fca5a5; }}
        
        /* Profit Colors */
        .profit-positive {{ color: #10b981; font-weight: 600; }}
        .profit-negative {{ color: #ef4444; font-weight: 600; }}
        
        /* Error Indicators */
        .error-indicator {{
            font-size: 16px;
            cursor: help;
            transition: transform 0.2s;
        }}
        .error-indicator:hover {{
            transform: scale(1.3);
        }}
        
        /* Errors Section */
        .errors-section {{ 
            background: #1a1f2e;
            border-radius: 12px; 
            padding: 20px;
            border: 1px solid #2d3748;
        }}
        .errors-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}
        .errors-section h2 {{ color: #fbbf24; }}
        
        /* Error Legend */
        .error-legend {{
            display: flex;
            gap: 15px;
            font-size: 11px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .legend-dot {{
            width: 10px;
            height: 10px;
            border-radius: 50%;
        }}
        .legend-dot.critical {{ background: #ef4444; }}
        .legend-dot.warning {{ background: #f59e0b; }}
        .legend-dot.funds {{ background: #8b5cf6; }}
        .legend-dot.database {{ background: #ec4899; }}
        .legend-dot.code {{ background: #06b6d4; }}
        .legend-dot.exchange {{ background: #f97316; }}
        .legend-dot.info {{ background: #6b7280; }}
        
        /* Search Box */
        .search-box {{
            margin-bottom: 20px;
            display: flex;
            gap: 10px;
            align-items: center;
        }}
        .search-input {{
            flex: 1;
            padding: 12px 16px;
            background: #1f2937;
            border: 1px solid #374151;
            border-radius: 8px;
            color: #e5e7eb;
            font-size: 14px;
        }}
        .search-input:focus {{
            outline: none;
            border-color: #10b981;
        }}
        .search-input::placeholder {{
            color: #6b7280;
        }}
        .filter-select {{
            padding: 12px 16px;
            background: #1f2937;
            border: 1px solid #374151;
            border-radius: 8px;
            color: #e5e7eb;
            font-size: 14px;
            cursor: pointer;
            min-width: 180px;
        }}
        .filter-select:focus {{
            outline: none;
            border-color: #10b981;
        }}
        .clear-search {{
            padding: 12px 20px;
            background: #374151;
            border: none;
            border-radius: 8px;
            color: #e5e7eb;
            cursor: pointer;
            font-size: 14px;
            transition: background 0.2s;
        }}
        .clear-search:hover {{
            background: #4b5563;
        }}
        
        /* Pagination */
        .pagination {{
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 10px;
            margin-top: 20px;
            padding: 20px;
        }}
        .page-btn {{
            padding: 8px 16px;
            background: #374151;
            border: 1px solid #4b5563;
            border-radius: 6px;
            color: #e5e7eb;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.2s;
        }}
        .page-btn:hover {{
            background: #4b5563;
            border-color: #10b981;
        }}
        .page-btn:disabled {{
            opacity: 0.5;
            cursor: not-allowed;
        }}
        .page-btn.active {{
            background: #10b981;
            border-color: #10b981;
            font-weight: 600;
        }}
        .page-info {{
            color: #9ca3af;
            font-size: 14px;
        }}
        
        /* Hidden class for filtering */
        .hidden {{ display: none !important; }}
        
        /* Tax Reports Section */
        .tax-section {{
            background: linear-gradient(135deg, #1e3a5f 0%, #1a2332 100%);
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 20px;
            border: 2px solid #10b981;
        }}
        .tax-section h2 {{
            color: #10b981;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .tax-controls {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .tax-input {{
            padding: 12px;
            background: #1f2937;
            border: 1px solid #374151;
            border-radius: 8px;
            color: #e5e7eb;
            font-size: 14px;
        }}
        .tax-input:focus {{
            outline: none;
            border-color: #10b981;
        }}
        .export-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
        }}
        .export-btn {{
            padding: 14px 20px;
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            color: white;
            border: none;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }}
        .export-btn:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 12px rgba(16, 185, 129, 0.3);
        }}
        .export-btn:active {{
            transform: translateY(0);
        }}
        .tax-summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid #374151;
        }}
        .tax-stat {{
            text-align: center;
        }}
        .tax-stat-label {{
            color: #9ca3af;
            font-size: 12px;
            margin-bottom: 5px;
        }}
        .tax-stat-value {{
            color: #10b981;
            font-size: 24px;
            font-weight: bold;
        }}
        
        /* Error Items */
        .error-item {{ 
            border-left: 4px solid #ef4444; 
            background: #1f2937;
            padding: 15px; 
            margin-bottom: 12px; 
            border-radius: 0 8px 8px 0;
        }}
        .error-header {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
        }}
        .error-type {{ 
            font-weight: 600; 
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
        }}
        .error-badge-critical {{ background: #7f1d1d; color: #fca5a5; }}
        .error-badge-warning {{ background: #78350f; color: #fcd34d; }}
        .error-badge-funds {{ background: #4c1d95; color: #c4b5fd; }}
        .error-badge-info {{ background: #374151; color: #9ca3af; }}
        .error-badge-database {{ background: #831843; color: #f9a8d4; }}
        .error-badge-code {{ background: #164e63; color: #67e8f9; }}
        .error-badge-exchange {{ background: #7c2d12; color: #fdba74; }}
        .error-timestamp {{ color: #6b7280; font-size: 12px; }}
        .error-message {{ color: #e5e7eb; font-size: 13px; line-height: 1.5; }}
        .error-context {{ color: #6b7280; font-size: 11px; margin-top: 8px; font-family: monospace; }}
        
        /* User Tiers Section */
        .tiers-section {{
            background: #1a1f2e;
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 20px;
            border: 1px solid #2d3748;
        }}
        .tiers-section h2 {{
            color: #e5e7eb;
            margin-bottom: 20px;
        }}
        .tiers-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
        }}
        .tier-column {{
            background: #111827;
            border-radius: 10px;
            padding: 15px;
            min-height: 200px;
        }}
        .tier-column.team {{
            border: 2px solid #10b981;
        }}
        .tier-column.vip {{
            border: 2px solid #f59e0b;
        }}
        .tier-column.standard {{
            border: 2px solid #6b7280;
        }}
        .tier-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #374151;
        }}
        .tier-header h3 {{
            margin: 0;
            font-size: 16px;
        }}
        .tier-header.team h3 {{ color: #10b981; }}
        .tier-header.vip h3 {{ color: #f59e0b; }}
        .tier-header.standard h3 {{ color: #9ca3af; }}
        .tier-count {{
            background: #374151;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 12px;
            color: #e5e7eb;
        }}
        .tier-user {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 10px;
            background: #1f2937;
            border-radius: 6px;
            margin-bottom: 8px;
            font-size: 13px;
        }}
        .tier-user:hover {{
            background: #2d3748;
        }}
        .tier-user-email {{
            color: #e5e7eb;
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .tier-user-stats {{
            color: #6b7280;
            font-size: 11px;
            margin-left: 10px;
        }}
        .tier-user-actions {{
            display: flex;
            gap: 5px;
            margin-left: 10px;
        }}
        .tier-btn {{
            padding: 3px 8px;
            border-radius: 4px;
            border: none;
            cursor: pointer;
            font-size: 11px;
            transition: all 0.2s;
        }}
        .tier-btn.to-team {{
            background: #065f46;
            color: #10b981;
        }}
        .tier-btn.to-team:hover {{
            background: #10b981;
            color: white;
        }}
        .tier-btn.to-vip {{
            background: #78350f;
            color: #f59e0b;
        }}
        .tier-btn.to-vip:hover {{
            background: #f59e0b;
            color: white;
        }}
        .tier-btn.to-standard {{
            background: #374151;
            color: #9ca3af;
        }}
        .tier-btn.to-standard:hover {{
            background: #6b7280;
            color: white;
        }}
        .tier-empty {{
            color: #6b7280;
            text-align: center;
            padding: 30px;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1>🚀 $NIKEPIG Admin Dashboard</h1>
                <div class="timestamp">{(datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')} SGT (GMT+8)</div>
            </div>
            <button class="refresh-btn" onclick="location.reload()">
                🔄 Refresh
            </button>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Users</div>
                <div class="stat-value" style="color: #e5e7eb;">{stats.get('total_users', 0)}</div>
                <div class="stat-sub">{stats.get('configured_users', 0)} configured</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Active Now</div>
                <div class="stat-value" style="color: #10b981;">{stats.get('active_now', 0)}</div>
                <div class="stat-sub">{stats.get('active_percent', 0):.1f}% of configured</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Trades</div>
                <div class="stat-value" style="color: #e5e7eb;">{stats.get('total_trades', 0)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Profit</div>
                <div class="stat-value" style="color: {profit_color};">{profit_prefix}${abs(stats.get('total_profit', 0)):.2f}</div>
                <div class="stat-sub">${stats.get('avg_profit', 0):.2f} avg/user</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Platform Capital</div>
                <div class="stat-value" style="color: #e5e7eb;">${stats.get('platform_capital', 0):,.0f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Current Value</div>
                <div class="stat-value" style="color: #e5e7eb;">${stats.get('current_value', 0):,.0f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Platform ROI</div>
                <div class="stat-value" style="color: {roi_color};">{roi_prefix}{stats.get('platform_roi', 0):.1f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Errors (1H)</div>
                <div class="stat-value" style="color: {'#ef4444' if stats.get('errors_1h', 0) > 0 else '#10b981'};">{stats.get('errors_1h', 0)}</div>
            </div>
        </div>
        
        <div class="tax-reports-section">
            <h2>💰 Tax & Income Reports</h2>
            <p style="color: #9ca3af; margin-bottom: 20px; font-size: 13px;">
                Export income data for Xero or tax filing. All amounts in USD. Fee rate: 10% of monthly profits. <strong>Only includes actually received payments</strong> (unpaid/expired invoices excluded).
            </p>
            
            <div class="report-controls">
                <select id="reportYear" class="report-select">
                    <!-- Years populated by JavaScript -->
                </select>
                
                <select id="reportMonth" class="report-select">
                    <option value="">Select Month</option>
                    <option value="1">January</option>
                    <option value="2">February</option>
                    <option value="3">March</option>
                    <option value="4">April</option>
                    <option value="5">May</option>
                    <option value="6">June</option>
                    <option value="7">July</option>
                    <option value="8">August</option>
                    <option value="9">September</option>
                    <option value="10">October</option>
                    <option value="11">November</option>
                    <option value="12">December</option>
                </select>
                
                <button class="download-btn" onclick="downloadMonthlyCSV()">
                    📥 Download Monthly CSV
                </button>
                
                <button class="download-btn" onclick="downloadYearlyCSV()">
                    📅 Download Yearly CSV
                </button>
                
                <button class="download-btn" onclick="downloadUserFeesCSV()">
                    👥 Download Per-User CSV
                </button>
            </div>
            
            <div id="incomeSummary" class="income-summary">
                <!-- Will be populated by JavaScript -->
            </div>
        </div>
        
        <!-- User Fee Tiers Section -->
        <div class="tiers-section">
            <h2>💰 User Fee Tiers</h2>
            <div class="tiers-grid">
                <!-- Team Column (0%) -->
                <div class="tier-column team">
                    <div class="tier-header team">
                        <h3>🏠 Team (0%)</h3>
                        <span class="tier-count">{len(users_by_tier.get('team', []))} users</span>
                    </div>
                    <div class="tier-users" id="tier-team">
                        {''.join([f'''
                        <div class="tier-user" data-user-id="{u['id']}">
                            <span class="tier-user-email" title="{u['email']}">{u['email']}</span>
                            <span class="tier-user-stats">${u['total_profit']:.0f}</span>
                            <div class="tier-user-actions">
                                <button class="tier-btn to-vip" onclick="changeTier({u['id']}, 'vip')" title="Move to VIP">⭐</button>
                                <button class="tier-btn to-standard" onclick="changeTier({u['id']}, 'standard')" title="Move to Standard">👤</button>
                            </div>
                        </div>
                        ''' for u in users_by_tier.get('team', [])]) or '<div class="tier-empty">No team members</div>'}
                    </div>
                </div>
                
                <!-- VIP Column (5%) -->
                <div class="tier-column vip">
                    <div class="tier-header vip">
                        <h3>⭐ VIP (5%)</h3>
                        <span class="tier-count">{len(users_by_tier.get('vip', []))} users</span>
                    </div>
                    <div class="tier-users" id="tier-vip">
                        {''.join([f'''
                        <div class="tier-user" data-user-id="{u['id']}">
                            <span class="tier-user-email" title="{u['email']}">{u['email']}</span>
                            <span class="tier-user-stats">${u['total_profit']:.0f}</span>
                            <div class="tier-user-actions">
                                <button class="tier-btn to-team" onclick="changeTier({u['id']}, 'team')" title="Move to Team">🏠</button>
                                <button class="tier-btn to-standard" onclick="changeTier({u['id']}, 'standard')" title="Move to Standard">👤</button>
                            </div>
                        </div>
                        ''' for u in users_by_tier.get('vip', [])]) or '<div class="tier-empty">No VIP users</div>'}
                    </div>
                </div>
                
                <!-- Standard Column (10%) -->
                <div class="tier-column standard">
                    <div class="tier-header standard">
                        <h3>👤 Standard (10%)</h3>
                        <span class="tier-count">{len(users_by_tier.get('standard', []))} users</span>
                    </div>
                    <div class="tier-users" id="tier-standard">
                        {''.join([f'''
                        <div class="tier-user" data-user-id="{u['id']}">
                            <span class="tier-user-email" title="{u['email']}">{u['email']}</span>
                            <span class="tier-user-stats">${u['total_profit']:.0f}</span>
                            <div class="tier-user-actions">
                                <button class="tier-btn to-team" onclick="changeTier({u['id']}, 'team')" title="Move to Team">🏠</button>
                                <button class="tier-btn to-vip" onclick="changeTier({u['id']}, 'vip')" title="Move to VIP">⭐</button>
                            </div>
                        </div>
                        ''' for u in users_by_tier.get('standard', [])]) or '<div class="tier-empty">No standard users</div>'}
                    </div>
                </div>
            </div>
        </div>
        
        <div class="users-section">
            <h2>👥 Users ({stats.get('total_users', 0)})</h2>
            <div class="search-box">
                <input 
                    type="text" 
                    id="userSearch" 
                    class="search-input" 
                    placeholder="🔍 Search by email or API key..."
                    onkeyup="filterUsers()"
                />
                <button class="clear-search" onclick="clearSearch()">Clear</button>
            </div>
            <table id="usersTable">
                <thead>
                    <tr>
                        <th>Status</th>
                        <th>Email</th>
                        <th>API Key</th>
                        <th>Fingerprint</th>
                        <th>Capital</th>
                        <th>Trades</th>
                        <th>Profit</th>
                        <th>ROI</th>
                        <th>Errors</th>
                    </tr>
                </thead>
                <tbody>{user_rows}</tbody>
            </table>
        </div>
        
        {review_positions_section}
        
        <!-- API Wallet Expiry Section -->
        <div class="users-section" id="expirySection" style="border: 2px solid #667eea;">
            <h2 style="color: #818cf8;">🔑 API Wallet Expiry Tracker</h2>
            <p style="color: #9ca3af; margin-bottom: 15px; font-size: 13px;">
                Hyperliquid API wallets expire after 180 days. Monitor and manage expiry dates for your vault bot and all followers.
            </p>
            
            <!-- Vault Leader Card -->
            <div style="background: #1e293b; border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h3 style="color: #e5e7eb; margin: 0;">🏦 Vault Leader Bot</h3>
                    <span id="vaultExpiryBadge" style="padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 600;">Loading...</span>
                </div>
                <div id="vaultExpiryInfo" style="color: #9ca3af; margin-bottom: 15px;">Loading vault expiry status...</div>
                
                <!-- Update Vault Expiry Form -->
                <div style="background: #0f172a; border-radius: 8px; padding: 15px; border: 1px solid #1e293b;">
                    <label style="color: #94a3b8; font-size: 13px; display: block; margin-bottom: 8px;">
                        Update vault expiry date (from Hyperliquid API page → "Valid Until" column):
                    </label>
                    <div style="display: flex; gap: 10px; align-items: center;">
                        <input type="datetime-local" id="vaultExpiryInput" 
                            style="flex: 1; padding: 8px 12px; background: #1e293b; border: 1px solid #475569; border-radius: 6px; color: #e5e7eb; font-size: 14px;">
                        <button onclick="updateVaultExpiry()" 
                            style="padding: 8px 20px; background: #667eea; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600; white-space: nowrap;">
                            💾 Save
                        </button>
                        <button onclick="forceExpiryCheck()" 
                            style="padding: 8px 20px; background: #f59e0b; color: #1e293b; border: none; border-radius: 6px; cursor: pointer; font-weight: 600; white-space: nowrap;">
                            🧪 Test Check Now
                        </button>
                    </div>
                    <div id="vaultUpdateResult" style="margin-top: 8px; font-size: 13px;"></div>
                </div>
            </div>
            
            <!-- Follower Expiry Table -->
            <div style="background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155;">
                <h3 style="color: #e5e7eb; margin: 0 0 15px 0;">👥 Follower API Wallets</h3>
                <div id="followerExpiryStats" style="display: flex; gap: 20px; margin-bottom: 15px;">
                    <div style="color: #9ca3af; font-size: 13px;">Loading...</div>
                </div>
                <table id="followerExpiryTable" style="width: 100%;">
                    <thead>
                        <tr>
                            <th style="text-align: left; padding: 8px; color: #9ca3af; font-size: 12px; border-bottom: 1px solid #334155;">Email</th>
                            <th style="text-align: left; padding: 8px; color: #9ca3af; font-size: 12px; border-bottom: 1px solid #334155;">Expires</th>
                            <th style="text-align: center; padding: 8px; color: #9ca3af; font-size: 12px; border-bottom: 1px solid #334155;">Days Left</th>
                            <th style="text-align: center; padding: 8px; color: #9ca3af; font-size: 12px; border-bottom: 1px solid #334155;">Agent</th>
                            <th style="text-align: center; padding: 8px; color: #9ca3af; font-size: 12px; border-bottom: 1px solid #334155;">Status</th>
                        </tr>
                    </thead>
                    <tbody id="followerExpiryBody">
                        <tr><td colspan="5" style="text-align: center; padding: 20px; color: #6b7280;">Loading...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="errors-section">
            <div class="errors-header">
                <h2>⚠️ Error History (SGT / GMT+8)</h2>
                <div class="error-legend">
                    <div class="legend-item"><span class="legend-dot critical"></span> Auth/Credential</div>
                    <div class="legend-item"><span class="legend-dot warning"></span> Network/Timeout</div>
                    <div class="legend-item"><span class="legend-dot funds"></span> Insufficient Funds</div>
                    <div class="legend-item"><span class="legend-dot database"></span> Database</div>
                    <div class="legend-item"><span class="legend-dot code"></span> Code/System</div>
                    <div class="legend-item"><span class="legend-dot exchange"></span> Exchange API</div>
                    <div class="legend-item"><span class="legend-dot info"></span> Other</div>
                </div>
            </div>
            <div class="search-box">
                <input 
                    type="text" 
                    id="errorSearch" 
                    class="search-input" 
                    placeholder="🔍 Search errors by user, type, or message..."
                    onkeyup="filterErrors()"
                />
                <select id="errorTimeFilter" class="filter-select" onchange="filterErrors()">
                    <option value="">All Time</option>
                    <option value="24">Last 24 Hours</option>
                    <option value="168">Last 7 Days</option>
                    <option value="720">Last 30 Days</option>
                </select>
                <select id="errorTypeFilter" class="filter-select" onchange="filterErrors()">
                    <option value="">All Error Types</option>
                    <option value="auth">🔴 Auth/Credential</option>
                    <option value="network">🟠 Network/Timeout</option>
                    <option value="funds">🟣 Insufficient Funds</option>
                    <option value="trade">🔵 Trade Execution</option>
                    <option value="database">💗 Database</option>
                    <option value="code">🔷 Code/System</option>
                    <option value="exchange">🟧 Exchange API</option>
                    <option value="other">⚪ Other</option>
                </select>
                <button class="clear-search" onclick="clearErrorFilters()">Clear</button>
            </div>
            <div id="errorCount" style="color: #9ca3af; font-size: 13px; margin-bottom: 15px;"></div>
            {error_items}
            <div class="pagination" id="errorPagination"></div>
        </div>
    </div>
    
    <script>
    // ============ DYNAMIC YEAR POPULATION ============
    async function populateYears() {{
        const yearSelect = document.getElementById('reportYear');
        
        try {{
            // Fetch available years from database
            const response = await fetch(`/admin/reports/available-years?password=${{'{ADMIN_PASSWORD}'}}`);
            const result = await response.json();
            
            if (result.status === 'success') {{
                const years = result.years;
                const currentYear = result.current_year;
                
                years.forEach(year => {{
                    const option = document.createElement('option');
                    option.value = year;
                    option.textContent = year;
                    if (year === currentYear) {{
                        option.selected = true;
                    }}
                    yearSelect.appendChild(option);
                }});
                
                // Load income summary after years are populated
                loadIncomeSummary();
            }} else {{
                // Fallback: just show current year
                const currentYear = new Date().getFullYear();
                const option = document.createElement('option');
                option.value = currentYear;
                option.textContent = currentYear;
                option.selected = true;
                yearSelect.appendChild(option);
                
                // Load income summary after fallback year is set
                loadIncomeSummary();
            }}
        }} catch (error) {{
            console.error('Error populating years:', error);
            // Fallback: just show current year
            const currentYear = new Date().getFullYear();
            const option = document.createElement('option');
            option.value = currentYear;
            option.textContent = currentYear;
            option.selected = true;
            yearSelect.appendChild(option);
            
            // Load income summary after fallback year is set
            loadIncomeSummary();
        }}
    }}
    
    // Populate years on load
    populateYears();
    
    // ============ TAX REPORTS FUNCTIONALITY ============
    const ADMIN_PASSWORD = '{ADMIN_PASSWORD}';
    
    function downloadMonthlyCSV() {{
        const year = document.getElementById('reportYear').value;
        const month = document.getElementById('reportMonth').value;
        
        if (!month) {{
            alert('Please select a month');
            return;
        }}
        
        const url = `/admin/reports/monthly-csv?year=${{year}}&month=${{month}}&password=${{ADMIN_PASSWORD}}`;
        window.location.href = url;
    }}
    
    function downloadYearlyCSV() {{
        const year = document.getElementById('reportYear').value;
        const url = `/admin/reports/yearly-csv?year=${{year}}&password=${{ADMIN_PASSWORD}}`;
        window.location.href = url;
    }}
    
    function downloadUserFeesCSV() {{
        const year = document.getElementById('reportYear').value;
        const startDate = `${{year}}-01-01`;
        const endDate = `${{year}}-12-31`;
        
        const url = `/admin/reports/user-fees-csv?start_date=${{startDate}}&end_date=${{endDate}}&password=${{ADMIN_PASSWORD}}`;
        window.location.href = url;
    }}
    
    // Load income summary on page load
    async function loadIncomeSummary() {{
        const year = document.getElementById('reportYear').value;
        
        // Skip if year is not selected (dropdown not populated yet)
        if (!year) {{
            console.log('Skipping income summary - year not selected');
            return;
        }}
        
        try {{
            const response = await fetch(`/admin/reports/income-summary?year=${{year}}&password=${{ADMIN_PASSWORD}}`);
            const result = await response.json();
            
            if (result.status === 'success') {{
                const data = result.data;
                
                const summaryHTML = `
                    <div class="income-card">
                        <div class="income-label">Total Fees Received</div>
                        <div class="income-value">$${{data.total_fees_received.toFixed(2)}}</div>
                    </div>
                    <div class="income-card">
                        <div class="income-label">Total Payments</div>
                        <div class="income-value">${{data.total_payments}}</div>
                    </div>
                    <div class="income-card">
                        <div class="income-label">Paying Users</div>
                        <div class="income-value">${{data.unique_users_year}}</div>
                    </div>
                    <div class="income-card">
                        <div class="income-label">Avg Fee/Month</div>
                        <div class="income-value">$${{data.avg_fee_per_month.toFixed(2)}}</div>
                    </div>
                    <div class="income-card">
                        <div class="income-label">Avg Fee/User</div>
                        <div class="income-value">$${{data.avg_fee_per_user.toFixed(2)}}</div>
                    </div>
                `;
                
                document.getElementById('incomeSummary').innerHTML = summaryHTML;
            }}
        }} catch (error) {{
            console.error('Error loading income summary:', error);
        }}
    }}
    
    // Update summary when year changes
    document.getElementById('reportYear').addEventListener('change', loadIncomeSummary);
    
    // Note: loadIncomeSummary is now called from within populateYears() after dropdown is populated
    
    // ============ USER SEARCH FUNCTIONALITY ============
    function filterUsers() {{
        const searchInput = document.getElementById('userSearch').value.toLowerCase();
        const table = document.getElementById('usersTable');
        const rows = table.getElementsByTagName('tr');
        
        let visibleCount = 0;
        // Start from 1 to skip header row
        for (let i = 1; i < rows.length; i++) {{
            const row = rows[i];
            const text = row.textContent.toLowerCase();
            
            if (text.includes(searchInput)) {{
                row.style.display = '';
                visibleCount++;
            }} else {{
                row.style.display = 'none';
            }}
        }}
        
        // Update visible count
        const header = document.querySelector('.users-section h2');
        const totalUsers = {stats.get('total_users', 0)};
        if (searchInput) {{
            header.textContent = `👥 Users (${{visibleCount}} of ${{totalUsers}})`;
        }} else {{
            header.textContent = `👥 Users (${{totalUsers}})`;
        }}
    }}
    
    function clearSearch() {{
        document.getElementById('userSearch').value = '';
        filterUsers();
    }}
    
    // ============ ERROR FILTERING FUNCTIONALITY ============
    function filterErrors() {{
        const searchInput = document.getElementById('errorSearch').value.toLowerCase();
        const typeFilter = document.getElementById('errorTypeFilter').value;
        const timeFilter = document.getElementById('errorTimeFilter').value;
        const errorItems = document.querySelectorAll('.error-item');
        
        console.log('Filter triggered:', {{ searchInput, typeFilter, timeFilter, itemCount: errorItems.length }});
        
        let visibleCount = 0;
        let totalErrors = errorItems.length;
        
        // Calculate time cutoff if time filter is set
        let cutoffTime = null;
        if (timeFilter) {{
            const hoursAgo = parseInt(timeFilter);
            cutoffTime = new Date(Date.now() - (hoursAgo * 60 * 60 * 1000));
        }}
        
        errorItems.forEach(item => {{
            const user = (item.getAttribute('data-user') || '').toLowerCase();
            const message = (item.getAttribute('data-message') || '').toLowerCase();
            const category = (item.getAttribute('data-error-category') || '').toLowerCase();
            const errorType = (item.getAttribute('data-error-type') || '').toLowerCase();
            const timestamp = item.getAttribute('data-timestamp') || '';
            
            // Check search text match (search in user, message, and category)
            const searchMatch = !searchInput || 
                                user.includes(searchInput) || 
                                message.includes(searchInput) || 
                                category.includes(searchInput);
            
            // Check type filter match
            const typeMatch = !typeFilter || errorType === typeFilter;
            
            // Check time filter match
            let timeMatch = true;
            if (cutoffTime && timestamp) {{
                // Parse timestamp like "2025-11-25 19:54:22 SGT"
                const tsWithoutTZ = timestamp.replace(' SGT', '');
                const itemTime = new Date(tsWithoutTZ);
                // Adjust for SGT (add 8 hours to compare with local)
                timeMatch = itemTime >= cutoffTime;
            }}
            
            // Show/hide based on all filters
            if (searchMatch && typeMatch && timeMatch) {{
                item.classList.remove('hidden');
                visibleCount++;
            }} else {{
                item.classList.add('hidden');
            }}
        }});
        
        console.log('Filter results:', {{ visibleCount, totalErrors }});
        
        // Update count display
        const countDisplay = document.getElementById('errorCount');
        if (searchInput || typeFilter || timeFilter) {{
            countDisplay.textContent = `Showing ${{visibleCount}} of ${{totalErrors}} errors`;
            countDisplay.style.display = 'block';
        }} else {{
            countDisplay.style.display = 'none';
        }}
        
        // Reset pagination to page 1 when filtering
        currentPage = 1;
        paginateErrors();
    }}
    
    function clearErrorFilters() {{
        document.getElementById('errorSearch').value = '';
        document.getElementById('errorTypeFilter').value = '';
        document.getElementById('errorTimeFilter').value = '';
        
        // Remove all hidden classes
        const errorItems = document.querySelectorAll('.error-item');
        errorItems.forEach(item => item.classList.remove('hidden'));
        
        // Hide count display
        document.getElementById('errorCount').style.display = 'none';
        
        // Reset pagination
        currentPage = 1;
        paginateErrors();
    }}
    
    // ============ ERROR PAGINATION ============
    const errorsPerPage = 10;
    let currentPage = 1;
    let totalErrors = 0;
    
    function paginateErrors() {{
        // Only paginate visible (not filtered out) errors
        const allErrors = document.querySelectorAll('.error-item');
        const visibleErrors = Array.from(allErrors).filter(item => !item.classList.contains('hidden'));
        totalErrors = visibleErrors.length;
        const totalPages = Math.ceil(totalErrors / errorsPerPage);
        
        // First, hide all errors
        allErrors.forEach(item => {{
            // Don't touch hidden class (used by filters)
            // Just use display for pagination
            if (!item.classList.contains('hidden')) {{
                item.style.display = 'none';
            }}
        }});
        
        // Show only the errors for current page
        visibleErrors.forEach((item, index) => {{
            const pageNumber = Math.floor(index / errorsPerPage) + 1;
            if (pageNumber === currentPage) {{
                item.style.display = 'block';
            }}
        }});
        
        // Build pagination controls
        const pagination = document.getElementById('errorPagination');
        if (totalPages <= 1) {{
            pagination.style.display = 'none';
            return;
        }}
        
        pagination.style.display = 'flex';
        let html = '';
        
        // Previous button
        html += `<button class="page-btn" onclick="changePage(${{currentPage - 1}})" ${{currentPage === 1 ? 'disabled' : ''}}>← Prev</button>`;
        
        // Page info
        html += `<span class="page-info">Page ${{currentPage}} of ${{totalPages}} (${{totalErrors}} errors)</span>`;
        
        // Next button
        html += `<button class="page-btn" onclick="changePage(${{currentPage + 1}})" ${{currentPage === totalPages ? 'disabled' : ''}}>Next →</button>`;
        
        // Jump to first/last
        if (totalPages > 3) {{
            html += `<button class="page-btn" onclick="changePage(1)" ${{currentPage === 1 ? 'disabled' : ''}}>First</button>`;
            html += `<button class="page-btn" onclick="changePage(${{totalPages}})" ${{currentPage === totalPages ? 'disabled' : ''}}>Last</button>`;
        }}
        
        pagination.innerHTML = html;
    }}
    
    function changePage(page) {{
        const visibleErrors = Array.from(document.querySelectorAll('.error-item')).filter(item => !item.classList.contains('hidden'));
        const totalPages = Math.ceil(visibleErrors.length / errorsPerPage);
        if (page < 1 || page > totalPages) return;
        
        currentPage = page;
        paginateErrors();
        
        // Scroll to errors section
        document.querySelector('.errors-section').scrollIntoView({{ behavior: 'smooth', block: 'start' }});
    }}
    
    // Initialize pagination on page load
    window.addEventListener('load', () => {{
        paginateErrors();
    }});
    
    // Change user fee tier
    function changeTier(userId, newTier) {{
        console.log('changeTier called:', userId, newTier);
        const tierNames = {{'team': 'Team (0%)', 'vip': 'VIP (5%)', 'standard': 'Standard (10%)'}};
        if (confirm(`Move user to ${{tierNames[newTier]}}?`)) {{
            console.log('User confirmed, sending request...');
            fetch('/admin/update-user-tier', {{
                method: 'POST',
                headers: {{
                    'Content-Type': 'application/json',
                    'X-Admin-Key': '{ADMIN_PASSWORD}'
                }},
                body: JSON.stringify({{user_id: userId, new_tier: newTier}})
            }})
            .then(response => {{
                console.log('Response status:', response.status);
                if (!response.ok) {{
                    return response.text().then(text => {{
                        console.error('Error response body:', text);
                        try {{
                            const data = JSON.parse(text);
                            throw new Error(data.detail || 'Update failed');
                        }} catch (e) {{
                            throw new Error(text || 'Update failed');
                        }}
                    }});
                }}
                return response.json();
            }})
            .then(data => {{
                console.log('Tier update successful:', data);
                alert('Success! ' + (data.message || 'User tier updated'));
                location.reload();
            }})
            .catch(err => {{
                console.error('Tier update error:', err);
                alert('Error updating tier: ' + err.message);
            }});
        }} else {{
            console.log('User cancelled');
        }}
    }}
    
    // Review position deletion
    function deletePosition(posId) {{
        if (confirm('Delete this position from review? This action cannot be undone.')) {{
            fetch('/admin/delete-review-position/' + posId, {{
                method: 'DELETE',
                headers: {{'X-Admin-Key': '{ADMIN_PASSWORD}'}}
            }})
            .then(response => {{
                if (!response.ok) {{
                    return response.json().then(data => {{
                        throw new Error(data.detail || 'Delete failed');
                    }});
                }}
                return response.json();
            }})
            .then(data => {{
                console.log('Delete successful:', data);
                location.reload();
            }})
            .catch(err => {{
                console.error('Delete error:', err);
                alert('Error deleting position: ' + err.message);
            }});
        }}
    }}

    // ==================== API EXPIRY MANAGEMENT ====================
    
    async function loadExpiryStatus() {{
        try {{
            const response = await fetch(`/admin/api-expiry/status?password=${{'{ADMIN_PASSWORD}'}}`);
            const data = await response.json();
            
            // Vault Leader section
            const vaultInfo = document.getElementById('vaultExpiryInfo');
            const vaultBadge = document.getElementById('vaultExpiryBadge');
            
            if (data.vault_leader) {{
                const v = data.vault_leader;
                const statusColors = {{
                    'ok': {{ bg: '#064e3b', text: '#6ee7b7', label: '✅ OK' }},
                    'warning': {{ bg: '#78350f', text: '#fbbf24', label: '⚠️ Expiring Soon' }},
                    'critical': {{ bg: '#7f1d1d', text: '#fca5a5', label: '🔴 Critical' }},
                    'expired': {{ bg: '#7f1d1d', text: '#fca5a5', label: '⛔ Expired' }}
                }};
                const s = statusColors[v.status] || statusColors['ok'];
                vaultBadge.style.background = s.bg;
                vaultBadge.style.color = s.text;
                vaultBadge.textContent = s.label;
                
                vaultInfo.innerHTML = `
                    <div style="display: flex; gap: 30px; flex-wrap: wrap;">
                        <div><span style="color: #6b7280;">Expires:</span> <strong style="color: #e5e7eb;">${{v.expiry_date}}</strong></div>
                        <div><span style="color: #6b7280;">Days left:</span> <strong style="color: ${{v.days_remaining <= 7 ? '#ef4444' : v.days_remaining <= 30 ? '#fbbf24' : '#10b981'}};">${{v.days_remaining}}</strong></div>
                    </div>
                `;
            }} else {{
                vaultBadge.style.background = '#374151';
                vaultBadge.style.color = '#9ca3af';
                vaultBadge.textContent = 'Not Set';
                vaultInfo.innerHTML = '<span style="color: #f59e0b;">⚠️ No vault expiry date configured. Set one below after authorizing your API wallet on Hyperliquid.</span>';
            }}
            
            // Follower stats
            const statsDiv = document.getElementById('followerExpiryStats');
            statsDiv.innerHTML = `
                <div style="background: #0f172a; padding: 8px 16px; border-radius: 8px;">
                    <span style="color: #6b7280; font-size: 12px;">Tracked</span><br>
                    <strong style="color: #e5e7eb; font-size: 18px;">${{data.total_followers_tracked}}</strong>
                </div>
                <div style="background: #0f172a; padding: 8px 16px; border-radius: 8px;">
                    <span style="color: #6b7280; font-size: 12px;">Expiring Soon</span><br>
                    <strong style="color: ${{data.expiring_soon > 0 ? '#fbbf24' : '#10b981'}}; font-size: 18px;">${{data.expiring_soon}}</strong>
                </div>
                <div style="background: #0f172a; padding: 8px 16px; border-radius: 8px;">
                    <span style="color: #6b7280; font-size: 12px;">Expired</span><br>
                    <strong style="color: ${{data.expired > 0 ? '#ef4444' : '#10b981'}}; font-size: 18px;">${{data.expired}}</strong>
                </div>
            `;
            
            // Follower table
            const tbody = document.getElementById('followerExpiryBody');
            if (data.followers && data.followers.length > 0) {{
                tbody.innerHTML = data.followers.map(f => {{
                    const statusColors = {{
                        'ok': {{ bg: '#064e3b', text: '#6ee7b7', dot: '🟢' }},
                        'warning': {{ bg: '#78350f', text: '#fbbf24', dot: '🟡' }},
                        'critical': {{ bg: '#7f1d1d', text: '#fca5a5', dot: '🔴' }},
                        'expired': {{ bg: '#7f1d1d', text: '#fca5a5', dot: '⛔' }}
                    }};
                    const s = statusColors[f.status] || statusColors['ok'];
                    const daysColor = f.days_remaining < 0 ? '#ef4444' : f.days_remaining <= 7 ? '#ef4444' : f.days_remaining <= 30 ? '#fbbf24' : '#10b981';
                    
                    return `<tr>
                        <td style="padding: 10px 8px; color: #e5e7eb; border-bottom: 1px solid #1e293b;">${{f.email}}</td>
                        <td style="padding: 10px 8px; color: #9ca3af; border-bottom: 1px solid #1e293b; font-size: 13px;">${{f.expiry_date}}</td>
                        <td style="padding: 10px 8px; text-align: center; border-bottom: 1px solid #1e293b;">
                            <strong style="color: ${{daysColor}};">${{f.days_remaining < 0 ? 'EXPIRED' : f.days_remaining + 'd'}}</strong>
                        </td>
                        <td style="padding: 10px 8px; text-align: center; border-bottom: 1px solid #1e293b;">
                            ${{f.agent_active ? '<span style="color: #10b981;">● Active</span>' : '<span style="color: #6b7280;">○ Off</span>'}}
                        </td>
                        <td style="padding: 10px 8px; text-align: center; border-bottom: 1px solid #1e293b;">
                            <span style="background: ${{s.bg}}; color: ${{s.text}}; padding: 2px 10px; border-radius: 12px; font-size: 12px;">${{s.dot}} ${{f.status}}</span>
                        </td>
                    </tr>`;
                }}).join('');
            }} else {{
                tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 30px; color: #6b7280;">No followers with API wallets configured yet</td></tr>';
            }}
            
        }} catch (err) {{
            console.error('Failed to load expiry status:', err);
            document.getElementById('vaultExpiryInfo').innerHTML = '<span style="color: #ef4444;">Failed to load expiry data</span>';
        }}
    }}
    
    async function updateVaultExpiry() {{
        const input = document.getElementById('vaultExpiryInput');
        const resultDiv = document.getElementById('vaultUpdateResult');
        
        if (!input.value) {{
            resultDiv.innerHTML = '<span style="color: #ef4444;">Please select a date</span>';
            return;
        }}
        
        // Convert local datetime to ISO
        const expiryDate = new Date(input.value).toISOString();
        
        try {{
            const response = await fetch(`/admin/api-expiry/vault?password=${{'{ADMIN_PASSWORD}'}}&expiry_date=${{encodeURIComponent(expiryDate)}}`, {{
                method: 'POST'
            }});
            const data = await response.json();
            
            if (data.status === 'success') {{
                resultDiv.innerHTML = `<span style="color: #10b981;">✅ Updated! Expires: ${{data.expiry_date}} (${{data.days_remaining}} days from now). Reminders reset.</span>`;
                // Reload the section
                setTimeout(loadExpiryStatus, 1000);
            }} else {{
                resultDiv.innerHTML = `<span style="color: #ef4444;">❌ ${{data.detail || data.error || 'Failed'}}</span>`;
            }}
        }} catch (err) {{
            resultDiv.innerHTML = `<span style="color: #ef4444;">❌ Error: ${{err.message}}</span>`;
        }}
    }}
    
    async function forceExpiryCheck() {{
        const resultDiv = document.getElementById('vaultUpdateResult');
        resultDiv.innerHTML = '<span style="color: #f59e0b;">⏳ Running expiry check & sending emails...</span>';
        
        try {{
            const response = await fetch(`/admin/api-expiry/check-now?password=${{'{ADMIN_PASSWORD}'}}`, {{
                method: 'POST'
            }});
            const data = await response.json();
            
            if (data.status === 'success') {{
                resultDiv.innerHTML = `<span style="color: #10b981;">✅ ${{data.message}}</span>`;
                setTimeout(loadExpiryStatus, 1000);
            }} else {{
                resultDiv.innerHTML = `<span style="color: #ef4444;">❌ ${{data.detail || 'Failed'}}</span>`;
            }}
        }} catch (err) {{
            resultDiv.innerHTML = `<span style="color: #ef4444;">❌ Error: ${{err.message}}</span>`;
        }}
    }}
    
    // Load expiry data on page load
    loadExpiryStatus();
    // Refresh every 5 minutes
    setInterval(loadExpiryStatus, 300000);
    
    </script>
</body>
</html>"""
