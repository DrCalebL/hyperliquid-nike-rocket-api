"""
Nike Rocket Hyperliquid Follower System - Database Models
==========================================================

Mirrors Kraken follower_models.py exactly, adapted for Hyperliquid.
Supports:
- Encrypted storage of Hyperliquid wallet credentials
- Signal delivery tracking
- Trade/P&L tracking
- 30-day rolling billing

Author: Nike Rocket Team
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
import os

from config import get_fee_rate, get_tier_display

Base = declarative_base()

# Encryption key for credentials
ENCRYPTION_KEY = os.getenv("CREDENTIALS_ENCRYPTION_KEY")
if ENCRYPTION_KEY:
    cipher = Fernet(ENCRYPTION_KEY.encode())
else:
    cipher = None


class User(Base):
    """Follower user model - WITH ENCRYPTED HYPERLIQUID CREDENTIALS"""
    __tablename__ = "follower_users"
    
    id = Column(Integer, primary_key=True, index=True)
    api_key = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, nullable=False)
    
    # User tier for fee rates (same as Kraken)
    fee_tier = Column(String, default='standard')
    
    # Encrypted Hyperliquid credentials
    hl_private_key_encrypted = Column(Text, nullable=True)
    hl_wallet_address = Column(String, nullable=True)  # Public wallet address (non-sensitive)
    credentials_set = Column(Boolean, default=False)
    
    # Agent status
    agent_active = Column(Boolean, default=False)
    agent_started_at = Column(DateTime, nullable=True)
    agent_last_poll = Column(DateTime, nullable=True)
    
    # Access control
    access_granted = Column(Boolean, default=True)
    suspended_at = Column(DateTime, nullable=True)
    suspension_reason = Column(String, nullable=True)
    
    # Account tracking
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)
    
    # 30-Day Rolling Billing (current cycle)
    billing_cycle_start = Column(DateTime, nullable=True)
    current_cycle_profit = Column(Float, default=0.0)
    current_cycle_trades = Column(Integer, default=0)
    
    # Pending invoice
    pending_invoice_id = Column(String, nullable=True)
    pending_invoice_amount = Column(Float, default=0.0)
    invoice_due_date = Column(DateTime, nullable=True)
    
    # Tier change scheduling
    next_cycle_fee_tier = Column(String, nullable=True)
    
    # All-time tracking
    total_profit = Column(Float, default=0.0)
    total_trades = Column(Integer, default=0)
    total_fees_paid = Column(Float, default=0.0)
    
    # Portfolio tracking
    portfolio_initialized = Column(Boolean, default=False)
    initial_capital = Column(Float, default=0.0)
    
    # Relationships
    signal_deliveries = relationship("SignalDelivery", back_populates="user")
    trades = relationship("Trade", back_populates="user")
    payments = relationship("Payment", back_populates="user")
    
    def set_hl_credentials(self, private_key: str, wallet_address: str):
        """Encrypt and store Hyperliquid credentials"""
        if not cipher:
            raise Exception("Encryption key not configured")
        
        self.hl_private_key_encrypted = cipher.encrypt(private_key.encode()).decode()
        self.hl_wallet_address = wallet_address
        self.credentials_set = True
    
    def get_hl_credentials(self):
        """Decrypt and return Hyperliquid credentials"""
        if not self.credentials_set or not cipher:
            return None, None
        
        try:
            private_key = cipher.decrypt(self.hl_private_key_encrypted.encode()).decode()
            return private_key, self.hl_wallet_address
        except Exception:
            return None, None
    
    def check_payment_status(self) -> bool:
        """Check if user has access (30-day billing system) - same as Kraken"""
        if not self.pending_invoice_id:
            return True
        if not self.invoice_due_date:
            return True
        
        from config import utc_now
        now = utc_now()
        
        due_date = self.invoice_due_date
        if due_date.tzinfo is None:
            from datetime import timezone
            due_date = due_date.replace(tzinfo=timezone.utc)
        
        return now <= due_date
    
    @property
    def fee_percentage(self) -> float:
        return get_fee_rate(self.fee_tier)
    
    @property
    def fee_tier_display(self) -> str:
        return get_tier_display(self.fee_tier)


class Signal(Base):
    """Trading signal from master algorithm"""
    __tablename__ = "signals"
    
    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(String, unique=True, index=True, nullable=False)
    
    # Signal details
    action = Column(String, nullable=False)  # BUY or SELL
    symbol = Column(String, nullable=False)
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    take_profit = Column(Float, nullable=False)
    leverage = Column(Float, default=1.0)
    
    # Risk percentage
    risk_pct = Column(Float, default=0.02)
    
    # Market context
    timeframe = Column(String, nullable=True)
    trend_strength = Column(Float, nullable=True)
    volatility = Column(Float, nullable=True)
    notes = Column(String, nullable=True)
    
    # Timing
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    expires_at = Column(DateTime, nullable=True)
    
    # Relationships
    deliveries = relationship("SignalDelivery", back_populates="signal")


class SignalDelivery(Base):
    """Tracks signal delivery to each user"""
    __tablename__ = "signal_deliveries"
    
    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(Integer, ForeignKey("signals.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("follower_users.id"), nullable=False)
    
    # Delivery tracking
    delivered_at = Column(DateTime, default=datetime.utcnow)
    acknowledged = Column(Boolean, default=False)
    acknowledged_at = Column(DateTime, nullable=True)
    
    # Execution tracking
    executed = Column(Boolean, default=False)
    executed_at = Column(DateTime, nullable=True)
    execution_price = Column(Float, nullable=True)
    
    # Failure tracking
    failed = Column(Boolean, default=False)
    failure_reason = Column(String, nullable=True)
    retry_count = Column(Integer, default=0)
    
    # Relationships
    signal = relationship("Signal", back_populates="deliveries")
    user = relationship("User", back_populates="signal_deliveries")


class OpenPosition(Base):
    """Tracks open positions waiting for TP or SL to fill"""
    __tablename__ = "open_positions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("follower_users.id"), nullable=False)
    signal_id = Column(Integer, ForeignKey("signals.id"), nullable=True)
    
    # Hyperliquid order tracking
    entry_order_id = Column(String, nullable=False)
    tp_order_id = Column(String, nullable=True)
    sl_order_id = Column(String, nullable=True)
    
    # Position details
    symbol = Column(String, nullable=False)  # e.g. "ADA"
    hl_symbol = Column(String, nullable=False)  # Hyperliquid format e.g. "ADA"
    side = Column(String, nullable=False)  # BUY or SELL
    quantity = Column(Float, nullable=False)
    leverage = Column(Float, default=1.0)
    
    # Actual fill price
    entry_fill_price = Column(Float, nullable=True)
    
    # Target prices from signal
    target_tp = Column(Float, nullable=False)
    target_sl = Column(Float, nullable=False)
    
    # Timing
    opened_at = Column(DateTime, default=datetime.utcnow)
    
    # Status
    status = Column(String, default='open')  # open, closed, error
    
    # Relationships
    user = relationship("User")


class Trade(Base):
    """Completed trade record"""
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("follower_users.id"), nullable=False)
    signal_id = Column(Integer, ForeignKey("signals.id"), nullable=True)
    
    # Trade identifiers
    trade_id = Column(String, unique=True, nullable=False)
    hl_order_id = Column(String, nullable=True)
    
    # Timing
    opened_at = Column(DateTime, nullable=False)
    closed_at = Column(DateTime, nullable=False, index=True)
    
    # Trade details
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)
    
    entry_price = Column(Float, nullable=False)
    exit_price = Column(Float, nullable=False)
    position_size = Column(Float, nullable=False)
    leverage = Column(Float, default=1.0)
    
    # P&L
    profit_usd = Column(Float, nullable=False)
    profit_percent = Column(Float, nullable=True)
    
    # Fee tracking
    fee_charged = Column(Float, default=0.0)
    
    # Notes
    notes = Column(String, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="trades")


class Payment(Base):
    """Payment record"""
    __tablename__ = "payments"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("follower_users.id"), nullable=False)
    
    amount_usd = Column(Float, nullable=False)
    currency = Column(String, default="USD")
    
    coinbase_charge_id = Column(String, unique=True, nullable=True)
    status = Column(String, default="pending")
    
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    for_month = Column(String, nullable=True)
    profit_amount = Column(Float, default=0.0)
    tx_hash = Column(String, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="payments")


class SystemStats(Base):
    """System-wide statistics snapshot"""
    __tablename__ = "system_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    
    total_users = Column(Integer, default=0)
    active_users = Column(Integer, default=0)
    suspended_users = Column(Integer, default=0)
    
    total_signals = Column(Integer, default=0)
    total_trades = Column(Integer, default=0)
    total_profit = Column(Float, default=0.0)
    total_fees_collected = Column(Float, default=0.0)
    
    snapshot_at = Column(DateTime, default=datetime.utcnow, index=True)


class BalanceSnapshot(Base):
    """Balance snapshots for equity curve tracking"""
    __tablename__ = "balance_snapshots"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("follower_users.id"), nullable=False)
    
    balance = Column(Float, nullable=False)
    snapshot_type = Column(String, default="auto")  # auto, deposit, withdrawal, initial
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class ErrorLog(Base):
    """Error logs for admin dashboard"""
    __tablename__ = "error_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    api_key = Column(String, nullable=True)
    error_type = Column(String, nullable=False)
    error_message = Column(Text, nullable=True)
    context = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


def get_db_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()


def init_db(engine):
    Base.metadata.create_all(bind=engine)
    print("✅ Database tables created successfully")
    
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    required_tables = [
        'follower_users', 'signals', 'signal_deliveries',
        'trades', 'payments', 'system_stats', 'open_positions',
        'balance_snapshots', 'error_logs'
    ]
    
    missing = [t for t in required_tables if t not in tables]
    if missing:
        print(f"⚠️ Missing tables: {missing}")
    else:
        print("✅ Database schema up to date")


__all__ = [
    'Base', 'User', 'Signal', 'SignalDelivery', 'OpenPosition', 'Trade',
    'Payment', 'SystemStats', 'BalanceSnapshot', 'ErrorLog',
    'get_db_session', 'init_db'
]
