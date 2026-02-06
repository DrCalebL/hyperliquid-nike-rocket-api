"""
Nike Rocket Hyperliquid Configuration
======================================

Centralized configuration for fee tiers, constants, and utility functions.
Single source of truth - EXACT MIRROR of Kraken config.

Author: Nike Rocket Team
"""

import os
from datetime import datetime, timezone
from typing import Optional

# =============================================================================
# FEE TIERS - Single Source of Truth (same as Kraken)
# =============================================================================

FEE_TIERS = {
    'team': {
        'rate': 0.00,
        'display': '🏠 Team (0%)',
        'description': 'Team members - no fees'
    },
    'vip': {
        'rate': 0.05,
        'display': '⭐ VIP (5%)',
        'description': 'VIP customers'
    },
    'standard': {
        'rate': 0.10,
        'display': '👤 Standard (10%)',
        'description': 'Standard customers'
    },
}

DEFAULT_TIER = 'standard'


def get_fee_rate(tier: Optional[str]) -> float:
    if not tier or tier not in FEE_TIERS:
        return FEE_TIERS[DEFAULT_TIER]['rate']
    return FEE_TIERS[tier]['rate']


def get_tier_display(tier: Optional[str]) -> str:
    if not tier or tier not in FEE_TIERS:
        return FEE_TIERS[DEFAULT_TIER]['display']
    return FEE_TIERS[tier]['display']


def get_tier_percentage_str(tier: Optional[str]) -> str:
    rate = get_fee_rate(tier)
    return f"{int(rate * 100)}%"


def get_valid_tiers() -> list:
    return list(FEE_TIERS.keys())


# =============================================================================
# DATETIME UTILITIES
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=None)
    return dt


# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

def is_production() -> bool:
    env = os.getenv("ENVIRONMENT", "").lower()
    railway_env = os.getenv("RAILWAY_ENVIRONMENT", "").lower()
    return env == "production" or railway_env == "production" or bool(os.getenv("RAILWAY_PROJECT_ID"))


def get_admin_email() -> str:
    return os.getenv("ADMIN_EMAIL", "calebws87@gmail.com")


# =============================================================================
# BILLING CONSTANTS
# =============================================================================

BILLING_CYCLE_DAYS = 30
PAYMENT_GRACE_DAYS = 7
REMINDER_DAYS = [3, 5, 7]

ERROR_MESSAGE_MAX_LENGTH = 1000
ERROR_CONTEXT_MAX_LENGTH = 2000

# =============================================================================
# HYPERLIQUID-SPECIFIC CONSTANTS
# =============================================================================

SIGNAL_EXPIRATION_MINUTES = 15  # Signals expire after 15 minutes

# Supported Hyperliquid symbols
SUPPORTED_SYMBOLS = [
    'BTC', 'ETH', 'SOL', 'ADA', 'AVAX', 'DOT', 'LINK',
    'MATIC', 'DOGE', 'ARB', 'OP', 'SUI', 'APT', 'SEI',
    'TIA', 'INJ', 'NEAR', 'ATOM', 'FTM', 'ONDO',
]
