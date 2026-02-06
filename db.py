"""
Nike Rocket Hyperliquid - Database Connection Pool Singleton
=============================================================
Provides a single shared connection pool for all modules.

Usage:
    from db import get_pool
    
    async def my_function():
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetch("SELECT ...")

Author: Nike Rocket Team
"""
import os
import asyncpg
import logging

logger = logging.getLogger("DB_POOL")

_pool = None


def get_database_url() -> str:
    """Get and normalize DATABASE_URL"""
    url = os.getenv("DATABASE_URL")
    if url and url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


async def get_pool() -> asyncpg.Pool:
    """
    Get the shared database connection pool.
    Creates it on first call, reuses thereafter.
    
    Pool settings:
    - min_size=5: Keep 5 connections warm
    - max_size=20: Allow up to 20 concurrent connections
    """
    global _pool
    
    if _pool is None:
        DATABASE_URL = get_database_url()
        if not DATABASE_URL:
            raise Exception("DATABASE_URL not set")
        
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        logger.info("✅ Database pool created (min=5, max=20)")
    
    return _pool


async def close_pool():
    """Close the pool (call on shutdown)"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("🛑 Database pool closed")
