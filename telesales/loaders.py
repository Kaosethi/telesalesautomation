# telesales/loaders.py
"""
Data loaders for candidate pools.

Design:
- When USE_REAL_DB=false  -> return deterministic mock data (easy to test end‑to‑end now)
- When USE_REAL_DB=true   -> placeholders where we will add real SQL later

Each loader returns a standardized pandas DataFrame with at least:
  username, phone, source_key, platform, last_login, last_seen,
  reward_tier (e.g., GOLD/SILVER), tier (optional for Tier A), ark_gem_balance (optional)

Windows:
- We simulate "Hot Lead" (3–7 days inactive), "Cold Lead" (8–14), "Hibernated" (15+)
- Later we will derive windows from the Google Sheet’s Windows tab
"""

from __future__ import annotations

from typing import Optional, Tuple
from datetime import datetime, timedelta
import os
import random

import pandas as pd

from .constants import (
    SOURCE_PC, SOURCE_MOBILE,
    WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED,
)


# ----------------------------- helpers ----------------------------------------

def _today() -> datetime:
    # Keep tz-naive here; we compute inactivity days elsewhere using app tz if needed
    return datetime.now()


def _inactive_range_for_window(window_label: str) -> Tuple[int, int]:
    """
    Return (min_days, max_days_inclusive) for a given window label.
    For Hibernated we clamp to a wide range (15–40) to keep mock data reasonable.
    """
    if window_label == WINDOW_HOT:
        return (3, 7)
    if window_label == WINDOW_COLD:
        return (8, 14)
    # Hibernated: 15+
    return (15, 40)


def _gen_last_activity(dmin: int, dmax: int) -> Tuple[datetime, datetime]:
    """
    Generate a (last_login, last_seen) pair such that inactivity is within [dmin, dmax].
    We keep last_seen >= last_login.
    """
    days = random.randint(dmin, dmax)
    base = _today() - timedelta(days=days)
    last_login = base.replace(hour=10, minute=0, second=0, microsecond=0)
    last_seen = last_login + timedelta(hours=random.randint(0, 12))
    return last_login, last_seen


def _mock_reward_tier() -> str:
    return random.choice(["GOLD", "SILVER"])


def _mock_tier() -> str:
    """Rough distribution where some are Tier A and others non‑A."""
    return random.choice(["A-1", "A-2", "B-1", "B-2", "C-1"])


def _mock_phone() -> str:
    """
    Generate a Thai‑looking number (10 digits, often starting with 0).
    We keep it simple for mocks.
    """
    start = random.choice(["08", "09", "06"])
    rest = "".join(random.choice("0123456789") for _ in range(8))
    return start + rest


# ----------------------------- mock loaders -----------------------------------

def _mock_candidates(source_key: str, window_label: str, n: int = 50) -> pd.DataFrame:
    """
    Deterministic‑ish mock set per source+window. Use TEST_SEED to pin results.
    """
    seed = int(os.getenv("TEST_SEED", "12345"))
    random.seed(f"{source_key}|{window_label}|{seed}")

    dmin, dmax = _inactive_range_for_window(window_label)
    rows = []
    for i in range(1, n + 1):
        username = f"{source_key}_user{i:03d}"
        phone = _mock_phone()
        last_login, last_seen = _gen_last_activity(dmin, dmax)
        rows.append(
            {
                "username": username,
                "phone": phone,
                "source_key": source_key,
                "platform": source_key,  # alias used elsewhere
                "last_login": last_login,
                "last_seen": last_seen,
                "reward_tier": _mock_reward_tier(),   # GOLD/SILVER
                "tier": _mock_tier(),                 # A‑*, B‑*, C‑*
                "ark_gem_balance": random.randint(1000, 50000),  # pseudo balance
            }
        )
    return pd.DataFrame(rows)


# ----------------------------- public API -------------------------------------

def load_candidates_for_window(
    source_key: str,
    window_label: str,
    use_real_db: bool = False,
    db_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Return candidates from the given source within a specific window.
    - In mock mode: generate synthetic data aligned with the window inactivity range.
    - In real mode: TODO (SQL to be added when DB schema is confirmed)
    """
    if not use_real_db:
        return _mock_candidates(source_key, window_label, n=40)

    # ---- Real DB branch (placeholder) ----
    # Here we will use SQLAlchemy or psycopg2 depending on your DB.
    # Example outline:
    #   engine = create_engine(db_url)
    #   query = """
    #       SELECT username, phone, ... FROM some_table
    #       WHERE last_login BETWEEN :start AND :end
    #   """
    #   df = pd.read_sql(query, engine, params={...})
    # For now, return empty and let the pipeline keep working.
    return pd.DataFrame()
