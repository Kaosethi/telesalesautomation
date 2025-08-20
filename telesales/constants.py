# telesales/constants.py
"""
Central constants: source keys, window labels, output column names, and headers.

Notes
- We keep separate username column titles for Tier A vs Non‑A:
  * COL_USERNAME      -> "username"   (Tier A spec)
  * COL_USERNAME_OUT  -> "Username"   (Non‑A spec)
- Added COL_SOURCE ("Source") to both headers so platform (PC/Mobile) is visible.
"""

from __future__ import annotations

# --------------------------- source keys --------------------------------------

# Canonical platform/source strings (must match Config + loaders)
SOURCE_PC: str = "cabal_pc_th"
SOURCE_MOBILE: str = "cabal_mobile_th"

# --------------------------- window labels ------------------------------------

WINDOW_HOT: str = "Hot Lead"       # 3–7 days
WINDOW_COLD: str = "Cold"          # 8–14 days
WINDOW_HIBERNATED: str = "Hibernated"  # 15+ days

# --------------------------- column names -------------------------------------

# Shared logical fields (map to exact sheet headers below)
COL_ASSIGN_DATE: str = "Assign Date"

# Tier A uses lowercase "username" per spec; Non‑A uses "Username"
COL_USERNAME: str = "username"
COL_USERNAME_OUT: str = "Username"

# Phone fields
COL_CALLING_CODE: str = "Calling Code"
COL_PHONE: str = "Phone Number"

# New: Source/platform column (PC/Mobile)
COL_SOURCE: str = "Source"

# Tiering / status
COL_TIER: str = "Tier"
COL_INACTIVE_DAYS: str = "Inactive Duration (Days)"

# Finance/game columns (Tier A)
COL_AMOUNT: str = "amount"
COL_ARK_GEM: str = "Ark Gem"
COL_REWARD: str = "Reward"

# Telesales columns (Non‑A)
COL_REWARD_RANK: str = "Reward Rank"
COL_TELESALE: str = "Telesale"

# --------------------------- header schemas -----------------------------------

# Tier A (10 columns)
TIER_A_HEADERS = [
    "No.",
    COL_USERNAME,          # "username"
    COL_PHONE,             # "Phone Number"
    COL_SOURCE,            # "Source" (NEW)
    COL_TIER,              # "Tier"
    COL_INACTIVE_DAYS,     # "Inactive Duration (Days)"
    COL_AMOUNT,            # "amount"
    COL_ARK_GEM,           # "Ark Gem"
    COL_REWARD,            # "Reward"
    COL_ASSIGN_DATE,       # "Assign Date"
]

# Non‑A (14 columns)
NON_A_HEADERS = [
    "No.",
    COL_USERNAME_OUT,      # "Username"
    COL_CALLING_CODE,      # "Calling Code"
    COL_PHONE,             # "Phone Number"
    COL_SOURCE,            # "Source" (NEW)
    COL_TIER,              # "Tier"
    COL_INACTIVE_DAYS,     # "Inactive Duration (Days)"
    COL_REWARD_RANK,       # "Reward Rank"
    COL_TELESALE,          # "Telesale"
    COL_ASSIGN_DATE,       # "Assign Date"
    "Recall Date/Time",
    "Call Status",
    "Answer Status",
    "Result",
]

# --------------------------- helpers ------------------------------------------

def is_tier_a(label: object) -> bool:
    """
    Return True if a tier label is an A‑tier (e.g., 'A-1', 'A-2').
    Empty/None/non‑string -> False.
    """
    if label is None:
        return False
    s = str(label).strip().upper()
    return s.startswith("A-")
