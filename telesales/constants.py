# telesales/constants.py
"""
Single source of truth for column names, source keys, and fixed headers.
Keeping these here prevents typos and makes refactors easy.
"""

# --- App / time ---------------------------------------------------------------

APP_TZ_DEFAULT = "Asia/Bangkok"

# --- Sources (must match Config sheet values) ---------------------------------

SOURCE_PC = "cabal_pc_th"
SOURCE_MOBILE = "cabal_mobile_th"

ALL_SOURCES = (SOURCE_PC, SOURCE_MOBILE)

# --- Window labels (must match your Windows tab 'label' values) ---------------

WINDOW_HOT = "Hot Lead"
WINDOW_COLD = "Cold Lead"
WINDOW_HIBERNATED = "Hibernated"

WINDOW_ORDER = [WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED]  # priority 1→3

# --- Common logical columns ---------------------------------------------------

COL_ASSIGN_DATE = "Assign Date"
COL_USERNAME = "username"
COL_USERNAME_OUT = "Username"  # Non‑A output header
COL_PHONE = "Phone Number"
COL_CALLING_CODE = "Calling Code"
COL_PLATFORM = "platform"       # aka source_key in some contexts
COL_SOURCE_KEY = "source_key"   # internal name we’ll use where needed
COL_TIER = "Tier"
COL_INACTIVE_DAYS = "Inactive Duration (Days)"
COL_REWARD_RANK = "Reward Rank"
COL_TELESALE = "Telesale"       # caller name for Non‑A
COL_WINDOW = "window_label"

# History / enrichment (lives in Compile)
COL_CALL_STATUS = "Call Status"
COL_ANSWER_STATUS = "Answer Status"
COL_RESULT = "Result"
COL_HISTORY = "History"
COL_HISTORY_DATE = "History Date"
COL_FREQUENCY = "frequency"
COL_ATTEMPT_NUM = "Attempt Number"
COL_RECENT_ADMIN = "Recent Admin"

# Tier A specific extras
COL_AMOUNT = "amount"
COL_ARK_GEM = "Ark Gem"
COL_REWARD = "Reward"

# --- Output headers -----------------------------------------------------------
# Keep these EXACTLY as your spec states.

# Tier A daily tab & Compile (finance/game header)
TIER_A_HEADERS = [
    "No.",
    COL_USERNAME,               # "username"
    COL_PHONE,                  # "Phone Number"
    COL_TIER,                   # "Tier"
    COL_INACTIVE_DAYS,          # "Inactive Duration (Days)"
    COL_AMOUNT,                 # "amount"
    COL_ARK_GEM,                # "Ark Gem"
    COL_REWARD,                 # "Reward"
    COL_ASSIGN_DATE,            # "Assign Date"
]

# Non‑A daily tab & Compile (telesales header; finalized 13 cols)
NON_A_HEADERS = [
    "No.",
    COL_USERNAME_OUT,           # "Username"
    COL_CALLING_CODE,           # "Calling Code" (e.g., "=+66")
    COL_PHONE,                  # "Phone Number" (local number, no leading 0)
    COL_TIER,                   # "Tier"
    COL_INACTIVE_DAYS,          # "Inactive Duration (Days)"
    COL_REWARD_RANK,            # "Reward Rank"
    COL_TELESALE,               # "Telesale" (caller)
    COL_ASSIGN_DATE,            # "Assign Date"
    "Recall Date/Time",         # placeholder from spec; we pass through if present
    COL_CALL_STATUS,            # "Call Status"
    COL_ANSWER_STATUS,          # "Answer Status"
    COL_RESULT,                 # "Result"
]

# --- Tiers --------------------------------------------------------------------
# Any label starting with "A-" is Tier A; everything else Non‑A.
TIER_A_PREFIX = "A-"

def is_tier_a(label: str | None) -> bool:
    """Return True if the tier label belongs to Tier A (e.g., A-1, A-2)."""
    if not label:
        return False
    return str(label).strip().upper().startswith("A-")
