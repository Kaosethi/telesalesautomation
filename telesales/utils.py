# telesales/utils.py
"""
Small, beginner-friendly helpers:
- Timezone-aware "now" and today's tab key (DD-MM-YYYY)
- Phone normalization + Thailand calling-code split
- Inactive Duration (Days) calculation
"""

from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import re

# --- Timezone helpers ---------------------------------------------------------

def _app_tz() -> ZoneInfo:
    """Return the app timezone, default Asia/Bangkok."""
    return ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Bangkok"))

def now_local() -> datetime:
    """Current time in app timezone."""
    return datetime.now(_app_tz())

def today_key() -> str:
    """Tab name as DD-MM-YYYY in app timezone."""
    d = now_local().date()
    return f"{d.day:02d}-{d.month:02d}-{d.year}"

# --- Phone formatting ---------------------------------------------------------

_DIGITS = re.compile(r"\D+")

def normalize_phone(raw: str | int | float | None) -> str:
    """
    Keep digits only (strip spaces, dashes, dots). Safe for Excel-imported numbers.
    Examples:
      '093-123-4567' -> '0931234567'
      934322113.0    -> '934322113'
    """
    if raw is None:
        return ""
    return _DIGITS.sub("", str(raw))

def split_calling_code_th(local_digits: str) -> tuple[str, str]:
    """
    For Thailand outputs in Non‑A:
      Calling Code  -> '=+66'
      Phone Number  -> local digits (no leading 0)
    Example:
      '0931234567' -> ('=+66', '931234567')
      '812345678'  -> ('=+66', '812345678')
    """
    local = local_digits or ""
    if local.startswith("0"):
        local = local[1:]
    return "=+66", local

# --- Lead age / inactivity ----------------------------------------------------

# telesales/utils.py (only replace inactive_days)
def inactive_days(last_login: datetime | None, last_seen: datetime | None) -> int:
    """
    Inactive Duration (Days) = days since last activity in app timezone.
    Prefer last_login; fallback to last_seen. If both missing, return -1.

    Handles both tz-aware and tz-naive datetimes:
    - If tz-naive: assume it's already in app timezone (Asia/Bangkok) and attach tzinfo.
    - If tz-aware: convert to app timezone.
    """
    base = last_login or last_seen
    if not base:
        return -1

    tz = _app_tz()

    # pandas.Timestamp is a subclass of datetime; can be tz-naive
    if getattr(base, "tzinfo", None) is None:
        # assume local (Bangkok) if naive
        base_local = base.replace(tzinfo=tz).date()
    else:
        base_local = base.astimezone(tz).date()

    return (now_local().date() - base_local).days
