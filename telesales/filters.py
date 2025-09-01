# telesales/filters.py
"""
Filtering layer (Thai rules) applied to candidate pools BEFORE writing.

Inputs:
- pool_df: candidates (from loaders / merged windows). Columns expected:
  username, phone, source_key/platform, reward_tier, last_login/last_seen, tier (optional)
- compile_df: current month's Compile (same tier), used to count statuses this month.
- blacklist_df: central blacklist from the Config sheet: username, phone, source_key
- redeemed_usernames_today: set[str] from Grafana (if enabled)

Config toggles are passed in explicitly so this module stays stateless/testable.
"""

from __future__ import annotations

from typing import Iterable, Optional, Set, Tuple
import pandas as pd
from .utils import today_key


# ---- Thai status sets (as given) ---------------------------------------------

UNREACHABLE_ANS_STATUSES = {
    "ไม่รับสาย",         # no answer
    "ติดต่อไม่ได้",       # cannot contact
    "กดตัดสาย",           # cut the call
    "รับสายไม่สะดวกคุย",  # answered but not convenient to talk
}

ANSWERED_STATUS = "รับสาย"

RESULT_INVALID_NUMBER = "เบอร์เสีย"
RESULT_NOT_INTERESTED = "ไม่สนใจ"
RESULT_NOT_OWNER = "ไม่ใช่เจ้าของไอดี"


# ---- utilities ----------------------------------------------------------------

def _ensure_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _safe_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).fillna("")


def _triple_key(df: pd.DataFrame) -> pd.Series:
    """(phone, username, source_key/platform) as a combined string key for fast matching."""
    phone = _safe_str_series(df.get("phone", pd.Series()))
    user = _safe_str_series(df.get("username", pd.Series()))
    # some code uses "platform", some "source_key" — try both
    src = _safe_str_series(df.get("source_key", df.get("platform", pd.Series())))
    return phone + "|" + user + "|" + src


# ---- main filter ---------------------------------------------------------------

def apply_filters(
    pool_df: pd.DataFrame,
    *,
    compile_df: Optional[pd.DataFrame] = None,
    blacklist_df: Optional[pd.DataFrame] = None,
    redeemed_usernames_today: Optional[Iterable[str]] = None,
    # toggles
    drop_unreachable_repeat: bool = True,
    unreachable_min_count: int = 2,
    drop_answered_this_month: bool = True,
    drop_invalid_number: bool = True,
    drop_not_interested_this_month: bool = True,
    drop_not_owner_as_blacklist: bool = True,
    drop_redeemed_today: bool = True,
) -> pd.DataFrame:
    """
    Returns a filtered copy of pool_df.
    All drops are applied with simple boolean masks; missing columns are treated as non-matching.
    """
    df = _ensure_df(pool_df).copy()
    if df.empty:
        return df

    comp = _ensure_df(compile_df)
    bl = _ensure_df(blacklist_df)
    redeemed_set: Set[str] = set(redeemed_usernames_today or [])

    keep = pd.Series(True, index=df.index)

    # --- Central blacklist (triple match) -------------------------------------
    if not bl.empty:
        bl_key = _triple_key(bl).unique()
        pool_key = _triple_key(df)
        before = keep.sum()
        keep &= ~pool_key.isin(bl_key)
        dropped_blacklist = int(before - keep.sum())
    else:
        dropped_blacklist = 0

    # --- Compile-based rules (this month) -------------------------------------
    if not comp.empty:
        # normalize text cols
        comp_ans = _safe_str_series(comp.get("Answer Status", pd.Series()))
        comp_res = _safe_str_series(comp.get("Result", pd.Series()))
        comp_user = _safe_str_series(comp.get("Username", comp.get("username", pd.Series())))
        comp_phone = _safe_str_series(comp.get("Phone Number", comp.get("phone", pd.Series())))
        comp_platform = _safe_str_series(comp.get("platform", comp.get("source_key", pd.Series())))

        # Build a compact df to count by user+phone+platform this month
        comp_min = pd.DataFrame({
            "username": comp_user,
            "phone": comp_phone,
            "platform": comp_platform,
            "ans": comp_ans,
            "res": comp_res,
        })

        # Today idempotency: drop candidates already assigned today
        comp_assign = _safe_str_series(comp.get("Assign Date", pd.Series()))
        today_str = today_key()
        if "Assign Date" in comp.columns:
            comp_today = pd.DataFrame({
                "username": comp_user,
                "phone": comp_phone,
                "platform": comp_platform,
                "assign": comp_assign,
            })
            today_keys = set(
                comp_today.loc[comp_today["assign"].astype(str) == today_str, ["username","phone","platform"]]
                .dropna(how="all")
                .apply(lambda r: f"{r['phone']}|{r['username']}|{r['platform']}", axis=1)
                .tolist()
            )
        else:
            today_keys = set()

        # Unreachable count ≥ N (this month)
        if drop_unreachable_repeat:
            unreachable_mask = comp_min["ans"].isin(UNREACHABLE_ANS_STATUSES)
            unreachable_counts = (
                comp_min[unreachable_mask]
                .groupby(["username", "phone", "platform"], dropna=False)
                .size()
                .rename("unreach_cnt")
            )
        else:
            unreachable_counts = pd.Series(dtype="int64")

        # Answered-before (this month)
        if drop_answered_this_month:
            answered_keys = set(
                comp_min.loc[comp_min["ans"] == ANSWERED_STATUS, ["username", "phone", "platform"]]
                .dropna(how="all")
                .apply(lambda r: f"{r['phone']}|{r['username']}|{r['platform']}", axis=1)
                .tolist()
            )
        else:
            answered_keys = set()

        # Not interested (this month)
        if drop_not_interested_this_month:
            not_interested_keys = set(
                comp_min.loc[comp_min["res"] == RESULT_NOT_INTERESTED, ["username", "phone", "platform"]]
                .dropna(how="all")
                .apply(lambda r: f"{r['phone']}|{r['username']}|{r['platform']}", axis=1)
                .tolist()
            )
        else:
            not_interested_keys = set()

        # Build pool keys to match against Compile-derived sets
        pool_key = _triple_key(df)

        # Today duplicates
        if today_keys:
            before = keep.sum()
            keep &= ~pool_key.isin(today_keys)
            dropped_idempotent = int(before - keep.sum())
        else:
            dropped_idempotent = 0

        # Apply unreachable≥N
        if drop_unreachable_repeat and not unreachable_counts.empty:
            # join counts onto df by (username, phone, platform)
            df3 = df.assign(_key=pool_key)
            # explode unreachable_counts into a df for merge
            uc_df = unreachable_counts.reset_index().assign(
                _key=lambda t: t["phone"].astype(str) + "|" + t["username"].astype(str) + "|" + t["platform"].astype(str)
            )[['_key', 'unreach_cnt']]
            df3 = df3.merge(uc_df, on="_key", how="left")
            df3["unreach_cnt"] = df3["unreach_cnt"].fillna(0).astype(int)
            before = keep.sum()
            keep &= df3["unreach_cnt"] < int(unreachable_min_count)
            dropped_unreachable = int(before - keep.sum())
            print(f"[filters] dropped_unreachable={dropped_unreachable}")
        else:
            dropped_unreachable = 0

        # Answered this month
        if drop_answered_this_month and answered_keys:
            before = keep.sum()
            keep &= ~pool_key.isin(answered_keys)
            dropped_answered = int(before - keep.sum())
            print(f"[filters] dropped_answered={dropped_answered}")
        else:
            dropped_answered = 0

        # Not interested this month
        if drop_not_interested_this_month and not_interested_keys:
            before = keep.sum()
            keep &= ~pool_key.isin(not_interested_keys)
            dropped_not_interested = int(before - keep.sum())
        else:
            dropped_not_interested = 0

        # Debug print once with counters and date hints for idempotency
        try:
            uniq_assign = sorted(set(comp_assign.dropna().astype(str).unique().tolist()))[:5]
            print(f"[filters] today={today_str} sample_assign={uniq_assign} drops idempotent={dropped_idempotent} unreachable={dropped_unreachable} answered={dropped_answered} not_interested={dropped_not_interested} blacklist={dropped_blacklist}")
        except Exception:
            pass

    # --- Lifetime rules from past results (ever) -------------------------------
    # These require looking at lifetime outcomes. Since we only have current-month Compile right now,
    # we treat them as "drop if present in pool row already" or skip (until lifetime history available).
    if drop_invalid_number and "Result" in df.columns:
        keep &= df["Result"].astype(str) != RESULT_INVALID_NUMBER

    if drop_not_owner_as_blacklist and "Result" in df.columns:
        keep &= df["Result"].astype(str) != RESULT_NOT_OWNER

    # --- Redeemed today from Grafana ------------------------------------------
    if drop_redeemed_today and redeemed_set:
        # pool may use 'username' col
        keep &= ~df["username"].astype(str).isin(redeemed_set)

    # Done
    return df[keep].reset_index(drop=True)
