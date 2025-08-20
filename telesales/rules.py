# telesales/rules.py
"""
Rules for windows, dedupe, and re‑query (Non‑A only).

What this provides:
- tag_window(df, label): adds a 'window_label' column to a DataFrame
- earlier_window_wins_dedupe(df): keep 1 row per phone; earlier window (Hot < Cold < Hibernated) wins
- requery_non_a(pools_by_window, target_rows): start from Hot; if short, pull Cold then Hibernated
- build_non_a_pool(pools_by_window, target_rows): helper that wraps requery + dedupe
- build_tier_a_pool(pools_by_window): Tier A = Hot only (no re‑query)

Inputs are plain pandas DataFrames produced by loaders. Each df should have:
  - 'username', 'phone', 'source_key' (or 'platform'), 'last_login'/'last_seen', etc.
  - After calling tag_window(...), they will also have 'window_label'

You can wire this into pipeline.py later like:
  hot_pc  = tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_HOT, ...), WINDOW_HOT)
  hot_mob = tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HOT, ...), WINDOW_HOT)
  cold_pc = tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_COLD, ...), WINDOW_COLD)
  ... (and so on)
  pools = {WINDOW_HOT: [hot_pc, hot_mob], WINDOW_COLD: [cold_pc, cold_mob], WINDOW_HIBERNATED: [...]}

Then:
  non_a_rows = build_non_a_pool(pools, target_rows=available_callers * PER_CALLER_TARGET)
  tier_a_rows = build_tier_a_pool(pools)
"""

from __future__ import annotations

from typing import Dict, List, Tuple
import pandas as pd

from .constants import (
    SOURCE_PC, SOURCE_MOBILE,
    WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED,
)

# Window priority: earlier (hot) wins over later (cold/hibernated)
WINDOW_PRIORITY: List[str] = [WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED]
_WINDOW_RANK = {w: i for i, w in enumerate(WINDOW_PRIORITY)}


# --------------------------------------------------------------------------- #
# Basic helpers
# --------------------------------------------------------------------------- #

def tag_window(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Return a copy of df with a 'window_label' column set to label.
    Safe to call on empty/None; you get an empty DataFrame back.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["window_label"] = label
    return out


def _concat(pools_by_window: Dict[str, List[pd.DataFrame]]) -> pd.DataFrame:
    """Concatenate all provided pools (by window) into a single DataFrame."""
    frames: List[pd.DataFrame] = []
    for w in WINDOW_PRIORITY:
        for df in pools_by_window.get(w, []):
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def earlier_window_wins_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep ONE row per phone number, preferring the row with the earlier window
    (Hot -> Cold -> Hibernated). If phones appear in the same window from
    multiple sources, keep the first occurrence (stable).
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    # Rank windows; unknown windows get a large rank (go to the back)
    tmp["_win_rank"] = tmp.get("window_label", "").map(lambda w: _WINDOW_RANK.get(str(w), 9999))
    # Deduplicate by phone: keep the row with the smallest window rank, then first occurrence
    tmp.sort_values(by=["_win_rank"], kind="stable", inplace=True)
    deduped = tmp.drop_duplicates(subset=["phone"], keep="first").drop(columns=["_win_rank"])
    return deduped.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Re‑query loop (Non‑A)
# --------------------------------------------------------------------------- #

def requery_non_a(
    pools_by_window: Dict[str, List[pd.DataFrame]],
    target_rows: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Start with HOT; if short, append COLD; if still short, append HIBERNATED.
    Deduplicate phones with earlier_window_wins_dedupe at each step.
    Stop once we reach target_rows or run out of data.

    Returns (final_df, debug_counts_by_window)
    """
    # Start with Hot only
    chosen_frames: List[pd.DataFrame] = []
    counts: Dict[str, int] = {}

    for w in WINDOW_PRIORITY:
        # Add all pools for this window
        for df in pools_by_window.get(w, []):
            if isinstance(df, pd.DataFrame) and not df.empty:
                chosen_frames.append(df)

        # Concatenate what we have so far and dedupe
        if chosen_frames:
            merged = pd.concat(chosen_frames, ignore_index=True)
        else:
            merged = pd.DataFrame()

        deduped = earlier_window_wins_dedupe(merged)
        counts[w] = len(deduped)

        # Check target after every window
        if len(deduped) >= int(target_rows):
            return deduped.head(int(target_rows)).reset_index(drop=True), counts

    # Not enough rows even after Hibernated; return whatever we got
    return (earlier_window_wins_dedupe(pd.concat(chosen_frames, ignore_index=True))
            if chosen_frames else pd.DataFrame()), counts


def build_non_a_pool(
    pools_by_window: Dict[str, List[pd.DataFrame]],
    target_rows: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Convenience wrapper around requery_non_a.
    """
    return requery_non_a(pools_by_window, target_rows=target_rows)


# --------------------------------------------------------------------------- #
# Tier A pool (Hot only, no re‑query)
# --------------------------------------------------------------------------- #

def build_tier_a_pool(pools_by_window: Dict[str, List[pd.DataFrame]]) -> pd.DataFrame:
    """
    Tier A uses only HOT window (no re‑query).
    If multiple sources provide HOT pools, we concat and dedupe by phone with earlier-window wins
    (here all are HOT, so it just keeps first seen).
    """
    hot_frames = pools_by_window.get(WINDOW_HOT, [])
    frames = [df for df in hot_frames if isinstance(df, pd.DataFrame) and not df.empty]
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True)
    return earlier_window_wins_dedupe(merged)
