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
# Source-first re-query (Non-A)
# --------------------------------------------------------------------------- #

def _normalize_mix_local(weights: Dict[str, float]) -> Dict[str, float]:
    cleaned = {str(k): float(v) for k, v in (weights or {}).items() if v is not None and float(v) > 0}
    s = sum(cleaned.values())
    if s <= 0:
        return cleaned
    return {k: v / s for k, v in cleaned.items()}

def _hamilton_apportion_local(total: int, weights: Dict[str, float]) -> Dict[str, int]:
    if total <= 0 or not weights:
        return {k: 0 for k in weights.keys()}
    base: Dict[str, int] = {}
    rem: List[Tuple[str, float]] = []
    ssum = 0
    for k, w in weights.items():
        ideal = total * w
        b = int(ideal // 1)
        base[k] = b
        ssum += b
        rem.append((k, ideal - b))
    leftover = total - ssum
    rem.sort(key=lambda x: x[1], reverse=True)
    i = 0
    while leftover > 0 and i < len(rem):
        base[rem[i][0]] += 1
        leftover -= 1
        i += 1
    return base

def _filter_by_source(df: pd.DataFrame, source_key: str) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    return df[df.get("source_key", "") == source_key]

def requery_non_a_source_first(
    pools_by_window: Dict[str, List[pd.DataFrame]],
    mix_weights: Dict[str, float],
    target_rows: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Source-first selection:
      1) Compute per-source quotas from normalized weights so sum == target_rows.
      2) For each source independently: pull Hot -> Cold -> Hibernated up to its quota.
      3) Dedupe by phone (earlier window wins) within and across sources.
      4) If total < target_rows, borrow from remaining other-source pools in window order.
    Returns (final_df, debug_counts_by_window) where counts reflect the last deduped tally per window.
    """
    weights = _normalize_mix_local(mix_weights or {})
    quotas = _hamilton_apportion_local(int(target_rows), weights)

    # Prepare per-window lists
    pools: Dict[str, List[pd.DataFrame]] = {w: [df for df in pools_by_window.get(w, []) if isinstance(df, pd.DataFrame)] for w in WINDOW_PRIORITY}

    # Remaining frames per source per window (mutable copies)
    remaining: Dict[str, Dict[str, pd.DataFrame]] = {}
    sources = list(weights.keys())
    for s in sources:
        remaining[s] = {}
        for w in WINDOW_PRIORITY:
            # concat frames for that window then filter by source
            pool_w = pd.concat(pools.get(w, []), ignore_index=True) if pools.get(w) else pd.DataFrame()
            remaining[s][w] = _filter_by_source(pool_w, s)

    taken_frames: List[pd.DataFrame] = []

    # Helper to drop phones from remaining once taken
    def _drop_taken_from_remaining(phones: pd.Series) -> None:
        ph_set = set(phones.astype(str).tolist())
        for s in sources:
            for w in WINDOW_PRIORITY:
                dfw = remaining[s][w]
                if isinstance(dfw, pd.DataFrame) and not dfw.empty:
                    remaining[s][w] = dfw[~dfw["phone"].astype(str).isin(ph_set)]

    # Step 1-2: fill per source in window order
    for s in sources:
        need = int(quotas.get(s, 0))
        if need <= 0:
            continue
        parts: List[pd.DataFrame] = []
        got = 0
        for w in WINDOW_PRIORITY:
            if got >= need:
                break
            dfw = remaining[s][w]
            if dfw is None or dfw.empty:
                continue
            # Merge current parts with new window, dedupe, then take head up to need
            merged = pd.concat(parts + [dfw], ignore_index=True)
            deduped = earlier_window_wins_dedupe(merged)
            take_n = min(need, len(deduped))
            chosen = deduped.head(take_n)
            parts = [chosen]
            got = len(chosen)
        if parts:
            chosen = parts[0]
            taken_frames.append(chosen)
            _drop_taken_from_remaining(chosen["phone"])

    # Step 3-4: Borrow if short
    combined = pd.concat(taken_frames, ignore_index=True) if taken_frames else pd.DataFrame()
    combined = earlier_window_wins_dedupe(combined) if not combined.empty else combined
    if len(combined) < int(target_rows):
        # Build a borrowing pool from all remaining sources in window order
        borrow_frames: List[pd.DataFrame] = []
        for w in WINDOW_PRIORITY:
            for s in sources:
                dfw = remaining[s][w]
                if isinstance(dfw, pd.DataFrame) and not dfw.empty:
                    borrow_frames.append(dfw)
        if borrow_frames:
            bor = pd.concat([combined] + borrow_frames, ignore_index=True)
            bor = earlier_window_wins_dedupe(bor)
            combined = bor.head(int(target_rows))

    # Debug counts by window after final dedupe
    counts: Dict[str, int] = {}
    if isinstance(combined, pd.DataFrame) and not combined.empty:
        for w in WINDOW_PRIORITY:
            counts[w] = int((combined.get("window_label", "") == w).sum())
    else:
        for w in WINDOW_PRIORITY:
            counts[w] = 0

    return combined.reset_index(drop=True), counts


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
