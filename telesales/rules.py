# telesales/rules.py
"""
Rules for windows, dedupe, and re-query (Non-A only).

What this provides:
- tag_window(df, label): adds a 'window_label' column to a DataFrame
- earlier_window_wins_dedupe(df): keep 1 row per phone; earlier window (Hot < Cold < Hibernated) wins
- requery_non_a(pools_by_window, target_rows): start from Hot; if short, pull Cold then Hibernated
- build_non_a_pool(pools_by_window, target_rows): helper that wraps requery + dedupe
- build_tier_a_pool(pools_by_window): Tier A = Hot only (no re-query, amount >= threshold if available)

Inputs are plain pandas DataFrames produced by loaders. Each df should have:
  - 'username', 'phone', 'source_key' (or 'platform'), 'last_login'/'last_seen', etc.
  - After calling tag_window(...), they will also have 'window_label'

When DB is connected, Tier A = amount >= 100000 (as per SQL). For mock data, no filtering happens.
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
    """Return a copy of df with a 'window_label' column set to label."""
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
    """Deduplicate by phone, earlier windows win (Hot < Cold < Hibernated)."""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["_win_rank"] = tmp.get("window_label", "").map(lambda w: _WINDOW_RANK.get(str(w), 9999))
    tmp.sort_values(by=["_win_rank"], kind="stable", inplace=True)
    deduped = tmp.drop_duplicates(subset=["phone"], keep="first").drop(columns=["_win_rank"])
    return deduped.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Re-query loop (Non-A)
# --------------------------------------------------------------------------- #

def requery_non_a(
    pools_by_window: Dict[str, List[pd.DataFrame]],
    target_rows: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Start with Hot; if short, append Cold then Hibernated."""
    chosen_frames: List[pd.DataFrame] = []
    counts: Dict[str, int] = {}

    for w in WINDOW_PRIORITY:
        for df in pools_by_window.get(w, []):
            if isinstance(df, pd.DataFrame) and not df.empty:
                chosen_frames.append(df)

        merged = pd.concat(chosen_frames, ignore_index=True) if chosen_frames else pd.DataFrame()
        deduped = earlier_window_wins_dedupe(merged)
        counts[w] = len(deduped)

        if len(deduped) >= int(target_rows):
            return deduped.head(int(target_rows)).reset_index(drop=True), counts

    return (earlier_window_wins_dedupe(pd.concat(chosen_frames, ignore_index=True))
            if chosen_frames else pd.DataFrame()), counts


def build_non_a_pool(
    pools_by_window: Dict[str, List[pd.DataFrame]],
    target_rows: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
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
    """Source-first selection: quotas by source, borrow if short."""
    weights = _normalize_mix_local(mix_weights or {})
    quotas = _hamilton_apportion_local(int(target_rows), weights)

    pools: Dict[str, List[pd.DataFrame]] = {w: [df for df in pools_by_window.get(w, []) if isinstance(df, pd.DataFrame)] for w in WINDOW_PRIORITY}

    remaining: Dict[str, Dict[str, pd.DataFrame]] = {}
    sources = list(weights.keys())
    for s in sources:
        remaining[s] = {}
        for w in WINDOW_PRIORITY:
            pool_w = pd.concat(pools.get(w, []), ignore_index=True) if pools.get(w) else pd.DataFrame()
            remaining[s][w] = _filter_by_source(pool_w, s)

    taken_frames: List[pd.DataFrame] = []

    def _drop_taken_from_remaining(phones: pd.Series) -> None:
        ph_set = set(phones.astype(str).tolist())
        for s in sources:
            for w in WINDOW_PRIORITY:
                dfw = remaining[s][w]
                if isinstance(dfw, pd.DataFrame) and not dfw.empty:
                    remaining[s][w] = dfw[~dfw["phone"].astype(str).isin(ph_set)]

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

    combined = pd.concat(taken_frames, ignore_index=True) if taken_frames else pd.DataFrame()
    combined = earlier_window_wins_dedupe(combined) if not combined.empty else combined
    if len(combined) < int(target_rows):
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

    counts: Dict[str, int] = {}
    if isinstance(combined, pd.DataFrame) and not combined.empty:
        for w in WINDOW_PRIORITY:
            counts[w] = int((combined.get("window_label", "") == w).sum())
    else:
        for w in WINDOW_PRIORITY:
            counts[w] = 0

    return combined.reset_index(drop=True), counts


# --------------------------------------------------------------------------- #
# Tier A pool (Hot only, filter by amount if available)
# --------------------------------------------------------------------------- #

def build_tier_a_pool(pools_by_window: Dict[str, List[pd.DataFrame]]) -> pd.DataFrame:
    """Tier A = Hot only; filter by amount >= 100000 if available."""
    hot_frames = pools_by_window.get(WINDOW_HOT, [])
    frames = [df for df in hot_frames if isinstance(df, pd.DataFrame) and not df.empty]
    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)

    # ✅ Apply Tier A filter if amount column exists
    if "amount" in merged.columns:
        try:
            merged = merged[pd.to_numeric(merged["amount"], errors="coerce") >= 100000]
        except Exception:
            pass
    elif "Tier" in merged.columns:
        # Optional: if a pre-computed Tier exists, keep only A-tiers
        merged = merged[merged["Tier"].astype(str).str.startswith("A-")]

    return earlier_window_wins_dedupe(merged)
