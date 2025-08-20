# telesales/assign.py
"""
Mix‑aware assignment for Non‑A ONLY.

This module assigns Non‑A *raw rows* (before schema mapping) to available callers,
using per‑source mix weights (e.g., 0.5 PC / 0.5 Mobile) and PER_CALLER_TARGET.

IMPORTANT
- Run this BEFORE building the Non‑A 13‑column output. It expects a 'source_key'
  column on the input rows and will output a new 'telesale' column.
- The pipeline’s _build_non_a_df can then read that 'telesale' column and place
  it into the 'Telesale' output field.

API
- assign_mix_aware(non_a_rows, callers, per_caller_target, mix_weights)

  non_a_rows: pandas.DataFrame with columns at least: ['username','phone','source_key', ...]
  callers: list[str] of caller names (available=TRUE from Config sheet, later)
  per_caller_target: int, e.g., 80
  mix_weights: dict like {'cabal_pc_th': 0.5, 'cabal_mobile_th': 0.5}. Will be normalized.

Returns: a copy of non_a_rows with a new column 'telesale' filled for the first
         N = callers * per_caller_target rows (if enough data). It leaves extra
         rows unassigned (you can drop or keep them depending on policy).
"""

from __future__ import annotations

from typing import Dict, List, Tuple
import math
import pandas as pd

from .constants import SOURCE_PC, SOURCE_MOBILE

def _normalize_mix(mix: Dict[str, float]) -> Dict[str, float]:
    """Ensure weights sum to 1.0; if empty or all zeros, default to 0.5/0.5 for PC/Mobile."""
    # Remove negatives and None
    cleaned = {k: float(v) for k, v in (mix or {}).items() if v is not None and float(v) > 0}
    total = sum(cleaned.values())
    if total <= 0:
        return {SOURCE_PC: 0.5, SOURCE_MOBILE: 0.5}
    return {k: v / total for k, v in cleaned.items()}

def _take_first(df: pd.DataFrame, n: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (head_n, tail_rest) without modifying original index order."""
    n = max(0, int(n))
    if df is None or df.empty or n == 0:
        return (df.head(0).copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(), df.copy())
    head = df.head(n).copy()
    tail = df.iloc[len(head):].copy()
    return head, tail

def assign_mix_aware(
    non_a_rows: pd.DataFrame,
    callers: List[str],
    per_caller_target: int,
    mix_weights: Dict[str, float],
) -> pd.DataFrame:
    """
    Assign Non‑A rows to callers using per‑source mix.

    Steps:
      1) Split the pool by source_key (e.g., PC vs Mobile).
      2) For each caller, compute desired counts per source using mix weights.
      3) Take that many rows from each source bucket (FIFO/stable).
      4) Move to next caller (round‑robin).
      5) If buckets run short, we take whatever remains proportionally.

    Returns a COPY of non_a_rows with new column 'telesale' filled where assigned.
    """
    if not isinstance(non_a_rows, pd.DataFrame) or non_a_rows.empty:
        return pd.DataFrame(columns=list(non_a_rows.columns) + ["telesale"]) if isinstance(non_a_rows, pd.DataFrame) else pd.DataFrame()

    callers = [c for c in callers if str(c).strip()]
    if not callers or per_caller_target <= 0:
        # Nothing to assign
        out = non_a_rows.copy()
        out["telesale"] = ""
        return out

    weights = _normalize_mix(mix_weights)

    # Buckets per source (stable order retained)
    by_src: Dict[str, pd.DataFrame] = {}
    for src in weights.keys():
        by_src[src] = non_a_rows[non_a_rows.get("source_key", "") == src].copy()

    # Any rows from unknown sources are placed in a catch‑all and distributed last
    unknown_df = non_a_rows[~non_a_rows.get("source_key", "").isin(weights.keys())].copy()

    assigned_frames: List[pd.DataFrame] = []
    remaining_by_src: Dict[str, pd.DataFrame] = {k: v.copy() for k, v in by_src.items()}

    for caller in callers:
        # Ideal counts for this caller by source (floor), and keep remainder for later pass
        desired: Dict[str, int] = {}
        remainder_tracker: List[Tuple[str, float]] = []
        for src, w in weights.items():
            ideal = per_caller_target * w
            cnt = math.floor(ideal)
            desired[src] = cnt
            remainder_tracker.append((src, ideal - cnt))

        # Distribute remaining slots (due to floors) by largest remainders first
        remaining_slots = per_caller_target - sum(desired.values())
        if remaining_slots > 0:
            remainder_tracker.sort(key=lambda x: x[1], reverse=True)
            for src, _rem in remainder_tracker:
                if remaining_slots <= 0:
                    break
                desired[src] += 1
                remaining_slots -= 1

        # Pull rows from each source bucket
        taken_parts: List[pd.DataFrame] = []
        for src, need in desired.items():
            head, tail = _take_first(remaining_by_src.get(src, pd.DataFrame()), need)
            if not head.empty:
                head = head.copy()
                head["telesale"] = caller
                taken_parts.append(head)
                remaining_by_src[src] = tail

        # If underfilled (buckets ran short), top up from unknown or any remaining across sources
        got = sum(len(p) for p in taken_parts)
        short = per_caller_target - got
        if short > 0:
            # First, try unknown source bucket
            if not unknown_df.empty:
                add, unknown_df = _take_first(unknown_df, short)
                if not add.empty:
                    add["telesale"] = caller
                    taken_parts.append(add)
                    got += len(add)
                    short = per_caller_target - got

            # Then, pull extra from any sources that still have rows
            if short > 0:
                for src in weights.keys():
                    if short <= 0:
                        break
                    add, tail2 = _take_first(remaining_by_src.get(src, pd.DataFrame()), short)
                    if not add.empty:
                        add["telesale"] = caller
                        taken_parts.append(add)
                        short -= len(add)
                        remaining_by_src[src] = tail2

        if taken_parts:
            assigned_frames.append(pd.concat(taken_parts, ignore_index=True))

    # Any leftovers remain unassigned (telesale empty)
    assigned_df = pd.concat(assigned_frames, ignore_index=True) if assigned_frames else pd.DataFrame()
    out = non_a_rows.copy()
    out["telesale"] = ""
    if not assigned_df.empty:
        # Align columns, then fill telesale where we assigned (by index alignment)
        out.loc[assigned_df.index, "telesale"] = assigned_df["telesale"].values

    return out
