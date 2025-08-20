# telesales/assign.py
"""
Mix‑aware assignment for Non‑A ONLY (strict, even distribution).

What this guarantees:
- Every available caller gets the SAME number of rows (no 17/16 skew).
- Per‑caller mix is enforced via Hamilton apportionment (e.g., target=16 at 50/50 -> 8 PC, 8 Mobile),
  with smart top‑ups only if a source bucket is short.

Leftovers (when total rows % callers != 0) are intentionally left unassigned,
so counts remain perfectly equal, as requested.

Inputs:
  non_a_rows: DataFrame with at least ['username','phone','source_key', ...]
  callers: list[str]
  per_caller_target: int (upper bound before feasibility checks)
  mix_weights: dict like {'cabal_pc_th': 0.5, 'cabal_mobile_th': 0.5}

Output:
  Returns a COPY of non_a_rows with a new column 'telesale' filled on assigned rows.
  Unassigned rows keep telesale="" so the pipeline can decide whether to drop them.
"""

from __future__ import annotations

from typing import Dict, List, Tuple
import math
import pandas as pd

from .constants import SOURCE_PC, SOURCE_MOBILE


def _normalize_mix(mix: Dict[str, float]) -> Dict[str, float]:
    """Normalize positive weights to sum=1.0; default to 0.5/0.5 if empty."""
    cleaned = {k: float(v) for k, v in (mix or {}).items()
               if v is not None and float(v) > 0}
    total = sum(cleaned.values())
    if total <= 0:
        return {SOURCE_PC: 0.5, SOURCE_MOBILE: 0.5}
    return {k: v / total for k, v in cleaned.items()}


def _hamilton_apportion(total: int, weights: Dict[str, float]) -> Dict[str, int]:
    """
    Hamilton (largest remainder) apportionment:
    - base = floor(total * w_s) for each source s
    - distribute remaining seats to largest remainders
    """
    if total <= 0:
        return {k: 0 for k in weights.keys()}
    base = {}
    remainders = []
    ssum = 0
    for k, w in weights.items():
        ideal = total * w
        b = math.floor(ideal)
        base[k] = b
        ssum += b
        remainders.append((k, ideal - b))
    leftover = total - ssum
    remainders.sort(key=lambda x: x[1], reverse=True)
    i = 0
    while leftover > 0 and i < len(remainders):
        base[remainders[i][0]] += 1
        leftover -= 1
        i += 1
    return base


def _take_head(df: pd.DataFrame, n: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (head, tail) without altering original index values."""
    n = max(0, int(n))
    if not isinstance(df, pd.DataFrame) or df.empty or n == 0:
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
    if not isinstance(non_a_rows, pd.DataFrame) or non_a_rows.empty:
        return pd.DataFrame(columns=list(getattr(non_a_rows, "columns", [])) + ["telesale"])

    callers = [c for c in (callers or []) if str(c).strip()]
    if not callers:
        out = non_a_rows.copy()
        out["telesale"] = ""
        return out

    # Buckets per source (stable order preserved via filtering)
    weights = _normalize_mix(mix_weights)
    by_src: Dict[str, pd.DataFrame] = {
        s: non_a_rows[non_a_rows.get("source_key", "") == s].copy()
        for s in weights.keys()
    }
    # Unknown sources go to a top‑up bucket
    unknown_df = non_a_rows[~non_a_rows.get("source_key", "").isin(weights.keys())].copy()

    total_available = sum(len(df) for df in by_src.values()) + len(unknown_df)
    k = len(callers)

    # Make counts perfectly equal: cap target to floor(total / k)
    target = min(max(0, int(per_caller_target)), total_available // k)

    # If target is 0, nothing to assign (better equal than skewed)
    if target <= 0:
        out = non_a_rows.copy()
        out["telesale"] = ""
        return out

    # For each caller, compute per‑source quotas using Hamilton apportionment.
    # This enforces the per‑caller mix (e.g., target=16, 50/50 => 8+8).
    assigned_frames: List[pd.DataFrame] = []
    remaining: Dict[str, pd.DataFrame] = {k: v.copy() for k, v in by_src.items()}

    for caller in callers:
        quotas = _hamilton_apportion(target, weights)

        taken_parts: List[pd.DataFrame] = []
        # First pass: pull exactly the quota from each source (or as many as available)
        got = 0
        for src, need in quotas.items():
            if need <= 0:
                continue
            head, tail = _take_head(remaining.get(src, pd.DataFrame()), need)
            if not head.empty:
                head["telesale"] = caller
                taken_parts.append(head)
                got += len(head)
                remaining[src] = tail

        # If underfilled due to source shortage, top‑up from other sources proportionally
        short = target - got
        if short > 0:
            # Try other known sources with remaining rows
            # Prefer sources with most remaining rows to minimize skew.
            sources_by_left = sorted(
                [(s, len(df)) for s, df in remaining.items() if len(df) > 0],
                key=lambda x: x[1],
                reverse=True,
            )
            for src, _left in sources_by_left:
                if short <= 0:
                    break
                add, tail2 = _take_head(remaining[src], short)
                if not add.empty:
                    add["telesale"] = caller
                    taken_parts.append(add)
                    short -= len(add)
                    remaining[src] = tail2

        # Still short? Pull from unknown bucket, if any.
        if short > 0 and not unknown_df.empty:
            add, unknown_df = _take_head(unknown_df, short)
            if not add.empty:
                add["telesale"] = caller
                taken_parts.append(add)
                short -= len(add)

        # At this point, caller either has exactly `target` rows, or less if global capacity isn’t enough.
        # But because we capped `target` to floor(total/k), we should hit exact targets for all callers.
        if taken_parts:
            assigned_frames.append(pd.concat(taken_parts, ignore_index=False))

    # Build output: fill telesale for assigned indices; leave others empty
    out = non_a_rows.copy()
    out["telesale"] = ""
    if assigned_frames:
        assigned = pd.concat(assigned_frames, ignore_index=False)
        # Align back to original indices
        out.loc[assigned.index, "telesale"] = assigned["telesale"].values

    return out
