from __future__ import annotations
import pandas as pd
from datetime import datetime
from ..constants import (
    TIER_A_HEADERS, COL_USERNAME, COL_PHONE, COL_TIER, COL_INACTIVE_DAYS,
    COL_AMOUNT, COL_ARK_GEM, COL_REWARD, COL_ASSIGN_DATE, COL_SOURCE
)
from ..utils import today_key, normalize_phone, inactive_days

def build_tier_a_df(source_rows: pd.DataFrame, ark_gem_col: str) -> pd.DataFrame:
    if source_rows is None or source_rows.empty:
        return pd.DataFrame(columns=TIER_A_HEADERS)

    phones = source_rows["phone"].map(normalize_phone)
    inact = source_rows.apply(
        lambda r: inactive_days(
            r.get("last_login") if isinstance(r.get("last_login"), datetime) else None,
            r.get("last_seen") if isinstance(r.get("last_seen"), datetime) else None,
        ),
        axis=1,
    )

    data = {
        "No.": list(range(1, len(source_rows) + 1)),
        COL_USERNAME: source_rows["username"].astype(str).tolist(),
        COL_PHONE: phones.tolist(),
        COL_TIER: source_rows.get("tier", "").tolist() if "tier" in source_rows.columns else [""] * len(source_rows),
        COL_INACTIVE_DAYS: inact.tolist(),
        COL_AMOUNT: [""] * len(source_rows),
        COL_ARK_GEM: source_rows.get(ark_gem_col, "").tolist() if ark_gem_col in source_rows.columns else [""] * len(source_rows),
        COL_REWARD: source_rows.get("reward_tier", "").tolist() if "reward_tier" in source_rows.columns else [""] * len(source_rows),
        COL_ASSIGN_DATE: [today_key()] * len(source_rows),
    }
    if COL_SOURCE in TIER_A_HEADERS:
        data[COL_SOURCE] = (
            source_rows["source_key"].astype(str).tolist()
            if "source_key" in source_rows.columns else [""] * len(source_rows)
        )

    # lock exact header order
    df = pd.DataFrame({h: data.get(h, [""] * len(source_rows)) for h in TIER_A_HEADERS})
    return df[TIER_A_HEADERS]
