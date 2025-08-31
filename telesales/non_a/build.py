from __future__ import annotations
import pandas as pd
from datetime import datetime
from ..constants import NON_A_HEADERS, COL_USERNAME_OUT, COL_CALLING_CODE, COL_PHONE, COL_TIER, COL_INACTIVE_DAYS, COL_REWARD_RANK, COL_TELESALE, COL_ASSIGN_DATE, COL_SOURCE
from ..utils import today_key, normalize_phone, split_calling_code_th, inactive_days

def build_non_a_df(source_rows: pd.DataFrame) -> pd.DataFrame:
    if source_rows is None or source_rows.empty:
        return pd.DataFrame(columns=NON_A_HEADERS)

    local_digits = source_rows["phone"].map(normalize_phone)
    cc, local = zip(*[split_calling_code_th(p) for p in local_digits])
    inact = source_rows.apply(lambda r: inactive_days(
        r.get("last_login") if isinstance(r.get("last_login"), datetime) else None,
        r.get("last_seen") if isinstance(r.get("last_seen"), datetime) else None), axis=1)

    data = {
        "No.": list(range(1, len(source_rows) + 1)),
        COL_USERNAME_OUT: source_rows["username"].astype(str).tolist(),
        COL_CALLING_CODE: list(cc),
        COL_PHONE: list(local),
        COL_TIER: source_rows.get("tier", [""] * len(source_rows)).tolist() if "tier" in source_rows.columns else [""] * len(source_rows),
        COL_INACTIVE_DAYS: inact.tolist(),
        COL_REWARD_RANK: source_rows.get("reward_tier", "").tolist() if "reward_tier" in source_rows.columns else [""] * len(source_rows),
        COL_TELESALE: source_rows.get("telesale", "").tolist() if "telesale" in source_rows.columns else [""] * len(source_rows),
        COL_ASSIGN_DATE: [today_key()] * len(source_rows),
        "Recall Date/Time": [""] * len(source_rows),
        "Call Status": [""] * len(source_rows),
        "Answer Status": [""] * len(source_rows),
        "Result": [""] * len(source_rows),
    }
    if COL_SOURCE in NON_A_HEADERS:
        data[COL_SOURCE] = source_rows.get("source_key", "").astype(str).tolist() if "source_key" in source_rows.columns else [""] * len(source_rows)

    df = pd.DataFrame({h: data.get(h, [""] * len(source_rows)) for h in NON_A_HEADERS})
    return df[NON_A_HEADERS]
