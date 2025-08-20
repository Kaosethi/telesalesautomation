# telesales/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
import pandas as pd
from datetime import datetime

from .config import load_config
from .io_gsheets import SheetsClient, SheetsInfo
from .constants import (
    SOURCE_PC, SOURCE_MOBILE,
    TIER_A_HEADERS, NON_A_HEADERS,
    COL_ASSIGN_DATE, COL_USERNAME, COL_USERNAME_OUT,
    COL_PHONE, COL_CALLING_CODE, COL_TIER,
    COL_INACTIVE_DAYS, COL_REWARD_RANK, COL_TELESALE,
    COL_AMOUNT, COL_ARK_GEM, COL_REWARD,
    WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED,
    is_tier_a,
)
from .utils import today_key, normalize_phone, split_calling_code_th, inactive_days
from .notify import notify_discord
from .loaders import load_candidates_for_window
from . import filters, rules
from .assign import assign_mix_aware


@dataclass
class TierWriteResult:
    tier: str
    file_name: str
    tab_name: str
    row_count: int
    sheet_url: str
    spreadsheet_id: str


# ----------------------------- helpers ----------------------------------------

def _finalize_to_headers(data: Dict[str, List], headers: List[str]) -> pd.DataFrame:
    """Build df with exact header order; fill/broadcast safely."""
    n_rows = 0
    for v in data.values():
        if isinstance(v, list):
            n_rows = max(n_rows, len(v))

    def _as_list(val, target_len: int):
        if isinstance(val, list):
            if len(val) == target_len:
                return val
            if len(val) > target_len:
                return val[:target_len]
            return val + [""] * (target_len - len(val))
        return [val] * target_len

    fixed: Dict[str, List] = {}
    for h in headers:
        if h in data:
            fixed[h] = _as_list(data[h], n_rows)
        else:
            fixed[h] = [""] * n_rows

    df = pd.DataFrame(fixed)
    return df[headers]


# ----------------------------- build dataframes -------------------------------

def _build_tier_a_df(source_rows: pd.DataFrame, ark_gem_col: str) -> pd.DataFrame:
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
    if "Platform" in TIER_A_HEADERS:
        data["Platform"] = source_rows.get("source_key", "").tolist() if "source_key" in source_rows.columns else [""] * len(source_rows)

    return _finalize_to_headers(data, TIER_A_HEADERS)


def _build_non_a_df(source_rows: pd.DataFrame) -> pd.DataFrame:
    if source_rows is None or source_rows.empty:
        return pd.DataFrame(columns=NON_A_HEADERS)

    local_digits = source_rows["phone"].map(normalize_phone)
    cc, local = zip(*[split_calling_code_th(p) for p in local_digits])

    inact = source_rows.apply(
        lambda r: inactive_days(
            r.get("last_login") if isinstance(r.get("last_login"), datetime) else None,
            r.get("last_seen") if isinstance(r.get("last_seen"), datetime) else None,
        ),
        axis=1,
    )

    data = {
        "No.": list(range(1, len(source_rows) + 1)),
        COL_USERNAME_OUT: source_rows["username"].astype(str).tolist(),
        COL_CALLING_CODE: list(cc),
        COL_PHONE: list(local),
        COL_TIER: source_rows.get("tier", "").tolist() if "tier" in source_rows.columns else [""] * len(source_rows),
        COL_INACTIVE_DAYS: inact.tolist(),
        COL_REWARD_RANK: source_rows.get("reward_tier", "").tolist() if "reward_tier" in source_rows.columns else [""] * len(source_rows),
        COL_TELESALE: source_rows.get("telesale", "").tolist() if "telesale" in source_rows.columns else [""] * len(source_rows),
        COL_ASSIGN_DATE: [today_key()] * len(source_rows),
        "Recall Date/Time": [""] * len(source_rows),
        "Call Status": [""] * len(source_rows),
        "Answer Status": [""] * len(source_rows),
        "Result": [""] * len(source_rows),
    }
    if "Platform" in NON_A_HEADERS:
        data["Platform"] = source_rows.get("source_key", "").tolist() if "source_key" in source_rows.columns else [""] * len(source_rows)

    return _finalize_to_headers(data, NON_A_HEADERS)


# ----------------------------- core write ops ---------------------------------

def _write_tier(sc: SheetsClient, tier_label: str, df: pd.DataFrame) -> TierWriteResult:
    info: SheetsInfo = sc.find_or_create_month_file(tier_label)
    day_tab = today_key()
    sc.ensure_tabs(info.spreadsheet_id, ["Compile", day_tab])  # Compile first in list
    sc.write_df_to_tab(info.spreadsheet_id, day_tab, df)
    sc.upsert_compile(info.spreadsheet_id, df, assign_date_col=COL_ASSIGN_DATE)
    return TierWriteResult(
        tier=tier_label,
        file_name=info.title,
        tab_name=day_tab,
        row_count=len(df),
        sheet_url=info.spreadsheet_url,
        spreadsheet_id=info.spreadsheet_id,
    )


def _read_available_callers(sc: SheetsClient, config_sheet_id: str | None) -> List[str]:
    if not config_sheet_id:
        return []

    df = sc.read_tab_as_df(config_sheet_id, "Callers")
    if df is None or df.empty:
        print("[callers] Callers tab empty or missing → no assignment")
        return []

    # Map headers case-insensitively
    cols = {str(c).strip().lower(): c for c in df.columns}
    name_col = cols.get("name") or cols.get("caller") or cols.get("telesale") or list(cols.values())[0]
    avail_col = cols.get("available") or "available"

    def _to_bool(v):
        # Accept numbers (1, 1.0) and common truthy strings
        if isinstance(v, (int, float)):
            try:
                return float(v) != 0.0
            except Exception:
                return False
        s = str(v).strip().lower()
        return s in {"1", "1.0", "true", "t", "yes", "y"}

    if avail_col not in df.columns:
        # No availability column: treat all names as available
        names = df[name_col].dropna().astype(str).map(str.strip).tolist()
        callers = [n for n in names if n]
        print(f"[callers] No 'Available' column; using all: {callers}")
        return callers

    mask = df[avail_col].map(_to_bool)
    names = df.loc[mask, name_col].dropna().astype(str).map(str.strip).tolist()
    callers = [n for n in names if n]
    print(f"[callers] Available callers: {callers}")
    return callers



# ----------------------------- public run -------------------------------------

def run_mock_hot_only() -> Dict[str, TierWriteResult]:
    cfg = load_config()
    sc = SheetsClient(
        service_account_file=cfg.service_account_file,
        output_folder_id=cfg.output_folder_id,
        output_prefix=cfg.output_prefix,
    )

    # Load per-window, per-source (mock)
    hot_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_HOT,        cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HOT)
    hot_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HOT,        cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HOT)
    cold_pc = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_COLD,       cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_COLD)
    cold_mob= rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_COLD,       cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_COLD)
    hib_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_HIBERNATED, cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HIBERNATED)
    hib_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HIBERNATED, cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HIBERNATED)

    pools = {
        WINDOW_HOT:        [hot_pc, hot_mob],
        WINDOW_COLD:       [cold_pc, cold_mob],
        WINDOW_HIBERNATED: [hib_pc, hib_mob],
    }

    # Tier A = HOT only then keep only A-*
    a_rows_raw = rules.build_tier_a_pool(pools)
    if not a_rows_raw.empty:
        a_rows_raw = a_rows_raw[a_rows_raw.get("tier", "").map(is_tier_a)]

    # Non‑A = re‑query then keep only non‑A
    callers = _read_available_callers(sc, cfg.config_sheet_id)
    per_caller = max(1, int(cfg.per_caller_target))
    target_rows_non_a = (len(callers) * per_caller) if callers else 100000
    non_a_rows_raw, _ = rules.build_non_a_pool(pools, target_rows=target_rows_non_a)
    if not non_a_rows_raw.empty:
        non_a_rows_raw = non_a_rows_raw[~non_a_rows_raw.get("tier", "").map(is_tier_a)]

    # Read Compile tabs (may be empty)
    info_a = sc.find_or_create_month_file("Tier A")
    compile_a = sc.read_tab_as_df(info_a.spreadsheet_id, "Compile")
    info_n = sc.find_or_create_month_file("Non A")
    compile_n = sc.read_tab_as_df(info_n.spreadsheet_id, "Compile")

    blacklist_df = pd.DataFrame()
    redeemed: List[str] = []

    # Filters
    a_rows_f = filters.apply_filters(
        a_rows_raw, compile_df=compile_a, blacklist_df=blacklist_df,
        redeemed_usernames_today=redeemed,
        drop_unreachable_repeat=cfg.drop_unreachable_repeat,
        unreachable_min_count=cfg.unreachable_min_count,
        drop_answered_this_month=cfg.drop_answered_this_month,
        drop_invalid_number=cfg.drop_invalid_number,
        drop_not_interested_this_month=cfg.drop_not_interested_this_month,
        drop_not_owner_as_blacklist=cfg.drop_not_owner_as_blacklist,
        drop_redeemed_today=cfg.drop_redeemed_today,
    )
    non_a_rows_f = filters.apply_filters(
        non_a_rows_raw, compile_df=compile_n, blacklist_df=blacklist_df,
        redeemed_usernames_today=redeemed,
        drop_unreachable_repeat=cfg.drop_unreachable_repeat,
        unreachable_min_count=cfg.unreachable_min_count,
        drop_answered_this_month=cfg.drop_answered_this_month,
        drop_invalid_number=cfg.drop_invalid_number,
        drop_not_interested_this_month=cfg.drop_not_interested_this_month,
        drop_not_owner_as_blacklist=cfg.drop_not_owner_as_blacklist,
        drop_redeemed_today=cfg.drop_redeemed_today,
    )

    # Assignment (Non‑A only)
    if callers and not non_a_rows_f.empty:
        mix = {"cabal_pc_th": 0.5, "cabal_mobile_th": 0.5}  # TODO: read from Config tab later
        non_a_rows_f = assign_mix_aware(
            non_a_rows_f,
            callers=callers,
            per_caller_target=per_caller,
            mix_weights=mix,
        )

    # Map to output schemas
    tier_a_df = _build_tier_a_df(a_rows_f, ark_gem_col=cfg.ark_gem_column)
    non_a_df  = _build_non_a_df(non_a_rows_f)

    # Write
    results: Dict[str, TierWriteResult] = {}
    results["Tier A"] = _write_tier(sc, "Tier A", tier_a_df)
    results["Non A"] = _write_tier(sc, "Non A", non_a_df)

    # Notify (only if webhook is set)
    a = results["Tier A"]
    if cfg.webhook_a:
        notify_discord(
            cfg.webhook_a,
            tier_label="Tier A",
            file_name=a.file_name,
            tab_name=a.tab_name,
            row_count=a.row_count,
            sheet_url=a.sheet_url,
        )

    n = results["Non A"]
    if cfg.webhook_non_a:
        notify_discord(
            cfg.webhook_non_a,
            tier_label="Non-A",
            file_name=n.file_name,
            tab_name=n.tab_name,
            row_count=n.row_count,
            sheet_url=n.sheet_url,
        )

    return results
