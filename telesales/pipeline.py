# telesales/pipeline.py
"""
Dry‑run friendly pipeline (mock data flowing to Sheets).

Now with windows + re‑query:
- Loads mock rows for HOT/COLD/HIBERNATED (PC + Mobile)
- Tier A = HOT only
- Non‑A = HOT, if short then add COLD, if still short add HIBERNATED
- Applies Thai drop filters per tier (using this month's Compile; empty in dry‑run)
- Writes daily tabs + upserts Compile; sends Discord (skips if webhook unset)

Next steps:
- Add Callers reader (from Config sheet) and compute target_rows_non_a
  = available_callers * PER_CALLER_TARGET
- Add assign.py for mix‑aware per‑caller distribution (Non‑A only)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
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
from . import filters
from . import rules



@dataclass
class TierWriteResult:
    tier: str             # "Tier A" or "Non A"
    file_name: str        # e.g., "CBTH-Tier A - 08-2025"
    tab_name: str         # e.g., "20-08-2025"
    row_count: int        # number of rows written
    sheet_url: str        # spreadsheet URL (placeholder in dry-run)
    spreadsheet_id: str


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

    df = pd.DataFrame({
        "No.": range(1, len(source_rows) + 1),
        COL_USERNAME: source_rows["username"].astype(str),
        COL_PHONE: phones,
        COL_TIER: source_rows.get("tier", ""),
        COL_INACTIVE_DAYS: inact,
        COL_AMOUNT: "",  # unknown from mocks
        COL_ARK_GEM: source_rows.get(ark_gem_col, ""),
        COL_REWARD: source_rows.get("reward_tier", ""),
        COL_ASSIGN_DATE: today_key(),
    })
    return df[TIER_A_HEADERS]


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

    df = pd.DataFrame({
        "No.": range(1, len(source_rows) + 1),
        COL_USERNAME_OUT: source_rows["username"].astype(str),
        COL_CALLING_CODE: list(cc),
        COL_PHONE: list(local),
        COL_TIER: source_rows.get("tier", ""),
        COL_INACTIVE_DAYS: inact,
        COL_REWARD_RANK: source_rows.get("reward_tier", ""),
        COL_TELESALE: "",                   # will fill after assignment later
        COL_ASSIGN_DATE: today_key(),
        "Recall Date/Time": "",
        "Call Status": "",
        "Answer Status": "",
        "Result": "",
    })
    return df[NON_A_HEADERS]


# ----------------------------- core write ops ---------------------------------

def _write_tier(sc: SheetsClient, tier_label: str, df: pd.DataFrame) -> TierWriteResult:
    info: SheetsInfo = sc.find_or_create_month_file(tier_label)
    day_tab = today_key()

    sc.ensure_tabs(info.spreadsheet_id, ["Compile", day_tab])
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


# ----------------------------- public runs ------------------------------------

def run_mock_hot_only() -> Dict[str, TierWriteResult]:
    """
    Mock daily run:
      - Load HOT/COLD/HIBERNATED for PC & Mobile (mock mode)
      - Tier A = build_tier_a_pool (HOT only)
      - Non‑A = build_non_a_pool (re‑query windows toward target)
      - Apply Thai filters per tier
      - Write daily tabs + upsert Compile
    """
    cfg = load_config()
    sc = SheetsClient(
        service_account_file=cfg.service_account_file,
        output_folder_id=cfg.output_folder_id,
        output_prefix=cfg.output_prefix,
    )

    # ---- Load per-window, per-source (mock) ----
    hot_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_HOT, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_pc or cfg.db_webview), WINDOW_HOT)
    hot_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HOT, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_mobile or cfg.db_webview), WINDOW_HOT)

    cold_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_COLD, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_pc or cfg.db_webview), WINDOW_COLD)
    cold_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_COLD, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_mobile or cfg.db_webview), WINDOW_COLD)

    hib_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_HIBERNATED, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_pc or cfg.db_webview), WINDOW_HIBERNATED)
    hib_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HIBERNATED, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_mobile or cfg.db_webview), WINDOW_HIBERNATED)

    pools = {
        WINDOW_HOT:        [hot_pc, hot_mob],
        WINDOW_COLD:       [cold_pc, cold_mob],
        WINDOW_HIBERNATED: [hib_pc, hib_mob],
    }

    # ---- Build Tier A (HOT only) & Non‑A (re‑query) ----
    # TODO (soon): available_callers = count from Callers tab (available=TRUE)
    # target_rows_non_a = available_callers * cfg.per_caller_target
    target_rows_non_a = 120  # demo target until Callers reader is added

    a_rows_raw = rules.build_tier_a_pool(pools)                       # HOT only
    non_a_rows_raw, _dbg = rules.build_non_a_pool(pools, target_rows=target_rows_non_a)

    # ---- Read this month's Compile (empty in dry‑run) ----
    info_a = sc.find_or_create_month_file("Tier A")
    compile_a = sc.read_tab_as_df(info_a.spreadsheet_id, "Compile")

    info_n = sc.find_or_create_month_file("Non A")
    compile_n = sc.read_tab_as_df(info_n.spreadsheet_id, "Compile")

    # Central blacklist + redeemed (TODO: wire real sources later)
    blacklist_df = pd.DataFrame()
    redeemed = []

    # ---- Apply Thai drop filters on RAW rows ----
    a_rows_f = filters.apply_filters(
        a_rows_raw,
        compile_df=compile_a,
        blacklist_df=blacklist_df,
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
        non_a_rows_raw,
        compile_df=compile_n,
        blacklist_df=blacklist_df,
        redeemed_usernames_today=redeemed,
        drop_unreachable_repeat=cfg.drop_unreachable_repeat,
        unreachable_min_count=cfg.unreachable_min_count,
        drop_answered_this_month=cfg.drop_answered_this_month,
        drop_invalid_number=cfg.drop_invalid_number,
        drop_not_interested_this_month=cfg.drop_not_interested_this_month,
        drop_not_owner_as_blacklist=cfg.drop_not_owner_as_blacklist,
        drop_redeemed_today=cfg.drop_redeemed_today,
    )

    # ---- Map to output schemas ----
    tier_a_df = _build_tier_a_df(a_rows_f, ark_gem_col=cfg.ark_gem_column)
    non_a_df  = _build_non_a_df(non_a_rows_f)

    # ---- Write to Sheets (still dry‑run if creds missing) ----
    results: Dict[str, TierWriteResult] = {}
    results["Tier A"] = _write_tier(sc, "Tier A", tier_a_df)
    results["Non A"] = _write_tier(sc, "Non A", non_a_df)

    # ---- Notify (skips if webhook missing) ----
    a = results["Tier A"]
    notify_discord(
        cfg.webhook_a,
        tier_label="Tier A",
        file_name=a.file_name,
        tab_name=a.tab_name,
        row_count=a.row_count,
        sheet_url=a.sheet_url,
    )

    n = results["Non A"]
    notify_discord(
        cfg.webhook_non_a,
        tier_label="Non-A",
        file_name=n.file_name,
        tab_name=n.tab_name,
        row_count=n.row_count,
        sheet_url=n.sheet_url,
    )

    return results
