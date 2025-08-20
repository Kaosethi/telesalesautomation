# telesales/pipeline.py
"""
Dry‑run friendly pipeline (mock data flowing to Sheets).

What it does right now
- Loads config
- Creates Sheets client (dry‑run if creds/IDs missing)
- Loads mock "Hot Lead" rows for PC + Mobile
- Splits Tier A (A-*) vs Non‑A (others)
- Applies Thai drop filters per tier (using this month's Compile; empty in dry‑run)
- Builds Tier A dataframe (Tier‑A headers, no caller assignment)
- Builds Non‑A dataframe (13‑col telesales header, Telesale blank for now)
- Ensures month files/tabs, writes daily tabs, upserts Compile
- Sends Discord notifications (skips if webhook unset)

Next steps later
- Add rules.py (multi‑window merge + re‑query for Non‑A)
- Add assign.py (mix‑aware caller assignment for Non‑A)
"""

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
    WINDOW_HOT,
    is_tier_a,
)
from .utils import today_key, normalize_phone, split_calling_code_th, inactive_days
from .notify import notify_discord
from .loaders import load_candidates_for_window
from . import filters


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
    """
    Map raw rows -> Tier A header schema.
    Columns we fill:
      No., username, Phone Number, Tier, Inactive Duration (Days),
      amount, Ark Gem, Reward, Assign Date
    Notes:
      - amount: unknown from mock → blank
      - Ark Gem: pass-through from ark_gem_col if present
      - Reward: from reward_tier
    """
    if source_rows is None or source_rows.empty:
        return pd.DataFrame(columns=TIER_A_HEADERS)

    # Normalize phones + inactivity days
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

    # Ensure exact column order
    return df[TIER_A_HEADERS]


def _build_non_a_df(source_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Map raw rows -> Non‑A 13‑column telesales schema.
    Columns:
      No., Username, Calling Code, Phone Number, Tier, Inactive Duration (Days),
      Reward Rank, Telesale, Assign Date, Recall Date/Time, Call Status, Answer Status, Result
    Notes:
      - Calling Code / Phone split follows Thai rule (=+66 + local number)
      - Telesale blank for now (assignment comes later)
      - Recall/Call/Answer/Result left blank (to be filled by callers later)
    """
    if source_rows is None or source_rows.empty:
        return pd.DataFrame(columns=NON_A_HEADERS)

    # Normalize phones + split calling code
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
        COL_TELESALE: "",                   # filled after assignment later
        COL_ASSIGN_DATE: today_key(),
        "Recall Date/Time": "",             # left blank for callers
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
    Current skeleton run:
      - Pulls ONLY Hot Lead for PC+Mobile (mock data)
      - Split A-* vs Non‑A
      - Apply filters per tier using this month's Compile (empty in true dry‑run)
      - Tier A = no caller assignment
      - Non‑A = no assignment yet (will add later)
    """
    cfg = load_config()
    sc = SheetsClient(
        service_account_file=cfg.service_account_file,
        output_folder_id=cfg.output_folder_id,
        output_prefix=cfg.output_prefix,
    )

    # Load "Hot Lead" for both sources (mock mode by default)
    pc = load_candidates_for_window(
        SOURCE_PC, WINDOW_HOT, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_pc or cfg.db_webview
    )
    mobile = load_candidates_for_window(
        SOURCE_MOBILE, WINDOW_HOT, use_real_db=cfg.use_real_db, db_url=cfg.db_webview_mobile or cfg.db_webview
    )
    all_rows = pd.concat([pc, mobile], ignore_index=True)

    # Split by tier label (A-* = Tier A)
    a_rows = all_rows[all_rows.get("tier", "").map(is_tier_a)].copy()
    non_a_rows = all_rows[~all_rows.get("tier", "").map(is_tier_a)].copy()

    # Read this month's Compile per tier (empty in dry‑run placeholder)
    info_a = sc.find_or_create_month_file("Tier A")
    compile_a = sc.read_tab_as_df(info_a.spreadsheet_id, "Compile")

    info_n = sc.find_or_create_month_file("Non A")
    compile_n = sc.read_tab_as_df(info_n.spreadsheet_id, "Compile")

    # Central blacklist + redeemed (TODO wire when config sheet/DB is available)
    blacklist_df = pd.DataFrame()
    redeemed = []  # e.g., usernames from Grafana for today

    # ---- Apply Thai drop filters per tier on RAW rows ----
    a_rows_f = filters.apply_filters(
        a_rows,
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
        non_a_rows,
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

    # Build per-tier output DataFrames
    tier_a_df = _build_tier_a_df(a_rows_f, ark_gem_col=cfg.ark_gem_column)
    non_a_df = _build_non_a_df(non_a_rows_f)

    # Write both
    results: Dict[str, TierWriteResult] = {}
    results["Tier A"] = _write_tier(sc, "Tier A", tier_a_df)
    results["Non A"] = _write_tier(sc, "Non A", non_a_df)

    # Notify (safe: skips if webhook missing)
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
