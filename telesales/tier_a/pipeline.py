from __future__ import annotations
import os
import pandas as pd
from dataclasses import dataclass
from ..config import load_config
from ..io_gsheets import SheetsClient, SheetsInfo
from ..constants import SOURCE_PC, SOURCE_MOBILE, WINDOW_HOT, COL_ASSIGN_DATE
from .. import rules, filters
from ..utils import today_key
from .build import build_tier_a_df
from ..loaders import load_candidates_for_window, set_window_overrides
from ..notify import notify_discord

@dataclass
class TierWriteResult:
    tier: str
    file_name: str
    tab_name: str
    row_count: int
    sheet_url: str
    spreadsheet_id: str

def _write(sc: SheetsClient, label: str, df: pd.DataFrame) -> TierWriteResult:
    info: SheetsInfo = sc.find_or_create_month_file(label)
    day_tab = today_key()
    sc.ensure_tabs(info.spreadsheet_id, ["Compile", day_tab])
    sc.write_df_to_tab(info.spreadsheet_id, day_tab, df)
    sc.upsert_compile(info.spreadsheet_id, df, assign_date_col=COL_ASSIGN_DATE)
    return TierWriteResult(label, info.title, day_tab, len(df), info.spreadsheet_url, info.spreadsheet_id)

def run() -> TierWriteResult:
    cfg = load_config()
    sc = SheetsClient(service_account_file=cfg.service_account_file, output_folder_id=cfg.output_folder_id, output_prefix=cfg.output_prefix)

    # Holidays/weekend gate (RUN_DATE-aware)
    run_date_env = os.getenv("RUN_DATE")
    holidays_df = sc.read_tab_as_df(cfg.config_sheet_id, "Holidays") if cfg.config_sheet_id else pd.DataFrame()
    try:
        import pandas as _pd
        target_day = _pd.to_datetime(run_date_env).date() if run_date_env else _pd.Timestamp.today().date()
        # Weekend skip (Sat=5, Sun=6)
        if target_day.weekday() >= 5:
            print(f"[Tier A] weekend {target_day} — skipping")
            return TierWriteResult("Tier A", "", today_key(), 0, "", "")
        # Holidays sheet: presence of date implies holiday (no boolean needed)
        if holidays_df is not None and not holidays_df.empty:
            cols = {str(c).strip().lower(): c for c in holidays_df.columns if isinstance(c, str)}
            dcol = cols.get("date") or list(cols.values())[0]
            hd = holidays_df.copy()
            hd["_date"] = _pd.to_datetime(hd[dcol], errors="coerce", dayfirst=True).dt.date
            if (hd["_date"] == target_day).any():
                print("[Tier A] holiday today — skipping")
                return TierWriteResult("Tier A", "", today_key(), 0, "", "")
    except Exception:
        pass

    # Windows overrides before pool loading
    win_df = sc.read_tab_as_df(cfg.config_sheet_id, "Windows") if cfg.config_sheet_id else pd.DataFrame()
    if win_df is not None and not win_df.empty:
        cols = {str(c).strip().lower(): c for c in win_df.columns if isinstance(c, str)}
        lcol = cols.get("label") or list(cols.values())[0]
        minc = cols.get("day_min") or "day_min"
        maxc = cols.get("day_max") or "day_max"
        overrides = {}
        for _, r in win_df.iterrows():
            label = str(r.get(lcol, "")).strip()
            if not label:
                continue
            dmin = pd.to_numeric(r.get(minc, None), errors="coerce")
            dmax = pd.to_numeric(r.get(maxc, None), errors="coerce")
            dmin = int(dmin) if pd.notna(dmin) else 0
            dmax_val = int(dmax) if pd.notna(dmax) else None
            overrides[label] = (dmin, dmax_val)
        if overrides:
            set_window_overrides(overrides)

    # HOT only; then A-tier filter happens inside rules.build_tier_a_pool
    hot_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_HOT, cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HOT)
    hot_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HOT, cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HOT)
    a_rows_raw = rules.build_tier_a_pool({WINDOW_HOT: [hot_pc, hot_mob]})

    # Read month file (for Compile) and central Blacklist
    month_info: SheetsInfo = sc.find_or_create_month_file("Tier A")
    compile_df = sc.read_tab_as_df(month_info.spreadsheet_id, "Compile") if month_info.spreadsheet_id else pd.DataFrame()
    blacklist_df = sc.read_tab_as_df(cfg.config_sheet_id, "Blacklist") if cfg.config_sheet_id else pd.DataFrame()

    a_rows_f = filters.apply_filters(
        a_rows_raw,
        compile_df=compile_df,
        blacklist_df=blacklist_df,
        redeemed_usernames_today=[],
        drop_unreachable_repeat=cfg.drop_unreachable_repeat,
        unreachable_min_count=cfg.unreachable_min_count,
        drop_answered_this_month=cfg.drop_answered_this_month,
        drop_invalid_number=cfg.drop_invalid_number,
        drop_not_interested_this_month=cfg.drop_not_interested_this_month,
        drop_not_owner_as_blacklist=cfg.drop_not_owner_as_blacklist,
        drop_redeemed_today=cfg.drop_redeemed_today,
    )

    df = build_tier_a_df(a_rows_f, ark_gem_col=cfg.ark_gem_column)
    res = _write(sc, "Tier A", df)
    if cfg.webhook_a:
        notify_discord(cfg.webhook_a, tier_label="Tier A", file_name=res.file_name, tab_name=res.tab_name, row_count=res.row_count, sheet_url=res.sheet_url)
    return res
