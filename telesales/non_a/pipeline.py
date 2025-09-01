from __future__ import annotations
import os
import pandas as pd
from dataclasses import dataclass
from ..config import load_config
from ..io_gsheets import SheetsClient, SheetsInfo
from ..constants import (
    SOURCE_PC, SOURCE_MOBILE,
    WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED,
    COL_ASSIGN_DATE,
)
from .. import rules, filters
from ..utils import today_key
from .build import build_non_a_df
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


def _read_available_callers(sc: SheetsClient, config_sheet_id: str | None) -> list[str]:
    if not config_sheet_id:
        return []
    df = sc.read_tab_as_df(config_sheet_id, "Callers")
    if df is None or df.empty:
        return []
    df.columns = [str(c).strip().lower() for c in df.columns]
    name_col = "name" if "name" in df.columns else df.columns[0]
    avail_col = "available" if "available" in df.columns else None

    def _b(v):
        try:
            if pd.notna(v) and isinstance(v, (int, float)):
                return float(v) != 0.0
        except Exception:
            pass
        s = str(v).strip().lower()
        return s in {"1", "1.0", "true", "t", "yes", "y"}

    if avail_col and avail_col in df.columns:
        mask = df[avail_col].map(_b)
        return df.loc[mask, name_col].dropna().astype(str).map(str.strip).tolist()
    else:
        return df[name_col].dropna().astype(str).map(str.strip).tolist()


def _read_mix_weights(sc: SheetsClient, config_sheet_id: str | None) -> dict[str, float]:
    if not config_sheet_id:
        return {}
    df = sc.read_tab_as_df(config_sheet_id, "Config")
    if df is None or df.empty:
        return {}
    df.columns = [str(c).strip().lower() for c in df.columns]
    sk_col = "source_key" if "source_key" in df.columns else df.columns[0]
    en_col = "enabled" if "enabled" in df.columns else None
    mw_col = "mix_weight" if "mix_weight" in df.columns else None

    def _b(v):
        try:
            if pd.notna(v) and isinstance(v, (int, float)):
                return float(v) != 0.0
        except Exception:
            pass
        s = str(v).strip().lower()
        return s in {"1", "1.0", "true", "t", "yes", "y"}

    if en_col and en_col in df.columns:
        df = df[df[en_col].map(_b)]
    if not mw_col or mw_col not in df.columns or sk_col not in df.columns:
        return {}
    ww = pd.to_numeric(df[mw_col], errors="coerce").fillna(0.0)
    df[mw_col] = ww
    df = df[df[mw_col] > 0]
    weights = {str(r[sk_col]).strip(): float(r[mw_col]) for _, r in df[[sk_col, mw_col]].iterrows()}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def assign_mix_balanced(df: pd.DataFrame, callers: list[str], per_caller_target: int, mix_weights: dict[str, float]) -> pd.DataFrame:
    out = []
    for source_key, weight in mix_weights.items():
        subset = df[df["source_key"] == source_key].copy()
        if subset.empty:
            continue
        needed = len(subset)
        caller_cycle = (callers * ((needed // len(callers)) + 1))[:needed]
        subset["telesale"] = caller_cycle
        out.append(subset)
    if out:
        return pd.concat(out, ignore_index=True)
    return df


def run() -> TierWriteResult:
    cfg = load_config()
    sc = SheetsClient(
        service_account_file=cfg.service_account_file,
        output_folder_id=cfg.output_folder_id,
        output_prefix=cfg.output_prefix,
    )

    # Holidays/weekend gate
    run_date_env = os.getenv("RUN_DATE")
    holidays_df = sc.read_tab_as_df(cfg.config_sheet_id, "Holidays") if cfg.config_sheet_id else pd.DataFrame()
    try:
        import pandas as _pd
        target_day = (
            _pd.to_datetime(run_date_env, format="%Y-%m-%d", errors="coerce").date()
            if run_date_env else _pd.Timestamp.today().date()
        )
        if target_day.weekday() >= 5:
            print(f"[Non-A] weekend {target_day} — skipping")
            return TierWriteResult("Non A", "", today_key(), 0, "", "")
        if holidays_df is not None and not holidays_df.empty:
            holidays_df.columns = [str(c).strip().lower() for c in holidays_df.columns]
            dcol = "date" if "date" in holidays_df.columns else holidays_df.columns[0]
            hd = holidays_df.copy()
            hd["_date"] = _pd.to_datetime(hd[dcol], format="%Y-%m-%d", errors="coerce").dt.date
            holidays_set = set(hd["_date"].dropna())
            if target_day in holidays_set:
                print(f"[Non-A] holiday {target_day} — skipping")
                return TierWriteResult("Non A", "", today_key(), 0, "", "")
    except Exception as e:
        print(f"[Non-A] holiday check error: {e}")

    # Pools
    hot_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_HOT, cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HOT)
    hot_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HOT, cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HOT)
    cold_pc = rules.tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_COLD, cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_COLD)
    cold_mob= rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_COLD, cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_COLD)
    hib_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_HIBERNATED, cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HIBERNATED)
    hib_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HIBERNATED, cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HIBERNATED)

    pools = {"Hot Lead": [hot_pc, hot_mob], "Cold": [cold_pc, cold_mob], "Hibernated": [hib_pc, hib_mob]}

    print("[Non-A] pool sizes:",
          f"Hot PC={len(hot_pc)}, Hot Mobile={len(hot_mob)}, "
          f"Cold PC={len(cold_pc)}, Cold Mobile={len(cold_mob)}, "
          f"Hibernated PC={len(hib_pc)}, Hibernated Mobile={len(hib_mob)}")

    # Build raw pool
    mix_for_selection = _read_mix_weights(sc, cfg.config_sheet_id) or {"cabal_pc_th": 0.5, "cabal_mobile_th": 0.5}
    non_a_raw, _ = rules.requery_non_a_source_first(pools, mix_for_selection, target_rows=100000)

    # 🚫 Exclude Tier A strictly
    if "tier" in non_a_raw.columns:
        before = len(non_a_raw)
        non_a_raw = non_a_raw[~non_a_raw["tier"].astype(str).str.startswith("A")]
        after = len(non_a_raw)
        print(f"[Non-A] dropped {before - after} Tier-A users from raw pool")

    # Filters
    month_info: SheetsInfo = sc.find_or_create_month_file("Non A")
    compile_df = sc.read_tab_as_df(month_info.spreadsheet_id, "Compile")
    blacklist_df = sc.read_tab_as_df(cfg.config_sheet_id, "Blacklist")
    non_a_f = filters.apply_filters(
        non_a_raw,
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

    # Assignment
    callers = _read_available_callers(sc, cfg.config_sheet_id)
    per_caller = max(1, int(cfg.per_caller_target))
    if callers and not non_a_f.empty:
        mix = _read_mix_weights(sc, cfg.config_sheet_id) or {"cabal_pc_th": 0.5, "cabal_mobile_th": 0.5}
        non_a_f = assign_mix_balanced(non_a_f, callers=callers, per_caller_target=per_caller, mix_weights=mix)

        # 🔍 Debug distribution
        if "telesale" in non_a_f.columns and "source_key" in non_a_f.columns:
            ct = non_a_f.groupby(["telesale", "source_key"]).size().unstack(fill_value=0)
            print("[Non-A] per-caller distribution:\n", ct)

    df = build_non_a_df(non_a_f)

    res = _write(sc, "Non A", df)
    if cfg.webhook_non_a:
        notify_discord(
            cfg.webhook_non_a,
            tier_label="Non-A",
            file_name=res.file_name,
            tab_name=res.tab_name,
            row_count=res.row_count,
            sheet_url=res.sheet_url,
        )
    return res
