from __future__ import annotations
import os
import pandas as pd
from dataclasses import dataclass
from ..config import load_config
from ..io_gsheets import SheetsClient, SheetsInfo
from ..constants import SOURCE_PC, SOURCE_MOBILE, WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED, COL_ASSIGN_DATE
from .. import rules, filters
from ..utils import today_key
from .build import build_non_a_df
from ..loaders import load_candidates_for_window, set_window_overrides
from ..assign import assign_mix_aware
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
    cols = {str(c).strip().lower(): c for c in df.columns}
    name_col = cols.get("name") or cols.get("caller") or cols.get("telesale") or list(cols.values())[0]
    avail_col = cols.get("available") or "available"

    def _b(v):
        s = str(v).strip().lower()
        return s in {"1", "1.0", "true", "t", "yes", "y"}

    if avail_col not in df.columns:
        return df[name_col].dropna().astype(str).map(str.strip).tolist()

    mask = df[avail_col].map(_b)
    return df.loc[mask, name_col].dropna().astype(str).map(str.strip).tolist()

def _read_mix_weights(sc: SheetsClient, config_sheet_id: str | None) -> dict[str, float]:
    if not config_sheet_id:
        return {}
    df = sc.read_tab_as_df(config_sheet_id, "Config")
    if df is None or df.empty:
        return {}
    cols = {str(c).strip().lower(): c for c in df.columns if isinstance(c, str)}
    sk_col = cols.get("source_key") or cols.get("source") or list(cols.values())[0]
    en_col = cols.get("enabled") or "enabled"
    mw_col = cols.get("mix_weight") or "mix_weight"

    def _b(v): return str(v).strip().lower() in {"1", "true", "yes", "y"}

    if en_col in df.columns:
        df = df[df[en_col].map(_b)]
    if sk_col not in df.columns or mw_col not in df.columns:
        return {}

    ww = pd.to_numeric(df[mw_col], errors="coerce").fillna(0.0)
    df = df.assign(**{mw_col: ww})
    df = df[df[mw_col] > 0]

    weights = {str(r[sk_col]).strip(): float(r[mw_col]) for _, r in df[[sk_col, mw_col]].iterrows()}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}

def run() -> TierWriteResult:
    cfg = load_config()
    sc = SheetsClient(service_account_file=cfg.service_account_file, output_folder_id=cfg.output_folder_id, output_prefix=cfg.output_prefix)

    # Holidays gate (RUN_DATE-aware)
    run_date_env = os.getenv("RUN_DATE")
    holidays_df = sc.read_tab_as_df(cfg.config_sheet_id, "Holidays") if cfg.config_sheet_id else pd.DataFrame()
    if holidays_df is not None and not holidays_df.empty:
        cols = {str(c).strip().lower(): c for c in holidays_df.columns if isinstance(c, str)}
        dcol = cols.get("date") or list(cols.values())[0]
        hcol = cols.get("holiday") or "holiday"
        try:
            import pandas as _pd
            target_day = _pd.to_datetime(run_date_env).date() if run_date_env else _pd.Timestamp.today().date()
            hd = holidays_df.copy()
            hd["_date"] = _pd.to_datetime(hd[dcol], errors="coerce").dt.date
            mask = hd["_date"] == target_day
            if hcol in hd.columns and mask.any():
                val = str(hd.loc[mask, hcol].iloc[0]).strip().lower()
                if val in {"1","true","t","yes","y"}:
                    print("[Non-A] holiday today — skipping")
                    # Early return: no writes, match TierWriteResult signature (include spreadsheet_id)
                    return TierWriteResult("Non A", "", today_key(), 0, "", "")
        except Exception:
            pass

    # Windows overrides (from Config sheet) — must happen before loading pools
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
            # Canonicalization handled inside loaders.set_window_overrides
            overrides[label] = (dmin, dmax_val)
        if overrides:
            set_window_overrides(overrides)

    # Pools
    hot_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_HOT,        cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HOT)
    hot_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HOT,        cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HOT)
    cold_pc = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_COLD,       cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_COLD)
    cold_mob= rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_COLD,       cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_COLD)
    hib_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC,     WINDOW_HIBERNATED, cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HIBERNATED)
    hib_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HIBERNATED, cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HIBERNATED)

    pools = {"Hot Lead": [hot_pc, hot_mob], "Cold": [cold_pc, cold_mob], "Hibernated": [hib_pc, hib_mob]}

    # Build Non-A and filter using source-first selection
    callers = _read_available_callers(sc, cfg.config_sheet_id)
    per_caller = max(1, int(cfg.per_caller_target))
    target_rows_non_a = len(callers) * per_caller if callers else 100000

    # Read month file (for Compile) and central Blacklist
    month_info: SheetsInfo = sc.find_or_create_month_file("Non A")
    compile_df = sc.read_tab_as_df(month_info.spreadsheet_id, "Compile") if month_info.spreadsheet_id else pd.DataFrame()
    blacklist_df = sc.read_tab_as_df(cfg.config_sheet_id, "Blacklist") if cfg.config_sheet_id else pd.DataFrame()

    # Mix weights for selection quotas
    mix_for_selection = _read_mix_weights(sc, cfg.config_sheet_id) or {"cabal_pc_th": 0.5, "cabal_mobile_th": 0.5}

    non_a_raw, _ = rules.requery_non_a_source_first(pools, mix_for_selection, target_rows=target_rows_non_a)
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
    if callers and not non_a_f.empty:
        mix = _read_mix_weights(sc, cfg.config_sheet_id) or {"cabal_pc_th": 0.5, "cabal_mobile_th": 0.5}
        non_a_f = assign_mix_aware(non_a_f, callers=callers, per_caller_target=per_caller, mix_weights=mix)

        # Safety: fill any blank telesale by round-robin to least-loaded callers
        if "telesale" in non_a_f.columns:
            counts = non_a_f[non_a_f["telesale"] != ""]["telesale"].value_counts().to_dict()
            for c in callers:
                counts.setdefault(c, 0)
            blanks_idx = non_a_f.index[non_a_f["telesale"] == ""].tolist()
            i = 0
            while i < len(blanks_idx) and callers:
                # pick least-loaded caller
                pick = min(callers, key=lambda c: counts.get(c, 0))
                idx = blanks_idx[i]
                non_a_f.at[idx, "telesale"] = pick
                counts[pick] = counts.get(pick, 0) + 1
                i += 1

    df = build_non_a_df(non_a_f)
    # Debug: quotas vs picked vs borrowed (approximate borrow = picked - quota if > 0)
    try:
        quotas = rules._hamilton_apportion_local(int(target_rows_non_a), rules._normalize_mix_local(mix_for_selection))
        # Count picked by source_key in the df we are about to write
        picked_by_source = {}
        if "Source" in df.columns:
            src_series = df["Source"].astype(str)
        elif "platform" in df.columns:
            src_series = df["platform"].astype(str)
        elif "source_key" in df.columns:
            src_series = df["source_key"].astype(str)
        else:
            src_series = pd.Series([], dtype=str)
        for k, v in src_series.value_counts().to_dict().items():
            picked_by_source[str(k)] = int(v)
        # Align on known keys
        keys = sorted(set(list(quotas.keys()) + list(picked_by_source.keys())))
        borrowed = {k: max(0, picked_by_source.get(k, 0) - int(quotas.get(k, 0))) for k in keys}
        # Build compact dicts for print
        def _fmt(d):
            return {k: int(d.get(k, 0)) for k in sorted(d.keys())}
        print(f"[Non-A] selection quotas={_fmt(quotas)} picked={_fmt(picked_by_source)} borrowed={_fmt(borrowed)}")
    except Exception:
        pass
    res = _write(sc, "Non A", df)
    if cfg.webhook_non_a:
        notify_discord(cfg.webhook_non_a, tier_label="Non-A", file_name=res.file_name, tab_name=res.tab_name, row_count=res.row_count, sheet_url=res.sheet_url)
    return res
