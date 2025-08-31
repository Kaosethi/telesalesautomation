from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from ..config import load_config
from ..io_gsheets import SheetsClient, SheetsInfo
from ..constants import SOURCE_PC, SOURCE_MOBILE, WINDOW_HOT, WINDOW_COLD, WINDOW_HIBERNATED, NON_A_HEADERS, COL_ASSIGN_DATE
from .. import rules, filters
from ..utils import today_key
from .build import build_non_a_df
from ..loaders import load_candidates_for_window
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
    if not config_sheet_id: return []
    df = sc.read_tab_as_df(config_sheet_id, "Callers")
    if df is None or df.empty: return []
    cols = {str(c).strip().lower(): c for c in df.columns}
    name_col = cols.get("name") or cols.get("caller") or cols.get("telesale") or list(cols.values())[0]
    avail_col = cols.get("available") or "available"
    def _b(v):
        s = str(v).strip().lower()
        return s in {"1","1.0","true","t","yes","y"}
    if avail_col not in df.columns:
        return df[name_col].dropna().astype(str).map(str.strip).tolist()
    mask = df[avail_col].map(_b)
    return df.loc[mask, name_col].dropna().astype(str).map(str.strip).tolist()

def _read_mix_weights(sc: SheetsClient, config_sheet_id: str | None) -> dict[str,float]:
    if not config_sheet_id: return {}
    df = sc.read_tab_as_df(config_sheet_id, "Config")
    if df is None or df.empty: return {}
    cols = {str(c).strip().lower(): c for c in df.columns if isinstance(c, str)}
    sk = cols.get("source_key") or cols.get("source") or list(cols.values())[0]
    en = cols.get("enabled") or "enabled"
    mw = cols.get("mix_weight") or "mix_weight"
    def _b(v): return str(v).strip().lower() in {"1","true","yes","y"}
    if en in df.columns:
        df = df[df[en].map(_b)]
    if sk not in df.columns or mw not in df.columns: return {}
    w = pd.to_numeric(df[mw], errors="coerce").fillna(0.0)
    df = df.assign(**{mw: w})
    df = df[df[mw] > 0]
    weights = {str(r[sk]).strip(): float(r[mw]) for _, r in df[[sk, mw]].iterrows()}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}

def run() -> TierWriteResult:
    cfg = load_config()
    sc = SheetsClient(service_account_file=cfg.service_account_file, output_folder_id=cfg.output_folder_id, output_prefix=cfg.output_prefix)

    # Load all pools
    hot_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_HOT,        cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HOT)
    hot_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HOT,   cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HOT)
    cold_pc = rules.tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_COLD,      cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_COLD)
    cold_mob= rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_COLD,  cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_COLD)
    hib_pc  = rules.tag_window(load_candidates_for_window(SOURCE_PC, WINDOW_HIBERNATED,cfg.use_real_db, cfg.db_webview_pc or cfg.db_webview), WINDOW_HIBERNATED)
    hib_mob = rules.tag_window(load_candidates_for_window(SOURCE_MOBILE, WINDOW_HIBERNATED,cfg.use_real_db, cfg.db_webview_mobile or cfg.db_webview), WINDOW_HIBERNATED)

    pools = { "Hot Lead":[hot_pc, hot_mob], "Cold":[cold_pc, cold_mob], "Hibernated":[hib_pc, hib_mob] }

    # Build Non-A raw then filter out A-tiers
    target_rows_non_a = 100000
    callers = _read_available_callers(sc, cfg.config_sheet_id)
    if callers:
        target_rows_non_a = len(callers) * max(1, int(cfg.per_caller_target))

    non_a_raw, _ = rules.build_non_a_pool(pools, target_rows=target_rows_non_a)
    non_a_f = filters.apply_filters(
        non_a_raw, compile_df=None, blacklist_df=pd.DataFrame(), redeemed_usernames_today=[],
        drop_unreachable_repeat=cfg.drop_unreachable_repeat, unreachable_min_count=cfg.unreachable_min_count,
        drop_answered_this_month=cfg.drop_answered_this_month, drop_invalid_number=cfg.drop_invalid_number,
        drop_not_interested_this_month=cfg.drop_not_interested_this_month, drop_not_owner_as_blacklist=cfg.drop_not_owner_as_blacklist,
        drop_redeemed_today=cfg.drop_redeemed_today,
    )

    # Assignment with mix
    if callers and not non_a_f.empty:
        mix = _read_mix_weights(sc, cfg.config_sheet_id) or {"cabal_pc_th": 0.5, "cabal_mobile_th": 0.5}
        non_a_f = assign_mix_aware(non_a_f, callers=callers, per_caller_target=int(cfg.per_caller_target), mix_weights=mix)

    df = build_non_a_df(non_a_f)
    res = _write(sc, "Non A", df)
    if cfg.webhook_non_a:
        notify_discord(cfg.webhook_non_a, tier_label="Non-A", file_name=res.file_name, tab_name=res.tab_name, row_count=res.row_count, sheet_url=res.sheet_url)
    return res
