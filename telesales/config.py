# telesales/config.py
"""
Loads environment variables and exposes a typed Config object.

- Safe parsing for bools/ints (with defaults)
- Gentle hints if critical keys are missing (no crashes)
- Timezone normalization, integer clamping, absolute path helper
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
import os
from pathlib import Path
from dotenv import load_dotenv


# ------------------------- parsing helpers ------------------------------------

_TRUE_SET = {"1", "true", "yes", "y", "on"}
def _as_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in _TRUE_SET

def _as_int(val: Optional[str], default: int) -> int:
    try:
        return int(str(val).strip())
    except (TypeError, ValueError):
        return default

def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def _norm_tz(val: Optional[str]) -> str:
    # Keep it simple; your app uses Asia/Bangkok by default
    return (val or "Asia/Bangkok").strip()

def _abs_path(p: Optional[str]) -> Optional[str]:
    if not p:
        return p
    return str(Path(p).expanduser().resolve())


# ------------------------- data model -----------------------------------------

@dataclass
class Config:
    # Google / Sheets
    service_account_file: Optional[str]
    config_sheet_id: Optional[str]
    output_folder_id: Optional[str]
    output_prefix: str = "CBTH"

    # App behavior
    per_caller_target: int = 80
    include_weekends: bool = False
    audit_csv: bool = False
    strict_schema: bool = True
    app_timezone: str = "Asia/Bangkok"

    # Data sources (DB)
    use_real_db: bool = False
    db_webview: Optional[str] = None
    db_webview_pc: Optional[str] = None
    db_webview_mobile: Optional[str] = None
    db_grafana: Optional[str] = None
    redemption_time_column: str = "created_at"

    # Drop toggles
    drop_unreachable_repeat: bool = True
    unreachable_min_count: int = 2
    drop_answered_this_month: bool = False
    drop_invalid_number: bool = True
    drop_not_interested_this_month: bool = True
    drop_not_owner_as_blacklist: bool = True
    drop_redeemed_today: bool = True

    # Optional column names / tables
    ark_gem_column: str = "ark_gem_balance"
    lifetime_topup_table: str = "user_lifetime_topup"
    lifetime_topup_key: str = "username"
    lifetime_topup_amount_col: str = "total_topup"

    # Notifications
    webhook_a: Optional[str] = None
    webhook_non_a: Optional[str] = None

    # -------- convenience --------
    def to_dict(self) -> Dict[str, Any]:
        """Useful for debugging/printing."""
        return asdict(self)

    def hint_if_incomplete(self) -> None:
        """Print gentle warnings if required keys are missing."""
        missing = []
        if not self.service_account_file:
            missing.append("GOOGLE_SERVICE_ACCOUNT_FILE")
        if not self.config_sheet_id:
            missing.append("CONFIG_SHEET_ID")
        if not self.output_folder_id:
            missing.append("OUTPUT_DRIVE_FOLDER_ID")
        if missing:
            print(
                "[config] Heads up: missing env keys -> "
                + ", ".join(missing)
                + ". You can still run dry-runs; fill them before real Sheets writes."
            )

    def hint_if_files_missing(self) -> None:
        """Warn if the service account file path is set but not found."""
        if self.service_account_file and not Path(self.service_account_file).exists():
            print(
                f"[config] Warning: service account file not found at: {self.service_account_file}\n"
                "         Put your JSON at that path or update GOOGLE_SERVICE_ACCOUNT_FILE."
            )


# ------------------------- loader ---------------------------------------------

def load_config() -> Config:
    # Load .env if present
    load_dotenv()

    # Raw env → parsed values
    cfg = Config(
        # Google / Sheets
        service_account_file=_abs_path(os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")),
        config_sheet_id=os.getenv("CONFIG_SHEET_ID"),
        output_folder_id=os.getenv("OUTPUT_DRIVE_FOLDER_ID"),
        output_prefix=os.getenv("OUTPUT_FILE_PREFIX", "CBTH"),

        # App behavior
        per_caller_target=_as_int(os.getenv("PER_CALLER_TARGET"), 80),
        include_weekends=_as_bool(os.getenv("INCLUDE_WEEKENDS"), False),
        audit_csv=_as_bool(os.getenv("AUDIT_CSV"), False),
        strict_schema=_as_bool(os.getenv("STRICT_SCHEMA"), True),
        app_timezone=_norm_tz(os.getenv("APP_TIMEZONE")),

        # Data sources (DB)
        use_real_db=_as_bool(os.getenv("USE_REAL_DB"), False),
        db_webview=os.getenv("DATABASE_URL_WEBVIEW"),
        db_webview_pc=os.getenv("DATABASE_URL_WEBVIEW_PC"),
        db_webview_mobile=os.getenv("DATABASE_URL_WEBVIEW_MOBILE"),
        db_grafana=os.getenv("DATABASE_URL_GRAFANA"),
        redemption_time_column=os.getenv("REDEMPTION_TIME_COLUMN", "created_at"),

        # Drop toggles
        drop_unreachable_repeat=_as_bool(os.getenv("DROP_UNREACHABLE_REPEAT"), True),
        unreachable_min_count=_as_int(os.getenv("UNREACHABLE_MIN_COUNT"), 2),
        drop_answered_this_month=_as_bool(os.getenv("DROP_ANSWERED_THIS_MONTH"), True),
        drop_invalid_number=_as_bool(os.getenv("DROP_INVALID_NUMBER"), True),
        drop_not_interested_this_month=_as_bool(os.getenv("DROP_NOT_INTERESTED_THIS_MONTH"), True),
        drop_not_owner_as_blacklist=_as_bool(os.getenv("DROP_NOT_OWNER_AS_BLACKLIST"), True),
        drop_redeemed_today=_as_bool(os.getenv("DROP_REDEEMED_TODAY"), True),

        # Optional columns / tables
        ark_gem_column=os.getenv("ARK_GEM_COLUMN", "ark_gem_balance"),
        lifetime_topup_table=os.getenv("LIFETIME_TOPUP_TABLE", "user_lifetime_topup"),
        lifetime_topup_key=os.getenv("LIFETIME_TOPUP_KEY", "username"),
        lifetime_topup_amount_col=os.getenv("LIFETIME_TOPUP_AMOUNT_COL", "total_topup"),

        # Notifications
        webhook_a=os.getenv("DISCORD_WEBHOOK_A"),
        webhook_non_a=os.getenv("DISCORD_WEBHOOK_NON_A"),
    )

    # Friendly clamps / normalization
    cfg.unreachable_min_count = _clamp(cfg.unreachable_min_count, 1, 10)
    if not cfg.output_prefix:
        cfg.output_prefix = "CBTH"

    # Hints (don’t crash)
    cfg.hint_if_incomplete()
    cfg.hint_if_files_missing()
    return cfg


# ------------------------- manual smoke test ----------------------------------
if __name__ == "__main__":
    c = load_config()
    print("Loaded config:")
    for k, v in c.to_dict().items():
        print(f"  {k}: {v}")
