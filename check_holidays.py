# check_holidays.py (debug version)
import pandas as pd
from telesales.config import load_config
from telesales.io_gsheets import SheetsClient

def main():
    cfg = load_config()
    sc = SheetsClient(
        service_account_file=cfg.service_account_file,
        output_folder_id=cfg.output_folder_id,
        output_prefix=cfg.output_prefix,
    )

    df = sc.read_tab_as_df(cfg.config_sheet_id, "Holidays")
    if df is None or df.empty:
        print("⚠️ No Holidays tab found or it's empty.")
        return

    cols = {str(c).strip().lower(): c for c in df.columns if isinstance(c, str)}
    dcol = cols.get("date") or list(cols.values())[0]

    raw_values = df[dcol].dropna().astype(str).tolist()
    print("🔍 Raw values from sheet:")
    for r in raw_values:
        print("  ", repr(r))

    print("\n✅ Parsed holidays:")
    for r in raw_values:
        try:
            # First, force ISO (YYYY-MM-DD)
            parsed = pd.to_datetime(r, format="%Y-%m-%d", errors="raise").date()
        except Exception:
            # Fallback: try DMY (01/09/2025, 01-09-2025)
            parsed = pd.to_datetime(r, dayfirst=True, errors="coerce").date()
        print(f"  {r}  →  {parsed}")

if __name__ == "__main__":
    main()
