from telesales.tier_a import pipeline as tier_a
from telesales.non_a import pipeline as non_a
from telesales.io_gsheets import SheetsClient
from telesales.config import load_config
import pandas as pd


if __name__ == "__main__":
    cfg = load_config()
    sc = SheetsClient(
        service_account_file=cfg.service_account_file,
        output_folder_id=cfg.output_folder_id,
        output_prefix=cfg.output_prefix,
    )

    # Blacklist comes from central config sheet
    blacklist_df = sc.read_tab_as_df(cfg.config_sheet_id, "Blacklist") if cfg.config_sheet_id else pd.DataFrame()

    # Run Tier A
    tier_a_res = tier_a.run()
    # After Tier A daily tab is finalized, read fresh Compile
    tier_a_compile = sc.read_tab_as_df(tier_a_res.spreadsheet_id, "Compile")

    # Run Non-A (use updated Compile + same Blacklist)
    non_a_res = non_a.run()

    print("[Tier A]", tier_a_res.row_count, tier_a_res.sheet_url)
    print("[Non A]", non_a_res.row_count, non_a_res.sheet_url)
