# test_blacklist.py
from telesales.config import load_config
from telesales.io_gsheets import SheetsClient
from telesales import filters
import pandas as pd

cfg = load_config()
sc = SheetsClient(
    service_account_file=cfg.service_account_file,
    output_folder_id=cfg.output_folder_id,
    output_prefix=cfg.output_prefix,
)

# --- 1. Read Blacklist tab ---
blacklist_df = sc.read_tab_as_df(cfg.config_sheet_id, "Blacklist")
print("\n=== Blacklist tab (raw) ===")
print(blacklist_df.head())
print("Columns:", list(blacklist_df.columns))

# --- 2. Read Tier A daily tab (today) ---
tier_a_info = sc.find_or_create_month_file("Tier A")
today_tab = filters.today_key()
pool_df = sc.read_tab_as_df(tier_a_info.spreadsheet_id, today_tab)

print("\n=== Tier A daily tab (raw) ===")
print(pool_df.head())
print("Columns:", list(pool_df.columns))

# --- 3. Build keys for both ---
bl_keys = filters._triple_key(blacklist_df).unique().tolist()
pool_keys = filters._triple_key(pool_df).unique().tolist()

print("\n=== Sample Blacklist keys ===")
print(bl_keys[:5])

print("\n=== Sample Pool keys ===")
print(pool_keys[:5])

# --- 4. Show overlaps ---
overlaps = set(bl_keys) & set(pool_keys)
print("\n=== Overlaps between Blacklist and Pool ===")
print(overlaps if overlaps else "None found")
