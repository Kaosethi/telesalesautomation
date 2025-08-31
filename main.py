# main.py
from telesales.tier_a.pipeline import run as run_tier_a
from telesales.non_a.pipeline import run as run_non_a

if __name__ == "__main__":
    a = run_tier_a()
    n = run_non_a()
    print("[Tier A]", a.row_count, a.sheet_url)
    print("[Non A]", n.row_count, n.sheet_url)
