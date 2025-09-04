<!-- PATH: README.md -->
# Telesales Automation

Python pipeline that allocates daily call targets to telesales agents from two sources (**cabal_pc_th**, **cabal_mobile_th**) using a single **Google Sheet** as the anchor for config (Holidays, Config, Windows, Blacklist, Cabal_Tiers, Callers).  
Outputs two monthly spreadsheets: **Tier A** (no caller split) and **Non-A** (per-caller, mix-aware).

## Quickstart
1) Install **Python 3.10+**  
2) Create venv (Windows):

       python -m venv .venv
       .\.venv\Scripts\activate

3) Install requirements:

       pip install -r requirements.txt

4) Copy and edit env:

       copy .env.example .env

5) Share your **output Drive folder** with the service-account email (Editor).  
6) Run:

       python main.py

## What you get each run
- **Tier A**: Hot window only (3–7 days) + high-value rule; no caller assignment.
- **Non-A**: per-caller targets with **mix weights** (PC/Mobile), fairness across callers.
- Self-healing headers; idempotent daily re-runs; DRY-RUN safety when Drive/creds missing.
- Non-A sheets enforce dropdowns: **L = Call Status**, **M = Result** (Thai).

## Docs
- `docs/SETUP.md` – one-time setup (APIs, creds, folder sharing)
- `docs/CONFIGURATION.md` – anchor Google Sheet schema + mix-weight rules
- `docs/DATA_SCHEMAS.md` – input/output/compile columns
- `docs/RUNBOOK.md` – daily operations (holiday gate, month rollover, idempotency)
- `docs/TESTS.md` – manual test matrix & results
- `docs/TROUBLESHOOTING.md` – quick fixes (Drive perms, DRY-RUN, ratios)
