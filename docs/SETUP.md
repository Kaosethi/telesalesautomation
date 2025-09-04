<!-- PATH: docs/SETUP.md -->
# Setup

## Prerequisites
- Python **3.10+**
- A Google Cloud project with **Sheets API** and **Drive API** enabled
- A **service account** JSON key

## Install
    pip install -r requirements.txt

## Credentials & Permissions
1) Put your service account JSON somewhere safe and set in `.env`:
   
   `GOOGLE_SERVICE_ACCOUNT_FILE=/abs/path/to/service_account.json`
   
2) Share the **output Drive folder** with the **service account email** as **Editor**.  
3) Ensure the service account can **read** the anchor Config Sheet (`CONFIG_SHEET_ID`).

## Environment
Copy `.env.example` to `.env` and fill at minimum:
- `CONFIG_SHEET_ID` (the Sheet with **Holidays/Config/Windows/Blacklist/Cabal_Tiers/Callers**)
- `OUTPUT_DRIVE_FOLDER_ID` (Drive folder for monthly files)
- (Optional) Discord webhooks

## Verify a run
    python main.py

You should see logs and—if Drive permissions are missing/invalid—**DRY-RUN** messages and placeholder links (no crash).
