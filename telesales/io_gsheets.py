# telesales/io_gsheets.py
"""
Google Sheets / Drive helpers with a *forgiving* dry‑run mode.

- If Google creds or required IDs are missing, we DO NOT crash.
  We log what we would have done and return placeholders.
- When creds are present, we use gspread + Drive API to:
    * find or create a month file in the target Drive folder
    * ensure tabs exist
    * write a DataFrame to a tab
    * upsert to Compile (remove today's rows, append fresh)

Requirements (already in requirements.txt):
  - gspread, gspread-dataframe
  - google-api-python-client, google-auth
  - pandas
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional, Tuple
import os
import sys

import pandas as pd

# Third‑party Google libs
try:
    import gspread
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except Exception as _e:
    # We allow imports to fail in environments without these libs yet.
    gspread = None
    set_with_dataframe = None
    get_as_dataframe = None
    Credentials = None
    build = None
    HttpError = Exception  # type: ignore


# ----------------------------- datamodel --------------------------------------

@dataclass
class SheetsInfo:
    spreadsheet_id: str
    spreadsheet_url: str
    title: str


# ----------------------------- client -----------------------------------------

class SheetsClient:
    def __init__(
        self,
        *,
        service_account_file: Optional[str],
        output_folder_id: Optional[str],
        output_prefix: str = "CBTH",
    ) -> None:
        self.output_folder_id = output_folder_id
        self.output_prefix = output_prefix

        self._dry_run_reason: Optional[str] = None
        self.gc = None
        self.drive = None

        # Decide if we can do real work
        if not service_account_file:
            self._dry_run_reason = "missing GOOGLE_SERVICE_ACCOUNT_FILE"
            self._log_dry_run("no service account")
            return

        if gspread is None or Credentials is None or build is None:
            self._dry_run_reason = "google libraries not available"
            self._log_dry_run("google libs missing")
            return

        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
            self.gc = gspread.authorize(creds)
            self.drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        except Exception as e:
            self._dry_run_reason = f"auth error: {e}"
            self._log_dry_run("auth error")
            return

        if not output_folder_id:
            self._dry_run_reason = "missing OUTPUT_DRIVE_FOLDER_ID"
            self._log_dry_run("no output folder id")
            # Still keep gspread handy for read‑only tasks, but we won’t create files.

    # -------------------------- util / state ----------------------------------

    @property
    def dry_run(self) -> bool:
        return self._dry_run_reason is not None

    def _log(self, msg: str) -> None:
        print(f"[sheets] {msg}")

    def _log_dry_run(self, msg: str) -> None:
        print(f"[sheets:DRY‑RUN] {msg}")

    # -------------------------- naming helpers --------------------------------

    def month_title(self, tier_label: str, dt: Optional[datetime] = None) -> str:
        d = dt or datetime.now()
        return f"{self.output_prefix}-{tier_label} - {d:%m-%Y}"

    # -------------------------- Drive helpers ---------------------------------

    def _drive_search_by_name(self, name: str) -> Optional[Tuple[str, str]]:
        """
        Return (file_id, webViewLink) for a spreadsheet with exact name in output folder.
        """
        if self.drive is None or not self.output_folder_id:
            return None
        try:
            safe_name = name.replace("'", "\\'")
            q = (
                f"name = '{safe_name}' and "
                f"mimeType = 'application/vnd.google-apps.spreadsheet' and "
                f"trashed = false and "
                f"'{self.output_folder_id}' in parents"
            )
            resp = self.drive.files().list(
                q=q, fields="files(id, name, webViewLink)", pageSize=1
            ).execute()
            files = resp.get("files", [])
            if files:
                f = files[0]
                return f["id"], f.get("webViewLink", f"https://docs.google.com/spreadsheets/d/{f['id']}")
            return None
        except HttpError as e:
            self._log(f"Drive search error: {e}")
            return None

    def _drive_create_spreadsheet(self, name: str) -> Optional[Tuple[str, str]]:
        if self.drive is None or self.gc is None or not self.output_folder_id:
            return None
        try:
            # Create empty Spreadsheet via Drive
            file_metadata = {
                "name": name,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [self.output_folder_id],
            }
            file = self.drive.files().create(
                body=file_metadata,
                fields="id, webViewLink"
            ).execute()
            file_id = file["id"]
            url = file.get("webViewLink", f"https://docs.google.com/spreadsheets/d/{file_id}")
            self._log(f"Created spreadsheet: {name} ({file_id})")
            return file_id, url
        except HttpError as e:
            self._log(f"Drive create error: {e}")
            return None

    # -------------------------- public API ------------------------------------

    def find_or_create_month_file(self, tier_label: str) -> SheetsInfo:
        """
        Ensure a monthly spreadsheet exists for given tier.
        Returns SheetsInfo(spreadsheet_id, url, title).
        """
        title = self.month_title(tier_label)

        if self.dry_run:
            self._log_dry_run(f"find_or_create_month_file('{title}')")
            return SheetsInfo("DRY-RUN", "https://example.com", title)

        # Try find
        found = self._drive_search_by_name(title)
        if found:
            file_id, url = found
            return SheetsInfo(file_id, url, title)

        # Create
        created = self._drive_create_spreadsheet(title)
        if created:
            file_id, url = created
            return SheetsInfo(file_id, url, title)

        # Fallback (shouldn't happen often)
        self._log(f"Could not create/find spreadsheet: {title}; returning placeholder.")
        return SheetsInfo("UNKNOWN", "https://docs.google.com", title)

    def ensure_tabs(self, spreadsheet_id: str, tab_names: Iterable[str]) -> None:
        """
        Ensure each tab exists. If missing, create with 1x1 grid.
        """
        if self.dry_run or spreadsheet_id in {"DRY-RUN", "UNKNOWN"}:
            self._log_dry_run(f"ensure_tabs({spreadsheet_id}, {list(tab_names)})")
            return

        sh = self.gc.open_by_key(spreadsheet_id)  # type: ignore
        existing = {ws.title for ws in sh.worksheets()}
        for name in tab_names:
            if name not in existing:
                sh.add_worksheet(title=name, rows=1, cols=1)
                self._log(f"Created tab '{name}' in {spreadsheet_id}")

    def write_df_to_tab(self, spreadsheet_id: str, tab_name: str, df: pd.DataFrame) -> None:
        """
        Replace the entire tab with df (including headers).
        """
        if df is None:
            return
        if self.dry_run or spreadsheet_id in {"DRY-RUN", "UNKNOWN"}:
            self._log_dry_run(f"write_df_to_tab({tab_name}) rows={len(df)}")
            return

        if set_with_dataframe is None:
            self._log("gspread-dataframe not available; cannot write. (Did you install requirements?)")
            return

        sh = self.gc.open_by_key(spreadsheet_id)  # type: ignore
        try:
            ws = sh.worksheet(tab_name)
        except gspread.WorksheetNotFound:  # type: ignore
            ws = sh.add_worksheet(title=tab_name, rows=1, cols=1)
        # Clear and write
        ws.clear()
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

    def read_tab_as_df(self, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
        """
        Read a tab as DataFrame. If missing, returns empty df.
        """
        if self.dry_run or spreadsheet_id in {"DRY-RUN", "UNKNOWN"}:
            self._log_dry_run(f"read_tab_as_df({tab_name}) -> empty df")
            return pd.DataFrame()

        if get_as_dataframe is None:
            self._log("gspread-dataframe not available; cannot read. Returning empty df.")
            return pd.DataFrame()

        sh = self.gc.open_by_key(spreadsheet_id)  # type: ignore
        try:
            ws = sh.worksheet(tab_name)
        except gspread.WorksheetNotFound:  # type: ignore
            return pd.DataFrame()

        df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
        # Drop completely empty rows
        if df is not None and not df.empty:
            df = df.dropna(how="all")
        return df if df is not None else pd.DataFrame()

    def upsert_compile(self, spreadsheet_id: str, today_df: pd.DataFrame, assign_date_col: str = "Assign Date") -> None:
        """
        Upsert today's rows into Compile:
          - read Compile
          - drop rows where Assign Date == today (string match)
          - append today's rows
          - write back
        """
        if today_df is None:
            return
        if self.dry_run or spreadsheet_id in {"DRY-RUN", "UNKNOWN"}:
            self._log_dry_run(f"upsert_compile(rows={len(today_df)})")
            return

        compile_df = self.read_tab_as_df(spreadsheet_id, "Compile")
        # Normalize column if missing
        if assign_date_col not in today_df.columns:
            # We don't try to guess here; just log.
            self._log("Assign Date column missing in today_df; writing raw append to Compile.")
            new_df = pd.concat([compile_df, today_df], ignore_index=True)
        else:
            today_val_set = set(str(v) for v in today_df[assign_date_col].astype(str).fillna(""))
            if compile_df is not None and not compile_df.empty and assign_date_col in compile_df.columns:
                mask = ~compile_df[assign_date_col].astype(str).isin(today_val_set)
                kept = compile_df[mask]
            else:
                kept = pd.DataFrame(columns=today_df.columns)
            new_df = pd.concat([kept, today_df], ignore_index=True)

        self.write_df_to_tab(spreadsheet_id, "Compile", new_df)
