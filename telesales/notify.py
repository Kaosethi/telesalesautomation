# telesales/notify.py
"""
Discord notifications (one message per tier).
- Safe by default: if webhook is missing/invalid, we just log and return False.
- No external dependencies beyond `requests` (already in requirements.txt).
"""

from __future__ import annotations
from typing import Optional
import requests
import json


def notify_discord(
    webhook_url: Optional[str],
    *,
    tier_label: str,
    file_name: str,
    tab_name: str,
    row_count: int,
    sheet_url: str,
) -> bool:
    """
    Send a simple, readable notification.

    Returns:
        True if the request looks successful (2xx), else False.
    """
    if not webhook_url:
        print(f"[notify] Skipped: webhook not set for {tier_label}.")
        return False

    content = (
        f"**Telesales list ready – {tab_name} ({tier_label})**\n"
        f"📄 **File:** {file_name}\n"
        f"🗂️ **Tab:** {tab_name}\n"
        f"📊 **Rows:** {row_count}\n"
        f"{sheet_url}"
    )

    try:
        resp = requests.post(
            webhook_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"content": content}),
            timeout=10,
        )
        if 200 <= resp.status_code < 300:
            print(f"[notify] Sent Discord message for {tier_label} ({row_count} rows).")
            return True
        else:
            print(
                f"[notify] Discord responded with {resp.status_code}: {resp.text[:200]}"
            )
            return False
    except requests.RequestException as e:
        print(f"[notify] Error sending Discord message: {e}")
        return False
