<!-- PATH: docs/TROUBLESHOOTING.md -->
# Troubleshooting

## Drive 404 / cannot create files
- Ensure `OUTPUT_DRIVE_FOLDER_ID` is correct and shared with the **service account** (Editor).
- On failure, the run uses placeholders and **DRY-RUN** logs (no crash).

## SpreadsheetNotFound on ensure_tabs
- Fixed: `ensure_tabs()` skips when spreadsheet id is `"DRY-RUN"` / `"UNKNOWN"`.

## Only one source appears in Non-A
- The other source may be siphoned to Tier-A or filtered out.
- Mix weights are **targets**; actual split depends on availability after filters.

## Mix weights look 50/50 even when set 0.1/0.8
- Mock pools can be balanced; with real DB rows, ratios will skew correctly.
- Invalid weights fall back to **50/50**; sums normalize to 1.0.

## SettingWithCopyWarning (pandas)
- Cosmetic and safe to ignore; can be silenced by assigning via `.loc`.

## Compile rows “disappear”
- Likely dropped by this-month **Answered/Not-Interested** filters using Compile history.
- Change the test rows’ `Assign Date` to last month or toggle the drops off to verify behavior.
