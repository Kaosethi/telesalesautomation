<!-- PATH: docs/RUNBOOK.md -->
# Telesales Automation – Runbook

## Defaults & Rules
- Timezone: **Asia/Bangkok**
- Daily tab names: `DD-MM-YYYY`
- **Tier A**: Hot (3–7) only; no per-caller split; Tiering via `amount ≥ 100000` or `Tier` starts with `A-`
- **Non-A**: per-caller targets using **mix weights**, with re-query (Cold → Hibernated) if short
- Idempotent: re-runs replace today’s daily tab and upsert Compile (remove today then append)

## Run
    python main.py

**Expected logs**
- `[filters] DD-MM-YYYY drops: blacklist=X, idempotent=Y, unreachable=Z, answered=A, not_interested=B → kept=M/N`
- `[Non-A] pool sizes: Hot PC=..., Hot Mobile=..., Cold PC=..., ...`
- `[Non-A] per-caller distribution:` (PC/Mobile columns)
- `[Tier A] <rows> <url>` / `[Non A] <rows> <url>`

## Holiday / Weekend Gate
- If `INCLUDE_WEEKENDS=false`, Sat/Sun ⇒ Non-A can be skipped.
- If **Holidays** tab marks `holiday=TRUE`, Non-A can be skipped.

## Month rollover / backfill
Set `.env`:

    RUN_DATE=2025-09-01

Run again to target `... - 09-2025` files. If the service account can’t create files, pre-create the month files in the output folder—the code will detect them.

## Non-A dropdowns
- Applied **only** on Non-A: **Column L = Call Status**, **Column M = Result** (Thai options above).
- Tier-A tabs do **not** get dropdowns.

## Phone normalization
- Normalize once (digits only). In Non-A, display as `Calling Code` + local number if needed; in Tier-A, show `Phone Number` only.

## Errors & safety
- Missing/invalid Drive/creds ⇒ **DRY-RUN** placeholders; no crash.
- If one tier write fails, the other continues.
- Missing Discord webhook ⇒ skip silently.
