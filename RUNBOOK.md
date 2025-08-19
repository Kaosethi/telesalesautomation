# Telesales Automation – Final Defaults / Runbook

This section defines the final defaults and operational rules.  
Use it as the reference for `.env`, DB joins, output structure, and scheduling.  

---

## 1. Database Columns

- **REDEMPTION_TIME_COLUMN** = `created_at` (default for “redeemed today” filter)
- **ARK_GEM_COLUMN** = `ark_gem_balance` (pass-through to Tier A sheet)
- **Lifetime top-up join**
  - Table: `user_lifetime_topup`
  - Key: `username`
  - Amount column: `total_topup`

---

## 2. Timezone & Dates

- Timezone: **Asia/Bangkok**
- Inactive Duration = `(today_date - last_login_date).days` (local midnight boundaries)
- Daily tab names = `DD-MM-YYYY` (Bangkok time)

---

## 3. Phone Normalization

- Normalize once: strip spaces/dashes; keep digits only
- **Non-A output:** split into `Calling Code` (e.g. `+66`) + local number
- **Tier A output:** `Phone Number` only (no split)

---

## 4. Platform / Source Normalization

- Canonical source strings:
  - `cabal_pc_th`
  - `cabal_mobile_th`
- Use these for blacklist triple-match consistency.

---

## 5. Idempotency Keys

- Compile upsert identity = `(assign_date, phone)`
- Behavior: remove today’s rows → append fresh run

---

## 6. Google API Setup

- Service account must have **Drive & Sheets scopes**
- Access required for:
  - Config sheet
  - Output Drive folder
- Add retry/backoff on 429 / 5xx responses

---

## 7. Errors & Partial Failures

- If one tier write fails → don’t block the other; log & continue
- If Discord webhook is missing/invalid → skip silently

---

## 8. Security

- Keep `service_account.json` **out of VCS**
- Store secrets (Google creds, Discord webhooks) in `.env`

---

## 9. Enrichment Sources

- Enrich from **Compile (current month)**:
  - Frequency
  - Attempt number
  - Latest history (+date)
  - Recent admin
- If Compile empty (first day) → default to `0` / blank

---

## 10. Run Schedule

- Run once per **workday** after source data is ready
- Suggested cron:  
  `0 9 * * 1-5` → 09:00 Asia/Bangkok
- Document schedule in README for deployment target (e.g. GitHub Actions / Cloud Run)

---

## 11. Explicitly Covered (and Good)

- Two monthly spreadsheets (Tier A, Non-A), each with daily tabs + Compile
- **Tier A:** Hot Lead only (3–7), no caller assignment, finance/game header
- **Non-A:** mix-aware caller assignment, re-query windows if short
- Thai filters:
  - ติดต่อไม่ได้ ≥ N (default N=2)
  - รับสายแล้ว เดือนนี้ → drop
  - เบอร์เสีย → blacklist
  - ไม่สนใจ เดือนนี้ → drop
  - ไม่ใช่เจ้าของไอดี → lifetime blacklist
  - Redeemed today → drop
  - Central blacklist triple-match
- Discord notifications (per tier) with:
  - File name (monthly sheet)
  - Tab name (today’s date)
  - Row count
  - Link
- `.env` defines all toggles + column/table names → schema changes don’t require spec changes
