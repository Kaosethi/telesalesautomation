<!-- PATH: docs/TESTS.md -->
# Manual Tests & Status

| Test | Expected | Status | Notes |
|---|---|---|---|
| No callers available | Non-A writes rows; Telesale empty | ✅ Pass | Verified |
| Bad/missing mix weights (Non-A) | Fallback 50/50 per caller | ⏸ Pending DB | Tier-A shows fallback; Non-A depends on source availability |
| Disabled source (Mobile) | Only PC used | ✅ Pass | `enabled=FALSE` for Mobile |
| Headers integrity | Writer restores headers | ✅ Pass | Self-healing |
| Compile idempotency | No duplicate rows same day | ✅ Pass | Stable counts |
| Invalid numbers filter | Bad phones removed | ✅ Pass | With `DROP_INVALID_NUMBER=true` |
| Answered / Not-Interested | Exclude this month | ✅ Pass (behavior) / ⏸ Pending DB (counters) | Exclusion works; counters to verify with DB |
| Blacklist exclusion | Blacklisted users absent | ✅ Pass | Drop counts reflect blacklist |
| Month rollover via RUN_DATE | Targets next month | ✅ Pass | Creation requires Drive perms |
| Drive failure safety | DRY-RUN; no crash | ✅ Pass | Guarded `ensure_tabs()` |
