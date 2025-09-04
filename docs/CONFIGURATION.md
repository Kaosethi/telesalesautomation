<!-- PATH: docs/CONFIGURATION.md -->
# Anchor Google Sheet – Schema

The code reads a single Sheet with these tabs:

## 1) Holidays
| column | type | notes |
|---|---|---|
| date | date/string | `YYYY-MM-DD` recommended |
| holiday | bool | TRUE/FALSE |
| note | string | optional |

- If `holiday=TRUE`, Non-A can be skipped (unless `INCLUDE_WEEKENDS=true`).
- Weekends honored per `INCLUDE_WEEKENDS`.

## 2) Config
| column | type | required | example |
|---|---|---:|---|
| source_key | enum | ✓ | `cabal_pc_th`, `cabal_mobile_th` |
| enabled | bool | ✓ | TRUE |
| adapter_path | string | ✓ | `adapters.cabal_pc:fetch_candidates` |
| country_allowlist | string |  | `66` |
| mix_weight | float |  | `0.5` |

**Mix-weight rules**
- Any invalid/blank/≤0 → **fallback 50/50**
- Sums ≠ 1 (including >1) → **normalize** to sum=1
- Actual split may deviate if one source has fewer available rows after filters.

## 3) Windows
| label | day_min | day_max | priority |
|---|---:|---:|---:|
| Hot Lead | 3 | 7 | 1 |
| Cold Lead | 8 | 14 | 2 |
| Hibernated | 15 |  | 3 |

- `day_max` empty = open-ended.
- Earlier priority wins on dedupe (“earlier-window-wins”).

## 4) Blacklist
| username | phone | source_key |
|---|---|---|
| alice | 812345678 | cabal_pc_th |

- Any match dropped from outputs (strict matching).

## 5) Cabal_Tiers
| min_topup | label |
|---:|---|
| 100000 | A-2 |
| 50000 | B-2 |

- Used for Tiering; if numeric `amount` exists, `amount ≥ 100000` → Tier A.

## 6) Callers
| telesale | Available |
|---|---|
| Amm | TRUE |
| Bas | FALSE |

- Only `Available=TRUE` callers are used to size Non-A daily targets.
