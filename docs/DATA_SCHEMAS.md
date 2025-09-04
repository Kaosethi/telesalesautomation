<!-- PATH: docs/DATA_SCHEMAS.md -->
# Data Schemas

## Candidate pool (from loaders)
Required:
- `username:str`, `phone:str`, `source_key:str` (`cabal_pc_th` / `cabal_mobile_th`)
- `last_login:datetime|str`

Optional:
- `amount:float`, `Tier:str`, `ark_gem_balance:float`

Added during processing:
- `window_label:str` in {Hot, Cold, Hibernated}

## Non-A (daily tab)
- `Assign Date` (string date, `DD-MM-YYYY`)
- `telesale` (caller; blank if no callers)
- `source_key` (`cabal_pc_th` / `cabal_mobile_th`)
- `username`, `phone`
- `window_label` (Hot/Cold/Hibernated)
- **Call Status** (Column **L**) — dropdown
- **Result** (Column **M**) — dropdown

**Dropdown options (Thai)**
- Call Status (L):
  - รับสาย
  - ไม่รับสาย
  - ติดต่อไม่ได้
  - กดตัดสาย
  - รับสายไม่สะดวกคุย
- Result (M):
  - เบอร์เสีย
  - ไม่สนใจ
  - ไม่ใช่เจ้าของไอดี

## Non-A (Compile)
- Same columns as daily; used for history & filters (this-month answered / not-interested)

## Tier-A (daily & Compile)
- No per-caller assignment
- Includes Tiering fields if present: `Tier`, `amount`, `ark_gem_balance`
