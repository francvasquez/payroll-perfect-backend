# Payroll Perfect — Backend Summary

High-level map of how the application works, with emphasis on the Python Lambda backend. Read this before changing processing, S3 paths, or API actions.

For adding a **new tenant**, see `docs/client_integration.md`. For object keys, see `docs/s3_structure.md`.

---

## What the product does

Payroll Perfect audits one **pay period** at a time, and has the ability to audit a single employee through multiple pay-periods. A client user uploads:

- **TA** — time & attendance punches (required)
- **WFN** — payroll register (required; “WFN” is historical naming, not ADP-only)
- **Waiver** — meal-period waivers (optional)

The backend normalizes those Excel files, runs compliance and variance checks (meal breaks, OT/DT, FLSA, min wage, and similar), and returns a JSON payload the React app renders as tables. Results are stored so the user can reopen a prior period without reprocessing.

---

## Two codebases, one AWS stack

| Repo | Role |
|------|------|
| `pp-react-app/` | React SPA (Amplify). Wizard: upload → parameters → process → results. Auth via Cognito. |
| `lambda-backend/` | Single Python Lambda behind API Gateway. All authenticated API traffic hits this function. |

Region is **us-west-1**. Functions: `analytics-backend` (branch `main`) and `analytics-backend-dev` (branch `dev`). GitHub Actions zips `.py` files and calls `aws lambda update-function-code`.

The stack is **shared**. Tenants are isolated by `client_id`, not by separate Lambdas, buckets, or APIs.

```text
Browser  →  Cognito (login)
         →  API Gateway  →  Lambda (lambda_function.lambda_handler)
                              ├─ S3  (files, config.json, results.json)
                              └─ Aurora PostgreSQL  (punch + daily tables)
```

---

## How the frontend talks to the backend

There is **one POST endpoint**. The JSON body includes an `action` string. `lambda_handler` parses the body, `route_action` dispatches, and handlers return a **plain dict**. The handler wraps that dict as `{ statusCode, headers, body }`.

Known failures raise `AppError` (safe message + HTTP status, optional `code`). Anything else becomes a generic 500.

### Actions

| `action` | What it does |
|----------|----------------|
| `get-upload-url` | Presigned PUT URL so the browser uploads Excel **directly to S3** |
| `process-files` | Main pipeline: read S3 keys → Waiver → WFN → TA → `results.json` |
| `get-client-config` / `save-client-config` | Load/save `clients/{id}/config.json` |
| `list-pay-periods` | Folders under `processed/`; metadata from each `results.json` |
| `load-processed-results` | Return a saved `results.json` (reopen an audit) |
| `save-annotations` / `load-annotations` / `delete-annotations` | Notes on result rows; cleared on reprocess |
| `delete-pay-period` | Delete that date under `raw/`, `csv/`, `processed/`, plus matching DB rows |
| `query-ta-records` / `get-ta-columns` | Query UI against `{client_id}_ta` (and daily totals when needed) |

Default / unknown `action` is an error. File processing is **not** a fall-through.

### Typical create-audit sequence

1. Browser asks for a presigned URL (`get-upload-url`) and PUTs `ta.xlsx` / `wfn.xlsx` (and optionally `waiver.xlsx`).
2. Browser POSTs `process-files` with `ta_key`, `wfn_key`, optional `waiver_key`, `pay_date`, `client_id`, and the current `client_config`.
3. Lambda writes `results.json` and returns the same payload for the Results step.
4. Later visits use `list-pay-periods` + `load-processed-results`.

Uploads use standardized names (`ta.xlsx`, `wfn.xlsx`, `waiver.xlsx`). The original filename only helps the UI detect file type (`ta`/`time`, `wfn`, `waiver`).

---

## Identity and `client_id`

On login, the SPA reads `cognito:groups[0]` and treats that as `client_id`. That string must match:

- The Cognito group name
- The S3 prefix `clients/{client_id}/`
- The key in `CLIENT_CONFIGS` (`client_config.py`)
- Database tables `{client_id}_ta` and `{client_id}_daily_df`

A user should belong to **one** client group. Use a slug (lowercase letters, numbers, underscores).

---

## Two configuration layers

Do not confuse these.

| | **`CLIENT_CONFIGS`** (`client_config.py`) | **`config.json`** (S3) |
|--|------------------------------------------|-------------------------|
| **When it changes** | Code deploy | Uploaded once, then editable in the Parameters step |
| **What it holds** | `anchor_pay_date`; per-export **detection** (header row + fingerprint columns); **column mappings** (rename / concat / substring); optional `drop_rows` | Pay-period length, lag to pay date, workweek start, min wages, OT/DT thresholds, consecutive-day and shift-gap rules, **per-location overrides** |
| **Who owns it** | Developer mapping a new file format | Payroll/HR (with a seed file from ops) |

`get-client-config` reads S3 and **injects** `anchor_pay_date` from `CLIENT_CONFIGS`. Anchor is not stored in the JSON file.

Excel detection: for each system under `wfn_systems` / `ta_systems`, peek at the configured header row. If the fingerprint columns exist, that mapping is used. If none match → `WFN_SYSTEM_UNRECOGNIZED` or `TA_SYSTEM_UNRECOGNIZED` (400).

After mapping, columns should match the **standard names** in `TA_TARGET_SCHEMA` / `WFN_TARGET_SCHEMA`. TA **ID** must equal WFN **IDX** for the same person.

---

## Processing pipeline (`process-files`)

Orchestrator: `helper/file_processor.py` → `handle_file_upload`. Order is fixed: **Waiver → WFN → TA**. TA uses the other two as lookups.

```text
verify TA + WFN keys
delete annotations for this pay date
waiver (optional) → Has_Waiver_Bool by employee ID
WFN → normalize, require core columns, skip unsupported payroll tables
TA  → normalize, require full TA schema, validate pay-date window,
      attach waiver + WFN fields, compute shifts / breaks / OT-DT,
      write DB (best effort)
generate_results → put results.json → return payload to React
```

Also written: parsed CSVs under `csv/{pay_date}/` (and waiver CSV/JSON under `waiver/`).

### Waiver

Dedupes on `ID` and optional `Prior_ID_1` (both map to waived). Presence on the file → waiver on file. If omitted, every punch is treated as no waiver.

### WFN (`wfn/wfn_process.py`)

**Core columns required:** `IDX`, `Payroll Name`, `Pay Date`, `Location`. Pay dates in the file must match the requested pay date (422 if not).

Everything else is **capability-based** (`wfn/wfn_capabilities.py`). Missing columns disable that results block; the JSON **always** includes all eight `results.wfn` keys, with `[]` plus a `summary.wfn_exceptions` message for skipped tables.

### TA (`ta/ta_process.py`)

All of `TA_TARGET_SCHEMA` must exist after mapping or processing stops.

Then it:

1. Drops non-punch rows (`drop_rows`).
2. Validates punches against the pay-period window and `anchor_pay_date`. Off-cycle is 400; “straggler” punches just outside the window are **409** so the UI can ask the user to continue (`ignore_warnings`).
3. Builds shift grouping, meal-break flags, split-shift, reporting-time warnings; pulls break credits / hire date / regular rate from WFN; waiver flag from the waiver file.
4. Builds `daily_df` (hours by workday, OT/DT). `ta/ta_weekly_rules.py` applies week and consecutive-day rules; if `cba_consec_anyweek` is on, it may read the **prior** period from `{client_id}_daily_df`.
5. Filters `daily_df` to the target pay date and upserts punches + daily rows.

**DB write does not fail the audit.** If Aurora is paused or the upsert errors, `results.json` is still produced and the payload includes a `db_write` status for the UI.

Masks for “which rows appear in which table” live in `ta/ta_masks.py` and `wfn/wfn_masks.py`. Column lists for those tables live in `app_config.py`.

### Results contract (`helper/results.py`)

`generate_results` is the UI schema: `metadata`, `summary` (row counts, timings, `wfn_exceptions`), `db_cols`, `wfn.{blocks}`, `ta.{tables}`. Each table is a list of row dicts (datetimes as ISO strings, NaN as `null`). That object is both the HTTP response and `processed/{pay_date}/results.json`.

---

## Storage

### S3 (`S3_BUCKET`, default `pp-client-data`)

```text
clients/{client_id}/
  config.json
  raw/{pay_date}/ta.xlsx, wfn.xlsx
  csv/{pay_date}/ta.csv, wfn.csv
  processed/{pay_date}/results.json, annotations.json
  waiver/waiver.xlsx   # one file per client, not per date
```

S3 helpers: `helper/aws.py`. The bucket already exists; prefixes appear on first write. `config.json` must exist **before** first login or the SPA falls back to empty defaults.

### PostgreSQL (`helper/db_utils.py`)

Env: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.

| Table | Contents |
|-------|----------|
| `{client_id}_ta` | Punch-level rows for the Query UI |
| `{client_id}_daily_df` | Daily OT/DT totals; also used for CBA streak carryover |

`CREATE TABLE IF NOT EXISTS` on first successful save. Pay-period delete removes rows for that date (and the S3 prefixes above). Waiver and `config.json` are kept.

---

## Backend layout

```text
lambda_function.py      # entry: CORS, parse, route, wrap response
exceptions.py           # AppError, ValidationError, unrecognized-file codes
app_config.py           # S3 bucket, CORS, default pay-period numbers, result column lists
client_config.py        # TA/WFN target schemas + CLIENT_CONFIGS (per-client file maps)

helper/
  action_router.py      # action → handler
  aux.py                # parse body, pay-period date math, require TA+WFN keys
  file_processor.py     # process-files orchestrator
  aws.py                # S3 + presigned URLs + config/annotations/pay periods
  db_utils.py           # Aurora connect, upsert, query, delete
  results.py            # results.json / API payload

ta/                     # punch pipeline, masks, weekly/consecutive rules
wfn/                    # payroll pipeline, masks, which tables can run
waiver/                 # ID (+ optional Prior_ID_1) → waived if present
utility.py              # normalize_client_data, drop_rows, schema column helpers
```

`utility.normalize_client_data` is the shared mapping engine (rename, concat, substring).

---

## Pay-period dates

From `config.json` global values (see `helper/aux.py`):

- **last_date** (end of work period) = pay date − `days_bet_payroll_end_and_pay_date`
- **first_date** = last_date − `pay_period_length` + 1 day

`anchor_pay_date` is a known check date on the client’s calendar; it keeps workdays on the correct fiscal cycle. Location keys in `config.json` must match **Location** in the files after mapping.

---

## Mental model for new work

| If you are changing… | Start here |
|----------------------|------------|
| A new API action | `action_router.py` + a handler; keep returning a dict |
| Excel mapping / new client format | `CLIENT_CONFIGS` in `client_config.py` |
| Which payroll tables can run | `wfn/wfn_capabilities.py` then `wfn_process.py` / `results.py` |
| A time-card check or table | `ta_process.py` → `ta_utility` / `ta_masks` → `results.py` (`ta` keys) |
| OT week / consecutive-day math | `ta/ta_weekly_rules.py` |
| S3 keys or config/annotations | `helper/aws.py` and `docs/s3_structure.md` |
| Query UI / DB schema | `helper/db_utils.py` and `COLUMN_TO_KEEP_DB` in `app_config.py` |
| HTTP errors the UI special-cases | `exceptions.py` (`code` on the body); 409 stragglers; 422 WFN pay-date mismatch |

The React wizard does not use a router: `App.tsx` `currentStep` 0–3. Results tabs read `results.ta.*` and `results.wfn.*` from this backend’s JSON contract.
