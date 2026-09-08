# Payroll Perfect — New Client Onboarding Guide

**Purpose:** A step-by-step roadmap for integrating a new client (tenant) into Payroll Perfect. 

**Outcome of a successful onboard:** The client’s users can sign in, upload Time & Attendance (TA) and Payroll (WFN) Excel files, and run a first pay-period audit.

**Time & Attendance** and **Payroll / WFN** are the two required exports. A **meal-waiver** file is optional.

---

## How a client is isolated

Payroll Perfect is **multi-tenant on a shared AWS stack**. You do **not** create a new Lambda, API, S3 bucket, or database cluster per client.

| Layer | How the new client is isolated |
|-------|--------------------------------|
| **Identity** | Cognito **group name** = `client_id` (first group on the user’s token) |
| **Files & results** | `s3://pp-client-data/clients/{client_id}/…` |
| **Business rules** | `clients/{client_id}/config.json` in S3 |
| **File-format mapping** | Entry in `CLIENT_CONFIGS` in `lambda-backend/client_config.py` (code deploy) |
| **Database** | Tables `{client_id}_ta` and `{client_id}_daily_df`, created automatically on first successful process |

Pick a `client_id` once and use it everywhere: Cognito group, S3 prefix, `CLIENT_CONFIGS` key, and table names. Recommended form: lowercase letters, numbers, and underscores only (e.g. `acme_hotels`). Do **not** put a user in more than one client group — the app uses the first group on the token.

---

## Roadmap at a glance

| Step | Who | What |
|------|-----|------|
| **1** | Sales / CS | Choose `client_id` and collect sample files + business rules |
| **2** | Developer | Map the client’s Excel formats in `CLIENT_CONFIGS` |
| **3** | Ops | Create the Cognito group and users |
| **4** | Ops | Upload `config.json` to S3 |
| **5** | Developer | Deploy the Lambda with the new `CLIENT_CONFIGS` entry |
| **6** | CS + client | Run a pilot pay period and tune |
| **7** | Ops | Add remaining users and go live |

Shared AWS resources (Lambda, API Gateway, S3 bucket, Aurora) stay as they are. Folders and database tables appear when the first files are processed.

---

## Step 1 — Collect client data

Ask the client (or their payroll/IT contact) for **one complete pay period** of sample files, plus the setup items below. Excel (`.xlsx` or `.xls`) is required. Redacted samples are acceptable if employee IDs stay consistent across Time & Attendance and Payroll.

### 1a. Files to request

- [ ] Sample **time & attendance** export for one pay period
- [ ] Sample **payroll register** (WFN) export for the **same** pay period
- [ ] Sample **waiver** file (if they use meal waivers), or written confirmation they do not
- [ ] If they use **more than one TA export format** (e.g. different properties), a sample **per format**

The upload UI recognizes files by name: the filename should contain `ta` or `time` (time cards), `wfn` (payroll), or `waiver`. Original names are otherwise ignored; the app stores them as `ta.xlsx`, `wfn.xlsx`, and `waiver.xlsx`.

### 1b. System setup information

| Item | What to ask for | Example |
|------|-----------------|--------|
| **Anchor pay date** | One **known pay date** from their payroll calendar (`YYYY-MM-DD`). Used to align workdays to the correct pay period. | `2026-01-16` |
| **Payroll system** | Vendor/product name for the payroll register export. | ADP Workforce Now, UKG, etc. |
| **Time system** | Vendor/product name for the punch export. | ADP Time & Attendance, UKG Workforce Manager, etc. |
| **Primary contact** | Payroll/HR person for mapping questions during setup. | name + email |

### 1c. Employee ID rule (must be true after mapping)

Time-card **ID** and payroll **IDX** must identify the **same person**. Your raw files may use different columns (e.g. ADP `CO.` + `FILE#`); that is fine — we concatenate or rename them during Step 2 so they match.

### 1d. Time & Attendance fields (required)

Column names below are the **standard labels after mapping**. The client’s headers can differ.

All of these are required for time-card processing:

| Field | Description |
|-------|-------------|
| **ID** | Unique employee identifier (must match payroll IDX). |
| **Location** | Site or company code where the employee works. |
| **Employee** | Employee name (display on reports). |
| **In Punch** | Clock-in date/time. |
| **Out Punch** | Clock-out date/time. |
| **Status** | Employment or punch status (e.g. active, terminated). |
| **Status Date** | Date associated with the status, if applicable. |

### 1e. Payroll / WFN fields (flexible)

Payroll is **flexible**: the file will load with a small core set. We run only the checks the file supports; missing sections disable those payroll tables (with a notice in the app).

**Bare minimum** (file will not load without these):

| Field | Description |
|-------|-------------|
| **IDX** | Unique employee identifier (must match Time & Attendance ID). |
| **Payroll Name** | Employee name as shown on the payroll register. |
| **Pay Date** | Check pay date on each row. |
| **Location** | Company or location code (used for wage and rule overrides). |

**Full list — provide what they have**

Identifiers: **IDX**, **Location**, **Payroll Name**, **Pay Date**

Status and rates: **FLSA Status**, **Position Status**, **Hire Date**, **Job Description**, **Termination Date**, **Regular Rate Paid**

Standard hours & earnings: **Regular Hours**, **Overtime Hours**, **Double Time Hours**, **Regular Earnings Total**, **Overtime Earnings**

Additional earnings: **Misc FLSA Earnings**, **Bonus Earnings**, **Commission Earnings**, **Auto Gratuity Earnings**, **Restricted Service Charge Earnings**, **Bellman Service Charge Earnings**, **Double Time Earnings**

Break, rest, sick, vacation: **Break Credit Hours**, **Break Credit Earnings**, **Rest Credit Hours**, **Rest Credit Earnings**, **Sick Pay Hours**, **Sick Pay Earnings**, **Vacation Hours**, **Vacation Earnings**

**Typical impact if sections are missing**

| If they cannot provide… | Payroll tables affected |
|-------------------------|-------------------------|
| Only the **bare minimum** | Most payroll variance tables stay empty; time-card vs payroll OT/DT may still run if TA includes hours. |
| RROP-related hours/earnings columns | Overtime, double-time, break, rest, and sick **RROP vs actual paid** tables. |
| Break credit columns | Break credit variance table only. |
| Rest credit columns | Rest credit variance table only. |
| Sick columns | Sick credit variance table only. |
| FLSA / min wage / status columns | FLSA check, minimum wage check, and/or non-active check as applicable. |

### 1f. Meal waiver (optional)

If used, the file needs an employee **ID** (matching TA) and a **Check** column. A value of `x` (any case) means a valid meal-period waiver is on file. If they skip the waiver file, every punch is treated as having **no** waiver.

### 1g. Business rules for `config.json`

Collect **global** defaults, plus **per-location** overrides where rules differ by site. Use the same location codes that appear in the files.

| Setting | Description | Example |
|---------|-------------|---------|
| **pay_period_length** | Length of the pay period in **days**. | `14` (biweekly) |
| **days_bet_payroll_end_and_pay_date** | Days between the **end of the work period** and the **pay date**. | `6` |
| **workweek_start** | First day of the workweek for overtime. | `"Sunday"` |
| **pay_periods_per_year** | Number of pay periods per year. | `26` |
| **min_wage** | Employer minimum wage used in audits (often a contracted rate). | `17.75` |
| **state_min_wage** | State minimum wage. | `16.90` |
| **ot_day_max** | Daily hours after which day-level OT applies. | `8` |
| **ot_week_max** | Weekly hours after which week-level OT applies. | `40` |
| **dt_day_max** | Daily hours threshold for double-time. | `12` |
| **number_of_consec_days_before_ot** | Consecutive days worked before consecutive-day OT. | `6` |
| **time_gap_for_new_shift** | Minutes between punches to treat as a **new shift**. | `60` |
| **cba_consec_anyweek** | If `true`, consecutive-day rules can cross workweek boundaries (CBA). | `false` |

---

## Step 2 — Map file formats in code

This is the main developer task. Sample files from Step 1 are mapped onto the standard field names in `lambda-backend/client_config.py` under `CLIENT_CONFIGS`.

Add an entry keyed by the same `client_id`:

```python
CLIENT_CONFIGS = {
    "acme_hotels": {
        "anchor_pay_date": "2026-01-16",  # YYYY-MM-DD; a real pay date on their calendar
        "wfn_systems": { ... },
        "ta_systems": { ... },
    },
}
```

For each payroll or time-system format, define:

1. **Detection** — header row index and a few columns that uniquely identify that export (so the app can auto-detect which mapping to use).
2. **Mappings** — rename or transform source columns to the standard names in Step 1. Supported transforms today: simple rename, **concat** (several columns + delimiter), and **substring**.
3. **drop_rows** (optional) — drop blank punches, test employees, or pay codes that are not time punches.
4. **force_type** (optional) — force a column to string so IDs are not read as numbers.

You can register **more than one** TA (or WFN) system per client. The reader peeks at the header and picks the first matching system. If nothing matches, processing fails with “unrecognized file format.”

**`anchor_pay_date` lives in this code entry**, not in S3. It is injected when the app loads `config.json`.

Copy the `demo_client` block in `client_config.py` as a starting template. If the client’s headers already match the standard names, mappings can be empty.

---

## Step 3 — Create Cognito access

In the existing Cognito user pool (no new pool):

1. Create a **group** whose name is exactly `client_id` (e.g. `acme_hotels`).
2. Create each user (email/password, or your existing invite flow).
3. Add the user to **that one group only**.

On login, the app sets `clientId` from `cognito:groups[0]`. If the group is missing or misspelled, the user cannot load that client’s files or config.

---

## Step 4 — Upload runtime config to S3

Put business rules at:

```text
s3://pp-client-data/clients/{client_id}/config.json
```

Do this **before** the first login. If the file is missing, the UI falls back to empty/zero defaults, which will produce wrong pay-period math.

Upload the file to the existing bucket (`S3_BUCKET`, default `pp-client-data`). You do **not** create a new bucket. The `clients/{client_id}/` prefix appears when this object (or the first upload) is written.

Example:

```json
{
  "global": {
    "pay_period_length": 14,
    "days_bet_payroll_end_and_pay_date": 6,
    "workweek_start": "Sunday",
    "pay_periods_per_year": 26,
    "min_wage": 17.75,
    "state_min_wage": 16.90,
    "ot_day_max": 8,
    "ot_week_max": 40,
    "dt_day_max": 12,
    "number_of_consec_days_before_ot": 6,
    "time_gap_for_new_shift": 60,
    "cba_consec_anyweek": false
  },
  "locations": {
    "001": {
      "min_wage": 18.00,
      "state_min_wage": 17.00
    },
    "002": {
      "cba_consec_anyweek": true
    }
  }
}
```

Include a location key only when that site **differs** from global. Any global field may be overridden. After go-live, users can also edit and save this file from the Parameters step in the app.

After the first processed period, that client’s S3 layout looks like:

```text
clients/{client_id}/
  config.json
  raw/{pay_date}/ta.xlsx, wfn.xlsx
  csv/{pay_date}/ta.csv, wfn.csv
  processed/{pay_date}/results.json, annotations.json
  waiver/waiver.xlsx, waiver.csv, waiver.json
```

---

## Step 5 — Deploy the backend

Ship the `CLIENT_CONFIGS` change from Step 2 to the Lambda the app already uses (`analytics-backend` / `analytics-backend-dev`). Until this deploy lands, the new client’s files will not be recognized.

You do **not** change Lambda environment variables, API Gateway routes, VPC, or RDS security groups for a new client.

---

## Step 6 — Pilot one pay period

Have one client user sign in and run the wizard:

1. Upload TA + WFN (and waiver if applicable) for a known pay date that matches the **anchor pay date** cycle.
2. Confirm Parameters (from `config.json`) look right; adjust and save if needed.
3. Process.

**Check that:**

- File formats were detected (no “unrecognized system” error).
- Employee IDs line up between time cards and payroll (spot-check a few people).
- Pay date aligns with punches and the payroll register.
- Time-card tables populate.
- Payroll tables that should run have rows; tables skipped for missing columns show the in-app notice.
- Database tables `{client_id}_ta` and `{client_id}_daily_df` exist after a successful run (created automatically).

If detection or IDs are wrong, fix mappings in `CLIENT_CONFIGS` and redeploy (Step 5). If wages or OT thresholds are wrong, edit `config.json` (Step 4 or the Parameters screen). Reprocessing a pay date clears annotations for that date.

---

## Step 7 — Go live

- Invite remaining users into the same Cognito group.
- Confirm they can open prior processed periods from the pay-period list.
- Optional: add location overrides as additional sites come online.
- Optional: keep a waiver file current under `clients/{client_id}/waiver/` (one file per client, not per pay date).

---

## What you do **not** provision per client

| Resource | Why |
|----------|-----|
| New S3 bucket | One bucket; data lives under `clients/{client_id}/`. |
| New Lambda | Same `analytics-backend` / `-dev` for everyone. |
| New API Gateway | Same API URL; `client_id` comes from the Cognito group and request body. |
| New Aurora cluster or manual tables | `{client_id}_ta` and `{client_id}_daily_df` are created on first successful process. |

---

## Delivery checklist

**From the client (Step 1)**

- [ ] Sample TA export (Excel), one pay period
- [ ] Sample payroll/WFN export, same pay period
- [ ] Waiver sample or written “we do not use waivers”
- [ ] Extra TA samples if they have multiple punch-export formats
- [ ] Anchor pay date (`YYYY-MM-DD`)
- [ ] Payroll and time system names
- [ ] Completed global business rules (and location overrides, if any)
- [ ] Primary payroll/HR contact

**Integrator work (Steps 2–5)**

- [ ] `client_id` chosen (Cognito group = S3 prefix = `CLIENT_CONFIGS` key)
- [ ] `CLIENT_CONFIGS` entry: detection, mappings, `anchor_pay_date`
- [ ] Cognito group + at least one pilot user
- [ ] `config.json` uploaded to `clients/{client_id}/config.json`
- [ ] Lambda deployed with the new mapping

**Pilot (Step 6)**

- [ ] Pilot user can sign in and load Parameters from S3 (not zeros)
- [ ] One pay period processes end-to-end
- [ ] IDs match across TA and payroll
- [ ] Expected payroll tables populated; skipped tables explained in the UI

---

*Internal reference: standard field names align with `TA_TARGET_SCHEMA` and `WFN_TARGET_SCHEMA` in `lambda-backend/client_config.py`. Payroll minimum fields and per-table requirements align with `WFN_CORE_SCHEMA` / `WFN_BLOCK_REQUIREMENTS` in `lambda-backend/wfn/wfn_capabilities.py`. S3 layout: `lambda-backend/docs/s3_structure`.*
