# WFN Payroll Data Requirements — Client Expectations

This document explains which payroll (WFN) data enables each table in the **Payroll** tab of the Payroll Health Check Dashboard. Use it when onboarding clients who may export **partial** payroll files.

Column names below are **after normalization** (standard Title Case labels the system uses once the file is loaded). Raw export headers may differ by payroll system; for ADP, see [ADP mapping](#adp-example-mapping) at the end.

---

## Always required (file will not process without these)

These four fields must be present or **the entire payroll upload fails** at intake:

| Column | Purpose |
|--------|---------|
| **IDX** | Employee identifier (must align with Time & Attendance `ID`) |
| **Payroll Name** | Employee name on reports |
| **Pay Date** | Pay date on each row (must match the pay date selected in the app) |
| **Location** | Company/location code (used for location-based minimum wage overrides) |

If any of these are missing, the client will not receive any payroll tables.

---

## Shared RROP bundle (five variance tables)

Five tables share the same **regular rate of pay (RROP)** inputs. If **any** column in this bundle is missing, **all five** of the following checks are disabled:

| Column | What it represents |
|--------|-------------------|
| Regular Hours | Regular hours worked |
| Overtime Hours | Overtime hours |
| Double Time Hours | Double-time hours |
| Regular Earnings Total | Regular pay dollars |
| Overtime Earnings | Overtime pay dollars |
| Misc FLSA Earnings | Misc FLSA-adjustable earnings |
| Bonus Earnings | Bonus |
| Commission Earnings | Commission |
| Auto Gratuity Earnings | Auto gratuities |
| Restricted Service Charge Earnings | Restricted service charge |
| Bellman Service Charge Earnings | Bellman/service charge |

**Tables that depend on this bundle (each may need additional columns):**

1. Overtime RROP vs Actual Paid
2. Doubletime RROP vs Actual Paid *(also needs double-time earnings — see below)*
3. Break Credit RROP vs Actual Paid *(also needs break columns — see below)*
4. Rest Credit RROP vs Actual Paid *(also needs rest columns — see below)*
5. Sick RROP vs Actual Paid *(also needs sick columns — see below)*

---

## Per-table requirements

### 1. Overtime RROP vs Actual Paid

**Requires:** entire [RROP bundle](#shared-rrop-bundle-five-variance-tables)

**What it shows:** Employees where overtime paid on the check differs from RROP-based overtime due by more than ±$0.01.

---

### 2. Doubletime RROP vs Actual Paid

**Requires:** RROP bundle **plus:**

| Column |
|--------|
| Double Time Earnings |

**What it shows:** Double-time pay variances.

---

### 3. Break Credit RROP vs Actual Paid

**Requires:** RROP bundle **plus:**

| Column |
|--------|
| Break Credit Hours |
| Break Credit Earnings |
| Regular Rate Paid |

**What it shows:** Break credit hours and earnings vs RROP-based break credit due.

---

### 4. Rest Credit RROP vs Actual Paid

**Requires:** RROP bundle **plus:**

| Column |
|--------|
| Rest Credit Hours |
| Rest Credit Earnings |
| Regular Rate Paid |

**What it shows:** Rest credit variances.

---

### 5. Sick RROP vs Actual Paid

**Requires:** RROP bundle **plus:**

| Column |
|--------|
| FLSA Status |
| Regular Rate Paid |
| Sick Pay Hours |
| Sick Pay Earnings |

**What it shows:** Sick pay variances (uses FLSA exempt vs non-exempt logic).

---

### 6. FLSA Check

**Does not require the full RROP bundle.** Only:

| Column |
|--------|
| FLSA Status |
| Regular Rate Paid |
| Position Status |

**What it shows:** Exempt employees flagged when regular rate paid is below the system threshold.

---

### 7. Minimum Wage Check

**Does not require the full RROP bundle.** Requires:

| Column |
|--------|
| Position Status |
| FLSA Status |
| Regular Hours |
| Regular Earnings Total |
| Regular Rate Paid |
| Sick Pay Earnings |
| Vacation Earnings |

**What it shows:** Employees below minimum wage rules (uses global and location-specific thresholds when configured).

---

### 8. Non-Active Check

**Requires:**

| Column |
|--------|
| Position Status |
| Regular Hours |
| Job Description |
| Hire Date |
| Vacation Hours |
| Termination Date |

**What it shows:** Terminated or on-leave employees who still have regular hours on the check.

---

## What the client sees in the app

| Situation | What they see |
|-----------|----------------|
| Required core columns missing | Upload fails with an error before results are generated. |
| Pay date on file does not match selected pay date | Modal explaining mismatch (up to 5 sample employee IDs); processing cannot continue. |
| A check’s required columns are missing | That table is **hidden**. An info alert lists which checks were skipped and which columns were missing. |
| A check ran but found zero issues | Table **still appears**: stat cards show **0**; **Show Table** displays **“No data found”**. |
| A check ran and found issues | Normal table with flagged rows. |

Skipped checks appear under **summary → wfn_exceptions** in the API response and in the blue info box on the Payroll tab.

---

## Quick scenarios (partial data)

| Client can provide… | They will typically see… |
|---------------------|-------------------------|
| Core only (IDX, name, pay date, location) | No payroll tables; all checks listed as skipped. |
| Core + FLSA / position / rate fields only | **FLSA Check**; **Minimum Wage Check** only if hours, earnings, sick, and vacation columns are also present. |
| Core + non-active fields | **Non-Active Check** only. |
| Core + full RROP bundle | OT and credit variance tables **only if** each block’s extra columns are also present; **Doubletime** only if double-time earnings column exists. |
| Full payroll export (all columns above) | All eight payroll tables (each may show zero rows if no variances are found). |

---

## ADP example mapping

For the current **demo_client / ADP** configuration, raw columns are transformed at intake:

| Raw export column | Becomes |
|-------------------|---------|
| CO. + FILE# | IDX (`CO.` + `0` + zero-padded FILE#) |
| CO. | Location |
| PAY DATE | Pay Date |
| FLSA Code | FLSA Status |
| REG | Regular Hours |
| OT | Overtime Hours |
| DBLTIME HRS | Double Time Hours |
| Overtime Earnings Total | Overtime Earnings |
| HIREDATE | Hire Date |
| Job Title Description | Job Description |
| J_Break Credits_Additional Hours | Break Credit Hours |
| J_Break Credits_Additional Earnings | Break Credit Earnings |
| RC - Rest Credit Hours | Rest Credit Hours |
| RC_Rest Credit_Earnings | Rest Credit Earnings |
| S_Sick Pay_Hours | Sick Pay Hours |
| S_Sick Pay_Earnings | Sick Pay Earnings |
| V_Vacation_Hours | Vacation Hours |
| V_Vacation_Earnings | Vacation Earnings |
| *(and other ADP earnings codes — see `ADP_WFN_COLUMN_MAPPINGS` in `client_config.py`)* |

Columns that already match standard names in the export (e.g. **Payroll Name**, **Position Status**, **Regular Earnings Total**, **Regular Rate Paid**, **Termination Date**) are kept as-is.

Detection of an ADP file requires **CO.** and **PAY DATE** in the header row (raw headers, before mapping). Other columns are renamed via `ADP_WFN_COLUMN_MAPPINGS` in `client_config.py`.

---

## Technical reference

Runtime rules are defined in:

- `lambda-backend/wfn/wfn_capabilities.py` — `WFN_CORE_SCHEMA`, `WFN_RROP_COLUMNS`, `WFN_BLOCK_REQUIREMENTS`
- `lambda-backend/client_config.py` — `WFN_TARGET_SCHEMA`, `ADP_WFN_COLUMN_MAPPINGS`, per-client `wfn_systems` mappings

When changing requirements, update `wfn_capabilities.py` and this document together.
