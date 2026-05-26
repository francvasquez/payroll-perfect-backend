# Payroll Perfect — Client Data Request

**Purpose:** Use this checklist when onboarding a new client. Collect sample exports and business rules so our team can configure the account and run a pilot pay period.

**What to send us:** One complete pay period of sample files (Excel preferred), plus the setup items in Sections 2–3 below. Redacted samples are acceptable if employee IDs stay consistent across Time & Attendance and Payroll files.

---

## 1. Payroll & time data fields

Column names below are the **standard labels** our system uses after we map your export. Your file may use different headers (e.g. ADP uses `CO.`, `FILE#`, `REG`); we will map them during setup.

### Time & Attendance (required)

All fields below are **required** for time-card processing. Employee **ID** must match payroll **IDX** (see below).

| Field | Description |
|-------|-------------|
| **ID** | Unique employee identifier (must match payroll IDX for the same person). |
| **Location** | Site or company code where the employee works. |
| **Employee** | Employee name (for display on reports). |
| **In Punch** | Clock-in date/time for each punch. |
| **Out Punch** | Clock-out date/time for each punch. |
| **Status** | Employment or punch status (e.g. active, terminated, leave). |
| **Status Date** | Date associated with the status, if applicable. |

**Also request (optional but recommended):** Meal **waiver** list — employee ID plus a flag indicating a valid meal-period waiver on file (used for California meal-break checks).

---

### Payroll / WFN (flexible)

Payroll data is **flexible**: clients do not need every field in the full list. We run only the checks that your file supports; missing sections simply disable those payroll tables (with a clear notice in the app).

#### Bare minimum (required — payroll file will not load without these)

| Field | Description |
|-------|-------------|
| **IDX** | Unique employee identifier (must match Time & Attendance ID). |
| **Payroll Name** | Employee name as shown on the payroll register. |
| **Pay Date** | Check pay date on each row (must match the pay date selected in the app). |
| **Location** | Company or location code (used for location-specific wage and rule overrides). |

#### Full payroll field list (provide what you have)

**Identifiers**

| Field | Description |
|-------|-------------|
| **IDX** | Unique employee identifier (must match Time & Attendance ID). |
| **Location** | Company or location code. |
| **Payroll Name** | Employee name on the payroll register. |
| **Pay Date** | Check pay date for the row. |

**Status and rates**

| Field | Description |
|-------|-------------|
| **FLSA Status** | Exempt vs non-exempt classification (e.g. E / N). |
| **Position Status** | Employment status (e.g. active, terminated, leave). |
| **Hire Date** | Employee hire date. |
| **Job Description** | Job title or description. |
| **Termination Date** | Termination date, if applicable. |
| **Regular Rate Paid** | Regular rate of pay used on the check (for display and sick/split-shift logic). |

**Standard hours & earnings**

| Field | Description |
|-------|-------------|
| **Regular Hours** | Regular (straight-time) hours paid in the period. |
| **Overtime Hours** | Overtime hours paid. |
| **Double Time Hours** | Double-time hours paid. |
| **Regular Earnings Total** | Total regular earnings dollars. |
| **Overtime Earnings** | Total overtime earnings dollars. |

**Additional earnings (non-discretionary & bonuses)**

| Field | Description |
|-------|-------------|
| **Misc FLSA Earnings** | Miscellaneous FLSA-related adjustable earnings. |
| **Bonus Earnings** | Bonus pay. |
| **Commission Earnings** | Commission pay. |
| **Auto Gratuity Earnings** | Auto-gratuity allocations. |
| **Restricted Service Charge Earnings** | Restricted service charge allocations. |
| **Bellman Service Charge Earnings** | Bellman or similar service charge allocations. |
| **Double Time Earnings** | Double-time earnings dollars (for variance vs calculated DT due). |

**Break, rest, sick, vacation**

| Field | Description |
|-------|-------------|
| **Break Credit Hours** | Meal break credit hours paid. |
| **Break Credit Earnings** | Meal break credit dollars paid. |
| **Rest Credit Hours** | Rest break credit hours paid. |
| **Rest Credit Earnings** | Rest break credit dollars paid. |
| **Sick Pay Hours** | Sick pay hours. |
| **Sick Pay Earnings** | Sick pay dollars. |
| **Vacation Hours** | Vacation hours (e.g. for non-active checks). |
| **Vacation Earnings** | Vacation earnings dollars. |

**Typical impact if sections are missing**

| If the client cannot provide… | Payroll tables affected |
|------------------------------|-------------------------|
| Only the **bare minimum** | Payroll variance and compliance tables are limited; time-card vs payroll OT/DT may still run if TA includes hours. |
| RROP-related hours/earnings columns | Overtime, double-time, break, rest, and sick **RROP vs actual paid** tables. |
| Break credit columns | Break credit variance table only. |
| Rest credit columns | Rest credit variance table only. |
| Sick columns | Sick credit variance table only. |
| FLSA / min wage / status columns | FLSA check, minimum wage check, and/or non-active check as applicable. |

---

## 2. System setup information

Please collect the following from the client (or their payroll/IT contact):

| Item | What to ask for | Example |
|------|-----------------|--------|
| **Anchor pay date** | One **known pay date** from their payroll calendar (YYYY-MM-DD). Used to align workdays to the correct pay period. | `2026-01-16` |
| **Payroll system name** | Vendor/product name for the **payroll register** export (WFN). | ADP Workforce Now, UKG, etc. |
| **Time system name** | Vendor/product name for the **time & attendance** punch export (TA). | ADP Time & Attendance, UKG Workforce Manager, etc. |

If they use more than one TA export format (e.g. different properties), list each format and provide a **sample file per format**.

---

## 3. Business rules (`config.json`)

These settings drive pay-period dates, minimum wage, overtime thresholds, and location overrides. Provide **global** defaults for the client, plus **per-location** overrides where rules differ by site.

### Global settings (required)

| Setting | Description | Example |
|---------|-------------|--------|
| **pay_period_length** | Length of the pay period in **days**. | `14` (biweekly) |
| **days_bet_payroll_end_and_pay_date** | Days between the **end of the work period** and the **pay date**. | `6` |
| **workweek_start** | First day of the workweek for overtime calculations. | `"Sunday"` |
| **pay_periods_per_year** | Number of pay periods per year. | `26` |
| **min_wage** | Employer minimum wage used in audits (often local contracted rate). | `17.75` |
| **state_min_wage** | State minimum wage (e.g. California). | `16.90` |
| **ot_day_max** | Daily hours after which **day-level** OT rules apply. | `8` |
| **ot_week_max** | Weekly hours after which **week-level** OT rules apply. | `40` |
| **dt_day_max** | Daily hours threshold used for **double-time** logic. | `12` |
| **number_of_consec_days_before_ot** | Consecutive days worked before consecutive-day OT rules apply. | `6` |
| **time_gap_for_new_shift** | Minimum minutes between punches to treat as a **new shift**. | `60` |
| **cba_consec_anyweek** | If `true`, consecutive-day rules can apply across workweek boundaries (CBA). If `false`, standard workweek rules only. | `false` |

### Per-location overrides (optional)

Use the **location code** that appears in payroll and time files as the key. Include only settings that **differ** from global for that site.

**Example structure:**

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
      "ot_week_max": 40,
      "cba_consec_anyweek": true
    }
  }
}
```

Any field listed under **global** may be overridden per location (same names as in the table above).

---

## 4. Delivery checklist for sales

- [ ] Sample **time & attendance** export for one pay period (Excel: `.xlsx` or `.xls`)
- [ ] Sample **payroll register** export for the **same** pay period
- [ ] Sample **waiver** file (if client uses meal waivers), or written confirmation they do not
- [ ] **Anchor pay date** (YYYY-MM-DD)
- [ ] **Payroll system** and **time system** names (and samples per format if multiple)
- [ ] Completed **global** business rules (Section 3)
- [ ] **Per-location** overrides, if any (Section 3)
- [ ] Primary contact for payroll/HR questions during setup

---

*Internal reference: field lists align with `TA_TARGET_SCHEMA` and `WFN_TARGET_SCHEMA` in `lambda-backend/client_config.py`. Payroll minimum fields align with `WFN_CORE_SCHEMA` in `lambda-backend/wfn/wfn_capabilities.py`.*
