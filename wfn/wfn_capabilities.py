"""
Maps each WFN results block (results.wfn keys) to intake columns required after normalization.

Used for partial payroll intake: if the client file is missing columns for a block,
that block is skipped in wfn_process.py, returns [] in results.wfn, and is listed in
summary.wfn_exceptions so the React payroll tab can explain what was not run.

To change behavior:
  - Add/remove a column from a block's list → that block won't run unless all listed
    columns exist post-normalization (see client_config mappings).
  - Add a new block → also add to WFN_BLOCK_ORDER, wfn_process.py logic, results.py
    (_build_wfn_results), and ResultsStep WFN_BLOCK_LABELS.
"""

# ---------------------------------------------------------------------------
# WFN_CORE_SCHEMA — required for ANY payroll processing
# ---------------------------------------------------------------------------
# These columns must exist after normalize_client_data or the entire WFN file fails.
# They identify the employee, pay period, and location (for parameter overrides).
#
# Modification:
#   - Adding a column here makes onboarding stricter (more clients will fail intake).
#   - Removing a column allows thinner files but may break pay-date validation or
#     location-based min wage logic in wfn_process.py.
# ---------------------------------------------------------------------------
WFN_CORE_SCHEMA = [
    "IDX",
    "Payroll Name",
    "PAY DATE",
    "Location",
]

# ---------------------------------------------------------------------------
# WFN_RROP_COLUMNS — shared inputs for regular-rate-of-pay (RROP) variance blocks
# ---------------------------------------------------------------------------
# Used by overtime, doubletime, break, rest, and sick variance checks. When all of
# these are present, wfn_process.py computes Base Rate, non-discretionary totals,
# and RROP once before running those blocks.
#
# Modification:
#   - Removing a column from this list allows clients to skip non-discretionary
#     earnings and still run OT/DT/break/rest/sick blocks (RROP math may be less accurate).
#   - Adding a column here disables all five RROP blocks for clients who don't
#     export that earning type — prefer adding to a single block instead.
# ---------------------------------------------------------------------------
WFN_RROP_COLUMNS = [
    "REG",
    "OT",
    "DBLTIME HRS",
    "Regular Earnings Total",
    "Overtime Earnings Total",
    "A_MISC ADJUST_flsa earnings",
    "B_Bonus_Additional Earnings",
    "C_Ee Commission_Additional Earnings",
    "E_Auto Gratuities_Additional Earnings",
    "X_RESTR SVC CHG_Additional Earnings",
    "Y_BELLMANSVCCHG_Additional Earnings",
]

# ---------------------------------------------------------------------------
# WFN_BLOCK_ORDER — stable order for logging, summary, and results JSON keys
# ---------------------------------------------------------------------------
# Modification: order only affects display/logging, not calculations. New blocks
# should be appended in the position you want them to appear in documentation.
# ---------------------------------------------------------------------------
WFN_BLOCK_ORDER = [
    "overtime_checks_variances",  # Overtime RROP vs Actual Paid
    "doubletime_checks_variances",  # Doubletime RROP vs Actual Paid
    "break_credit_variances",  # Break Credit RROP vs Actual Paid
    "rest_credit_variances",  # Rest Credit RROP vs Actual Paid
    "sick_credit_variances",  # Sick RROP vs Actual Paid
    "flsa_check",  # FLSA Check
    "min_wage_check",  # Minimum Wage Check
    "non_active_check",  # Non-Active Check
]

# ---------------------------------------------------------------------------
# WFN_BLOCK_REQUIREMENTS — per-table intake columns (post-normalization names)
# ---------------------------------------------------------------------------
# Keys match results.wfn and generate_results / React payroll tab.
# If any required column is missing, the block is disabled ([]) and listed in
# summary.wfn_exceptions as "Missing col1, col2, ...".
# ---------------------------------------------------------------------------
WFN_BLOCK_REQUIREMENTS = {
    # --- overtime_checks_variances ---
    # UI: "Overtime RROP vs Actual Paid"
    # Compares 1.5× OT earnings due (RROP-based) vs overtime actually paid on the check.
    # Modification: dropping a WFN_RROP column here allows OT check without full bonus/
    # commission columns; adding columns requires more fields from the payroll export.
    "overtime_checks_variances": WFN_RROP_COLUMNS,
    # --- doubletime_checks_variances ---
    # UI: "Doubletime RROP vs Actual Paid"
    # Compares double-time due (2× rate) vs D_Double Time_Additional Earnings paid.
    # Modification: remove "D_Double Time_Additional Earnings" only if you change
    # wfn_process.py to use a different paid-DT column; otherwise block stays disabled.
    "doubletime_checks_variances": WFN_RROP_COLUMNS
    + ["D_Double Time_Additional Earnings"],
    # --- break_credit_variances ---
    # UI: "Break Credit RROP vs Actual Paid"
    # Compares break credit due (RROP × break hours) vs break credit earnings paid.
    # Modification: "Regular Rate Paid" is for display in the table; removing it only
    # affects output columns, not the variance math (still needs break hours/earnings).
    "break_credit_variances": WFN_RROP_COLUMNS
    + [
        "J_Break Credits_Additional Hours",
        "J_Break Credits_Additional Earnings",
        "Regular Rate Paid",
    ],
    # --- rest_credit_variances ---
    # UI: "Rest Credit RROP vs Actual Paid"
    # Compares rest credit due vs RC_Rest Credit_Earnings paid.
    # Modification: clients without rest credits should omit these columns; block
    # will be skipped automatically with a wfn_exceptions message.
    "rest_credit_variances": WFN_RROP_COLUMNS
    + [
        "RC - Rest Credit Hours",
        "RC_Rest Credit_Earnings",
        "Regular Rate Paid",
    ],
    # --- sick_credit_variances ---
    # UI: "Sick RROP vs Actual Paid"
    # Compares sick pay due (uses FLSA Code for exempt RROP) vs sick earnings paid.
    # Modification: requires FLSA Code for exempt sick formula; removing it disables
    # this block for all clients until wfn_process sick logic is changed.
    "sick_credit_variances": WFN_RROP_COLUMNS
    + [
        "FLSA Code",
        "Regular Rate Paid",
        "S_Sick Pay_Hours",
        "S_Sick Pay_Earnings",
    ],
    # --- flsa_check ---
    # UI: "FLSA Check"
    # Flags exempt employees whose regular rate paid is below threshold (MinE).
    # Does not need full RROP columns — only FLSA status and rate.
    # Modification: loosening this list enables FLSA check on thinner payroll files;
    # adding columns (e.g. REG) is unnecessary unless you change the flsa mask logic.
    "flsa_check": [
        "FLSA Code",
        "Regular Rate Paid",
        "Position Status",
    ],
    # --- min_wage_check ---
    # UI: "Minimum Wage Check"
    # Flags non-exempt and exempt employees below location/global min wage rules.
    # Uses sick and vacation earnings in the exempt comparison.
    # Modification: needs Location in WFN_CORE_SCHEMA for overrides; removing
    # S_Sick Pay_Earnings or V_Vacation_Earnings changes who gets flagged for exempt.
    "min_wage_check": [
        "Position Status",
        "FLSA Code",
        "REG",
        "Regular Earnings Total",
        "Regular Rate Paid",
        "S_Sick Pay_Earnings",
        "V_Vacation_Earnings",
    ],
    # --- non_active_check ---
    # UI: "Non-Active Check"
    # Flags terminated/leave employees who still have regular hours on the check.
    # Modification: removing HIREDATE or Termination Date only affects displayed
    # columns in results; REG and Position Status are required for the check itself.
    "non_active_check": [
        "Position Status",
        "REG",
        "Job Title Description",
        "HIREDATE",
        "V_Vacation_Hours",
        "Termination Date",
    ],
}

# Blocks that depend on WFN_RROP_COLUMNS being fully present (see wfn_process.py).
# Used for documentation; rrop_inputs_present() is the runtime check.
RROP_DEPENDENT_BLOCKS = {
    "overtime_checks_variances",
    "doubletime_checks_variances",
    "break_credit_variances",
    "rest_credit_variances",
    "sick_credit_variances",
}


def assess_wfn_blocks(df_columns):
    """
    Compares normalized WFN column names to WFN_BLOCK_REQUIREMENTS.

    Returns:
        enabled_blocks: set of result keys that wfn_process.py should run
        wfn_exceptions: { block_key: "Missing col1, col2, ..." } for disabled blocks

    Modification: changing WFN_BLOCK_REQUIREMENTS automatically updates which blocks
    run and what appears in summary.wfn_exceptions — no change needed here unless
    you add custom rules (e.g. enable block when ANY of several columns exist).
    """
    cols = set(df_columns)
    enabled = set()
    exceptions = {}

    for block_key in WFN_BLOCK_ORDER:
        required = WFN_BLOCK_REQUIREMENTS[block_key]
        missing = [c for c in required if c not in cols]
        if missing:
            exceptions[block_key] = f"Missing {', '.join(missing)}"
        else:
            enabled.add(block_key)

    return enabled, exceptions


def rrop_inputs_present(df_columns):
    """
    True when all WFN_RROP_COLUMNS exist — wfn_process.py then computes shared RROP fields.

    Modification: if you split RROP (e.g. OT without bonuses), shrink WFN_RROP_COLUMNS
    and adjust which blocks list WFN_RROP_COLUMNS vs a smaller shared list.
    """
    return all(c in df_columns for c in WFN_RROP_COLUMNS)
