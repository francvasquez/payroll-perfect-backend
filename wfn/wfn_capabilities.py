"""
Maps each WFN results block (results.wfn keys) to intake columns required after normalization.

Used for partial payroll intake: if the client file is missing columns for a block,
that block is skipped in wfn_process.py, returns [] in results.wfn, and is listed in
summary.wfn_exceptions so the React payroll tab can explain what was not run.

Column names are standard Title Case labels (post-normalization), not ADP export codes.
"""

WFN_CORE_SCHEMA = [
    "IDX",
    "Payroll Name",
    "Pay Date",
    "Location",
]

WFN_RROP_COLUMNS = [
    "Regular Hours",
    "Overtime Hours",
    "Double Time Hours",
    "Regular Earnings Total",
    "Overtime Earnings",
    "Misc FLSA Earnings",
    "Bonus Earnings",
    "Commission Earnings",
    "Auto Gratuity Earnings",
    "Restricted Service Charge Earnings",
    "Bellman Service Charge Earnings",
]

WFN_BLOCK_ORDER = [
    "overtime_checks_variances",
    "doubletime_checks_variances",
    "break_credit_variances",
    "rest_credit_variances",
    "sick_credit_variances",
    "flsa_check",
    "min_wage_check",
    "non_active_check",
]

WFN_BLOCK_REQUIREMENTS = {
    "overtime_checks_variances": WFN_RROP_COLUMNS,
    "doubletime_checks_variances": WFN_RROP_COLUMNS + ["Double Time Earnings"],
    "break_credit_variances": WFN_RROP_COLUMNS
    + [
        "Break Credit Hours",
        "Break Credit Earnings",
        "Regular Rate Paid",
    ],
    "rest_credit_variances": WFN_RROP_COLUMNS
    + [
        "Rest Credit Hours",
        "Rest Credit Earnings",
        "Regular Rate Paid",
    ],
    "sick_credit_variances": WFN_RROP_COLUMNS
    + [
        "FLSA Status",
        "Regular Rate Paid",
        "Sick Pay Hours",
        "Sick Pay Earnings",
    ],
    "flsa_check": [
        "FLSA Status",
        "Regular Rate Paid",
        "Position Status",
    ],
    "min_wage_check": [
        "Position Status",
        "FLSA Status",
        "Regular Hours",
        "Regular Earnings Total",
        "Regular Rate Paid",
        "Sick Pay Earnings",
        "Vacation Earnings",
    ],
    "non_active_check": [
        "Position Status",
        "Regular Hours",
        "Job Description",
        "Hire Date",
        "Vacation Hours",
        "Termination Date",
    ],
}

RROP_DEPENDENT_BLOCKS = {
    "overtime_checks_variances",
    "doubletime_checks_variances",
    "break_credit_variances",
    "rest_credit_variances",
    "sick_credit_variances",
}


def assess_wfn_blocks(df_columns):
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
    return all(c in df_columns for c in WFN_RROP_COLUMNS)
