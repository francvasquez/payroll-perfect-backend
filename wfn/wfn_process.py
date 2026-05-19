import numpy as np
import utility
from client_config import WFN_TARGET_SCHEMA
from exceptions import AppError
from wfn.wfn_capabilities import (
    WFN_CORE_SCHEMA,
    assess_wfn_blocks,
    rrop_inputs_present,
)
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Threshold used in FLSA Check (exempt employees)
MinE = 100


def process_data_wfn(
    df,
    client_params,
    wfn_system_config,
    min_wage,
    state_min_wage,
    pay_periods_per_year,
    pay_date,
):
    ######### DF CLEANUP AND PREP #################

    # 1. Normalization: column rename, transform, and drop (per client config)
    df = utility.normalize_client_data(df, wfn_system_config)

    # 2. Core validation — must have these even for partial payroll output
    missing_core = [col for col in WFN_CORE_SCHEMA if col not in df.columns]
    if missing_core:
        logger.info(f"Columns in wfn dataframe post normalization: {list(df.columns)}")
        error_msg = f"CRITICAL: Missing required core columns: {missing_core}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # 3. Drop rows per client config (e.g. blank punches, test employees)
    df = utility.drop_rows(df, wfn_system_config)

    # 4. Keep only target-schema columns that the client actually provided
    #    (optional columns may be absent — those output blocks will be skipped)
    df = utility.keep_available_schema_columns(df, WFN_TARGET_SCHEMA)

    # 5. Assure timestamps are in Panda's datetime format
    df = utility.to_pandas_datetime(df, "PAY DATE")

    # 6. Ensure inputted Pay Date matches the contents of the file
    is_valid, msg = utility.validate_wfn_pay_date(df, pay_date)
    if not is_valid:
        raise AppError(msg, status_code=422)

    # 7. Decide which payroll result blocks we can run from available columns
    #    wfn_exceptions is returned to the front-end (summary.wfn_exceptions)
    enabled_blocks, wfn_exceptions = assess_wfn_blocks(df.columns)
    logger.info(f"WFN enabled blocks: {sorted(enabled_blocks)}")
    if wfn_exceptions:
        logger.info(f"WFN restricted blocks: {wfn_exceptions}")

    # Location overrides from client config (min wage, pay periods per year, etc.)
    locations_config = client_params.get("locations", {})

    ######### SHARED RROP INPUTS (OT, DT, BREAK, REST, SICK) #################

    # Several variance blocks share regular-rate-of-pay math; compute once if possible
    if rrop_inputs_present(df.columns):
        # Base rate from regular earnings / regular hours
        df["Base Rate"] = (df["Regular Earnings Total"] / df["REG"]).round(4)

        # Flag if any non-discretionary earnings are present (display only)
        df["Non-Disc Earnings"] = (
            (
                df["Y_BELLMANSVCCHG_Additional Earnings"]
                + df["X_RESTR SVC CHG_Additional Earnings"]
                + df["E_Auto Gratuities_Additional Earnings"]
                + df["C_Ee Commission_Additional Earnings"]
                + df["B_Bonus_Additional Earnings"]
            )
            > 0
        ).map({True: "YES", False: ""})

        # Sum of all non-discretionary wages for RROP
        df["Total Non Discretionary Wage"] = (
            df["A_MISC ADJUST_flsa earnings"]
            + df["Y_BELLMANSVCCHG_Additional Earnings"]
            + df["X_RESTR SVC CHG_Additional Earnings"]
            + df["E_Auto Gratuities_Additional Earnings"]
            + df["C_Ee Commission_Additional Earnings"]
            + df["B_Bonus_Additional Earnings"]
        )

        # Regular rate of pay for non-discretionary wages
        df["Regular Rate of Pay for Non Discretionary Wages"] = df[
            "Total Non Discretionary Wage"
        ] / (df["REG"] + df["OT"] + df["DBLTIME HRS"])

        # OT premium attributable to non-discretionary income
        df["OT for Non Discretionary Income"] = (
            df["Regular Rate of Pay for Non Discretionary Wages"] * 0.5
        )

        # RROP used by break / rest / sick credit blocks
        df["RROP"] = df["Base Rate"] + (
            df["Total Non Discretionary Wage"]
            / (df["REG"] + df["OT"] + df["DBLTIME HRS"])
        )

    ######### OVERTIME (results.wfn.overtime_checks_variances) #################

    if "overtime_checks_variances" in enabled_blocks:
        df["1.5x OT rate based on straight hourly rate"] = df["Base Rate"] * 1.5
        df["1.5x OT Rate"] = (
            df["1.5x OT rate based on straight hourly rate"]
            + df["OT for Non Discretionary Income"]
        )
        df["1.5 OT Worked"] = df["OT"]
        df["1.5 OT Earnings Due"] = (
            df["1.5x OT Rate"] * df["1.5 OT Worked"]
        ).round(2)
        df["Actual Pay Check"] = df["Overtime Earnings Total"]
        df["Variance"] = (df["Actual Pay Check"] - df["1.5 OT Earnings Due"]).round(2)

    ######### DOUBLE TIME (results.wfn.doubletime_checks_variances) #################

    if "doubletime_checks_variances" in enabled_blocks:
        df["Double Time Rate"] = 2 * (
            df["Base Rate"] + df["OT for Non Discretionary Income"]
        )
        df["Double Time Hours"] = df["DBLTIME HRS"]
        df["Double Time Due"] = (
            df["Double Time Hours"] * df["Double Time Rate"]
        ).round(2)
        df["Actual Pay Check Dble"] = df["D_Double Time_Additional Earnings"]
        df["Variance Dble"] = (
            df["Actual Pay Check Dble"] - df["Double Time Due"]
        ).round(2)

    ######### BREAK CREDIT (results.wfn.break_credit_variances) #################

    if "break_credit_variances" in enabled_blocks:
        df["Break Credit Hours"] = df["J_Break Credits_Additional Hours"]
        df["Break Credit Due"] = (df["RROP"] * df["Break Credit Hours"]).round(2)
        df["Actual Pay BrkCrd"] = df["J_Break Credits_Additional Earnings"]
        df["Variance BrkCrd"] = (
            df["Actual Pay BrkCrd"] - df["Break Credit Due"]
        ).round(2)
        df["Break Credit Due / Break Credit Hours"] = (
            df["Break Credit Due"] / df["Break Credit Hours"]
        ).round(2)

    ######### REST CREDIT (results.wfn.rest_credit_variances) #################

    if "rest_credit_variances" in enabled_blocks:
        df["Rest Credit Hours"] = df["RC - Rest Credit Hours"]
        df["Rest Credit Due"] = (df["RROP"] * df["Rest Credit Hours"]).round(2)
        df["Actual Pay RestCrd"] = df["RC_Rest Credit_Earnings"]
        df["Variance RestCrd"] = (
            df["Actual Pay RestCrd"] - df["Rest Credit Due"]
        ).round(2)
        df["Rest Credit Due / Rest Credit Hours"] = (
            df["Rest Credit Due"] / df["Rest Credit Hours"]
        ).round(2)

    ######### SICK CREDIT (results.wfn.sick_credit_variances) #################

    if "sick_credit_variances" in enabled_blocks:
        df["Sick Credit Hours"] = df["S_Sick Pay_Hours"]
        # Exempt employees use a different sick RROP formula
        df["RROP Sick"] = np.where(
            df["FLSA Code"] == "E",
            df["Regular Rate Paid"] / (10 * 8),
            df["RROP"],
        )
        df["Sick Credit Due"] = (df["Sick Credit Hours"] * df["RROP Sick"]).round(2)
        df["Sick Paid"] = df["S_Sick Pay_Earnings"]
        df["Variance Sick"] = (df["Sick Paid"] - df["Sick Credit Due"]).round(2)
        df["Sick Credit Due / Sick Credit Hours"] = (
            df["Sick Credit Due"] / df["Sick Credit Hours"]
        ).round(2)

    ######### MIN WAGE / FLSA HELPERS #################

    # Min wage and FLSA checks need Base Rate even when full RROP block was not run
    needs_min_wage_helpers = (
        "min_wage_check" in enabled_blocks or "flsa_check" in enabled_blocks
    )
    if needs_min_wage_helpers and "Base Rate" not in df.columns:
        if "Regular Earnings Total" in df.columns and "REG" in df.columns:
            df["Base Rate"] = (df["Regular Earnings Total"] / df["REG"]).round(4)

    ######### LOCATION OVERRIDE COLUMNS (min wage check) #################

    if "min_wage_check" in enabled_blocks:
        # Is there a location based minimum wage? Else take global "min_wage"
        df["Min Wage"] = utility.apply_override_else_global(
            df, "Location", "min_wage", min_wage, locations_config
        )
        # Is there a location based california minimum wage? Else take global "state_min_wage"
        df["Cal Min Wage"] = utility.apply_override_else_global(
            df, "Location", "state_min_wage", state_min_wage, locations_config
        )
        # Is there a location based pay periods per year? Else take global "pay_periods_per_year"
        df["Pay Periods per Year"] = utility.apply_override_else_global(
            df,
            "Location",
            "pay_periods_per_year",
            pay_periods_per_year,
            locations_config,
        )
        # Minimum wage threshold for exempt (40-hr equivalent per pay period)
        df["Min Wage 40"] = (
            df["Cal Min Wage"] * 40 * 52 * 2
        ) / df["Pay Periods per Year"]

    ######### FLSA CHECK (results.wfn.flsa_check) #################

    if "flsa_check" in enabled_blocks:
        df["FLSA Check"] = np.where(
            (df["Regular Rate Paid"] < MinE) & (df["FLSA Code"] == "E"),
            "CHECK",
            "",
        )

    ######### MINIMUM WAGE CHECK (results.wfn.min_wage_check) #################

    if "min_wage_check" in enabled_blocks:
        df["Minimum Wage"] = np.where(
            (df["Position Status"] == "Leave"),
            "",
            np.where(
                (df["FLSA Code"] == "N")
                & (df["Base Rate"].round(2) >= df["Min Wage"]),
                "",
                np.where(
                    (df["FLSA Code"] == "E")
                    & (
                        df["Regular Rate Paid"]
                        + df["S_Sick Pay_Earnings"]
                        + df["V_Vacation_Earnings"]
                        >= df["Min Wage 40"]
                    ),
                    "",
                    "CHECK",
                ),
            ),
        )

    ######### NON-ACTIVE CHECK (results.wfn.non_active_check) #################

    if "non_active_check" in enabled_blocks:
        df["Non-Active"] = np.where(
            (df["REG"] > 0)
            & (
                (df["Position Status"] == "Terminated")
                | (df["Position Status"] == "Leave")
            ),
            "CHECK",
            "",
        )

    return df, wfn_exceptions
