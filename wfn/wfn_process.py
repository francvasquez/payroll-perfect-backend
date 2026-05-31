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

MinE = 100


def process_data_wfn(
    df,
    client_params,
    wfn_system_config,
    min_wage,
    state_min_wage,
    pay_periods_per_year,
    pay_date,
    skip_intake_prep=False,
):
    ######### DF CLEANUP AND PREP #################

    # Normalization always runs exactly once per file:
    #   - Standard upload: raw Excel → normalize here.
    #   - Multi-period intake: file_processor already normalized and filtered
    #     to this pay period (_prepare_wfn_for_discovery in discover_handler.py).
    #     skip_intake_prep=True skips a second pass, which would duplicate columns
    #     (e.g. ADP CO. → Location mapped twice) and break downstream logic.
    if not skip_intake_prep:
        df = utility.normalize_client_data(df, wfn_system_config)
    else:
        df = df.copy()  # already normalized on the intake pass; avoid mutating the slice

    missing_core = [col for col in WFN_CORE_SCHEMA if col not in df.columns]
    if missing_core:
        logger.info(f"Columns in wfn dataframe post normalization: {list(df.columns)}")
        error_msg = f"CRITICAL: Missing required core columns: {missing_core}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Row drops follow the same first-pass rule as normalization (see above).
    if not skip_intake_prep:
        df = utility.drop_rows(df, wfn_system_config)
    df = utility.keep_available_schema_columns(df, WFN_TARGET_SCHEMA)
    df = utility.to_pandas_datetime(df, "Pay Date")

    is_valid, msg = utility.validate_wfn_pay_date(df, pay_date)
    if not is_valid:
        raise AppError(msg, status_code=422)

    enabled_blocks, wfn_exceptions = assess_wfn_blocks(df.columns)
    logger.info(f"WFN enabled blocks: {sorted(enabled_blocks)}")
    if wfn_exceptions:
        logger.info(f"WFN restricted blocks: {wfn_exceptions}")

    locations_config = client_params.get("locations", {})

    ######### SHARED RROP INPUTS (OT, DT, BREAK, REST, SICK) #################

    if rrop_inputs_present(df.columns):
        df["Base Rate"] = (df["Regular Earnings Total"] / df["Regular Hours"]).round(4)

        df["Non-Disc Earnings"] = (
            (
                df["Bellman Service Charge Earnings"]
                + df["Restricted Service Charge Earnings"]
                + df["Auto Gratuity Earnings"]
                + df["Commission Earnings"]
                + df["Bonus Earnings"]
            )
            > 0
        ).map({True: "YES", False: ""})

        df["Total Non Discretionary Wage"] = (
            df["Misc FLSA Earnings"]
            + df["Bellman Service Charge Earnings"]
            + df["Restricted Service Charge Earnings"]
            + df["Auto Gratuity Earnings"]
            + df["Commission Earnings"]
            + df["Bonus Earnings"]
        )

        df["Regular Rate of Pay for Non Discretionary Wages"] = df[
            "Total Non Discretionary Wage"
        ] / (
            df["Regular Hours"]
            + df["Overtime Hours"]
            + df["Double Time Hours"]
        )

        df["OT for Non Discretionary Income"] = (
            df["Regular Rate of Pay for Non Discretionary Wages"] * 0.5
        )

        df["RROP"] = df["Base Rate"] + (
            df["Total Non Discretionary Wage"]
            / (df["Regular Hours"] + df["Overtime Hours"] + df["Double Time Hours"])
        )

    ######### OVERTIME (results.wfn.overtime_checks_variances) #################

    if "overtime_checks_variances" in enabled_blocks:
        df["1.5x OT rate based on straight hourly rate"] = df["Base Rate"] * 1.5
        df["1.5x OT Rate"] = (
            df["1.5x OT rate based on straight hourly rate"]
            + df["OT for Non Discretionary Income"]
        )
        df["1.5 OT Worked"] = df["Overtime Hours"]
        df["1.5 OT Earnings Due"] = (df["1.5x OT Rate"] * df["1.5 OT Worked"]).round(2)
        df["Actual Pay Check"] = df["Overtime Earnings"]
        df["Variance"] = (df["Actual Pay Check"] - df["1.5 OT Earnings Due"]).round(2)

    ######### DOUBLE TIME (results.wfn.doubletime_checks_variances) #################

    if "doubletime_checks_variances" in enabled_blocks:
        df["Double Time Rate"] = 2 * (
            df["Base Rate"] + df["OT for Non Discretionary Income"]
        )
        df["Double Time Due"] = (
            df["Double Time Hours"] * df["Double Time Rate"]
        ).round(2)
        df["Actual Pay Check Dble"] = df["Double Time Earnings"]
        df["Variance Dble"] = (
            df["Actual Pay Check Dble"] - df["Double Time Due"]
        ).round(2)

    ######### BREAK CREDIT (results.wfn.break_credit_variances) #################

    if "break_credit_variances" in enabled_blocks:
        df["Break Credit Due"] = (df["RROP"] * df["Break Credit Hours"]).round(2)
        df["Actual Pay BrkCrd"] = df["Break Credit Earnings"]
        df["Variance BrkCrd"] = (
            df["Actual Pay BrkCrd"] - df["Break Credit Due"]
        ).round(2)
        df["Break Credit Due / Break Credit Hours"] = (
            df["Break Credit Due"] / df["Break Credit Hours"]
        ).round(2)

    ######### REST CREDIT (results.wfn.rest_credit_variances) #################

    if "rest_credit_variances" in enabled_blocks:
        df["Rest Credit Due"] = (df["RROP"] * df["Rest Credit Hours"]).round(2)
        df["Actual Pay RestCrd"] = df["Rest Credit Earnings"]
        df["Variance RestCrd"] = (
            df["Actual Pay RestCrd"] - df["Rest Credit Due"]
        ).round(2)
        df["Rest Credit Due / Rest Credit Hours"] = (
            df["Rest Credit Due"] / df["Rest Credit Hours"]
        ).round(2)

    ######### SICK CREDIT (results.wfn.sick_credit_variances) #################

    if "sick_credit_variances" in enabled_blocks:
        df["Sick Credit Hours"] = df["Sick Pay Hours"]
        df["RROP Sick"] = np.where(
            df["FLSA Status"] == "E",
            df["Regular Rate Paid"] / (10 * 8),
            df["RROP"],
        )
        df["Sick Credit Due"] = (df["Sick Credit Hours"] * df["RROP Sick"]).round(2)
        df["Sick Paid"] = df["Sick Pay Earnings"]
        df["Variance Sick"] = (df["Sick Paid"] - df["Sick Credit Due"]).round(2)
        df["Sick Credit Due / Sick Credit Hours"] = (
            df["Sick Credit Due"] / df["Sick Credit Hours"]
        ).round(2)

    ######### MIN WAGE / FLSA HELPERS #################

    needs_min_wage_helpers = (
        "min_wage_check" in enabled_blocks or "flsa_check" in enabled_blocks
    )
    if needs_min_wage_helpers and "Base Rate" not in df.columns:
        if "Regular Earnings Total" in df.columns and "Regular Hours" in df.columns:
            df["Base Rate"] = (df["Regular Earnings Total"] / df["Regular Hours"]).round(4)

    ######### LOCATION OVERRIDE COLUMNS (min wage check) #################

    if "min_wage_check" in enabled_blocks:
        df["Min Wage"] = utility.apply_override_else_global(
            df, "Location", "min_wage", min_wage, locations_config
        )
        df["Cal Min Wage"] = utility.apply_override_else_global(
            df, "Location", "state_min_wage", state_min_wage, locations_config
        )
        df["Pay Periods per Year"] = utility.apply_override_else_global(
            df,
            "Location",
            "pay_periods_per_year",
            pay_periods_per_year,
            locations_config,
        )
        df["Min Wage 40"] = (df["Cal Min Wage"] * 40 * 52 * 2) / df["Pay Periods per Year"]

    ######### FLSA CHECK (results.wfn.flsa_check) #################

    if "flsa_check" in enabled_blocks:
        df["FLSA Check"] = np.where(
            (df["Regular Rate Paid"] < MinE) & (df["FLSA Status"] == "E"),
            "CHECK",
            "",
        )

    ######### MINIMUM WAGE CHECK (results.wfn.min_wage_check) #################

    if "min_wage_check" in enabled_blocks:
        df["Minimum Wage"] = np.where(
            (df["Position Status"] == "Leave"),
            "",
            np.where(
                (df["FLSA Status"] == "N")
                & (df["Base Rate"].round(2) >= df["Min Wage"]),
                "",
                np.where(
                    (df["FLSA Status"] == "E")
                    & (
                        df["Regular Rate Paid"]
                        + df["Sick Pay Earnings"]
                        + df["Vacation Earnings"]
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
            (df["Regular Hours"] > 0)
            & (
                (df["Position Status"] == "Terminated")
                | (df["Position Status"] == "Leave")
            ),
            "CHECK",
            "",
        )

    return df, wfn_exceptions
