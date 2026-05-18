import numpy as np
import utility
from client_config import WFN_TARGET_SCHEMA
from exceptions import AppError
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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

    # 1. Normalization: Columns Rename, Transform & Drop
    df = utility.normalize_client_data(df, wfn_system_config)

    # 2. Validation: Check if all neccesary columns post-mapping are present, if not stop processing.
    missing = [col for col in WFN_TARGET_SCHEMA if col not in df.columns]
    if missing:
        logger.info(f"Columns in wfn dataframe post normalization: {list(df.columns)}")
        error_msg = f"CRITICAL: Missing required columns: {missing}"
        logger.error(error_msg)  # CloudWatch Logs trigger alerts if set up
        raise ValueError(error_msg)  # Raise stops execution in Lambda

    # 3. Re-order 'Core' columns are always first (makes the DB readable)
    other_cols = [col for col in df.columns if col not in WFN_TARGET_SCHEMA]
    df = df[WFN_TARGET_SCHEMA + other_cols]

    # 4. Drops rows that are not punches base on client configuration
    df = utility.drop_rows(df, wfn_system_config)

    # 5. Assure timestamps are in Panda's datetime format
    df = utility.to_pandas_datetime(df, "PAY DATE")

    # 6. Ensure inputed Pay Date matches the contents of the file
    is_valid, msg = utility.validate_wfn_pay_date(df, pay_date)
    if not is_valid:
        raise AppError(msg, status_code=422)

    # Variables - extract loc config
    MinE = 100
    locations_config = client_params.get("locations", {})  ## overrides

    # Aux Cols
    df["Base Rate"] = (df["Regular Earnings Total"] / df["REG"]).round(4)
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
    df["Total Non Discretionary Wage"] = (
        df["A_MISC ADJUST_flsa earnings"]
        + df["Y_BELLMANSVCCHG_Additional Earnings"]
        + df["X_RESTR SVC CHG_Additional Earnings"]
        + df["E_Auto Gratuities_Additional Earnings"]
        + df["C_Ee Commission_Additional Earnings"]
        + (df["B_Bonus_Additional Earnings"])
    )
    df["Regular Rate of Pay for Non Discretionary Wages"] = df[
        "Total Non Discretionary Wage"
    ] / (df["REG"] + df["OT"] + df["DBLTIME HRS"])
    df["OT for Non Discretionary Income"] = (
        df["Regular Rate of Pay for Non Discretionary Wages"] * 0.5
    )
    df["1.5x OT rate based on straight hourly rate"] = df["Base Rate"] * 1.5
    df["1.5x OT Rate"] = (
        df["1.5x OT rate based on straight hourly rate"]
        + df["OT for Non Discretionary Income"]
    )
    df["1.5 OT Worked"] = df["OT"]
    df["1.5 OT Earnings Due"] = (df["1.5x OT Rate"] * df["1.5 OT Worked"]).round(2)
    df["Actual Pay Check"] = df["Overtime Earnings Total"]
    df["Variance"] = (df["Actual Pay Check"] - df["1.5 OT Earnings Due"]).round(2)

    # Double Time
    df["Double Time Rate"] = 2 * (
        df["Base Rate"] + df["OT for Non Discretionary Income"]
    )
    df["Double Time Hours"] = df["DBLTIME HRS"]
    df["Double Time Due"] = (df["Double Time Hours"] * df["Double Time Rate"]).round(2)
    df["Actual Pay Check Dble"] = df["D_Double Time_Additional Earnings"]
    df["Variance Dble"] = (df["Actual Pay Check Dble"] - df["Double Time Due"]).round(2)

    # Break Credit
    df["Break Credit Hours"] = df["J_Break Credits_Additional Hours"]
    df["RROP"] = df["Base Rate"] + (
        df["Total Non Discretionary Wage"] / (df["REG"] + df["OT"] + df["DBLTIME HRS"])
    )
    df["Break Credit Due"] = (df["RROP"] * df["Break Credit Hours"]).round(2)
    df["Actual Pay BrkCrd"] = df["J_Break Credits_Additional Earnings"]
    df["Variance BrkCrd"] = (df["Actual Pay BrkCrd"] - df["Break Credit Due"]).round(2)
    df["Break Credit Due / Break Credit Hours"] = (
        df["Break Credit Due"] / df["Break Credit Hours"]
    ).round(2)

    # Rest Credit
    df["Rest Credit Hours"] = df["RC - Rest Credit Hours"]
    df["Rest Credit Due"] = (df["RROP"] * df["Rest Credit Hours"]).round(2)
    df["Actual Pay RestCrd"] = df["RC_Rest Credit_Earnings"]
    df["Variance RestCrd"] = (df["Actual Pay RestCrd"] - df["Rest Credit Due"]).round(2)
    df["Rest Credit Due / Rest Credit Hours"] = (
        df["Rest Credit Due"] / df["Rest Credit Hours"]
    ).round(2)

    # Sick Credit
    df["Sick Credit Hours"] = df["S_Sick Pay_Hours"]
    df["RROP Sick"] = np.where(
        df["FLSA Code"] == "E", df["Regular Rate Paid"] / (10 * 8), df["RROP"]
    )
    df["Sick Credit Due"] = (df["Sick Credit Hours"] * df["RROP Sick"]).round(2)
    df["Sick Paid"] = df["S_Sick Pay_Earnings"]
    df["Variance Sick"] = (df["Sick Paid"] - df["Sick Credit Due"]).round(2)
    df["Sick Credit Due / Sick Credit Hours"] = (
        df["Sick Credit Due"] / df["Sick Credit Hours"]
    ).round(2)

    ## OVERRIDE COLUMN CREATION ###
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
        df, "Location", "pay_periods_per_year", pay_periods_per_year, locations_config
    )
    # Is there a location based minimum wage 40? Else take global "min_wage_40"
    df["Min Wage 40"] = (df["Cal Min Wage"] * 40 * 52 * 2) / df["Pay Periods per Year"]
    ####

    # FLSA, Min Wage, Non-Active Checks

    df["FLSA Check"] = np.where(
        (df["Regular Rate Paid"] < MinE) & (df["FLSA Code"] == "E"), "CHECK", ""
    )
    df["Minimum Wage"] = np.where(
        (df["Position Status"] == "Leave"),
        "",
        np.where(
            (df["FLSA Code"] == "N") & (df["Base Rate"].round(2) >= df["Min Wage"]),
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
    df["Non-Active"] = np.where(
        (df["REG"] > 0)
        & (
            (df["Position Status"] == "Terminated") | (df["Position Status"] == "Leave")
        ),
        "CHECK",
        "",
    )

    return df
