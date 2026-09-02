import pandas as pd


def same_date_as_prev(df):
    return df["Date"] == df["Prev Date"]


def break_less_than_30(df):
    return (
        (df["Break Time (min)"] < 30)
        & (df["Hours Worked Shift"] >= 5)
        & df["Is Break?"]
    )  # df["Is Break?"] ignores midnight punches and first punches of shift


def first_meal_break(df):
    return (
        (df["Hours Worked Shift"] > 6)
        & df["New Shift?"]
        & (df["Punch Length (hrs)"] > 5)
    )


def waiver_on_file(df):
    # Explicitly check for both the string "Yes" and the boolean True. As we are migrating that column to boolean.
    mask = (df["Waiver on File?"] == "Yes") | (df["Waiver on File?"] == True)
    return mask


def split_shift(df):
    # Boolean: By law, if break greater than 60 minutes, it may be a split shift
    split_shift_60 = df["Break Time (min)"] > 60
    # Boolean: And if break less than 800 minutes, it may be a split shift
    split_shift_long_check = df["Break Time (min)"] < 800
    # Boolean: Was anyone paid split shift below min wage
    split_min_wage_check = df["Split Paid ($)"] < df["Split at Min Wage ($)"]

    split_shift = (
        split_shift_60
        & split_shift_long_check
        & split_min_wage_check
        & same_date_as_prev(df)
    )
    return split_shift


def did_not_break_new(df):
    # LB 1/21/26: If the punch is greater than 5 hours, you get a credit unless the shift
    # is 6 hours or less and there is a waiver on file.

    mask = (
        df["New Shift?"]  # is first punch of shift
        & df["Is New Punch?"]  # exclude midnight punches
        & (df["Punch Length (hrs)"] > 5)  # stapled punch length
        & ~(
            (df["Hours Worked Shift"] <= 6) & waiver_on_file(df)
        )  # exclude bonafied waived
        & (
            (df["Hours Worked Shift"] <= 6) & (df["Hours Worked Shift"] > 5)
        )  # prints only bet 5 and 6
    )
    return mask


def short_shift_warning(df):
    return (df["Hours Worked Shift"] > 0) & (df["Hours Worked Shift"] < 4.0)


# Workforce Manager intake columns used only for RTP suppression.
# Captured onto VOLUNTEERED_SHORT_SHIFT_COL before schema prune.
OUT_EXC_COL = "Out Exc"
COMMENTS_TEXT_COL = "Comments Text"
VOLUNTEERED_SHORT_SHIFT_COL = "Volunteered Short Shift"
_OUT_EXC_SHORT_SHIFT = "short shift"
_COMMENTS_VOLUNTEERED = "volunteered"


def volunteered_short_shift_punch(df):
    """Punch-level WFM exception: Out Exc is Short Shift and Comments Text is Volunteered.

    Prefers the stamped processing flag (raw Excel columns are dropped after intake).
    Missing columns → no suppression (Time and Attendance files, older WFM exports).
    """
    if VOLUNTEERED_SHORT_SHIFT_COL in df.columns:
        return df[VOLUNTEERED_SHORT_SHIFT_COL].fillna(False).astype(bool)

    if OUT_EXC_COL not in df.columns or COMMENTS_TEXT_COL not in df.columns:
        return pd.Series(False, index=df.index)

    out_exc = df[OUT_EXC_COL].astype(str).str.strip().str.casefold()
    comments = df[COMMENTS_TEXT_COL].astype(str).str.strip().str.casefold()
    return (out_exc == _OUT_EXC_SHORT_SHIFT) & (comments == _COMMENTS_VOLUNTEERED)


def volunteered_short_shift(df):
    """True for every punch in a shift that has a volunteered short-shift exception.

    RTP is shift-level (Hours Worked Shift); Out Exc lives on the clock-out punch,
    so the exception is expanded to the whole shift.
    """
    punch_flag = volunteered_short_shift_punch(df)
    if df.empty or "Shift Number" not in df.columns or "ID" not in df.columns:
        return punch_flag
    return (
        punch_flag.groupby([df["ID"], df["Shift Number"]], dropna=False)
        .transform("any")
        .astype(bool)
    )


def did_not_break_new_all(df):
    # For anomalies table only
    mask = (
        df["New Shift?"]  # is first punch of shift
        & df["Is New Punch?"]  # exclude midnight punches
        & (df["Punch Length (hrs)"] > 5)  # stapled punch length
        & ~(
            (df["Hours Worked Shift"] <= 6) & waiver_on_file(df)
        )  # exclude bonafied waived
    )
    return mask


def non_zero_var(df):
    return df["Variance"] != 0


def zero_rows_ot_dt(df):
    mask = (
        (df["OT_Hours_Pay_Period"] == 0)
        & (df["OT_Hours_Paid"] == 0)
        & (df["DT_Hours_Pay_Period"] == 0)
        & (df["DT_Hours_Paid"] == 0)
    )
    return mask


def unique_ids_datetime(df):
    # Keeps first unique pair ID + Date. Note that all Date has been normalized to midnight when adding date helper cols.
    return ~df.duplicated(subset=["ID", "Date"])


def unique_ids(df):
    # Returns a Boolean mask that keeps the first occurrence of every unique ID and filters out duplicates.
    return ~df.duplicated(subset=["ID"])


def over_twelve(df):
    mask = unique_ids_datetime(df) & df["12hr Credit Due"]
    return mask


def check_consec(df):
    return df["Consec_OT_Hours"] > 0


def OT_var_mask(df):
    mask = df["OT_Variance_(hrs)"].abs() >= 0.01
    return mask


def DT_var_mask(df):
    mask = df["DT_Variance_(hrs)"].abs() >= 0.01
    return mask
