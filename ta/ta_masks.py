import pandas as pd
import datetime


def same_date_as_prev(df):
    return df["Date"] == df["Prev Date"]


def same_date_as_next(df):
    return df["Date"] == df["Next Date"]


def first_shift(df):
    return df["Next Break Time (min)"] < 60


def short_break(df):
    # evaluate deletion
    return (df["Break Time (min)"] < 30) & (df["Shift Length (hrs)"] >= 5)


def break_less_than_30(df):
    return (
        (df["Break Time (min)"] < 30) & (df["Hours Worked Shift"] >= 5) & df["is_break"]
    )  # df["is_break"] ignores midnight punches and first punches of shift


def is_first_punch_of_shift(df):
    one_am = datetime.time(1, 0)  # 01:00:00
    return (df["Break Time (min)"] >= 60) | (  # Must be first punch of shift
        (df["Break Time (min)"].isna()) & (df["In Punch"].dt.time >= one_am)
    )  # We don't have break time but the punch is greater than 1 am
    # which tells me it must be the first punch of the shift as well.


def prev_in_punch_midnight(df):
    return df["Prev In Punch"].dt.time == pd.Timestamp("00:00:00").time()


def waiver_on_file(df):
    return df["Waiver on File?"] == "Yes"


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


def greater_than_five(df):
    return df["Totaled Amount"] > 5


def is_second_half_of_shift(df):
    return df["Break Time (min)"] < 60


def shift_bet_5_and_6(df):
    return (df["Shift Length (hrs)"] > 5) & (df["Shift Length (hrs)"] <= 6)


def shift_greater_than_6(df):
    return df["Shift Length (hrs)"] > 6


def did_not_break_new(df):
    # LB 1/21/26: If the punch is greater than 5 hours, you get a credit unless the shift
    # is 6 hours or less and there is a waiver on file.

    mask = (
        df["new_shift"]  # is first punch of shift
        & df["new_punch"]  # exclude midnight punches
        & (df["Punch Length (hrs)"] > 5)  # stapled punch length
        & ~(
            (df["Hours Worked Shift"] <= 6) & waiver_on_file(df)
        )  # exclude bonafied waived
        & (
            (df["Hours Worked Shift"] <= 6) & (df["Hours Worked Shift"] > 5)
        )  # prints only bet 5 and 6
    )
    return mask


def did_not_break_new_all(df):
    # For anomalies table only
    mask = (
        df["new_shift"]  # is first punch of shift
        & df["new_punch"]  # exclude midnight punches
        & (df["Punch Length (hrs)"] > 5)  # stapled punch length
        & ~(
            (df["Hours Worked Shift"] <= 6) & waiver_on_file(df)
        )  # exclude bonafied waived
    )
    return mask


def spans_midnight(df):
    # Need to include second condition otherwise would skip punches that
    # start at 12:00:00 AM.
    return (df["In Punch"].dt.date != df["Out Punch"].dt.date) | (
        df["In Punch"].dt.time == pd.Timestamp("00:00:00").time()
    )


def non_zero_var(df):
    return df["Variance"] != 0


def zero_rows_bypunch(df):
    mask = (
        (df["Total OT Hours Pay Period"] == 0)
        & (df["OT Hours Paid"] == 0)
        & (df["Total DT Hours Pay Period"] == 0)
        & (df["DT Hours Paid"] == 0)
    )
    return mask


def over_six(df):
    # Boolean: Shifts greater than 6 hours.
    return df["Totaled Amount"] > 6


def unique_ids_datetime(df):
    # Keeps first unique pair ID + Date. Note that all Date has been normalized to midnight when adding date helper cols.
    return ~df.duplicated(subset=["ID", "Date"])


def unique_ids(df):
    # Returns a Boolean mask that keeps the first occurrence of every unique ID and filters out duplicates.
    return ~df.duplicated(subset=["ID"])


def break_credit(df):
    # Boolean: Create mask without dups and break credit earned.
    # return (~df.duplicated("ID")) & (df["Paid Break Credit (hrs)"] > 0)
    return df["Paid Break Credit (hrs)"] > 0


def over_twelve(df):
    mask = unique_ids_datetime(df) & df["12hr Credit Due"]
    return mask


def check_seven_consec(df):
    return df["Hours in Consecutive Days"] > 0


def OT_var_mask(df):
    mask = df["OT Variance (hrs)"].abs() >= 0.01
    return mask


def DT_var_mask(df):
    mask = df["DT Variance (hrs)"].abs() >= 0.01
    return mask


def first_day_matches_first_workweek_day(df, start_of_week):
    return df["First day of Streak"].dt.day_name() == start_of_week
