def var_below(df, col, thres=0.01):
    return df[col].abs() > thres


def flsa(df):
    return df["FLSA Check"] == "CHECK"


def min_wage_check(df):
    return (df["Minimum Wage"] == "CHECK") & (df["REG"] > 0)


def non_active_check(df):
    return df["Non-Active"] == "CHECK"
