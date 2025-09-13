def var_below(df, col, thres=0.01):
    return df[col].abs() > thres
