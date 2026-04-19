## Use debug_to_s3 function in aws.py - then pull it using local code. For example
    debug_id = "2JV0005917"
        debug_cols = [
        "Employee",
        "ID",
        "Attributed_Workday",
        "Hours_Worked",
        "Regular_Hrs",
        "OT_Hrs",
        "DT_Hrs",
        "Fiscal_Pay_Date",
        "OT_Hours_Pay_Period",
        "DT_Hours_Pay_Period",
        "OT_Hours_Paid",
        "DT_Hours_Paid",
        "OT_Variance_(hrs)",
        "DT_Variance_(hrs)",
        "Workweek_ID",
        "Days_Worked_In_Week",
        "Is_Consecutive_Day_Rule",
        "First_Day_of_Streak",
        "Consec_OT_Hours",
        "Consec_DT_Hours",
        "Cum_Reg_Hrs",
        "Weekly_OT_Spillover",
    ]
    debug_to_s3(df, debug_id, debug_cols, "pp-debug-bucket")