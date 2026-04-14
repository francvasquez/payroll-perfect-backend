## Use debug_to_s3 function in aws.py - for example
    debug_id = "2JV0005917"
    debug_cols = [
        "ID",
        "In Punch",
        "Workday Hours",
        "Work Week",
        "Week Hours",
        "Total Hours Pay Period",
        "OT Day Max",
        "OT Week Max",
        "Workday OT Hours",
        "Sum of Workday OT Hours",
        "Week OT Hours Gross",
        "Week OT Hours Net",
        "Total OT Hours Week",
        "Total OT Hours Pay Period",
    ]
    debug_to_s3(df, debug_id, debug_cols, "pp-debug-bucket")