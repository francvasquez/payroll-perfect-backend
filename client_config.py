# This is a check at the very top of ta_process
PP_REQUIRED_COLUMNS = [
    "ID",  # unique per employee. Important: First three chars are Location Code
    "Employee",  # employee name (full name)
    "In Punch",  # containing both date and time info of the punch
    "Out Punch",  # containing both date and time info of the punch
    "Totaled Amount",  # total hours worked in the punch (decimal hours)
]

# Also at the very top, drop junk columns and map col names interface
# Note: Python created helper columns are dropped before database write in save_to_database_fast function
CLIENT_CONFIGS = {
    "demo_client": {  # BH
        "mappings": {"Employee": "Employee", "ID": "ID"},
        "drop_columns": ["Org Path", "Date/Time"],
    },
    "client_b": {  # sample client
        "mappings": {"Staff_No": "ID", "Clock_In": "In Punch"},
        "drop_columns": ["Temp_Calculation_Field", "Audit_Log_ID"],
    },
}

# For DB UI: Columns that the user should not be able to see on the pulldown,
# either because they are required (already there) or just fluff
EXCLUDE_FROM_PULLDOWN = {
    "ID",  # req
    "Employee",  # req
    "In Punch",  # req
    "Out Punch",  # req
    "Time Zone",
    "Pay Rule",
    "Org Path",
    "Primary Account",
    "Apply To",
    "Money Amount",
    "Day Amount",
    "Xfr/Move: Account",
    "Xfr: Work Rule",
    "Date/Time",
}
