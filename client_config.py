PP_REQUIRED_COLUMNS = [
    "ID",  # unique per employee. Important: First three chars are Location Code
    "Employee",  # employee name (full name)
    "In Punch",  # containing both date and time info of the punch
    "Out Punch",  # containing both date and time info of the punch
    "Totaled Amount",  # total hours worked in the punch (decimal hours)
]


# "mappings" are column remapping for specific client data.
# "drop_columns" are junk columns to be dropped at the top (won't be processed at all).
# note that system created helper columns are dropped before database write in save_to_database_fast function
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

# columns that the user should not be able to see on the pulldown,
# either because they are required or junk
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
}
