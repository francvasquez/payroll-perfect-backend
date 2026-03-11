PP_REQUIRED_COLUMNS = [
    "ID",  # unique per employee. Important: First three chars are Location Code e.g. 22R0000143
    "Employee",  # employee name (full name) e.g. Abarca, Maria Del Carmen
    "In Punch",  # containing both date & time e.g. 46038.5180555556 Excel format
    "Out Punch",
    "Status",
    "Status Date",
]

CLIENT_CONFIGS = {
    "demo_client": {  # BH
        "systems": {
            "Time and Attendance": {
                "detection": {
                    "columns": ["Employee", "In Punch Comment"],
                    "header": 7,  # The file must contain "columns" in the correct "header"
                },
                "mappings": {"Employee": "Employee"},  # For reference
                "drop_columns": [
                    "Org Path",
                    "Date/Time",
                    "Totaled Amount",
                ],  # Cols which we know are not needed
                "drop_rows": {
                    "In Punch": "Blank",
                    "Out Punch": "Blank",
                },
            },
            "Workforce Manager": {
                "force_type": {
                    "Payroll File Number": str,
                },
                "detection": {
                    "columns": ["Employee", "Home Labor Category"],
                    "header": 5,
                },
                "mappings": {
                    "ID": {
                        "source_columns": ["Pay Group", "Payroll File Number"],
                        "transform": "concat",
                        "delimiter": "0",
                    }
                },
                "drop_columns": [
                    "Totaled Amount",
                ],
                "drop_rows": {  ## This is on an OR basis
                    "In Punch": "Blank",
                    "Out Punch": "Blank",
                    "Apply To": ["Sick Pay", "Vacation"],
                    "ID": ["TESTEE"],
                },
            },
        },
    },
    "new_client": {  # add new client
        "systems": {
            "systemA": {
                "detection": {
                    "columns": ["ColA", "ColB"],
                    "header": 0,
                },
                "mappings": {"Staff_No": "ID", "Clock_In": "In Punch"},  # Sample
                "drop_columns": [
                    "Temp_Calculation_Field",
                    "Audit_Log_ID",
                ],
            },
        },
    },
}

# CLIENT_CONFIGS = {
#     "demo_client": {  # BH
#         "mappings": {"Employee": "Employee", "ID": "ID"},
#         "drop_columns": ["Org Path", "Date/Time"],
#     },
#     "client_b": {  # sample client
#         "mappings": {"Staff_No": "ID", "Clock_In": "In Punch"},
#         "drop_columns": ["Temp_Calculation_Field", "Audit_Log_ID"],
#     },
# }

# For DB UI: Columns that the user should not be able to see on the pulldown,
# either because they are required (already there) or just fluff
EXCLUDE_FROM_PULLDOWN = (
    {  # TODO evaluate as many of those are already block from dbase at write
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
)
