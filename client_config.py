# PP_REQUIRED_COLUMNS = {
#     "ta": [
#         "ID",  # unique per employee. Important: First three chars are Location Code e.g. 22R0000143
#         "Employee",  # employee name (full name) e.g. Abarca, Maria Del Carmen
#         "In Punch",  # containing both date & time e.g. 46038.5180555556 Excel format
#         "Out Punch",
#         "Status",
#         "Status Date",
#     ]
# }

# This will be the columns required post normalization in process_data_ta step 1. This is also the gold standard to
# pass along to new client when requesting files.
PP_TARGET_SCHEMA = {
    "ta": [
        "ID",  # Standardized unique identifier
        "Location",  # Explicitly required for downstream math
        "Employee",
        "In Punch",
        "Out Punch",
        "Status",
        "Status Date",
    ]
}

CLIENT_CONFIGS = {
    "demo_client": {  # BH
        "anchor_pay_date": "2026-01-16",  # This is the anchor pay date used to calculate to which fiscal pay dates each work day belongs to. Should be a known pay date in the client's payroll calendar. Format: YYYY-MM-DD
        "systems": {
            "Time and Attendance": {
                "detection": {
                    "columns": ["Employee", "In Punch Comment"],
                    "header": 7,  # The file must contain "columns" in the correct "header"
                },
                "mappings": {
                    "Employee": "Employee",  # For reference
                    # NEW: Extract first 3 characters from ID
                    "Location": {
                        "source_column": "ID",
                        "transform": "substring",
                        "start": 0,
                        "end": 3,
                    },
                },
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
                    },
                    "Location": {
                        "source_column": "ID",
                        "transform": "substring",
                        "start": 0,
                        "end": 3,
                    },
                },
                "drop_columns": [
                    "Totaled Amount",
                ],
                "drop_rows": {  ## This is on an OR basis
                    "In Punch": "Blank",
                    "Out Punch": "Blank",
                    "Apply To": ["Sick Pay", "Vacation"],
                    "ID": ["TESTEE"],
                    "Employee": ["testee"],
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
                "drop_columns": [
                    "Temp_Calculation_Field",
                    "Audit_Log_ID",
                ],
            },
        },
    },
}
