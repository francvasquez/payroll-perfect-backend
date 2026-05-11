# This will be the columns required post normalization in process_data_ta step 1. Basically this is the data that we need to request to new clients. It doesn't matter if it doesn't come and be specific columns but somehow the data needs to be present. We can transform using CLIENT_CONFIGS and the method normalize_client_data.

PP_TARGET_SCHEMA = {
    "ta": [
        "ID",  # Standardized unique identifier - must match WFN's IDX
        "Location",
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
        "wfn_systems": {
            "ADP": {"detection": {"columns": ["CO.", "PAY DATE"], "header": 5}}
        },
        "ta_systems": {
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
                    "Location": "Pay Group",  # Ensure this is after "ID" mapping since it uses "Pay Group" column which will get renamed to "Location" above
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
    "new_client": {},  # reference for adding new client
}
