PP_REQUIRED_COLUMNS = {
    "ta": [
        "ID",  # unique per employee. Important: First three chars are Location Code e.g. 22R0000143
        "Employee",  # employee name (full name) e.g. Abarca, Maria Del Carmen
        "In Punch",  # containing both date & time e.g. 46038.5180555556 Excel format
        "Out Punch",
        "Status",
        "Status Date",
    ]
}

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
                "mappings": {"Staff_No": "ID", "Clock_In": "In Punch"},  # Sample
                "drop_columns": [
                    "Temp_Calculation_Field",
                    "Audit_Log_ID",
                ],
            },
        },
    },
}
