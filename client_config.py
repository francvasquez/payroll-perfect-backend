# This will be the columns required post normalization in process_data_ta step 1. Basically this is the data that we need to request to new clients. It doesn't matter if it doesn't come and be specific columns but somehow the data needs to be present. We can transform using CLIENT_CONFIGS and the method normalize_client_data.

TA_TARGET_SCHEMA = [
    "ID",  # Standardized unique identifier - must match WFN's IDX
    "Location",
    "Employee",
    "In Punch",
    "Out Punch",
    "Status",
    "Status Date",
]

WFN_TARGET_SCHEMA = [  # See WFN_CORE_SCHEMA for minimum requirements
    # Identifiers
    "IDX",  # For ADP - see mapping below. Built from CO. + FILE# during normalization; must match TA's ID
    "Location",
    "Payroll Name",
    "Pay Date",
    # Status and Rates
    "FLSA Status",
    "Position Status",
    "Hire Date",
    "Job Description",
    "Termination Date",
    "Regular Rate Paid",
    # Standard Hours & Earnings
    "Regular Hours",
    "Overtime Hours",
    "Double Time Hours",
    "Regular Earnings Total",
    "Overtime Earnings",
    # Additional Earnings (Non-Discretionary & Bonuses)
    "Misc FLSA Earnings",
    "Bonus Earnings",
    "Commission Earnings",
    "Auto Gratuity Earnings",
    "Restricted Service Charge Earnings",
    "Bellman Service Charge Earnings",
    "Double Time Earnings",
    # Break, Rest, Sick, Vacation Hours & Earnings
    "Break Credit Hours",
    "Break Credit Earnings",
    "Rest Credit Hours",
    "Rest Credit Earnings",
    "Sick Pay Hours",
    "Sick Pay Earnings",
    "Vacation Hours",
    "Vacation Earnings",
]

# ADP export headers → standard names (Title Case). Detection still uses raw CO., PAY DATE.
ADP_WFN_COLUMN_MAPPINGS = {
    "IDX": {
        "source_columns": ["CO.", "FILE#"],
        "transform": "concat",
        "delimiter": "0",
        "preprocess": {
            "CO.": {"astype": "str"},
            "FILE#": {"astype": "int", "zfill": 6},
        },
    },
    "Location": "CO.",
    "Pay Date": "PAY DATE",
    "FLSA Status": "FLSA Code",
    "Hire Date": "HIREDATE",
    "Job Description": "Job Title Description",
    "Regular Hours": "REG",
    "Overtime Hours": "OT",
    "Double Time Hours": "DBLTIME HRS",
    "Overtime Earnings": "Overtime Earnings Total",
    "Misc FLSA Earnings": "A_MISC ADJUST_flsa earnings",
    "Bonus Earnings": "B_Bonus_Additional Earnings",
    "Commission Earnings": "C_Ee Commission_Additional Earnings",
    "Auto Gratuity Earnings": "E_Auto Gratuities_Additional Earnings",
    "Restricted Service Charge Earnings": "X_RESTR SVC CHG_Additional Earnings",
    "Bellman Service Charge Earnings": "Y_BELLMANSVCCHG_Additional Earnings",
    "Double Time Earnings": "D_Double Time_Additional Earnings",
    "Break Credit Hours": "J_Break Credits_Additional Hours",
    "Break Credit Earnings": "J_Break Credits_Additional Earnings",
    "Rest Credit Hours": "RC - Rest Credit Hours",
    "Rest Credit Earnings": "RC_Rest Credit_Earnings",
    "Sick Pay Hours": "S_Sick Pay_Hours",
    "Sick Pay Earnings": "S_Sick Pay_Earnings",
    "Vacation Hours": "V_Vacation_Hours",
    "Vacation Earnings": "V_Vacation_Earnings",
}

CLIENT_CONFIGS = {
    "demo_client": {  # BH
        "anchor_pay_date": "2026-01-16",  # This is the anchor pay date used to calculate to which fiscal pay dates each work day belongs to. Should be a known pay date in the client's payroll calendar. Format: YYYY-MM-DD
        "wfn_systems": {
            "ADP": {
                "detection": {"columns": ["CO.", "PAY DATE"], "header": 5},
                "mappings": ADP_WFN_COLUMN_MAPPINGS,
            },
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
