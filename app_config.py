import os

# S3 config - uses environment variable with fallback to production bucket
S3_BUCKET = os.environ.get("S3_BUCKET", "pp-client-data")

# API configuration
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
}

# These are fallbacks in case the front doesn't pass default (global) parameters
# Consider different approach?
DEFAULT_PAY_PERIOD_LENGTH = 14
DEFAULT_DAYS_BET_PAYROLL_END_AND_PAY_DATE = 6
DEFAULT_MIN_WAGE = 17.25
DEFAULT_OT_DAY_MAX = 8.0
DEFAULT_OT_WEEK_MAX = 40
DEFAULT_DT_DAY_MAX = 12
DEFAULT_WORKWEEK_START = "Sunday"
DEFAULT_CONSEC_DAYS_WORKWEEK = False
DEFAULT_CONSEC_DAYS_BEFORE_OT = 6
DEFAULT_STATE_MIN_WAGE = 16.50
DEFAULT_PAY_PERIODS_PER_YEAR = 26

WEEKDAYS = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]

# COLS for printing waiver
WAIVER_PRINT_COLS = ["Name", "Check_Pure"]

# COLS for printing WFN
COLUMNS_TO_SHOW = [
    "IDX",
    "Payroll Name",
    "1.5 OT Earnings Due",
    "Actual Pay Check",
    "Variance",
]
COLUMNS_TO_SHOW_DBLE = [
    "IDX",
    "Payroll Name",
    "Double Time Due",
    "Actual Pay Check Dble",
    "Variance Dble",
]
COLUMNS_TO_SHOW_BRKCRD = [
    "IDX",
    "Payroll Name",
    "Break Credit Hours",
    "Break Credit Due",
    "Actual Pay BrkCrd",
    "Variance BrkCrd",
    "Break Credit Due / Break Credit Hours",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_REST = [
    "IDX",
    "Payroll Name",
    "Rest Credit Hours",
    "Rest Credit Due",
    "Actual Pay RestCrd",
    "Variance RestCrd",
    "Rest Credit Due / Rest Credit Hours",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_SICK = [
    "IDX",
    "Payroll Name",
    "Sick Credit Hours",
    "Sick Credit Due",
    "Sick Paid",
    "Variance Sick",
    "Sick Credit Due / Sick Credit Hours",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_FLSA = [
    "IDX",
    "Payroll Name",
    "Position Status",
    "FLSA Code",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_MINWAGE = [
    "IDX",
    "Payroll Name",
    "Position Status",
    "FLSA Code",
    "REG",
    "Base Rate",
    "Regular Rate Paid",
    "S_Sick Pay_Earnings",
    "V_Vacation_Earnings",
]
COLUMNS_TO_SHOW_NONACTIVE = [
    "IDX",
    "Payroll Name",
    "Position Status",
    "Job Title Description",
    "HIREDATE",
    "V_Vacation_Hours",
    "Termination Date",
    "REG",
]
# COLS for printing TA
COLS_ANOMALIES = [
    "ID",
    "Employee",
    "Paid Break Credit (hrs)",
    "Due Break Credit (hrs)",
    "Variance",
    "Short Break",
    "Did Not Break",
    "Over Twelve",
]

COLS_PRINT2_B = [
    "ID",
    "Employee",
    "Hire Date",
    "In Punch",
    "Out Punch",
    "Punch Length (hrs)",
    "Hours Worked Shift",
    "Paid Break Credit (hrs)",
    "Waiver on File?",
    # "Flag",
]
COLS_PRINT3a = [
    "Employee",
    "ID",
    "Prev In Punch",
    "Prev Out Punch",
    "Break Time (min)",
    "In Punch",
    "Punch Length (hrs)",
    "Paid Break Credit (hrs)",
    "Hours Worked Shift",
    # "Waiver on File?",
]

COLS_PRINT5 = [
    "Employee",
    "ID",
    "Prev Out Punch",
    "In Punch",
    "Break Time (min)",
    "Regular Rate Paid",
    "Split Paid ($)",
    "Split at Min Wage ($)",
    "Split Shift Due ($)",
]

COLS_PRINT7 = [
    "ID",
    "Employee",
    "Date",
    "Hours Worked Shift",
    "Paid Break Credit (hrs)",
]
COLS_PRINT8 = [
    "Employee",
    "ID",
    "In Punch",  # Trigger Date
    "First_Day_of_Streak",
    "Consec_OT_Hours",
    "Consec_DT_Hours",
]
COLS_PRINT9 = [
    "Employee",
    "ID",
    "OT_Hours_Pay_Period",
    "OT_Hours_Paid",
    "OT_Variance_(hrs)",
]
COLS_PRINT9a = [
    "Employee",
    "ID",
    "DT_Hours_Pay_Period",
    "DT_Hours_Paid",
    "DT_Variance_(hrs)",
]

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


COLUMN_TO_KEEP_DB = {
    "Base": ["ID", "Employee", "In Punch", "Out Punch"],
    "Metadata": ["Last Updated", "Pay Date"],
    "Employee": ["Waiver on File?", "Location", "Status", "Status Date"],
    "Punch Info": [
        "Punch Length (hrs)",
        "Break Time (min)",
        "Is Break?",
        "Is New Punch?",
    ],
    "Violation Flags": [
        "Short Break",
        "Did Not Break",
        "12hr Credit Due",
        "Split Shift Due ($)",
    ],
    "Shift Info": [
        "New Shift?",
        "Shift Number",
        "Hours Worked Shift",
        "Shift Start",
        "First Punch of Shift?",
        "Punch Number in Shift",
    ],
    "12 hour Calcs": [
        "Over Twelve",
        "Break Count",
        "Break Order",
        "2nd Break Start",
        "Hours to 2nd Break",
    ],
    # "Comments": ["In Punch Comment", "Out Punch Comment", "Pay Code Comment"]
}
