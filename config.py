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
DEFAULT_MIN_WAGE = 17.25
# DEFAULT_MIN_WAGE_40 = 2640.00
DEFAULT_OT_DAY_MAX = 8.0
DEFAULT_OT_WEEK_MAX = 40
DEFAULT_DT_DAY_MAX = 12
DEFAULT_WORKWEEK_START = "Sunday"
# DEFAULT_EXEMPT_MIN_ANNUAL_WAGE = 68640
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
    "Payroll Name",
    "IDX",
    "1.5 OT Earnings Due",
    "Actual Pay Check",
    "Variance",
]
COLUMNS_TO_SHOW_DBLE = [
    "Payroll Name",
    "IDX",
    "Double Time Due",
    "Actual Pay Check Dble",
    "Variance Dble",
]
COLUMNS_TO_SHOW_BRKCRD = [
    "Payroll Name",
    "IDX",
    "Break Credit Hours",
    "Break Credit Due",
    "Actual Pay BrkCrd",
    "Variance BrkCrd",
    "Break Credit Due / Break Credit Hours",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_REST = [
    "Payroll Name",
    "IDX",
    "Rest Credit Hours",
    "Rest Credit Due",
    "Actual Pay RestCrd",
    "Variance RestCrd",
    "Rest Credit Due / Rest Credit Hours",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_SICK = [
    "Payroll Name",
    "IDX",
    "Sick Credit Hours",
    "Sick Credit Due",
    "Sick Paid",
    "Variance Sick",
    "Sick Credit Due / Sick Credit Hours",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_FLSA = [
    "Payroll Name",
    "IDX",
    "Position Status",
    "FLSA Code",
    "Regular Rate Paid",
]
COLUMNS_TO_SHOW_MINWAGE = [
    "Payroll Name",
    "IDX",
    "Position Status",
    "FLSA Code",
    "REG",
    "Base Rate",
    "Regular Rate Paid",
    "S_Sick Pay_Earnings",
    "V_Vacation_Earnings",
]
COLUMNS_TO_SHOW_NONACTIVE = [
    "Payroll Name",
    "IDX",
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
COLS_PRINT = ["Employee", "Totaled Amount"]
COLS_PRINT2_A = [
    "ID",
    "Employee",
    "Prev Out Punch",
    "Break Time (min)",
    "In Punch",
    "Out Punch",
    "Totaled Amount",
    "Shift Length (hrs)",
    # "Total Worked Hours Workday",
    "Next Break Time (min)",
    "Next In Punch",
    # "Next Out Punch",
    "Next Out Punch",
    "Next Punch Length (hrs)",
    "Paid Break Credit (hrs)",
    "Waiver on File?",
    # "Flag",
]
COLS_PRINT2_B = [
    "ID",
    "Employee",
    "Prev Out Punch",
    "Break Time (min)",
    "In Punch",
    "Out Punch",
    "Totaled Amount",
    "Hours Worked Shift",
    "Paid Break Credit (hrs)",
    "Waiver on File?",
    # "Flag",
]
COLS_PRINT3 = [
    "Employee",
    "ID",
    "Prev In Punch",
    "Prev Out Punch",
    "Break Time (min)",
    "In Punch",
    "Totaled Amount",
    "Next Break Time (min)",
    "Paid Break Credit (hrs)",
    "Shift Length (hrs)",
    "Waiver on File?",
]

COLS_PRINT3a = [
    "Employee",
    "ID",
    "Prev In Punch",
    "Prev Out Punch",
    "Break Time (min)",
    "In Punch",
    "Totaled Amount",
    "Paid Break Credit (hrs)",
    "Hours Worked Shift",
    "Waiver on File?",
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
    "In Punch",
    "Hours in Consecutive Days",
    "Totaled Amount",
    "First day of Streak",
]
COLS_PRINT9 = [
    "Employee",
    "ID",
    "Total OT Hours Pay Period",
    "OT Hours Paid",
    "OT Variance (hrs)",
]
COLS_PRINT9a = [
    "Employee",
    "ID",
    "Total DT Hours Pay Period",
    "DT Hours Paid",
    "DT Variance (hrs)",
]
COLS_PRINT10 = [
    "ID",
    "Employee",
    "Paid Break Credit (hrs)",
    "Due Break Credit (hrs)",
    "Variance",
    "Short Break",
    "Did Not Break",
    "Over Twelve",
]
COLUMNS_TO_DROP_FOR_DATABASE = [
    "Prev In Punch",
    "Prev Out Punch",
    "Next In Punch",
    "Next Out Punch",
    "Prev Date",
    "Next Date",
    "Prev Punch Length (hrs)",
    "Next Punch Length (hrs)",
    "Prev ID",
    "Next ID",
    "Waiver Lookup",
]
