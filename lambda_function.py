import json
import pandas as pd
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta

def lambda_handler(event, context):
    # STEP 0: Variables - will be passed by user later
    min_wage = 15.00
    min_wage_40 = 22.50
    ot_day_max = 8  # Max hours before OT in a day

    # STEP 1: Create test data for Waiver
    waiver_test_df = pd.DataFrame({
        'Name': ['Boats, Mary', 'Abbey, John', 'Smith, Bob'],
        'Check': ['X', 'x', '']
    })
    
    # STEP 2: Process Waiver (needed by TA)
    processed_waiver_df = process_waiver(waiver_test_df)
    
    # STEP 3: Create test data for WFN
    wfn_test_df = pd.DataFrame({
        'CO.': ['22R', 'EGH'],
        'Payroll Name': ['Boats, Mary', 'Abbey, John'],
        'FILE#': ['000143', '006230'],
        'PAY DATE':['08/02/2025', '08/02/2025'],
        'FLSA Code':['N', 'N'],
        'Position Status': ['Active', 'Active'],
        'Regular Rate Paid': [20.25, 20.50],
        'REG': [80.00, 35.98],
        'OT': [29.13, 0.00],
        'DBLTIME HRS': [0.00, 0.00],
        'S_Sick Pay_Hours': [0, 0],
        'V_Vacation_Hours': [0, 0],
        'J_Break Credits_Additional Hours': [4.00, 0.00],
        'RC - Rest Credit Hours':[0,0],
        'Regular Earnings Total': [1620.00, 737.59],
        'Overtime Earnings Total': [884.82, 0.00],
        'D_Double Time_Additional Earnings': [0,0],
        'S_Sick Pay_Earnings': [0, 0],
        'V_Vacation_Earnings': [0, 0],
        'J_Break Credits_Additional Earnings': [0,0],
        'C_Ee Commission_Additional Earnings': [0,0],
        'RC_Rest Credit_Earnings': [0,0],
        'E_Auto Gratuities_Additional Earnings': [0,0],
        'X_RESTR SVC CHG_Additional Earnings': [0,0],
        'Y_BELLMANSVCCHG_Additional Earnings': [0,0],
        'A_MISC ADJUST_flsa earnings': [0,0],
        'SB_Sales Bonus_Earnings': [0,0],
        'B_Bonus_Additional Earnings': [0,0]
    })
    
    # STEP 4: Process WFN (needed by TA)
    processed_wfn_df = process_data_wfn(wfn_test_df, min_wage, min_wage_40)
    
    # STEP 5: Create test data for TA
    ta_test_df = pd.DataFrame({
        'ID': ['000143', '000143', '006230', '006230'],
        'Employee': ['Boats, Mary', 'Boats, Mary', 'Abbey, John', 'Abbey, John'],
        'Date/Time': ['08/01/2025', '08/01/2025', '08/01/2025', '08/01/2025'],
        'In Punch': ['08/01/2025 08:00', '08/01/2025 13:00', '08/01/2025 09:00', '08/01/2025 14:00'],
        'Out Punch': ['08/01/2025 12:00', '08/01/2025 17:30', '08/01/2025 13:00', '08/01/2025 18:00'],
        'Totaled Amount': [4.00, 4.50, 4.00, 4.00],
    })
    
    # STEP 6: Process TA (uses both processed dataframes)
    
    try:
        df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
            ta_test_df, 
            min_wage, 
            ot_day_max, 
            processed_waiver_df, 
            processed_wfn_df
        )
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'message': 'All three modules processed successfully!',
                'ta_rows': len(df),
                'bypunch_rows': len(bypunch_df),
                'stapled_rows': len(stapled_df),
                'anomalies_rows': len(anomalies_df)
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'module': 'TA processing'
            })
        }