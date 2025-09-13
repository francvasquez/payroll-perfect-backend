# lambda_function.py
import json
import pandas as pd
from wfn.wfn_process import process_data_wfn

def lambda_handler(event, context):
    # Create test df with ALL columns from your data
    test_df = pd.DataFrame([
        {
            'CO.': '22R',
            'PAY DATE': '08/15/2025',
            'FILE#': '000143',
            'Payroll Name': 'Boats, Mary',
            'Home Department Code': '021035',
            'Home Department Description': 'Inspectors',
            'Job Title Description': 'Inspectors',
            'FLSA Code': 'N',
            'Position Status': 'Active',
            'HIREDATE': '02/18/1988',
            'Rehire Date': '',
            'Termination Date': '',
            'Status Effective Date': '05/01/2023',
            'Regular Rate Paid': 20.25,
            'REG': 80.00,
            'OT': 29.13,
            'DBLTIME HRS': 0.00,
            'HW_Holiday Worked_Hours': 0.00,
            'H_Holiday_Hours': 0.00,
            'S_Sick Pay_Hours': 0.00,
            'S19_Sick C19_Hours': 0.00,
            'V_Vacation_Hours': 0.00,
            'Regular Earnings Total': 1620.00,
            'Overtime Earnings Total': 884.82,
            'D_Double Time_Additional Earnings': 0.00,
            'HW_Holiday Worked_Earnings': 0.00,
            'H_Holiday_Earnings': 0.00,
            'S_Sick Pay_Earnings': 0.00,
            'S19_Sick C19_Earnings': 0.00,
            'V_Vacation_Earnings': 0.00,
            'A_MISC ADJUST_flsa earnings': 0.00,
            'C_Ee Commission_Additional Earnings': 0.00,
            'E_Auto Gratuities_Additional Earnings': 0.00,
            'X_RESTR SVC CHG_Additional Earnings': 0.00,
            'Y_BELLMANSVCCHG_Additional Earnings': 0.00,
            'B_Bonus_Additional Earnings': 0.00,
            'SB_Sales Bonus_Earnings': 0.00,
            'BD_BonusDiscretion_Earnings': 0.00,
            'T - Tips': 0.00,
            'RET_$ Retro Pay_Earnings': 0.00,
            'J_Break Credits_Additional Earnings': 0.00,
            'J_Break Credits_Additional Hours': 0.00,
            'RC_Rest Credit_Earnings': 0.00,
            'RC - Rest Credit Hours': 0.00
        },
        {
            'CO.': 'EGH',
            'PAY DATE': '08/15/2025',
            'FILE#': '006230',
            'Payroll Name': 'Abbey, John',
            'Home Department Code': '061005',
            'Home Department Description': 'Desk Clerks',
            'Job Title Description': 'Guest Service Agent',
            'FLSA Code': 'N',
            'Position Status': 'Active',
            'HIREDATE': '06/22/2023',
            'Rehire Date': '',
            'Termination Date': '',
            'Status Effective Date': '02/10/2024',
            'Regular Rate Paid': 20.50,
            'REG': 35.98,
            'OT': 0.00,
            'DBLTIME HRS': 0.00,
            'HW_Holiday Worked_Hours': 0.00,
            'H_Holiday_Hours': 0.00,
            'S_Sick Pay_Hours': 0.00,
            'S19_Sick C19_Hours': 0.00,
            'V_Vacation_Hours': 0.00,
            'Regular Earnings Total': 737.59,
            'Overtime Earnings Total': 0.00,
            'D_Double Time_Additional Earnings': 0.00,
            'HW_Holiday Worked_Earnings': 0.00,
            'H_Holiday_Earnings': 0.00,
            'S_Sick Pay_Earnings': 0.00,
            'S19_Sick C19_Earnings': 0.00,
            'V_Vacation_Earnings': 0.00,
            'A_MISC ADJUST_flsa earnings': 0.00,
            'C_Ee Commission_Additional Earnings': 0.00,
            'E_Auto Gratuities_Additional Earnings': 0.00,
            'X_RESTR SVC CHG_Additional Earnings': 22.48,
            'Y_BELLMANSVCCHG_Additional Earnings': 0.00,
            'B_Bonus_Additional Earnings': 6.00,
            'SB_Sales Bonus_Earnings': 0.00,
            'BD_BonusDiscretion_Earnings': 0.00,
            'T - Tips': 0.00,
            'RET_$ Retro Pay_Earnings': 0.00,
            'J_Break Credits_Additional Earnings': 0.00,
            'J_Break Credits_Additional Hours': 0.00,
            'RC_Rest Credit_Earnings': 0.00,
            'RC - Rest Credit Hours': 0.00
        }
    ])
    
    # Parameters
    min_wage = 15.00
    min_wage_40 = 22.50
    
    try:
        processed_df = process_data_wfn(test_df, min_wage, min_wage_40)
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'message': 'WFN processing works!',
                'rows_processed': len(processed_df)
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }