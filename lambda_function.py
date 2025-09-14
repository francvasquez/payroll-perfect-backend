import json, base64, io
import pandas as pd
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta

def lambda_handler(event, context):
    try:
        # Check if this is a real request with files
        if event.get('body'):
            body = json.loads(event['body'])
            
            # Get parameters from request
            min_wage = body.get('min_wage', 15.00)
            min_wage_40 = body.get('min_wage_40', 22.50)
            ot_day_max = body.get('ot_day_max', 8)
            
            # Process uploaded files (Base64 encoded)
            if 'waiver_file' in body:
                waiver_content = base64.b64decode(body['waiver_file'])
                waiver_df = pd.read_excel(io.BytesIO(waiver_content))
                processed_waiver_df = process_waiver(waiver_df)
            
            if 'wfn_file' in body:
                wfn_content = base64.b64decode(body['wfn_file'])
                wfn_df = pd.read_excel(io.BytesIO(wfn_content), header=6)  # Headers on row 6
                processed_wfn_df = process_data_wfn(wfn_df, min_wage, min_wage_40)
            
            if 'ta_file' in body:
                ta_content = base64.b64decode(body['ta_file'])
                ta_df = pd.read_excel(io.BytesIO(ta_content))
                df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
                    ta_df, min_wage, ot_day_max, 
                    processed_waiver_df, processed_wfn_df
                )
                
                # Return processed data
                return {
                    'statusCode': 200,
                    'headers': {'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({
                        'success': True,
                        'summary': {
                            'ta_rows': len(df),
                            'anomalies': len(anomalies_df),
                            'bypunch_rows': len(bypunch_df)
                        },
                        # Include sample data for UI
                        'anomalies_sample': anomalies_df.head(10).to_dict('records') if len(anomalies_df) > 0 else []
                    })
                }
        
        # Otherwise run test data (your existing code)
        # ... existing test code ...
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }