## NEED UPDATE S3lambda_function
- Takes event attachment excel files (enconded)
- Decodes them back to excel
- Stores them in memory as df using pd.read_excel()
- Runs process functions
- For now, returns sample anomalies_df with length data of some others df. 