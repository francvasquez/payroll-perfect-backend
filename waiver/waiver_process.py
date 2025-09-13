import numpy as np

def process_waiver(df):
    # Import Excel file to df (REMOVED - LAMBDA WILL PASS A DF)
    # df = utility.import_excel(waiver_file, config.WAIVER_KEY_COLS)

    # Remove dups otherwise won't work as a lookup reference
    processed_waiver_df = df.drop_duplicates(subset="Name", keep="first").copy()

    # Replaces Check column removing leading / traling spaces with strip, makes everything lowercase, then creates Yes or No if x.
    processed_waiver_df["Check_Pure"] = np.where(
        processed_waiver_df["Check"].str.strip().str.lower() == "x", "Yes", "No"
    )

    return processed_waiver_df
