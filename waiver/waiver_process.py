import numpy as np


def process_waiver(df):

    # Remove dups otherwise won't work as a lookup reference
    processed_waiver_df = df.drop_duplicates(subset="Name", keep="first").copy()

    # Check_Pure becomes Boolean
    processed_waiver_df["Has_Waiver_Bool"] = np.where(
        processed_waiver_df["Check"].str.strip().str.lower() == "x", True, False
    )

    return processed_waiver_df
