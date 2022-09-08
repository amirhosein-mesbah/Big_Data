import pandas as pd
import glob


def merge_df():
    # merging two csv files
    df = pd.concat(map(pd.read_csv, sorted(glob.glob('./dags/data/crypto_*.csv'))), ignore_index=True)
    df.to_csv("./dags/data/final_crypto.csv", index=False)