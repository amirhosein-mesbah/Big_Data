import pandas as pd
import glob


def merge_df():
    # merging two csv files
    df = pd.concat(map(pd.read_csv, sorted(glob.glob('./dags/data/news_df_*.csv'))), ignore_index=True)
    df.to_csv("./dags/data/final_news_df.csv", index=False)