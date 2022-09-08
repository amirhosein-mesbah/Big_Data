import os
import glob

def remove_dfs():
    for f in sorted(glob.glob('./dags/data/news_df_*.csv')):
        os.remove(f)