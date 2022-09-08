import os
import glob

def remove_dfs():
    for f in sorted(glob.glob('./dags/data/crypto_*.csv')):
        os.remove(f)