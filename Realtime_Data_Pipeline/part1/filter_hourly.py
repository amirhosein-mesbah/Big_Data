import jdatetime
from datetime import timedelta, datetime
import pandas as pd

def filter():
    hour = str(datetime.now().hour)
    date_now = jdatetime.datetime.now() - timedelta(hours=1)
    date_now_str = date_now.strftime("%Y/%m/%d %H:%M")
    df = pd.read_csv("./dags/data/crypto_" +hour + ".csv")
    df= df[~(df.datetime < date_now_str)].reset_index(drop=True)
    df.to_csv(".dags/data/crypto_" +hour + ".csv", index=False)
    
