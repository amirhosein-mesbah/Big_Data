import pandas as pd

def filter():
    df = pd.read_csv("./dags/data/final_news_df.csv")
    df = df.drop_duplicates(subset=['title'], keep=False)
    df = df.reset_index(drop=True)
    df.to_csv("./dags/data/final_news_df.csv", index=False)
    
