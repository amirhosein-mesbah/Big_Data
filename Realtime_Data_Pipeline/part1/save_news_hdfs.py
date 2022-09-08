import pyhdfs
from datetime import datetime

def save():
    fs = pyhdfs.HdfsClient(hosts='namenode:50070')
    now = datetime.now()
    dir= str(now.year) + "-" + str(now.month) + "-"+ str(now.hour) +"-"+ str(now.minute)
    if not fs.exists("/news/"+dir):
        fs.mkdirs("/news/" +dir)
    fs.copy_from_local("./dags/data/final_news_df.csv", "/news/"+dir+"/final_news_df_" +str(now.day)+"_"+ str(now.hour) +"_"+ str(now.minute)+ ".csv")
