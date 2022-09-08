import pyhdfs
from datetime import datetime

def save():
    fs = pyhdfs.HdfsClient(hosts='namenode:50070')
    now = datetime.now()
    dir= str(now.year) + "-" + str(now.month) + "-"+ str(now.hour) +"-"+ str(now.minute)
    if not fs.exists("/crypto/"+dir):
        fs.mkdirs("/crypto/" +dir)
    fs.copy_from_local("./dags/data/final_crypto.csv", "/crypto/"+dir+"/final_crypto_" +str(now.day)+"_"+ str(now.hour) +"_"+ str(now.minute) + ".csv")
