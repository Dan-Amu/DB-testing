import mysql.connector
import time
from random import randint
import threading
import sys

def connectToDatabase():
    databaseConnection = mysql.connector.connect(
            host='192.168.0.19',
            user='root',
            password='voxicon'
        )
    return databaseConnection

def threadPrint(threadID, texttoprint):
    print(f'Thread: {threadID} ', texttoprint)

def runQuery(tID):

    dibi = connectToDatabase()
    dbcursor = dibi.cursor()
    for i in range(1, 5):
        seed = randint(1, 300)
        start = seed*10
        query = f"select * from tpcc.customer where c_id between {start} and {start+50};"
        threadPrint(tID, "Current query:"+query)
        dbcursor.execute(query)
         
        threadPrint(tID, len(dbcursor.fetchall()))

try:
    if sys.argv[1]: 
        threadcount = int(sys.argv[1])
        threadcount += 1
    else:
        threadcount = 20
except:
    threadcount = 20
allthreads = []
for num in range(0, threadcount-1):
    allthreads.append(threading.Thread(target=runQuery, args=(num,)))
for num in range(0, threadcount-1):
    allthreads[num].start()
#x4.start()
