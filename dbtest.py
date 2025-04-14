import mysql.connector
import time
from random import randint
import threading
import sys

def connectToDatabase():
    databaseConnection = mysql.connector.connect(
            host='192.168.0.19',
            user='root',
            password='voxicon',
            database='Test'
        )
    return databaseConnection

def threadPrint(threadID, texttoprint):
    print(f'Thread: {threadID} ', texttoprint)

def runQuery(tID, startTime, endTime):
    global queries_ran
    loops = 0
    queries_ran[tID] = 0
    while True:
        dibi = connectToDatabase()
        dbcursor = dibi.cursor()
        #for i in range(1, 5):
           # seed = randint(1, 300)
           # start = seed*10
           # query = f"select * from tpcc.customer where c_id between {start} and {start+50};"
           # query = "call StressTest('1000', '100')"
           # threadPrint(tID, "Current query:"+query)
           # dbcursor.execute(query)
           #  
           # threadPrint(tID, len(dbcursor.fetchall()))

#        query = "call StressTest('1000', '10')"
        seed = randint(1, 300)
        start = seed*10
        query = f"select * from tpcc.customer where c_id between {start} and {start+20};"
        threadPrint(tID, "Current query:"+query)
        dbcursor.execute(query)

        #threadPrint(tID, len(dbcursor.fetchall()))
        loops = loops+1
        curTime = time.time()
        int_startTime = int(startTime)
        int_curTime = int(curTime)
        int_endTime = int(endTime)
        threadPrint(tID, f"Time Elapsed: {int_curTime-int_startTime}s / {int_endTime-int_startTime}s")
        if curTime > endTime:
            threadPrint(tID, "Exited. Iterations:"+str(loops))
            break
        else:
            queries_ran[tID] = queries_ran[tID] + 1
    
try:
    if sys.argv[1]: 
        threadcount = int(sys.argv[1])
        threadcount += 1
except:
    threadcount = 20

try:
    if sys.argv[2]: 
        runTime = int(sys.argv[2])
except:
    runTime = 5
queries_ran = [i for i in range(0, threadcount)]
startTime = time.time()
runTime = runTime * 60
endTime = startTime+runTime
allthreads = []
for num in range(0, threadcount-1):
    allthreads.append(threading.Thread(target=runQuery, args=(num,startTime, endTime)))
for num in range(0, threadcount-1):
    time.sleep(0.5)
    allthreads[num].start()

# wait for all threads to finish
for num in range(0, threadcount-1):
    allthreads[num].join()

total = 0
for n in queries_ran:
    total = total + n
print("Total queries finished:", total)
#threadsStarted = 0
#while True:
#    if startTime > (time.time() + 60):
#        break
#    for i in range(0, len(allthreads)):
#        if not allthreads[i].is_alive():
#            print(f"thread {i} restarted")
#            allthreads[i] = threading.Thread(target=runQuery, args=(num,))
#           
#        time.sleep(0.1)
#        
#    threadsStarted = threadsStarted + 1


#x4.start()
