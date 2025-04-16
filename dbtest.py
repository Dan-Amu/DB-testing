import mysql.connector
import time
from random import randint
import threading
import sys
import os.path

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
def pickRandQuery(tID, startTime, endTime):
    choice = randint(1, 3)
    match choice:
        case 1:
            readQuery
def runQuery(tID, startTime, endTime):
    global queries_ran
    #global debugInfo
    debugInfo.append(["Thread:", tID])
    loops = 0
    queries_ran[tID] = 0
    while True:
        #debugInfo[tID].append("Before DBconn"+str(time.time()))
        
        dibi = connectToDatabase()
        dbcursor = dibi.cursor()

        #debugInfo[tID].append("After DBconn"+str(time.time()))

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
        seed = randint(1, 30000)
        start = seed*1
        query = f"select * from tpcc.customer where c_id between {start} and {start+20};"
        #query = f"select * from tpcc.customer where c_id like {start};"

        dbcursor.execute(query)

        #threadPrint(tID, len(dbcursor.fetchall()))
        loops = loops+1
        dibi.close()
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
            threadPrint(tID, "query counted. Current amount:"+str(queries_ran[tID]))
    
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
queries_ran = [i for i in range(0, threadcount-1)]
startTime = time.time()
runTime = runTime * 60
endTime = startTime+runTime

debugInfo = []

allthreads = []
for num in range(0, threadcount-1):
    allthreads.append(threading.Thread(target=runQuery, args=(num,startTime, endTime)))
for num in range(0, threadcount-1):
    time.sleep(0.05)
    allthreads[num].start()
    print("Started thread: ", num)

# wait for all threads to finish
for num in range(0, threadcount-1):
    allthreads[num].join()

total = 0
for n in queries_ran:
    total = total + n
#print(debugInfo)
print("\n\n")
print(queries_ran)
runtimeMinutes = round(runTime/60, 1)

# Write test results to file including date and time of test run
current_time = time.localtime()
# Format it as a date string
now = time.strftime("%Y-%m-%d %H:%M:%S", current_time)
resultsFile = open("./testresults.txt", "a")
resultsFile.write("Test run at: "+now)
resultsFile.write(f"Total queries finished in {runtimeMinutes} minutes: "+total)
resultsFile.write(str(total/runtimeMinutes)+" Queries per minute.")
resultsFile.close()

print(f"Total queries finished in {runtimeMinutes} minutes:", total)
print(total/runtimeMinutes, " Queries per minute.")
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
