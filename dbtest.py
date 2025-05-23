import mysql.connector
import psycopg2
import time
from random import randint
import threading
import sys
import os.path

def connectToDatabase(server):
    match server:
        case "MySQL":
            databaseConnection = mysql.connector.connect(
                    host='192.168.0.19',
                    user='root',
                    password='voxicon',
                    port=3307,
                    database='dbtest1'
                )
            return databaseConnection

        case "PostgreSQL":
            databaseConnection = psycopg2.connect(
                    host='192.168.0.19',
                    user='postgres',
                    password='qaz123',
                    dbname='dbtest1'
                )
            return databaseConnection

        case "MongoDB":
            databaseConnection = pymongo.MongoClient('mongodb://192.168.0.19')
            return databaseConnection
            #print("Not yet Implemented")
        case _:
            print("Invalid server type")


def threadPrint(threadID, texttoprint):
    print(f'Thread: {threadID} ', texttoprint)

def runQueryMySQL(tID, startTime, endTime):
    global queries_ran
    #global debugInfo
    debugInfo.append(["Thread:", tID])
    loops = 0
    queries_ran[tID] = 0


    dibi = connectToDatabase("MySQL")
    dbcursor = dibi.cursor()
    query = "select MAX(order_id) from orders"
    dbcursor.execute(query)
    maxid = dbcursor.fetchone()
    dibi.close()
    while True:
        
        dibi = connectToDatabase("MySQL")
        dbcursor = dibi.cursor()

       # seed = randint(1, 30000)
       # start = seed*1
       # query = f"select * from tpcc.customer where c_id between {start} and {start+20};"

       #query = f"select * from tpcc.customer where c_id like {start};"
        random_id = randint(0, int(maxid[0]))
        
       # Perform SELECT with JOINs
        query = f"""
            SELECT o.order_id, c.name AS customer_name, p.name AS product_name, oi.quantity, pay.amount
           FROM orders o
           JOIN customers c ON o.customer_id = c.customer_id
           JOIN order_items oi ON o.order_id = oi.order_id
           JOIN products p ON oi.product_id = p.product_id
           JOIN payments pay ON o.order_id = pay.order_id
           WHERE o.order_id = { random_id }
           LIMIT 10
        """
        dbcursor.execute(query)
        response = dbcursor.fetchall()
        print(response)
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

def runQueryPostgreSQL(tID, startTime, endTime):
    global queries_ran
    #global debugInfo
    debugInfo.append(["Thread:", tID])
    loops = 0
    queries_ran[tID] = 0


    while True:
        
        dibi = connectToDatabase("PostgreSQL")
        dbcursor = dibi.cursor()

       # seed = randint(1, 30000)
       # start = seed*1
       # query = f"select * from tpcc.customer where c_id between {start} and {start+20};"

       #query = f"select * from tpcc.customer where c_id like {start};"

       # Perform SELECT with JOINs
        query = """
            SELECT o.order_id, c.name AS customer_name, p.name AS product_name, oi.quantity, pay.amount
           FROM orders o
           JOIN customers c ON o.customer_id = c.customer_id
           JOIN order_items oi ON o.order_id = oi.order_id
           JOIN products p ON oi.product_id = p.product_id
           JOIN payments pay ON o.order_id = pay.order_id
           ORDER BY RANDOM()
           LIMIT 10
        """

        dbcursor.execute(query)

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

def runQueryMongoDB(tID, startTime, endTime):
    global queries_ran
    #global debugInfo
    debugInfo.append(["Thread:", tID])
    loops = 0
    queries_ran[tID] = 0


    while True:
        
        dibi = connectToDatabase("MongoDB")

        
        query = """
            SELECT o.order_id, c.name AS customer_name, p.name AS product_name, oi.quantity, pay.amount
           FROM orders o
           JOIN customers c ON o.customer_id = c.customer_id
           JOIN order_items oi ON o.order_id = oi.order_id
           JOIN products p ON oi.product_id = p.product_id
           JOIN payments pay ON o.order_id = pay.order_id
           ORDER BY RAND()
           LIMIT 10
        """

        dbcursor.execute(query)

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

supportedDatabases = [
        "mysql",
        "postgresql"
        #"mongodb"
        ]
if sys.argv[1] in supportedDatabases:
    dbType = sys.argv[1]

try:
    if sys.argv[2]: 
        threadcount = int(sys.argv[2])
        threadcount += 1
except:
    threadcount = 20
try:
    if sys.argv[3]: 
        runTime = int(sys.argv[3])
except:
    runTime = 5

queries_ran = [i for i in range(0, threadcount-1)]
startTime = time.time()
runTime = runTime * 60
endTime = startTime+runTime


#query = f"select * from tpcc.customer where c_id between {start} and {start+20};"
debugInfo = []

allthreads = []
#check which query function to use
if dbType == "mysql":
    for num in range(0, threadcount-1):
        allthreads.append(threading.Thread(target=runQueryMySQL, args=(num, startTime, endTime)))
elif dbType == "postgresql":
    for num in range(0, threadcount-1):
        allthreads.append(threading.Thread(target=runQueryPostgreSQL, args=(num, startTime, endTime)))
else:
    print("Unsupported database type.")
    exit()

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
runtimeMinutes = runTime/60

# Write test results to file including date and time of test run
current_time = time.localtime()
# Format it as a date string
now = time.strftime("%Y-%m-%d %H:%M:%S", current_time)
resultsFile = open("./testresults.txt", "a")
resultsFile.write("\nTest run at: "+now)
resultsFile.write("\nParameters:\nTest Type: "+dbType+"\nTest duration: "+str(runtimeMinutes)+" minutes\n"+"Thread count: "+str(threadcount-1))
resultsFile.write(f"\nTotal queries finished in {runtimeMinutes} minutes: "+str(total))
resultsFile.write("\n"+str(total/runtimeMinutes)+" Queries per minute.\n\n")
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
