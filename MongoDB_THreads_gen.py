import pymongo
import uuid
import random
from datetime import datetime, timedelta
import threading
import time

# Configuration
num_iterations = 67 # Number of iterations
batch_size = 800 # Number of records per batch
threadcount = 70 # Number of simultaneous threads

def threadPrint(threadID, texttoprint):
    print(f'Thread: {threadID} ', texttoprint)

# Database connection
def databaseConnection():
    conn = pymongo.MongoClient('mongodb://192.168.0.16:27017')
    return conn
#conn = databaseConnection()
#db = conn["dbtest1"]

def creationquery(tID):
    
    conn = databaseConnection()
    db = conn["dbtest1"]
   # conn = psycopg2.connect(
   #     host='192.168.0.10',
   #     user='postgres',
   #     password='qaz123',
   #     dbname='dbtest1'
   #     )
   for i in range(num_iterations):
       collection = db["customers"]
       customers = [
           {
               "name": f"Customer_{uuid.uuid4()}",
               "email": f"{uuid.uuid4()}@example.com"
           }
           for _ in range(batch_size)
       ]
        collection.insert_many(customers)

        threadPrint(tID, str(batch_size)+ " customers created "+ str(i)+ " times")
        # Insert batch_size new products

        collection = db["products"]
        products = [
            {
                "name": f'Product_{uuid.uuid4()}',
                "price": round(random.uniform(1, 100), 2),
                "stock": random.randint(1, 100))
            for _ in range(batch_size)
        ]
        collection.insert_many(products)
        threadPrint(tID, str(batch_size)+ " products created "+ str(i)+ " times")

        # Fetch customer and product IDs
        collection = db["customers"]
        random_docs = collection.aggregate([
            {"$sample": {"size": batch_size}},
            {"$project": {"_id": 1}}
        ])
        customer_ids = [doc["_id"] for doc in random_docs]
        
        collection = db["products"]
        random_docs = collection.aggregate([
            {"$sample": {"size": batch_size}},
            {"$project": {"_id": 1, "price": 1}}
        ])
        product_data = [
                [doc["_id"], for doc in random_docs],
                [doc["price"], for doc in random_docs]
                ]

        # Create orders, order_items, and payments
        for customer_id in customer_ids:

            collection = db["orders"]
            order_id = collection.insert_one({"customer": customer_id}
            
            #cursor.execute("INSERT INTO orders (customer_id) VALUES (%s)", (customer_id,))
    
            product_id, price = random.choice(product_data)
            quantity = random.randint(1, 5)
            amount = price * quantity
            collection = db["order_items"]
            collection.insert_one({
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity
                })
            #cursor.execute(
                #"INSERT INTO order_items (order_id, product_id, quantity) VALUES (%s, %s, %s)",
                #(order_id, product_id, quantity)
            #)
            collection = db["payments"]
            collection.insert_one({
                "order_id": order_id,
                "amount": amount
                })
      #      cursor.execute(
      #          "INSERT INTO payments (order_id, amount) VALUES (%s, %s)",
      #          (order_id, amount)
      #      )
      #  conn.commit()
        threadPrint(tID, str(batch_size)+ " orders created "+ str(i)+ " times")
    
    #    # Perform SELECT with JOINs
    #    cursor.execute("""
    #        SELECT o.order_id, c.name AS customer_name, p.name AS product_name, oi.quantity, pay.amount
    #        FROM orders o
    #        JOIN customers c ON o.customer_id = c.customer_id
    #        JOIN order_items oi ON o.order_id = oi.order_id
    #        JOIN products p ON oi.product_id = p.product_id
    #        JOIN payments pay ON o.order_id = pay.order_id
    #        ORDER BY o.order_date DESC
    #        LIMIT 10
    #    """)
    #    results = cursor.fetchall()
    #    for row in results:
    #        print(row)
    
    #    # Perform UPDATE on products
    #    cursor.execute("SELECT product_id FROM products ORDER BY RAND() LIMIT %s", (batch_size,))
    #    product_ids = [row[0] for row in cursor.fetchall()]
    #    for pid in product_ids:
    #        cursor.execute("UPDATE products SET stock = stock + 10 WHERE product_id = %s", (pid,))
    #    conn.commit()
    #
    #    # Perform DELETE on old payments
    #    cutoff_date = datetime.now() - timedelta(days=1)
    #    cursor.execute("DELETE FROM payments WHERE payment_date < %s", (cutoff_date,))
    #    conn.commit()
    #
    # Close connection
    cursor.close()
    conn.close()
allthreads = []
for num in range(0, threadcount):
    allthreads.append(threading.Thread(target=creationquery, args=(num, )))
for num in range(0, threadcount):
    time.sleep(0.05)
    allthreads[num].start()
    print("Started thread: ", num)

