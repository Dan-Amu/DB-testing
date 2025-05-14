import mysql.connector
import uuid
import random
from datetime import datetime, timedelta

# Configuration
num_iterations = 10000  # Number of iterations
batch_size = 100     # Number of records per batch

# Database connection
conn = mysql.connector.connect(
    host='192.168.0.10',
    user='root',
    password='voxicon',
    database='test123'
)
cursor = conn.cursor()

# Create tables if they do not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    stock INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS payments (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    amount DECIMAL(10,2),
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
)
""")
conn.commit()
print("tables created")
for i in range(num_iterations):
    print("Starting iteration: "+str(i))
    # Insert batch_size new customers
    customers = [
        (f'Customer_{uuid.uuid4()}', f'{uuid.uuid4()}@example.com')
        for _ in range(batch_size)
    ]
    cursor.executemany(
        "INSERT INTO customers (name, email) VALUES (%s, %s)", customers
    )
    conn.commit()
    # Insert batch_size new products
    products = [
        (f'Product_{uuid.uuid4()}', round(random.uniform(1, 100), 2), random.randint(1, 100))
        for _ in range(batch_size)
    ]
    cursor.executemany(
        "INSERT INTO products (name, price, stock) VALUES (%s, %s, %s)", products
    )
    conn.commit()

    # Fetch customer and product IDs
    cursor.execute("SELECT customer_id FROM customers ORDER BY RAND() LIMIT %s", (batch_size,))
    customer_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT product_id, price FROM products ORDER BY RAND() LIMIT %s", (batch_size,))
    product_data = cursor.fetchall()

    # Create orders, order_items, and payments
    for customer_id in customer_ids:
        cursor.execute("INSERT INTO orders (customer_id) VALUES (%s)", (customer_id,))
        order_id = cursor.lastrowid

        product_id, price = random.choice(product_data)
        quantity = random.randint(1, 5)
        amount = price * quantity

        cursor.execute(
            "INSERT INTO order_items (order_id, product_id, quantity) VALUES (%s, %s, %s)",
            (order_id, product_id, quantity)
        )
        cursor.execute(
            "INSERT INTO payments (order_id, amount) VALUES (%s, %s)",
            (order_id, amount)
        )
    conn.commit()

    # Perform SELECT with JOINs
   # cursor.execute("""
   #     SELECT o.order_id, c.name AS customer_name, p.name AS product_name, oi.quantity, pay.amount
   #     FROM orders o
   #     JOIN customers c ON o.customer_id = c.customer_id
   #     JOIN order_items oi ON o.order_id = oi.order_id
   #     JOIN products p ON oi.product_id = p.product_id
   #     JOIN payments pay ON o.order_id = pay.order_id
   #     ORDER BY o.order_date DESC
   #     LIMIT 10
   # """)
   # results = cursor.fetchall()
   # for row in results:
   #     print(row)

   # # Perform UPDATE on products
   # cursor.execute("SELECT product_id FROM products ORDER BY RAND() LIMIT %s", (batch_size,))
   # product_ids = [row[0] for row in cursor.fetchall()]
   # for pid in product_ids:
   #     cursor.execute("UPDATE products SET stock = stock + 10 WHERE product_id = %s", (pid,))
   # conn.commit()

   # # Perform DELETE on old payments
   # cutoff_date = datetime.now() - timedelta(days=1)
   # cursor.execute("DELETE FROM payments WHERE payment_date < %s", (cutoff_date,))
   # conn.commit()

# Close connection
cursor.close()
conn.close()
