import psycopg2
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from tabulate import tabulate

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASSWORD,
    host=config.DB_HOST,
    port=config.DB_PORT
)
cur = conn.cursor()


query = """
SELECT rewards_receipt_status,
    SUM(purchased_item_count) total_items_purchased
FROM fact_receipts
WHERE rewards_receipt_status IN ('FINISHED', 'REJECTED') -- There is no ACCEPTED STATUS, hence assuming that FINISHED = ACCEPTED
GROUP BY rewards_receipt_status
;
"""

cur.execute(query)

# Get column names
columns = [desc[0] for desc in cur.description]

# Fetch data
users = cur.fetchall()

# Print formatted table (handling empty data)
if users:
    print(tabulate(users, headers=columns, tablefmt="grid"))  # Use "grid" for better formatting
else:
    print("No data found.")

# Close connection
cur.close()
conn.close()
