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

# Fetch all users
query = """
WITH recent_month AS (
    SELECT DATE_TRUNC('month', MAX(fr.date_scanned)) AS latest_month
    FROM fact_receipts fr
    LEFT JOIN fact_receipt_items fri ON fr._id = fri.receipt_id
    LEFT JOIN dim_items di ON fri.item_id = di._id
    LEFT JOIN dim_brands db ON di.brand_id = db._id
    where brand_id is not null
),

receipts_raw AS (
SELECT fr._id receipt_id,
    fr.date_scanned,
    fri.final_price,
    di.brand_id,
    db.name
FROM fact_receipts fr
LEFT JOIN fact_receipt_items fri ON fr._id = fri.receipt_id
LEFT JOIN dim_items di ON fri.item_id = di._id
LEFT JOIN dim_brands db ON di.brand_id = db._id
where brand_id is not null
    AND fr.date_scanned < (SELECT * FROM recent_month)
    AND fr.date_scanned >= (SELECT latest_month FROM recent_month) - INTERVAL '1 month'
)

SELECT brand_id,
    name,
    max(date_scanned),
    count(*)
FROM receipts_raw
GROUP BY brand_id, name
ORDER BY count(*) DESC
LIMIT 5


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
