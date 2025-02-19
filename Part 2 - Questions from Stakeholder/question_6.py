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
WITH latest_date AS (
    SELECT MAX(createdDate) AS latest_created_date
    FROM dim_users
),
user_past_6_months AS (
    SELECT *
    FROM dim_users
    WHERE createdDate >= (SELECT latest_created_date FROM latest_date) - INTERVAL '6 months'
),

receipts_from_user_past_6_months AS (
SELECT fr._id receipt_id,
    fr.user_id,
    fr.date_scanned,
    fri.final_price,
    di.brand_id,
    db.name
FROM fact_receipts fr
LEFT JOIN fact_receipt_items fri ON fr._id = fri.receipt_id
LEFT JOIN dim_items di ON fri.item_id = di._id
LEFT JOIN dim_brands db ON di.brand_id = db._id
where di.brand_id IS NOT NULL
    AND db.name IS NOT NULL
    AND user_id IN (SELECT _id FROM user_past_6_months)
)

SELECT name as brand_name,
    count(distinct receipt_id) as num_transactions
FROM receipts_from_user_past_6_months
GROUP BY brand_name
ORDER BY  num_transactions DESC
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
