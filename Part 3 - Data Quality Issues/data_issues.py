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

# 18% of users are missing last login date
query1 = """
SELECT COUNT(CASE WHEN lastlogin IS NULL THEN 1 END) * 1.0 / COUNT(*) AS proportion_null_lastlogin
FROM dim_users;
;
"""
cur.execute(query1)
# Get column names
columns = [desc[0] for desc in cur.description]
# Fetch data
users = cur.fetchall()
# Print formatted table (handling empty data)
if users:
    print(tabulate(users, headers=columns, tablefmt="grid"))  # Use "grid" for better formatting
else:
    print("No data found.")




#23% of brand_code is missing, this is critical to link the receipts table to brands, there is no brand_id in the receipts.json
#Hence brand_code missing means we cannot link 23% of items. 
#Ideally brand_id is part of receipts and there are no/ very low number of missing brand_id's
query2 = """
SELECT COUNT(CASE WHEN brand_code IS NULL THEN 1 END) * 1.0 / COUNT(*) as proportion_null_brand_code
FROM dim_brands
LIMIT 10
;
"""
cur.execute(query2)
# Get column names
columns = [desc[0] for desc in cur.description]
# Fetch data
users = cur.fetchall()
# Print formatted table (handling empty data)
if users:
    print(tabulate(users, headers=columns, tablefmt="grid"))  # Use "grid" for better formatting
else:
    print("No data found.")



#51 records have more than 150 day gap between purchase date and scan date, which do not sound correct, some records have more than 1000 days of difference
query3 = """

WITH day_diff AS(
SELECT 
    DATE(date_scanned) AS scanned_date, 
    DATE(purchase_date) AS purchased_date,
    DATE(date_scanned) - DATE(purchase_date) AS day_difference
FROM fact_receipts
WHERE DATE(date_scanned) - DATE(purchase_date) > 100
)

SELECT COUNT(*) FROM day_diff;

;
"""
cur.execute(query3)
# Get column names
columns = [desc[0] for desc in cur.description]
# Fetch data
users = cur.fetchall()
# Print formatted table (handling empty data)
if users:
    print(tabulate(users, headers=columns, tablefmt="grid"))  # Use "grid" for better formatting
else:
    print("No data found.")


#88% of brand_id is null, this means we are not able to connect 88% of the recipt items to the respective brands
query4 = """
SELECT COUNT(CASE WHEN brand_id IS NULL THEN 1 END)*1.0/ COUNT(*) proportion_of_brand_id_null
FROM dim_items
;
"""
cur.execute(query4)
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
