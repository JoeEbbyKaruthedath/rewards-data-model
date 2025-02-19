import json
import uuid
import psycopg2
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Function to check if barcode exists in dim_items
def check_item_exists(cur, barcode):
    cur.execute("SELECT EXISTS(SELECT 1 FROM dim_items WHERE barcode = %s);", (barcode,))
    return cur.fetchone()[0]  # Returns True if item exists, False otherwise

def get_brand_id(cur, brand_code):
    cur.execute("SELECT _id FROM dim_brands WHERE brand_code = %s;", (brand_code,))
    result = cur.fetchone()  # Fetch the first row from the result
    print(brand_code, ":", result)
    return result[0] if result else None  # Return the _id if found, otherwise return None


# Clean and transform JSON data for dim_items
def clean_items_data(file_path, cur):
    cleaned_data = []
    seen_barcodes = set()

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            try:
                record = json.loads(line.strip())

                # Extract purchased items from receipts
                for item in record.get("rewardsReceiptItemList", []):
                    barcode = item.get("barcode", None)
                    if not barcode or barcode in seen_barcodes:
                        continue  # Skip items without a barcode or duplicate barcodes

                    seen_barcodes.add(barcode)  # Track unique barcodes

                    # Generate item_id from barcode
                    #item_id = generate_item_id(barcode)

                    # Check if item already exists
                    if check_item_exists(cur, barcode):
                        print(f"⚠️ Skipping existing item: {barcode}")
                        continue

                    description = item.get("description", "UNKNOWN")
                    category = item.get("category", None)  # Some items may not have category
                    brand_code = item.get("brandCode", None)
                    brand_id = get_brand_id(cur, brand_code)

                    # Append cleaned record
                    cleaned_data.append(( barcode, description, category, brand_id))  # brand_id is NULL

            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line} - {e}")

    return cleaned_data

# Insert data into PostgreSQL
def insert_items_into_postgres(cleaned_data):
    conn = psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO dim_items (barcode, description, category, brand_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (barcode) DO NOTHING;
    """

    cur.executemany(insert_query, cleaned_data)
    conn.commit()
    cur.close()
    conn.close()

# Run the script
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "receipts.json"))

# Open DB connection once for efficiency
conn = psycopg2.connect(
    dbname=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASSWORD,
    host=config.DB_HOST,
    port=config.DB_PORT
)
cur = conn.cursor()

cleaned_data = clean_items_data(file_path, cur)
insert_items_into_postgres(cleaned_data)

# Close connection
cur.close()
conn.close()

print("Items inserted successfully!")
