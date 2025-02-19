import json
import uuid
import psycopg2
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Convert MongoDB ObjectId to UUID
def convert_oid_to_uuid(oid):
    return str(uuid.UUID(int=int(oid[:16], 16)))  # Truncate to 16 bytes

# Clean and transform JSON data
def clean_brands_data(file_path):
    cleaned_data = []
    seen_ids = set()
    seen_barcodes = set()

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            try:
                record = json.loads(line.strip())

                # Extract values
                mongo_id = record["_id"]["$oid"]
                barcode = record["barcode"]
                if mongo_id in seen_ids:
                    continue  # Avoid duplicates
                if barcode in seen_barcodes:
                    continue # Avoid duplicates
                seen_ids.add(mongo_id)
                seen_barcodes.add(barcode)

                barcode = record.get("barcode", None)
                brand_code = record.get("brandCode", None)
                category = record.get("category", None)
                category_code = record.get("categoryCode", None)
                cpg_id = record.get("cpg", {}).get("$id", {}).get("$oid", None)  # Extract CPG ID if present
                name = record.get("name", None)
                top_brand = record.get("topBrand", False)  # Default to False if missing

                # Append cleaned record
                cleaned_data.append((
                    convert_oid_to_uuid(mongo_id),  # Convert _id to UUID
                    barcode,
                    brand_code,
                    category,
                    category_code,
                    convert_oid_to_uuid(cpg_id) if cpg_id else None,  # Convert cpg ID if present
                    top_brand,
                    name
                ))

            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line} - {e}")

    return cleaned_data

# Insert data into PostgreSQL
def insert_brands_into_postgres(cleaned_data):
    conn = psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO dim_brands (_id, barcode, brand_code, category, category_code, cpg, top_brand, name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (_id) DO NOTHING;
    """

    cur.executemany(insert_query, cleaned_data)
    conn.commit()
    cur.close()
    conn.close()

# Run the script
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "brands.json"))
cleaned_data = clean_brands_data(file_path)
insert_brands_into_postgres(cleaned_data)

print("Brands data inserted successfully!")
