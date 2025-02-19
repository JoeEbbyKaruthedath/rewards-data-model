import json
import psycopg2
import uuid
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config


# Convert MongoDB ObjectId to UUID
def convert_oid_to_uuid(oid):
    return str(uuid.UUID(int=int(oid[:16], 16)))  # Convert first 16 bytes to UUID

# Function to check if receipt exists in fact_receipts
def check_receipt_exists(cur, receipt_id):
    cur.execute("SELECT EXISTS(SELECT 1 FROM fact_receipts WHERE _id = %s);", (receipt_id,))
    return cur.fetchone()[0]

# Function to get item_id from dim_items
def get_item_id(cur, barcode):
    cur.execute("SELECT _id FROM dim_items WHERE barcode = %s;", (barcode,))
    result = cur.fetchone()
    return result[0] if result else None  # Return item_id if exists, else None

# Clean and transform JSON data for fact_receipt_items
def clean_receipt_items_data(file_path, cur):
    cleaned_data = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            try:
                record = json.loads(line.strip())

                # Extract receipt_id
                receipt_id = record["_id"]["$oid"]
                receipt_id = convert_oid_to_uuid(receipt_id)

                # Skip receipt if it doesn't exist in fact_receipts
                if not check_receipt_exists(cur, receipt_id):
                    print(f"Skipping items for missing receipt {receipt_id}.")
                    continue
                
                # Extract purchased items
                for item in record.get("rewardsReceiptItemList", []):
                    barcode = item.get("barcode", None)
                    if not barcode:
                        continue  # Skip items without a barcode

                    # Get item_id from dim_items
                    item_id = get_item_id(cur, barcode)

                    final_price = float(item.get("finalPrice", 0))
                    item_price = float(item.get("itemPrice", 0))
                    quantity_purchased = int(item.get("quantityPurchased", 1))
                    user_flagged_barcode = item.get("userFlaggedBarcode", None)
                    user_flagged_new_item = bool(item.get("userFlaggedNewItem", False))
                    user_flagged_price = float(item.get("userFlaggedPrice", 0)) if "userFlaggedPrice" in item else None
                    user_flagged_quantity = int(item.get("userFlaggedQuantity", 1))
                    needs_fetch_review = bool(item.get("needsFetchReview", False))
                    rewards_group = item.get("rewardsGroup", None)
                    rewards_product_partner_id = item.get("rewardsProductPartnerId", None)

                    if rewards_product_partner_id == None:
                        rewards_product_partner_id = None
                    else:
                        rewards_product_partner_id = convert_oid_to_uuid(rewards_product_partner_id)

                    cleaned_data.append((
                        receipt_id,
                        item_id,  # item_id from dim_items (may be NULL)
                        final_price,
                        item_price,
                        quantity_purchased,
                        user_flagged_barcode,
                        user_flagged_new_item,
                        user_flagged_price,
                        user_flagged_quantity,
                        needs_fetch_review,
                        rewards_group,
                        rewards_product_partner_id
                    ))

            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line} - {e}")

    return cleaned_data

# Insert data into PostgreSQL
def insert_receipt_items_into_postgres(cleaned_data):
    conn = psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO fact_receipt_items (
            receipt_id, item_id, final_price, item_price, quantity_purchased,
            user_flagged_barcode, user_flagged_new_item, user_flagged_price, 
            user_flagged_quantity, needs_fetch_review, rewards_group, rewards_product_partner_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
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

cleaned_data = clean_receipt_items_data(file_path, cur)
insert_receipt_items_into_postgres(cleaned_data)

# Close connection
cur.close()
conn.close()

print("Receipt items inserted successfully!")
