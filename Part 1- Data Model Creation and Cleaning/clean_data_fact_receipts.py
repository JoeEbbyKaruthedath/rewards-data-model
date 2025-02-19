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
    return str(uuid.UUID(int=int(oid[:16], 16)))  # Convert first 16 bytes to UUID

# Convert MongoDB timestamp ($date) to PostgreSQL TIMESTAMP
def convert_mongo_date(timestamp):
    return datetime.fromtimestamp(timestamp / 1000) if timestamp else None

# Function to check if user_id exists in dim_users
def check_user_exists(cur, user_id):
    cur.execute("SELECT EXISTS(SELECT 1 FROM dim_users WHERE _id = %s);", (user_id,))
    return cur.fetchone()[0]  # Returns True if user exists, False otherwise

# Clean and transform JSON data for fact_receipts
def clean_receipts_data(file_path, cur):
    cleaned_data = []
    seen_receipt_ids = set()

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            try:
                record = json.loads(line.strip())

                # Extract values
                mongo_id = record["_id"]["$oid"]
                if mongo_id in seen_receipt_ids:
                    continue  # Avoid duplicates
                seen_receipt_ids.add(mongo_id)

                user_id = record.get("userId", None)
                if user_id:
                    user_id = convert_oid_to_uuid(user_id)
                    
                    # Skip the record if user does not exist
                    if not check_user_exists(cur, user_id):
                        print(f"Skipping receipt {mongo_id} because user {user_id} is missing in dim_users.")
                        continue  # Skip this receipt

                purchase_date = convert_mongo_date(record["purchaseDate"]["$date"]) if "purchaseDate" in record else None
                date_scanned = convert_mongo_date(record["dateScanned"]["$date"]) if "dateScanned" in record else None
                create_date = convert_mongo_date(record["createDate"]["$date"]) if "createDate" in record else None
                modify_date = convert_mongo_date(record["modifyDate"]["$date"]) if "modifyDate" in record else None
                finished_date = convert_mongo_date(record["finishedDate"]["$date"]) if "finishedDate" in record else None
                points_awarded_date = convert_mongo_date(record["pointsAwardedDate"]["$date"]) if "pointsAwardedDate" in record else None
                points_earned = float(record.get("pointsEarned", 0))
                bonus_points_earned = int(record.get("bonusPointsEarned", 0))
                bonus_points_reason = record.get("bonusPointsEarnedReason", None)
                purchased_item_count = int(record.get("purchasedItemCount", 0))
                total_spent = float(record.get("totalSpent", 0))
                rewards_receipt_status = record.get("rewardsReceiptStatus", None)

                # Append cleaned record
                cleaned_data.append((
                    convert_oid_to_uuid(mongo_id),  # Convert _id to UUID
                    user_id,  # Converted userId
                    purchase_date,
                    date_scanned,
                    create_date,
                    modify_date,
                    finished_date,
                    points_awarded_date,
                    points_earned,
                    bonus_points_earned,
                    bonus_points_reason,
                    purchased_item_count,
                    total_spent,
                    rewards_receipt_status
                ))

            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line} - {e}")

    return cleaned_data

# Insert data into PostgreSQL
def insert_receipts_into_postgres(cleaned_data):
    conn = psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO fact_receipts (
            _id, user_id, purchase_date, date_scanned, create_date, modify_date,
            finished_date, points_awarded_date, points_earned, bonus_points_earned,
            bonus_points_reason, purchased_item_count, total_spent, rewards_receipt_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (_id) DO NOTHING;
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

cleaned_data = clean_receipts_data(file_path, cur)
insert_receipts_into_postgres(cleaned_data)

# Close connection
cur.close()
conn.close()

print("Receipts data inserted successfully!")
