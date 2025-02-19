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

# Convert MongoDB timestamp ($date) to PostgreSQL TIMESTAMP
def convert_mongo_date(timestamp):
    return datetime.fromtimestamp(timestamp / 1000) if timestamp else None

# Clean and transform JSON data
def clean_json_data(file_path):
    cleaned_data = []
    seen_ids = set()

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            try:
                record = json.loads(line.strip())

                # Extract values
                mongo_id = record["_id"]["$oid"]
                if mongo_id in seen_ids:
                    continue  # Avoid duplicates
                seen_ids.add(mongo_id)

                created_date = convert_mongo_date(record["createdDate"]["$date"]) if "createdDate" in record else None
                last_login = convert_mongo_date(record["lastLogin"]["$date"]) if "lastLogin" in record else None
                role = record.get("role", "consumer").upper()
                active = record.get("active", False)
                state = record.get("state", None)
                sign_up_source = record.get("signUpSource", None)

                # Append cleaned record
                cleaned_data.append((
                    convert_oid_to_uuid(mongo_id),  # Convert _id to UUID
                    state,
                    created_date,
                    last_login,
                    role,
                    active,
                    sign_up_source
                ))

            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line} - {e}")

    return cleaned_data

# Insert data into PostgreSQL
def insert_into_postgres(cleaned_data):
    conn = psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO dim_users (_id, state, createdDate, lastLogin, role, active, sign_up_source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (_id) DO NOTHING;
    """

    cur.executemany(insert_query, cleaned_data)
    conn.commit()
    cur.close()
    conn.close()

# Run the script
#file_path = "data/users.json"  # Adjust if needed
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "users.json"))
cleaned_data = clean_json_data(file_path)
insert_into_postgres(cleaned_data)

print("Data inserted successfully!")
