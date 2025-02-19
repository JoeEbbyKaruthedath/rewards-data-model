import psycopg2
import config

def create_table():
    """
    Creates a sample table in the database.
    """
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        cur = conn.cursor()

        # cur.execute("""
        #     CREATE TABLE receipts (
        #         _id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        #         bonusPointsEarned INT,
        #         bonusPointsEarnedReason TEXT,
        #         createDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         dateScanned TIMESTAMP,
        #         finishedDate TIMESTAMP,
        #         modifyDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         pointsAwardedDate TIMESTAMP,
        #         pointsEarned INT,
        #         purchaseDate TIMESTAMP,
        #         purchasedItemCount INT,
        #         rewardsReceiptStatus TEXT,
        #         totalSpent DECIMAL(10,2)
        # );

        # """)
        #cur.execute("DROP TABLE IF EXISTS brands CASCADE;")

        conn.commit()
        cur.close()
        conn.close()
        print("Table 'users' created successfully!")

    except Exception as e:
        print(f"Error creating table: {e}")

if __name__ == "__main__":
    create_table()
