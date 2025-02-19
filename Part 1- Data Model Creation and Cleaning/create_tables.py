import psycopg2
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

def create_tables():
    """
    Creates Users, Receipts, Brands, and Receipt Items tables in PostgreSQL.
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

        # Drop existing tables to avoid conflicts
        cur.execute("DROP TABLE IF EXISTS fact_receipt_items CASCADE;")
        cur.execute("DROP TABLE IF EXISTS fact_receipts CASCADE;")
        cur.execute("DROP TABLE IF EXISTS dim_items CASCADE;")
        cur.execute("DROP TABLE IF EXISTS dim_users CASCADE;")
        cur.execute("DROP TABLE IF EXISTS dim_brands CASCADE;")

        # Create Users Table (Dimension)
        cur.execute("""
            CREATE TABLE dim_users (
                _id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                state VARCHAR(2),
                createdDate TIMESTAMP,
                lastLogin TIMESTAMP,
                role VARCHAR(20) DEFAULT 'CONSUMER',
                active BOOLEAN,
                sign_up_source TEXT
            );
        """)

        # Create Receipts Table (Fact Table)
        cur.execute("""
            CREATE TABLE fact_receipts (
                _id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID REFERENCES dim_users(_id) ON DELETE CASCADE,
                purchase_date TIMESTAMP,
                date_scanned TIMESTAMP,
                create_date TIMESTAMP,
                modify_date TIMESTAMP,
                finished_date TIMESTAMP,
                points_awarded_date TIMESTAMP,
                points_earned NUMERIC(10,2),
                bonus_points_earned INT,
                bonus_points_reason TEXT,
                purchased_item_count INT,
                total_spent NUMERIC(10,2),
                rewards_receipt_status TEXT
            );
        """)

        # Create Brands Table (Dimension)
        cur.execute("""
            CREATE TABLE dim_brands (
                _id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                barcode VARCHAR(50) UNIQUE,
                brand_code VARCHAR(50),
                category VARCHAR(100),
                category_code VARCHAR(50),
                cpg UUID,
                top_brand BOOLEAN,
                name VARCHAR(100) NOT NULL
            );
        """)

        # Create Items Table (Dimension)
        cur.execute("""
            CREATE TABLE dim_items (
                _id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                barcode VARCHAR(50) UNIQUE,
                description TEXT,
                category TEXT,
                brand_id UUID REFERENCES dim_brands(_id) ON DELETE SET NULL
            );
        """)

        # Create Receipt Items Table (Fact Table - Purchased Items)
        cur.execute("""
            CREATE TABLE fact_receipt_items (
                _id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                receipt_id UUID REFERENCES fact_receipts(_id) ON DELETE CASCADE,
                item_id UUID REFERENCES dim_items(_id) ON DELETE CASCADE,
                final_price NUMERIC(10,2),
                item_price NUMERIC(10,2),
                quantity_purchased INT,
                user_flagged_barcode VARCHAR(50),
                user_flagged_new_item BOOLEAN,
                user_flagged_price NUMERIC(10,2),
                user_flagged_quantity INT,
                needs_fetch_review BOOLEAN,
                rewards_group TEXT,
                rewards_product_partner_id UUID
            );
        """)

        conn.commit()
        cur.close()
        conn.close()
        print("Tables created successfully!")

    except Exception as e:
        print(f"Error creating tables: {e}")

if __name__ == "__main__":
    create_tables()
