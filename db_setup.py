import psycopg2
import config

def create_database():
    """
    Connects to PostgreSQL and creates the database if it does not exist.
    """
    try:
        # Connect to PostgreSQL without specifying a database
        conn = psycopg2.connect(
            dbname="postgres",
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        conn.autocommit = True  # Allow DB creation
        cur = conn.cursor()

        # Check if the database already exists
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{config.DB_NAME}'")
        exists = cur.fetchone()

        if not exists:
            cur.execute(f"CREATE DATABASE {config.DB_NAME}")
            print(f"Database '{config.DB_NAME}' created successfully!")
        else:
            print(f"Database '{config.DB_NAME}' already exists.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating database: {e}")

if __name__ == "__main__":
    create_database()
