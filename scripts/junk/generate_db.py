import psycopg2
from faker import Faker
import io
import time
import sys

# Connection details
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "8549"


def ensure_table(conn):
    cur = conn.cursor()

    # Create a sample table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user2 (
                id INTEGER PRIMARY KEY,
                name VARCHAR PRIMARY KEY,
                email VARCHAR,

                address TEXT,
                created_at TIMESTAMP
            );
        """)
    print("Table ensured to exist.")

def generate_and_insert_data(conn, num_records, start_id):
    """
    Generates mock data and inserts it into PostgreSQL using copy_from for performance.
    """
    cur = conn.cursor()
    fake = Faker()
        
    # Use StringIO as an in-memory CSV file
    csv_buffer = io.StringIO()
    cur_id = start_id
    for _ in range(num_records):
        id = start_id
        name = fake.name().replace(',', '') # Remove commas to avoid CSV issues
        email = fake.email()
        address = fake.address().replace('\n', ' ').replace(',', '')
        # Format data into a CSV line
        csv_buffer.write(f"{id},{name},{email},{address}\n")
        start_id = start_id + 1
            
    # Move the "file" pointer back to the beginning
    csv_buffer.seek(0)
    start_time = time.time()
    # Use copy_from to efficiently insert the data
    cur.copy_from(
        csv_buffer,
        'users',
        columns=('id', 'name', 'email', 'address'),
        sep=','
    )
        
    end_time = time.time()
    print(f"Inserted {num_records} records in {end_time - start_time:.2f} seconds using copy_from.")

if __name__ == "__main__":
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        # Set autocommit to True for simplicity, but in a real app you might manage transactions
        conn.autocommit = True
        ensure_table(conn)
        total = int(sys.argv[1])
        current = 0
        while current < total:
            batch = 100000
            if current + batch > total:
                batch = total - current
            generate_and_insert_data(conn, batch, current + int(sys.argv[2]))
            current = current + batch

    except psycopg2.OperationalError as e:
        print(f"Could not connect to the database: {e}")
        print("Please check your connection details and ensure the database is running.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
