import psycopg2
import os

# other users make a .env file and fill it 
DB_CONFIG = {
        'dbname': os.getenv('POSTGRES_DB', 'tweedbt'),
        'user': os.getenv('POSTGRES_USER', 'kk'),
        'password': os.getenv('POSTGRES_PASSWORD', 'admin'),
        'host': os.getenv('POSTGRES_HOST', '127.0.0.1'),
        'port': os.getenv('POSTGRES_PORT', '5432')
        }
# Connection details
conn = psycopg2.connect(**DB_CONFIG)
# Create a cursor
cur = conn.cursor()

# Run a simple query
cur.execute("SELECT version();")

# Fetch and print result
db_version = cur.fetchone()
print("Connected to:", db_version)

# Always close!
cur.close()
conn.close()
