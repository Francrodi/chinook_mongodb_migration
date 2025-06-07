from rdb import create_connection
from mongodb import mongo_connection
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    pg_conn = create_connection()
    if pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print("Versión de PostgreSQL:", version)
        pg_conn.close()