from rdb import create_connection as pg_create_connection
from mongodb import create_connection as mongo_connection
from migrator import migrate_artists
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    with pg_create_connection() as pg_conn:
        if pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print("Versión de PostgreSQL:", version)        
        mongo_conn = mongo_connection()
        print(mongo_conn.list_collection_names())
        print("Migrando artistas...")
        pg_id_artists = migrate_artists(pg_conn, mongo_conn)
        print(pg_id_artists)
        