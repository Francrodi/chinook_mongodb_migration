from rdb import create_connection as pg_create_connection
from mongodb import create_connection as mongo_connection
from migrator import migrate_artists, migrate_albums, migrate_tracks
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    with pg_create_connection() as pg_conn:
        if pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print("Versión de PostgreSQL:", version)        
        mongo_client = mongo_connection()
        print(mongo_client.list_collection_names())
        print("Migrando artistas...")
        pg_artists_ids = migrate_artists(pg_conn, mongo_client)
        print("Migrando albumes...")
        pg_albums_ids = migrate_albums(pg_artists_ids, pg_conn, mongo_client)
        print("Migrando tracks...")
        pg_tracks_ids = migrate_tracks(pg_albums_ids, pg_conn, mongo_client)
        print(pg_tracks_ids[1])
        