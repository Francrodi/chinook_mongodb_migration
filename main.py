from rdb import create_connection as pg_create_connection
from mongodb import create_connection as mongo_connection
import migrator
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
        print("Migrando artistas...")
        pg_artists_ids = migrator.migrate_artists(pg_conn, mongo_client)
        print("Migrando albumes...")
        pg_albums_ids = migrator.migrate_albums(pg_artists_ids, pg_conn, mongo_client)
        print("Migrando tracks...")
        pg_tracks_ids = migrator.migrate_tracks(pg_albums_ids, pg_conn, mongo_client)
        print("Migrando playlists...")
        migrator.migrate_playlists(pg_tracks_ids, pg_conn, mongo_client)
        print("Migrando empleados...")
        pg_employees_id = migrator.migrate_employees(pg_conn, mongo_client)
        print("Migrando clientes...")
        pg_customers_id = migrator.migrate_customers(pg_employees_id, pg_conn, mongo_client)
        