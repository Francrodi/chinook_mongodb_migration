from rdb import create_connection as pg_create_connection
from mongodb import create_connection as mongo_connection
import migrator
from dotenv import load_dotenv

load_dotenv()

def migrate_v1(mongo_client):
    with pg_create_connection() as pg_conn:
        if pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print("Versión de PostgreSQL:", version)        
        print("Migrando artistas...")
        pg_artists_ids = migrator.migrate_artists(pg_conn, mongo_client)
        print("Migrando albumes...")
        pg_albums_ids = migrator.migrate_albums(pg_artists_ids, pg_conn, mongo_client)
        print("Migrando tracks...")
        pg_tracks_ids = migrator.migrate_tracks(pg_albums_ids, pg_conn, mongo_client)
        print("Migrando playlists...")
        migrator.migrate_playlists(pg_tracks_ids, pg_conn, mongo_client)
        print("Migrando empleados...")
        pg_employees_ids = migrator.migrate_employees(pg_conn, mongo_client)
        print("Migrando clientes...")
        pg_customers_ids = migrator.migrate_customers(pg_employees_ids, pg_conn, mongo_client)
        print("Migrando facturas...")
        migrator.migrate_invoices(pg_customers_ids, pg_tracks_ids, pg_conn, mongo_client)
        print("Migración finalizada con éxito.")
    
if __name__ == "__main__":
    mongo_client = mongo_connection()
    migrate_v1(mongo_client)
    