from rdb import create_connection as pg_create_connection, get_artist_songs as pg_artist_songs
from mongodb import MongoConnection
import mongodb
import migrator
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def migrate_v1(mongodb: MongoConnection):
    with pg_create_connection() as pg_conn:
        if pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print("Versión de PostgreSQL:", version)        
        print("Migrando artistas...")
        pg_artists_ids = migrator.migrate_artists(pg_conn, mongodb)
        print("Migrando albumes...")
        pg_albums_ids = migrator.migrate_albums(pg_artists_ids, pg_conn, mongodb)
        print("Migrando tracks...")
        pg_tracks_ids = migrator.migrate_tracks(pg_albums_ids, pg_conn, mongodb)
        print("Migrando playlists...")
        migrator.migrate_playlists(pg_tracks_ids, pg_conn, mongodb)
        print("Migrando empleados...")
        pg_employees_ids = migrator.migrate_employees(pg_conn, mongodb)
        print("Migrando clientes...")
        pg_customers_ids = migrator.migrate_customers(pg_employees_ids, pg_conn, mongodb)
        print("Migrando facturas...")
        migrator.migrate_invoices(pg_customers_ids, pg_tracks_ids, pg_conn, mongodb)
        print("Migración finalizada con éxito.")
    
if __name__ == "__main__":
    mongodb = MongoConnection()
    mongodb.setup_db()
    migrate_v1(mongodb)
    
    mongodb.get_artist_songs("AC/DC")
    mongodb.get_amount_of_songs_selled()
    mongodb.get_artists_in_genre("Rock")
    mongodb.songs_in_playlist("Music")
    mongodb.get_quantity_sold_tracks_by_artist()
    mongodb.get_songs_bougth_by_customer("684db5dab47b2bc9a8b156ee")
    mongodb.invoices_in_date_range(datetime(2021, 1, 1), datetime(2021, 2, 1))
    mongodb.get_genres_quantity_sold()
    mongodb.amount_sold_by_month()
    
    