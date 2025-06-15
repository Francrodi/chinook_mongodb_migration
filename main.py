from rdb import PostgresHandler
from mongodb import MongoConnection
import mongodb
import migrator
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def migrate_v1(mongodb: MongoConnection, pg_handler: PostgresHandler): 
    print("Migrando artistas...")
    pg_artists_ids = migrator.migrate_artists(pg_handler, mongodb)
    print("Migrando albumes...")
    pg_albums_ids = migrator.migrate_albums(pg_artists_ids, pg_handler, mongodb)
    print("Migrando tracks...")
    pg_tracks_ids = migrator.migrate_tracks(pg_albums_ids, pg_handler, mongodb)
    print("Migrando playlists...")
    migrator.migrate_playlists(pg_tracks_ids, pg_handler, mongodb)
    print("Migrando empleados...")
    pg_employees_ids = migrator.migrate_employees(pg_handler, mongodb)
    print("Migrando clientes...")
    pg_customers_ids = migrator.migrate_customers(pg_employees_ids, pg_handler, mongodb)
    print("Migrando facturas...")
    migrator.migrate_invoices(pg_customers_ids, pg_tracks_ids, pg_handler, mongodb)
    print("Migración finalizada con éxito.")
    
if __name__ == "__main__":
    mongodb = MongoConnection()
    mongodb.setup_db()
    pg_handler = PostgresHandler()
    migrate_v1(mongodb, pg_handler)
    
    # Test queries mongo:
    print("MongoDB: ")
    mongodb.get_artist_songs("AC/DC")
    mongodb.get_amount_of_songs_selled()
    mongodb.get_artists_in_genre("Rock")
    mongodb.songs_in_playlist("Music")
    mongodb.get_quantity_sold_tracks_by_artist()
    mongodb.get_songs_bought_by_customer(mongodb.get_customer_id_from_last_name("Hansen"))
    mongodb.invoices_in_date_range(datetime(2021, 1, 1), datetime(2021, 2, 4))
    mongodb.get_genres_quantity_sold()
    mongodb.amount_sold_by_month()
    
    # Test queries pg:
    print("Postgres: ")
    pg_handler.get_artist_songs("AC/DC")
    pg_handler.get_amount_of_songs_selled()
    pg_handler.get_artists_in_genre("Rock")
    pg_handler.songs_in_playlist("Music")
    pg_handler.get_quantity_sold_tracks_by_artist()
    pg_handler.get_songs_bought_by_customer("4")
    pg_handler.invoices_in_date_range(datetime(2021, 1, 1), datetime(2021, 2, 4))
    pg_handler.get_genres_quantity_sold()
    pg_handler.amount_sold_by_month()
    
    
    