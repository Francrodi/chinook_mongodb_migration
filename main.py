from rdb import PostgresHandler
from mongodb import MongoConnection
import mongodb
import migrator
from optimizer import MongoOptimizer
from mongodb_benchmark_helper import DatabaseBenchmark
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
    optimizer = MongoOptimizer(mongodb)
    pg_benchmark = DatabaseBenchmark("PostgreSQL")
    mongo_benchmark = DatabaseBenchmark("MongoDB")
    mongo_benchmark1 = DatabaseBenchmark("MongoDBIndex")
    
    
    migrate_v1(mongodb, pg_handler)
    
    artist_params = [{"artist": i} for i in ["AC/DC", "Alice In Chains", "Led Zeppelin", "Yo-Yo Ma", "Whitesnake"]]
    artist_genre_params = [{"genre": i} for i in ["Rock", "Metal", "Classical", "Drama", "Easy Listening"]]
    playlist_songs_params = [{"playlist": i} for i in ["Music", "Movies", "Grunge", "TV Shows", "Classical 101 - Deep Cuts"]]
    customer_songs_params = [{"customer_id": i} for i in ["4", "7", "8", "22", "51"]]
    mongo_db_customer_songs_params = [{"customer_id": mongodb.get_customer_id_from_last_name(i)} for i in ["Hansen", "Gruber", "Peeters", "Leacock", "Johansson"]]
    
    invoices_date_range_params = [{"start_date": i, "end_date": j} for (i, j) in [
        (datetime(2021, 1, 1), datetime(2021, 2, 1)),
        (datetime(2021, 3, 1), datetime(2021, 4, 1)),
        (datetime(2022, 5, 10), datetime(2022, 5, 20)),
        (datetime(2023, 2, 9), datetime(2022, 6, 1)),
        (datetime(2024, 1, 1), datetime(2025, 1, 1))
    ]]
    
    print("Benchmark PostgreSQL")
    
    pg_benchmark.benchmark_query(pg_handler.get_artist_songs, "Canciones de un artista", param_list=artist_params, iterations=100)
    pg_benchmark.benchmark_query(pg_handler.get_amount_of_songs_selled, "Cantidad de ventas por cancion", iterations=100)
    pg_benchmark.benchmark_query(pg_handler.get_artists_in_genre, "Artistas segun genero", param_list=artist_genre_params, iterations=100)
    pg_benchmark.benchmark_query(pg_handler.songs_in_playlist, "Canciones en playlist", param_list=playlist_songs_params, iterations=100)
    pg_benchmark.benchmark_query(pg_handler.get_quantity_sold_tracks_by_artist, "Cantidad de canciones vendidas por artista", iterations=100)
    pg_benchmark.benchmark_query(pg_handler.get_songs_bought_by_customer, "Canciones compradas por cliente", param_list=customer_songs_params, iterations=100)
    pg_benchmark.benchmark_query(pg_handler.invoices_in_date_range, "Ventas en periodo de tiempo", param_list=invoices_date_range_params, iterations=100)
    pg_benchmark.benchmark_query(pg_handler.get_genres_quantity_sold, "Ventas segun genero", iterations=100)
    pg_benchmark.benchmark_query(pg_handler.amount_sold_by_month, "Ventas segun mes", iterations=100)
    
    print("Benchmark MongoDB")
  
    mongo_benchmark.benchmark_query(mongodb.get_artist_songs, "Canciones de un artista", param_list=artist_params, iterations=100)
    mongo_benchmark.benchmark_query(mongodb.get_amount_of_songs_selled, "Cantidad de ventas por cancion", iterations=100)
    mongo_benchmark.benchmark_query(mongodb.get_artists_in_genre, "Artistas segun género", param_list=artist_genre_params, iterations=100)
    mongo_benchmark.benchmark_query(mongodb.songs_in_playlist, "Canciones en playlist", param_list=playlist_songs_params, iterations=100)
    mongo_benchmark.benchmark_query(mongodb.get_quantity_sold_tracks_by_artist, "Cantidad de canciones vendidas por artista", iterations=100)
    mongo_benchmark.benchmark_query(mongodb.get_songs_bought_by_customer, "Canciones compradas por cliente", param_list=mongo_db_customer_songs_params, iterations=100)
    mongo_benchmark.benchmark_query(mongodb.invoices_in_date_range, "Ventas en periodo de tiempo", param_list=invoices_date_range_params, iterations=100)
    mongo_benchmark.benchmark_query(mongodb.get_genres_quantity_sold, "Ventas segun genero", iterations=100)
    mongo_benchmark.benchmark_query(mongodb.amount_sold_by_month, "Ventas segun mes", iterations=100)
    

    
    print("Optimizacion con indices: ")
    optimizer.add_track_album_artist_indexes()
    mongo_benchmark1.benchmark_query(mongodb.get_artist_songs, "Canciones de un artista", param_list=artist_params, iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.get_amount_of_songs_selled, "Cantidad de ventas por cancion", iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.get_artists_in_genre, "Artistas segun género", param_list=artist_genre_params, iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.songs_in_playlist, "Canciones en playlist", param_list=playlist_songs_params, iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.get_quantity_sold_tracks_by_artist, "Cantidad de canciones vendidas por artista", iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.get_songs_bought_by_customer, "Canciones compradas por cliente", param_list=mongo_db_customer_songs_params, iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.invoices_in_date_range, "Ventas en periodo de tiempo", param_list=invoices_date_range_params, iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.get_genres_quantity_sold, "Ventas segun genero", iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.amount_sold_by_month, "Ventas segun mes", iterations=100)
    
    
    print("Optimizacion con patrones: ")
    optimizer.embed_artist_name_in_tracks()
    optimizer.embed_quantity_sold_into_tracks()
    mongo_benchmark.benchmark_query(mongodb.get_artist_songs_v2, "Canciones de un artista (embebido)", param_list=artist_params, iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.get_amount_of_songs_selled_v2, "Cantidad de ventas por cancion (embebido)", iterations=100)
    mongo_benchmark.benchmark_query(mongodb.get_artists_in_genre_v2, "Artistas segun género (embebido)", param_list=artist_genre_params, iterations=100)
    mongo_benchmark1.benchmark_query(mongodb.get_quantity_sold_tracks_by_artist_v2, "Cantidad de canciones vendidas por artista (embebido)", iterations=100)
    
    