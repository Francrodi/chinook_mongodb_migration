import psycopg2
from psycopg2.extras import RealDictCursor
import os

class PostgresHandler:
    def __init__(self):
        try:
            # Parámetros de conexión
            connection = psycopg2.connect(
                host=os.getenv('PG_URL'),
                port=os.getenv('PG_PORT'),
                database=os.getenv('PG_DB'),
                user=os.getenv('PG_USER_DB'),
                password=os.getenv('PG_PASSWORD_DB')  
            )
            print("✅ Conexión exitosa a PostgreSQL")
            self.pg_conn = connection
        except Exception as e:
            print(f"❌ Error al conectar: {e}")
    
    def select_artists(self) -> list[tuple]:
        with self.pg_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM ARTIST")
            return cursor.fetchall()

    def select_albums(self) -> list[tuple]:
        with self.pg_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM ALBUM")
            return cursor.fetchall()
        
    def select_tracks_with_genre_and_media_type(self) -> list[dict]:
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('''
                            SELECT T.*, G.name AS genre, MT.name AS media_type
                            FROM TRACK T 
                            JOIN GENRE G ON T.GENRE_ID = G.GENRE_ID
                            JOIN MEDIA_TYPE MT ON T.MEDIA_TYPE_ID = MT.MEDIA_TYPE_ID 
                        ''')
            return cursor.fetchall()
        
    def select_playlists_with_tracks(self) -> list[tuple]:
        with self.pg_conn.cursor() as cursor:
            cursor.execute('''
                            SELECT p.*, pt.track_id 
                            FROM PLAYLIST P 
                            LEFT JOIN PLAYLIST_TRACK PT ON p.PLAYLIST_ID = pt.PLAYLIST_ID 
                        ''')
            return cursor.fetchall()
        
    def select_employees(self) -> list[dict]:
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM EMPLOYEE")
            return cursor.fetchall()
        
    def select_customers(self) -> list[dict]:
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM CUSTOMER")
            return cursor.fetchall()


    def select_invoices_with_invoices_lines(self) -> list[tuple]:
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('''
                            SELECT I.*, IL.track_id, IL.unit_price, IL.quantity
                            FROM INVOICE I 
                            JOIN INVOICE_LINE IL ON I.invoice_id = IL.invoice_id 
                        ''')
            return cursor.fetchall()
    
    def get_artist_songs(self, artist: str = "AC/DC"):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT t.track_id, t.name AS track_name, al.title AS album_title, ar.name AS artist_name
                        FROM track t
                        JOIN album al ON t.album_id = al.album_id
                        JOIN artist ar ON al.artist_id = ar.artist_id
                        WHERE ar.name = '{artist}';
                ''')
            return cursor.fetchall()
    
    def get_amount_of_songs_selled(self):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT t.name, SUM(il.quantity) AS total_units_sold
                        FROM invoice_line il
                        JOIN track t ON il.track_id = t.track_id
                        GROUP BY t.track_id, t.name
                        ORDER BY total_units_sold DESC;
                ''')
            return cursor.fetchall()
    
    
    def songs_in_playlist(self, playlist: str):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT *
                        FROM playlist p 
                        JOIN playlist_track pt ON pt.playlist_id = p.playlist_id
                        JOIN track t ON t.track_id = pt.track_id 
                        WHERE p.name = '{playlist}'
                ''')
            return cursor.fetchall()
    
    
    def get_artists_in_genre(self, genre: str):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT DISTINCT(a2.name)
                        FROM genre g
                        JOIN track t ON t.genre_id = g.genre_id 
                        JOIN album a ON a.album_id = t.album_id 
                        JOIN artist a2 ON a2.artist_id = a.artist_id 
                        WHERE g.name = '{genre}'
                ''')
            return cursor.fetchall()
    
    
    def get_quantity_sold_tracks_by_artist(self):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT ar.name, SUM(il.quantity) AS total_units_sold
                        FROM artist ar
                        JOIN album al ON ar.artist_id = al.artist_id
                        JOIN track t ON al.album_id = t.album_id
                        JOIN invoice_line il ON t.track_id = il.track_id
                        GROUP BY ar.artist_id, ar.name
                        ORDER BY total_units_sold DESC;
                ''')
            return cursor.fetchall()
    
    
    def get_songs_bought_by_customer(self, customer_id: str):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT DISTINCT t.name
                        FROM customer c
                        JOIN invoice i ON c.customer_id = i.customer_id
                        JOIN invoice_line il ON i.invoice_id = il.invoice_id
                        JOIN track t ON il.track_id = t.track_id
                        WHERE c.customer_id = {customer_id};
                ''')
            return cursor.fetchall()
    
     
    def invoices_in_date_range(self, start_date, end_date):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT SUM(il.quantity) AS total_units_sold
                        FROM invoice i
                        JOIN invoice_line il ON i.invoice_id = il.invoice_id
                        WHERE i.invoice_date BETWEEN '{start_date}' AND '{end_date}'
                ''')
            return cursor.fetchall()
    
        
    def get_genres_quantity_sold(self):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT g.name AS genre, COUNT(il.invoice_line_id) AS tracks_sold, SUM(il.unit_price * il.quantity) AS revenue
                        FROM genre g
                        JOIN track t ON g.genre_id = t.genre_id
                        JOIN invoice_line il ON t.track_id = il.track_id
                        GROUP BY g.name
                        ORDER BY revenue DESC;
                ''')
            return cursor.fetchall()
    
    
    def amount_sold_by_month(self):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f'''
                        SELECT TO_CHAR(i.invoice_date, 'YYYY-MM') AS MONTH, SUM(il.quantity) 
                        FROM invoice i 
                        JOIN invoice_line il ON i.invoice_id = il.invoice_id 
                        GROUP BY month
                ''')
            return cursor.fetchall()
