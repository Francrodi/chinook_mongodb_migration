import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import connection
import os


def create_connection() -> connection:
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
        return connection
    except OperationalError as e:
        print(f"❌ Error al conectar: {e}")
        return None

def select_artists(pg_conn: connection) -> list[tuple]:
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM ARTIST")
        return cursor.fetchall()
    
def select_albums(pg_conn: connection) -> list[tuple]:
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM ALBUM")
        return cursor.fetchall()
        