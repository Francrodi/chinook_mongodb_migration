import psycopg2
from psycopg2 import OperationalError
import os


def create_connection():
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

