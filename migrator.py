from pymongo import MongoClient
from psycopg2.extensions import connection
from rdb import select_artists
from mongodb import insert_artists
from bson.objectid import ObjectId


def migrate_artists(pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_id_artists = {} # diccionario con id postgres a id mongo para busqueda de FK
    artists_docs = [] # listado de artistas a insertar en mongo
    pg_artists = select_artists(pg_conn)
    for id, name in pg_artists:
        artist_doc_id = ObjectId()
        pg_id_artists[id] = artist_doc_id
        artists_docs.append({
            "_id": artist_doc_id,
            "name": name
        })
    insert_artists(artists_docs, mongo_client)
    return pg_id_artists