from pymongo import MongoClient
from psycopg2.extensions import connection
from rdb import select_artists, select_albums, select_tracks_with_genre_and_media_type
from mongodb import insert_artists, insert_albums, insert_tracks
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128


def migrate_artists(pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_artists_ids = {} # diccionario con id postgres a id mongo para busqueda de FK
    artists_docs = [] # listado de artistas a insertar en mongo
    pg_artists = select_artists(pg_conn)
    for id, name in pg_artists:
        artist_doc_id = ObjectId()
        pg_artists_ids[id] = artist_doc_id
        artists_docs.append({
            "_id": artist_doc_id,
            "name": name
        })
    insert_artists(artists_docs, mongo_client)
    return pg_artists_ids

def migrate_albums(pg_artists_ids, pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_albums_ids = {} # diccionario con id postgres a id mongo para busqueda de FK
    albums_docs = [] # listado de albumes a insertar en mongo
    pg_albums = select_albums(pg_conn)
    for id, title, artist_id in pg_albums:
        album_doc_id = ObjectId()
        pg_albums_ids[id] = album_doc_id
        albums_docs.append({
            "_id": album_doc_id,
            "title": title,
            "artist_id": pg_artists_ids[artist_id]
        })
    insert_albums(albums_docs, mongo_client)
    return pg_albums_ids

def migrate_tracks(pg_albums_ids: list, pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_tracks_ids = {} # diccionario con id postgres a id mongo para busqueda de FK
    tracks_docs = [] # listado de tracks a insertar en mongo
    pg_tracks = select_tracks_with_genre_and_media_type(pg_conn)
    for track in pg_tracks:
        track_doc_id = ObjectId()
        pg_tracks_ids[track["track_id"]] = track_doc_id
        tracks_docs.append({
            "_id": track_doc_id,
            "name": track["name"],
            "album_id": pg_albums_ids[track["album_id"]],
            "media_type": track["media_type"],
            "genre": track["genre"],
            "composer": track["composer"],
            "milliseconds": track["milliseconds"],
            "bytes": track["bytes"],
            "unit_price": Decimal128(track["unit_price"]),
        })
    insert_tracks(tracks_docs, mongo_client)
    return pg_tracks_ids