from pymongo import MongoClient
from psycopg2.extensions import connection
import rdb
import mongodb
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128


def migrate_artists(pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_artists_ids = {} # diccionario con id postgres a id mongo para busqueda de FK
    artists_docs = [] # listado de artistas a insertar en mongo
    pg_artists = rdb.select_artists(pg_conn)
    for id, name in pg_artists:
        artist_doc_id = ObjectId()
        pg_artists_ids[id] = artist_doc_id
        artists_docs.append({
            "_id": artist_doc_id,
            "name": name
        })
    mongodb.insert_artists(artists_docs, mongo_client)
    return pg_artists_ids

def migrate_albums(pg_artists_ids, pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_albums_ids = {} # diccionario con id postgres a id mongo para busqueda de FK
    albums_docs = [] # listado de albumes a insertar en mongo
    pg_albums = rdb.select_albums(pg_conn)
    for id, title, artist_id in pg_albums:
        album_doc_id = ObjectId()
        pg_albums_ids[id] = album_doc_id
        albums_docs.append({
            "_id": album_doc_id,
            "title": title,
            "artist_id": pg_artists_ids[artist_id]
        })
    mongodb.insert_albums(albums_docs, mongo_client)
    return pg_albums_ids

def migrate_tracks(pg_albums_ids: list, pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_tracks_ids = {} # diccionario con id postgres a id mongo para busqueda de FK
    tracks_docs = [] # listado de tracks a insertar en mongo
    pg_tracks = rdb.select_tracks_with_genre_and_media_type(pg_conn)
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
    mongodb.insert_tracks(tracks_docs, mongo_client)
    return pg_tracks_ids

def migrate_playlists(pg_tracks_ids: list, pg_conn: connection, mongo_client: MongoClient):
    playlists = {} # Diccionario clave_pg: valor_doc_mongo
    pg_playlists = rdb.select_playlists_with_tracks(pg_conn)
    for playlist_id, playlist_name, track_id in pg_playlists:
        if playlist_id in playlists:
            playlists[playlist_id]["tracks"].append(pg_tracks_ids[track_id])
        else:
            playlists[playlist_id] = {
                "_id": ObjectId(),
                "name": playlist_name,
                "tracks": [] if track_id is None else [pg_tracks_ids[track_id]] # Para evitar problemas con playlists vacias
            }
    mongodb.insert_playlists(list(playlists.values()), mongo_client)
    
def migrate_employees(pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_employees_ids = {} # diccionario con id postgres a id mongo para busqueda de FK y referencias circulares
    employees_docs = []
    pg_employees = rdb.select_employees(pg_conn)
    for employee in pg_employees:
        employees_docs.append({
            "_id": _resolve_employee_id(employee["employee_id"], pg_employees_ids),
            "first_name": employee["first_name"],
            "last_name": employee["last_name"],
            "title": employee["title"],
            "reports_to": _resolve_employee_id(employee["reports_to"], pg_employees_ids),
            "birth_date": employee["birth_date"],
            "hire_date": employee["hire_date"],
            "address": employee["address"],
            "city": employee["city"],
            "state": employee["state"],
            "country": employee["country"],
            "postal_code": employee["postal_code"],
            "phone": employee["phone"],
            "fax": employee["fax"],
            "email": employee["email"]
        })
    mongodb.insert_employees(employees_docs, mongo_client)
    return pg_employees_ids

def migrate_customers(pg_employees_ids: list, pg_conn: connection, mongo_client: MongoClient) -> list:
    pg_customers_ids = {} # diccionario con id postgres a id mongo para busqueda de FK
    customer_docs = []
    pg_customers = rdb.select_customers(pg_conn)
    for customer in pg_customers:
        customer_doc_id = ObjectId()
        pg_customers_ids[customer["customer_id"]] = customer_doc_id
        customer_docs.append({
            "_id": customer_doc_id,
            "first_name": customer["first_name"],
            "last_name": customer["last_name"],
            "company": customer["company"],
            "address": customer["address"],
            "city": customer["city"],
            "state": customer["state"],
            "country": customer["country"],
            "postal_code": customer["postal_code"],
            "phone": customer["phone"],
            "fax": customer["fax"],
            "email": customer["email"],
            "support_rep_id": None if customer["support_rep_id"] is None else pg_employees_ids[customer["support_rep_id"]]
        })
    mongodb.insert_customers(customer_docs, mongo_client)
    return pg_customers_ids

def migrate_invoices(pg_customers_ids: list, pg_tracks_ids: list, pg_conn: connection, mongo_client: MongoClient):
    invoices = {} # Diccionario clave_pg: valor_doc_mongo
    pg_invoices = rdb.select_invoices_with_invoices_lines(pg_conn)
    for invoice in pg_invoices:
        line = {
            "track_id": pg_tracks_ids[invoice["track_id"]],
            "unit_price": Decimal128(invoice["unit_price"]),
            "quantity": invoice["quantity"] 
        }
        if invoice["invoice_id"] in invoices:
            invoices[invoice["invoice_id"]]["lines"].append(line)
        else:
            invoices[invoice["invoice_id"]] = {
                "_id": ObjectId(),
                "customer_id": pg_customers_ids[invoice["customer_id"]],
                "invoice_date": invoice["invoice_date"],
                "billing_address": invoice["billing_address"],
                "billing_city": invoice["billing_city"],
                "billing_state": invoice["billing_state"],
                "billing_country": invoice["billing_country"],
                "billing_postal_code": invoice["billing_postal_code"],
                "total": Decimal128(invoice["total"]),
                "lines": [line]
            }
    mongodb.insert_invoices(list(invoices.values()), mongo_client)

def _resolve_employee_id(pg_employee_id, pg_employees_ids: dict) -> ObjectId | None:
    if pg_employee_id is None: return None
    if pg_employee_id in pg_employees_ids: return pg_employees_ids[pg_employee_id]
    pg_employees_ids[pg_employee_id] = ObjectId()
    return pg_employees_ids[pg_employee_id]