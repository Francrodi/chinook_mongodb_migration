from pymongo import MongoClient
import os

def create_connection() -> MongoClient:
    APP_NAME = os.getenv('MONGO_APP_NAME')
    APP_URL = os.getenv('MONGO_APP_URL')
    DB_PASSWORD = os.getenv('MONGO_PASSWORD_DB')
    DB_USER = os.getenv('MONGO_USER_DB')
    MONGO_DB_TARGET = os.getenv('MONGO_DB_TARGET')
    uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{APP_URL}/?retryWrites=true&w=majority&appName={APP_NAME}"

    client = MongoClient(uri)
    client.admin.command('ping')
    print("✅ Conexión exitosa a MongoDB")
    client.drop_database(MONGO_DB_TARGET)
    return client[MONGO_DB_TARGET]

def insert_artists(artists_docs: list, mongo_client: MongoClient):
    mongo_client.artists.insert_many(artists_docs)
    
def insert_albums(albums_docs: list, mongo_client: MongoClient):
    mongo_client.albums.insert_many(albums_docs)
    
def insert_tracks(tracks_docs: list, mongo_client: MongoClient):
    mongo_client.tracks.insert_many(tracks_docs)
    
def insert_playlists(playlists_docs: list, mongo_client: MongoClient):
    mongo_client.playlists.insert_many(playlists_docs)
    
def insert_employees(employees_docs: list, mongo_client: MongoClient):
    mongo_client.employees.insert_many(employees_docs)

def insert_customers(customer_docs: list, mongo_client: MongoClient):
    mongo_client.customers.insert_many(customer_docs)