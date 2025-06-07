from pymongo import MongoClient
import os

def mongo_connection():
    APP_NAME = os.getenv('MONGO_APP_NAME')
    APP_URL = os.getenv('MONGO_APP_URL')
    DB_PASSWORD = os.getenv('MONGO_PASSWORD_DB')
    DB_USER = os.getenv('MONGO_USER_DB')
    uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{APP_URL}/?retryWrites=true&w=majority&appName={APP_NAME}"

    client = MongoClient(uri)
    client.admin.command('ping')
    return client["chinook1"]