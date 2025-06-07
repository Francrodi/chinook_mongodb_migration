from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd
from bson import ObjectId
import random
import uuid

load_dotenv()

APP_NAME = "Chinook-POC1"
APP_URL = "chinook-poc1.b9l5f32.mongodb.net"
uri = f"mongodb+srv://{os.getenv('USER_DB')}:{os.getenv('PASSWORD_DB')}@{APP_URL}/?retryWrites=true&w=majority&appName={APP_NAME}"

client = MongoClient(uri)
client.admin.command('ping')
db = client["chinook1"]

res = db.tracks.find()
df = pd.DataFrame(list(res))
print(df.head())