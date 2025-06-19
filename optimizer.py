from mongodb import MongoConnection

class MongoOptimizer:
    def __init__(self, mongo_connection: MongoConnection):
        self.mongo = mongo_connection
        
    def add_track_album_artist_indexes(self):
        print("Agregando indices en colecciones...")
        self.mongo.create_track_album_artist_index()