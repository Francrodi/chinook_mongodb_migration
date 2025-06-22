from mongodb import MongoConnection

class MongoOptimizer:
    def __init__(self, mongo_connection: MongoConnection):
        self.mongo = mongo_connection
        
    def add_track_album_artist_indexes(self):
        print("Agregando indices en colecciones...")
        self.mongo.create_track_album_artist_index()
        
    def embed_artist_name_in_tracks(self):
        print("Agregando artistas en tracks...")
        self.mongo.embed_artists_on_tracks()
        self.mongo.create_artist_track_index()
        
    def embed_quantity_sold_into_tracks(self):
        print("Agregando cantidad de ventas en tracks...")
        self.mongo.embed_quantity_sold_in_tracks()
        self.mongo.set_zero_quantity_sold_for_unsold_tracks()
        