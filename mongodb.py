from pymongo import MongoClient
import os
from bson.objectid import ObjectId

class MongoConnection:
    def __init__(self):
        DB_URL = os.getenv('MONGO_DB_URL')
        MONGO_DB_TARGET = os.getenv('MONGO_DB_TARGET')    

        client = MongoClient(DB_URL)
        client.admin.command('ping')
        print("✅ Conexión exitosa a MongoDB")
        self.client = client
        self.db = client[MONGO_DB_TARGET]
    
    def setup_db(self):
        self.client.drop_database(os.getenv('MONGO_DB_TARGET'))

    def insert_artists(self, artists_docs: list):
        self.db.artists.insert_many(artists_docs)
        
    def insert_albums(self, albums_docs: list):
        self.db.albums.insert_many(albums_docs)
        
    def insert_tracks(self, tracks_docs: list):
        self.db.tracks.insert_many(tracks_docs)
        
    def insert_playlists(self, playlists_docs: list):
        self.db.playlists.insert_many(playlists_docs)
        
    def insert_employees(self, employees_docs: list):
        self.db.employees.insert_many(employees_docs)

    def insert_customers(self, customer_docs: list):
        self.db.customers.insert_many(customer_docs)
        
    def insert_invoices(self, invoices_docs: list):
        self.db.invoices.insert_many(invoices_docs)
    
    def insert_track(self, track):
        self.db.tracks.insert_one(track)
        
    def embed_artists_on_tracks(self):
        pipeline = [
            {
                "$lookup": {
                    "from": "albums",
                    "localField": "album_id",
                    "foreignField": "_id",
                    "as": "album"
                }
            },
            { "$unwind": "$album" },
            {
                "$lookup": {
                    "from": "artists",
                    "localField": "album.artist_id",
                    "foreignField": "_id",
                    "as": "artist"
                }
            },
            { "$unwind": "$artist" },
            {
                "$addFields": {
                    "artist_name": "$artist.name"
                }
            },
            {
                "$project": {
                    "album": 0,
                    "artist": 0
                }
            },
            {
                "$merge": {
                    "into": "tracks",
                    "whenMatched": "merge",
                    "whenNotMatched": "discard"
                }
            }
        ]
        self.db.tracks.aggregate(pipeline)
        
    def embed_quantity_sold_in_tracks(self):
        pipeline = [
            {"$unwind": "$lines"},
            {
              "$group": {
                  "_id": "$lines.track_id",
                  "quantity_sold": {"$sum": "$lines.quantity"}
              }  
            },
            {
                "$merge": {
                    "into": "tracks",
                    "on": "_id",
                    "whenMatched": "merge",
                    "whenNotMatched": "discard"
                }
            }
        ]
        self.db.invoices.aggregate(pipeline)
    
    def set_zero_quantity_sold_for_unsold_tracks(self):
        self.db.tracks.update_many(
            {"quantity_sold": {"$exists": False}},
            {"$set": {"quantity_sold": 0}}
        )

    def get_artist_songs(self, artist: str):
        pipeline = [
            {"$match": {"name": artist}},
            {"$lookup": {
                "from": "albums",
                "localField": "_id",
                "foreignField": "artist_id",
                "as": "album"
            }},
            {"$unwind": "$album"},
            {"$lookup": {
                "from": "tracks",
                "localField": "album._id",
                "foreignField": "album_id",
                "as": "track"
            }},
            {"$unwind": "$track"},
            {"$project": {"_id": 1, "track_name": "$track.name"}}
        ]
        res = self.db.artists.aggregate(pipeline)
    
    def get_artist_songs_v2(self, artist: str):
        res = self.db.tracks.find({"$artist_name": artist}, {"_id": 1, "track_name": "$track.name"})

    
    def get_amount_of_songs_selled(self):
        pipeline = [
            # Descomponer el array de líneas de factura
            {"$unwind": "$lines"},

            # Agrupar por track_id
            {"$group": {
                "_id": "$lines.track_id",
                "total_ventas": {"$sum": "$lines.quantity"}
            }},

            # Unir con la colección "tracks"
            {"$lookup": {
                "from": "tracks",
                "localField": "_id",
                "foreignField": "_id", 
                "as": "track"
            }},
            {"$unwind": "$track"},

            # Proyección final
            {"$project": {
                "_id": 0,
                "nombre_cancion": "$track.name",
                "total_ventas": 1
            }},

            # Ordenar por cantidad vendida
            {"$sort": {"total_ventas": -1}}
        ]
        result = self.db.invoices.aggregate(pipeline)

        # for track in result:
        #     print(f"{track['nombre_cancion']} - Vendida: {track['total_vendida']}")
    
    def get_amount_of_songs_selled_v2(self):
        result = self.db.tracks.find(
            {}, {"_id": 0, "nombre_cancion": "$name", "total_vendidas": "$quantity_sold"}
            ).sort("quantity_sold", -1)

    
    def get_artists_in_genre(self, genre: str):

        pipeline = [
            # Filtrar solo las canciones del género deseado
            {"$match": {"genre": genre}},

            # Unir con la colección de álbumes
            {"$lookup": {
                "from": "albums",
                "localField": "album_id",
                "foreignField": "_id",
                "as": "album"
            }},
            {"$unwind": "$album"},

            # Unir con la colección de artistas
            {"$lookup": {
                "from": "artists",
                "localField": "album.artist_id",
                "foreignField": "_id",
                "as": "artist"
            }},
            {"$unwind": "$artist"},

            # Agrupar por artista (eliminamos duplicados)
            {"$group": {
                "_id": "$artist._id",
                "nombre_artista": {"$first": "$artist.name"}
            }},

            # Ordenar alfabéticamente
            {"$sort": {"nombre_artista": 1}}
        ]
        result = self.db.tracks.aggregate(pipeline)
        
        # print(f"Artistas con canciones del género '{genre}':\n")
        # for artista in result:
        #     print("-", artista["nombre_artista"])
        
    def get_artists_in_genre_v2(self, genre: str):
        pipeline = [
            {"$match": {"genre": genre}},
            {"$group": {
                "_id": "$tracks.artist_name",
                "nombre_artista": {"$first": "$tracks.artist_name"}
            }},
            {"$sort": {"nombre_artista": 1}}
        ]
        result = self.db.tracks.aggregate(pipeline)
        
        # print(f"Artistas con canciones del género '{genre}':\n")
        # for artista in result:
        #     print("-", artista["nombre_artista"])

        
    def songs_in_playlist(self, playlist: str):

        pipeline = [
            # Filtrar solo la playlist deseada
            {"$match": {"name": playlist}},

            # Unir con la colección de canciones (tracks)
            {"$lookup": {
                "from": "tracks",
                "localField": "tracks",  # array de IDs
                "foreignField": "_id",
                "as": "canciones"
            }},

            # Desenrollar canciones si quieres una por línea
            {"$unwind": "$canciones"},

            # Proyectar solo lo relevante
            {"$project": {
                "_id": 0,
                "playlist": "$name",
                "nombre_cancion": "$canciones.name",
            }}
        ]
        result = self.db.playlists.aggregate(pipeline)
        # for track in result:
        #     print(f"{track['nombre_cancion']}")
    
       
    def get_quantity_sold_tracks_by_artist(self):
        pipeline = [
            # Descomponer líneas de factura
            {"$unwind": "$lines"},

            # Agrupar por track_id para sumar las ventas por track
            {"$group": {
                "_id": "$lines.track_id",  # referencia al track
                "unidades_vendidas": {"$sum": "$lines.quantity"}
            }},

            # Unir con la colección de tracks
            {"$lookup": {
                "from": "tracks",
                "localField": "_id",
                "foreignField": "_id",
                "as": "track"
            }},
            {"$unwind": "$track"},

            # Unir con la colección de albums (usando track.album_id)
            {"$lookup": {
                "from": "albums",
                "localField": "track.album_id",
                "foreignField": "_id",
                "as": "album"
            }},
            {"$unwind": "$album"},

            # Unir con la colección de artists (usando album.artist_id)
            {"$lookup": {
                "from": "artists",
                "localField": "album.artist_id",
                "foreignField": "_id",
                "as": "artist"
            }},
            {"$unwind": "$artist"},

            # Agrupar por artista y sumar todas sus ventas
            {"$group": {
                "_id": {"$first": "$artist.id"},
                "nombre_artista": {"$first": "$artist.name"},
                "ventas_totales": {"$sum": "$unidades_vendidas"}
            }},
            {"$project":
                {
                    "_id": 0,
                    "nombre_artista": 1,
                    "ventas_totales": 1
                }
            },

            # Ordenar descendente por ventas
            {"$sort": {"ventas_totales": -1}}
        ]

        result = self.db.invoices.aggregate(pipeline)

        # for artista in result:
        #     print(f"{artista['nombre_artista']}: {artista['ventas_totales']} unidades vendidas")
    
    def get_quantity_sold_tracks_by_artist_v2(self):
        pipeline = [
            # Descomponer líneas de factura
            {"$unwind": "$lines"},

            # Agrupar por track_id para sumar las ventas por track
            {"$group": {
                "_id": "$lines.track_id",  # referencia al track
                "unidades_vendidas": {"$sum": "$lines.quantity"}
            }},

            # Unir con la colección de tracks
            {"$lookup": {
                "from": "tracks",
                "localField": "_id",
                "foreignField": "_id",
                "as": "track"
            }},
            {"$unwind": "$track"},

            # Agrupar por artista y sumar todas sus ventas
            {"$group": {
                "_id": "$track.artist_name",
                "ventas_totales": {
                    "$sum": "$unidades_vendidas"
                }
            }},

            # Ordenar descendente por ventas
            {"$sort": {"ventas_totales": -1}},
            {"$project":
                {
                    "_id": 0,
                    "nombre_artista": "$_id",
                    "ventas_totales": 1
                }
            }
        ]

        result = self.db.invoices.aggregate(pipeline)

        # for artista in result:
        #     print(f"{artista['nombre_artista']}: {artista['ventas_totales']} unidades vendidas")
    
    def get_quantity_sold_tracks_by_artist_v3(self):
        pipeline = [
            # Agrupar por artista
            {"$group": {
                "_id": "$artist_name",
                "ventas_totales": {"$sum": "$quantity_sold"}
            }},
            {"$project":
                {
                    "_id": 0,
                    "nombre_artista": "$_id",
                    "ventas_totales": 1
                }
            }
        ]
        
        result = self.db.tracks.aggregate(pipeline)
        # for artista in result:
        #     print(f"{artista['nombre_artista']}: {artista['ventas_totales']} unidades vendidas")
        
    
    def get_songs_bought_by_customer(self, customer_id: str):
        pipeline = [
            # Filtrar facturas del cliente
            {"$match": {"customer_id": ObjectId(customer_id)}},

            # Descomponer las líneas de factura
            {"$unwind": "$lines"},

            # Unir con la colección de tracks por track_id
            {"$lookup": {
                "from": "tracks",
                "localField": "lines.track_id",
                "foreignField": "_id",
                "as": "track"
            }},
            {"$unwind": "$track"},
            {"$group": {
                "_id": "$track._id",
                "nombre_cancion": {"$first": "$track.name"}
            }},

            # Ordenar alfabéticamente (opcional)
            {"$sort": {"_id": 1}}
        ]
        
        result = self.db.invoices.aggregate(pipeline)
        # print("Canciones compradas por el cliente:\n")
        # for doc in result:
        #     print("-", doc)
        
    
    def invoices_in_date_range(self, start_date, end_date):
        pipeline = [
            # Filtrar facturas entre las fechas dadas
            {"$match": {
                "invoice_date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }},

            # Descomponer las líneas de factura
            {"$unwind": "$lines"},

            # Sumar la cantidad total de unidades vendidas
            {"$group": {
                "_id": None,
                "cantidad_ventas": {"$sum": "$lines.quantity"}
            }},
            {"$project": {
                "_id": 0,
                "cantidad_ventas": 1
            }}
        ]
        result = list(self.db.invoices.aggregate(pipeline))
        # print(result)

     
    def get_genres_quantity_sold(self):
        pipeline = [
            # Descomponer líneas de cada factura
            {"$unwind": "$lines"},

            # Unir con la colección de tracks
            {"$lookup": {
                "from": "tracks",
                "localField": "lines.track_id",
                "foreignField": "_id",
                "as": "track"
            }},
            {"$unwind": "$track"},

            # Agrupar por género y sumar cantidad de unidades vendidas
            {"$group": {
                "_id": "$track.genre",
                "cantidad_ventas": {"$sum": "$lines.quantity"}
            }},

            # Ordenar descendente por ventas
            {"$sort": {"cantidad_ventas": -1}}
        ]
        result = self.db.invoices.aggregate(pipeline)

        # print("Ventas por género:\n")
        # for genero in result:
        #     print(f"{genero['_id']}: {genero['cantidad_ventas']} unidades, {genero['monto']}")
    
    def get_genres_quantity_sold_v2(self):
        pipeline = [
            # Agrupar por género y sumar cantidad de unidades vendidas
            {"$group": {
                "_id": "$genre",
                "cantidad_ventas": {"$sum": "$quantity_sold"},
            }},
            # Ordenar descendente por ventas
            {"$sort": {"cantidad_ventas": -1}}
        ]
        result = self.db.tracks.aggregate(pipeline)

        # print("Ventas por género:\n")
        # for genero in result:
        #     print(f"{genero['_id']}: {genero['cantidad_ventas']} unidades, {genero['monto']}")
        
       
    def amount_sold_by_month(self):
        pipeline = [
            # Descomponer cada línea de la factura
            {"$unwind": "$lines"},

            # Agrupar por mes y año extraído desde invoice_date
            {"$group": {
                "_id": {
                    "año": {"$year": "$invoice_date"},
                    "mes": {"$month": "$invoice_date"}
                },
                "cantidad_ventas": {"$sum": "$lines.quantity"}
            }},

            # Ordenar cronológicamente
            {"$sort": {
                "_id.año": 1,
                "_id.mes": 1
            }}
        ]
        result = self.db.invoices.aggregate(pipeline)
        # print("Cantidad de ventas por mes:\n")
        # for doc in result:
        #     año = doc["_id"]["año"]
        #     mes = doc["_id"]["mes"]
        #     print(f"{año}-{mes:02d}: {doc['cantidad_ventas']} unidades")
    
    def get_customer_id_from_last_name(self, last_name: str):
        customer = self.db.customers.find({"last_name": last_name})
        return customer[0]["_id"]
    
    def create_track_album_artist_index(self):
        self.db.artists.create_index([("name", 1)])
        self.db.albums.create_index([("artist_id", 1)])
        self.db.tracks.create_index([("album_id", 1)])
        
    def create_artist_track_index(self):
        self.db.tracks.create_index([("artist_name", 1)])
    