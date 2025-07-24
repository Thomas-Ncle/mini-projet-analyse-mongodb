import csv
import requests
from datetime import datetime
from pymongo import MongoClient, errors

# MongoDB connection details
DB_HOST = ""
DB_USERNAME = ""
DB_PASSWORD = ""
DB_NAME = ""

OMDB_API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiMThmMTczZGQ4YjNkZTlhZmFkY2RiZDNjZTJjYzdlMCIsIm5iZiI6MTc0MDM5MzM1OC45MzgsInN1YiI6IjY3YmM0YjhlMzNkNzQ5Y2MzOWJlYmExNiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.fAfdhtMV-zCS3tpvY7NkAlzDzVwVwclJYpq1Uj8qWeY"

def connect_to_mongo(HOST, USER, PSWD):
    print("[BDD] Connecting to MongoDB...")
    try:
        c = MongoClient(HOST, port=27017, username=USER, password=PSWD, authSource=USER, authMechanism="SCRAM-SHA-256")
        print("[BDD] Connected to MongoDB !")
        return c
    except errors.PyMongoError as e:
        print("[ERREUR] " + str(e))

# Add document to the collection
def insert_data_to_mongo(c, collection_name, data):
    try:
        db = c[DB_NAME]
        collection = db[collection_name]
        collection.insert_many(data)
        print(f'Data inserted successfully into {DB_NAME}.{collection_name}')
    except errors.PyMongoError as e:
        print(f"An error occurred while inserting data: {e}")

# Function to convert dataset to array
def dataset_to_array(movies_path, links_path, ratings_path, c, collection_name):
    print("Opening the files...")
    with open(movies_path, 'r', encoding='utf-8') as movie_file, \
         open(links_path, 'r', encoding='utf-8') as links_file, \
         open(ratings_path, 'r', encoding='utf-8') as ratings_file:

        reader_movie = csv.DictReader(movie_file)
        reader_links = csv.DictReader(links_file)
        reader_ratings = csv.DictReader(ratings_file)
        print("Files opened !")

        print("Mapping the files...")
        links_dict = {row['movieId']: row for row in reader_links}
        ratings_dict = {}

        # Iterate through the ratings and populate the dictionary
        for row_ratings in reader_ratings:
            movie_id = row_ratings['movieId']
            if movie_id not in ratings_dict:
                ratings_dict[movie_id] = []
            ratings_dict[movie_id].append({
                'userId': int(row_ratings['userId']),
                'rating': float(row_ratings['rating']),
            })

        # Initialize dataJSON list and counter
        dataJSON = []
        cpt = 0

        # Iterate through the movies and fetch additional data
        for row_movie in reader_movie:
            cpt += 1
            print(f"→ {cpt} inserted data")
            movie_id = int(row_movie['movieId'])
            if str(movie_id) in links_dict:
                tmdbId = links_dict[str(movie_id)].get('tmdbId')

                # Vérification de l'ID TMDb
                if not tmdbId:
                    print(f"[WARNING] No TMDb ID found for movie ID {movie_id}")
                    continue

                headers = {
                    "accept": "application/json",
                    "Authorization": "Bearer " + OMDB_API_TOKEN,
                }

                try:
                    print(f"Fetching data for TMDb ID: {tmdbId}")  # Debugging line
                    response = requests.get(f"https://api.themoviedb.org/3/movie/{tmdbId}/credits", headers=headers)
                    if response.status_code == 200:
                        tmdbAPIResponse = response.json()
                    else:
                        print(f"[ERROR] Failed to fetch data for movie ID {tmdbId}: {response.status_code}")
                        continue
                except Exception as err:
                    print(f"[ERROR] {err} for movie ID {movie_id} with TMDb ID {tmdbId}")
                    continue

                actors = [member['name'] for member in tmdbAPIResponse.get('cast', [])]
                directors = [member['name'] for member in tmdbAPIResponse.get('crew', []) if member.get('job') == 'Director']

                ratings = ratings_dict.get(str(movie_id), [])
                # Append the movie data to the JSON array
                dataJSON.append({
                    '_id': movie_id,
                    'title': row_movie['title'],
                    'genres': row_movie['genres'].split('|'),
                    'imdbId': links_dict[str(movie_id)]['imdbId'],
                    'tmdbId': tmdbId,
                    'cast': {
                        'actors': actors,
                        'directors': directors
                    },
                    'ratings': ratings
                })

        # Insert data into MongoDB
            insert_data_to_mongo(c, collection_name, dataJSON)
            dataJSON = []
        return dataJSON

bd = connect_to_mongo(DB_HOST, DB_USERNAME, DB_PASSWORD)
dataJSON = dataset_to_array("ml-latest/movies.csv", "ml-latest/links.csv", "ml-latest/ratings.csv", bd, "Movie")
print(dataJSON)
