from pprint import pprint

from pymongo import MongoClient, ASCENDING, errors
import matplotlib.pyplot as plt

# MongoDB connection details
DB_HOST = "mongodb.iem"
DB_USERNAME = "tn837970"
DB_PASSWORD = "tn837970"
DB_NAME = "tn837970"

def connect_to_mongo(HOST, USER, PSWD):
    print("[BDD] Connecting to MongoDB...")
    try:
        c = MongoClient(
            HOST,
            port=27017,
            username=USER,
            password=PSWD,
            authSource=USER,
            authMechanism="SCRAM-SHA-256",
        )
        print("[BDD] Connected to MongoDB!")
        return c
    except errors.PyMongoError as e:
        print("[ERROR BDD] " + str(e))

def plot_movie_ratings(title, filename, collection):
    try:
        movie = collection.find_one({"title": title})
    except errors.PyMongoError as e:
        print("[ERROR SELECTION MOVIE] " + str(e))

    if movie:
        ratings = [rating['rating'] for rating in movie['ratings']]
        plt.hist(ratings, bins=10, edgecolor='black')
        plt.title(f'Distribution des notes pour "{title}"')
        plt.xlabel('Note')
        plt.ylabel('Fréquence')
        plt.savefig(filename)  # Save the plot with the specified filename
        plt.close()
    else:
        print(f"No movie found with the title '{title}'.")

def plot_popular_movies(filename, collection):
    pipeline = [
        {"$project": {"title": 1, "ratingCount": {"$size": "$ratings"}}},
        {"$sort": {"ratingCount": -1}},
        {"$limit": 10}
    ]
    popular_movies = list(collection.aggregate(pipeline))
    titles = [movie['title'] for movie in popular_movies]
    counts = [movie['ratingCount'] for movie in popular_movies]

    plt.barh(titles, counts, color='skyblue')
    plt.xlabel('Nombre de notes')
    plt.title('Films les plus populaires')
    plt.gca().invert_yaxis()
    plt.savefig(filename)  # Save the plot with the specified filename
    plt.close()

def plot_genre_popularity(filename, collection):
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {
            "_id": "$genres",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    genre_popularity = list(collection.aggregate(pipeline))
    genres = [genre['_id'] for genre in genre_popularity]
    counts = [genre['count'] for genre in genre_popularity]

    plt.bar(genres, counts, color='lightgreen')
    plt.xlabel('Genre')
    plt.ylabel('Nombre de films')
    plt.title('Popularité des genres')
    plt.xticks(rotation=45)
    plt.savefig(filename)  # Save the plot with the specified filename
    plt.close()

def get_movie_rating_avg(title, collection):
        pipeline = [
            {"$match": {"title": title}},
            {"$unwind": "$ratings"},
            {"$group": {
                "_id": "$title",
                "avgRating": {"$avg": "$ratings.rating"},
                "nbReviews": {"$sum": 1}
            }}
        ]
        result = list(collection.aggregate(pipeline))

c = connect_to_mongo(DB_HOST, DB_USERNAME, DB_PASSWORD)
db=c.tn837970
movie = db.Movie

# Execute visualization functions
plot_movie_ratings("Toy Story (1995)", "toystory_ratings.png", movie)
# plot_popular_movies("popular_movies.png", movie)
# # plot_genre_popularity("genre_popularity.png", movie)
# get_movie_rating_avg("Toy Story (1995)", movie)

