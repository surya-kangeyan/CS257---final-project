from pymongo import MongoClient
import time

# Connection to MongoDB
client = MongoClient('localhost', 27017)

# Accessing the 'IMDB' database
db = client['IMDB']

# Movie title and new votes count
movie_title = "Pauvre Pierrot"  # Replace with the actual movie title
new_votes_count = 1234  # Replace with the new votes count

# Step 1: Find the tconst for the given movie title
startTime = time.time()

movie = db['titleBasics'].find_one({"primaryTitle": movie_title}, {"_id": 0, "tconst": 1})
if movie is None:
    print("Movie not found.")
else:
    tconst = movie.get('tconst')

    # Step 2: Retrieve and print the current numOfVotes
    current_rating = db['titleRatings'].find_one({"tconst": tconst})
    if current_rating:
        old_votes = current_rating.get('numVotes', 'Not available')
        print(f"Old numOfVotes for '{movie_title}': {old_votes}")

        # Step 3: Update numOfVotes
        db['titleRatings'].update_one({"tconst": tconst}, {"$set": {"numVotes": new_votes_count}})

        # Step 4: Retrieve and print the updated numOfVotes and record
        updated_rating = db['titleRatings'].find_one({"tconst": tconst})
        new_votes = updated_rating.get('numVotes', 'Not available')
        print(f"New numOfVotes for '{movie_title}': {new_votes}")
        print("Updated movie record:", updated_rating)
    else:
        print("Ratings not found for the movie.")
endTime = time.time()
execution_time = endTime - startTime
print(f"Pipeline execution time: {execution_time} seconds")