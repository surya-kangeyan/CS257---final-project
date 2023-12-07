# impt 1.Retrieve movie titles along with their directors' names.

pipeline1 = [
    {"$match": {"titleType": "movie"}},
    {"$lookup": {
        "from": "titleCrew",
        "localField": "tconst",
        "foreignField": "tconst",
        "as": "crew"
    }},
    {"$unwind": "$crew"},
    {"$lookup": {
        "from": "nameBasics",
        "localField": "crew.directors",
        "foreignField": "nconst",
        "as": "directors"
    }},
    {"$project": {
        "primaryTitle": 1,
        "startYear": 1,
        "genres": 1,
        "directors": "$directors.primaryName"
    }}
]

# impt 2.Retrieve movie titles released in a specific year and genre
# title_basics
pipeline2 = [
    {
        "$match": {
            "startYear": 2020,
            "genres": { "$regex": "Action" }
        }
    },
    {
        "$project": {
            "primaryTitle": 1,
            "startYear": 1,
            "genres": 1,
            "_id": 0
        }
    }
]

# impt 3.Find the average runtime of movies by genre.

pipeline3 = [
    {
        "$match": {
            "titleType": "movie",
            "genres": {"$in": ["Comedy", "Documentary", "Action"]}
        }
    },
    {
        "$group": {
            "_id": "$genres",
            "avg_runting": {"$avg": "$runtimeMinutes"}
        }
    }
]


# impt 4.Find the average runtime of adult movies with a specific genre(Drama): // titleBasics
pipeline4 = [
    {
        "$match": {
            "isAdult": 1,  # Filter for adult movies
            "genres": "Drama",  # Filter for the specific genre
            "runtimeMinutes": {"$ne": None}  # Exclude movies without a runtime
        }
    },
    {
        "$group": {
            "_id": None,  # Grouping all matching documents together
            "avgRuntime": {"$avg": "$runtimeMinutes"}  # Calculating the average runtime
        }
    }
]


 # impt 5. List top-rated directors along with their average movie ratings //run it on titleCrew
pipeline5 = [
    {
        "$lookup": {
            "from": "titleRatings",
            "localField": "tconst",
            "foreignField": "tconst",
            "as": "ratings"
        }
    },
    {
        "$unwind": "$directors"
    },
    {
        "$unwind": "$ratings"
    },
    {
        "$group": {
            "_id": "$directors",
            "avgRating": {"$avg": "$ratings.averageRating"}
        }
    },
    {
        "$sort": {"avgRating": -1}
    },
    {
        "$limit": 50  # Optional: Adjust the number to get a specific count of top directors
    },
    {
        "$lookup": {
            "from": "nameBasics",
            "localField": "_id",
            "foreignField": "nconst",
            "as": "director_info"
        }
    },
    {
        "$project": {
            "director_name": "$director_info.primaryName",
            "avgRating": 1,
            "_id": 0
        }
    }
]
# impt 6: Find the decade with the highest average movie rating
pipeline6 = [
    {
        "$match": {
            "titleType": "movie",
            "startYear": {"$ne": "\\N"}  # Exclude movies without a valid start year
        }
    },
    {
        "$lookup": {
            "from": "titleRatings",
            "localField": "tconst",
            "foreignField": "tconst",
            "as": "ratings"
        }
    },
    {
        "$unwind": "$ratings"
    },
    {
        "$addFields": {
            "startYearInt": {
                "$cond": {
                    "if": {"$ne": ["$startYear", "\\N"]},
                    "then": {"$toInt": "$startYear"},
                    "else": None
                }
            }
        }
    },
    {
        "$match": {
            "startYearInt": {"$ne": None}
        }
    },
    {
        "$addFields": {
            "decade": {"$subtract": ["$startYearInt", {"$mod": ["$startYearInt", 10]}]}
        }
    },
    {
        "$group": {
            "_id": "$decade",
            "averageRating": {"$avg": "$ratings.averageRating"}
        }
    },
    {
        "$sort": {"averageRating": -1}
    },
    {
        "$limit": 1
    }
]

# impt : 7. Find the average rating of movies released in the last five years grouped by genre //title basics
pipeline7 = [
    {
        "$match": {
            "titleType": "movie",
            "startYear": {"$gte": 2023}
        }
    },
    {
        "$lookup": {
            "from": "titleRatings",
            "localField": "tconst",
            "foreignField": "tconst",
            "as": "ratings"
        }
    },
    {
        "$unwind": "$ratings"
    },
    {
        "$unwind": "$genres"
    },
    {
        "$group": {
            "_id": "$genres",
            "avgRating": {"$avg": "$ratings.averageRating"}
        }
    }
]
# impt 8.Find the top 10 most voted TV series with episodes longer than 30 minutes
pipeline8= [
    {
        "$match": {
            "titleType": "tvEpisode",
            "runtimeMinutes": {"$gt": 30}
        }
    },
    {
        "$lookup": {
            "from": "titleBasics",
            "localField": "parentTconst",
            "foreignField": "tconst",
            "as": "tvSeries"
        }
    },
    {
        "$unwind": "$tvSeries"
    },
    {
        "$lookup": {
            "from": "titleRatings",
            "localField": "parentTconst",
            "foreignField": "tconst",
            "as": "ratings"
        }
    },
    {
        "$unwind": "$ratings"
    },
    {
        "$group": {
            "_id": "$parentTconst",
            "tvSeriesTitle": {"$first": "$tvSeries.primaryTitle"},
            "totalVotes": {"$sum": "$ratings.numVotes"}
        }
    },
    {
        "$sort": {"totalVotes": -1}
    },
    {
        "$limit": 50
    }
]
pipeline9 = [
    {
        "$match": {
            "titleType": "tvEpisode"  # Filter to include only TV series
        }
    },
    {
        "$lookup": {
            "from": "titleRatings",  # Join with the ratings collection
            "localField": "tconst",
            "foreignField": "tconst",
            "as": "ratings"
        }
    },
    {
        "$unwind": "$ratings"  # Deconstruct the ratings array
    },
    {
        "$sort": {"ratings.numVotes": -1}  # Sort by number of votes
    },
    {
        "$limit": 10  # Limit to top 10 TV series
    },
    {
        "$project": {
            "title": "$primaryTitle",
            "averageRating": "$ratings.averageRating",
            "numVotes": "$ratings.numVotes",
            "_id": 0
        }
    }
]

