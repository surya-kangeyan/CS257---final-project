from pymongo import MongoClient
import time
import pipelines
# Connection to MongoDB (replace 'localhost' and '27017' with your MongoDB host and port if different)
client = MongoClient('localhost', 27017)

# Accessing the 'IMDB' database
db = client['IMDB']
# -----------------------------------------------------------------

db['titleBasics'].create_index([('titleType', 1)])
db['titleBasics'].create_index([('tconst', 1)])
db['titleCrew'].create_index([('tconst', 1)])
db['nameBasics'].create_index([('nconst', 1)])
db['titleCrew'].create_index('directors')
db['nameBasics'].create_index('primaryProfession')
db['titleRatings'].create_index([('tconst', 1)])
db['titleBasics'].create_index([("titleType", 1), ("startYear", -1)])
db['titleBasics'].create_index([("titleType", 1), ("runtimeMinutes", 1)])
db['titleBasics'].create_index([("parentTconst", 1)])
db['titleBasics'].create_index([("titleType", 1), ("startYear", 1)])



# -----------------------------------------------------------------
# db['titleBasics'].drop_index([('titleType', 1)])
# db['titleBasics'].drop_index([('tconst', 1)])
# db['titleCrew'].drop_index([('tconst', 1)])
# db['nameBasics'].drop_index([('nconst', 1)])
# db['titleCrew'].drop_index('directors')
# db['nameBasics'].drop_index('primaryProfession')
# db['titleRatings'].drop_index([('tconst', 1)])
# db['titleBasics'].drop_index([("titleType", 1), ("startYear", -1)])
# db['titleBasics'].drop_index([("titleType", 1), ("runtimeMinutes", 1)])
# db['titleBasics'].drop_index([("parentTconst", 1)])
# db['titleBasics'].drop_index([("titleType", 1), ("startYear", 1)])
# db['titleBasics'].drop_index([("parentTconst", 1)])
# db['titleBasics'].drop_index([("titleType", 1), ("startYear", 1)])
# print("Indices dropped successfully.")
# -----------------------------------------------------------------

startTime = time.time()

Qpipeline = pipelines.pipeline9
results = db['titleBasics'].aggregate(Qpipeline)

endTime = time.time()
execution_time = endTime - startTime
print(f"Pipeline execution time: {execution_time} seconds")

# explain_data = db.command(
#     'aggregate', 'collection_name', pipeline=Qpipeline, explain=True
# )
#
# # Printing the explanation
# print(explain_data)

# Printing the results
for doc in results:
    print(doc)