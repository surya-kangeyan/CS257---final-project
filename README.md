# CS257 final project - A comparative study on disk-based and in-memory database systems

A detailed study of on disk-based (_MySQL, MongoDB_) and in-memory database (_Redis_) systems to analyze and evaluate thier performance on various constraints. Recorded and displayed the findings, and provided our inference from the data. 

# MongoDB
### Step 1 - Download and install MongoDB Community edition
Download it from here - https://www.mongodb.com/try/download/community

### Step 2 - Download and install MongoDB Compass
Download it from - https://www.mongodb.com/try/download/compass

### Step 3 - Import Dataset 
Download the dataset and import different tables into MongoDB Collections.

### Step 4 - Prepare Queries
Identify and prepare queries for different operations like Selection, Aggregation, Updation, Deletion.

### Step 5 - Index Identification
Identify Column Indices  for faster execution of queries.<br>
Identified indices for the queries executed in our projects were :<br>
  Indices were created on fields - **titleType, tconst, parentTconst** for Collection **titleType**.<br>
  Indices were created on fields - **tconst, directors** for Collection **titleCrew**.<br>
  Indices were created on fields - **nconst, primaryProfession** for Collection **nameBasics**.<br>
  Index was created on field - **tconst** for Collection **titleRating**.<br>
  Compound indices were created on fields - **titleType & startyear, titleType & runtimeMinutes** for Collection **titleType**
  
### Step 6 - Pipeline Execution
Pipelines are separated in a field named pipelines.py.<br>
Changing the pipeline variable name in main.py would help in executing the required pipeline.<br>
Update query pipeline is isolated on a separate file named updateQuery.py <br>

#### Collection names to be used while executing pipelines

**titleCrew:**
Pipeline 1,<br>
Pipeline 5

**titleBasics:**
Pipeline 3,<br>
Pipeline 4,<br>
Pipeline 6,<br>
Pipeline 7,<br>
Pipeline 8,<br>
Pipeline 9

# MySql
All the .sql files are used for setting up the Mysql tables.
The imdb-create-tables.sql is used to create tables in mysql.
The imdb-load-data.sql is used to load the tsv files in the schema.

# Redis

### Step 1 - Download and install redis
Download it from here - https://redis.io/download/

### Step 2 - Prepare .redis files to load data on Redis server
Use the code in the setupData.py to obtain .redis data file. <br>
For simplicity, we are attaching the .redis files we have used to load the data. <br>
Find it here -> https://drive.google.com/drive/folders/1YFCtkPF2GWWwV2I373IRB1DCBIz1laVv?usp=sharing <br>
<br>
To load data on Redis server, use the command -> _redis-cli -h localhost -p 6379 < pathToFile/file_name.redis_


### Step 3 - Execute Queries
Each queries are placed in a separate .py file with two/three implementations.

##### Implementation 1 - Python
Using pure python script, we retrieve the query result from redis server.
The execution time of this slow since python program generates too many roundtrips to Redis on a single connection, so there exists a network latency

##### Implementation 2 - Redis Pieplining in Python
Redis pipelining is comparatively faster than previous implementation because it improves performance by issuing multiple commands at once without waiting for the response to each individual command.
Note that we used pipelins support provided by redis module in Python to achieve this.

##### Implementation 3 - Lua Scripting
This is the most efficient approach since lua script directly runs on redis server so all the commands are executed at once and result is retrieved.


### Step 4 - Peformance evaluation
We used _timeit_ pyhton module to do runtime evaluation around the python functions.<br>

We implemented python and lua script implementations for all the queries.<br>
For demonstration purposes, we implemented Redis Pieplining in Python for few queries.<br>
Runtime for the implemented approaches will be printed at the end of each python files.<br>



# All Queries:
1. Retrieve movie titles along with their directors' names.
2. Calculate the average runtime of movies by directors' professions.
3. Retrieve movie titles released in a specific year and genre
4. Find the average runtime of movies by genre.
5. Find the average runtime of adult movies with a specific genre:
6. List top-rated directors along with their average movie ratings:
7. Find the decade with the highest average movie rating:
8. Find the top 10 most voted TV series with episodes longer than 30 minutes
9. Given a movie title, update numOfVotes for that movie


# Individual Contributions

### Entire Team
1. Contributed to dataset shortlisting 
2. Contributed to the dataset limitation discussions.
3. Contributed to the research of each database technology chosen.
4. Query preparation - Total 9 queries - 3 queries/person.
5. Preparation of power point slides.
6. Presenatation and demo.
7. Report writing.

### Sujith Kumaar - Redis module
1. Formatted dataset to .redis format to load the data
2. Researched on redis pipelining and lua script
3. Designed to data model to use with Redis and came up with the relevant data structures
4. Implemented pure python scripts, redis pipelining scripts and lua scripts for all the 9 queries

### SuryaKangeyan - MongoDB module   
1. Imported entire dataset to IMDB database using MongoDB Compass.
2. Carried out research on python scripts for generating pipelines.
3. Designed implemented and modified python pipelines for the above mentioned queries.
4. Identified appropriate indices to make query evaluation faster.

### Vrushali - MongoDB module  
