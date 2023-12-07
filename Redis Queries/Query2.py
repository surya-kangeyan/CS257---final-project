# 1.Retrieve movie titles released in a specific year and genre.
import timeit
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
final = []
def retrieveMoviesByYearAndGenre(startYear, genre):
    c = 0
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            key = 'movie:' + j
            x = r.hmget(key, ['title', 'startYear', 'genres'])
            if x[1] == startYear:
                if genre in x[2].split(','):
                    print(c, x)
                    c += 1

start = timeit.default_timer()
retrieveMoviesByYearAndGenre('1965', 'Drama')
final.append(["The query response time with Python- :", timeit.default_timer() - start])
print("The difference of time is :", timeit.default_timer() - start)



def retrieveMoviesByYearAndGenre1(startYear, genre):
    # Create a Redis connection
    # Create a pipeline
    pipeline = r.pipeline()
    c = 0

    # Get all items from the "title_basics" hash
    title_basics_data = r.hgetall("title_basics")

    # Iterate through the items in the pipeline
    for key, value in title_basics_data.items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            movie_key = 'movie:' + j

            # Use the pipeline to retrieve data for the movie_key
            pipeline.hmget(movie_key, ['title', 'startYear', 'genres'])

    # Execute the pipeline
    results = pipeline.execute()

    # Iterate through the results
    for result in results:
        title, movie_start_year, genres_data = result

        if movie_start_year == startYear and genre in genres_data.split(','):
            print(c, result)
            c += 1

start = timeit.default_timer()
retrieveMoviesByYearAndGenre1('1965', 'Drama')
final.append(["The query response time with Python pipeline- :", timeit.default_timer() - start])
print("The difference of time is :", timeit.default_timer() - start)



def retrieveMoviesByYearAndGenre2(startYear, genre):
    # Lua script
    lua_script = """
    local results = {}
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            if j ~= '' then
                local movieKey = 'movie:' .. j

                -- Retrieve title, startYear, and genres using HMGET
                local title, startYear, genres = unpack(redis.call('HMGET', movieKey, 'title', 'startYear', 'genres'))

                if startYear == ARGV[1] and string.find(genres, ARGV[2]) then
                    table.insert(results, {title, startYear, genres})
                end
            end
        end
    end

    return results
    """

    # Execute the Lua script
    results = r.eval(lua_script, 0, startYear, genre)

    # Print the results
    for result in results:
        print(result)

# Example usage
start = timeit.default_timer()
retrieveMoviesByYearAndGenre2('1965', 'Drama')
final.append(["The query response time with Python Lua- :", timeit.default_timer() - start])
print("The difference of time is :", timeit.default_timer() - start)

print(final)