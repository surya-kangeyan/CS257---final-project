import time
import timeit
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
final = []
def retrieveMovieTitlesAlongWithDirectosNames():
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            titleKey = 'movie:' + j
            title = r.hmget(titleKey, ['title', 'directors'])
            d = title[1].split(',')
            directors = []
            for dir in d:
                directorKey = 'person:' + dir
                director = r.hmget(directorKey, ['primaryName'])
                directors.append(director[0])
            # print("TITLE: ", title[0], ". DIRECTOR(S): ", directors)


start = timeit.default_timer()
retrieveMovieTitlesAlongWithDirectosNames()
final.append(["The query response time with Python - :", timeit.default_timer() - start])
print("The query response time with Python - :", timeit.default_timer() - start)

def retrieveMovieTitlesAlongWithDirectorsNamesPipeline():
    start_time = time.time()

    pipeline = r.pipeline()

    title_basics_data = r.hgetall("title_basics")

    for key, value in title_basics_data.items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            titleKey = 'movie:' + j

            pipeline.hmget(titleKey, ['title', 'directors'])

    results = pipeline.execute()

    for result in results:
        title = result[0]
        directors_data = result[1]

        if directors_data:
            d = directors_data.split(',')
            directors = []

            for dir in d:
                if dir:
                    directorKey = 'person:' + dir

                    pipeline.hmget(directorKey, ['primaryName'])

            directors_result = pipeline.execute()

            for director_data in directors_result:
                director_name = director_data[0]
                directors.append(director_name)


    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time  # Calculate the execution time

    print(execution_time)

start = timeit.default_timer()
retrieveMovieTitlesAlongWithDirectorsNamesPipeline()
final.append(["The query response time with Python Pipeline - :", timeit.default_timer() - start])
print("The query response time with Python Pipeline - :", timeit.default_timer() - start)


def retrieveMovieTitlesAlongWithDirectorsNamesLua():

    lua_script = """
    local results = {}
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            if j ~= '' then
                local titleKey = 'movie:' .. j

                -- Retrieve title and directors using HMGET
                local title, directors = unpack(redis.call('HMGET', titleKey, 'title', 'directors'))

                if title and directors then
                    local directorsList = {}

                    -- Iterate over directors and retrieve their names
                    for dir in string.gmatch(directors, '([^,]+)') do
                        local directorKey = 'person:' .. dir
                        local director = redis.call('HMGET', directorKey, 'primaryName')
                        if director[1] then
                            table.insert(directorsList, director[1])
                        end
                    end

                    -- Add the result to the list
                    table.insert(results, 'TITLE: ' .. title .. '. DIRECTOR(S): ' .. table.concat(directorsList, ', '))
                end
            end
        end
    end

    return results
    """

    results = r.eval(lua_script, 0)

    for result in results:
        print(result)

start = timeit.default_timer()
retrieveMovieTitlesAlongWithDirectorsNamesLua()
final.append(["The query response time with Python Lua - :", timeit.default_timer() - start])
print("The query response time with Python Lua - :", timeit.default_timer() - start)

print(final)