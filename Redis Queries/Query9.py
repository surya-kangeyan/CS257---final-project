# Given a movie title, update numOfVotes for that movie

import timeit
import redis
import json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
final = []
def updateNumOfVotesForGivenMovie(movie):
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            key = 'movie:' + j
            x = r.hmget(key, ['title', 'numVotes'])

            if x[0] == movie:
                existingVotes = x[1]
                r.hset(key, 'numVotes', int(existingVotes) + 1)
                currentVotes = r.hmget(key, ['numVotes'])
                print(f'Movie: {movie}, Previous votes: {existingVotes}, Current votes: {currentVotes}')
                return

start = timeit.default_timer()
updateNumOfVotesForGivenMovie('Karla')
final.append(["The query response time with Python - :", timeit.default_timer() - start])
print("The query run time with only python is :", timeit.default_timer() - start)


def updateNumOfVotesForGivenMovieLua(movie):
    lua_script = """
    local movie = ARGV[1]
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            if j ~= '' then
                local key = 'movie:' .. j
                local x = redis.call('HMGET', key, 'title', 'numVotes')
    
                if x[1] == movie then
                    local existingVotes = x[2]
                    redis.call('HSET', key, 'numVotes', tonumber(existingVotes) + 1)
                    local currentVotes = tonumber(redis.call('HGET', key, 'numVotes'))
                    return cjson.encode({movie = movie, previousVotes = tonumber(existingVotes), currentVotes = currentVotes})
                end
            end
        end
    end
    return cjson.encode({})  -- Return an empty table if the movie is not found
    """

    result_str = r.eval(lua_script, 0, movie)
    result = json.loads(result_str)
    if result:
        print(
            f"Movie: {result['movie']}, Previous votes: {result['previousVotes']}, Current votes: {result['currentVotes']}")
    else:
        print("Movie not found.")


start = timeit.default_timer()
updateNumOfVotesForGivenMovieLua('Karla')
final.append(["The query response time with Python Lua- :", timeit.default_timer() - start])
print("The query run time with only python + lua is :", timeit.default_timer() - start)

print(final)