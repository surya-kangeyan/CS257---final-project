# Find the top 10 most voted TV series with episodes longer than 30 minutes
import time
import timeit
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
final = []

def findMostVotedTVSeriesWithEpisodesMoreThan30Mins():
    result = []
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            key = 'movie:' + j
            x = r.hmget(key, ['title', 'titleType', 'runtimeMinutes', 'numVotes'])
            if x[1] and x[1] == 'tvSeries':
                if x[2] and int(x[2]) >= 30:
                    if x[3]:
                        result.append([x[0], int(x[3])])
    result.sort(key= lambda x:x[1], reverse=True)
    print(result[:10])

start = timeit.default_timer()
findMostVotedTVSeriesWithEpisodesMoreThan30Mins()
final.append(["The query response time with Python- :", timeit.default_timer() - start])
print("The difference of time is :", timeit.default_timer() - start)


def findMostVotedTVSeriesWithEpisodesMoreThan30MinsLua():
    lua_script = """

    local result = {}
    
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            local movieKey = 'movie:' .. j
    
            local title, titleType, runtimeMinutes, numVotes = unpack(redis.call('HMGET', movieKey, 'title', 'titleType', 'runtimeMinutes', 'numVotes'))
    
            if titleType and titleType == 'tvSeries' then
                if runtimeMinutes and tonumber(runtimeMinutes) and tonumber(runtimeMinutes) >= 30 then
                    if numVotes then
                        table.insert(result, {title, tonumber(numVotes)})
                    end
                end
            end
        end
    end
    
    table.sort(result, function(a, b)
        return a[2] > b[2]
    end)
    
    local top10 = {}
    for i = 1, math.min(#result, 10) do
        table.insert(top10, result[i])
    end
    
    return top10

        """

    result = r.eval(lua_script, 0)

    if result is not None:
        print(result)
    else:
        print("No adult movies found for the given genre.")

start = timeit.default_timer()
findMostVotedTVSeriesWithEpisodesMoreThan30MinsLua()
final.append(["The query response time with Python Lua- :", timeit.default_timer() - start])
print("The difference of time is :", timeit.default_timer() - start)

print(final)



