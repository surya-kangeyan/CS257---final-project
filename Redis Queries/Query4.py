# Query 1: Find the average runtime of adult movies with a specific genre
import timeit
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
final = []
def getAvgRuntimeOfAdultMoviesWithGenre(genre):
    avg = 0
    c = 0
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            key = 'movie:' + j
            x = r.hmget(key, ['runtimeMinutes', 'isAdult', 'genres'])
            if x[1] == '1' and genre in x[2].split(','):
                c += 1
                if x[0]: avg += int(x[0])
    print(avg/c)

start = timeit.default_timer()
getAvgRuntimeOfAdultMoviesWithGenre('Drama')
final.append(["The query response time with Python - :", timeit.default_timer() - start])
print("The query run time with only python is :", timeit.default_timer() - start)


def getAvgRuntimeOfAdultMoviesWithGenreLua(genre):
    lua_script = """
    local avg = 0
    local count = 0
    
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            if j ~= '' then
                local movieKey = 'movie:' .. j
    
                -- Retrieve runtimeMinutes, isAdult, and genres using HMGET
                local runtimeMinutes, isAdult, genres = unpack(redis.call('HMGET', movieKey, 'runtimeMinutes', 'isAdult', 'genres'))
    
                if isAdult == '1' and string.find(genres, ARGV[1]) then
                    if runtimeMinutes and tonumber(runtimeMinutes) then
                        avg = avg + tonumber(runtimeMinutes)
                        count = count + 1
                    end
                end
            end
        end
    end
    
    if count > 0 then
        return avg / count
    else
        return nil
    end
    """

    result = r.eval(lua_script, 0, genre)

    if result is not None:
        print(result)
    else:
        print("No adult movies found for the given genre.")

start = timeit.default_timer()
getAvgRuntimeOfAdultMoviesWithGenreLua('Drama')
final.append(["The query response time with Python Lua- :", timeit.default_timer() - start])
print("The query run time with only python + lua is :", timeit.default_timer() - start)

print(final)