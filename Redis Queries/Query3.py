# Find the average runtime of movies by genre
import timeit
import redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

final = []
def avgRuntimeByGenre(genre):
    time, count = 0, 0
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            key = 'movie:' + j
            x = r.hmget(key, ['runtimeMinutes', 'genres'])
            if x[1] and genre in x[1].split(','):
                if x[0]:
                    time += int(x[0])
                    count += 1
    print("\nAverage runtime of movies by genre: ", time/count)

start = timeit.default_timer()
avgRuntimeByGenre('Romance')
final.append(["The query response time with Python - :", timeit.default_timer() - start])
print("The query run time with only python :", timeit.default_timer() - start)



def avgRuntimeByGenreLua(genre):
    lua_script = """
    local time, count = 0, 0
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            if j ~= '' then
                local movieKey = 'movie:' .. j
        
                -- Retrieve runtimeMinutes and genres using HMGET
                local runtimeMinutes, genres = unpack(redis.call('HMGET', movieKey, 'runtimeMinutes', 'genres'))
        
                if genres and string.find(genres, ARGV[1]) then
                    if runtimeMinutes and tonumber(runtimeMinutes) then
                        time = time + tonumber(runtimeMinutes)
                        count = count + 1
                    end
                end
            end
        end
    end
    
    if count > 0 then
        return time / count
    else
        return nil
    end
    """

    # Execute the Lua script
    result = r.eval(lua_script, 0, genre)

    # Print the result
    if result is not None:
        print("\nAverage runtime of movies by genre:", result)
    else:
        print("No movies found for the given genre.")

start = timeit.default_timer()
avgRuntimeByGenreLua('Romance')
final.append(["The query response time with Python Lua - :", timeit.default_timer() - start])

print("The query run time in lua + python :", timeit.default_timer() - start)

print(final)