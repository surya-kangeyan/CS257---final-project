# Find the decade with the highest average movie rating

import timeit

import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
final = []
def decadeWithHighestAvgMovieRating():
    result = {}
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            key = 'movie:' + j
            x = r.hmget(key, ['startYear', 'averageRating'])
            if x[0]:
                decade = (int(x[0]) // 10) * 10
                if x[1]:
                    if decade in result:
                        sumOfRatings, count, avgRating = result.get(decade)
                        sumOfRatings += float(x[1])
                        count += 1
                        result[decade] = [sumOfRatings, count, sumOfRatings / count]
                    else:
                        result[decade] = [float(x[1]), 1, float(x[1])]
    maxRating, maxDecade = -1, -1
    for k, v in result.items():
        if v[-1] > maxRating:
            maxRating = v[-1]
            maxDecade = k
    print(maxDecade, maxRating)

start = timeit.default_timer()
decadeWithHighestAvgMovieRating()
final.append(["The query response time with Python - :", timeit.default_timer() - start])
print("The query run time with only python is :", timeit.default_timer() - start)


def decadeWithHighestAvgMovieRatingLua():
    lua_script = """
    -- Lua script for decade with highest average movie rating
    
    local result = {}
    
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            if j ~= '' then
                local movieKey = 'movie:' .. j
    
                -- Retrieve startYear and averageRating using HMGET
                local startYear, averageRating = unpack(redis.call('HMGET', movieKey, 'startYear', 'averageRating'))
    
                if startYear and tonumber(startYear) then
                    local decade = math.floor(tonumber(startYear) / 10) * 10
                                    
                    if averageRating then
                        if result[decade] then
                            local sumOfRatings, count, avgRating = unpack(result[decade])
                            sumOfRatings = sumOfRatings + (averageRating and tonumber(averageRating) or 0)
                            count = count + 1
                            avgRating = sumOfRatings / count
                            result[decade] = {sumOfRatings, count, tostring(avgRating)}
                        else
                            result[decade] = {averageRating and tonumber(averageRating) or 0, 1, tostring(averageRating)}
                        end
                    end
                end
            end
        end
    end
    
    local maxRating, maxDecade = -1, -1

    for k, v in pairs(result) do
        local avgRating = v[3] and tonumber(v[3])
        if avgRating and avgRating > maxRating then
            maxRating = avgRating
            maxDecade = k
        end
    end
    
    -- Ensure rounding consistency
    maxDecade = tostring(math.floor(tonumber(maxDecade) / 10) * 10)
    maxRating = tostring(maxRating)
    
    return {maxDecade, maxRating}


    """

    # Execute the Lua script
    result = r.eval(lua_script, 0)

    # Print the result
    print(result)

start = timeit.default_timer()
decadeWithHighestAvgMovieRatingLua()
final.append(["The query response time with Python Lua- :", timeit.default_timer() - start])
print("The query run time with only python + lua is :", timeit.default_timer() - start)

print(final)