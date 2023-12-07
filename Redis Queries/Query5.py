# List top-rated directors along with their average movie ratings

import timeit

import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
finalResult = []


def getTopRatedDirectorsWithAvgRating():
    result = {}
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            if not j:
                continue
            key = 'movie:' + j
            x = r.hmget(key, ['directors', 'averageRating'])
            for d in x[0].split(','):
                key = 'person:' + d
                y = r.hmget(key, ['primaryName'])
                name = y[0]
                if name in result:
                    sumOfRatings, count, avgRating = result.get(name)
                    sumOfRatings = sumOfRatings + float(x[1]) if x[1] else sumOfRatings
                    count += 1
                    result[name] = [sumOfRatings, count, sumOfRatings / count]
                else:
                    if x[1]: result[name] = [float(x[1]), 1, float(x[1])]
    final = []
    for k, v in result.items():
        final.append([k, v[-1]])
    final.sort(key=lambda x: x[-1], reverse=True)
    print(final)

#
# start = timeit.default_timer()
# getTopRatedDirectorsWithAvgRating()
# finalResult.append(["The query response time with Python - :", timeit.default_timer() - start])
# print("The query run time with only python is :", timeit.default_timer() - start)


def getTopRatedDirectorsWithAvgRatingLua():
    lua_script = """    
    local result = {}
    
    for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
        for j in string.gmatch(value, '([^,]+)') do
            if j ~= '' then
                local movieKey = 'movie:' .. j
    
                local directors, averageRating = unpack(redis.call('HMGET', movieKey, 'directors', 'averageRating'))
    
                if directors and type(directors) == 'string' then
                    for director in directors:gmatch('[^,]+') do
                        local directorKey = 'person:' .. director
                        local name = redis.call('HMGET', directorKey, 'primaryName')[1]
    
                        if result[name] then
                            local sumOfRatings, count, avgRating = unpack(result[name])
                            sumOfRatings = sumOfRatings + (averageRating and tonumber(averageRating) or 0)
                            count = count + 1
                            avgRating = sumOfRatings / count
                            result[name] = {sumOfRatings, count, tostring(avgRating)}
                        else
                            result[name] = {averageRating and tonumber(averageRating) or 0, 1, tostring(averageRating)}
                        end
                    end
                end
            end
        end
    end
    
    local final = {}
    for k, v in pairs(result) do
        table.insert(final, {k, v[2] ~= nil and tonumber(v[3]) or nil})
    end
    
    table.sort(final, function(a, b)
        return a[2] and b[2] and tonumber(a[2]) > tonumber(b[2])
    end)
    
    -- Convert avgRating back to string for final result
    for _, v in ipairs(final) do
        v[2] = v[2] ~= nil and tostring(v[2]) or nil
    end

    return final
    """

    result = r.eval(lua_script, 0)
    print(result)


start = timeit.default_timer()
getTopRatedDirectorsWithAvgRatingLua()
finalResult.append(["The query response time with Python Lua- :", timeit.default_timer() - start])
print("The query run time with only python + lua is :", timeit.default_timer() - start)

for f in finalResult:
    print(f)
