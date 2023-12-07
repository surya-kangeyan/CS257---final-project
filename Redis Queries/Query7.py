# Find the average rating of movies released in the last five years grouped by genre

import time
import timeit
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

final = []
def getAvgRatingOfLast5YearsGroupedByGenre():
    movie_titles = []
    genre = {}
    for key, value in r.hgetall("title_basics").items():
        v = value.split(',')
        for j in v:
            key = 'movie:' + j
            x = r.hmget(key, ['title', 'startYear', 'genres'])
            x.append(key)
            if x[1] and '2018' <= x[1] <= '2023':
                movie_titles.append(x)
                for g in x[2].split(','):
                    if g:
                        if g not in genre: genre[g] = [x[0]]
                        else:
                            genre[g].append(x[0])

    print(genre)

start = timeit.default_timer()
getAvgRatingOfLast5YearsGroupedByGenre()
final.append(["The query response time with Python- :", timeit.default_timer() - start])
print("The difference of time is :", timeit.default_timer() - start)


def getAvgRatingOfLast5YearsGroupedByGenreLua():
    lua_script = """
        -- Lua script for average rating of movies in the last 5 years grouped by genre

    -- Lua script for average rating of movies in the last 5 years grouped by genre

local movie_titles = {}
local genre = {}

for _, value in pairs(redis.call('HGETALL', 'title_basics')) do
    for j in string.gmatch(value, '([^,]+)') do
        local movieKey = 'movie:' .. j

        -- Retrieve title, startYear, and genres using HMGET
        local title, startYear, genres = unpack(redis.call('HMGET', movieKey, 'title', 'startYear', 'genres'))
        local key = movieKey

        if startYear and tonumber(startYear) then
            if tonumber(startYear) >= 2018 and tonumber(startYear) <= 2023 then
                local movieData = {title, startYear, genres, key}

                if genres then
                    for g in string.gmatch(genres, '([^,]+)') do
                        if not genre[g] then
                            genre[g] = {}
                        end
                        table.insert(genre[g], title)
                    end
                end
            end
        end
    end
end

-- Convert genre table to a list of tables
local genreList = {}
for k, v in pairs(genre) do
    table.insert(genreList, {k, v})
end

return genreList


    """

    result = r.eval(lua_script, 0)

    if result is not None:
        print(result)
    else:
        print("No adult movies found for the given genre.")

start = timeit.default_timer()
getAvgRatingOfLast5YearsGroupedByGenreLua()
final.append(["The query response time with Python Lua- :", timeit.default_timer() - start])
print("The difference of time is :", timeit.default_timer() - start)

print(final)