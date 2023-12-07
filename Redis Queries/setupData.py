import csv

with open('/Users/spartan/Desktop/My Files/SJSU/Fall 23/CS 257/Project/title.basics.tsv', 'r',
          encoding='utf-8') as tsvfile, open(
    '/Users/spartan/Desktop/My Files/SJSU/Fall 23/CS 257/Project/Dataset/title_basics_main withAllData.redis', 'w',
    encoding='utf-8') as redisfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')

    l = ''
    for i, row in enumerate(reader):
        tconst = row['tconst']
        l = l + ',' + tconst
    redisfile.write(f'HSET "title_basics" movies "{l}"\n')

# Ex - "title_basics" : {"movie:00919121", "movie:00919121", "movie:00919121",...}


with open('/Users/spartan/Desktop/My Files/SJSU/Fall 23/CS 257/Project/title.basics.tsv', 'r',
          encoding='utf-8') as tsvfile, open(
    '/Users/spartan/Desktop/My Files/SJSU/Fall 23/CS 257/Project/Dataset/title_basics.redis', 'w',
    encoding='utf-8') as redisfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')

    for i, row in enumerate(reader):
        if i >= 100000:
            break

        tconst = row['tconst']
        redisfile.write(f'HSET "movie:{tconst}" title "{row["primaryTitle"]}" titleType "{row["titleType"]}"\n')

# Ex - "movie:00919121" : {"movieName", "OtherDetails",....}

def process_tsv_to_redis(tsv_file, redis_file):
    with open(tsv_file, 'r') as tsv, open(redis_file, 'w') as redis:
        header = tsv.readline().strip().split('\t')
        for line in tsv:
            data = line.strip().split('\t')
            movie_id = data[0]
            redis.write(f'HSET "movie:{movie_id}" ')
            for i in range(1, len(header)):
                key = header[i]
                value = data[i] if data[i] != '\\N' else ''
                redis.write(f'"{key}" "{value}" ')
            redis.write('\n')


# redis-cli -h localhost -p 6379 < /Users/spartan/Desktop/cs257dataset/title_basics.redis
