--Query1

SET @start_time = CURRENT_TIMESTAMP();
SELECT Names_.name_ AS Director_Name, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Directors
JOIN Titles ON Directors.title_id = Titles.title_id
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Directors.name_id
ORDER BY Avg_Rating DESC;

SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;


EXPLAIN: 
EXPLAIN SELECT Names_.name_ AS Director_Name, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Directors
JOIN Titles ON Directors.title_id = Titles.title_id
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Directors.name_id
ORDER BY Avg_Rating DESC;

EXPLAIN ANALYZE :
EXPLAIN ANALYZE SELECT Names_.name_ AS Director_Name, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Directors
JOIN Titles ON Directors.title_id = Titles.title_id
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Directors.name_id
ORDER BY Avg_Rating DESC;

--Query 2
SET @start_time = CURRENT_TIMESTAMP();
SELECT Titles.primary_title AS TV_Series_Title, Title_ratings.num_votes AS Num_Votes
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'tvSeries' AND Titles.runtime_minutes > 30
ORDER BY Title_ratings.num_votes DESC
LIMIT 10;

SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;


EXPLAIN :

EXPLAIN SELECT Titles.primary_title AS TV_Series_Title, Title_ratings.num_votes AS Num_Votes
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'tvSeries' AND Titles.runtime_minutes > 30
ORDER BY Title_ratings.num_votes DESC
LIMIT 10;

EXPLAIN ANALYZE:
EXPLAIN ANALYZE SELECT Titles.primary_title AS TV_Series_Title, Title_ratings.num_votes AS Num_Votes
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'tvSeries' AND Titles.runtime_minutes > 30
ORDER BY Title_ratings.num_votes DESC
LIMIT 10;


--Query 3
SET @start_time = CURRENT_TIMESTAMP();
UPDATE Title_ratings
SET num_votes = num_votes + 1
WHERE title_id IN (
    SELECT title_id
    FROM Titles
    WHERE primary_title = 'Karla'
);
SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;

EXPLAIN:
EXPLAIN UPDATE Title_ratings
SET num_votes = num_votes + 1
WHERE title_id IN (
    SELECT title_id
    FROM Titles
    WHERE primary_title = 'Karla'
);

EXPLAIN ANALYZE:
EXPLAIN ANALYZE  UPDATE Title_ratings
SET num_votes = num_votes + 1
WHERE title_id IN (
    SELECT title_id
    FROM Titles
    WHERE primary_title = 'Karla'
);