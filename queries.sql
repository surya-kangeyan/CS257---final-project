--ALL QUERIES:


--QUERY 1 EXECUTION TIME
SET @start_time = CURRENT_TIMESTAMP();
SELECT Titles.primary_title AS Movie_Title, Names_.name_ AS Director_Name
FROM Titles
JOIN Directors ON Titles.title_id = Directors.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie';

SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;

--QUERY 1 EXPLAIN
EXPLAIN SELECT Titles.primary_title AS Movie_Title, Names_.name_ AS Director_Name
FROM Titles
JOIN Directors ON Titles.title_id = Directors.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie';


--QUERY 1 EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT Titles.primary_title AS Movie_Title, Names_.name_ AS Director_Name
FROM Titles
JOIN Directors ON Titles.title_id = Directors.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie';

--QUERY 2 EXECUTION TIME
SET @start_time = CURRENT_TIMESTAMP();
SELECT Name_worked_as.profession AS Director_Profession, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Directors ON Titles.title_id = Directors.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
JOIN Name_worked_as ON Names_.name_id = Name_worked_as.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Name_worked_as.profession;

SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;


--QUERY 2 EXPLAIN 
EXPLAIN SELECT Name_worked_as.profession AS Director_Profession, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Directors ON Titles.title_id = Directors.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
JOIN Name_worked_as ON Names_.name_id = Name_worked_as.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Name_worked_as.profession;

--QUERY 2 EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT Name_worked_as.profession AS Director_Profession, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Directors ON Titles.title_id = Directors.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
JOIN Name_worked_as ON Names_.name_id = Name_worked_as.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Name_worked_as.profession;


--QUERY 3 EXECUTION TIME
SET @start_time = CURRENT_TIMESTAMP();
SELECT Titles.primary_title AS Movie_Title, Titles.start_year AS Release_Year, Title_genres.genre AS Movie_Genre
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.start_year = 1970 AND Title_genres.genre = 'Action';

SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;

--QUERY 3 EXPLAIN
EXPLAIN SELECT Titles.primary_title AS Movie_Title, Titles.start_year AS Release_Year, Title_genres.genre AS Movie_Genre
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.start_year = 1970 AND Title_genres.genre = 'Action';

--QUERY 3 EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT Titles.primary_title AS Movie_Title, Titles.start_year AS Release_Year, Title_genres.genre AS Movie_Genre
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.start_year = 1970 AND Title_genres.genre = 'Action';

--QUERY 4 EXECUTION TIME
SET @start_time = CURRENT_TIMESTAMP();
SELECT Title_genres.genre AS Movie_Genre, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.title_type = 'movie'
and Title_genres.genre='Comedy';
SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;

--QUERY 4 EXPLAIN
EXPLAIN SELECT Title_genres.genre AS Movie_Genre, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.title_type = 'movie'
and Title_genres.genre='Comedy';

--QUERY 4 EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT Title_genres.genre AS Movie_Genre, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.title_type = 'movie'
and Title_genres.genre='Comedy';

--QUERY 5 EXECUTION TIME
SET @start_time = CURRENT_TIMESTAMP();
SELECT Title_genres.genre AS Movie_Genre, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.title_type = 'movie' AND Titles.is_adult = 1 AND Title_genres.genre = 'Drama'
GROUP BY Title_genres.genre;
SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;

--QUERY 5 EXPLAIN
EXPLAIN SELECT Title_genres.genre AS Movie_Genre, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.title_type = 'movie' AND Titles.is_adult = 1 AND Title_genres.genre = 'Drama'
GROUP BY Title_genres.genre;

--QUERY 5 EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT Title_genres.genre AS Movie_Genre, AVG(Titles.runtime_minutes) AS Avg_Runtime
FROM Titles
JOIN Title_genres ON Titles.title_id = Title_genres.title_id
WHERE Titles.title_type = 'movie' AND Titles.is_adult = 1 AND Title_genres.genre = 'Drama'
GROUP BY Title_genres.genre;


--QUERY 6 EXECUTION TIME
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

--QUERY 6 EXPLAIN
EXPLAIN SELECT Names_.name_ AS Director_Name, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Directors
JOIN Titles ON Directors.title_id = Titles.title_id
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Directors.name_id
ORDER BY Avg_Rating DESC;

--QUERY 6 EXPLAIN ANALYZE 
EXPLAIN ANALYZE SELECT Names_.name_ AS Director_Name, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Directors
JOIN Titles ON Directors.title_id = Titles.title_id
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
JOIN Names_ ON Directors.name_id = Names_.name_id
WHERE Titles.title_type = 'movie'
GROUP BY Directors.name_id
ORDER BY Avg_Rating DESC;

--QUERY 7 EXECUTION TIME
SET @start_time = CURRENT_TIMESTAMP();
SELECT FLOOR(Titles.start_year / 10) * 10 AS Decade_Start_Year, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'movie'
GROUP BY Decade_Start_Year
ORDER BY Avg_Rating DESC
LIMIT 1;
SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;

--QUERY 7 EXPLAIN
EXPLAIN SELECT FLOOR(Titles.start_year / 10) * 10 AS Decade_Start_Year, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'movie'
GROUP BY Decade_Start_Year
ORDER BY Avg_Rating DESC
LIMIT 1;

--QUERY 7 EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT FLOOR(Titles.start_year / 10) * 10 AS Decade_Start_Year, AVG(Title_ratings.average_rating) AS Avg_Rating
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'movie'
GROUP BY Decade_Start_Year
ORDER BY Avg_Rating DESC
LIMIT 1;


--QUERY 8 EXECUTION TIME
SET @start_time = CURRENT_TIMESTAMP();
SELECT Titles.primary_title AS TV_Series_Title, Title_ratings.num_votes AS Num_Votes
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'tvSeries' AND Titles.runtime_minutes > 30
ORDER BY Title_ratings.num_votes DESC
LIMIT 10;

SET @end_time = CURRENT_TIMESTAMP();

SELECT TIMEDIFF(@end_time, @start_time) AS Execution_Time;


--QUERY 8 EXPLAIN 

EXPLAIN SELECT Titles.primary_title AS TV_Series_Title, Title_ratings.num_votes AS Num_Votes
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'tvSeries' AND Titles.runtime_minutes > 30
ORDER BY Title_ratings.num_votes DESC
LIMIT 10;

--QUERY 8 EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT Titles.primary_title AS TV_Series_Title, Title_ratings.num_votes AS Num_Votes
FROM Titles
JOIN Title_ratings ON Titles.title_id = Title_ratings.title_id
WHERE Titles.title_type = 'tvSeries' AND Titles.runtime_minutes > 30
ORDER BY Title_ratings.num_votes DESC
LIMIT 10;

-- QUERY 9 EXECUTION TIME
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

--QUERY 9 EXPLAIN:
EXPLAIN UPDATE Title_ratings
SET num_votes = num_votes + 1
WHERE title_id IN (
    SELECT title_id
    FROM Titles
    WHERE primary_title = 'Karla'
);

--QUERY 9 EXPLAIN ANALYZE
EXPLAIN ANALYZE  UPDATE Title_ratings
SET num_votes = num_votes + 1
WHERE title_id IN (
    SELECT title_id
    FROM Titles
    WHERE primary_title = 'Karla'
);
