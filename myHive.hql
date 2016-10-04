== For Hive tables

CREATE EXTERNAL TABLE IF NOT EXISTS UserMovieRatings (
userId int,
movieId int,
rating int,
unixTimestamp bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://us-east-1.elasticmapreduce.samples/sparksql/movielens/user-movie-ratings';

==

CREATE EXTERNAL TABLE IF NOT EXISTS MovieDetails (
movieId int,
title string,
genres array<string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
collection items terminated by '|'
STORED AS TEXTFILE
LOCATION 's3://us-east-1.elasticmapreduce.samples/sparksql/movielens/movie-details';

==

CREATE EXTERNAL TABLE IF NOT EXISTS UserDetails (
userId int,
age int,
gender CHAR(1),
occupation string,
zipCode String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 's3://us-east-1.elasticmapreduce.samples/sparksql/movielens/user-details';

==

SELECT genre, rating, count(*)  ratingCount
FROM UserMovieRatings r
JOIN (SELECT movieid, title ,genre FROM moviedetails lateral view explode(genres) gen AS genre ) m
ON (r.movieid = m.movieid)
GROUP BY genre, rating;