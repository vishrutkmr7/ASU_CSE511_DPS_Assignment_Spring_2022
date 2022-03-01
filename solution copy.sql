CREATE TABLE users (
    userid int NOT NULL,
    name char(20),
    PRIMARY KEY (userid)
);

CREATE TABLE movies (
    movieid int NOT NULL,
    title text,
    PRIMARY KEY (movieid)
);

CREATE TABLE taginfo (
    tagid int NOT NULL,
    content char(255),
    PRIMARY KEY (tagid)
);

CREATE TABLE genres (
    genreid int NOT NULL,
    name char(20),
    PRIMARY KEY (genreid)
);

CREATE TABLE ratings (
    userid int NOT NULL,
    movieid int NOT NULL,
    rating numeric,
    timestamp bigint DEFAULT EXTRACT('epoch' FROM CURRENT_TIMESTAMP) ::bigint,
    PRIMARY KEY (userid, movieid),
    CONSTRAINT valid_time CHECK (timestamp >= 0 AND timestamp <= EXTRACT('epoch' FROM CURRENT_TIMESTAMP)::bigint),
    CONSTRAINT valid_rating CHECK (rating >= 0 AND rating <= 5),
    FOREIGN KEY (userid) REFERENCES users (userid),
    FOREIGN KEY (movieid) REFERENCES movies (movieid)
);

CREATE TABLE tags (
    userid int NOT NULL,
    movieid int NOT NULL,
    tagid int NOT NULL,
    timestamp bigint DEFAULT EXTRACT('epoch' FROM CURRENT_TIMESTAMP) ::bigint,
    PRIMARY KEY (userid, movieid, tagid),
    CONSTRAINT valid_time CHECK (timestamp >= 0 AND timestamp <= EXTRACT('epoch' FROM CURRENT_TIMESTAMP)::bigint),
    FOREIGN KEY (userid) REFERENCES users (userid),
    FOREIGN KEY (movieid) REFERENCES movies (movieid),
    FOREIGN KEY (tagid) REFERENCES taginfo (tagid)
);

CREATE TABLE hasagenre (
    movieid int NOT NULL,
    genreid int NOT NULL,
    PRIMARY KEY (movieid, genreid),
    FOREIGN KEY (movieid) REFERENCES movies (movieid),
    FOREIGN KEY (genreid) REFERENCES genres (genreid)
);

-- Delete these before submitting
COPY users
FROM
    '/Users/vishrutjha/Documents/ASU/CSE 511 DPS/Assignment 1/Coursera-ASU-Database/course1/assignment1/exampleinput/users.dat' DELIMITER '%';

COPY movies
FROM
    '/Users/vishrutjha/Documents/ASU/CSE 511 DPS/Assignment 1/Coursera-ASU-Database/course1/assignment1/exampleinput/movies.dat' DELIMITER '%';

COPY taginfo
FROM
    '/Users/vishrutjha/Documents/ASU/CSE 511 DPS/Assignment 1/Coursera-ASU-Database/course1/assignment1/exampleinput/taginfo.dat' DELIMITER '%';

COPY genres
FROM
    '/Users/vishrutjha/Documents/ASU/CSE 511 DPS/Assignment 1/Coursera-ASU-Database/course1/assignment1/exampleinput/genres.dat' DELIMITER '%';

COPY ratings
FROM
    '/Users/vishrutjha/Documents/ASU/CSE 511 DPS/Assignment 1/Coursera-ASU-Database/course1/assignment1/exampleinput/ratings.dat' DELIMITER '%';

COPY tags
FROM
    '/Users/vishrutjha/Documents/ASU/CSE 511 DPS/Assignment 1/Coursera-ASU-Database/course1/assignment1/exampleinput/tags.dat' DELIMITER '%';

COPY hasagenre
FROM
    '/Users/vishrutjha/Documents/ASU/CSE 511 DPS/Assignment 1/Coursera-ASU-Database/course1/assignment1/exampleinput/hasagenre.dat' DELIMITER '%';

-- Assignment 2 queries
CREATE TABLE query1 AS
SELECT
    genres.name AS name,
    COUNT(hasagenre.movieid) AS moviecount
FROM
    genres
    INNER JOIN hasagenre ON genres.genreid = hasagenre.genreid
GROUP BY
    genres.name;

CREATE TABLE query2 AS
SELECT
    genres.name AS name,
    hasgen.a AS rating
FROM
    genres
    INNER JOIN (
        SELECT
            avg(ratings.rating) AS a,
            hasagenre.genreid
        FROM
            hasagenre
            INNER JOIN ratings ON hasagenre.movieid = ratings.movieid
        GROUP BY
            hasagenre.genreid) hasgen ON genres.genreid = hasgen.genreid;

CREATE TABLE query3 AS
SELECT
    title AS title,
    count(ratings.rating) AS countofratings
FROM
    movies,
    ratings
WHERE
    movies.movieid = ratings.movieid
GROUP BY
    movies.movieid
HAVING
    count(movies.movieid) >= 10;

CREATE TABLE query4 AS
SELECT
    movies.movieid,
    movies.title
FROM
    movies
    INNER JOIN (
        SELECT
            hasagenre.movieid AS movieid,
            hasagenre.genreid AS genreid
        FROM
            hasagenre
        WHERE
            hasagenre.genreid = (
                SELECT
                    genres.genreid
                FROM
                    genres
                WHERE
                    name = 'Comedy')) comedyonly ON comedyonly.movieid = movies.movieid;

CREATE TABLE query5 AS
SELECT
    movies.title,
    hasgen.r AS average
FROM
    movies
    INNER JOIN (
        SELECT
            AVG(ratings.rating) AS r,
            ratings.movieid
        FROM
            ratings
        GROUP BY
            ratings.movieid
        ORDER BY
            ratings.movieid) hasgen ON hasgen.movieid = movies.movieid;

CREATE TABLE query6 AS
SELECT
    avg(rating) AS average
FROM
    ratings AS a,
    hasagenre AS b,
    genres AS c
WHERE
    a.movieid = b.movieid
    AND b.genreid = c.genreid
    AND c.name = 'Comedy';

CREATE TABLE comedyonly AS
SELECT
    hasagenre.movieid AS movieid,
    hasagenre.genreid AS genreid
FROM
    hasagenre
WHERE
    hasagenre.genreid = (
        SELECT
            genres.genreid
        FROM
            genres
        WHERE
            name = 'Comedy');

CREATE TABLE romanceonly AS
SELECT
    hasagenre.movieid AS movieid,
    hasagenre.genreid AS genreid
FROM
    hasagenre
WHERE
    hasagenre.genreid = (
        SELECT
            genres.genreid
        FROM
            genres
        WHERE
            name = 'Romance');

CREATE TABLE romcom AS
SELECT
    romanceonly.movieid AS movieid
FROM
    romanceonly
    INNER JOIN comedyonly ON romanceonly.movieid = comedyonly.movieid;

CREATE TABLE romancebutnotcomedy AS
SELECT
    romanceonly.movieid AS movieid
FROM
    romanceonly
WHERE
    romanceonly.movieid NOT IN (
        SELECT
            movieid
        FROM
            comedyonly);

CREATE TABLE query7 AS
SELECT
    avg(rating) AS average
FROM
    ratings
    INNER JOIN romcom ON ratings.movieid = romcom.movieid;

CREATE TABLE query8 AS
SELECT
    avg(rating) AS average
FROM
    ratings
    INNER JOIN romancebutnotcomedy ON ratings.movieid = romancebutnotcomedy.movieid;

CREATE TABLE query9 AS
SELECT
    ratings.movieid AS movieid,
    ratings.rating AS rating
FROM
    ratings
WHERE
    ratings.userid = :v1;

