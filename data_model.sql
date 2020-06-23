"""
SQL (DDL) for data model.
(Note this file is superfluous, as these queries
are already contained in the sql_queries.py, but they
are included here for completeness.)
"""

CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int NOT NULL,
    duration decimal NOT NULL
)

CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude varchar,
    longitude varchar
)

CREATE TABLE IF NOT EXISTS time(
    timestamp timestamp PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)

CREATE TABLE IF NOT EXISTS users(
    user_id int PRIMARY KEY,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender char NOT NULL,
    level varchar NOT NULL
)
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id serial PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar NOT NULL,
    song_id varchar,
    artist_id varchar,
    session_id int NOT NULL,
    location varchar NOT NULL,
    user_agent varchar NOT NULL
)
