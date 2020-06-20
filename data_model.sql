"""
SQL (DDL) for data model.
(Note this file is superfluous, as these queries
are already contained in the sql_queries.py, but they
are included here for completeness.)
"""


CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year int,
    duration decimal
)

CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude varchar,
    longitude varchar
)

CREATE TABLE IF NOT EXISTS time(
    timestamp timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)

CREATE TABLE IF NOT EXISTS users(
    user_id int primary key,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
)

CREATE TABLE IF NOT EXISTS songplays(
    songplay_id serial PRIMARY KEY,
    start_time timestamp,
    user_id varchar,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar
)