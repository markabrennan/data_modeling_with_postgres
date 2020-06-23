"""
This file contains static query data to be
used in the create_tables.py script and etl.py script.
"""


# DROP TABLES
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
user_table_drop = "DROP TABLE IF EXISTS users"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"


# CREATE TABLES
song_table_create = """CREATE TABLE IF NOT EXISTS songs (
                        song_id varchar PRIMARY KEY,
                        title varchar NOT NULL,
                        artist_id varchar NOT NULL,
                        year int NOT NULL,
                        duration decimal NOT NULL)"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists(
                            artist_id varchar PRIMARY KEY,
                            name varchar NOT NULL,
                            location varchar,
                            latitude varchar,
                            longitude varchar)"""

time_table_create = """CREATE TABLE IF NOT EXISTS time(
                            timestamp timestamp PRIMARY KEY,
                            hour int NOT NULL,
                            day int NOT NULL,
                            week int NOT NULL,
                            month int NOT NULL,
                            year int NOT NULL,
                            weekday int NOT NULL)"""

user_table_create = """CREATE TABLE IF NOT EXISTS users(
                            user_id int PRIMARY KEY,
                            first_name varchar NOT NULL,
                            last_name varchar NOT NULL,
                            gender char NOT NULL,
                            level varchar NOT NULL)"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id serial PRIMARY KEY,
                            start_time timestamp NOT NULL,
                            user_id varchar NOT NULL,
                            level varchar NOT NULL,
                            song_id varchar,
                            artist_id varchar,
                            session_id int NOT NULL,
                            location varchar NOT NULL,
                            user_agent varchar NOT NULL)"""

# INSERT RECORDS
song_table_insert = """INSERT INTO songs(song_id, title, artist_id, year, duration)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) DO NOTHING"""

artist_table_insert = """INSERT INTO artists(artist_id, name, location, latitude, longitude) 
                            VALUES(%s, %s, %s, %s, %s)
                            ON CONFLICT (artist_id) DO NOTHING"""

time_table_insert = """INSERT INTO time(timestamp, hour, day, week, month, year, weekday) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)"""

user_table_insert = """INSERT INTO users(user_id, first_name, last_name, gender, level) 
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level"""

songplay_table_insert = """INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""


# FIND SONGS
song_select_qry = "SELECT song_id, s.artist_id \
                    FROM songs s JOIN artists a on s.artist_id = a.artist_id \
                    WHERE title = '{}' and a.name = '{}' and duration = {}"


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]