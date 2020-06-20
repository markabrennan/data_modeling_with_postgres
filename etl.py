"""
etl.py is a single, large script file that implements an ETL pipeline
used to read song and event data from JSON files, and insert data
from these files into the relevant Postgres DB tables.

A main routine coordinates all the work, invoking functions to read
data, and then insert data.
"""

import os
import sys
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from psycopg2.errors import UniqueViolation
import json
from json import JSONDecodeError
import datetime
import re
import logging
from config_mgr import ConfigMgr



def get_files(filepath):
    """Given a file path, walk the directory hierarchy
    and collect the absolute path of all JSON files.
    NOTE:  This file was provided in the etl.ipynb template!

    Args:       file path
    Returns:    list of JSON files
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def get_song_and_artist_data(song_files):
    """Iterate over the song files and collect data for both
    songs and artists.
    Args:       List of song files
    Returns:    list of song data dicts, list of artist data dicts
    """
    song_data = []
    artist_data = []
    for file in song_files:
        with open(file) as f:
            try:
                data = json.load(f)
                song_data.append(dict(song_id=data['song_id'],
                                      title=data['title'],
                                      artist_id=data['artist_id'],
                                      year=data['year'],
                                      duration=data['duration']))
                artist_data.append(dict(artist_id=data['artist_id'],
                                        artist_name=data['artist_name'],
                                        artist_location=data['artist_location'],
                                        artist_latitude=data['artist_latitude'],
                                        artist_longitude=data['artist_longitude']))

            except KeyError as e:
                logging.critical(f'Key Error:  {str(e)}')
                continue   
            except JSONDecodeError as e:
                logging.critical('Msg: {e.msg}, Doc: {e.doc}, Pos: {e.pos}, LineNo: {e.lineno}, ColNo: {e.colno}')
                continue
    return song_data, artist_data


def insert_song_data(song_data, conn, cur):
    """Insert data from the list of song data dicts into the song table.
    Args:       song_data: List of song data dicts
                conn: DB connection
                cur:  DB cursor
    Returns:    None
    """
    for song in song_data:
        try:
            song_data = (song['song_id'], 
                         song['title'],
                         song['artist_id'],
                         song['year'],
                         song['duration'])
            cur.execute(song_table_insert, song_data)
        except psycopg2.Error as e:
            logging.warning('caught psycopg2 exception!')
            logging.warning(e.pgerror)
            logging.warning(e.diag.message_primary)
            continue
        except KeyError as e:
            logging.warrning(f'Key Error:  {str(e)}')
            continue   
    # end of for loop            
    conn.commit()    


def insert_artist_data(artist_data, conn, cur):
    """Insert data from the list of artist data dicts.
    Args:       artist_data: list of artist data dicts
                conn:  DB connection
                cur:   DB cursor
    Returns:    None
    """
    artists_seen = set()    
    for artist in artist_data:
        artist_id = artist['artist_id']
        if artist_id and artist_id not in artists_seen:
            artists_seen.add(artist_id)
            artist_data = (artist_id, 
                           artist['artist_name'],
                           artist['artist_location'],
                           artist['artist_latitude'],
                           artist['artist_longitude'])
            try:
                cur.execute(artist_table_insert, artist_data)
            except psycopg2.Error as e:
                    logging.warning('caught psycopg2 exception!')
                    logging.warning(e.pgerror)
                    logging.warning(e.diag.message_primary)
                    continue
    # end of for loop                    
    conn.commit()


def get_all_log_data(log_files):
    """Read all log event data and build up a list of dictionaries
    containing all the relevant data fields we are interested in using
    for our DB inserts.
    Args:       list of all log event JSON files
    Returns:    list of log data dicts
    """
    all_log_data = []
    for file in log_files:
        with open(file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                except JSONDecodeError as e:
                    logging.warning('Msg: {e.msg}, Doc: {e.doc}, Pos: {e.pos}, LineNo: {e.lineno}, ColNo: {e.colno}')
                    continue
                    
                if data['page'] == 'NextSong':
                    try:
                        all_log_data.append(dict(ts=data['ts'],
                                                 user_id=data['userId'],
                                                 first_name=data['firstName'],
                                                 last_name=data['lastName'],
                                                 gender=data['gender'],
                                                 level=data['level'],
                                                 song_title=data['song'],
                                                 artist_name=data['artist'],
                                                 length=data['length'],
                                                 session_id=data['sessionId'],
                                                 location=data['location'],
                                                 user_agent=data['userAgent']
                                                ))
                    except KeyError as e:
                        logging.warning(f'Key Error:  {str(e)}')
                        continue
    return all_log_data


def get_timestamp(ts):
    """Helper function to construct a date time object
    from the milliseconds timestamp, as well as return
    a formatted date-time string.
    Args:       ts: millisecond timestampe
    Returns:    date time object; date time string.
    """
    dt = datetime.datetime.fromtimestamp(ts / 1000)
    return dt, dt.strftime('%Y-%m-%d %H:%M:%S.%f')


def insert_time_data(all_log_data, conn, cur):
    """Take time data from the list of log event data dicts
    and prepare the insert values, then insert into the
    time table.
    Args:       all_log_data: list of log data dicts
                conn:  DB connection
                cur:   DB cursor
    Returns:    None
    """
    seen_timestamps = set()
    for entry in all_log_data:
        ts = None
        try:
            ts = entry['ts']
        except KeyError as e:
            logging.warning(f'Key Error:  {str(e)}')
            continue
        if ts and ts not in seen_timestamps:
            seen_timestamps.add(ts)
            dt, timestamp = get_timestamp(ts)
            hour = dt.hour
            day = dt.day
            year, week, weekday = dt.isocalendar()
            month = dt.month
            insert_vals = (timestamp, hour, day, week, month, year, weekday)
            try:
                cur.execute(time_table_insert , insert_vals)
            except psycopg2.Error as e:
                    logging.warning('caught psycopg2 exception!')
                    logging.warning(e.pgerror)
                    logging.warning(e.diag.message_primary)
                    continue
    # end of for loop
    conn.commit()


def insert_user_data(all_log_data, conn, cur):
    """Extract user data from the lost of log event data dicts
    and insert into the user table.
    Args:       all_log_data: list of log event data dicts
                conn:  DB connection
                cur:   DB cursor
    Returns:    None
    """
    seen_users = set()
    for entry in all_log_data:
        user_id = first_name = last_name = gender = level = None
        try:
            user_id = entry['user_id']
            first_name = entry['first_name']
            last_name = entry['last_name']
            gender = entry['gender']
            level = entry['level']
        except KeyError as e:
            logging.warning(f'Key Error:  {str(e)}')
            continue
        if user_id and user_id not in seen_users and first_name and last_name and gender and level: 
            seen_users.add(user_id)
            insert_vals = (user_id, first_name, last_name, gender, level)
            try:
                cur.execute(user_table_insert, insert_vals)
            except psycopg2.Error as e:
                    logging.warning('caught psycopg2 exception!')
                    logging.warning(e.pgerror)
                    logging.warning(e.diag.message_primary)
                    continue    
    # end of for loop
    conn.commit()    


def insert_songplay_data(all_log_data, conn, cur):
    """Extract all songplay data attributes from list of
    log event data dicts, as well as fetch associated song_ids and artist_ids 
    (when they exist!) to enrich fields to insert into the main songplay table.
    Args:       all_log_data: list of log data dicts
                conn:  DB connection
                cur:   DB cursor                    
    Returns:    None
    """
    for entry in all_log_data:
        # find the song ID and artist ID based on the title, artist name, and duration of a song.
        #  timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent 
        song_title = artist_name = duration = None
        song_title = entry['song_title'] 
        artist_name = entry['artist_name']
        duration = entry['length']
        if song_title and artist_name and duration:
            # escape apostrophes in song titles
            song_title = re.sub("'", "''", song_title)
            # and escape apostrophes in artist names
            artist_name = re.sub("'", "''", artist_name)            
            query = song_select_qry.format(song_title, artist_name, duration)
            try:
                song_id = artist_id = None
                cur.execute(query)
                row = cur.fetchone()
                if row:
                    song_id, artist_id = row
                
                _, timestamp = get_timestamp(entry['ts'])
                user_id = entry['user_id']
                level = entry['level']
                session_id = entry['session_id']
                location = entry['location']
                user_agent = entry['user_agent']
                insert_vals = (timestamp, user_id, level, song_id, artist_id, 
                               session_id, location, duration)
                cur.execute(songplay_table_insert, insert_vals)

            except psycopg2.Error as e:
                logging.warning('caught psycopg2 exception!')
                logging.warning(e.pgerror)
                logging.warning(e.diag.message_primary)
                continue     
            except KeyError as e:
                logging.warning(f'Key Error:  {str(e)}')
                continue
    # end of for loop                
    conn.commit()    



def main():
    """Main routine to drive all the work for the ETP pipeline.
    Args:       None
    Returns:    0 for success; -1 for failure.
        Processing steps:
            - Read all song and artist data and store in 
            list of data dicts
            - Read all log event data and store in list of 
            data dicts
            - Insert song data
            - Insert artist data
            - Insert time data
            - Insert user data
            - Insert songplays data
    """
    # For now the only way to change the debug environment is statically;
    # TO DO:  add env variable as argument
    cfg = ConfigMgr(env='INFO') 

    try:
        logging.info("Pipeline: connecting to DB")
        conn = psycopg2.connect(cfg.get_db_connect_string())
        cur = conn.cursor()
    except Exception as e:
        logging.critical(f"Failed to connect to DB - aborting: {str(e)}")
        return -1

    # Collect song data files and read all song and artist data
    # and insert
    try:
        logging.info("Pipeline: processing song and artist data")
        song_data, artist_data = get_song_and_artist_data(get_files(cfg.get("SONG_DATA")))
    except Exception as e:
        logging.critical(f"Failed to process song and artist data - aborting: {str(e)}")
        return -1
        
    # insert song data and artist data
    try:
        insert_song_data(song_data, conn, cur)
        insert_artist_data(artist_data, conn, cur)
    except Exception as e:
        logging.critical(f"Failed to insert song and artist data - aborting: {str(e)}")
        return -1
        
    # Collect all log event data
    try:
        logging.info("Pipeline: processing log even data")
        all_log_data = get_all_log_data(get_files(cfg.get("LOG_DATA")))
    except Exception as e:
        logging.critical(f"Failed to process log event data - aborting: {str(e)}")
        return -1
    
    # Insert time data:
    try:
        logging.info("Pipeline: inserting time data")
        insert_time_data(all_log_data, conn, cur)
    except Exception as e:
        logging.critical(f"Failed to insert time data - aborting: {str(e)}")
        return -1

    # insert user data:
    try:
        logging.info("Pipeline: inserting user data")
        insert_user_data(all_log_data, conn, cur)
    except Exception as e:
        logging.critical(f"Failed to insert user data - aborting: {str(e)}")
        return -1

    # insert songplay data:
    try:
        logging.info("Pipeline: inserting songplay data")
        insert_songplay_data(all_log_data, conn, cur)
    except Exception as e:
        logging.critical(f"Failed to insert songplay data - aborting: {str(e)}")
        return -1
        
    # success
    logging.info("Pipeline: completed processing all data")        

    conn.close()
    # return success
    return 0


if __name__ == "__main__":
    sys.stderr.write(f'Running pipeline - check etl.log\n\n')
    sys.stderr.flush()

    ret_val = main()
    sys.exit(ret_val)