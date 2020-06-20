"""
create_tables.py does the work of dropping and creating
the main database - sparkifydb - as well as running queries
to drop and create five tables used for the data model.

TO DO:  Add more exception handling!

NOTE: Most of these functions come from the project template;
I have added logging and configuration.
"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import sys
import logging
import config_mgr


def create_database(cfg):
    """Create the database, after first dropping it 
    (if it exists).
    NOTE: This function and its steps come from the 
    project template!

    Args:       cfg is an instance of the ConfigMgr class
                which has config info.
    Returns:    DB cursor and connection.
    """
    # connect to "landing" database (pre-existing "studentdb")
    # from where we'll create our new, app-specific DB
    conn = psycopg2.connect(cfg.get_db_landing_connect_string())
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.errors.ObjectInUse as e: 
        logging.warning(f"can't drop DB. Error:  {str(e)}")
        logging.warning('Going to try to create sparkifydb')
    
    logging.info('Successfully dropped sparkifydb')

    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.errors.ObjectInUse as e: 
        logging.critical(f"can't create DB - exiting! Error:  {str(e)}")
        conn.close()    
        sys.exit(-1)
    
    logging.info('Successfully created sparkifydb')

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(cfg.get_db_connect_string())
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """Iterate over list of "drop table" queries and invoke
    the drop table commands on each table.
    Args:       cur: DB cursor
                conn: DB connection
    Returns:    None.
    """
    logging.debug('Running drop table queries')
    for query in drop_table_queries:
        logging.debug(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Iterate over list of "drop table" queries and invoke
    the create table commands on each table.
    Args:       cur: DB cursor
                conn: DB connection
    Returns:    None.
    """
    logging.debug('Running create table queries')
    for query in create_table_queries:
        logging.debug(query)
        cur.execute(query)
        conn.commit()


def main():
    """Main routine coodinates the work
    Args:       None
    Returns:    0 for success
    """
    cur, conn = create_database(config_mgr.ConfigMgr(env='DB'))
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    # success
    return 0


if __name__ == "__main__":
    sys.stderr.write(f'Running create_tables - check db.log\n\n')
    sys.stderr.flush()
    ret_val = main()
    sys.exit(ret_val)