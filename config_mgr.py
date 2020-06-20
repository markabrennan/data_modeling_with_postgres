"""
config.mgr.py

Config Mgr encapsulates code necessary
to fetch config, regardless of environment. It loads
the JSON config file and accesses values through
the dictionary.
"""

import json
import logging
import os
import sys

DEF_CONFIG_SRC = './config.json'


class ConfigMgr:
    def __init__(self, env=None, config_src=None):
        """
        Initialize object with environment,
        defaulting to "DEFAULT" section if it is
        not selected.
        """
        if config_src is None:
            self.config_file = DEF_CONFIG_SRC
        else:
            self.config_file = config_src

        with open(self.config_file, 'r') as json_config:
            self.config = json.load(json_config)
        # get list of top-level environments from config;
        # if env is not one of them, or is None,
        # default to the "DEFAULT" environment.
        environments = [e for e in self.config]
        if env is None or env not in environments:
            self.env = 'DEFAULT'
        else:
            self.env = env

        # set up logging
        log_file = self.config[self.env]['LOG_FILE']
        log_level = self.config[self.env]['LOG_LEVEL']
        logging.basicConfig(filename=log_file,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            level=log_level)
        logging.info(f'Config Mgr: Config set up; env: {self.env}')
        logging.debug(f'CWD: {os.getcwd()}')
        logging.debug(f'search path: {sys.path}')

    def get(self, label):
        """Get the config value with the label.
        Args:       label: key to json dict for config value
        Returns:    The associated config value, or None.
        """
        if label in self.config[self.env]:
            return self.config[self.env][label]
        else:
            logging.warning(f'Config Mgr->get(): label: {label} not configured')
            return None

    def get_db_connect_string(self):
        """ Return a fully-formatted connect string for     
        psycopg2.connect using the config paramters.
        Args:       None
        Returns:    Fully-formmatted psycopg2.connect string
        """
        template_string = "host={} dbname={} user={} password={}"
        return template_string.format(self.get("DB_HOST"),
                                      self.get("DB_NAME"),
                                      self.get("DB_USER"),
                                      self.get("DB_PASSWORD"))

    def get_db_landing_connect_string(self):
        """ Returns a formatted connect string to connect to the 
        "landing" DB (which is the pre-existing "studentdb") in order
        to be able to drop and create the "sparkifydb".
        Args:       None
        Returns:    formatted connect string for the "studentdb" DB.
        """
        template_string = "host={} dbname={} user={} password={}"
        return template_string.format(self.get("DB_HOST"),
                                      self.get("DB_LANDING_NAME"),
                                      self.get("DB_LANDING_USER"),
                                      self.get("DB_LANDING_PASSWORD"))