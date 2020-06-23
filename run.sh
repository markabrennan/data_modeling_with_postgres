#!/bin/bash

##
## Startup script to run the full pipline:
## First run the create_tables.py script;
## Then run the ETL pipeline.
##

python3 ./create_tables.py
python3 ./etl.py
