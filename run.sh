#!/bin/bash

##
## Startup script to run the full pipline:
## First run the create_tables.py script;
## Then run the ETL pipeline.
##

python3.7 ./create_tables.py
python3.7 ./etl.py
