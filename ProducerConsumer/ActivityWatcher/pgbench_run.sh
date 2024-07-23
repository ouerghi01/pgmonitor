#!/bin/bash

# Set the database password
export PGPASSWORD='000'

# Define common variables
HOST="localhost"
PORT="5432"
USER="aziz"
DBNAME="bench"

# Initialize the database with custom users and posts
pgbench -h $HOST -p $PORT -U $USER -f  ProducerConsumer/ActivityWatcher/init_custom_pgbench.sql -c 500 -t 1000 $DBNAME

# Run SELECT operations
pgbench -h $HOST -p $PORT -U $USER -f ProducerConsumer/ActivityWatcher/select_custom_pgbench.sql -c 100 -T 100 $DBNAME

# Run DELETE operations
pgbench -h $HOST -p $PORT -U $USER -f ProducerConsumer/ActivityWatcher/delete_custom_pgbench.sql -c 10 -T 10 $DBNAME
