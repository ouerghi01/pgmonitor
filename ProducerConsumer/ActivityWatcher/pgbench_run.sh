#!/bin/bash

export PGPASSWORD='postgres'  

# Define common variables
HOST="postgresql_db" 
PORT="5432"
USER="postgres"  
DBNAME="postgres"  

pgbench -h $HOST -p $PORT -U $USER -f /scripts/init_custom_pgbench.sql -c 10 -T 10 $DBNAME

# Run SELECT operations
pgbench -h $HOST -p $PORT -U $USER -f /scripts/select_custom_pgbench.sql -c 10 -T 10 $DBNAME
pgbench -h $HOST -p $PORT -U $USER -f /scripts/delete_custom_pgbench.sql -c 10 -T 10 $DBNAME

