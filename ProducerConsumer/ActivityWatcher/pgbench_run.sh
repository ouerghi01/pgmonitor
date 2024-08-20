#!/bin/bash

export PGPASSWORD='postgres'  

# Define common variables
HOST="postgresql_db" 
PORT="5432"
USER="postgres"  
DBNAME="postgres"  


# Run SELECT operations
pgbench -h $HOST -p $PORT -U $USER -f /scripts/select_custom_pgbench.sql -c 100 -T 100 $DBNAME
