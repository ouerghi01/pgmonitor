#!/bin/bash
TEMP_FILE="ProducerConsumer/Pg_activity_Data/activities.csv"
export PGPASSWORD='postgres'
pg_activity --output="$TEMP_FILE" --host='localhost' --port='5432' --username='postgres'
unset PGPASSWORD
echo "pg_activity data has been written to $TEMP_FILE"
