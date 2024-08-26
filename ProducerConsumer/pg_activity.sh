#!/bin/bash
TEMP_FILE="ProducerConsumer/Pg_activity_Data/activities.csv"
export PGPASSWORD='postgres'

# Run pg_activity and capture the exit status
pg_activity --output="$TEMP_FILE" --host='localhost' --port='5432' --username='postgres'
STATUS=$?
# Check if the command ran successfully
if [ $STATUS -eq 0 ]; then
    echo "pg_activity ran successfully."
else
    echo "pg_activity failed to run."
fi
