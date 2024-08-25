#!/bin/bash

# Define the container name and paths
CONTAINER_NAME="python_app"
SCRIPT_PATH="/scripts/Pg_activity_Data"
TEMP_FILE="${SCRIPT_PATH}/activities.csv"

# Create the directory in the container
docker exec "$CONTAINER_NAME" mkdir -p "$SCRIPT_PATH"

# Ensure the temporary file is created
docker exec "$CONTAINER_NAME" touch "$TEMP_FILE"

# Set the PostgreSQL password and run pg_activity inside the container
docker exec "$CONTAINER_NAME" bash -c "
export PGPASSWORD='postgres';
pg_activity --output '$TEMP_FILE' --host='localhost' --port='5432' --username='postgres';
unset PGPASSWORD;
"

# Output success message
echo "pg_activity data has been written to $TEMP_FILE"
