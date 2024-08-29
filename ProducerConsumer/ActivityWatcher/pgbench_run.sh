#!/bin/bash

# Set default values for environment variables if not provided
export PGPASSWORD=${PGPASSWORD:-'postgres'}
HOST=${HOST:-'postgresql_db'}
PORT=${PORT:-'5432'}
USER=${USER:-'postgres'}
DBNAME=${DBNAME:-'postgres'}
CONNECTIONS=${CONNECTIONS:-'99'}
DURATION=${DURATION:-'100'}
DELETE_CONNECTIONS=${DELETE_CONNECTIONS:-'1'}
DELETE_DURATION=${DELETE_DURATION:-'1'}

# Define function to run pgbench and handle errors
run_pgbench() {
  local script=$1
  local connections=$2
  local duration=$3

  echo "Running pgbench with script: $script"
  if ! pgbench -h "$HOST" -p "$PORT" -U "$USER" -f "$script" -c "$connections" -T "$duration" "$DBNAME"; then
    echo "pgbench failed for script: $script"
    exit 1
  fi
}

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h "$HOST" -p "$PORT" -U "$USER"; do
  echo "PostgreSQL is not ready yet. Waiting..."
  sleep 2
done

# Run pgbench commands
#run_pgbench "/app/init_custom_pgbench.sql" "$CONNECTIONS" "$DURATION"
run_pgbench "/app/select_custom_pgbench.sql" "$CONNECTIONS" "$DURATION"
run_pgbench "/app/delete_custom_pgbench.sql" "$DELETE_CONNECTIONS" "$DELETE_DURATION"

# Clean up
unset PGPASSWORD
echo "pgbench tests completed successfully."
