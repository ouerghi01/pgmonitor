#!/bin/bash
export PGPASSWORD='postgres'
HOST="postgresql_db"
PORT="5432"
USER="postgres"
DBNAME="postgres"
pgbench -h "$HOST" -p "$PORT" -U "$USER" -f "ProducerConsumer/ActivityWatcher/init_custom_pgbench.sql" -c 99 -T 100 "$DBNAME"
pgbench -h "$HOST" -p "$PORT" -U "$USER" -f "ProducerConsumer/ActivityWatcher/select_custom_pgbench.sql" -c 99 -T 100 "$DBNAME"
pgbench -h "$HOST" -p "$PORT" -U "$USER" -f "ProducerConsumer/ActivityWatcher/delete_custom_pgbench.sql" -c 1 -T 1 "$DBNAME"
unset PGPASSWORD
