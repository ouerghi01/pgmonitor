

SELECT * FROM users,posts ;

SELECT * FROM pg_stat_activity;
SELECT * FROM pg_stat_replication;
SELECT * FROM pg_stat_database;
SELECT * FROM pg_stat_user_tables;
SELECT * FROM pg_stat_user_indexes;
SELECT * FROM pg_stat_user_functions;
SELECT pg_size_pretty(pg_database_size('bench'));
SELECT pg_size_pretty(pg_total_relation_size('users'));
SELECT pg_size_pretty(pg_total_relation_size('posts'));

