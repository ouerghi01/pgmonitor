#!/usr/bin/python 

def getDescText () :
        return  "Options to be passed as checks  \
                backends  -->> Checks for the number of clients/connections currently connected to the specified database\
                connections  -->> Tests the databases if they currently accept connections or not  \
                wals  -->> keeps tab on the number/size of wal files present in the pg_xlog directory  \
                vacuum -->> Inspects the tables and produces a report of those that has not been vauumed for a given period of time  \
                autovacuum -->> Same as vacuum except that this is triggered by the database's internal processes  \
                analyze  -->> reports tables that have not been analyzed after a given period of time  \
                autoanalyze  -->> Same as analyze but triggered by the database's internal processes. \
                table_bloat  -->> Inspect all tables and reports the bloat percentage which is usually a function of vacuum and/or autovacuuming not running regularly  \
                index_bloat -->> Same as table bloat, except for indexes. This could be remedied by reindexing  \
                blocking  -->> This checks for locks transactional/relational that are blocking other processes from running.  \
                nonblocking -->> This checks for locks that are running more than the specified type. This normally do not block, but could become a blocking lock \
                if it runs longer than necessary. In some cases, it is the long running locks that become the blocking types  \
                table_size -->> Monitors table sizes  \
                index_size -->> Monitors index sizes  \
                database_size  -->> Monitors database sizes  \
                checkpoints -->> Monitors checkpoint frequency  \
                duplicate_indexes -->> Monitors indexes and report duplicates  \
                replica_lag -->> Monitors all replicas connected to a given master "
