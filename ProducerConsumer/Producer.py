import subprocess
from kafka import KafkaProducer
import logger
from datetime import datetime
import asyncpg
import asyncio
import logging
import csv
import random
from sshtunnel import SSHTunnelForwarder
import paramiko
import json
import os
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaProducer
import time
from multiprocessing import Event, Process, Manager,Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEvent, PatternMatchingEventHandler
import  watchdog
import getpass
import subprocess
import threading as thread
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryTracker:
    _instance = None
    _connection_pool = None
    _ssh_tunnel = None
    dbname = "bench"
    user = "postgres"
    password = "123"
    db_params = {
        'database': dbname,
        'user': user,
        'password': password,
        'host': 'localhost',  # Connect through local port after tunneling
        'port': '5432'        # Local port after forwarding
    }
    # SSH and database credentials
    #ssh_host = "192.168.1.1"
    #ssh_port = 22
    #ssh_username = 'user'
    #ssh_key_file = '/path/to/private/key'
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(QueryTracker, cls).__new__(cls)
        return cls._instance
    def __init__(self,event_stop,topic="query-monitoring"):
        if not hasattr(self, 'initialized'):
            self.ai_producer=None
            self.topic = topic
            self.thread_pool = ThreadPoolExecutor(max_workers=10)
            self.initialized = False
            self.event_stop = event_stop
            self.queries=[
                ('pg_stat_activity', 'SELECT * FROM pg_stat_activity;'),
                ('pg_stat_user_tables','''
    SELECT
        s.schemaname,
        s.relname AS table_name,
        pg_size_pretty(pg_total_relation_size(s.relid)) AS total_size,
        pg_size_pretty(pg_relation_size(s.relid)) AS table_size,
        pg_size_pretty(pg_total_relation_size(s.relid) - pg_relation_size(s.relid)) AS indexes_size,
        s.seq_scan AS sequential_scans,
        s.idx_scan AS index_scans
    FROM
        pg_stat_user_tables s
    ORDER BY
        pg_total_relation_size(s.relid) DESC;

    ''')
            ]
    async def setup(self):
        if not self.initialized:
            self.ai_producer = AIOKafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),enable_idempotence=True
            )
            await self.ai_producer.start()
            self.initialized = True
    @classmethod
    async def establish_connection(cls):
        if not cls._connection_pool:
           try:
              # cls.ssh_tunnel = SSHTunnelForwarder(
              #   (cls.ssh_host, cls.ssh_port),
              #   ssh_username=cls.ssh_username,
              #   ssh_pkey=cls.ssh_key_file,
              #   remote_bind_address=('localhost', 5432),
              #   local_bind_address=('127.0.0.1', 5433)
              # )
              
              #cls._ssh_tunnel.start()
              cls._connection_pool = await asyncpg.create_pool(
                    **cls.db_params,
                    max_size=100,
                    min_size=50
                )
              logger.info("Database pool connection opened")
           except Exception as e:
                logger.exception("Error establishing database connection pool: %s", e)
        return cls._connection_pool
    async def run_query(self, query):
        if not self._connection_pool:
            await self.establish_connection()
        query_type, query_string = query
        
        try:
            async with self._connection_pool.acquire() as conn:
                rows = await conn.fetch(query_string)
                if not rows:
                    return
                result = [dict(row) for row in rows]  # convert rows to dictionaries
                for item in result:
                    for key, value in item.items():
                        if isinstance(value, datetime):
                            item[key] = value.isoformat()
                await self.ai_producer.send(
                    self.topic,
                    key=f"{random.randrange(999)}".encode(),
                    value={
                        "type_query": query_type,
                        "data": result
                    }
                )
        except Exception as e:
            logger.error("Error occurred while sending data to Kafka: %s", str(e))
            raise
        
    async def close(self):
        if self._connection_pool:
            try:
                await self._connection_pool.close()
                logger.info("Database pool connection closed")
            except Exception as e:
                logger.exception("Error closing database pool connection: %s", e)
        else:
            logger.warning("Connection pool was not initialized or already closed")
            
    async def run_queries(self):
        await self.setup()
        tasks = [self.run_query(query) for query in self.queries]
        results=await asyncio.gather(*tasks,return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error("Error occurred during query execution: %s", str(result))
        
def run_performance_test():
    bash_script_path = 'ProducerConsumer/pg_activity.sh'
    if not os.path.exists(bash_script_path):
        raise FileNotFoundError(f"The file {bash_script_path} does not exist.")
    current_user = getpass.getuser()
    try:
        subprocess.run(["sudo", "chown", current_user, bash_script_path], check=True)
        subprocess.run(["sudo", "chown", "postgres", "ProducerConsumer/Pg_activity_Data/activities.csv"], check=True)

        subprocess.run(["sudo", "chmod", "+x", bash_script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        exit(1)
    try:
        result = subprocess.Popen([bash_script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = result.communicate()
        if result.returncode != 0:
            print(f"Error running the script: {stderr}")
            exit(1)
        if result.pid:
            print(f"Script PID: {result.pid}")
        
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        exit(1)

class Handler(PatternMatchingEventHandler):
    def __init__(self, event_stop):
        PatternMatchingEventHandler.__init__(self, patterns=['*.csv'], ignore_directories=True, case_sensitive=False)
        self.producer=KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.event_stop = event_stop
        self.temp_filename_csv = r"./ProducerConsumer/Pg_activity_Data/activities.csv"
        self.last_position = 0
        self.skip_header = False
        super().__init__()
    def on_modified(self, event) -> None:
        if event.src_path==self.temp_filename_csv:
           self.process_event(event)
       
    def process_event(self, event):
        try:
              with open(event.src_path, 'r') as f:
                rows = list(csv.reader(f))
                if len(rows) <= 1:
                    return  
                last_line = rows[-1]
                if (len(last_line)>1):
                    last_line = last_line[0]+","+last_line[1]
                else:
                    last_line = last_line[0]
                if not last_line:
                    time.sleep(0.01)
                    return
                
                parts = last_line.split(";")
                if len(parts) >= 14:
                    record = {
                        "datetimeutc": parts[0].strip('"'),
                        "pid": parts[1].strip('"'),
                        "database": parts[2].strip('"'),
                        "appname": parts[3].strip('"'),
                        "user": parts[4].strip('"'),
                        "client": parts[5].strip('"'),
                        "cpu": parts[6].strip('"'),
                        "memory": parts[7].strip('"'),
                        "read": parts[8].strip('"'),
                        "write": parts[9].strip('"'),
                        "duration": parts[10].strip('"'),
                        "wait": parts[11].strip('"'),
                        "io_wait": parts[12].strip('"'),
                        "state": parts[13].strip('"'),
                        "query": ";".join(parts[14:]).strip('"'),
                    }
                    self.producer.send(topic='db-monitoring', key=f"{random.randrange(999)}".encode(), value=record)
        except FileNotFoundError as e:
            print(f"Error: {e}")
            self.event_stop.set()
async def query_monitoring_task(event_stop):
    query_tracker = QueryTracker(event_stop)
    try:
        while not query_tracker.event_stop.is_set():
           await query_tracker.run_queries()
    finally:
        await query_tracker.close()
        print("Connection closed.")
        event_stop.set()
        return
def start_query_monitoring(event_stop):
    asyncio.run(query_monitoring_task(event_stop))
def Producer_Data_Monitoring(event_stop, temp_filename):
    process_query_monitoring = Process(target=start_query_monitoring, args=(event_stop,))
    event_handler = Handler(event_stop)
    observer = watchdog.observers.Observer()
    observer.schedule(event_handler, path=temp_filename, recursive=False)
    observer.start()
    process_query_monitoring.start()
    try:
        while observer.is_alive():
            observer.join(1)
    except KeyboardInterrupt:
        event_stop.set()
        observer.stop()
        event_handler.close()
        observer.join()
        process_query_monitoring.join()
    finally:
        if observer.is_alive():
            observer.stop()
            observer.join()

