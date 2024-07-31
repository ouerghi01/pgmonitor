import subprocess
from kafka import KafkaProducer
import csv
import random
import json
import os
import time
from multiprocessing import Event, Process, Manager,Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEvent, PatternMatchingEventHandler
import  watchdog
import getpass
import subprocess

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
                    print(record)
                    self.producer.send(topic='db-monitoring', key=f"{random.randrange(999)}".encode(), value=record)
        except FileNotFoundError as e:
            print(f"Error: {e}")
            self.event_stop.set()
        
def Producer_Data_Monitoring(event_stop, temp_filename):
    event_handler = Handler(event_stop)
    observer = watchdog.observers.Observer()
    observer.schedule(event_handler, path=temp_filename, recursive=False)
    observer.start()
    try:
        while observer.is_alive():
            observer.join(1)
    except KeyboardInterrupt:
        event_stop.set()
        observer.stop()
        event_handler.close()
    observer.join()

