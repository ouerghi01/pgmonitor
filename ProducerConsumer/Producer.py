import subprocess
from kafka import KafkaProducer
import random
import json
import os
import time
from multiprocessing import Event, Process, Manager,Queue
import signal
import multiprocessing
import getpass


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
    start = time.time()
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
    end = time.time()
    execution_time = end - start
    print(f"Execution time: {execution_time:.2f} seconds")


def Producer_Data_Monitoring(event_stop, temp_filename):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    last_position = 0
    while not event_stop.is_set():
        try:
            with open(temp_filename, 'r') as f:
                f.seek(last_position)
                header_skipped = False
                while not event_stop.is_set():
                    line = f.readline()
                    if not line:
                        break  # Exit if no more lines

                    last_position = f.tell()

                    if line.strip():
                        if not header_skipped:
                            header_skipped = True  # Skip the header line
                            continue

                        # Skip non-data lines
                        if line.startswith("*") or "uptime" in line or "Global:" in line:
                            continue

                        # Split the line by semicolon, handling quoted fields
                        parts = line.strip().split(';')
                        if len(parts) >= 14:  # Ensure there are enough fields
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
                                "query": ";".join(parts[14:]).strip('"'),  # Join remaining parts as the query
                            }
                            producer.send(topic='db-monitoring', key=f"{random.randrange(999)}".encode(), value=record)
                    time.sleep(1)  # Sleep briefly to avoid busy-waiting

        except KeyboardInterrupt:
            print("Stopping...")
            break
        except Exception as e:
            print(f"Error reading file: {e}")
            break

    producer.close()
    print("Producer closed.")
