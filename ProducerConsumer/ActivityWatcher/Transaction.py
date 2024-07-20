import subprocess
import schedule # Install with `pip install schedule`
import time
import os
import getpass
from threading import Thread
from multiprocessing import Event
import ray
counter = 0
stop_event = Event()

def run_performance_test():
    bash_script_path = 'ProducerConsumer/ActivityWatcher/pgbench_run.sh'
    if not os.path.exists(bash_script_path):
        raise FileNotFoundError(f"The file {bash_script_path} does not exist.")
    current_user = getpass.getuser()
    try:
        subprocess.run(["sudo", "chown", current_user, bash_script_path], check=True)
        subprocess.run(["sudo", "chmod", "+x", bash_script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        exit(1)
    start = time.time()
    try:
        result = subprocess.run([bash_script_path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        exit(1)
    end = time.time()
    execution_time = end - start
    print(f"Execution time: {execution_time:.2f} seconds")
    stop_event.set()  # Signal to stop periodic tasks

def DBStressMonitor():
    print("DBStressMonitor started")
    while True:
        run_performance_test()
        stop_event.clear()  
        time.sleep(60)  


