from ProducerConsumer.ActivityWatcher.Transaction import DBStressMonitor
from ProducerConsumer.Producer_Consumer import ExecuteProducerConsumer
#from ProducerConsumer.Notify import NotificationOn
#from ProducerConsumer.Producer import run_performance_test
import os
import asyncio
os.environ['PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT'] = '10'  
import multiprocessing 
import logging
logging.basicConfig(level=logging.DEBUG)

import ray 
def runDBStressMonitor():
    event_stop = asyncio.Event() 
    db_stress_monitor = multiprocessing.Process(target=DBStressMonitor)
    db_stress_monitor.start()
    db_executor = multiprocessing.Process(target=ExecuteProducerConsumer, args=(event_stop,))
    db_executor.start()    
    db_stress_monitor.join()
    db_executor.join()
if __name__ == "__main__":
    ray.init()
    try:
        while True:
            runDBStressMonitor()
    except KeyboardInterrupt:
        print("Program interrupted. Shutting down...")
        ray.shutdown()
#kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <your-topic-name> --from-beginning
