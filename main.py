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
    def db_stress_monitor_wrapper(event_stop):
        try:
            logging.info("DBStressMonitor started")
            DBStressMonitor(event_stop)
        except Exception as e:
            logging.error(f"Error in DBStressMonitor: {e}")
    db_stress_monitor = multiprocessing.Process(target=db_stress_monitor_wrapper, args=(event_stop,))
    db_stress_monitor.start()

    db_executor = multiprocessing.Process(target=ExecuteProducerConsumer, args=(event_stop,))
    #db_monitor_pg_activity = multiprocessing.Process(target=run_performance_test)
    #db_notify=multiprocessing.Process(target=NotificationOn)
    db_executor.start()    
    #db_monitor_pg_activity.start()
    db_stress_monitor.join()
    #db_monitor_pg_activity.join()
    db_executor.join()
if __name__ == "__main__":
    ray.init()
    try:
        while True:
            runDBStressMonitor()
    except KeyboardInterrupt:
        print("Program interrupted. Shutting down...")
        ray.shutdown()