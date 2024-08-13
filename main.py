from ProducerConsumer.ActivityWatcher.Transaction import DBStressMonitor
from ProducerConsumer.Producer_Consumer import ExecuteProducerConsumer
from ProducerConsumer.Notify import NotificationOn
from ProducerConsumer.Producer import run_performance_test
import multiprocessing 
import time
import ray 
def runDBStressMonitor():
    db_stress_monitor = multiprocessing.Process(target=DBStressMonitor)
    db_executor = multiprocessing.Process(target=ExecuteProducerConsumer)
    db_monitor_pg_activity = multiprocessing.Process(target=run_performance_test)
    #db_notify=multiprocessing.Process(target=NotificationOn)
    db_stress_monitor.start()
    db_executor.start()    
    db_monitor_pg_activity.start()
    db_stress_monitor.join()
    db_monitor_pg_activity.join()
    db_executor.join()
if __name__ == "__main__":
    ray.init()
    try:
        while True:
            runDBStressMonitor()
            time.sleep(5)  
    except KeyboardInterrupt:
        print("Program interrupted. Shutting down...")
        ray.shutdown()