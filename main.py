from ProducerConsumer.Producer_Consumer import ExecuteProducerConsumer

import os
import asyncio
os.environ['PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT'] = '10'  
import multiprocessing 
import logging
logging.basicConfig(level=logging.DEBUG)
import ray 
def runDBMonitor():
    event_stop = asyncio.Event() 
    db_executor = multiprocessing.Process(target=ExecuteProducerConsumer, args=(event_stop,))
    db_executor.start()    
    db_executor.join()
if __name__ == "__main__":
    ray.init()
    try:
        while True:
            runDBMonitor()
    except KeyboardInterrupt:
        print("Program interrupted. Shutting down...")
        ray.shutdown()
#kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <your-topic-name> --from-beginning
