from ProducerConsumer.Producer import Producer_Data_Monitoring
from ProducerConsumer.Consumer import Consumer_Data_Monitoring    
from multiprocessing import Event, Process, Manager
import schedule
import time
import os 
import psutil


     

def consumer(event_stop):

    print("Consumer started")
    Consumer_Data_Monitoring(event_stop)
    pass
filename=None 
pid=None 
def producer(event_stop):
    global filename ,pid 
    filename="ProducerConsumer/Pg_activity_Data/activities.csv"
    print('Producer started')
    def run_producer(event_stop):
        while not event_stop.is_set():
            if filename is not None:
                dir = "./"
                temp_filename_new = os.path.join(dir, filename)
                print(f"Producer: {temp_filename_new}")
                time.sleep(5)
                try:
                    
                    Producer_Data_Monitoring(event_stop, temp_filename_new)
                   
                except psutil.NoSuchProcess:
                    print(f"Process {pid} does not exist.")
                    return
            else:
                print("Error: temp_filename is None")
            event_stop.clear()  # Reset for next iteration

    run_producer(event_stop)
    

def ProducerConsumer():
    event_stop = Event()
    
    
    
    # Start consumer and producer processes
    consumer_process = Process(target=consumer, args=(event_stop,))
    producer_process = Process(target=producer, args=(event_stop, ))
    
    consumer_process.start()
    producer_process.start()
    
    # Wait for all processes to complete
    consumer_process.join()
    producer_process.join()
def ExecuteProducerConsumer():
   

    
    while True:
        ProducerConsumer()
        time.sleep(60)  # Sleep for 60 seconds before starting again



