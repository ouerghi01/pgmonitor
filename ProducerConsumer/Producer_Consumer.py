from ProducerConsumer.Producer import Producer_Data_Monitoring
from ProducerConsumer.Consumer import Consumer_Data_Monitoring    
from multiprocessing import  Process, Manager
import time
import os 
import psutil
import threading


     

def consumer(event_stop):
    print('Consumer started')
    
    Consumer_Data_Monitoring()

filename=None 
pid=None 
def producer(event_stop):
    global filename ,pid 
    filename = r"./ProducerConsumer/Pg_activity_Data/"
    print('Producer started')
    def run_producer(event_stop):
       
            try:
                Producer_Data_Monitoring(event_stop, filename)
            except psutil.NoSuchProcess:
                print(f"Process {pid} does not exist.")
                return
            
            event_stop.clear()  # Reset for next iteration

    run_producer(event_stop)
    

def ProducerConsumer():
    event_stop = threading.Event() 
    # Start consumer and producer processes
    consumer_process = Process(target=consumer, args=(event_stop,))
    producer_process = Process(target=producer, args=(event_stop, ))
    
    consumer_process.start()
    producer_process.start()
    
    # Wait for all processes to complete
    consumer_process.join()
    producer_process.join()

def ExecuteProducerConsumer():
     pass
   
    #while True :
      #ProducerConsumer()



