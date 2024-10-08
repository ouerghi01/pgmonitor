from ProducerConsumer.Producer import Producer_Data_Monitoring
from ProducerConsumer.Consumer import Consumer_Data_Monitoring    
from multiprocessing import  Process
import psutil
def consumer(event_stop):
    print('Consumer started')
    Consumer_Data_Monitoring(event_stop)
filename=None 
def producer(event_stop):
    global filename  
    filename = r"/app/ProducerConsumer/Pg_activity_Data/"
    print('Producer started')
    def run_producer(event_stop):
            try:
                Producer_Data_Monitoring(event_stop, filename)
            except psutil.NoSuchProcess:
                print('Process is not running')
                return
            event_stop.clear()  
    run_producer(event_stop)
    
def ProducerConsumer(event_stop):
    consumer_process = Process(target=consumer, args=(event_stop,))
    producer_process = Process(target=producer, args=(event_stop, ))
    consumer_process.start()
    producer_process.start()
    consumer_process.join()
    producer_process.join()

def ExecuteProducerConsumer(event_stop):
     while True :
      ProducerConsumer(event_stop)



