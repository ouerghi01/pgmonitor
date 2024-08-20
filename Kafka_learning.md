# Learn Apache Kafka with these Python examples

Wikipedia has a great definition of Kafka:  
> Apache Kafka is a distributed event store and stream-processing platform. It is an open-source system developed by the Apache Software Foundation written in Java and Scala. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds. Kafka can connect to external systems (for data import/export) via Kafka Connect, and provides the Kafka Streams libraries for stream processing applications. Kafka uses a binary TCP-based protocol that is optimized for efficiency and relies on a "message set" abstraction that naturally groups messages together to reduce the overhead of the network roundtrip. This "leads to larger network packets, larger sequential disk operations, contiguous memory blocks […] which allows Kafka to turn a bursty stream of random message writes into linear writes.
I think of Kafka as a distributed log system, and you can connect your application to produce messages to it. On the other side, you can attach applications called consumers to read from it.

![image](https://user-images.githubusercontent.com/2369982/166106915-e2c0b109-1bf8-4f1b-8c3b-00e45f41c5e3.png)


I want to show you what Kafka is and how it works using hands-on examples. To accomplish this, we are going to use Docker and Python.
Requirements
Docker (installation guide)
Docker compose (installation guide)
Python3
optional: virtualenv, pyenv


## Running Kafka locally

Let's start Kafka by using the docker-compose configuration provided by bitname:
```
$ git clone https://github.com/rafaelzimmermann/learn-kafka-with-python.git
$ cd learn-kafka-with-python
$ docker-compose up
```

You should know what the first and second lines are doing, on the third line we are downloading a docker-compose configuration provided by bitnami. This docker-compose will allow us to run one instance of Kafka broker and one instance of Zookeeper.
Zookeeper is used by Kafka to store some configuration, and also to help Kafka to manage the cluster. I am not going to get into how Kafka works, but as you progress in your study, reserve some time to read the Kafka documentation.

```
$ docker ps
CONTAINER ID   IMAGE                    PORTS                 
0d21cf862f72   bitnami/kafka:3.1        0.0.0.0:9092->9092/tcp
9ab14b93b163   bitnami/zookeeper:3.8    0.0.0.0:2181->2181/tcp
```

## Creating a topic

I mentioned earlier that I see Kafka as a distributed log system, and the topic is the entity used by Kafka to organize these logs.
Let's say you have a website, and you want to track each page view, then you might have a topic for that, to which you will produce a message every time a browser finishes loading the page. On the same page, you might also want to track how the user behaves, so you might have a topic to store how far the user scrolled or which buttons he clicked on, and so on.
The number of topics and their configuration will depend on each case, but the topic is the abstraction that will help you organize your messages. 
Each topic can be configured differently, depending on how many messages and consumers there are.
```
$ docker exec -it learning-kafka-kafka-1 bash
Now we are running bash inside our Kafka broker container, and there we will have access to some helper scripts that will allow us to manage our topics.
$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic pageview
>
$ kafka-topics.sh --list --bootstrap-server localhost:9092
> pageview
```

kafka-topics.sh is a script shipped with Kafka to help us manipulate the topics. In the first command, we created a topic called pageview. The argument bootstrap-server points to our brokers. As we run the command inside of the broker container, we provided localhost:9092
Right now, our topic has the default configuration, but we will check out different configurations as we go.

## Producing messages to our topic 

Now that we have our topic in place, let's send some messages to it using python. Let's prepare our project:

```
$ mkdir learn-kafka-with-python
$ cd learn-kafka-with-python
```

Let's install a python kafka client library:

```
pip3 install kafka-python
Now we can produce a message:
import random
import time
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
    producer.send(
        topic='pageview',
        key=f"{random.randrange(999)}".encode(),
        value=pageview.to_json().encode()
    )
    time.sleep(1)
```

Let's go through the code in case you are new to Python.  
- Import libraries
- Create an instance of KafkaProducer that will produce to Kafka on localhost:9092
- Produce a json serialized message to the topic pageview and wait 1 second

## Consuming messages from our topic

Before we jump into the code, we will need to understand a bit how Kafka manages the consumption of the messages.

![image](https://user-images.githubusercontent.com/2369982/166106681-cbee23ad-b335-4992-ab5e-43bb59f6ddcc.png)

Image author: https://docs.datastax.com/en/kafka/doc/kafka/kafkaHowMessages.html

The producer application produces messages to the broker for a given topic, and multiple consumers can read from the topic in a independent way or sharing the messages between multiple consumers.
Kafka tracks which messages were processed by each consumer application using a consumer group id. Consumers under a same group id will share the messages, parallelizing the work of consuming the messages.
We also need to tell Kafka from where we want to start reading, from the oldest message or from the most recent one.

```
from kafka import KafkaConsumer

consumer = KafkaConsumer('pageview',
                         auto_offset_reset='earliest',
                         group_id='pageview-group1',
                         bootstrap_servers=['kafka:9092'])
for message in consumer:
    print(f"""
        topic     => {message.topic}
        partition => {message.partition}
        offset    => {message.offset}
        key={message.key} value={message.value}
    """)
The output should be something like this:
topic     => pageview
partition => 0
offset    => 180
key=b'373' value=b'{"pageview": {"url": "URL"}}'
```

## Using multiple consumers

What makes Kafka powerful is how easy we can parallelize the consumption of the messages, taking away complexity from your code.
If you try to start a second consumer with the same consumer group id for our topic pageview, you will notice that the new consumer will start consuming messages, but the old one will become idle. This is because we didn't specify the number of partitions when we created our topic, and the default is one. The number of partition defines the number of consumer for a given consumer group id.

![image](https://user-images.githubusercontent.com/2369982/166106699-647dc954-041c-4422-a4b2-40e1081603c3.png)

![image](https://user-images.githubusercontent.com/2369982/166106704-fdb279b0-1c66-4e51-8192-e532e615d99b.png)


One consumer idle due to the number of partitionsLet's fix our topic configuration:

```
$ docker exec -it learn-kafka-with-python_kafka_1 bash
$ kafka-topics.sh \
    --topic pageview \
    --alter \
    --partitions 2 \
    --bootstrap-server localhost:9092
Now our topic has two partitions:
kafka-topics.sh --topic pageview --describe --bootstrap-server localhost:9092
Topic: pageview TopicId: yG6RS5SESlefaECYvn4mxQ PartitionCount: 2 ReplicationFactor: 1 Configs: segment.bytes=1073741824
 Topic: pageview Partition: 0 Leader: 1001 Replicas: 1001 Isr: 1001
 Topic: pageview Partition: 1 Leader: 1001 Replicas: 1001 Isr: 1001
```

Now you can start a second consumer, and will notice that they will share the work.


