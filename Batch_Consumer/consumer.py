from kafka import KafkaConsumer
import os
import kafka.errors
import time
from kafka import TopicPartition
from threading import Thread

def con1(consumer):
    for msg in consumer:
        print(msg)



KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ["MOVIE_DETAILS"]

while True:
    try:
        consumer1 = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER.split(","),
            group_id='grp1',
            )
        consumer2 = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER.split(","),
            group_id='grp1',
            )
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)
time.sleep(15)
consumer1.assign([TopicPartition(TOPIC, 0)])
consumer2.assign([TopicPartition(TOPIC, 1)])

threads = []

t1 = Thread(target=con1, args=(consumer1))
t2 = Thread(target=con1, args=(consumer2))
threads.append(t1)
threads.append(t2)

for x in threads:
     x.start()

 # Wait for all of them to finish
for x in threads:
     x.join()

# for message in consumer1:
#         print("Consumer 1   "+str(it))
#         it +=1
# it1 = 0
# for message in consumer1:
#         print("Consumer 2   "+str(it))
#         it1 +=1

