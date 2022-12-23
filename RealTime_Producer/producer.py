import json
from kafka import KafkaProducer,KafkaClient
import kafka.errors
import os
import time 


KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ["MOVIE_CRITICS"]


while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

mod = 0

time.sleep(30)
print("Passed")

with open('./Datasets/rotten_tomatoes_critic_reviews.csv','r') as f:
    reader = csv.reader(f, delimiter = '\t')
    for messages in reader:
        if mod % 3 ==0 :
            producer.send(TOPIC, messages.encode(),partition=0)
        elif mod % 3 ==1 :
            producer.send(TOPIC, messages.encode(),partition=1)
        else:
            producer.send(TOPIC, messages.encode(),partition=2)
        mod += 1
        producer.flush()