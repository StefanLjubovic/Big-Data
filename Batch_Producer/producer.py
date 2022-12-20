import json
from kafka import KafkaProducer,KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic
import kafka.errors
import os
import time 


KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ["MOVIE_DETAILS"]


while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER.split(","))
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

mod = 0

time.sleep(15)
# topic_list = []
# topic_list.append(NewTopic(name=TOPIC, num_partitions=1, replication_factor=1))

# kafka_client = KafkaClient(bootstrap_servers=KAFKA_BROKER.split(","))
# server_topics = kafka_client.topic_partitions

# if TOPIC in sever_topics:
# try:
#     deletion = admin_client.delete_topics([TOPIC])
# except:
#     print("error1")
# try:
#     admin_client.create_topics(new_topics=topic_list, validate_only=False)
# except:
#     print("error2")

# print("Passed")
with open('./Datasets/IMDB_reviews.json') as f:
    for line in f:
        # print('sending comment to kafka topic {TOPIC}')
        if mod % 2 ==0 :
            producer.send(TOPIC, line.encode(),partition=0)
        else:
            producer.send(TOPIC, line.encode(),partition=1)
        mod += 1