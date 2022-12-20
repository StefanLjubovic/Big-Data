from kafka import KafkaConsumer
import os
import kafka.errors
import time
from kafka import TopicPartition
import threading

def con1(consumer):
    print("bbbb")
    for msg in consumer:
        print(msg)


if __name__ == "__main__":
    KAFKA_BROKER = os.environ["KAFKA_BROKER"]
    TOPIC = os.environ["MOVIE_DETAILS"]
    master = "spark://MASTER_PUBLIC_DNS:7077" 
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
            consumer3 = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER.split(","),
                group_id='grp1',
                )
            print("Connected to Kafka!")
            break
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)
    time.sleep(30)
    consumer1.assign([TopicPartition(TOPIC, 0)])
    consumer2.assign([TopicPartition(TOPIC, 1)])
    consumer3.assign([TopicPartition(TOPIC, 2)])
    for msg in consumer2:
        print(msg)



