import json
from kafka import KafkaProducer,KafkaClient
import kafka.errors
import os
import time 
import json
import csv

def convert_line_to_json(schema, line):
    row = '{'
    columns = schema.split(',')

    for i in range(len(columns)):
        row += '"' + columns[i].strip() + '":' + '"' + items[i].strip() + '"'
        row += ','

    if row[len(row) - 1] == ',':
        row = row[:-1]
    row += '}'

    print(row)

    return row


KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ["MOVIE_CRITICS"]
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

# while True:
#     time.sleep(3)

with open("./datasets/rotten_tomatoes_critic_reviews.csv",'r') as f:
    reader = csv.reader(f)
    header = next(reader) 
    for row in reader:
        data = {header[i]: row[i] for i in range(len(header))}
        producer.send(TOPIC, str(data).encode())
        time.sleep(1)
producer.flush()