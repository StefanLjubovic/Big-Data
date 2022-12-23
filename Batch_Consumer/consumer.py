from kafka import KafkaConsumer
import os
import kafka.errors
import time
from kafka import TopicPartition
import threading
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def con1(consumer):
    print("bbbb")
    for msg in consumer:
        print(msg)


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

if __name__ == "__main__":
    KAFKA_BROKER = os.environ["KAFKA_BROKER"]
    TOPIC = os.environ["MOVIE_DETAILS"]
    master = "spark://spark-master:7077" 
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

    time.sleep(40)
    spark = SparkSession.builder \
        .master(master) \
        .appName("Spark Swarm") \
        .getOrCreate()
    quiet_logs(spark)




