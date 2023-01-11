import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import os
import time
import sys 

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000" 

    spark = SparkSession\
        .builder\
        .appName("HDFSData")\
        .getOrCreate()

    df = spark.read.json("../spark/primeri//Datasets/reviews.json")
    df.write.json(HDFS_NAMENODE + "/raw/reviews.json", mode="overwrite")

    movie_df = spark.read.json("../spark/primeri//Datasets/IMDB_movie_details.json")
    movie_df.write.json(HDFS_NAMENODE + "/raw/IMDB_movie_details.json", mode="overwrite")

    tomatoes_movies_df =  spark.read.csv("../spark/primeri//Datasets/rotten_tomatoes_movies.csv", header=True)
    tomatoes_movies_df.write.csv(HDFS_NAMENODE + "/raw/rotten_tomatoes_movies.csv",mode="overwrite",header=True)

    tomatoes_movies_df =  spark.read.csv("../spark/primeri//Datasets/rotten_tomatoes_critic_reviews.csv", header=True)
    tomatoes_movies_df.write.csv(HDFS_NAMENODE + "/raw/rotten_tomatoes_critic_reviews.csv",mode="overwrite",header=True)


