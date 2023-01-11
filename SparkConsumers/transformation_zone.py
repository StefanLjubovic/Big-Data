import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import os
import time
import sys 
from pyspark.sql import functions
from pyspark.sql.functions import *
from functools import reduce

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000" 

    spark = SparkSession\
        .builder\
        .appName("TransformationZone")\
        .getOrCreate()

    df = spark.read.json(HDFS_NAMENODE + "/raw/reviews.json")
    movie_df = spark.read.json(HDFS_NAMENODE + "/raw/IMDB_movie_details.json")
    tomatoes_movies_df =  spark.read.csv(HDFS_NAMENODE + "/raw/rotten_tomatoes_movies.csv", header=True)

    df_null = df = df.withColumn("null_count", reduce(lambda acc, c: acc + c, [when(isnull(c), 1).otherwise(0) for c in df.columns]))
    df = df.filter(df_null.null_count <= 2)
    df = df.drop("null_count")
    movie_df = movie_df.withColumn("rating", col("rating").cast("double"))
    df = df.withColumn("is_spoiler", when(df["is_spoiler"] == True, 1).otherwise(0))
    

    df.write.json(HDFS_NAMENODE + "/transformation/reviews.json", mode="overwrite")
    movie_df.write.json(HDFS_NAMENODE + "/transformation/IMDB_movie_details.json", mode="overwrite")
    tomatoes_movies_df.write.csv(HDFS_NAMENODE + "/transformation/rotten_tomatoes_movies.csv",mode="overwrite",header=True)
