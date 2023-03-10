from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import csv
schema = StructType() \
  .add("rotten_tomatoes_link", StringType()) \
  .add("critic_name", StringType()) \
  .add("top_critic", StringType()) \
  .add("publisher_name", StringType()) \
  .add("review_type", StringType()) \
  .add("review_score", StringType()) \
  .add("review_date", StringType()) \
  .add("review_content", StringType()) \



def clean_dataframe(df):
  df = df.select(
      col("timestamp"),
      from_json(col("value").cast("string"), schema).alias("data")).select("data.*", "timestamp")

  df = df.withColumn("review_score", col("review_score").cast(LongType()))

  return df

def write_df(dataframe,epoch_id,tablename):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    dataframe.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
    ).mode("append").save()

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000" 
    TOPIC = "rotten-recensions"

    spark = SparkSession\
        .builder\
        .appName("StreamingProcessing")\
        .getOrCreate()


    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka2:19093") \
        .option("subscribe", TOPIC) \
        .load()
    movie_df = spark.read.csv(HDFS_NAMENODE + "/transformation/rotten_tomatoes_movies.csv",header=True)
    df = clean_dataframe(df)


    # df = df.filter(df.review_score.isNotNull())
    # top_critics = df.filter("top_critic == 'True'") \
    #           .groupBy("critic_name") \
    #           .agg(
    #             count("*").alias("NumOfTopCritics")
    #           ) \
    #           .orderBy(desc("NumOfTopCritics"))

    # query1=top_critics.writeStream.outputMode("complete") \
    # .foreachBatch(lambda df, epoch_id: write_df(df, epoch_id, "top_critic_by_critics")) \
    # .start()

    top_critics_by_publisher = df.filter("top_critic == 'True'") \
            .groupBy("publisher_name") \
            .agg(
            count("*").alias("NumOfTopCriticsPublisher")
            ) \
            .orderBy(desc("NumOfTopCriticsPublisher"))

    query=top_critics_by_publisher.writeStream.outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: write_df(df, epoch_id, "top_critic_by_publisher")) \
    .start()
    query.awaitTermination()