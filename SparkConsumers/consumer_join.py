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

    count_critics = df.groupBy("rotten_tomatoes_link") \
            .agg(
            count("*").alias("NumberOfCritics")
            ) \
            .orderBy(desc("NumberOfCritics"))

    movie_df = movie_df.withColumnRenamed("rotten_tomatoes_link", "movie_id")
    df_joined = count_critics.join(movie_df, count_critics.rotten_tomatoes_link == movie_df.movie_id, "inner")
    df_joined = df_joined.select("NumberOfCritics", "rotten_tomatoes_link","movie_title")

    query=df_joined.writeStream.outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: write_df(df, epoch_id, "critic_count")) \
    .start()
    
    query.awaitTermination()
