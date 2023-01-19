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
    df = clean_dataframe(df)

    commentCounts = df.groupBy(window(df.timestamp, "1 minute", "30 seconds"), "review_type") \
        .count() \
            .orderBy(desc("review_type")) \
        .limit(10) \
        .orderBy(desc("window")) \
             .withColumn("window", to_json(col("window")))

    newCommentCounts = commentCounts.filter(commentCounts["review_type"] == "Fresh")
    query=newCommentCounts.writeStream.outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: write_df(df, epoch_id, "ratng_avg_rt")) \
    .start()

    query.awaitTermination()
