from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DateType, TimestampType
import sys


def writeToCassandra(df, epochId):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="stocks", keyspace="stockdata") \
            .mode("append") \
            .save()
    except Exception as e:
        print("Error writing to Cassandra:", e)


def writeToCassandraGrouped(df, epochId):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="grouped_stocks", keyspace="stockdata") \
            .mode("append") \
            .save()
    except Exception as e:
        print("Error writing grouped data to Cassandra:", e)


def pivotAndWriteToCassandra(batch_df, epochId):
    trade_types = ['buy', 'sell']

    pivoted_df = batch_df.groupBy("stock").pivot(
        "trade_type", trade_types).avg("price")

    for trade_type in trade_types:
        column_name = f"avg_price_{trade_type}"
        pivoted_df = pivoted_df.withColumnRenamed(trade_type, column_name)

    try:
        pivoted_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="pivoted_stocks", keyspace="stockdata") \
            .mode("append") \
            .save()
    except Exception as e:
        print("Error writing pivoted data to Cassandra:", e)


def main():
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra-Stocks") \
        .config("spark.cassandra.connection.host", "172.28.1.3") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    topic = sys.argv[1]

    schema = StructType([
        StructField("stock", StringType()),
        StructField("trade_id", StringType()),
        StructField("price", DecimalType(10, 2)),
        StructField("quantity", IntegerType()),
        StructField("trade_type", StringType()),
        StructField("trade_date", StringType()),
        StructField("trade_time", StringType())
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.28.1.2:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    query_console = df.selectExpr("CAST(value AS STRING)").writeStream \
        .outputMode("append") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    df_parsed = df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col("value"), schema).alias("data")) \
                  .select("data.*")

    query_cassandra = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(writeToCassandra) \
        .start()

    df_parsed = df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col("value"), schema).alias("data")) \
                  .select("data.*")

    df_grouped = df_parsed.groupBy("trade_type").agg(
        avg("price").alias("avg_price"))

    query_cassandra_grouped = df_grouped.writeStream \
        .outputMode("complete") \
        .foreachBatch(writeToCassandraGrouped) \
        .start()

    df_parsed = df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col("value"), schema).alias("data")) \
                  .select("data.*")

    query_pivot = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(pivotAndWriteToCassandra) \
        .start()

    query_console.awaitTermination()
    query_cassandra.awaitTermination()
    query_cassandra_grouped.awaitTermination()
    query_pivot.awaitTermination()


if __name__ == "__main__":
    main()
