from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, row_number, date_format, to_date
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
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


def rollupAndWriteToCassandra(batch_df, epochId):
    trade_date_format = 'yyyy-MM-dd'
    string_columns = [col(c).cast(StringType()) for c in batch_df.columns]
    batch_df_string = batch_df.select(*string_columns)
    batch_df_string = batch_df_string.filter(
        batch_df_string["trade_date"].isNotNull())
    batch_df_string = batch_df_string.withColumn("trade_date",
                                                 date_format(to_date("trade_date", "yyyy-MM-dd"), trade_date_format))
    rolled_up_df = batch_df_string.groupBy(
        "trade_date", "trade_type").agg(avg("price").alias("avg_price"))

    try:
        print("DataFrame content before writing to Cassandra:")
        rolled_up_df.show()
        rolled_up_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="rollup_stocks", keyspace="stockdata") \
            .mode("append") \
            .save()
    except Exception as e:
        print("Error writing rolled up data to Cassandra:", e)


def rankAndWriteToCassandra(batch_df, epochId):
    windowSpec = Window.partitionBy("trade_type").orderBy("price")
    ranked_df = batch_df.withColumn("rank", row_number().over(windowSpec))
    try:
        ranked_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="ranked_stocks", keyspace="stockdata") \
            .mode("append") \
            .save()
    except Exception as e:
        print("Error writing ranked data to Cassandra:", e)


def analyticsAndWriteToCassandra(batch_df, epochId):
    analytics_df = batch_df.withColumn(
        "avg_price_overall", avg("price").over(Window.partitionBy()))
    try:
        analytics_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="analytics_stocks", keyspace="stockdata") \
            .mode("append") \
            .save()
    except Exception as e:
        print("Error writing analytics data to Cassandra:", e)


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

    df_grouped = df_parsed.groupBy("trade_type").agg(
        avg("price").alias("avg_price"))

    query_cassandra_grouped = df_grouped.writeStream \
        .outputMode("complete") \
        .foreachBatch(writeToCassandraGrouped) \
        .start()

    query_cassandra = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(writeToCassandra) \
        .start()

    query_rollup = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(rollupAndWriteToCassandra) \
        .start()

    query_rank = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(rankAndWriteToCassandra) \
        .start()

    query_analytics = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(analyticsAndWriteToCassandra) \
        .start()

    query_pivot = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(pivotAndWriteToCassandra) \
        .start()

    query_console.awaitTermination()
    query_cassandra.awaitTermination()
    query_rollup.awaitTermination()
    query_rank.awaitTermination()
    query_analytics.awaitTermination()
    query_cassandra_grouped.awaitTermination()
    query_pivot.awaitTermination()


if __name__ == "__main__":
    main()
