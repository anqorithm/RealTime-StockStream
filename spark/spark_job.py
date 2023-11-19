from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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

    query_console.awaitTermination()
    query_cassandra.awaitTermination()


if __name__ == "__main__":
    main()
