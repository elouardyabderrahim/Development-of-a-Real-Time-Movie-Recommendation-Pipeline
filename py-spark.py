from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC 


# Initialize a Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Define the Kafka source for Structured Streaming
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Extract the value from the Kafka message and convert it to a string
value_df =  kafka_df.selectExpr("CAST(value as STRING)", "timestamp")
value_df.printSchema()

# Define a query to output the Kafka data to the console
query = value_df.writeStream.outputMode("append").format("console").option("format", "json").start()
# Start the streaming query
query.awaitTermination()