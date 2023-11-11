from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, concat_ws, avg, count, expr,lit,avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC 



# Initialize a Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:

    schema = StructType([
    StructField("adult", StringType()),
    StructField("backdrop_path", StringType()),
    StructField("genre_ids", ArrayType(IntegerType())),
    StructField("id", IntegerType()),
    StructField("original_language", StringType()),
    StructField("original_title", StringType()),
    StructField("overview", StringType()),
    StructField("popularity", StringType()),
    StructField("poster_path", StringType()),
    StructField("release_date", StringType()),
    StructField("title", StringType()),
    StructField("video", StringType()),
    StructField("vote_average", StringType()),
    StructField("vote_count", IntegerType())
])

# Read from Kafka topic
    kafka_source = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()




# Parse the JSON data
    schema_json = schema.json()

    print(schema_json)


    parsed_data = kafka_source.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_json).alias("data")) \
    .select("data.*") 
    transformed_data = parsed_data.withColumn("description", concat_ws(" ", col("title"), col("overview")))
    average_normalized_votes = transformed_data.agg(avg(transformed_data["vote_average"]).alias("normalized_votes"))




    print("parsed_data: ",transformed_data)


# Print the parsed data to the console
    query = transformed_data.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
finally:
    # Stop Spark context when done
    spark.stop()