import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, concat, col, avg, to_date, lit
from pyspark.sql.types import StructType, StringType, ArrayType, FloatType, IntegerType, DateType,BooleanType

from config import KAFKA_TOPIC,KAFKA_BOOTSTRAP_SERVERS
spark = SparkSession.builder \
    .appName("application-moviesdb") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

es_nodes = 'localhost:9200' 
es_port = '9200'  
es_resource = 'movies_popular_index'  

schema = StructType() \
    .add("adult", BooleanType()) \
    .add("backdrop_path", StringType()) \
    .add("genre_ids", ArrayType(IntegerType())) \
    .add("id", IntegerType()) \
    .add("original_language", StringType()) \
    .add("original_title", StringType()) \
    .add("popularity", FloatType()) \
    .add("poster_path", StringType()) \
    .add("release_date", DateType()) \
    .add("title", StringType()) \
    .add("video", BooleanType()) \
    .add("vote_average", FloatType()) \
    .add("vote_count", IntegerType())


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Data enrichment and transformation
transformed_df = parsed_df \
    .withColumn("description", concat(col("title"), lit(":"), col("overview"))) \

query = transformed_df \
    .writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/your_checkpoint_dir") \
    .option("es.nodes", es_nodes) \
    .option("es.port", es_port) \
    .option("es.resource", es_resource) \
    .start()

query.awaitTermination()



# import findspark
# findspark.init()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, concat_ws, avg
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC 

# # Initialize a Spark session
# spark = SparkSession.builder.appName("elasticSparkStreaming")\
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# try:
#     # Your existing schema definition
#     schema = StructType([
#         StructField("adult", StringType()),
#         StructField("backdrop_path", StringType()),
#         StructField("genre_ids", ArrayType(IntegerType())),
#         StructField("id", IntegerType()),
#         StructField("original_language", StringType()),
#         StructField("original_title", StringType()),
#         StructField("overview", StringType()),
#         StructField("popularity", StringType()),
#         StructField("poster_path", StringType()),
#         StructField("release_date", StringType()),
#         StructField("title", StringType()),
#         StructField("video", StringType()),
#         StructField("vote_average", StringType()),
#         StructField("vote_count", IntegerType())
#     ])

#     # Read from Kafka topic
#     kafka_source = spark.readStream.format("kafka") \
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#         .option("subscribe", KAFKA_TOPIC) \
#         .option("startingOffsets", "latest") \
#         .load()

#     # Parse the JSON data
#     schema_json = schema.json()

#     parsed_data = kafka_source.selectExpr("CAST(value AS STRING) as json") \
#         .select(from_json(col("json"), schema_json).alias("data")) \
#         .select("data.*").withColumn("description", concat_ws(" ", col("title"), col("overview")))

#     transformed_data = parsed_data.withColumn("description", concat_ws(" ", col("title"), col("overview")))
#     average_normalized_votes = transformed_data.agg(avg(transformed_data["vote_average"]).alias("normalized_votes"))

#     # Print the parsed data to the console
#     query = parsed_data.writeStream.outputMode("append").format("console").start()



#     query = parsed_data.writeStream \
#     .format("org.elasticsearch.spark.sql") \
#     .outputMode("append") \
#     .option("es.resource", "movies_index") \
#     .option("es.nodes", "localhost") \
#     .option("es.port", "9200") \
#     .option("es.nodes.wan.only", "false")\
#     .option("es.index.auto.create", "true")\
#     .option("checkpointLocation", "./checkpointLocation/tmp/") \
#     .start()


#     query.awaitTermination()

# finally:
#     # Stop Spark context when done
#     spark.stop()
   
