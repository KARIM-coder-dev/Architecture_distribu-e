from pyspark.sql import SparkSession
from pyspark.sql.types import  StructField, StructType, StringType
from pyspark.sql.functions import from_json, col


spark = SparkSession.builder.appName('ML-Projet').master('spark://39ca8d87675a:7077').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'churn_telecom'

SCHEMA = StructType([
    StructField("Call Id", StringType()), 
    StructField("Agent", StringType()), 
    StructField('Date', StringType()),
    StructField("Topic", StringType()), 
    StructField("Answered (Y/N)", StringType()), 
    StructField("Resolved", StringType()), 
    StructField("Speed of answer in seconds", StringType()), 
    StructField("AvgTalkDuration", StringType()), 
    StructField("Satisfaction rating", StringType()), 
])

df_telecom = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest")\
    .load() \
    .select(from_json(col("value").cast("string"), SCHEMA).alias("json")) \
    .select("json.*") 

df_telecom.writeStream \
          .format("console") \
          .outputMode("update")\
          .option("truncate", "false") \
          .start() \
          .awaitTermination()