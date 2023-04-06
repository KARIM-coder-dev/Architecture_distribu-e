from pyspark.sql import SparkSession
from pyspark.sql.types import  StructField, StructType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col, encode as F


spark = SparkSession.builder.appName('ML-Projet').master('spark://8aa33686ed76:7077').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'hi'


SCHEMA = StructType([
    StructField("year", IntegerType()),
    StructField("month", IntegerType()), 
    StructField("user_account_id", IntegerType()), 
    StructField("user_lifetime", IntegerType())   
    # StructField("user_intake", IntegerType()),    
    # StructField("user_no_outgoing_activity_in_days", IntegerType()),    
    # StructField("user_account_balance_last", FloatType()),    
    # StructField("user_spendings", FloatType()),
    # StructField("user_has_outgoing_calls", IntegerType()),    
    # StructField("user_has_outgoing_sms", IntegerType()),    
    # StructField("user_use_gprs", IntegerType()),    
    # StructField("user_does_reload", IntegerType()),    
    # StructField("reloads_inactive_days", IntegerType()),    
    # StructField("reloads_count", IntegerType()),    
    # StructField("reloads_sum", FloatType()),    
    # StructField("calls_outgoing_count", IntegerType()),    
    # StructField("calls_outgoing_spendings", FloatType()),    
    # StructField("calls_outgoing_duration", FloatType()),    
    # StructField("calls_outgoing_spendings_max", FloatType()),    
    # StructField("calls_outgoing_duration_max", FloatType()),    
    # StructField("calls_outgoing_inactive_days", IntegerType()),    
    # StructField("calls_outgoing_to_onnet_count", IntegerType()),    
    # StructField("calls_outgoing_to_onnet_spendings", FloatType()),    
    # StructField("calls_outgoing_to_onnet_duration", FloatType()),    
    # StructField("calls_outgoing_to_onnet_inactive_days", IntegerType()),    
    # StructField("calls_outgoing_to_offnet_count", IntegerType()),    
    # StructField("calls_outgoing_to_offnet_spendings", FloatType()),    
    # StructField("calls_outgoing_to_offnet_duration", FloatType()),    
    # StructField("calls_outgoing_to_offnet_inactive_days", IntegerType()),    
    # StructField("calls_outgoing_to_abroad_count", IntegerType()),    
    # StructField("calls_outgoing_to_abroad_spendings", FloatType()),    
    # StructField("calls_outgoing_to_abroad_duration", FloatType()),    
    # StructField("calls_outgoing_to_abroad_inactive_days", IntegerType()),    
    # StructField("sms_outgoing_count", IntegerType()),    
    # StructField("sms_outgoing_spendings", FloatType()),    
    # StructField("sms_outgoing_spendings_max", FloatType()),    
    # StructField("sms_outgoing_inactive_days", IntegerType()),    
    # StructField("sms_outgoing_to_onnet_count", IntegerType()),    
    # StructField("sms_outgoing_to_onnet_spendings", FloatType()),    
    # StructField("sms_outgoing_to_onnet_inactive_days", IntegerType()),    
    # StructField("sms_outgoing_to_offnet_count", IntegerType()),    
    # StructField("sms_outgoing_to_offnet_spendings", FloatType()),      
    # StructField("sms_outgoing_to_offnet_inactive_days", IntegerType()),    
    # StructField("sms_outgoing_to_abroad_count", IntegerType()),    
    # StructField("sms_outgoing_to_abroad_spendings", FloatType()),    
    # StructField("sms_outgoing_to_abroad_inactive_days", IntegerType()),    
    # StructField("sms_incoming_count", IntegerType()),    
    # StructField("sms_incoming_spendings", FloatType()),    
    # StructField("sms_incoming_from_abroad_count", IntegerType()),    
    # StructField("sms_incoming_from_abroad_spendings", FloatType()),
    # StructField("gprs_session_count", IntegerType()),
    # StructField("gprs_usage", FloatType()),
    # StructField("gprs_spendings", FloatType()),
    # StructField("gprs_inactive_days", IntegerType()),
    # StructField("last_100_reloads_count", IntegerType()),    
    # StructField("last_100_reloads_sum", FloatType()),
    # StructField("last_100_calls_outgoing_duration", FloatType()),
    # StructField("last_100_calls_outgoing_to_onnet_duration", FloatType()),
    # StructField("last_100_calls_outgoing_to_offnet_duration", FloatType()),
    # StructField("last_100_calls_outgoing_to_abroad_duration", FloatType()),
    # StructField("last_100_sms_outgoing_count", IntegerType()),
    # StructField("last_100_sms_outgoing_to_onnet_count", IntegerType()),
    # StructField("last_100_sms_outgoing_to_offnet_count", IntegerType()),
    # StructField("last_100_sms_outgoing_to_abroad_count", IntegerType()),
    # StructField("last_100_gprs_usage", FloatType()),
    # StructField("churn", IntegerType()),
                
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load() \
    .select(from_json(col("value").cast("string"), SCHEMA).alias("json")) \
    .select("json.*")


df.writeStream \
  .format("console") \
  .outputMode("append")\
  .option("truncate", "false") \
  .start() \
  .awaitTermination()






