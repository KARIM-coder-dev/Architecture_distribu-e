from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
from pymongo import MongoClient

sc = SparkContext(appName="ML-Projet")
ssc = StreamingContext(sc, 1)

consumer = KafkaConsumer('stream_telecom', bootstrap_servers=['kafka:9092'])

def stocker_donnees(rdd):
    if not rdd.isEmpty():
        client = MongoClient("mongodb://localhost:27017")
        db = client["Projet_ML"]
        collection = db["data_telecom"]
        collection.insert_many(rdd.collect())

lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.foreachRDD(stocker_donnees)

ssc.start()
ssc.awaitTermination()