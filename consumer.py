import os
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def process(time, rdd):
    print("========= %s =========" % str(time))
    if not rdd.isEmpty():
        spark = getSparkSessionInstance(rdd.context.getConf())
        pathsArray = rdd.collect()

        for path in pathsArray:
            df = spark \
                .read \
                .format("csv") \
                .options(delimiter='\t') \
                .option("inferSchema",True) \
                .load(path)

            print('Received:', path)

            collection = path.split('.')[-2]

            df.write \
                .format("mongo") \
                .mode("append") \
                .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/gdelt." + collection) \
                .option("database", "gdelt") \
                .option("collection", collection) \
                .save()

            print('Saved to collection:', collection)

def main():
    KAFKA_HOST = 'localhost:9092'

    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 5)

    kvs = KafkaUtils.createDirectStream(ssc, ["t1"], {"metadata.broker.list": KAFKA_HOST})

    csv_paths = kvs.map(lambda x: x[1])
    
    csv_paths.foreachRDD(process)

    ssc.checkpoint("./checkpoint")

    ssc.start()
    ssc.awaitTermination()

    

if __name__ == "__main__":
    main()



