# pyspark imports
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import StringType, TimestampType, IntegerType, BinaryType

# schema registry imports
from confluent_kafka.schema_registry import SchemaRegistryClient

kafka_url = "kafka-broker:29092"
schema_registry_url = "http://schema-registry:8083"
kafka_producer_topic = "wikimedia"
kafka_analyzed_topic = "wikimedia.processed"
schema_registry_subject = f"{kafka_producer_topic}-value"
schema_registry_analyzed_data_subject = f"{kafka_analyzed_topic}-value"

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("wikimedia_consumer").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# UDF function
binary_to_string_udf = func.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())
# x->value, y->len
int_to_binary_udf = func.udf(lambda value, byte_size: (value).to_bytes(byte_size, byteorder='big'), BinaryType())

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version

def spark_consumer():
    wikimedia_df = spark \
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_url)\
    .option("subscribe", kafka_producer_topic)\
    .option("startingOffsets", "earliest")\
    .load()

    wikimedia_df.printSchema()
    # OUTPUT #
    # root
    # |-- key: binary (nullable = true)
    # |-- value: binary (nullable = true)
    # |-- topic: string (nullable = true)
    # |-- partition: integer (nullable = true)
    # |-- offset: long (nullable = true)
    # |-- timestamp: timestamp (nullable = true)
    # |-- timestampType: integer (nullable = true)


    # remove first 5 bytes from value
    wikimedia_df = wikimedia_df.withColumn('fixedValue', func.expr("substring(value, 6, length(value)-5)"))

    #  get schema id from value
    wikimedia_df = wikimedia_df.withColumn('valueSchemaId', binary_to_string_udf(func.expr("substring(value, 2, 4)")))

    # get schema using subject name
    _, latest_version_wikimedia = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)

    # deserialize data 
    fromAvroOptions = {"mode":"PERMISSIVE"}
    decoded_output = wikimedia_df.select(
        from_avro(
            func.col("fixedValue"), latest_version_wikimedia.schema.schema_str, fromAvroOptions
        )
        .alias("wikimedia")
    )
    wikimedia_value_df = decoded_output.select("wikimedia.*")
    # wikimedia_value_df.printSchema()
    # OUTPUT #
    # root
    # |-- bot: boolean (nullable = true)
    # |-- comment: string (nullable = true)
    # |-- id: integer (nullable = true)
    # |-- length: struct (nullable = true)
    # |    |-- new: integer (nullable = true)
    # |    |-- old: integer (nullable = true)
    # |-- meta: struct (nullable = true)
    # |    |-- domain: string (nullable = true)
    # |    |-- dt: string (nullable = true)
    # |    |-- id: string (nullable = true)
    # |    |-- offset: long (nullable = true)
    # |    |-- partition: integer (nullable = true)
    # |    |-- request_id: string (nullable = true)
    # |    |-- stream: string (nullable = true)
    # |    |-- topic: string (nullable = true)
    # |    |-- uri: string (nullable = true)
    # |-- minor: boolean (nullable = true)
    # |-- namespace: integer (nullable = true)
    # |-- parsedcomment: string (nullable = true)
    # |-- patrolled: boolean (nullable = true)
    # |-- revision: struct (nullable = true)
    # |    |-- new: integer (nullable = true)
    # |    |-- old: integer (nullable = true)
    # |-- schema: string (nullable = true)
    # |-- server_name: string (nullable = true)
    # |-- server_script_path: string (nullable = true)
    # |-- server_url: string (nullable = true)
    # |-- timestamp: integer (nullable = true)
    # |-- title: string (nullable = true)
    # |-- type: string (nullable = true)
    # |-- user: string (nullable = true)
    # |-- wiki: string (nullable = true)

    # counting numbers of bots & humans requesting for edit
    ## filter by type
    ### timestamp column is of type int, convert it to timestamp as withWatermark accepts timestamp type column. you can create date column by using func.from_unixtime but the result is datetime with string type which is not accepted by withWatermark
    wikimedia_value_df = wikimedia_value_df \
        .filter(func.col("type") == "edit") \
        .select("bot", func.col("timestamp").cast(TimestampType())) \

    ## groupby bot & find count
    wikimedia_value_df = wikimedia_value_df \
        .withWatermark("timestamp", "60 seconds") \
        .groupBy(
            # window params -> timestamp, window interval, sliding interval
            func.window(
                func.col("timestamp"),
                "60 seconds",
                "30 seconds"
            ),
            func.col("bot")
        ) \
        .agg(func.count("bot").alias('counts')) \
        # .orderBy(func.col("window").asc())

    ## add request_by field
    wikimedia_value_df = wikimedia_value_df.withColumn(
        "requested_by", 
        func.when(wikimedia_value_df.bot == True, "Bot")\
        .when(wikimedia_value_df.bot == False, "Human")\
        .otherwise("")
    ) \
    .select(
        func.struct(
            func.col("window.start").cast(StringType()).alias("start"), 
            func.col("window.end").cast(StringType()).alias("end"), 
        ).alias("window"),
        "requested_by", 
        "counts"
    )
    wikimedia_value_df.printSchema()
    # OUTPUT #
    # root
    # |-- window: struct (nullable = true)
    # |    |-- start: string (nullable = true)
    # |    |-- end: string (nullable = true)
    # |-- requested_by: string (nullable = false)
    # |-- counts: long (nullable = false)

    # write to sink(console)
    ## trigger is used for batch interval
    # wikimedia_value_df \
    # .writeStream \
    # .format("console") \
    # .trigger(processingTime='1 second') \
    # .outputMode("append") \
    # .option("truncate", "false") \
    # .start() \
    # .awaitTermination()
    # OUTPUT(complete) #
    # +------------------------------------------+------------+------+
    # |window                                    |requested_by|counts|
    # +------------------------------------------+------------+------+
    # |{2023-01-19 13:47:00, 2023-01-19 13:48:00}|Bot         |218   |
    # |{2023-01-19 13:47:00, 2023-01-19 13:48:00}|Human       |786   |
    # +------------------------------------------+------------+------+

    # get schema for analyzed data
    _, latest_version_analyzed_data = get_schema_from_schema_registry(schema_registry_url, schema_registry_analyzed_data_subject)

    # convert dataframe to binary data
    wikimedia_value_df = wikimedia_value_df \
    .select(to_avro(func.struct(
        func.col("window"),
        func.col("requested_by"),
        func.col("counts")
    ), latest_version_analyzed_data.schema.schema_str).alias("value"))

    # add magicbyte & schemaId to binary data
    magicByteBinary = int_to_binary_udf(func.lit(0), func.lit(1))
    schemaIdBinary = int_to_binary_udf(func.lit(latest_version_analyzed_data.schema_id), func.lit(4))
    wikimedia_value_df = wikimedia_value_df.withColumn("value", func.concat(magicByteBinary, schemaIdBinary, func.col("value")))

    # Write to Kafka Sink
    wikimedia_value_df \
    .writeStream \
    .format("kafka") \
    .trigger(processingTime='1 second') \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", kafka_url) \
    .option("topic", kafka_analyzed_topic) \
    .option("checkpointLocation", "hdfs://namenode:9000/user/admin/learning/streaming/checkpoint") \
    .start() \
    .awaitTermination()

spark_consumer()