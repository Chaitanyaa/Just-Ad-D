import os
import time
import calendar
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark'
from pandas.io.json import json_normalize
from ast import literal_eval
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import pyspark.sql.types as types
from pyspark.sql.functions import *
from pyspark.sql.functions import window

kafka_username = os.environ['CONFLUENT_KAFKA_USER']
kafka_password = os.environ['CONFLUENT_KAFKA_PASSWORD']
mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.sql.parquet.binaryAsString","true") \
    .config("spark.hadoop.fs.s3a.endpoint","https://s3.us-east-2.amazonaws.com") \
    .config('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4') \
    .config('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4') \
    .config("spark.executor.userClassPath", "true") \
    .config("spark.driver.userClassPath", "true") \
    .config("spark.jars","/home/ec2-user/outbrain/mysql-connector-java-5.1.49/mysql-connector-java-5.1.49.jar,/home/ec2-user/outbrain/mysql-connector-java-5.1.49/mysql-connector-java-5.1.49-bin.jar") \
    .getOrCreate()

user = "\""+kafka_username+"\""
password = "\""+kafka_password+"\""
jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username={user} password={password};".format(user=user,password=password)

dsraw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "pkc-4kgmg.us-west-2.aws.confluent.cloud:9092") \
  .option("subscribe", "hell4") \
  .option("startingOffsets", "earliest") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.username", kafka_username) \
  .option("kafka.sasl.password", kafka_password) \
  .option("kafka.sasl.jaas.config", jaas) \
  .load()

db_properties={}
db_properties['user']=mysql_username
db_properties['password']=mysql_password
db_properties['driver']="com.mysql.jdbc.Driver"

"""
schema = types.StructType([
        types.StructField('display_id', types.IntegerType(),False),
        types.StructField('ad_id', types.IntegerType(),False),
        types.StructField('clicked', types.IntegerType(),False),
        types.StructField('uuid', types.LongType(),False),
        types.StructField('timestamp', types.TimestampType(),False),
        types.StructField('document_id', types.IntegerType(),False),
        types.StructField('platform', types.IntegerType(),False),
        types.StructField('geo_location', types.StringType(),False),     
    ])
"""
# ds = dsraw.selectExpr("CAST(value AS STRING)")

def process_row(df,epochId):

    df = df.selectExpr("CAST(value AS STRING)")

    # Explode the microbatch into rows
    df = df.select(fn.explode(fn.split(df.value, "\n"))
            .alias("value"))
    
    # Explode JSON into columns
    ds = df.select(fn.json_tuple('value', 'uuid', 'document_id','timestamp','platform','geo_location','traffic_source') \
            .alias('uuid', 'document_id','timestamp','platform','geo_location','traffic_source'))

    # Do processing
    
    # Get Top 3 Websites with high user traffic
    ds1 = ds.groupby(fn.col('document_id')) \
            .agg(
                fn.count(fn.col('uuid')),
                fn.max(fn.col('timestamp').cast('string'))) \
            .withColumnRenamed("COUNT(uuid)", "users") \
            .withColumnRenamed("max(CAST(timestamp AS STRING))", "timestamp") \
            .orderBy(fn.col("users"), ascending=False).limit(3)
    
    # Get platform distribution
    ds2 = ds.groupby(fn.col('platform')) \
            .agg(
                fn.count(fn.col('uuid')),
                fn.max(fn.col('timestamp').cast('string'))) \
            .withColumnRenamed("COUNT(uuid)", "users") \
            .withColumnRenamed("max(CAST(timestamp AS STRING))", "timestamp") \
            .orderBy(fn.col("users"), ascending=False)

    # Get traffic distribution
    ds3 = ds.groupby(fn.col('traffic_source')) \
            .agg(
                fn.count(fn.col('uuid')),
                fn.max(fn.col('timestamp').cast('string'))) \
            .withColumnRenamed("COUNT(uuid)", "users") \
            .withColumnRenamed("max(CAST(timestamp AS STRING))", "timestamp") \
            .orderBy(fn.col("users"), ascending=False)
            
    ds.printSchema()
    
    # Write to db
    if(df.rdd.isEmpty()):
        #do nothing
        pass
    else:
        ds1.write.jdbc(url="jdbc:mysql://54.193.71.186:3306/Project", table="page_views", mode="append", properties=db_properties)
        ds2.write.jdbc(url="jdbc:mysql://54.193.71.186:3306/Project", table="page_views_platform", mode="append", properties=db_properties)
        ds3.write.jdbc(url="jdbc:mysql://54.193.71.186:3306/Project", table="page_views_traffic", mode="append", properties=db_properties)
        pass

# For Handling late and out of order data - Watermark, Window, Sliding Window
# dk = \
#   dsraw \
#     .withWatermark("timestamp", "10 minutes") \
#     .groupBy(window("timestamp", "5 minute")) \
#     .count()  

# Write the batch stream to Mysql database.
dsraw.writeStream.format("console").foreachBatch(process_row).start().awaitTermination()