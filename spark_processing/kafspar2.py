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
  .option("subscribe", "hell3") \
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
        types.StructField('timestamp', types.IntegerType(),False),
        types.StructField('document_id', types.IntegerType(),False),
        types.StructField('platform', types.IntegerType(),False),
        types.StructField('geo_location', types.StringType(),False),     
    ])
"""
ds = dsraw.selectExpr("CAST(value AS STRING)")


def process_row(df,epochId):

    # Explode the microbatch into rows
    df = df.select(fn.explode(fn.split(df.value, "\n"))
            .alias("value"))
    
    # Explode JSON into columns
    ds = df.select(fn.json_tuple('value', 'display_id', 'ad_id','clicked','uuid','timestamp','document_id','platform','geo_location') \
            .alias('display_id', 'ad_id','clicked','uuid','timestamp','document_id','platform','geo_location'))

    # Do processing
    ds = ds.groupby(fn.col('ad_id')) \
            .agg(
                fn.max(fn.col('timestamp').cast('string')),
                fn.sum(fn.col('clicked')), 
                fn.count(fn.col('clicked'))) \
            .withColumnRenamed("SUM(clicked)", "clicks") \
            .withColumnRenamed("COUNT(clicked)", "views") \
            .withColumnRenamed("max(CAST(timestamp AS STRING))", "timestamp")
            
    ds.printSchema()
    
    # Write to db
    if(df.rdd.isEmpty()):
        #do nothing
        k=1
    else:
        ds.write.jdbc(url="jdbc:mysql://54.193.71.186:3306/Project", table="mytopic", mode="append", properties=db_properties)
    pass
    
# Write the batch stream to Mysql database.
ds.writeStream.format("console").foreachBatch(process_row).start().awaitTermination()