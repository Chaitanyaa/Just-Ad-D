# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import time
import os
import boto3
import io
import pandas as pd
import calendar
from datetime import datetime
from pytz import timezone
import multiprocessing
import time
import sys
import random
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj,'to_dict'):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)

if __name__ == '__main__':

    # Initialization
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    # Create Producer instance
    p = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
	    'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })
    AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
    adclicks = io.BytesIO(s3.get_object(Bucket='adcomplete', Key='sampleadclicks.csv')['Body'].read())
    adclicks_df = pd.read_csv(adclicks,low_memory=False)
    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)
    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
    
    # Simulation for sample data.
    docs = [84256324,18986917,71725490,504304,51951532,11457105,66721705,369733]
    jsons = ''
    jsonspv = ''
    i=-1
    with open("../../data/page_views_sample.csv") as infile:
        for line in infile:
            i=i+1
            if(i>1):
                row = line.split(",")
                pstimezone = timezone('US/Pacific')
                us_time = datetime.now(pstimezone)
                times1 = us_time.strftime('%Y-%m-%d %H:%M:%S')
                msgpv = {'uuid': row[0], 'document_id': random.choice(docs), 'timestamp': times1, 'platform': row[3], 'geo_location': row[4], 'traffic_source': row[5]}
                jsonspv+= json.dumps(msgpv) + "\n"
                record_value = jsonspv
                record_key = row[2]
                time.sleep(0.01)
                print("Producing record: {}".format(record_key))
                if(i==50):
                    i=0
                    p.produce(topic, key=str(record_key), value=str(record_value), on_delivery=acked)
                    print("Produced: {}".format(record_key))
                jsonspv=''
                i=i+1