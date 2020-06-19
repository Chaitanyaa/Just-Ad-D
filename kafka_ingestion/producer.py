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
    pv = pd.read_csv('../../data/page_views_sample.csv')
    pv = pv.sort_values('timestamp')
    spv = pv.head(100)

    demo = [140949,183852,292368]
    click = [0,1]

    jsons = ''
    jsonspv = ''
    i=0
    while(1):
        for v in spv.itertuples():
            j = 0
            pstimezone = timezone('US/Pacific')
            us_time = datetime.now(pstimezone)
            times1 = us_time.strftime('%Y-%m-%d %H:%M:%S')
            msgpv = {'uuid': v[1], 'document_id': v[2], 'timestamp': times1, 'platform': v[4], 'geo_location': v[5]}
            jsonspv += json.dumps(msgpv) + "\n"
            record_value = jsonspv
            record_key = v[2]
            time.sleep(1)
            print("Producing record: {}".format(record_key))
            # p.produce(topic, key=str(record_key), value=str(record_value), on_delivery=acked) 
            jsonspv=''
            if(len(adclicks_df[adclicks_df['document_id']==v[2]])>1):
                a = adclicks_df[adclicks_df['document_id']==v[2]]
                for values in a.itertuples():
                    # time.sleep(0.01)
                    if(j==20):break
                    j=j+1
                    pstimezone = timezone('US/Pacific')
                    us_time = datetime.now(pstimezone)
                    times2 = us_time.strftime('%Y-%m-%d %H:%M:%S')
                    msg = {'display_id': values[1], 'ad_id': random.choice(demo), 'clicked': random.choice(click), 'uuid': values[4], 'timestamp': times2, 'document_id': values[6], 'platform': values[7], 'geo_location': values[8]}
                    jsons += json.dumps(msg) + "\n"
                    record_value = jsons
                    record_key = values[6]
                    print("Producing click record: {}".format(record_key))
                    # if(i==50):
                    #     i=0
                    p.produce(topic, key=str(record_key), value=str(record_value), on_delivery=acked)
                        # print("Produced: {}".format(record_key))
                        # break
                    jsons=''
                    # i=i+1
            p.flush()