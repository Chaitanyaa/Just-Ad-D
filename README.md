# Just (Ad)D : Adding Advertisements on the fly
![alt text](https://github.com/Chaitanyaa/Just-Ad-D/blob/master/idea.JPG)


## Table of Contents
1. [Introduction](README.md#Introduction)
2. [Problem Statement](README.md#ProblemStatement)
3. [Data Pipeline](README.md#DataPipeline)
4. [Workflow](README.md#Workflow)
5. [Data Source](README.md#DataSource)
6. [Repo Structure](README.md#RepoStructure)
7. [Slides](https://docs.google.com/presentation/d/1BV4d5XMUscUyXGjHr3mTFCDx6o5Y36EkbkwR76s3cLw/edit#slide=id.p)

## Introduction
Just (Ad)D is a distributed streaming data pipeline for analyzing an ad performance in real-time to potentially add them onto the websites that have high user traffic.

## ProblemStatement
Advertisements is all about gaining user attention, which includes targeting the right consumers, at the right times, through the right channels. Therefore, optimizing user attention to improve the conversion rates is crucial for the advertisers.

## DataPipeline
![alt text](https://github.com/Chaitanyaa/Just-Ad-D/blob/master/pipeline.jpg)

For ease of deployment and to avoid gruesome network configurations with individual EC2 instances, this pipeline design utilizes managed services like Confluent cloud for Kafka cluster and AWS EMR for Spark cluster.

## Workflow
* Sample dataset is stored in an EC2 instance. 
* In EC2, simulated messages are produced to page views and click event topics in Confluent Cloud which provides Kafka cluster as a service. 
* The messages are consumed by Spark to process the stream of messages for counting the number of clicks and views for each advertisement within the event-time windows. 
* Windowing and watermark usages are demonstrated to handle late and out of order data. 
* Each stream processed data is stored in MySQL database with timestamp.
* The continuous update on to the dB is queried and visualized on to live dashboard built using Plotly Dash.

## DataSource
A subset of [Outbrain Click Prediction](https://www.kaggle.com/c/outbrain-click-prediction/data) Kaggle dataset

## RepoStructure
```
Just-Ad-D/
├── dash_frontend
│   └── app.py
├── kafka_clicks.sh
├── kafka_ingestion
│   ├── ccloud_lib.py
│   ├── producer.py
│   ├── pvproducer.py
├── kafka_pv.sh
├── LICENSE
├── README.md
├── sparkjob_clicks.sh
├── sparkjob_pv.sh
├── spark_processing
│   ├── kafspar2.py
│   └── pvkafspar2.py
```
