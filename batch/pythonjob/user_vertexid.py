#!/usr/bin/env python
from collections import Counter
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']  
namenode_path = os.environ['name_node']

file_path = "/data/github/hourly/events/*.json"
vertexPath = "/data/github/vertexFile/"

SparkContext.setSystemProperty('spark.executor.memory', '2g')

sc = SparkContext("spark://"+master_ip+":7077", "vertex")
sqlContext = SQLContext(sc)
df_events = sqlContext.read.json(namenode_path + file_path)

# Spark job to get just the login and IDs from all the fields in the data
event_map = df_events.filter("type = 'PushEvent'")
user_rdd = event_map.map(lambda x: (x.actor.id, [x.repo.id])).groupByKey()

# get repo and extract the list of users for that repo
usr_repo = user_rdd.map(lambda x: ''.join([x[0],   ', '.join(str(s) for s in set([repo for sublist in x[1] for repo in sublist]))]))

#save data to file
usr_repo.saveAsTextFile(namenode_path + vertexPath)



