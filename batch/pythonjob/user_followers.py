#!/usr/bin/env python

# importing SparkContext and SQLContext from pyspark for batch processing
#pyspark --packages com.databricks:spark-csv_2.10:1.2.0
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import com.databricks.spark.csv._

master_ip = os.environ['master_ip']  
namenode_path = os.environ['name_node']
file_path = "/data/github/txt/fol/followers_*.csv"

# setting SparkContext and SQLContext
sc = SparkContext("spark://"+master_ip+":7077", "followers")
sqlContext = SQLContext(sc)

df_followers = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(namenode_path + file_path)
df_map = df_followers.map(lambda x: (x.fLogin, x.uLogin)).collect()

from cassandra.cluster import Cluster
cascluster = Cluster(['52.89.41.128', '52.89.60.184', '52.89.60.239', '52.89.26.208'])
casSession = cascluster.connect('community')

for userItem in df_map:
        casSession.execute('INSERT INTO community.followers(login, following) VALUES (%s, %s)', (userItem[0], [userItem[1]]))

casSession.shutdown()
cascluster.shutdown()
