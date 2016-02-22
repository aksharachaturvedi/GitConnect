import itertools
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']  
namenode_path = os.environ['name_node']

file_path = "/data/github/hourly/events/*.json"
edgePath = "/data/graphPersist/UserRepoGraph/edgeFile/"
SparkContext.setSystemProperty('spark.executor.memory', '4g')

sc = SparkContext("spark://"+master_ip +":7077", "EdgeProcesing")
sqlContext = SQLContext(sc)

df_events = sqlContext.read.json(namenode_path + file_path)

# Spark job to get just the login and IDs from all the fields in the data
event_map = df_events.filter("type = 'PushEvent'")
repo_rdd = event_map.map(lambda x: (x.repo.name, list([x.actor.id]))).groupByKey()

# get repo and extract the list of users for that repo
usr_repo = repo_rdd.map(lambda x: filter(None,itertools.combinations(filter(None,map(str,list(set([repo for sublist in x[1] for repo in sublist]))),2))))
usr_repo.saveAsTextFile(namenode_path + edgePath)
