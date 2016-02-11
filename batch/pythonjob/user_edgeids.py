import itertools
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# getting master node's IP and public DNS to run Spark job and read from HDFS
namenode_path = "hdfs://52.89.26.208:9000"
file_path = "/data/github/hourly/events/2015-08-31-5.json"
edgePath = "/data/graphPersist/UserRepoGraph/edgeFile/"

SparkContext.setSystemProperty('spark.executor.memory', '4g')

sc = SparkContext("spark://ip-172-31-0-75:7077", "eventRepoUser")
sqlContext = SQLContext(sc)

df_events = sqlContext.read.json(namenode_path + file_path)

# Spark job to get just the login and IDs from all the fields in the data
event_map = df_events.filter("type = 'PushEvent'")
repo_rdd = event_map.map(lambda x: (x.repo.name, list([x.actor.id]))).groupByKey()

# get repo and extract the list of users for that repo
#usr_repo = repo_rdd.map(lambda x: filter(None,itertools.combinations(list(set([repo for sublist in x[1] for repo in sublist])),2)))

usr_repo = repo_rdd.map(lambda x: filter(None,itertools.combinations(map(str,list(set([repo for sublist in x[1] for repo in sublist]))),2)))

usr_repo = repo_rdd.map(lambda x: filter(None,itertools.combinations(filter(None,map(str,list(set([repo for sublist in x[1] for repo in sublist]))),2))))

usr_repo.saveAsTextFile(namenode_path + edgePath)

# #insert in DB
# from cassandra.cluster import Cluster
# cascluster = Cluster(['52.89.41.128', '52.89.60.184', '52.89.60.239', '52.89.26.208'])
# casSession = cascluster.connect('community')
#
# for item in repo_user_map:
#     casSession.execute('INSERT INTO community.eventrepousermap(repository, user_commit) VALUES (%s, %s)',(item, {repo_user_map[item][0]:repo_user_map[item][1]}))
#
# casSession.shutdown()
# cascluster.shutdown()