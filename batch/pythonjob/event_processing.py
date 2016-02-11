from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']  
namenode_path = os.environ['name_node']
file_path = "/data/github/hourly/events/*.json"

sc = SparkContext("spark://" + master_ip + ":7077", "events")
sqlContext = SQLContext(sc)

df_events = sqlContext.read.json(namenode_path + file_path)
# Spark job to get just the login and IDs from all the fields in the data
event_map = df_events.filter("type = 'PushEvent'")

repo_rdd = event_map.map(lambda x: (x.repo.name, list([x.actor.login]))).groupByKey()

# get repo and extract the list of users for that repo
repo_user = repo_rdd.map(lambda x: {"repo":x[0], "user":[repo for sublist in x[1] for repo in sublist]}).collect()

# get the count of push request for a repo by that user
repo_user_map = {}
for item in repo_user:
    repo = item['repo']
    user, commits = Counter(item['user']).items()[0]
    if not repo_user_map.get(repo):
      repo_user_map[repo] = Counter(item['user']).items()[0]
    else:
      map = repo_user_map.get(repo)
      map[user] = commits
      repo_user_map[repo] = map

print("writing in cassandra")

#insert in DB
from cassandra.cluster import Cluster
cascluster = Cluster(['52.89.41.128', '52.89.60.184', '52.89.60.239', '52.89.26.208'])
casSession = cascluster.connect('community')

for item in repo_user_map:
    casSession.execute('INSERT INTO community.eventrepousermap(repository, user_commit) VALUES (%s, %s)',(item, {repo_user_map[item][0]:repo_user_map[item][1]}))

casSession.shutdown()
cascluster.shutdown()