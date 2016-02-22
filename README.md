# gitConnect
Analysing github connections and repositories (Finding Clusters)

The project is meant to showcase my learning experience of implementing a big data ETL pipeline. I choose to work with GitHub data from Git-Archive.

##Introduction

Networks are all around us and our connections within these networks deeply influences the way we think and act. By analyzing these connections we can identify sources of influences in the network, and find the communities that these connections form. I am finding clusters in github network and finding the top contributors in the network based on the push event on the repositories.

##Data
* Tera-byte of data from Git Archive.
* Dataset includes Users, Followers, Repositories and Events.
* Last 6 month’s events were taken into consideration.
* ~16 million push events happened to repositories.
* ~112 million total events processed

##AWS Clusters
5 m4.xlarge for Spark Cluster
4 m4.large for HDFS, Cassndra

##Ingestion
Data was sanitized by using Python scripts and stored in HDFS.

##Batch Processing
I used Spark and GraphX for batch processing. It is done in 2 steps.

* In the first step, data is read, sanitised and the relevant rows are extracted.
* In the second step, Graph is created and processed.

Github has over 20 events only 'PushEvents' are considered as the contribution. 

In graph users represents vertices and edges between them are the collaboration on the repositories.
The data from GitHub archive has inconsistent schema it is filtered in the batch process to generate:
* vertex file: which has users and the list of all the repositories that they have contributed to. 
* edge file: which has user to user mapping.

Graph Creation:
Filtered Push events from the entire set of events with the mapping of user to repository 
                    User --> Repository
Constructed graph from the User to Repository to :
                    Use --> User

Clusters are calculated using connected components algorithm.

##Serving Layer
Cassndra is used to save results and serve the front end.

![Pipeline](https://github.com/zenachaturvedi/GitConnect/blob/master/misc/DB%20schema.png)

##PipeLine
![Pipeline](https://github.com/zenachaturvedi/GitConnect/blob/master/misc/pipeline.png)

##Future Extensions
* Store in Graph Database for more analysis
* Recommend repositories to users based on his cluster.

