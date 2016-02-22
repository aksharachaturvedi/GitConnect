# gitConnect
Analysing connections and repositories (Finding Clusters in GitHub network)

The project is meant to showcase my learning experience of implementing a big data ETL pipeline. I choose to work with GitHub data from Git Archive.

##Introduction

Networks are all around us and our interation and connections within these networks deeply influences the way we think and act. By analyzing these connections we can identify sources of influences in the network, and find the communities that these connections form. I am finding clusters in github network and finding the top contributors in the network based on the push event on the repositories.

##Data
Tera-byte of data from Git Archive.

##Ingestion
Data was sanitized by usign Python scripts. And stored in HDFS.

##Batch Processing
The batch processing is done in 2 steps.

1) In the first step the data is read, sanitised and the relevant rows are extracted.
2) In the second job Graph is created using GraphX, and algorithm like PageRank and Connected Componends were applied.

##PipeLine
![alt tag](https://github.com/zenachaturvedi/GitConnect/tree/master/misc/pipeline.png)

##Future Extension
Generate communities
Store in Graph Database

