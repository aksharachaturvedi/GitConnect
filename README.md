# gitConnect
Analysing connections and repositories (Finding Clusters in GitHub network)

The project is meant to showcase my learning experience of implementing a big data ETL pipeline. I choose to work with GitHub data from Git Archive.

<strong>Introduction</strong><br/>

Networks are all around us and our interation and connections within these networks deeply influences the way we think and act. By analyzing these connections we can identify sources of influences in the network, and find the communities that these connections form. 

<strong>Data</strong> <br/>
Got over a tera-byte of data from Git Archive.

<strong>Ingestion</strong> <br/>
Data was sanitized by usign Python scripts. And stored in HDFS.

<strong>Batch Processing</strong> <br/>
The batch processing is done in 2 steps.

1) In the first step the data is read, sanitised and the relevant rows are extracted.
2) In the second job Graph is created using GraphX, and algorithm like PageRank and Connected Componends were applied.

<strong>PipeLine</strong> <br/>


