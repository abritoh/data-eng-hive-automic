# Hadoop / Hive Features

## Apache Hadoop

"*The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and storage. Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer, so delivering a highly-available service on top of a cluster of computers, each of which may be prone to failures.*"

Site: https://hadoop.apache.org

![Hadoop](./images/hadoop-logo.jpg "Hadoop")


## Apache Hive

"*Apache Hive is a distributed, fault-tolerant data warehouse system that enables analytics at a massive scale. Hive Metastore(HMS) provides a central repository of metadata that can easily be analyzed to make informed, data driven decisions, and therefore it is a critical component of many data lake architectures. Hive is built on top of Apache Hadoop and supports storage on S3, adls, gs etc though hdfs. Hive allows users to read, write, and manage petabytes of data using SQL.*"

Site: https://hive.apache.org

![Hadoop](./images/hive-logo.jpg "Hadoop")


____

## Hadoop and Hive Table of Features

```
+----------------------+------------------------------------------+---------------------------------------------+
| Feature              |              Hadoop                      |                  Hive                       |
+----------------------+------------------------------------------+---------------------------------------------+
| Purpose              | Distributed storage and processing of   | Data warehousing system built on top of      |
|                      | large datasets using MapReduce.         | Hadoop for querying and managing data        |
+----------------------+------------------------------------------+---------------------------------------------+
| Data Storage         | HDFS (Hadoop Distributed File System)   | Stores data in HDFS, but provides a SQL-     |
|                      | for fault-tolerant distributed storage. | like interface for querying it.              |
+----------------------+------------------------------------------+---------------------------------------------+
| Processing           | Uses MapReduce, YARN, or Spark for      | Runs queries using a SQL-like language,      |
|                      | distributed processing of data.         | compiling into MapReduce jobs.               |
+----------------------+------------------------------------------+---------------------------------------------+
| Query Language       | No SQL query language. Primarily uses   | HiveQL (SQL-like) for querying data.         |
|                      | Java or other languages with MapReduce. |                                              |
+----------------------+------------------------------------------+---------------------------------------------+
| Data Structure       | Works with raw data in HDFS.            | Works with structured and semi-structured    |
|                      | No schema enforcement.                  | data; supports tables, partitions, and       |
|                      |                                         | columns with schema.                         |
+----------------------+------------------------------------------+---------------------------------------------+
| Ease of Use          | Requires coding and knowledge of        | Easier to use than raw Hadoop;               |
|                      | MapReduce or other frameworks.          | allows users to run SQL queries without      |
|                      |                                         | writing MapReduce code.                      |
+----------------------+------------------------------------------+---------------------------------------------+
| Performance          | Limited by MapReduce (batch-oriented)   | Performs faster for querying compared to     |
|                      | but can be improved with Spark or       | raw MapReduce, especially on OLAP queries.   |
|                      | other frameworks.                       |                                              |
+----------------------+------------------------------------------+---------------------------------------------+
| Fault Tolerance      | High fault tolerance using replication  | Inherits Hadoop's fault tolerance but        |
|                      | of data across nodes in the cluster.    | optimized for easier querying of data.       |
+----------------------+------------------------------------------+---------------------------------------------+
| Real-Time Processing | Not designed for real-time processing.  | Not intended for real-time queries; it is    |
|                      | Can be combined with Spark for stream-  | batch-oriented, but can handle near real-    |
|                      | ing data processing.                    | time queries with optimization.              |
+----------------------+------------------------------------------+---------------------------------------------+
| Optimization         | Optimized for large batch processing.   | Optimized for querying large datasets using  |
|                      | May require manual tuning.              | indexing, partitioning, and cost-based       |
|                      |                                         | optimizations.                               |
+----------------------+------------------------------------------+---------------------------------------------+
| Integration          | Integrates with other big data tools,   | Integrates with Hadoop ecosystem tools like  |
|                      | like Apache Spark, HBase, and Kafka.    | HBase, Pig, and Mahout, and external tools.  |
+----------------------+------------------------------------------+---------------------------------------------+
| Supported Formats    | HDFS supports various formats like      | Supports multiple file formats like          |
|                      | text, Parquet, Avro, ORC, etc.          | Text, Parquet, ORC, Avro, and more.          |
+----------------------+------------------------------------------+---------------------------------------------+
| Community            | Large open-source community with        | Hive is widely used in the Hadoop ecosystem  |
|                      | broad ecosystem support.                | and has a strong open-source community.      |
+----------------------+------------------------------------------+---------------------------------------------+

```

