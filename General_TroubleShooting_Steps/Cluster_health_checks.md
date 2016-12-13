#Commands to check cluster health

*Check whether ResourceManager & NodeManager are running or not (Part of HDFS and YARN)*
```
ps -ef | grep -i manager
```
*Check whether NameNode, DataNode & SecondaryNameNode  are running or not*
```
ps -ef | grep -i node
```
*HDFS services running under this user (NameNode, DataNode & SecondaryNameNode service)*
```
ps -fu hdfs
```
*YARN services running under this user (ResourceManager & NodeManager service)*
```
ps -fu yarn
```
```
jdbc:mysql://quickstart.cloudera.3306 --> JDBC URL
hdfs://quickstart.cloudera:8020 --> NameNode URL
