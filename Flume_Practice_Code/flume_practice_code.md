*Simple Flume agent, reading from Netcat and logging it*
```
flume-ng agent -n a1 -c /home/cloudera/flume/conf -f /home/cloudera/flume/conf/example.conf
```
*Source: Netcat Sink: HDFS*
```
flume-ng agent -n a1 -c /home/cloudera/flume/conf -f /home/cloudera/flume/conf/example_hdfs_sink.conf
```
*Source: Netcat Sink: HDFS as text*
```
flume-ng agent -n a1 -c /home/cloudera/flume/conf -f /home/cloudera/flume/conf/example_hdfs_datastream.conf
```
*Source: Log generator from cloudera Sink: HDFS*
/opt/gen_logs/start_logs.sh is used to generate logs @ /opt/gen_logs/logs/access.log and used these logs to store into HDFS. Configuration is similar to /opt/examples/flume/conf/flume.conf
```
flume-ng agent -n a1 -c /home/cloudera/flume/conf -f /home/cloudera/flume/conf/example_log_hdfs.conf
```

