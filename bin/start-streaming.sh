#!/usr/bin/env bash
spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --num-executors 1 \
 --executor-memory 1025M \
 --executor-cores 3 \
 --driver-memory 1025M \
 --conf spark.ui.retainedJobs=100 \
 --conf spark.ui.retainedTasks=1000 \
 --conf spark.ui.retainedStages=500 \
 --conf spark.streaming.ui.retainedBatches=6 \
 --conf spark.sql.shuffle.partitions=30 \
 --conf spark.yarn.max.executor.failures=24 \
 --conf spark.streaming.receiver.maxRate=100 \
 --conf spark.streaming.kafka.maxRatePerPartition=100 \
 --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -verbose:gc -XX:+UseG1GC -Xloggc:gc.log" \
 --conf 'spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -verbose:gc -XX:+UseG1GC -Xloggc:gc.log' \
 --class com.zbj.finance.datapipeline.straming.StreamingJob \
 --jars lib/hbase-annotations-1.1.2.2.6.1.0-129.jar,lib/hbase-client-1.1.2.2.6.1.0-129.jar,lib/hbase-common-1.1.2.2.6.1.0-129.jar,lib/hbase-examples-1.1.2.2.6.1.0-129.jar,lib/hbase-hadoop2-compat-1.1.2.2.6.1.0-129.jar,lib/hbase-hadoop-compat-1.1.2.2.6.1.0-129.jar,lib/hbase-it-1.1.2.2.6.1.0-129.jar,lib/hbase-prefix-tree-1.1.2.2.6.1.0-129.jar,lib/hbase-procedure-1.1.2.2.6.1.0-129.jar,lib/hbase-protocol-1.1.2.2.6.1.0-129.jar,lib/hbase-resource-bundle-1.1.2.2.6.1.0-129.jar,lib/hbase-rest-1.1.2.2.6.1.0-129.jar,lib/hbase-rsgroup-1.1.2.2.6.1.0-129.jar,lib/hbase-server-1.1.2.2.6.1.0-129.jar,lib/hbase-shell-1.1.2.2.6.1.0-129.jar,lib/hbase-thrift-1.1.2.2.6.1.0-129.jar,lib/metrics-core-2.2.0.jar,lib/zkclient-0.9.jar,lib/spark-streaming_2.11-2.1.1.2.6.1.0-129.jar,lib/spark-streaming-kafka-0-8_2.11-2.1.1.jar,lib/kafka_2.11-0.8.2.1.jar,lib/kafka-clients-0.8.2.2.jar \
 --files  conf/pipeline.properties  pipeline-1.0-SNAPSHOT.jar

